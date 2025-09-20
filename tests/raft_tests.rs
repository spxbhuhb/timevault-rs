use openraft::storage::RaftLogStorage;
use openraft::RaftLogReader;
use timevault::PartitionHandle;
use timevault::config::PartitionConfig;
use timevault::store::paths;
use openraft::EntryPayload;
use tempfile::TempDir;
use uuid::Uuid;

fn mk_partition(tmp: &TempDir) -> PartitionHandle {
    let root = tmp.path().to_path_buf();
    let id = Uuid::nil();
    let mut cfg = PartitionConfig::default();
    cfg.format_plugin = "jsonl".to_string();
    PartitionHandle::create(root, id, cfg).expect("create partition")
}

#[test]
fn test_decode_entries_roundtrip_and_filter() {
    // Build 3 records manually in jsonl with kind field
    let nid = Uuid::nil();
    let l1 = serde_json::json!({
        "timestamp": 1,
        "log_id": {"leader_id": {"term": 1, "node_id": nid}, "index": 1},
        "kind": "Normal",
        "payload": {"a":1}
    });
    let l2 = serde_json::json!({
        "timestamp": 2,
        "log_id": {"leader_id": {"term": 1, "node_id": nid}, "index": 2},
        "kind": "Normal",
        "payload": {"b":2}
    });
    let l3 = serde_json::json!({
        "timestamp": 3,
        "log_id": {"leader_id": {"term": 2, "node_id": nid}, "index": 3},
        "kind": "Normal",
        "payload": {"c":3}
    });
    let mut buf = Vec::new();
    for v in [l1,l2,l3] {
        let mut line = serde_json::to_vec(&v).unwrap();
        line.push(b'\n');
        buf.extend_from_slice(&line);
    }

    let out = timevault::raft::decode_entries_for_tests(&buf, 2..=3);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].log_id.index, 2);
    assert_eq!(out[1].log_id.index, 3);
    match &out[0].payload {
        EntryPayload::Normal(timevault::raft::Request(v)) => assert_eq!(v["b"], serde_json::json!(2)),
        _ => panic!("unexpected payload"),
    }
}

#[tokio::test(flavor = "multi_thread")] 
async fn test_empty_partition_state_and_reads() {
    let td = TempDir::new().unwrap();
    let part = mk_partition(&td);
    let nid = Uuid::nil();
    let mut ad = timevault::raft::TvrLogAdapter::new(part.clone(), nid);

    let st = ad.get_log_state().await.expect("get_log_state");
    assert!(st.last_log_id.is_none());
    assert!(st.last_purged_log_id.is_none());

    let mut rd = ad.get_log_reader().await;
    let ents = rd.try_get_log_entries(1..=10).await.expect("read range");
    assert!(ents.is_empty());

    let v = ad.read_vote().await.expect("read_vote");
    assert!(v.is_none());

    let p = timevault::raft::read_purge_file_for_tests(&part).expect("read_purge_file");
    assert!(p.is_none());

    let part_dir = paths::partition_dir(part.root(), part.id());
    assert!(!paths::raft_vote_file(&part_dir).exists());
    assert!(!paths::raft_purge_file(&part_dir).exists());
}
