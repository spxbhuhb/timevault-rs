#![cfg(test)]

use crate::PartitionHandle;
use crate::store::partition::PartitionConfig;
use crate::raft::errors::recv_unit;
use crate::raft::log::*;
use crate::raft::{TvrNodeId, paths, TvrConfig, state};
use openraft::storage::RaftLogStorage;
use openraft::testing::{StoreBuilder, Suite};
use openraft::{BasicNode, CommittedLeaderId, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, Membership, Vote};
use openraft::{RaftLogReader, StorageError};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::mpsc::channel;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
#[cfg(feature = "traced-tests")]
use tracing_test::traced_test;
use uuid::Uuid;
use test_utils::test_dir;

pub type ValueRequest = serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValueResponse(pub Result<serde_json::Value, serde_json::Value>);

impl Default for ValueResponse {
    fn default() -> Self {
        Self(Ok(serde_json::Value::Null))
    }
}
pub type ValueConfig = TvrConfig<ValueRequest, ValueResponse>;
pub type ValueLogAdapter = TvrLogAdapter<ValueRequest, ValueResponse>;
pub type ValueStateMachine = state::TvrPartitionStateMachine<ValueRequest, ValueResponse>;

fn mk_partition(tmp: &TempDir) -> PartitionHandle {
    let root = tmp.path().to_path_buf();
    let id = Uuid::nil();
    let mut cfg = PartitionConfig::default();
    cfg.format_plugin = "jsonl".to_string();
    PartitionHandle::create(root, id, cfg).expect("create partition")
}

fn mk_nodeid(n: u64) -> u64 {
    n
}

fn mk_log_id(term: u64, node: TvrNodeId, index: u64) -> LogId<TvrNodeId> {
    LogId::new(CommittedLeaderId::new(term, node), index)
}

fn mk_partition_owned(test_name: &str) -> (PathBuf, PartitionHandle) {
    let uuid = Uuid::now_v7();
    let root = test_dir(test_name, uuid);
    let mut cfg = PartitionConfig::default();
    cfg.format_plugin = "jsonl".to_string();
    let part = PartitionHandle::create(root.clone(), uuid, cfg).unwrap();
    (root, part)
}

#[test]
fn test_encode_entry_roundtrip_fields() {
    let node = mk_nodeid(0xB0);
    let log_id = mk_log_id(3, node, 7);
    let payload = serde_json::json!({"value": 42});
    let entry: Entry<ValueConfig> = Entry {
        log_id,
        payload: EntryPayload::Normal(payload.clone()),
    };

    let buf = encode_entry(&entry);
    assert!(buf.ends_with(b"\n"));

    let record: serde_json::Value = serde_json::from_slice(&buf[..buf.len() - 1]).unwrap();
    assert_eq!(record["timestamp"].as_i64(), Some(entry.log_id.index as i64));
    assert_eq!(record["log_id"]["index"].as_u64(), Some(entry.log_id.index));
    assert_eq!(record["kind"], serde_json::json!("Normal"));
    assert_eq!(record["payload"], payload);
}

#[test]
fn test_decode_entries_handles_membership_and_filters() {
    let (_root, part) = mk_partition_owned("test_decode_entries_handles_membership_and_filters");
    let node = mk_nodeid(0xC1);
    let log_id1 = mk_log_id(1, node, 5);
    let log_id2 = mk_log_id(1, node, 6);
    let mut members = BTreeMap::new();
    members.insert(node, BasicNode::default());
    let membership = Membership::new(vec![BTreeSet::from([node])], members);
    let entries: Vec<Entry<ValueConfig>> = vec![
        Entry {
            log_id: log_id1,
            payload: EntryPayload::Blank,
        },
        Entry {
            log_id: log_id2,
            payload: EntryPayload::Membership(membership.clone()),
        },
    ];
    let mut buf = Vec::new();
    for entry in &entries {
        buf.extend_from_slice(&encode_entry(entry));
    }

    let decoded: Vec<Entry<ValueConfig>> = decode_entries(&part, &buf, log_id2.index..=log_id2.index);
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].log_id, log_id2);
    match &decoded[0].payload {
        EntryPayload::Membership(m) => assert_eq!(m, &membership),
        _ => panic!("expected membership entry"),
    }

    let decoded_empty: Vec<Entry<ValueConfig>> = decode_entries(&part, b"notjson\n", 0..=10);
    assert!(decoded_empty.is_empty());
}

#[test]
fn test_recv_map_error_paths() {
    let (tx, rx) = channel::<Result<(), crate::errors::TvError>>();
    tx.send(Err(crate::errors::TvError::ReadOnly)).unwrap();
    let err = recv_unit(rx, ErrorSubject::Logs, ErrorVerb::Write).unwrap_err();
    let io = err.into_io().unwrap();
    assert!(io.to_string().contains("Logs"));
    assert!(io.to_string().contains("read-only"));

    let (_, rx_closed) = channel::<Result<(), crate::errors::TvError>>();
    let err_closed = recv_unit(rx_closed, ErrorSubject::Logs, ErrorVerb::Read).unwrap_err();
    let io_closed = err_closed.into_io().unwrap();
    assert!(io_closed.to_string().contains("Logs"));
    assert!(io_closed.to_string().contains("Read"));
}

#[test]
fn test_vote_and_purge_persistence_helpers() {
    let (_tmp, part) = mk_partition_owned("test_vote_and_purge_persistence_helpers");
    let node = mk_nodeid(0xD2);
    let vote = Vote::new(2, node);
    save_vote_file(&part, &vote).unwrap();
    let loaded = read_vote_file(&part).unwrap();
    assert_eq!(loaded, Some(vote));

    let log_id = mk_log_id(1, node, 3);
    purge_with_persist(&part, &log_id).unwrap();
    assert!(paths::purge_file(&part).exists());
    let stored = read_purge_file(&part).unwrap();
    assert_eq!(stored, Some(log_id));
}

#[test]
fn test_get_state_reports_last_ids() {
    let (_tmp, part) = mk_partition_owned("test_get_state_reports_last_ids");
    let node = mk_nodeid(0xE3);
    let log_id1 = mk_log_id(1, node, 1);
    let log_id2 = mk_log_id(2, node, 2);
    let entries: [Entry<ValueConfig>; 2] = [
        Entry {
            log_id: log_id1,
            payload: EntryPayload::Normal(serde_json::json!({"a": 1})),
        },
        Entry {
            log_id: log_id2,
            payload: EntryPayload::Normal(serde_json::json!({"b": 2})),
        },
    ];
    for entry in &entries {
        part.append(entry.log_id.index, &encode_entry(entry)).unwrap();
    }

    save_purge_file(&part, &log_id1).unwrap();
    let state = get_state::<ValueRequest, ValueResponse>(&part).unwrap();
    assert_eq!(state.last_purged_log_id, Some(log_id1));
    assert_eq!(state.last_log_id, Some(log_id2));
}

#[test]
fn test_decode_entries_roundtrip_and_filter() {
    let (_root, part) = mk_partition_owned("test_decode_entries_roundtrip_and_filter");

    // Build 3 records manually in jsonl with kind field
    let l1 = serde_json::json!({
        "timestamp": 1,
        "log_id": {"leader_id": {"term": 1, "node_id": 1}, "index": 1},
        "kind": "Normal",
        "payload": {"a":1}
    });
    let l2 = serde_json::json!({
        "timestamp": 2,
        "log_id": {"leader_id": {"term": 1, "node_id": 2}, "index": 2},
        "kind": "Normal",
        "payload": {"b":2}
    });
    let l3 = serde_json::json!({
        "timestamp": 3,
        "log_id": {"leader_id": {"term": 2, "node_id": 3}, "index": 3},
        "kind": "Normal",
        "payload": {"c":3}
    });
    let mut buf = Vec::new();
    for v in [l1, l2, l3] {
        let mut line = serde_json::to_vec(&v).unwrap();
        line.push(b'\n');
        buf.extend_from_slice(&line);
    }

    let out: Vec<Entry<ValueConfig>> = decode_entries(&part, &buf, 2..=3);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].log_id.index, 2);
    assert_eq!(out[1].log_id.index, 3);
    match &out[0].payload {
        EntryPayload::Normal(v) => assert_eq!(v["b"], serde_json::json!(2)),
        _ => panic!("unexpected payload"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_empty_partition_state_and_reads() {
    let td = TempDir::new().unwrap();
    let part = mk_partition(&td);
    let mut ad = ValueLogAdapter::new(part.clone(), 1);

    let st = ad.get_log_state().await.expect("get_log_state");
    assert!(st.last_log_id.is_none());
    assert!(st.last_purged_log_id.is_none());

    let mut rd = ad.get_log_reader().await;
    let ents = rd.try_get_log_entries(1..=10).await.expect("read range");
    assert!(ents.is_empty());

    let v = ad.read_vote().await.expect("read_vote");
    assert!(v.is_none());

    let p = read_purge_file(&part).expect("read_purge_file");
    assert!(p.is_none());

    assert!(!paths::vote_file(&part).exists());
    assert!(!paths::purge_file(&part).exists());
}

struct TvrStoreBuilder {}

impl StoreBuilder<ValueConfig, ValueLogAdapter, ValueStateMachine, ()> for TvrStoreBuilder {
    async fn build(&self) -> Result<((), ValueLogAdapter, ValueStateMachine), StorageError<TvrNodeId>> {
        let (_root, part) = mk_partition_owned("openraft_test_all");
        let node_id = part.id().as_u64_pair().1;
        let log = ValueLogAdapter::new(part.clone(), node_id);
        let state = ValueStateMachine::new(part.clone())?;
        Ok(((), log, state))
    }
}

#[cfg_attr(feature = "traced-tests", traced_test)]
#[test]
fn openraft_test_all() -> Result<(), StorageError<TvrNodeId>> {
    Suite::test_all(TvrStoreBuilder {})?;
    Ok(())
}

// #[traced_test]
// #[tokio::test(flavor = "multi_thread")]
// async fn openraft_test_single() {
//     let (_, l, s) = TvrStoreBuilder { }.build().await.expect("build");
//     Suite::<TvrConfig,TvrLogAdapter,TvrPartitionStateMachine, TvrStoreBuilder, ()>::get_initial_state_log_ids(l,s).await.expect("get_initial_state_log_ids");
// }

// #[traced_test]
// #[tokio::test(flavor = "multi_thread")]
// async fn openraft_test_transfer_snapshot() {
//     Suite::transfer_snapshot(&TvrStoreBuilder { }).await.expect("transfer_snapshot");
// }
