#![cfg(feature = "openraft-adapter")]

use tempfile::TempDir;
use uuid::Uuid;

use timevault::partition::PartitionHandle;
use timevault::config::PartitionConfig;

use openraft::{Entry, LogId, RaftTypeConfig, Vote};
use openraft::storage::{RaftLogReader, RaftLogStorage};
use timevault::plugins::FormatPlugin;
use serde_json;

// A minimal RaftTypeConfig for tests. Adjust as needed to satisfy openraft 0.9.21.
#[derive(Debug)]
struct testCfg;

impl RaftTypeConfig for testCfg {
    type NodeId = u64;
    type Entry = u64;
}

// Helper to build a partition configured for openraft adapter usage
fn build_partition() -> PartitionHandle {
    let td = TempDir::new().unwrap();
    let root = td.into_path(); // keep dir alive for test duration
    let id = Uuid::now_v7();
    let mut cfg = PartitionConfig::default();
    cfg.format_plugin = "jsonl".to_string();
    cfg.key_is_timestamp = false;
    PartitionHandle::create(root, id, cfg).unwrap()
}

#[test]
fn append_and_read_back() {
    let part = build_partition();
    let mut log = timevault::store::openraft_adapter::TvRaftLog::<testCfg>::new(part.clone()).unwrap();

    // Manually append 3 entries with indices 1..=3 via partition
    let plugin = timevault::plugins::resolve_plugin("jsonl").unwrap();
    for i in 1u64..=3 {
        let ent = Entry { log_id: LogId::new(1, i, 1u64), payload: i };
        let ts = ent.log_id.index as i64;
        let val = serde_json::to_value(&ent).unwrap();
        let buf = plugin.encode(ts, &val).unwrap();
        part.append(ent.log_id.index, &buf).unwrap();
    }

    // Read [1,4)
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut reader = rt.block_on(RaftLogStorage::<testCfg>::get_log_reader(&mut log)).unwrap();
    let got = rt.block_on(RaftLogReader::<testCfg>::try_get_log_entries(&mut reader, 1..4)).unwrap();
    assert_eq!(got.len(), 3);
    assert_eq!(got[0].payload, 1);
    assert_eq!(got[1].payload, 2);
    assert_eq!(got[2].payload, 3);
}

#[test]
fn vote_roundtrip() {
    let part = build_partition();
    let mut log = timevault::store::openraft_adapter::TvRaftLog::<testCfg>::new(part.clone()).unwrap();

    let v = Vote::new(3, 42u64);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(RaftLogStorage::<testCfg>::save_vote(&mut log, &v)).unwrap();
    let r = rt.block_on(RaftLogStorage::<testCfg>::read_vote(&mut log)).unwrap();
    assert_eq!(r, Some(v));
}

#[test]
fn get_state_and_purge_truncate() {
    let part = build_partition();
    let mut log = timevault::store::openraft_adapter::TvRaftLog::<testCfg>::new(part.clone()).unwrap();

    // Initially empty
    let rt = tokio::runtime::Runtime::new().unwrap();
    let st = rt.block_on(RaftLogStorage::<testCfg>::get_log_state(&mut log)).unwrap();
    assert!(st.last_log_id.is_none());
    assert!(st.last_purged_log_id.is_none());

    // Append 1..=5 directly via partition
    let plugin = timevault::plugins::resolve_plugin("jsonl").unwrap();
    for i in 1u64..=5 {
        let ent = Entry { log_id: LogId::new(1, i, 1u64), payload: i };
        let ts = ent.log_id.index as i64;
        let val = serde_json::to_value(&ent).unwrap();
        let buf = plugin.encode(ts, &val).unwrap();
        part.append(ent.log_id.index, &buf).unwrap();
    }

    // Truncate at 3 (inclusive): remains 1..=3
    rt.block_on(RaftLogStorage::<testCfg>::truncate(&mut log, 3)).unwrap();

    let mut reader = rt.block_on(RaftLogStorage::<testCfg>::get_log_reader(&mut log)).unwrap();
    let got = rt.block_on(RaftLogReader::<testCfg>::try_get_log_entries(&mut reader, 1..10)).unwrap();
    assert_eq!(got.iter().map(|e| e.payload).collect::<Vec<_>>(), vec![1,2,3]);

    // Purge at 2 (inclusive): deletes up to 2
    let purge_id = LogId::new(1, 2, 1u64);
    rt.block_on(RaftLogStorage::<testCfg>::purge(&mut log, purge_id.clone())).unwrap();

    let st2 = rt.block_on(RaftLogStorage::<testCfg>::get_log_state(&mut log)).unwrap();
    assert_eq!(st2.last_purged_log_id, Some(purge_id));
    assert_eq!(st2.last_log_id.unwrap().index, 3);
}
