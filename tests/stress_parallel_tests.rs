use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

use timevault::config::{StoreConfig, PartitionConfig};
use timevault::store::Store;
use timevault::partition::PartitionHandle;
use timevault::plugins::FormatPlugin;

#[test]
fn stress_parallel_append_and_read() {
    const PARTS: usize = 4;
    const RECORDS_PER_PART: usize = 250_000; // 4 * 250k = 1M records

    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let store = Store::open(&root, StoreConfig::default()).unwrap();

    let mut handles = Vec::new();
    for _ in 0..PARTS {
        let id = Uuid::now_v7();
        let mut cfg = PartitionConfig::default();
        cfg.format_plugin = "jsonl_struct".to_string();
        PartitionHandle::create(root.clone(), id, cfg).unwrap();
        let h = store.open_partition(id).unwrap();
        handles.push(h);
    }
    let handles: Vec<_> = handles.into_iter().map(|h| Arc::new(h)).collect();

    std::thread::scope(|s| {
        // Append threads
        for (idx, h) in handles.iter().cloned().enumerate() {
            s.spawn(move || {
                let plugin = timevault::plugins::jsonl_struct::JsonlStructPlugin::default();
                for i in 0..RECORDS_PER_PART {
                    let ts = (idx as i64) * 1_000_000 + i as i64;
                    let payload = serde_json::json!({"a": i, "b": idx});
                    let line = plugin.encode(ts, &payload).unwrap();
                    h.append(ts, &line).unwrap();
                }
            });
        }
        // Read threads
        for h in handles.iter().cloned() {
            s.spawn(move || {
                for i in 0..100 {
                    let start = (i * 10_000) as i64;
                    let end = start + 5_000;
                    let _ = h.read_range(start, end);
                }
            });
        }
    });

    for h in handles {
        assert_eq!(h.stats().appends, RECORDS_PER_PART as u64);
    }
}

