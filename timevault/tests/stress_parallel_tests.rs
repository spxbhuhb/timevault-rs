#![cfg(feature = "stress-tests")]
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

use timevault::store::Store;
use timevault::store::StoreConfig;
use timevault::store::partition::{PartitionConfig, PartitionHandle};
use timevault::store::plugins::FormatPlugin;

#[test]
fn stress_parallel_append_and_read() {
    const PARTS: usize = 4;
    const RECORDS_PER_PART: usize = 250_000; // 4 * 250k = 1M records

    // Persist outputs under ./target/test similar to append_persistent_test
    let project_root = project_root();
    let tests_root = project_root.join("../target").join("test");
    fs::create_dir_all(&tests_root).unwrap();

    // Clear previous runs for this test to avoid accumulation
    if let Ok(entries) = fs::read_dir(&tests_root) {
        for e in entries.flatten() {
            if let Ok(name) = e.file_name().into_string() {
                if name.starts_with("stress_parallel_") {
                    let _ = fs::remove_dir_all(e.path());
                }
            }
        }
    }

    let id = Uuid::now_v7();
    let root = tests_root.join(format!("stress_parallel_{}", id));
    fs::create_dir_all(&root).unwrap();
    eprintln!("stress_parallel test root: {:?}", root);

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
                let plugin = timevault::store::plugins::jsonl_struct::JsonlStructPlugin::default();
                for i in 0..RECORDS_PER_PART {
                    let ts = (idx as i64) * 1_000_000 + i as i64;
                    let payload = serde_json::json!({"a": i, "b": idx});
                    let line = plugin.encode(ts, &payload).unwrap();
                    h.append(ts as u64, &line).unwrap();
                }
            });
        }
        // Read threads
        for h in handles.iter().cloned() {
            s.spawn(move || {
                for i in 0..100 {
                    let start = (i * 10_000) as i64;
                    let end = start + 5_000;
                    let _ = h.read_range(start as u64, end as u64);
                }
            });
        }
    });

    for h in handles {
        assert_eq!(h.stats().appends, RECORDS_PER_PART as u64);
    }
}

// Best-effort to get the project root when running with `cargo test`.
fn project_root() -> PathBuf {
    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        return PathBuf::from(manifest_dir);
    }
    std::env::current_dir().unwrap()
}
