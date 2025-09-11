use std::fs::{self};
use tempfile::TempDir;
use uuid::Uuid;

use timevault::store::paths;
use timevault::PartitionHandle;
use timevault::disk::manifest::ManifestLine;

fn write_metadata_with_roll(part_dir: &std::path::Path, id: Uuid, max_bytes: u64, max_hours: u64) {
    use timevault::disk::metadata::MetadataJson;
    use timevault::config::{ChunkRollCfg, IndexCfg, RetentionCfg};
    let m = MetadataJson {
        partition_id: id,
        format_version: 1,
        format_plugin: "jsonl".to_string(),
        chunk_roll: ChunkRollCfg { max_bytes, max_hours },
        index: IndexCfg::default(),
        retention: RetentionCfg::default(),
        key_is_timestamp: true,
    };
    let p = paths::partition_metadata(part_dir);
    std::fs::write(p, serde_json::to_vec(&m).unwrap()).unwrap();
}

fn read_manifest_lines(p: &std::path::Path) -> Vec<ManifestLine> {
    let s = std::fs::read_to_string(p).unwrap();
    s.lines().filter(|l| !l.trim().is_empty()).map(|l| serde_json::from_str::<ManifestLine>(l).unwrap()).collect()
}

fn enc(ts: i64, value: serde_json::Value) -> Vec<u8> {
    let rec = serde_json::json!({"timestamp": ts, "payload": value});
    let mut v = serde_json::to_vec(&rec).unwrap();
    v.push(b'\n');
    v
}

#[test]
fn roll_by_size_writes_close_and_new_open() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let part_dir = paths::partition_dir(&root, id);
    fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    // Write metadata with small max_bytes to trigger rolling
    write_metadata_with_roll(&part_dir, id, 20, 24);

    let h = PartitionHandle::open(root.clone(), id).unwrap();

    // Each record line will be > 10 bytes (ts and payload_len)
    h.append(1_000, &enc(1_000, serde_json::json!("a"))).unwrap();
    h.append(1_001, &enc(1_001, serde_json::json!("b"))).unwrap();
    // This should have triggered a roll before the second append or before third
    h.append(1_002, &enc(1_002, serde_json::json!("c"))).unwrap();

    let manifest_path = paths::partition_manifest(&part_dir);
    let lines = read_manifest_lines(&manifest_path);
    assert!(lines.len() >= 2, "expected at least two manifest lines, got {}", lines.len());
    // There should be at least one closed (with max) and last open (None)
    let any_closed = lines.iter().any(|l| l.max_order_key.is_some());
    assert!(any_closed, "expected at least one closed manifest entry");
    assert!(lines.last().unwrap().max_order_key.is_none(), "last manifest line should be open");
}

#[test]
fn roll_by_time_after_threshold() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let part_dir = paths::partition_dir(&root, id);
    fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    // Write metadata with 1 hour time roll and huge size to disable size-based
    write_metadata_with_roll(&part_dir, id, u64::MAX, 1);

    let h = PartitionHandle::open(root.clone(), id).unwrap();

    let base = 1_000_000; // ms
    h.append(base, b"x").unwrap();
    // +30 min: no roll expected yet
    h.append(base + 30 * 60 * 1000, b"y").unwrap();
    // +90 min: should roll before appending
    h.append(base + 90 * 60 * 1000, b"z").unwrap();

    let manifest_path = paths::partition_manifest(&part_dir);
    let lines = read_manifest_lines(&manifest_path);
    assert!(lines.len() >= 2);
    let any_closed = lines.iter().any(|l| l.max_order_key.is_some());
    assert!(any_closed, "expected at least one closed manifest entry");
    assert!(lines.last().unwrap().max_order_key.is_none(), "last manifest line should be open");
}
