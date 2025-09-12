use std::fs;
use tempfile::TempDir;
use uuid::Uuid;

use timevault::store::paths;
use timevault::PartitionHandle;
use timevault::disk::manifest::ManifestLine;

fn enc(ts: i64, value: serde_json::Value) -> Vec<u8> {
    let rec = serde_json::json!({"timestamp": ts, "payload": value});
    let mut v = serde_json::to_vec(&rec).unwrap();
    v.push(b'\n');
    v
}

fn write_metadata(part_dir: &std::path::Path, id: Uuid, roll_max_bytes: u64, index_max_records: u32) {
    use timevault::disk::metadata::MetadataJson;
    use timevault::config::{ChunkRollCfg, IndexCfg, RetentionCfg};
    let m = MetadataJson {
        partition_id: id,
        format_version: 1,
        format_plugin: "jsonl".to_string(),
        chunk_roll: ChunkRollCfg { max_bytes: roll_max_bytes, max_hours: 0 },
        index: IndexCfg { max_records: index_max_records, max_hours: 0 },
        retention: RetentionCfg::default(),
        key_is_timestamp: true,
    };
    let p = paths::partition_metadata(part_dir);
    fs::write(p, serde_json::to_vec(&m).unwrap()).unwrap();
}

fn read_manifest_lines(p: &std::path::Path) -> Vec<ManifestLine> {
    let s = std::fs::read_to_string(p).unwrap_or_default();
    s.lines().filter(|l| !l.trim().is_empty()).map(|l| serde_json::from_str::<ManifestLine>(l).unwrap()).collect()
}

fn count_lines(bytes: &[u8]) -> usize {
    let s = std::str::from_utf8(bytes).unwrap();
    s.lines().filter(|l| !l.is_empty()).count()
}

#[test]
fn truncate_noop_when_cutoff_after_tail() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let part_dir = paths::partition_dir(&root, id);
    fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    write_metadata(&part_dir, id, u64::MAX, 100);
    let h = PartitionHandle::open(root.clone(), id).unwrap();

    for i in 1..=5 { h.append(i, &enc(i as i64, serde_json::json!(i))).unwrap(); }
    let manifest_path = paths::partition_manifest(&part_dir);
    let before = read_manifest_lines(&manifest_path);

    h.truncate(10).unwrap();

    let after = read_manifest_lines(&manifest_path);
    assert_eq!(before.len(), after.len(), "manifest lines should be unchanged on noop");
    let data = h.read_range(1, 100).unwrap();
    assert_eq!(count_lines(&data), 5);
}

#[test]
fn truncate_inside_open_chunk() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let part_dir = paths::partition_dir(&root, id);
    fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    // No rolling, small index cadence
    write_metadata(&part_dir, id, u64::MAX, 2);
    let h = PartitionHandle::open(root.clone(), id).unwrap();

    for i in 1..=5 { h.append(i, &enc(i as i64, serde_json::json!(i))).unwrap(); }
    // Truncate from 4 inclusive → keep 1,2,3
    h.truncate(4).unwrap();
    let data = h.read_range(1, 100).unwrap();
    assert_eq!(count_lines(&data), 3);
}

#[test]
fn truncate_removes_newer_chunks() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let part_dir = paths::partition_dir(&root, id);
    fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    // Force small chunk size to roll quickly
    write_metadata(&part_dir, id, 80, 2);
    let h = PartitionHandle::open(root.clone(), id).unwrap();

    // Append multiple records to produce >=2 chunks
    for i in 1..=20 { h.append(i, &enc(i as i64, serde_json::json!(i))).unwrap(); }
    let manifest_path = paths::partition_manifest(&part_dir);
    let before = read_manifest_lines(&manifest_path);
    assert!(before.len() >= 2);

    // Truncate at the min of the last chunk should remove it entirely
    let last = before.last().unwrap().clone();
    h.truncate(last.min_order_key).unwrap();
    let after = read_manifest_lines(&manifest_path);
    // After should have removed the last chunk manifest line
    assert_eq!(after.len(), before.len() - 1);
}

#[test]
fn truncate_to_empty_partition() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let part_dir = paths::partition_dir(&root, id);
    fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    write_metadata(&part_dir, id, u64::MAX, 100);
    let h = PartitionHandle::open(root.clone(), id).unwrap();

    for i in 10..=12 { h.append(i, &enc(i as i64, serde_json::json!(i))).unwrap(); }
    h.truncate(1).unwrap(); // cutoff before earliest

    let manifest_path = paths::partition_manifest(&part_dir);
    let after = read_manifest_lines(&manifest_path);
    assert!(after.is_empty(), "manifest should be empty after full truncate");
    // Reads should return empty
    let data = h.read_range(0, 1000).unwrap();
    assert_eq!(data.len(), 0);
}
