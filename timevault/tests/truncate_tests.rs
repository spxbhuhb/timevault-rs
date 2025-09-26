use std::fs;
use tempfile::TempDir;
use uuid::Uuid;

use timevault::store::paths;
use timevault::PartitionHandle;
use timevault::store::disk::manifest::ManifestLine;
use timevault::store::partition::{ChunkRollCfg, IndexCfg, RetentionCfg};

fn enc(ts: i64, value: serde_json::Value) -> Vec<u8> {
    let rec = serde_json::json!({"timestamp": ts, "payload": value});
    let mut v = serde_json::to_vec(&rec).unwrap();
    v.push(b'\n');
    v
}

fn write_metadata(part_dir: &std::path::Path, id: Uuid, roll_max_bytes: u64, index_max_records: u32) {
    use timevault::store::disk::metadata::MetadataJson;
    let m = MetadataJson {
        partition_id: id,
        format_version: 1,
        format_plugin: "jsonl".to_string(),
        chunk_roll: ChunkRollCfg { max_bytes: roll_max_bytes, max_hours: 0 },
        index: IndexCfg { max_records: index_max_records, max_hours: 0 },
        retention: RetentionCfg::default(),
        key_is_timestamp: true,
        logical_purge: false,
        last_purge_id: None,
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

#[test]
fn truncate_inside_closed_chunk_drops_partial_block() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let part_dir = paths::partition_dir(&root, id);
    fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    // Roll after roughly five records so first chunk becomes closed
    write_metadata(&part_dir, id, 150, 2);
    let h = PartitionHandle::open(root.clone(), id).unwrap();

    // First chunk will have records 1-5, second chunk record 6
    for i in 1..=6 { h.append(i, &enc(i as i64, serde_json::json!(i))).unwrap(); }

    // Cut inside the closed first chunk at key 4. Previously, record 3 (<4) was dropped
    // because it shared an index block with record 4. Now it must be preserved.
    h.truncate(4).unwrap();
    let data = h.read_range(0, 100).unwrap();
    assert_eq!(count_lines(&data), 3);
}

#[test]
fn truncate_idempotent() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let part_dir = paths::partition_dir(&root, id);
    fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    write_metadata(&part_dir, id, u64::MAX, 2);
    let h = PartitionHandle::open(root.clone(), id).unwrap();

    for i in 1..=5 { h.append(i, &enc(i as i64, serde_json::json!(i))).unwrap(); }

    let manifest_path = paths::partition_manifest(&part_dir);
    h.truncate(4).unwrap();
    let first = std::fs::read_to_string(&manifest_path).unwrap();
    h.truncate(4).unwrap();
    let second = std::fs::read_to_string(&manifest_path).unwrap();
    assert_eq!(first, second, "manifest should not change on repeat truncate");
    let data = h.read_range(1, 100).unwrap();
    assert_eq!(count_lines(&data), 3);
}

#[test]
fn truncate_open_chunk_without_index() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let part_dir = paths::partition_dir(&root, id);
    fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    // Large index cadence so no index lines exist yet
    write_metadata(&part_dir, id, u64::MAX, 100);
    let h = PartitionHandle::open(root.clone(), id).unwrap();

    for i in 1..=5 { h.append(i, &enc(i as i64, serde_json::json!(i))).unwrap(); }
    h.truncate(4).unwrap();
    let data = h.read_range(1, 100).unwrap();
    assert_eq!(count_lines(&data), 3);
}

#[test]
fn truncate_inside_open_chunk_with_preceding_closed_chunk() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let part_dir = paths::partition_dir(&root, id);
    fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    // Roll after roughly five records so first chunk becomes closed
    write_metadata(&part_dir, id, 150, 2);
    let h = PartitionHandle::open(root.clone(), id).unwrap();

    // Records 1-5 in first closed chunk, 6-10 in second open chunk
    for i in 1..=10 { h.append(i, &enc(i as i64, serde_json::json!(i))).unwrap(); }

    // Truncate inside the second (open) chunk at key 8 -> keep 1..7
    h.truncate(8).unwrap();
    let data = h.read_range(0, 100).unwrap();
    assert_eq!(count_lines(&data), 7);
}

#[test]
fn truncate_then_append_creates_index_file_in_partition_dir() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let part_dir = paths::partition_dir(&root, id);
    fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    // Aggressive indexing to ensure index files are written immediately
    write_metadata(&part_dir, id, u64::MAX, 1);
    let h = PartitionHandle::open(root.clone(), id).unwrap();

    // Initial append to create a chunk and index file, then truncate everything
    h.append(1, &enc(1, serde_json::json!(1))).unwrap();
    h.truncate(1).unwrap();

    // Regression check: appending again should place the index file under this partition directory
    h.append(2, &enc(2, serde_json::json!(2))).unwrap();

    let manifest_path = paths::partition_manifest(&part_dir);
    let lines = read_manifest_lines(&manifest_path);
    assert_eq!(lines.len(), 1, "expected a single manifest line after re-append");
    let chunk_id = lines[0].chunk_id;
    let chunks_dir = paths::chunks_dir(&part_dir);
    let index_path = paths::index_file(&chunks_dir, chunk_id);
    assert!(index_path.exists(), "expected index file {:?} to exist", index_path);
}
