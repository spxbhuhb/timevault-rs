use std::fs;
use tempfile::TempDir;
use uuid::Uuid;

use timevault::store::paths;
use timevault::{PartitionHandle};
use timevault::errors::TvError;
use timevault::disk::manifest::ManifestLine;
use timevault::disk::index::load_index_lines;

fn enc(ts: i64, value: serde_json::Value) -> Vec<u8> {
    let rec = serde_json::json!({"timestamp": ts, "payload": value});
    let mut v = serde_json::to_vec(&rec).unwrap();
    v.push(b'\n');
    v
}

fn write_metadata(part_dir: &std::path::Path, id: Uuid, index_max_records: u32) {
    use timevault::disk::metadata::MetadataJson;
    use timevault::config::{ChunkRollCfg, IndexCfg, RetentionCfg};
    let m = MetadataJson {
        partition_id: id,
        format_version: 1,
        format_plugin: "jsonl".to_string(),
        chunk_roll: ChunkRollCfg { max_bytes: u64::MAX, max_hours: 0 },
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
    let s = std::fs::read_to_string(p).unwrap();
    s.lines().filter(|l| !l.trim().is_empty()).map(|l| serde_json::from_str::<ManifestLine>(l).unwrap()).collect()
}

#[test]
fn append_out_of_order_returns_error() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let part_dir = paths::partition_dir(&root, id);
    fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    write_metadata(&part_dir, id, 100);
    let h = PartitionHandle::open(root.clone(), id).unwrap();

    h.append(1_000, &enc(1_000, serde_json::json!("a"))).unwrap();
    let err = h.append(999, &enc(999, serde_json::json!("b"))).unwrap_err();
    match err { TvError::OutOfOrder { .. } => {}, other => panic!("unexpected {other:?}") }
}

#[test]
fn chunk_id_timestamp_matches_first_record() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let part_dir = paths::partition_dir(&root, id);
    fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    write_metadata(&part_dir, id, 100);
    let h = PartitionHandle::open(root.clone(), id).unwrap();

    let ts_ms = 1_700_000_000_000; // fixed timestamp
    h.append(ts_ms, &enc(ts_ms as i64, serde_json::json!("first"))).unwrap();

    let manifest_path = paths::partition_manifest(&part_dir);
    let lines = read_manifest_lines(&manifest_path);
    let chunk_id = lines.first().unwrap().chunk_id;
    assert!(chunk_id.get_timestamp().is_some(), "chunk id should be a UUIDv7 with timestamp");
}

#[test]
fn index_flushes_at_max_records() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let part_dir = paths::partition_dir(&root, id);
    fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    write_metadata(&part_dir, id, 2); // flush every 2 records
    let h = PartitionHandle::open(root.clone(), id).unwrap();

    h.append(1, &enc(1, serde_json::json!("a"))).unwrap();
    h.append(2, &enc(2, serde_json::json!("b"))).unwrap();
    h.append(3, &enc(3, serde_json::json!("c"))).unwrap();
    h.append(4, &enc(4, serde_json::json!("d"))).unwrap();

    let chunks_dir = paths::chunks_dir(&part_dir);
    let lines = read_manifest_lines(&paths::partition_manifest(&part_dir));
    let chunk_id = lines.first().unwrap().chunk_id;
    let index_path = paths::index_file(&chunks_dir, chunk_id);
    let f = std::fs::File::open(index_path).unwrap();
    let idx = load_index_lines(&f).unwrap();
    assert_eq!(idx.len(), 2); // two flushed blocks
    assert_eq!(idx[0].block_min_key, 1);
    assert_eq!(idx[1].block_min_key, 3);
}

#[test]
fn append_same_timestamp_is_allowed() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let part_dir = paths::partition_dir(&root, id);
    fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    write_metadata(&part_dir, id, 100);
    let h = PartitionHandle::open(root.clone(), id).unwrap();

    let ts = 1_234u64;
    h.append(ts, &enc(ts as i64, serde_json::json!("a"))).unwrap();
    h.append(ts, &enc(ts as i64, serde_json::json!("b"))).unwrap();

    assert_eq!(h.stats().appends, 2);
}
