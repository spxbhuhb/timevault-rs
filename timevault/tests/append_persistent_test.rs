use std::fs;
use uuid::Uuid;
use test_utils::test_dir;
use timevault::store::paths;
use timevault::PartitionHandle;
use timevault::disk::manifest::ManifestLine;

// This test intentionally keeps its outputs on disk under target/test so they can be
// manually inspected after `cargo test`. Other tests continue to use TempDir.
fn enc(ts: i64, value: serde_json::Value) -> Vec<u8> {
    let rec = serde_json::json!({"timestamp": ts, "payload": value});
    let mut v = serde_json::to_vec(&rec).unwrap();
    v.push(b'\n');
    v
}

#[test]
fn append_persistent_outputs_under_target_test() {

    let id = Uuid::now_v7();
    let root = test_dir("append_persistent", id);

    // Prepare partition dir and metadata with small max_bytes to allow small chunks
    let part_dir = paths::partition_dir(&root, id);
    fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    write_metadata_with_roll(&part_dir, id, 80, 24); // tiny size cap

    // Open handle and append a few records. This should create files under target/test
    let h = PartitionHandle::open(root.clone(), id).unwrap();
    h.append(1_000, &enc(1_000, serde_json::json!("hello"))).unwrap();
    h.append(1_001, &enc(1_001, serde_json::json!("world"))).unwrap();
    // Third append may roll depending on line sizes
    h.append(1_002, &enc(1_002, serde_json::json!("again"))).unwrap();

    // Sanity: manifest exists and has at least one line
    let manifest_path = paths::partition_manifest(&part_dir);
    assert!(manifest_path.exists(), "manifest.json should exist at {:?}", manifest_path);
    let lines = read_manifest_lines(&manifest_path);
    assert!(!lines.is_empty(), "manifest should have at least one line");
    // At least one index file should exist for a closed chunk (rolled)
    let chunks_dir = paths::chunks_dir(&part_dir);
    let mut any_index = false;
    for l in &lines {
        if l.max_order_key.is_some() {
            let ip = paths::index_file(&chunks_dir, l.chunk_id);
            if ip.exists() { any_index = true; break; }
        }
    }
    assert!(any_index, "expected at least one index file for a closed chunk");
}

fn write_metadata_with_roll(part_dir: &std::path::Path, id: Uuid, max_bytes: u64, max_hours: u64) {
    use timevault::disk::metadata::MetadataJson;
    use timevault::config::{ChunkRollCfg, IndexCfg, RetentionCfg};
    fs::create_dir_all(part_dir).unwrap();
    let m = MetadataJson {
        partition_id: id,
        format_version: 1,
        format_plugin: "jsonl".to_string(),
        chunk_roll: ChunkRollCfg { max_bytes, max_hours },
        index: IndexCfg::default(),
        retention: RetentionCfg::default(),
        key_is_timestamp: true,
        logical_purge: false,
        last_purge_id: None,
    };
    let p = paths::partition_metadata(part_dir);
    std::fs::write(p, serde_json::to_vec(&m).unwrap()).unwrap();
}

fn read_manifest_lines(p: &std::path::Path) -> Vec<ManifestLine> {
    let s = std::fs::read_to_string(p).unwrap();
    s.lines().filter(|l| !l.trim().is_empty()).map(|l| serde_json::from_str::<ManifestLine>(l).unwrap()).collect()
}
