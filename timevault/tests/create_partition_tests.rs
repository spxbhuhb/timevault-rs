use tempfile::TempDir;
use uuid::Uuid;

use timevault::PartitionHandle;
use timevault::store::partition::PartitionConfig;
use timevault::store::paths;

#[test]
fn create_initializes_structure_and_metadata() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();

    let mut cfg = PartitionConfig::default();
    cfg.format_plugin = "jsonl".to_string();

    let h = PartitionHandle::create(root.clone(), id, cfg.clone()).unwrap();
    assert_eq!(h.id(), id);

    let part_dir = paths::partition_dir(&root, id);
    assert!(paths::chunks_dir(&part_dir).exists());
    assert!(paths::tmp_dir(&part_dir).exists());
    assert!(paths::gc_dir(&part_dir).exists());

    let meta_path = paths::partition_metadata(&part_dir);
    assert!(meta_path.exists());
    let m = std::fs::read_to_string(&meta_path).unwrap();
    assert!(m.contains("jsonl"));

    let manifest_path = paths::partition_manifest(&part_dir);
    assert!(manifest_path.exists());
    let s = std::fs::read_to_string(&manifest_path).unwrap();
    assert!(s.is_empty(), "manifest should be empty");
}

#[test]
fn open_after_create_succeeds() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();

    let h = PartitionHandle::create(root.clone(), id, PartitionConfig::default()).unwrap();
    drop(h);

    let h2 = PartitionHandle::open(root.clone(), id).unwrap();
    assert_eq!(h2.id(), id);
}
