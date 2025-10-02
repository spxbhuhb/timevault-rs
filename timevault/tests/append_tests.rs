use tempfile::TempDir;
use uuid::Uuid;

use timevault::PartitionHandle;

#[test]
fn append_skips_duplicate_last_record() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();

    // Create a new partition with default config (jsonl plugin)
    let part = PartitionHandle::create(root.clone(), id, Default::default()).unwrap();

    let payload = b"{\"timestamp\":100}\n";

    // First append writes the record
    let ack1 = part.append(100, payload).unwrap();
    assert_eq!(ack1.offset, 0, "first append starts at offset 0");

    // Second append with identical key and payload should be skipped (idempotent)
    let _ack2 = part.append(100, payload).unwrap();

    // Stats should reflect only one actual append
    let stats = part.stats();
    assert_eq!(stats.appends, 1, "duplicate append should be skipped");
    assert_eq!(stats.bytes as usize, payload.len());

    // Under the hood, chunk file size should equal a single record length
    let part_dir = timevault::store::paths::partition_dir(part.root(), part.id());
    let chunks_dir = timevault::store::paths::chunks_dir(&part_dir);
    // New chunk id equals first order_key per implementation
    let chunk_path = timevault::store::paths::chunk_file(&chunks_dir, 100);
    let meta = std::fs::metadata(&chunk_path).unwrap();
    assert_eq!(meta.len() as usize, payload.len());
}

