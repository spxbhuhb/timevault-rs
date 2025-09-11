use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;

use tempfile::TempDir;
use uuid::Uuid;

use timevault::store::paths;
use timevault::disk::manifest::ManifestLine;
use timevault::disk::index::IndexLine;
use timevault::partition::recovery::load_partition_runtime_data;

fn setup_partition(root: &PathBuf, id: Uuid) -> (PathBuf, PathBuf) {
    let part_dir = paths::partition_dir(root, id);
    let chunks_dir = paths::chunks_dir(&part_dir);
    fs::create_dir_all(&chunks_dir).unwrap();
    write_metadata(&part_dir, id);
    (part_dir, chunks_dir)
}

fn write_metadata(part_dir: &PathBuf, id: Uuid) {
    use timevault::disk::metadata::MetadataJson;
    let m = MetadataJson {
        partition_id: id,
        format_version: 1,
        format_plugin: "jsonl".to_string(),
        chunk_roll: Default::default(),
        index: Default::default(),
        retention: Default::default(),
    };
    let p = paths::partition_metadata(part_dir);
    let s = serde_json::to_vec(&m).unwrap();
    std::fs::write(p, s).unwrap();
}

fn write_manifest_line(manifest_path: &PathBuf, line: &ManifestLine) {
    let mut f = fs::OpenOptions::new().create(true).append(true).open(manifest_path).unwrap();
    let mut buf = serde_json::to_vec(line).unwrap();
    buf.push(b'\n');
    f.write_all(&buf).unwrap();
}

fn write_chunk(chunks_dir: &PathBuf, chunk_id: Uuid, data: &[u8]) -> PathBuf {
    let p = paths::chunk_file(chunks_dir, chunk_id);
    let mut f = File::create(&p).unwrap();
    f.write_all(data).unwrap();
    p
}

fn write_index(chunks_dir: &PathBuf, chunk_id: Uuid, lines: &[IndexLine]) -> PathBuf {
    let p = paths::index_file(chunks_dir, chunk_id);
    let mut f = File::create(&p).unwrap();
    for l in lines {
        let mut buf = serde_json::to_vec(l).unwrap();
        buf.push(b'\n');
        f.write_all(&buf).unwrap();
    }
    p
}

#[test]
fn recovery_no_manifest_returns_default() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    // Create partition dir and metadata but do not create manifest
    let part_dir = paths::partition_dir(&root, id);
    std::fs::create_dir_all(paths::chunks_dir(&part_dir)).unwrap();
    write_metadata(&part_dir, id);
    let cache = load_partition_runtime_data(&root, id).unwrap();
    assert!(cache.cur_chunk_id.is_none());
    assert_eq!(cache.cur_chunk_size_bytes, 0);
}

#[test]
fn recovery_missing_chunk_yields_error() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let (part_dir, chunks_dir) = setup_partition(&root, id);
    let manifest = paths::partition_manifest(&part_dir);

    let chunk_id = Uuid::now_v7();
    // Manifest references last chunk id but we don't create the chunk file
    write_manifest_line(&manifest, &ManifestLine { chunk_id, min_ts_ms: 100, max_ts_ms: Some(200) });

    let err = load_partition_runtime_data(&root, id).unwrap_err();
    match err { timevault::errors::TvError::MissingFile { path } => assert_eq!(path, paths::chunk_file(&chunks_dir, chunk_id)), other => panic!("unexpected error: {other:?}") }
}

#[test]
fn recovery_missing_index_yields_error() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let (part_dir, chunks_dir) = setup_partition(&root, id);
    let manifest = paths::partition_manifest(&part_dir);

    let chunk_id = Uuid::now_v7();
    write_manifest_line(&manifest, &ManifestLine { chunk_id, min_ts_ms: 100, max_ts_ms: Some(300) });
    // Create chunk file but not index
    write_chunk(&chunks_dir, chunk_id, br#"{"timestamp":100}\n{"timestamp":200}\n"#.as_ref());

    let err = load_partition_runtime_data(&root, id).unwrap_err();
    match err { timevault::errors::TvError::MissingFile { path } => assert_eq!(path, paths::index_file(&chunks_dir, chunk_id)), other => panic!("unexpected error: {other:?}") }
}

#[test]
fn recovery_with_index_extends_block_and_reads_last_record() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let (part_dir, chunks_dir) = setup_partition(&root, id);
    let manifest = paths::partition_manifest(&part_dir);

    let chunk_id = Uuid::now_v7();
    write_manifest_line(&manifest, &ManifestLine { chunk_id, min_ts_ms: 100, max_ts_ms: Some(400) });

    // Build chunk: block (two records) then tail (one record)
    // Offsets: 0: {100}\n (9 bytes), 9: {200}\n (9 bytes), 18: {350}\n (9 bytes)
    let rec1 = b"{\"timestamp\":100}\n"; // 13? we'll compute len from bytes
    let rec2 = b"{\"timestamp\":200}\n";
    let tail = b"{\"timestamp\":350}\n";
    let mut chunk = Vec::new();
    chunk.extend_from_slice(rec1);
    let off2 = chunk.len() as u64;
    chunk.extend_from_slice(rec2);
    let off3 = chunk.len() as u64;
    chunk.extend_from_slice(tail);
    write_chunk(&chunks_dir, chunk_id, &chunk);

    // Index covers first two records as one block
    let block_len = rec1.len() as u64 + rec2.len() as u64;
    let idx = vec![IndexLine { block_min_ms: 100, block_max_ms: 200, file_offset_bytes: 0, block_len_bytes: block_len }];
    write_index(&chunks_dir, chunk_id, &idx);

    let cache = load_partition_runtime_data(&root, id).unwrap();
    // last_index_block_min/max should be extended to 350, and size increased by tail len
    assert_eq!(cache.cur_index_block_min_ts_ms, 100);
    assert_eq!(cache.cur_index_block_max_ts_ms, 350);
    assert_eq!(cache.cur_index_block_record_count, 1); // only tail counted in recovery extension
    assert_eq!(cache.cur_index_block_size_bytes, block_len + tail.len() as u64);
    assert_eq!(cache.cur_chunk_max_ts_ms, 350);
    // last_record_bytes should equal the tail line bytes
    assert_eq!(cache.cur_last_record_bytes.as_deref(), Some(tail.as_ref()));
}

#[test]
fn recovery_two_chunks_second_has_empty_index_initializes_runtime() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let (part_dir, chunks_dir) = setup_partition(&root, id);
    let manifest = paths::partition_manifest(&part_dir);

    // First chunk: closed with index
    let chunk1 = Uuid::now_v7();
    let rec1a = b"{\"timestamp\":1000}\n";
    let rec1b = b"{\"timestamp\":1001}\n";
    let mut data1 = Vec::new(); data1.extend_from_slice(rec1a); data1.extend_from_slice(rec1b);
    write_chunk(&chunks_dir, chunk1, &data1);
    // Index: one block covering both
    let idx1 = vec![IndexLine { block_min_ms: 1000, block_max_ms: 1001, file_offset_bytes: 0, block_len_bytes: (rec1a.len()+rec1b.len()) as u64 }];
    write_index(&chunks_dir, chunk1, &idx1);
    // Closed line
    write_manifest_line(&manifest, &ManifestLine { chunk_id: chunk1, min_ts_ms: 1000, max_ts_ms: Some(1001) });

    // Second chunk: open (no max), with an empty index file present
    let chunk2 = Uuid::now_v7();
    let rec2a = b"{\"timestamp\":1002}\n";
    let rec2b = b"{\"timestamp\":1003}\n";
    let mut data2 = Vec::new(); data2.extend_from_slice(rec2a); data2.extend_from_slice(rec2b);
    write_chunk(&chunks_dir, chunk2, &data2);
    // Touch empty index file for second chunk
    {
        let ip = paths::index_file(&chunks_dir, chunk2);
        std::fs::File::create(&ip).unwrap();
    }
    // Open manifest entry for second chunk
    write_manifest_line(&manifest, &ManifestLine { chunk_id: chunk2, min_ts_ms: 1002, max_ts_ms: None });

    let rt = load_partition_runtime_data(&root, id).unwrap();
    // Runtime should point to the current (second) chunk
    assert_eq!(rt.cur_chunk_id, Some(chunk2));
    assert_eq!(rt.cur_chunk_min_ts_ms, 1002);
    assert_eq!(rt.cur_chunk_size_bytes, data2.len() as u64);
    // Because the second index is empty, recovery treats it as no index and scans entire file
    assert_eq!(rt.cur_index_block_min_ts_ms, 1002);
    assert_eq!(rt.cur_index_block_max_ts_ms, 1003);
    assert_eq!(rt.cur_index_block_size_bytes, data2.len() as u64);
    assert_eq!(rt.cur_index_block_record_count, 2);
    // Chunk max should be determined from scan
    assert_eq!(rt.cur_chunk_max_ts_ms, 1003);
    // last record bytes equals rec2b
    assert_eq!(rt.cur_last_record_bytes.as_deref(), Some(rec2b.as_ref()));
}

#[test]
fn recovery_seek_to_misaligned_offset_errors() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let id = Uuid::now_v7();
    let (part_dir, chunks_dir) = setup_partition(&root, id);
    let manifest = paths::partition_manifest(&part_dir);

    let chunk_id = Uuid::now_v7();
    write_manifest_line(&manifest, &ManifestLine { chunk_id, min_ts_ms: 100, max_ts_ms: Some(400) });

    // Create chunk with two valid JSONL records
    let rec1 = b"{\"timestamp\":100}\n";
    let rec2 = b"{\"timestamp\":200}\n";
    let mut chunk = Vec::new();
    chunk.extend_from_slice(rec1);
    chunk.extend_from_slice(rec2);
    write_chunk(&chunks_dir, chunk_id, &chunk);

    // Corrupt index to point to a non-line-boundary start (offset 1 long block)
    let idx = vec![IndexLine { block_min_ms: 100, block_max_ms: 100, file_offset_bytes: 0, block_len_bytes: 1 }];
    write_index(&chunks_dir, chunk_id, &idx);

    let err = load_partition_runtime_data(&root, id).unwrap_err();
    // Expect an Io error originating from seek_to invalid input
    match err {
        timevault::errors::TvError::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io InvalidInput, got {other:?}"),
    }
}
