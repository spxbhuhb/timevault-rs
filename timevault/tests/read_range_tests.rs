use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use tempfile::TempDir;
use uuid::Uuid;

use timevault::store::paths;
use timevault::{PartitionHandle};
use timevault::errors::TvError;
use timevault::disk::manifest::ManifestLine;
use timevault::disk::index::IndexLine;

fn setup_partition(root: &PathBuf) -> (PartitionHandle, PathBuf, PathBuf) {
    let pid = Uuid::now_v7();
    let part_dir = paths::partition_dir(root, pid);
    let chunks_dir = paths::chunks_dir(&part_dir);
    fs::create_dir_all(&chunks_dir).unwrap();
    write_metadata(&part_dir, pid);
    let handle = PartitionHandle::open(root.clone(), pid).unwrap();
    (handle, part_dir, chunks_dir)
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
        key_is_timestamp: true,
        logical_purge: false,
        last_purge_id: None,
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

fn write_chunk(chunks_dir: &PathBuf, chunk_id: u64, data: &[u8]) -> PathBuf {
    let p = paths::chunk_file(chunks_dir, chunk_id);
    let mut f = File::create(&p).unwrap();
    f.write_all(data).unwrap();
    p
}

fn write_index(chunks_dir: &PathBuf, chunk_id: u64, lines: &[IndexLine]) -> PathBuf {
    let p = paths::index_file(chunks_dir, chunk_id);
    let mut f = File::create(&p).unwrap();
    for l in lines {
        let mut buf = serde_json::to_vec(l).unwrap();
        buf.push(b'\n');
        f.write_all(&buf).unwrap();
    }
    p
}

fn idx_line(min_ms: i64, max_ms: i64, off: u64, len: u64) -> IndexLine {
    IndexLine {
        block_min_key: min_ms as u64,
        block_max_key: max_ms as u64,
        file_offset_bytes: off,
        block_len_bytes: len,
    }
}

#[test]
fn test_invalid_range_returns_error() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let (h, _part_dir, _chunks_dir) = setup_partition(&root);
    let err = h.read_range(10, 5).unwrap_err();
    match err { TvError::InvalidRange { .. } => {}, _ => panic!("wrong error: {err:?}") }
}

#[test]
fn test_missing_manifest_returns_error() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let (h, part_dir, _chunks_dir) = setup_partition(&root);
    let manifest = paths::partition_manifest(&part_dir);
    assert!(!manifest.exists());
    let err = h.read_range(0, 100).unwrap_err();
    match err { TvError::MissingFile { path } => assert_eq!(path, manifest), _ => panic!("wrong error: {err:?}") }
}

#[test]
fn test_missing_chunk_returns_error() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let (h, part_dir, chunks_dir) = setup_partition(&root);

    let manifest = paths::partition_manifest(&part_dir);
    // manifest references a chunk id but no file created
    let chunk_id: u64 = 100;
    let m = ManifestLine { chunk_id, min_order_key: 100, max_order_key: Some(200) };
    write_manifest_line(&manifest, &m);

    let err = h.read_range(0, 300).unwrap_err();
    match err { TvError::MissingFile { path } => {
        assert_eq!(path, paths::chunk_file(&chunks_dir, chunk_id));
    }, _ => panic!("wrong error: {err:?}") }
}

#[test]
fn test_missing_index_returns_error_for_partial_read() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let (h, part_dir, chunks_dir) = setup_partition(&root);

    let manifest = paths::partition_manifest(&part_dir);
    let chunk_id: u64 = 100;
    // Rolled chunk [100, 300]
    write_manifest_line(&manifest, &ManifestLine { chunk_id, min_order_key: 100, max_order_key: Some(300) });

    // Create chunk data
    write_chunk(&chunks_dir, chunk_id, b"ABCDEFGH");

    // Request [150, 250] which is a strict subset -> not fully covered, should use index and fail
    let err = h.read_range(150, 250).unwrap_err();
    match err { TvError::MissingFile { path } => {
        assert_eq!(path, paths::index_file(&chunks_dir, chunk_id));
    }, _ => panic!("wrong error: {err:?}") }
}

#[test]
fn test_whole_chunk_fast_path_reads_entire_file() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let (h, part_dir, chunks_dir) = setup_partition(&root);

    let manifest = paths::partition_manifest(&part_dir);
    let chunk_id: u64 = 100;
    // Rolled chunk fully inside range [100, 300]
    write_manifest_line(&manifest, &ManifestLine { chunk_id, min_order_key: 100, max_order_key: Some(300) });

    // Chunk content
    let data = b"0123456789";
    write_chunk(&chunks_dir, chunk_id, data);

    // Request covering full chunk
    let out = h.read_range(50, 400).unwrap();
    assert_eq!(out, data);
}

#[test]
fn test_indexed_selection_reads_expected_ranges() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let (h, part_dir, chunks_dir) = setup_partition(&root);

    let manifest = paths::partition_manifest(&part_dir);
    let chunk_id: u64 = 100;
    write_manifest_line(&manifest, &ManifestLine { chunk_id, min_order_key: 100, max_order_key: Some(500) });

    // Build a chunk with 3 blocks: A(0..3), B(3..6), C(6..9)
    // Use bytes to simulate blocks at offsets with lengths
    // We'll map timestamps per index lines: 
    //  Block1: [100, 199] at offset 0 len 3 => b"AAA"
    //  Block2: [200, 299] at offset 3 len 3 => b"BBB"
    //  Block3: [300, 399] at offset 6 len 3 => b"CCC"
    let chunk_data = b"AAABBBCCC".to_vec();
    write_chunk(&chunks_dir, chunk_id, &chunk_data);

    let idx = vec![
        idx_line(100, 199, 0, 3),
        idx_line(200, 299, 3, 3),
        idx_line(300, 399, 6, 3),
    ];
    write_index(&chunks_dir, chunk_id, &idx);

    // Request [210, 320]: should pick Block2 (start at first block with max>=210) and then Block3 until block_min>320
    let out = h.read_range(210, 320).unwrap();
    assert_eq!(out, b"BBBCCC");

    // Request [50, 120]: picks from first block with max>=50 -> Block1 only
    let out2 = h.read_range(50, 120).unwrap();
    assert_eq!(out2, b"AAA");

    // Request [250, 260]: picks Block2 only
    let out3 = h.read_range(250, 260).unwrap();
    assert_eq!(out3, b"BBB");
}

#[test]
fn test_range_outside_returns_empty() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let (h, part_dir, chunks_dir) = setup_partition(&root);

    let manifest = paths::partition_manifest(&part_dir);
    let chunk_id: u64 = 100;
    write_manifest_line(&manifest, &ManifestLine { chunk_id, min_order_key: 100, max_order_key: Some(200) });
    write_chunk(&chunks_dir, chunk_id, b"XYZ");

    let out1 = h.read_range(0, 50).unwrap();
    assert!(out1.is_empty());
    let out2 = h.read_range(300, 400).unwrap();
    assert!(out2.is_empty());
}

#[test]
fn test_cross_chunk_reads_concatenate() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let (h, part_dir, chunks_dir) = setup_partition(&root);

    let manifest = paths::partition_manifest(&part_dir);
    let c1: u64 = 0;
    let c2: u64 = 50;
    write_manifest_line(&manifest, &ManifestLine { chunk_id: c1, min_order_key: 0, max_order_key: Some(49) });
    write_manifest_line(&manifest, &ManifestLine { chunk_id: c2, min_order_key: 50, max_order_key: Some(99) });
    write_chunk(&chunks_dir, c1, b"AAAA");
    write_chunk(&chunks_dir, c2, b"BBBB");

    let out = h.read_range(0, 200).unwrap();
    assert_eq!(out, b"AAAABBBB");
}
