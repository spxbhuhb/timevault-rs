use std::collections::HashMap;
use std::fs;
use tempfile::TempDir;
use uuid::Uuid;

use timevault::errors::{Result as TvResult, TvError};
use timevault::store::disk::index::IndexLine;
use timevault::store::disk::manifest::ManifestLine;
use timevault::store::disk::metadata::MetadataJson;
use timevault::store::paths;
use timevault::store::snapshot::{PartitionSnapshot, StoreSnapshot, install_store_snapshot};
use timevault::store::transfer::{DataTransfer, FileDownload, ManifestDownload, TransferRange};
use timevault::store::{Store, StoreConfig};

#[derive(Default, Clone)]
struct MockTransfer {
    manifests: HashMap<Uuid, Vec<ManifestLine>>,
    chunks: HashMap<(Uuid, u64), Vec<u8>>,
    indexes: HashMap<(Uuid, u64), Vec<u8>>,
}

impl MockTransfer {
    fn with_manifest(mut self, partition: Uuid, lines: Vec<ManifestLine>) -> Self {
        self.manifests.insert(partition, lines);
        self
    }

    fn with_chunk(mut self, partition: Uuid, chunk_id: u64, data: Vec<u8>) -> Self {
        self.chunks.insert((partition, chunk_id), data);
        self
    }

    fn with_index(mut self, partition: Uuid, chunk_id: u64, data: Vec<u8>) -> Self {
        self.indexes.insert((partition, chunk_id), data);
        self
    }
}

impl DataTransfer for MockTransfer {
    fn download_manifest(&self, partition: Uuid) -> TvResult<ManifestDownload> {
        let lines = self.manifests.get(&partition).cloned().unwrap_or_else(Vec::new);
        Ok(ManifestDownload {
            partition_id: partition,
            lines,
            version: None,
        })
    }

    fn download_chunk(&self, partition: Uuid, chunk_id: u64, range: TransferRange) -> TvResult<FileDownload> {
        let data = self
            .chunks
            .get(&(partition, chunk_id))
            .cloned()
            .unwrap_or_else(|| panic!("missing chunk data for {}:{}", partition, chunk_id));
        let remote_len = data.len() as u64;
        let start = range.start as usize;
        assert!(start <= data.len(), "requested start beyond remote length");
        let end = range.end.map(|e| e.min(remote_len)).unwrap_or(remote_len) as usize;
        let bytes = data[start..end].to_vec();
        Ok(FileDownload {
            partition_id: partition,
            chunk_id,
            requested_range: range,
            bytes,
            remote_len,
            version: None,
        })
    }

    fn download_index(&self, partition: Uuid, chunk_id: u64, range: TransferRange) -> TvResult<FileDownload> {
        let data = self
            .indexes
            .get(&(partition, chunk_id))
            .cloned()
            .unwrap_or_else(|| panic!("missing index data for {}:{}", partition, chunk_id));
        let remote_len = data.len() as u64;
        let start = range.start as usize;
        assert!(start <= data.len(), "requested start beyond remote index length");
        let end = range.end.map(|e| e.min(remote_len)).unwrap_or(remote_len) as usize;
        let bytes = data[start..end].to_vec();
        Ok(FileDownload {
            partition_id: partition,
            chunk_id,
            requested_range: range,
            bytes,
            remote_len,
            version: None,
        })
    }
}

fn default_metadata(partition_id: Uuid) -> MetadataJson {
    MetadataJson {
        partition_id,
        format_version: 1,
        format_plugin: "jsonl".into(),
        chunk_roll: Default::default(),
        index: Default::default(),
        retention: Default::default(),
        key_is_timestamp: true,
        logical_purge: false,
        last_purge_id: None,
    }
}

fn serialize_index_lines(lines: &[IndexLine]) -> Vec<u8> {
    let mut out = Vec::new();
    for line in lines {
        let json = serde_json::to_vec(line).expect("serialize index line");
        out.extend_from_slice(&json);
        out.push(b'\n');
    }
    out
}

fn setup_store() -> (TempDir, Store) {
    let temp = TempDir::new().unwrap();
    let root = temp.path().to_path_buf();
    let store = Store::open(&root, StoreConfig::default()).unwrap();
    (temp, store)
}

#[test]
fn install_snapshot_handles_empty_partition() {
    let (_temp, store) = setup_store();

    let partition_id = Uuid::now_v7();
    let metadata = default_metadata(partition_id);

    // Seed partition and add stale files that should be removed.
    let handle = store.ensure_partition(&metadata).unwrap();
    drop(handle);
    let part_dir = paths::partition_dir(store.root_path(), partition_id);
    let chunks_dir = paths::chunks_dir(&part_dir);
    fs::create_dir_all(&chunks_dir).unwrap();
    let stale_chunk = paths::chunk_file(&chunks_dir, 9);
    fs::write(&stale_chunk, b"old-chunk").unwrap();
    let stale_index = paths::index_file(&chunks_dir, 9);
    fs::write(&stale_index, b"{}").unwrap();

    let transfer = MockTransfer::default().with_manifest(partition_id, vec![]);

    let snapshot = StoreSnapshot {
        partitions: vec![PartitionSnapshot {
            partition_id,
            partition_metadata: metadata,
            last_chunk_id: None,
            last_chunk_size: 0,
            last_order_key: None,
        }],
    };

    install_store_snapshot(&store, &transfer, &snapshot).unwrap();

    let manifest_path = paths::partition_manifest(&part_dir);
    let manifest_text = fs::read_to_string(&manifest_path).unwrap();
    assert!(manifest_text.trim().is_empty());

    assert!(!stale_chunk.exists());
    assert!(!stale_index.exists());

    store.open_partition(partition_id).unwrap();
}

#[test]
fn install_snapshot_handles_single_chunk_partition() {
    let (_temp, store) = setup_store();

    let partition_id = Uuid::now_v7();
    let metadata = default_metadata(partition_id);

    let chunk_id = 1;
    let manifest_lines = vec![ManifestLine {
        chunk_id,
        min_order_key: 10,
        max_order_key: None,
    }];
    let chunk_data = b"abcdef".to_vec();
    let target_len = chunk_data.len() as u64;
    let index_lines = vec![
        IndexLine {
            block_min_key: 10,
            block_max_key: 15,
            file_offset_bytes: 0,
            block_len_bytes: 3,
        },
        IndexLine {
            block_min_key: 16,
            block_max_key: 20,
            file_offset_bytes: 3,
            block_len_bytes: 3,
        },
    ];

    let transfer = MockTransfer::default()
        .with_manifest(partition_id, manifest_lines.clone())
        .with_chunk(partition_id, chunk_id, chunk_data.clone())
        .with_index(partition_id, chunk_id, serialize_index_lines(&index_lines));

    let snapshot = StoreSnapshot {
        partitions: vec![PartitionSnapshot {
            partition_id,
            partition_metadata: metadata,
            last_chunk_id: Some(chunk_id),
            last_chunk_size: target_len,
            last_order_key: Some(20),
        }],
    };

    install_store_snapshot(&store, &transfer, &snapshot).unwrap();

    let part_dir = paths::partition_dir(store.root_path(), partition_id);
    let manifest_path = paths::partition_manifest(&part_dir);
    let manifest_text = fs::read_to_string(&manifest_path).unwrap();
    let applied_manifest: Vec<ManifestLine> = manifest_text.lines().filter(|l| !l.is_empty()).map(|line| serde_json::from_str(line).unwrap()).collect();
    assert_eq!(applied_manifest.len(), 1);
    assert_eq!(applied_manifest[0].chunk_id, chunk_id);
    assert_eq!(applied_manifest[0].min_order_key, 10);
    assert!(applied_manifest[0].max_order_key.is_none());

    let chunks_dir = paths::chunks_dir(&part_dir);
    let chunk_path = paths::chunk_file(&chunks_dir, chunk_id);
    assert_eq!(fs::metadata(&chunk_path).unwrap().len(), target_len);

    let index_path = paths::index_file(&chunks_dir, chunk_id);
    let index_text = fs::read_to_string(&index_path).unwrap();
    let applied_index: Vec<IndexLine> = index_text.lines().filter(|l| !l.is_empty()).map(|line| serde_json::from_str(line).unwrap()).collect();
    assert_eq!(applied_index.len(), index_lines.len());
    for (applied, expected) in applied_index.iter().zip(index_lines.iter()) {
        assert_eq!(applied.block_min_key, expected.block_min_key);
        assert_eq!(applied.block_max_key, expected.block_max_key);
        assert_eq!(applied.file_offset_bytes, expected.file_offset_bytes);
        assert_eq!(applied.block_len_bytes, expected.block_len_bytes);
    }

    store.open_partition(partition_id).unwrap();
}

#[test]
fn install_snapshot_handles_open_chunk_without_last_order_key() {
    let (_temp, store) = setup_store();

    let partition_id = Uuid::now_v7();
    let metadata = default_metadata(partition_id);

    let chunk_id = 3;
    let manifest_lines = vec![ManifestLine {
        chunk_id,
        min_order_key: 50,
        max_order_key: None,
    }];
    let chunk_data = b"abcdefgh".to_vec();
    let target_len = 3u64;
    let index_lines = vec![
        IndexLine {
            block_min_key: 50,
            block_max_key: 55,
            file_offset_bytes: 0,
            block_len_bytes: 2,
        },
        IndexLine {
            block_min_key: 56,
            block_max_key: 60,
            file_offset_bytes: 2,
            block_len_bytes: 6,
        },
    ];

    let transfer = MockTransfer::default()
        .with_manifest(partition_id, manifest_lines.clone())
        .with_chunk(partition_id, chunk_id, chunk_data.clone())
        .with_index(partition_id, chunk_id, serialize_index_lines(&index_lines));

    let snapshot = StoreSnapshot {
        partitions: vec![PartitionSnapshot {
            partition_id,
            partition_metadata: metadata,
            last_chunk_id: Some(chunk_id),
            last_chunk_size: target_len,
            last_order_key: None,
        }],
    };

    install_store_snapshot(&store, &transfer, &snapshot).unwrap();

    let part_dir = paths::partition_dir(store.root_path(), partition_id);
    let manifest_path = paths::partition_manifest(&part_dir);
    let manifest_text = fs::read_to_string(&manifest_path).unwrap();
    let applied_manifest: Vec<ManifestLine> = manifest_text.lines().filter(|l| !l.is_empty()).map(|line| serde_json::from_str(line).unwrap()).collect();
    assert_eq!(applied_manifest.len(), 1);
    assert_eq!(applied_manifest[0].chunk_id, chunk_id);
    assert_eq!(applied_manifest[0].min_order_key, 50);
    assert!(applied_manifest[0].max_order_key.is_none());

    let chunks_dir = paths::chunks_dir(&part_dir);
    let chunk_path = paths::chunk_file(&chunks_dir, chunk_id);
    assert_eq!(fs::metadata(&chunk_path).unwrap().len(), target_len);

    let index_path = paths::index_file(&chunks_dir, chunk_id);
    let index_text = fs::read_to_string(&index_path).unwrap();
    let applied_index: Vec<IndexLine> = index_text.lines().filter(|l| !l.is_empty()).map(|line| serde_json::from_str(line).unwrap()).collect();
    assert_eq!(applied_index.len(), 1);
    assert_eq!(applied_index[0].block_min_key, index_lines[0].block_min_key);
    assert_eq!(applied_index[0].block_max_key, index_lines[0].block_max_key);
    assert_eq!(applied_index[0].file_offset_bytes, index_lines[0].file_offset_bytes);
    assert_eq!(applied_index[0].block_len_bytes, index_lines[0].block_len_bytes);

    store.open_partition(partition_id).unwrap();
}

#[test]
fn install_snapshot_syncs_chunks_and_cleans_up() {
    let (_temp, store) = setup_store();

    let partition_id = Uuid::now_v7();
    let metadata = default_metadata(partition_id);

    // Ensure partition exists and plant a stale chunk/index pair that should be removed.
    let handle = store.ensure_partition(&metadata).unwrap();
    drop(handle);
    let part_dir = paths::partition_dir(store.root_path(), partition_id);
    let chunks_dir = paths::chunks_dir(&part_dir);
    fs::create_dir_all(&chunks_dir).unwrap();
    let stale_chunk = paths::chunk_file(&chunks_dir, 3);
    fs::write(&stale_chunk, b"obsolete").unwrap();
    let stale_index = paths::index_file(&chunks_dir, 3);
    fs::write(&stale_index, b"{}").unwrap();

    let closed_chunk_id = 1;
    let open_chunk_id = 2;

    let manifest_lines = vec![
        ManifestLine {
            chunk_id: closed_chunk_id,
            min_order_key: 100,
            max_order_key: Some(150),
        },
        ManifestLine {
            chunk_id: open_chunk_id,
            min_order_key: 200,
            max_order_key: None,
        },
    ];

    let closed_chunk_data = b"closed".to_vec();
    let open_chunk_data = b"open-chunk".to_vec();
    let open_target_len = 6u64;

    let closed_index_lines = vec![IndexLine {
        block_min_key: 100,
        block_max_key: 150,
        file_offset_bytes: 0,
        block_len_bytes: closed_chunk_data.len() as u64,
    }];
    let open_index_lines = vec![
        IndexLine {
            block_min_key: 200,
            block_max_key: 220,
            file_offset_bytes: 0,
            block_len_bytes: 3,
        },
        IndexLine {
            block_min_key: 221,
            block_max_key: 230,
            file_offset_bytes: 6,
            block_len_bytes: 4,
        },
    ];

    let transfer = MockTransfer::default()
        .with_manifest(partition_id, manifest_lines.clone())
        .with_chunk(partition_id, closed_chunk_id, closed_chunk_data.clone())
        .with_chunk(partition_id, open_chunk_id, open_chunk_data.clone())
        .with_index(partition_id, closed_chunk_id, serialize_index_lines(&closed_index_lines))
        .with_index(partition_id, open_chunk_id, serialize_index_lines(&open_index_lines));

    let snapshot = StoreSnapshot {
        partitions: vec![PartitionSnapshot {
            partition_id,
            partition_metadata: metadata,
            last_chunk_id: Some(open_chunk_id),
            last_chunk_size: open_target_len,
            last_order_key: Some(230),
        }],
    };

    install_store_snapshot(&store, &transfer, &snapshot).unwrap();

    // Manifest should be rewritten with both lines and no stale entries.
    let manifest_path = paths::partition_manifest(&part_dir);
    let manifest_text = fs::read_to_string(&manifest_path).unwrap();
    let applied_manifest: Vec<ManifestLine> = manifest_text.lines().filter(|l| !l.is_empty()).map(|line| serde_json::from_str(line).unwrap()).collect();
    assert_eq!(applied_manifest.len(), manifest_lines.len());
    for (applied, expected) in applied_manifest.iter().zip(manifest_lines.iter()) {
        assert_eq!(applied.chunk_id, expected.chunk_id);
        assert_eq!(applied.min_order_key, expected.min_order_key);
        assert_eq!(applied.max_order_key, expected.max_order_key);
    }

    // Closed chunk is fully downloaded.
    let closed_path = paths::chunk_file(&chunks_dir, closed_chunk_id);
    assert_eq!(fs::metadata(&closed_path).unwrap().len(), closed_chunk_data.len() as u64);

    // Closed index preserved full remote length and content.
    let closed_index_path = paths::index_file(&chunks_dir, closed_chunk_id);
    assert_eq!(fs::metadata(&closed_index_path).unwrap().len(), serialize_index_lines(&closed_index_lines).len() as u64);

    // Open chunk trimmed to snapshot size.
    let open_path = paths::chunk_file(&chunks_dir, open_chunk_id);
    assert_eq!(fs::metadata(&open_path).unwrap().len(), open_target_len);

    // Open index trimmed to only the first block within bounds.
    let open_index_path = paths::index_file(&chunks_dir, open_chunk_id);
    let open_index_text = fs::read_to_string(&open_index_path).unwrap();
    let trimmed_lines: Vec<IndexLine> = open_index_text.lines().filter(|l| !l.is_empty()).map(|line| serde_json::from_str(line).unwrap()).collect();
    assert_eq!(trimmed_lines.len(), 1);
    let trimmed = &trimmed_lines[0];
    let expected = &open_index_lines[0];
    assert_eq!(trimmed.block_min_key, expected.block_min_key);
    assert_eq!(trimmed.block_max_key, expected.block_max_key);
    assert_eq!(trimmed.file_offset_bytes, expected.file_offset_bytes);
    assert_eq!(trimmed.block_len_bytes, expected.block_len_bytes);

    // Stale chunk and index should be deleted.
    assert!(!stale_chunk.exists());
    assert!(!stale_index.exists());
}

#[test]
fn install_snapshot_errors_when_manifest_missing_chunk() {
    let (_temp, store) = setup_store();

    let partition_id = Uuid::now_v7();
    let metadata = default_metadata(partition_id);
    store.ensure_partition(&metadata).unwrap();

    let manifest_lines = vec![ManifestLine {
        chunk_id: 1,
        min_order_key: 0,
        max_order_key: Some(10),
    }];
    let transfer = MockTransfer::default().with_manifest(partition_id, manifest_lines);

    let snapshot = StoreSnapshot {
        partitions: vec![PartitionSnapshot {
            partition_id,
            partition_metadata: metadata,
            last_chunk_id: Some(2),
            last_chunk_size: 0,
            last_order_key: Some(10),
        }],
    };

    let result = install_store_snapshot(&store, &transfer, &snapshot);
    assert!(matches!(result, Err(TvError::InvalidSnapshot { .. })));
}

#[test]
fn install_snapshot_errors_when_metadata_partition_mismatched() {
    let (_temp, store) = setup_store();

    let partition_id = Uuid::now_v7();
    let mut metadata = default_metadata(partition_id);
    metadata.partition_id = Uuid::nil();

    let snapshot = StoreSnapshot {
        partitions: vec![PartitionSnapshot {
            partition_id,
            partition_metadata: metadata,
            last_chunk_id: None,
            last_chunk_size: 0,
            last_order_key: None,
        }],
    };

    let transfer = MockTransfer::default();
    let result = install_store_snapshot(&store, &transfer, &snapshot);
    assert!(matches!(result, Err(TvError::InvalidSnapshot { .. })));
}

#[test]
fn install_snapshot_errors_when_manifest_order_keys_overlap() {
    let (_temp, store) = setup_store();

    let partition_id = Uuid::now_v7();
    let metadata = default_metadata(partition_id);
    store.ensure_partition(&metadata).unwrap();

    let manifest_lines = vec![
        ManifestLine {
            chunk_id: 1,
            min_order_key: 0,
            max_order_key: Some(50),
        },
        ManifestLine {
            chunk_id: 2,
            min_order_key: 40,
            max_order_key: Some(60),
        },
    ];

    let transfer = MockTransfer::default().with_manifest(partition_id, manifest_lines);

    let snapshot = StoreSnapshot {
        partitions: vec![PartitionSnapshot {
            partition_id,
            partition_metadata: metadata,
            last_chunk_id: Some(2),
            last_chunk_size: 0,
            last_order_key: Some(60),
        }],
    };

    let result = install_store_snapshot(&store, &transfer, &snapshot);
    assert!(matches!(result, Err(TvError::InvalidSnapshot { .. })));
}

#[test]
fn install_snapshot_errors_when_manifest_chunk_ids_not_increasing() {
    let (_temp, store) = setup_store();

    let partition_id = Uuid::now_v7();
    let metadata = default_metadata(partition_id);
    store.ensure_partition(&metadata).unwrap();

    let manifest_lines = vec![
        ManifestLine {
            chunk_id: 1,
            min_order_key: 0,
            max_order_key: Some(10),
        },
        ManifestLine {
            chunk_id: 3,
            min_order_key: 11,
            max_order_key: Some(20),
        },
        ManifestLine {
            chunk_id: 2,
            min_order_key: 21,
            max_order_key: Some(30),
        },
    ];

    let transfer = MockTransfer::default().with_manifest(partition_id, manifest_lines);

    let snapshot = StoreSnapshot {
        partitions: vec![PartitionSnapshot {
            partition_id,
            partition_metadata: metadata,
            last_chunk_id: Some(2),
            last_chunk_size: 0,
            last_order_key: Some(20),
        }],
    };

    let result = install_store_snapshot(&store, &transfer, &snapshot);
    assert!(matches!(result, Err(TvError::InvalidSnapshot { .. })));
}

#[test]
fn install_snapshot_errors_when_open_chunk_remote_too_small() {
    let (_temp, store) = setup_store();

    let partition_id = Uuid::now_v7();
    let metadata = default_metadata(partition_id);
    store.ensure_partition(&metadata).unwrap();

    let manifest_lines = vec![ManifestLine {
        chunk_id: 5,
        min_order_key: 100,
        max_order_key: None,
    }];

    let transfer = MockTransfer::default().with_manifest(partition_id, manifest_lines.clone()).with_chunk(partition_id, 5, b"123".to_vec());

    let snapshot = StoreSnapshot {
        partitions: vec![PartitionSnapshot {
            partition_id,
            partition_metadata: metadata,
            last_chunk_id: Some(5),
            last_chunk_size: 5,
            last_order_key: Some(100),
        }],
    };

    let result = install_store_snapshot(&store, &transfer, &snapshot);
    assert!(matches!(result, Err(TvError::InvalidSnapshot { .. })));
}

#[test]
fn install_snapshot_errors_when_closed_index_exceeds_chunk() {
    let (_temp, store) = setup_store();

    let partition_id = Uuid::now_v7();
    let metadata = default_metadata(partition_id);
    store.ensure_partition(&metadata).unwrap();

    let closed_chunk_id = 7;
    let open_chunk_id = 8;

    let manifest_lines = vec![
        ManifestLine {
            chunk_id: closed_chunk_id,
            min_order_key: 0,
            max_order_key: Some(10),
        },
        ManifestLine {
            chunk_id: open_chunk_id,
            min_order_key: 11,
            max_order_key: None,
        },
    ];

    let invalid_closed_index = serialize_index_lines(&[IndexLine {
        block_min_key: 0,
        block_max_key: 10,
        file_offset_bytes: 0,
        block_len_bytes: 8,
    }]);

    let valid_open_index = serialize_index_lines(&[IndexLine {
        block_min_key: 11,
        block_max_key: 11,
        file_offset_bytes: 0,
        block_len_bytes: 4,
    }]);

    let transfer = MockTransfer::default()
        .with_manifest(partition_id, manifest_lines)
        .with_chunk(partition_id, closed_chunk_id, b"data".to_vec())
        .with_index(partition_id, closed_chunk_id, invalid_closed_index)
        .with_chunk(partition_id, open_chunk_id, b"open-data".to_vec())
        .with_index(partition_id, open_chunk_id, valid_open_index);

    let snapshot = StoreSnapshot {
        partitions: vec![PartitionSnapshot {
            partition_id,
            partition_metadata: metadata,
            last_chunk_id: Some(open_chunk_id),
            last_chunk_size: 4,
            last_order_key: Some(11),
        }],
    };

    let result = install_store_snapshot(&store, &transfer, &snapshot);
    assert!(matches!(result, Err(TvError::InvalidSnapshot { .. })));
}
