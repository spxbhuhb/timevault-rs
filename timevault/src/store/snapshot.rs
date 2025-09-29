use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::errors::{Result, TvError};
use crate::store::Store;
use crate::store::disk::index::{self, IndexLine};
use crate::store::disk::manifest::ManifestLine;
use crate::store::disk::metadata::MetadataJson;
use crate::store::partition::misc;
use crate::store::partition::{PartitionHandle, PartitionRuntime};
use crate::store::transfer::{DataTransfer, FileDownload, TransferRange};

const MAX_TRANSFER_ATTEMPTS: usize = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreSnapshot {
    pub partitions: Vec<PartitionSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSnapshot {
    pub partition_id: Uuid,
    pub partition_metadata: MetadataJson,
    pub last_chunk_id: Option<u64>,
    pub last_chunk_size: u64,
    pub last_order_key: Option<u64>,
}

pub fn install_store_snapshot<T: DataTransfer>(store: &Store, transfer: &T, snapshot: &StoreSnapshot) -> Result<()> {
    for partition_snapshot in &snapshot.partitions {
        install_partition_snapshot(store, transfer, partition_snapshot)?;
    }
    Ok(())
}

fn install_partition_snapshot<T: DataTransfer>(store: &Store, transfer: &T, snapshot: &PartitionSnapshot) -> Result<()> {
    if snapshot.partition_metadata.partition_id != snapshot.partition_id {
        return Err(TvError::InvalidSnapshot {
            reason: format!(
                "metadata partition id {} does not match snapshot id {}",
                snapshot.partition_metadata.partition_id, snapshot.partition_id
            ),
        });
    }

    let handle = store.ensure_partition(&snapshot.partition_metadata)?;
    let paths = misc::paths_for(&handle)?;
    let tmp_dir = crate::store::paths::tmp_dir(&paths.part_dir);
    fs::create_dir_all(&tmp_dir)?;

    let manifest_download = transfer.download_manifest(snapshot.partition_id)?;
    let manifest = rebase_manifest(&manifest_download.lines, snapshot)?;
    verify_manifest_sequence(&manifest, snapshot)?;
    misc::rewrite_manifest(&paths.manifest_path, &manifest)?;

    cleanup_extra_chunks(&paths, &manifest)?;
    sync_closed_chunks(transfer, snapshot.partition_id, &manifest, &paths, &tmp_dir)?;

    if let Some(last_chunk_id) = snapshot.last_chunk_id {
        let entry = manifest.last().ok_or_else(|| TvError::InvalidSnapshot {
            reason: format!("partition {} missing manifest entry for last chunk {}", snapshot.partition_id, last_chunk_id),
        })?;
        if entry.chunk_id != last_chunk_id {
            return Err(TvError::InvalidSnapshot {
                reason: format!("manifest last chunk {} does not match snapshot chunk {}", entry.chunk_id, last_chunk_id),
            });
        }
        let last_index_block = prepare_open_chunk(transfer, snapshot, entry, &paths, &tmp_dir)?;
        refresh_runtime_for_open_chunk(&handle, snapshot, entry, last_index_block.as_ref());
    } else {
        ensure_empty_partition(&handle);
    }

    Ok(())
}

fn rebase_manifest(lines: &[ManifestLine], snapshot: &PartitionSnapshot) -> Result<Vec<ManifestLine>> {
    if snapshot.last_chunk_id.is_none() {
        return Ok(Vec::new());
    }

    let target_chunk = snapshot.last_chunk_id.unwrap();
    let mut rebased = Vec::new();
    let mut found = false;
    for line in lines {
        rebased.push(line.clone());
        if line.chunk_id == target_chunk {
            found = true;
            break;
        }
    }

    if !found {
        return Err(TvError::InvalidSnapshot {
            reason: format!("manifest for partition {} missing chunk {}", snapshot.partition_id, target_chunk),
        });
    }

    if let Some(last) = rebased.last_mut() {
        last.max_order_key = None;
    }

    Ok(rebased)
}

fn verify_manifest_sequence(manifest: &[ManifestLine], snapshot: &PartitionSnapshot) -> Result<()> {
    if snapshot.last_chunk_id.is_none() {
        if !manifest.is_empty() {
            return Err(TvError::InvalidSnapshot {
                reason: format!("partition {} expected empty manifest but found entries", snapshot.partition_id),
            });
        }
        return Ok(());
    }

    if manifest.is_empty() {
        return Err(TvError::InvalidSnapshot {
            reason: format!("partition {} missing manifest entries for non-empty snapshot", snapshot.partition_id),
        });
    }

    let mut prev_chunk_id: Option<u64> = None;
    let mut prev_max_key: Option<u64> = None;
    for (idx, entry) in manifest.iter().enumerate() {
        if let Some(prev) = prev_chunk_id {
            if entry.chunk_id <= prev {
                return Err(TvError::InvalidSnapshot {
                    reason: format!("partition {} manifest chunk ids not strictly increasing", snapshot.partition_id),
                });
            }
        }
        if let Some(prev_max) = prev_max_key {
            if entry.min_order_key <= prev_max {
                return Err(TvError::InvalidSnapshot {
                    reason: format!("partition {} manifest coverage overlaps", snapshot.partition_id),
                });
            }
        }
        prev_chunk_id = Some(entry.chunk_id);
        let end_key = entry
            .max_order_key
            .or_else(|| if idx == manifest.len() - 1 { snapshot.last_order_key } else { None })
            .unwrap_or(entry.min_order_key);
        prev_max_key = Some(end_key);
    }

    if manifest.last().map(|m| m.chunk_id) != snapshot.last_chunk_id {
        return Err(TvError::InvalidSnapshot {
            reason: format!("partition {} last manifest chunk does not match snapshot", snapshot.partition_id),
        });
    }

    Ok(())
}

fn cleanup_extra_chunks(paths: &misc::Paths, manifest: &[ManifestLine]) -> Result<()> {
    let mut keep: HashSet<u64> = HashSet::new();
    for entry in manifest {
        keep.insert(entry.chunk_id);
    }
    let mut to_remove = Vec::new();
    for entry in fs::read_dir(&paths.chunks_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        if let Some(ext) = entry.path().extension().and_then(|s| s.to_str()) {
            if ext != "chunk" {
                continue;
            }
        } else {
            continue;
        }
        if let Some(id) = parse_chunk_id(&entry.path()) {
            if !keep.contains(&id) {
                to_remove.push(id);
            }
        }
    }
    if !to_remove.is_empty() {
        misc::delete_chunk_files(&paths.chunks_dir, &to_remove);
    }
    Ok(())
}

fn sync_closed_chunks<T: DataTransfer>(transfer: &T, partition_id: Uuid, manifest: &[ManifestLine], paths: &misc::Paths, tmp_dir: &Path) -> Result<()> {
    let closed_count = manifest.len().saturating_sub(1);
    for entry in manifest.iter().take(closed_count) {
        sync_closed_chunk(transfer, partition_id, entry, paths, tmp_dir)?;
    }
    Ok(())
}

fn sync_closed_chunk<T: DataTransfer>(transfer: &T, partition_id: Uuid, entry: &ManifestLine, paths: &misc::Paths, tmp_dir: &Path) -> Result<()> {
    let chunk_path = crate::store::paths::chunk_file(&paths.chunks_dir, entry.chunk_id);
    let tmp_chunk_path = temp_chunk_path(tmp_dir, entry.chunk_id);
    let chunk_outcome = download_file_resumable(&tmp_chunk_path, &chunk_path, None, |range| transfer.download_chunk(partition_id, entry.chunk_id, range))?;

    let local_chunk_len = fs::metadata(&chunk_path)?.len();
    if local_chunk_len != chunk_outcome.remote_len {
        return Err(TvError::InvalidSnapshot {
            reason: format!(
                "partition {} chunk {} length mismatch: local {} remote {}",
                partition_id, entry.chunk_id, local_chunk_len, chunk_outcome.remote_len
            ),
        });
    }

    let index_path = crate::store::paths::index_file(&paths.chunks_dir, entry.chunk_id);
    let tmp_index_path = temp_index_path(tmp_dir, entry.chunk_id);
    let index_outcome = download_file_resumable(&tmp_index_path, &index_path, None, |range| transfer.download_index(partition_id, entry.chunk_id, range))?;

    verify_index_file(&index_path, chunk_outcome.remote_len, Some(index_outcome.remote_len))?;
    Ok(())
}

fn prepare_open_chunk<T: DataTransfer>(transfer: &T, snapshot: &PartitionSnapshot, entry: &ManifestLine, paths: &misc::Paths, tmp_dir: &Path) -> Result<Option<IndexLine>> {
    let target_len = snapshot.last_chunk_size;
    let chunk_path = crate::store::paths::chunk_file(&paths.chunks_dir, entry.chunk_id);
    let tmp_chunk_path = temp_chunk_path(tmp_dir, entry.chunk_id);
    let chunk_outcome = download_file_resumable(&tmp_chunk_path, &chunk_path, Some(target_len), |range| {
        transfer.download_chunk(snapshot.partition_id, entry.chunk_id, range)
    })?;

    if chunk_outcome.remote_len < target_len {
        return Err(TvError::InvalidSnapshot {
            reason: format!(
                "partition {} chunk {} remote length {} smaller than target {}",
                snapshot.partition_id, entry.chunk_id, chunk_outcome.remote_len, target_len
            ),
        });
    }

    let index_path = crate::store::paths::index_file(&paths.chunks_dir, entry.chunk_id);
    let tmp_index_path = temp_index_path(tmp_dir, entry.chunk_id);
    download_file_resumable(&tmp_index_path, &index_path, None, |range| transfer.download_index(snapshot.partition_id, entry.chunk_id, range))?;

    let last_index_block = trim_open_index(&index_path, target_len)?;

    let local_len = fs::metadata(&chunk_path)?.len();
    if local_len != target_len {
        return Err(TvError::InvalidSnapshot {
            reason: format!("partition {} chunk {} length {} does not match target {}", snapshot.partition_id, entry.chunk_id, local_len, target_len),
        });
    }

    Ok(last_index_block)
}

fn refresh_runtime_for_open_chunk(handle: &PartitionHandle, snapshot: &PartitionSnapshot, entry: &ManifestLine, last_index: Option<&IndexLine>) {
    let min_key = entry.min_order_key;
    let max_key = snapshot.last_order_key.unwrap_or(min_key);
    let chunk_id = entry.chunk_id;
    let chunk_size = snapshot.last_chunk_size;

    handle.update_runtime(|rt| {
        let root = rt.cur_partition_root.clone();
        let id = rt.cur_partition_id;
        *rt = PartitionRuntime {
            cur_partition_root: root,
            cur_partition_id: id,
            ..PartitionRuntime::default()
        };
        rt.cur_chunk_id = Some(chunk_id);
        rt.cur_chunk_min_order_key = min_key;
        rt.cur_chunk_max_order_key = max_key;
        rt.cur_chunk_size_bytes = chunk_size;
        if let Some(ix) = last_index {
            rt.cur_index_block_min_order_key = ix.block_min_key;
            rt.cur_index_block_max_order_key = ix.block_max_key;
            rt.cur_index_block_size_bytes = ix.block_len_bytes;
            rt.cur_index_block_start_off = ix.file_offset_bytes;
            rt.cur_index_block_len_bytes = ix.block_len_bytes;
        } else {
            rt.cur_index_block_min_order_key = 0;
            rt.cur_index_block_max_order_key = 0;
            rt.cur_index_block_size_bytes = 0;
            rt.cur_index_block_start_off = chunk_size;
            rt.cur_index_block_len_bytes = 0;
        }
        rt.cur_index_block_record_count = 0;
        rt.cur_last_record_bytes = None;
    });
}

fn ensure_empty_partition(handle: &PartitionHandle) {
    handle.update_runtime(|rt| {
        let root = rt.cur_partition_root.clone();
        let id = rt.cur_partition_id;
        *rt = PartitionRuntime {
            cur_partition_root: root,
            cur_partition_id: id,
            ..PartitionRuntime::default()
        };
    });
}

fn trim_open_index(index_path: &Path, target_len: u64) -> Result<Option<IndexLine>> {
    let file = File::open(index_path)?;
    let mut lines = index::load_index_lines(&file)?;
    drop(file);
    lines.retain(|line| line.file_offset_bytes + line.block_len_bytes <= target_len);
    if let Some(last) = lines.last() {
        if last.file_offset_bytes + last.block_len_bytes > target_len {
            return Err(TvError::InvalidSnapshot {
                reason: format!("index coverage exceeds chunk length for {}", index_path.display()),
            });
        }
    }
    validate_index_lines(&lines, target_len)?;
    index::rewrite_index_atomic(index_path, &lines)?;
    Ok(lines.last().cloned())
}

fn verify_index_file(path: &Path, chunk_len: u64, expected_len: Option<u64>) -> Result<()> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    if let Some(expected) = expected_len {
        if metadata.len() != expected {
            return Err(TvError::InvalidSnapshot {
                reason: format!("index length mismatch for {}: local {} remote {}", path.display(), metadata.len(), expected),
            });
        }
    }
    let lines = index::load_index_lines(&file)?;
    drop(file);
    validate_index_lines(&lines, chunk_len)
}

fn validate_index_lines(lines: &[IndexLine], chunk_len: u64) -> Result<()> {
    let mut prev_key = None;
    let mut prev_offset = None;
    for line in lines {
        if line.block_max_key < line.block_min_key {
            return Err(TvError::InvalidSnapshot {
                reason: "index block keys out of order".into(),
            });
        }
        let end = line.file_offset_bytes.checked_add(line.block_len_bytes).ok_or_else(|| TvError::InvalidSnapshot {
            reason: "index block length overflow".into(),
        })?;
        if end > chunk_len {
            return Err(TvError::InvalidSnapshot {
                reason: "index block exceeds chunk length".into(),
            });
        }
        if let Some(prev) = prev_key {
            if line.block_min_key <= prev {
                return Err(TvError::InvalidSnapshot {
                    reason: "index block keys overlap".into(),
                });
            }
        }
        if let Some(prev_off) = prev_offset {
            if line.file_offset_bytes < prev_off {
                return Err(TvError::InvalidSnapshot {
                    reason: "index offsets not increasing".into(),
                });
            }
        }
        prev_key = Some(line.block_max_key);
        prev_offset = Some(line.file_offset_bytes + line.block_len_bytes);
    }
    Ok(())
}

struct DownloadOutcome {
    remote_len: u64,
}

fn download_file_resumable<F>(tmp_path: &Path, final_path: &Path, target_len: Option<u64>, mut fetch: F) -> Result<DownloadOutcome>
where
    F: FnMut(TransferRange) -> Result<FileDownload>,
{
    if let Some(parent) = tmp_path.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut attempt = 0;
    loop {
        attempt += 1;
        match download_attempt(tmp_path, target_len, &mut fetch) {
            Ok(outcome) => {
                finalize_download(tmp_path, final_path)?;
                return Ok(outcome);
            }
            Err(e) => {
                if attempt >= MAX_TRANSFER_ATTEMPTS {
                    return Err(e);
                }
                thread::sleep(Duration::from_millis(100 * attempt as u64));
            }
        }
    }
}

fn download_attempt<F>(tmp_path: &Path, target_len: Option<u64>, fetch: &mut F) -> Result<DownloadOutcome>
where
    F: FnMut(TransferRange) -> Result<FileDownload>,
{
    let mut file = OpenOptions::new().create(true).read(true).write(true).open(tmp_path)?;
    let mut current_len = file.metadata()?.len();
    if let Some(target) = target_len {
        if current_len > target {
            file.set_len(target)?;
            current_len = target;
        }
    }

    let mut remote_len: Option<u64> = None;
    loop {
        if let Some(target) = target_len {
            if current_len >= target {
                break;
            }
        } else if let Some(expected) = remote_len {
            if current_len >= expected {
                break;
            }
        }

        let range = TransferRange { start: current_len, end: target_len };
        let response = fetch(range)?;
        remote_len = Some(response.remote_len);
        if response.requested_range.start != current_len || response.requested_range.end != range.end {
            return Err(TvError::InvalidSnapshot {
                reason: "non-sequential transfer response".into(),
            });
        }
        if response.remote_len < current_len {
            return Err(TvError::InvalidSnapshot {
                reason: "remote length smaller than written data".into(),
            });
        }
        if let Some(target) = target_len {
            if response.remote_len < target {
                return Err(TvError::InvalidSnapshot {
                    reason: "remote length smaller than target".into(),
                });
            }
        }
        if response.bytes.is_empty() {
            break;
        }
        file.seek(SeekFrom::Start(current_len))?;
        file.write_all(&response.bytes)?;
        current_len += response.bytes.len() as u64;
    }

    let remote_len = remote_len.unwrap_or(current_len);
    if let Some(target) = target_len {
        if current_len != target {
            return Err(TvError::InvalidSnapshot { reason: "incomplete transfer".into() });
        }
    } else if current_len != remote_len {
        return Err(TvError::InvalidSnapshot { reason: "incomplete transfer".into() });
    }

    file.sync_all()?;
    Ok(DownloadOutcome { remote_len })
}

fn finalize_download(tmp_path: &Path, final_path: &Path) -> Result<()> {
    if final_path.exists() {
        fs::remove_file(final_path)?;
    }
    fs::rename(tmp_path, final_path)?;
    if let Some(dir) = final_path.parent() {
        let _ = crate::store::fsync::fsync_dir(dir);
    }
    Ok(())
}

fn temp_chunk_path(tmp_dir: &Path, chunk_id: u64) -> PathBuf {
    tmp_dir.join(format!("{:016x}.chunk.download", chunk_id))
}

fn temp_index_path(tmp_dir: &Path, chunk_id: u64) -> PathBuf {
    tmp_dir.join(format!("{:016x}.index.download", chunk_id))
}

fn parse_chunk_id(path: &Path) -> Option<u64> {
    let stem = path.file_stem()?.to_str()?;
    u64::from_str_radix(stem, 16).ok()
}
