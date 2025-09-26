use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};


use crate::store::disk::index::{IndexLine, load_index_lines};
use crate::store::disk::manifest::{ManifestLine, load_manifest};
use crate::errors::{Result, TvError};
use crate::store::partition::PartitionHandle;
use crate::store::partition::misc;
use crate::store::plugins::FormatPlugin;
use crate::store::paths;

pub fn purge(h: &PartitionHandle, cutoff_key: u64) -> Result<()> {
    misc::ensure_writable(h)?;
    let p = misc::paths_for(h)?;

    // Load meta+plugin and persist last_purge_id durably before doing anything else
    let (mut meta, plugin) = misc::load_meta_and_plugin(&p.part_dir)?;
    meta.last_purge_id = Some(cutoff_key);
    let meta_path = paths::partition_metadata(&p.part_dir);
    crate::store::disk::atomic::atomic_write_json(&meta_path, &meta)?;

    // If configured as logical purge, return immediately after persisting last_purge_id
    if meta.logical_purge {
        return Ok(());
    }

    // Physical purge path
    purge_partition_dir(&p.part_dir, cutoff_key, &*plugin)?;

    // Refresh runtime at the end
    misc::refresh_runtime(h, &meta)?;

    Ok(())
}

pub fn purge_partition_dir(part_dir: &std::path::Path, cutoff_key: u64, plugin: &dyn FormatPlugin) -> Result<()> {
    let manifest_path = paths::partition_manifest(part_dir);
    let chunks_dir = paths::chunks_dir(part_dir);
    let manifest = load_manifest(&manifest_path)?;
    if manifest.is_empty() {
        return Ok(());
    }

    // Build new manifest and deletion list
    let mut new_manifest: Vec<ManifestLine> = Vec::new();
    let mut to_delete: Vec<u64> = Vec::new();

    // We'll track the first overlapping chunk (the first chunk that has any data > cutoff)
    let mut overlap_idx: Option<usize> = None;

    for (i, m) in manifest.iter().enumerate() {
        match m.max_order_key {
            Some(max) => {
                if max <= cutoff_key {
                    to_delete.push(m.chunk_id);
                } else if m.min_order_key > cutoff_key {
                    if overlap_idx.is_none() {}
                    new_manifest.push(m.clone());
                } else {
                    overlap_idx = Some(i);
                }
            }
            None => {
                if m.min_order_key > cutoff_key {
                    new_manifest.push(m.clone());
                } else {
                    overlap_idx = Some(i);
                }
            }
        }
    }

    // Process overlap if any
    if let Some(i) = overlap_idx {
        let m = &manifest[i];
        let (kept_any, new_min_key, was_closed_max) = purge_chunk_from_start(&chunks_dir, m.chunk_id, cutoff_key, m.max_order_key, plugin)?;
        if kept_any {
            let mut upd = m.clone();
            upd.min_order_key = new_min_key.expect("new min key after purge");
            if was_closed_max.is_some() {
                upd.max_order_key = was_closed_max;
            } else {
                upd.max_order_key = None;
            }
            new_manifest.insert(0, upd);
        } else {
            to_delete.push(m.chunk_id);
        }
    }

    // Rewrite manifest and delete files
    crate::store::partition::misc::rewrite_manifest(&manifest_path, &new_manifest)?;
    crate::store::partition::misc::delete_chunk_files(&chunks_dir, &to_delete);

    Ok(())
}

// Purge records from the start of chunk up to and including cutoff_key.
// Returns: (kept_any, new_min_key, was_closed_max)
fn purge_chunk_from_start(chunks_dir: &std::path::Path, chunk_id: u64, cutoff_key: u64, was_closed_max: Option<u64>, plugin: &dyn FormatPlugin) -> Result<(bool, Option<u64>, Option<u64>)> {
    let chunk_path = paths::chunk_file(chunks_dir, chunk_id);
    if !chunk_path.exists() {
        return Err(TvError::MissingFile { path: chunk_path });
    }
    let index_path = paths::index_file(chunks_dir, chunk_id);

    // Load the existing index
    let mut idx_lines: Vec<IndexLine> = Vec::new();
    if index_path.exists() {
        let f = std::fs::File::open(&index_path)?;
        idx_lines = load_index_lines(&f)?;
    }

    // Determine scan_start as the end of the last block with block_max_key <= cutoff
    let mut scan_start: u64 = 0;
    for b in &idx_lines {
        if b.block_max_key <= cutoff_key {
            scan_start = b.file_offset_bytes + b.block_len_bytes;
        }
    }
    // First truly kept indexed block is the first with block_min_key > cutoff
    let first_kept_index_block: Option<IndexLine> = idx_lines.iter().find(|b| b.block_min_key > cutoff_key).cloned();

    // Scan to find the first record with key > cutoff
    let mut f = std::fs::File::open(&chunk_path)?;
    let mut scanner = plugin.scanner(&mut f)?;
    scanner.seek_to(scan_start)?;
    let mut first_kept_key: Option<u64> = None;
    let mut copy_start: Option<u64> = None;
    while let Some(m) = scanner.next()? {
        if (m.ts_ms as u64) > cutoff_key {
            first_kept_key = Some(m.ts_ms as u64);
            copy_start = Some(m.offset);
            break;
        }
    }

    // If nothing remains after purge
    let copy_start = match copy_start {
        Some(v) => v,
        None => {
            // Truncate index to empty and return kept_any=false
            if index_path.exists() {
                crate::store::disk::index::rewrite_index_atomic(&index_path, &[])?;
            }
            // Truncate file to 0
            let cf = OpenOptions::new().write(true).open(&chunk_path)?;
            cf.set_len(0)?;
            let _ = cf.sync_all();
            return Ok((false, None, was_closed_max));
        }
    };

    // Copy tail [copy_start..EOF] into tmp and atomically replace
    let new_len = rewrite_file_from_offset(&chunk_path, copy_start)?;

    // Rebuild index: keep only blocks with block_min_key > cutoff
    let mut new_index: Vec<IndexLine> = Vec::new();
    // Synthetic leading block if there is a gap before the first kept index block
    let leading_end_off: u64;
    if let Some(first_kept) = first_kept_index_block {
        // Old absolute offset of first kept block; new offset after rewrite will be (old - copy_start)
        let shifted_off = first_kept.file_offset_bytes.saturating_sub(copy_start);
        leading_end_off = shifted_off;
        // Adjust and push kept blocks strictly with block_min_key > cutoff
        for b in idx_lines.into_iter().filter(|b| b.block_min_key > cutoff_key) {
            new_index.push(IndexLine {
                block_min_key: b.block_min_key,
                block_max_key: b.block_max_key,
                file_offset_bytes: b.file_offset_bytes.saturating_sub(copy_start),
                block_len_bytes: b.block_len_bytes,
            });
        }
    } else {
        // No kept index blocks remain. Only synthesize a covering block when the chunk was closed;
        // open chunks purposely leave their trailing portion unindexed so cadence can rebuild it later.
        leading_end_off = if was_closed_max.is_some() { new_len } else { 0 };
    }

    // If there is a leading segment starting at 0 up to leading_end_off, synthesize block for it
    if leading_end_off > 0 {
        // We need accurate max key for the leading segment. Scan from 0 up to leading_end_off in the rewritten file.
        let mut f2 = std::fs::File::open(&chunk_path)?;
        let mut s2 = plugin.scanner(&mut f2)?;
        s2.seek_to(0)?;
        let mut last_key = first_kept_key.unwrap();
        while let Some(m) = s2.next()? {
            if m.offset >= leading_end_off {
                break;
            }
            last_key = m.ts_ms as u64;
            let end = m.offset + m.len as u64;
            if end >= leading_end_off {
                break;
            }
        }
        new_index.insert(
            0,
            IndexLine {
                block_min_key: first_kept_key.unwrap(),
                block_max_key: last_key,
                file_offset_bytes: 0,
                block_len_bytes: leading_end_off,
            },
        );
    }

    // Trailing coverage note:
    // - Closed chunks: pending index is flushed on roll, so after shifting kept blocks,
    //   index coverage already reaches EOF; no synthetic trailing block is needed.
    // - Open chunks: the tail may be legitimately unindexed due to index cadence; do not
    //   synthesize an index entry for that portion. It will be indexed later by cadence or roll.

    // Rewrite index file atomically
    crate::store::disk::index::rewrite_index_atomic(&index_path, &new_index)?;

    Ok((true, first_kept_key, was_closed_max))
}

fn rewrite_file_from_offset(path: &std::path::Path, start_off: u64) -> Result<u64> {
    // Create tmp file, copy tail, then rename
    let tmp = path.with_extension("chunk.tmp");
    let mut src = std::fs::File::open(path)?;
    let mut dst = OpenOptions::new().create(true).truncate(true).write(true).open(&tmp)?;
    src.seek(SeekFrom::Start(start_off))?;
    let mut buf = vec![0u8; 128 * 1024];
    let mut total: u64 = 0;
    loop {
        let n = src.read(&mut buf)?;
        if n == 0 {
            break;
        }
        dst.write_all(&buf[..n])?;
        total += n as u64;
    }
    dst.flush()?;
    let _ = dst.sync_all();
    std::fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        let _ = crate::store::fsync::fsync_dir(dir);
    }
    // Ensure new file size is set (rename preserves size)
    Ok(total)
}
