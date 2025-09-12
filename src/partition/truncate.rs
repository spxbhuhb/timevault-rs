use std::fs::{self, File, OpenOptions};

use uuid::Uuid;

use crate::disk::index::{IndexLine, load_index_lines};
use crate::disk::manifest::{ManifestLine, load_manifest};
use crate::disk::metadata::load_metadata;
use crate::errors::{Result, TvError};
use crate::partition::PartitionHandle;
use crate::plugins::FormatPlugin;
use crate::store::paths;

pub fn truncate(h: &PartitionHandle, cutoff_key: u64) -> Result<()> {
    if h.inner.read_only { return Err(TvError::ReadOnly); }
    let part_dir = paths::partition_dir(h.root(), h.id());
    let chunks_dir = paths::chunks_dir(&part_dir);
    let manifest_path = paths::partition_manifest(&part_dir);
    if !manifest_path.exists() { return Err(TvError::MissingFile { path: manifest_path }); }

    let manifest = load_manifest(&manifest_path)?;
    if manifest.is_empty() { return Ok(()); }

    // Load metadata and plugin for potential scanning
    let meta = load_metadata(&paths::partition_metadata(&part_dir))?;
    let plugin = crate::plugins::resolve_plugin(&meta.format_plugin)?;

    // Determine chunks to keep/delete and potential overlapping chunk
    let mut new_manifest: Vec<ManifestLine> = Vec::new();
    let mut to_delete: Vec<Uuid> = Vec::new();

    // Identify overlapping candidate index in original order (last one before deletion)
    let mut overlap_idx: Option<usize> = None;
    for (i, m) in manifest.iter().enumerate() {
        if m.min_order_key >= cutoff_key {
            to_delete.push(m.chunk_id);
            continue;
        }
        if let Some(max) = m.max_order_key {
            if max < cutoff_key {
                new_manifest.push(m.clone());
            } else {
                // overlapping closed chunk
                overlap_idx = Some(i);
            }
        } else {
            // open chunk and min < cutoff => overlapping open chunk
            overlap_idx = Some(i);
        }
    }

    // Process overlap chunk if any
    if let Some(i) = overlap_idx {
        let m = &manifest[i];
        // Perform truncation of chunk/index at cutoff
        let (kept_any, new_max_opt) = truncate_chunk_and_index(&chunks_dir, m.chunk_id, cutoff_key, m.max_order_key, &*plugin)?;
        if kept_any {
            // Update manifest line
            let mut upd = m.clone();
            if upd.max_order_key.is_some() {
                // closed chunk becomes closed with new max (last kept key)
                upd.max_order_key = new_max_opt;
            } else {
                // keep it open (no max), content now ends before cutoff
                upd.max_order_key = None;
            }
            new_manifest.push(upd);
        } else {
            // Entire chunk removed
            to_delete.push(m.chunk_id);
        }
    }

    // Rewrite manifest atomically
    crate::disk::manifest::rewrite_manifest_atomic(&manifest_path, &new_manifest)?;

    // Delete all chunks that are beyond cutoff (min >= cutoff) and any overlap removed chunk
    for id in to_delete.iter() {
        let cp = paths::chunk_file(&chunks_dir, *id);
        let ip = paths::index_file(&chunks_dir, *id);
        let _ = fs::remove_file(cp);
        let _ = fs::remove_file(ip);
    }
    // Best-effort fsync the chunks directory after deletions to persist unlinks
    let _ = crate::store::fsync::fsync_dir(&chunks_dir);
    
    // Refresh runtime by re-running recovery
    let rt = crate::partition::recovery::load_partition_runtime_data(h.root(), h.id(), &meta)?;
    *h.inner.runtime.write() = {
        let mut r = rt.clone();
        r.cur_partition_root = h.inner.root.clone();
        r.cur_partition_id = h.inner.id;
        rt
    };

    Ok(())
}

fn truncate_chunk_and_index(
    chunks_dir: &std::path::Path,
    chunk_id: Uuid,
    cutoff_key: u64,
    was_closed_max: Option<u64>,
    plugin: &dyn FormatPlugin,
) -> Result<(bool, Option<u64>)> {
    let chunk_path = paths::chunk_file(chunks_dir, chunk_id);
    if !chunk_path.exists() { return Err(TvError::MissingFile { path: chunk_path }); }
    let index_path = paths::index_file(chunks_dir, chunk_id);

    // Load existing index lines if present
    let mut kept_index: Vec<IndexLine> = Vec::new();
    let mut base_end_off: u64 = 0;
    let mut new_max_key: Option<u64> = None;
    if index_path.exists() {
        let f = File::open(&index_path)?;
        let idx = load_index_lines(&f)?;
        for b in idx {
            if b.block_max_key < cutoff_key {
                base_end_off = b.file_offset_bytes + b.block_len_bytes;
                new_max_key = Some(b.block_max_key);
                kept_index.push(b);
            } else {
                // drop overlapping and subsequent blocks
                break;
            }
        }
    }

    // Determine new length
    let mut new_len = base_end_off;
    if was_closed_max.is_none() {
        // open chunk: may have unindexed tail; scan from base_end_off to find first rec >= cutoff
        let mut f = File::open(&chunk_path)?;
        // Use plugin scanner
        let mut scanner = plugin.scanner(&mut f)?;
        scanner.seek_to(base_end_off)?;
        let mut first_kept_key: Option<u64> = None;
        let mut last_kept_end: u64 = base_end_off;
        let mut last_kept_key: Option<u64> = new_max_key; // continue max from index if any
        while let Some(m) = scanner.next()? {
            if (m.ts_ms as u64) >= cutoff_key { break; }
            if first_kept_key.is_none() { first_kept_key = Some(m.ts_ms as u64); }
            last_kept_end = m.offset + m.len as u64;
            last_kept_key = Some(m.ts_ms as u64);
        }
        // If we kept some tail bytes past base_end_off, create an index entry for them
        if last_kept_end > base_end_off {
            kept_index.push(IndexLine {
                block_min_key: first_kept_key.unwrap_or(new_max_key.unwrap_or(0)),
                block_max_key: last_kept_key.unwrap_or(new_max_key.unwrap_or(0)),
                file_offset_bytes: base_end_off,
                block_len_bytes: last_kept_end - base_end_off,
            });
        }
        new_len = last_kept_end;
        new_max_key = last_kept_key;
    } else {
        // closed chunk: cut at block boundary determined by index
        // nothing else to do; new_len already set
    }

    // If no bytes remain, delete files by caller
    if new_len == 0 {
        // Truncate files to zero for safety; caller may remove them
        let _ = truncate_file(&chunk_path, 0);
        let _ = crate::disk::index::rewrite_index_atomic(&index_path, &[]);
        return Ok((false, None));
    }

    // Truncate chunk file
    truncate_file(&chunk_path, new_len)?;

    // Rewrite index file to only kept blocks (no partial adjustment). For open chunk, kept blocks already end <= new_len.
    // For closed chunk, we ensured end is exactly last kept block end.
    if index_path.exists() {
        crate::disk::index::rewrite_index_atomic(&index_path, &kept_index)?;
    }

    Ok((true, new_max_key))
}

fn truncate_file(path: &std::path::Path, new_len: u64) -> Result<()> {
    let f = OpenOptions::new().write(true).open(path)?;
    f.set_len(new_len)?;
    let _ = f.sync_all();
    Ok(())
}
