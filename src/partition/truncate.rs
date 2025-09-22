use std::fs::File;

use uuid::Uuid;

use crate::disk::index::{load_index_lines, IndexLine};
use crate::disk::manifest::{load_manifest, ManifestLine};
use crate::errors::{Result, TvError};
use crate::partition::misc;
use crate::partition::PartitionHandle;
use crate::plugins::FormatPlugin;
use crate::store::paths;

pub fn truncate(h: &PartitionHandle, cutoff_key: u64) -> Result<()> {
    misc::ensure_writable(h)?;
    let p = misc::paths_for(h)?;

    let manifest = load_manifest(&p.manifest_path)?;
    if manifest.is_empty() { return Ok(()); }

    // Load metadata and plugin for potential scanning
    let (meta, plugin) = misc::load_meta_and_plugin(&p.part_dir)?;

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
        let (kept_any, new_max_opt) = truncate_chunk_and_index(&p.chunks_dir, m.chunk_id, cutoff_key, m.max_order_key, &*plugin)?;
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

    misc::rewrite_manifest(&p.manifest_path, &new_manifest)?;
    misc::delete_chunk_files(&p.chunks_dir, &to_delete);
    misc::refresh_runtime(h, &meta)?;

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
    let new_len;
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
        new_len = last_kept_end;
        new_max_key = last_kept_key;
    } else {
        // closed chunk: precise cut. If cutoff falls inside the next indexed block, scan within it.
        if index_path.exists() {
            let f = File::open(&index_path)?;
            let idx = load_index_lines(&f)?;
            // Find the first block whose max >= cutoff and whose file_offset == base_end_off
            if let Some(overlap) = idx.into_iter().find(|b| b.file_offset_bytes == base_end_off && b.block_max_key >= cutoff_key) {
                // Scan within this block to keep only records < cutoff
                let mut cf = File::open(&chunk_path)?;
                let mut scanner = plugin.scanner(&mut cf)?;
                scanner.seek_to(overlap.file_offset_bytes)?;
                let mut first_kept_key: Option<u64> = None;
                let mut last_kept_end = overlap.file_offset_bytes;
                let mut last_kept_key = new_max_key; // continues from previous blocks
                while let Some(m) = scanner.next()? {
                    if (m.offset as u64) >= overlap.file_offset_bytes + overlap.block_len_bytes { break; }
                    if (m.ts_ms as u64) >= cutoff_key { break; }
                    if first_kept_key.is_none() { first_kept_key = Some(m.ts_ms as u64); }
                    last_kept_end = m.offset + m.len as u64;
                    last_kept_key = Some(m.ts_ms as u64);
                }
                if last_kept_end > overlap.file_offset_bytes {
                    kept_index.push(IndexLine {
                        block_min_key: first_kept_key.unwrap_or(last_kept_key.unwrap_or(0)),
                        block_max_key: last_kept_key.unwrap_or(0),
                        file_offset_bytes: overlap.file_offset_bytes,
                        block_len_bytes: last_kept_end - overlap.file_offset_bytes,
                    });
                }
                new_len = last_kept_end;
                new_max_key = last_kept_key;
            } else {
                // cutoff at boundary or beyond: new_len remains base_end_off
                new_len = base_end_off;
            }
        } else {
            // No index for closed chunk: fall back to scanning from start
            let mut cf = File::open(&chunk_path)?;
            let mut scanner = plugin.scanner(&mut cf)?;
            let mut last_kept_end: u64 = 0;
            let mut last_kept_key: Option<u64> = None;
            while let Some(m) = scanner.next()? {
                if (m.ts_ms as u64) >= cutoff_key { break; }
                last_kept_end = m.offset + m.len as u64;
                last_kept_key = Some(m.ts_ms as u64);
            }
            new_len = last_kept_end;
            new_max_key = last_kept_key;
            kept_index.clear(); // unknown block boundaries; will rewrite empty index if exists
        }
    }

    // If no bytes remain, delete files by caller
    if new_len == 0 {
        // Truncate files to zero for safety; caller may remove them
        let _ = misc::truncate_file(&chunk_path, 0);
        let _ = crate::disk::index::rewrite_index_atomic(&index_path, &[]);
        return Ok((false, None));
    }

    // Truncate chunk file
    misc::truncate_file(&chunk_path, new_len)?;

    // Rewrite index file to only kept blocks (no partial adjustment). For open chunk, kept blocks already end <= new_len.
    // For closed chunk, we ensured end is exactly last kept block end.
    if index_path.exists() {
        crate::disk::index::rewrite_index_atomic(&index_path, &kept_index)?;
    }

    Ok((true, new_max_key))
}

