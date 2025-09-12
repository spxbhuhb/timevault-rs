use crate::disk::index::{IndexLine, load_index_lines};
use crate::disk::manifest::{ManifestLine, load_manifest};
use crate::errors::{TvError, Result};
use crate::partition::PartitionHandle;
use crate::store::paths;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use uuid::Uuid;

pub fn read_range(h: &PartitionHandle, from_key: u64, to_key: u64) -> Result<Vec<u8>> {
    if from_key > to_key { return Err(TvError::InvalidRange { from: from_key as i64, to: to_key as i64 }); }
    let part_dir = paths::partition_dir(h.root(), h.id());
    let chunks_dir = paths::chunks_dir(&part_dir);
    let manifest_path = paths::partition_manifest(&part_dir);
    if !manifest_path.exists() { return Err(TvError::MissingFile { path: manifest_path }); }

    let manifest = load_manifest(&manifest_path)?;
    let mut out = Vec::new();
    for m in select_chunks(&manifest, from_key, to_key) {
        let chunk_path = paths::chunk_file(&chunks_dir, m.chunk_id);
        if !chunk_path.exists() { return Err(TvError::MissingFile { path: chunk_path }); }
        if is_fully_covered(&m, from_key, to_key) {
            append_entire_file(&chunk_path, &mut out)?;
            continue;
        }
        append_indexed_ranges(&chunks_dir, m.chunk_id, from_key, to_key, &mut out)?;
    }
    Ok(out)
}


fn select_chunks(manifest: &[ManifestLine], from_key: u64, to_key: u64) -> Vec<ManifestLine> {
    let mut out = Vec::new();
    for m in manifest {
        if let Some(max) = m.max_order_key {
            if m.min_order_key <= to_key && max >= from_key { out.push(m.clone()); }
        } else {
            if m.min_order_key <= to_key { out.push(m.clone()); }
        }
    }
    out
}

fn is_fully_covered(m: &ManifestLine, from_key: u64, to_key: u64) -> bool {
    match m.max_order_key { Some(max) => m.min_order_key >= from_key && max <= to_key, None => false }
}

fn append_entire_file(path: &std::path::Path, out: &mut Vec<u8>) -> Result<()> {
    let mut f = File::open(path)?;
    f.read_to_end(out)?;
    Ok(())
}

fn append_indexed_ranges(chunks_dir: &std::path::Path, chunk_id: Uuid, from_key: u64, to_key: u64, out: &mut Vec<u8>) -> Result<()> {
    let chunk_path = paths::chunk_file(chunks_dir, chunk_id);
    let index_path = paths::index_file(chunks_dir, chunk_id);
    let mut f = File::open(&chunk_path).map_err(|_| TvError::MissingFile { path: chunk_path.clone() })?;
    let idx_f = File::open(&index_path).map_err(|_| TvError::MissingFile { path: index_path.clone() })?;
    let idx = load_index_lines(&idx_f)?;
    let mut ranges = select_block_ranges(&idx, from_key, to_key);
    // If this is an open chunk, the tail may be unindexed; include it if it overlaps the query.
    // Detect open chunk by checking if the last index block ends before EOF.
    if let Ok(meta) = f.metadata() {
        let file_len = meta.len();
        if let Some(last) = idx.last() {
            let last_end = last.file_offset_bytes + last.block_len_bytes;
            if last_end < file_len {
                // Unindexed tail exists; include it as a range.
                ranges.push((last_end, file_len - last_end));
            }
        } else {
            // No index entries; include entire file as a single range.
            if file_len > 0 { ranges.push((0, file_len)); }
        }
    }
    read_and_append_ranges(&mut f, &ranges, out)?;
    Ok(())
}

fn select_block_ranges(idx: &[IndexLine], from_key: u64, to_key: u64) -> Vec<(u64, u64)> {
    let mut ranges = Vec::new();
    let mut started = false;
    for b in idx {
        if !started {
            if b.block_max_key >= from_key { started = true; ranges.push((b.file_offset_bytes, b.block_len_bytes)); }
        } else {
            if b.block_min_key > to_key { break; }
            ranges.push((b.file_offset_bytes, b.block_len_bytes));
        }
    }
    ranges
}

fn read_and_append_ranges(f: &mut File, ranges: &[(u64, u64)], out: &mut Vec<u8>) -> Result<()> {
    let mut buf = vec![0u8; 64 * 1024];
    for &(off, len) in ranges {
        read_range_into(f, off, len, &mut buf, out)?;
    }
    Ok(())
}

fn read_range_into(f: &mut File, off: u64, len: u64, tmp: &mut [u8], out: &mut Vec<u8>) -> Result<()> {
    f.seek(SeekFrom::Start(off))?;
    let mut remaining = len as usize;
    while remaining > 0 {
        let to_read = remaining.min(tmp.len());
        let n = f.read(&mut tmp[..to_read])?;
        if n == 0 { break; }
        out.extend_from_slice(&tmp[..n]);
        remaining -= n;
    }
    Ok(())
}
