use std::fs::File;
use std::io::{Read, Seek};

use crate::disk::index::{IndexLine, load_index_lines};
use crate::plugins::FormatPlugin;
use crate::partition::PartitionRuntime;
use crate::store::paths;
use uuid::Uuid;

// Initialize runtime by using provided metadata for plugin selection, then manifest/index/chunk per design.md.
pub fn load_partition_runtime_data(root: &std::path::Path, id: Uuid, meta: &crate::disk::metadata::MetadataJson) -> crate::errors::Result<PartitionRuntime> {
    let plugin = crate::plugins::resolve_plugin(&meta.format_plugin)?;
    // Handle pending physical purge if any (idempotent). Logical purge is a no-op here.
    if !meta.logical_purge {
        if let Some(cutoff) = meta.last_purge_id {
            let part_dir = paths::partition_dir(root, id);
            // Best-effort: run purge; ignore errors due to missing files to avoid blocking recovery
            let _ = crate::partition::purge::purge_partition_dir(&part_dir, cutoff, &*plugin);
        }
    }
    load_partition_runtime_data_inner(root, id, &*plugin)
}

fn load_partition_runtime_data_inner(root: &std::path::Path, id: Uuid, plugin: &dyn FormatPlugin) -> crate::errors::Result<PartitionRuntime> {
    let part_dir = paths::partition_dir(root, id);
    let manifest_path = paths::partition_manifest(&part_dir);
    let mut cache = PartitionRuntime::default();
    if !manifest_path.exists() { return Ok(cache); }
    let lines = crate::disk::manifest::load_manifest(&manifest_path)?;
    let Some(last) = lines.last().cloned() else { return Ok(cache); };

    cache.cur_chunk_id = Some(last.chunk_id);
    cache.cur_chunk_min_order_key = last.min_order_key;
    // Do not infer max from min; will determine from chunk content/index
    cache.cur_chunk_max_order_key = last.max_order_key.unwrap_or(0);

    let chunks_dir = paths::chunks_dir(&part_dir);
    let chunk_path = paths::chunk_file(&chunks_dir, last.chunk_id);
    let meta = std::fs::metadata(&chunk_path).map_err(|_| crate::errors::TvError::MissingFile { path: chunk_path.clone() })?;
    cache.cur_chunk_size_bytes = meta.len();

    // Load last index and compute start offset for tail scan
    let index_path = paths::index_file(&chunks_dir, last.chunk_id);
    let (last_index, start_off) = load_last_index_and_start(&index_path, &mut cache)?;

    // Recover last record and tail stats using the selected plugin.
    recover_tail_with_plugin_from_offset(&chunk_path, start_off, &mut cache, last_index.is_none(), plugin)?;

    // If open chunk (no max in manifest) but we recovered a later max from last record, set it.
    // Determine last_chunk_max strictly from last chunk content/index
    let determined_max = if last_index.is_some() {
        Some(cache.cur_index_block_max_order_key)
    } else if cache.cur_index_block_record_count > 0 {
        Some(cache.cur_index_block_max_order_key)
    } else {
        None
    };

    if let Some(max_key) = determined_max {
        cache.cur_chunk_max_order_key = max_key;
    } else {
        return Err(crate::errors::TvError::Io(std::io::Error::new(std::io::ErrorKind::Other, "failed to determine cur_chunk_max_order_key from last chunk")));
    }

    Ok(cache)
}

fn load_last_index_and_start(index_path: &std::path::Path, cache: &mut PartitionRuntime) -> crate::errors::Result<(Option<IndexLine>, u64)> {
    if !index_path.exists() { return Err(crate::errors::TvError::MissingFile { path: index_path.to_path_buf() }); }
    let f = File::open(index_path)?;
    let mut idx = load_index_lines(&f)?;
    if let Some(ix) = idx.pop() {
        cache.cur_index_block_min_order_key = ix.block_min_key;
        cache.cur_index_block_max_order_key = ix.block_max_key;
        cache.cur_index_block_size_bytes = ix.block_len_bytes;
        cache.cur_index_block_start_off = ix.file_offset_bytes;
        cache.cur_index_block_len_bytes = ix.block_len_bytes;
        let start_off = ix.file_offset_bytes + ix.block_len_bytes;
        return Ok((Some(ix), start_off));
    }
    Ok((None, 0))
}

fn recover_tail_with_plugin_from_offset(chunk_path: &std::path::Path, start_off: u64, cache: &mut PartitionRuntime, no_index: bool, plugin: &dyn FormatPlugin) -> crate::errors::Result<()> {
    let mut f = File::open(chunk_path)?;
    // Use the partition's format plugin to scan records
    let mut scanner = plugin.scanner(&mut f)?;
    // Position scanner to the start offset and align to a record boundary if needed.
    scanner.seek_to(start_off)?;

    let mut last_key: Option<u64> = None;
    let mut first_key_in_scan: Option<u64> = None;
    let mut last_meta: Option<(u64, u32)> = None;
    let mut rec_count: u64 = 0;
    let mut total_bytes: u64 = 0;

    // Read from positioned offset to EOF
    while let Some(m) = scanner.next()? {
        let key = m.ts_ms as u64; // map plugin ts to order_key
        if first_key_in_scan.is_none() { first_key_in_scan = Some(key); }
        last_key = Some(key);
        last_meta = Some((m.offset, m.len));
        rec_count += 1;
        total_bytes += m.len as u64;
    }

    if rec_count > 0 {
        // If there was no index, initialize min from first scanned record.
        if no_index {
            if let Some(min_k) = first_key_in_scan { cache.cur_index_block_min_order_key = min_k; }
            if let Some(max_k) = last_key { cache.cur_index_block_max_order_key = max_k; }
            cache.cur_index_block_size_bytes = total_bytes;
            cache.cur_index_block_start_off = start_off;
            cache.cur_index_block_len_bytes = total_bytes;
        } else {
            // Extend indexed block
            if let Some(max_k) = last_key { cache.cur_index_block_max_order_key = max_k; }
            cache.cur_index_block_size_bytes = cache.cur_index_block_size_bytes + total_bytes;
            cache.cur_index_block_start_off = start_off;
            cache.cur_index_block_len_bytes = total_bytes;
        }
        cache.cur_index_block_record_count = rec_count;
        // Read last record bytes in a separate handle to avoid borrow conflicts
        let mut f2 = File::open(chunk_path)?;
        if let Some((off, len)) = last_meta {
            let bytes = read_record_bytes(&mut f2, off, len)?;
            cache.cur_last_record_bytes = Some(bytes);
        }
        if let Some(k) = last_key { cache.cur_chunk_max_order_key = cache.cur_chunk_max_order_key.max(k); }
    } else if no_index {
        cache.cur_index_block_start_off = start_off;
        cache.cur_index_block_len_bytes = total_bytes;
    }
    Ok(())
}

fn read_record_bytes(f: &mut (impl Read + Seek), offset: u64, len: u32) -> crate::errors::Result<Vec<u8>> {
    use std::io::{SeekFrom};
    let mut buf = vec![0u8; len as usize];
    f.seek(SeekFrom::Start(offset))?;
    f.read_exact(&mut buf)?;
    Ok(buf)
}
