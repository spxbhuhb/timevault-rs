use crate::errors::{Result, TvError};
use crate::store::disk::chunk;
use crate::store::disk::manifest::{ManifestLine, append_manifest_line, close_manifest_line};
use crate::store::partition::PartitionHandle;
use crate::store::paths;

pub fn append(h: &PartitionHandle, order_key: u64, payload: &[u8]) -> Result<crate::store::partition::AppendAck> {
    if h.inner.read_only {
        return Err(TvError::ReadOnly);
    }
    let part_dir = paths::partition_dir(h.root(), h.id());
    let chunks_dir = paths::chunks_dir(&part_dir);

    let offset = {
        let mut rt = h.inner.runtime.write();
        // Idempotency guard: if the incoming record has the same key as the current
        // chunk's max key and the payload is identical to the last appended record,
        // skip appending. This avoids duplicating the tail record during replays.
        if rt.cur_chunk_id.is_some()
            && order_key == rt.cur_chunk_max_order_key
            && rt
                .cur_last_record_bytes
                .as_ref()
                .map(|b| b.as_slice() == payload)
                .unwrap_or(false)
        {
            return Ok(crate::store::partition::AppendAck { offset: rt.cur_chunk_size_bytes });
        }
        if ensure_ordering(&rt, order_key)?.is_none() {
            return Ok(crate::store::partition::AppendAck { offset: rt.cur_chunk_size_bytes });
        }
        maybe_roll(h, &part_dir, &mut rt, order_key, payload.len() as u64)?;
        let chunk_id = rt.cur_chunk_id.expect("chunk id set by maybe_roll/start_new_chunk");
        let off = append_to_chunk(&chunks_dir, chunk_id, &payload)?; // payload is opaque, it is the responsibility of the caller to validate it
        // Index block bookkeeping
        start_block_if_needed(&mut rt, order_key, off);
        extend_block(&mut rt, order_key, payload.len() as u64);
        if should_flush_block(&rt, &h.inner.cfg.read(), order_key) {
            flush_block(&chunks_dir, chunk_id, &mut rt)?;
        }
        update_runtime_after_append(&mut rt, order_key, &payload);
        off
    };

    update_stats(h, payload.len() as u64);
    Ok(crate::store::partition::AppendAck { offset })
}

pub fn set_config(h: &PartitionHandle, delta: crate::store::partition::PartitionConfigDelta) -> Result<()> {
    let mut cfg = h.inner.cfg.write();
    if let Some(p) = delta.format_plugin {
        cfg.format_plugin = p;
    }
    Ok(())
}

fn ensure_ordering(rt: &crate::store::partition::PartitionRuntime, order_key: u64) -> Result<Option<()>> {
    if rt.cur_chunk_id.is_some() && order_key < rt.cur_chunk_max_order_key {
        return Ok(None);
    }
    Ok(Some(()))
}

fn maybe_roll(h: &PartitionHandle, part_dir: &std::path::Path, rt: &mut crate::store::partition::PartitionRuntime, order_key: u64, next_len: u64) -> Result<()> {
    let cfg = h.inner.cfg.read().clone();
    let manifest_path = paths::partition_manifest(part_dir);
    if rt.cur_chunk_id.is_none() {
        start_new_chunk(&manifest_path, rt, order_key)?;
    } else if should_roll(rt, &cfg, order_key, next_len) {
        // flush pending index block before closing chunk
        if rt.cur_index_block_record_count > 0 {
            if let Some(chunk_id) = rt.cur_chunk_id {
                flush_block(&paths::chunks_dir(part_dir), chunk_id, rt)?;
            }
        }
        finalize_current_chunk(&manifest_path, rt)?;
        start_new_chunk(&manifest_path, rt, order_key)?;
        h.inner.stats.lock().current_chunk_size = 0;
    }
    Ok(())
}

fn append_to_chunk(chunks_dir: &std::path::Path, chunk_id: u64, line: &[u8]) -> Result<u64> {
    let chunk_path = paths::chunk_file(chunks_dir, chunk_id);
    let mut f = chunk::open_chunk_append(&chunk_path)?;
    chunk::append_all(&mut f, line)
}

fn update_runtime_after_append(rt: &mut crate::store::partition::PartitionRuntime, order_key: u64, line: &[u8]) {
    rt.cur_chunk_max_order_key = rt.cur_chunk_max_order_key.max(order_key);
    rt.cur_chunk_size_bytes += line.len() as u64;
    rt.cur_last_record_bytes = Some(line.to_vec());
}

fn start_block_if_needed(rt: &mut crate::store::partition::PartitionRuntime, order_key: u64, start_off: u64) {
    if rt.cur_index_block_record_count == 0 {
        rt.cur_index_block_min_order_key = order_key;
        rt.cur_index_block_max_order_key = order_key;
        rt.cur_index_block_start_off = start_off;
        rt.cur_index_block_len_bytes = 0;
        rt.cur_index_block_record_count = 0;
    }
}

fn extend_block(rt: &mut crate::store::partition::PartitionRuntime, order_key: u64, len: u64) {
    rt.cur_index_block_max_order_key = rt.cur_index_block_max_order_key.max(order_key);
    rt.cur_index_block_len_bytes += len;
    rt.cur_index_block_record_count += 1;
}

fn should_flush_block(rt: &crate::store::partition::PartitionRuntime, cfg: &crate::store::partition::PartitionConfig, order_key: u64) -> bool {
    if rt.cur_index_block_record_count == 0 {
        return false;
    }
    let by_recs = cfg.index.max_records > 0 && (rt.cur_index_block_record_count as u32) >= cfg.index.max_records;
    let by_time = if cfg.key_is_timestamp {
        let max_ms = cfg.index.max_hours.saturating_mul(3_600_000);
        max_ms > 0 && (order_key.saturating_sub(rt.cur_index_block_min_order_key)) >= max_ms
    } else {
        false
    };
    by_recs || by_time
}

fn flush_block(chunks_dir: &std::path::Path, chunk_id: u64, rt: &mut crate::store::partition::PartitionRuntime) -> Result<()> {
    if rt.cur_index_block_record_count == 0 {
        return Ok(());
    }
    let index_path = paths::index_file(chunks_dir, chunk_id);
    let line = crate::store::disk::index::IndexLine {
        block_min_key: rt.cur_index_block_min_order_key,
        block_max_key: rt.cur_index_block_max_order_key,
        file_offset_bytes: rt.cur_index_block_start_off,
        block_len_bytes: rt.cur_index_block_len_bytes,
    };
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(index_path)?;
    let mut buf = serde_json::to_vec(&line)?;
    buf.push(b'\n');
    f.write_all(&buf)?;
    f.sync_all()?;
    // reset current block
    rt.cur_index_block_min_order_key = 0;
    rt.cur_index_block_max_order_key = 0;
    rt.cur_index_block_start_off = 0;
    rt.cur_index_block_len_bytes = 0;
    rt.cur_index_block_record_count = 0;
    Ok(())
}

fn update_stats(h: &PartitionHandle, bytes: u64) {
    let mut s = h.inner.stats.lock();
    s.appends += 1;
    s.bytes += bytes;
    s.current_chunk_size += bytes;
}

fn should_roll(rt: &crate::store::partition::PartitionRuntime, cfg: &crate::store::partition::PartitionConfig, order_key: u64, next_len: u64) -> bool {
    if rt.cur_chunk_id.is_none() {
        return false;
    }
    // Rule precedence:
    // 1) If chunk empty and next_len > MAX_BYTES, allow single oversized-record chunk: no roll.
    if rt.cur_chunk_size_bytes == 0 && cfg.chunk_roll.max_bytes > 0 && next_len > cfg.chunk_roll.max_bytes {
        return false;
    }
    // 2) If same key as last in chunk, do not roll to keep same keys together.
    if order_key == rt.cur_chunk_max_order_key {
        return false;
    }
    // 3) Size-based: roll if adding would meet or exceed max_bytes.
    let by_size = cfg.chunk_roll.max_bytes > 0 && rt.cur_chunk_size_bytes + next_len >= cfg.chunk_roll.max_bytes;
    if by_size {
        return true;
    }
    // 4) Time-based: only if key_is_timestamp and Δt from chunk min >= max_hours.
    if cfg.key_is_timestamp {
        let max_ms = cfg.chunk_roll.max_hours.saturating_mul(3_600_000);
        if max_ms > 0 && (order_key.saturating_sub(rt.cur_chunk_min_order_key)) >= max_ms {
            return true;
        }
    }
    false
}

fn finalize_current_chunk(manifest_path: &std::path::Path, rt: &crate::store::partition::PartitionRuntime) -> Result<()> {
    if rt.cur_chunk_id.is_none() {
        return Ok(());
    }
    // Close the last open manifest entry by rewriting the last line to include max_order_key
    close_manifest_line(manifest_path, rt)
}

fn start_new_chunk(manifest_path: &std::path::Path, rt: &mut crate::store::partition::PartitionRuntime, order_key: u64) -> Result<()> {
    let new_id = order_key;
    // Write an open manifest line for the new chunk (no max_order_key)
    append_manifest_line(
        manifest_path,
        ManifestLine {
            chunk_id: new_id,
            min_order_key: order_key,
            max_order_key: None,
        },
    )?;
    // Create an empty index file alongside the new chunk using partition runtime
    crate::store::disk::index::create_empty_index_file(rt, new_id)?;

    rt.cur_chunk_id = Some(new_id);
    rt.cur_chunk_min_order_key = order_key;
    rt.cur_chunk_max_order_key = order_key;
    rt.cur_chunk_size_bytes = 0;
    rt.cur_index_block_min_order_key = 0;
    rt.cur_index_block_max_order_key = 0;
    rt.cur_index_block_start_off = 0;
    rt.cur_index_block_len_bytes = 0;
    rt.cur_index_block_record_count = 0;
    Ok(())
}
