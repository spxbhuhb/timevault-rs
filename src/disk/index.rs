use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};

use crate::errors::Result;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexLine {
    pub block_min_key: u64,
    pub block_max_key: u64,
    pub file_offset_bytes: u64,
    pub block_len_bytes: u64,
}

pub fn load_index_lines(file: &File) -> Result<Vec<IndexLine>> {
    let reader = BufReader::new(file);
    let mut out = Vec::new();
    for line in reader.lines() {
        let l = line?;
        if l.trim().is_empty() { continue; }
        let idx = serde_json::from_str::<IndexLine>(&l)?;
        out.push(idx);
    }
    Ok(out)
}

// Create an empty index file for the current partition and given chunk.
// Uses partition runtime to locate the chunks directory.
pub fn create_empty_index_file(rt: &crate::partition::PartitionRuntime, chunk_id: uuid::Uuid) -> Result<()> {
    let part_dir = crate::store::paths::partition_dir(&rt.cur_partition_root, rt.cur_partition_id);
    let chunks_dir = crate::store::paths::chunks_dir(&part_dir);
    let ip = crate::store::paths::index_file(&chunks_dir, chunk_id);
    let mut f = OpenOptions::new().create(true).write(true).open(&ip)?;
    f.flush()?;
    let _ = f.sync_all();
    if let Some(dir) = ip.parent() { let _ = crate::store::fsync::fsync_dir(dir); }
    Ok(())
}

// Atomically rewrite an index file with the provided lines. Ensures data and directory durability.
pub fn rewrite_index_atomic(path: &std::path::Path, lines: &[IndexLine]) -> Result<()> {
    let tmp = path.with_extension("index.tmp");
    {
        let mut f = OpenOptions::new().create(true).truncate(true).write(true).open(&tmp)?;
        for l in lines {
            let mut buf = serde_json::to_vec(l)?; buf.push(b'\n'); f.write_all(&buf)?;
        }
        f.flush()?; let _ = f.sync_all();
    }
    std::fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() { let _ = crate::store::fsync::fsync_dir(dir); }
    Ok(())
}
