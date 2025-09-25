use std::path::{Path, PathBuf};
use uuid::Uuid;

pub fn partitions_root(root: &Path) -> PathBuf { root.join("partitions") }

pub fn partition_dir(root: &Path, id: Uuid) -> PathBuf {
    let b = id.as_bytes();
    let shard_hi = format!("{:02x}", b[14]);
    let shard_lo = format!("{:02x}", b[15]);
    partitions_root(root).join(shard_hi).join(shard_lo).join(id.to_string())
}

pub fn partition_manifest(part_dir: &Path) -> PathBuf { part_dir.join("manifest.jsonl") }
pub fn partition_metadata(part_dir: &Path) -> PathBuf { part_dir.join("metadata.json") }
pub fn chunks_dir(part_dir: &Path) -> PathBuf { part_dir.join("chunks") }
pub fn tmp_dir(part_dir: &Path) -> PathBuf { part_dir.join("tmp") }
pub fn gc_dir(part_dir: &Path) -> PathBuf { part_dir.join("gc") }

fn padded_hex(id: u64) -> String { format!("{:016x}", id) }

pub fn chunk_file(chunks_dir: &Path, chunk_id: u64) -> PathBuf { chunks_dir.join(format!("{}.chunk", padded_hex(chunk_id))) }
pub fn index_file(chunks_dir: &Path, chunk_id: u64) -> PathBuf { chunks_dir.join(format!("{}.index", padded_hex(chunk_id))) }

pub fn list_partitions(root: &Path) -> crate::errors::Result<Vec<Uuid>> {
    let mut out = Vec::new();
    let root = partitions_root(root);
    if !root.exists() { return Ok(out); }
    for a in std::fs::read_dir(root)? {
        let a = a?; if !a.file_type()?.is_dir() { continue; }
        for b in std::fs::read_dir(a.path())? {
            let b = b?; if !b.file_type()?.is_dir() { continue; }
            for c in std::fs::read_dir(b.path())? {
                let c = c?; if !c.file_type()?.is_dir() { continue; }
                if let Ok(s) = c.file_name().into_string() { if let Ok(id) = Uuid::parse_str(&s) { out.push(id); } }
            }
        }
    }
    Ok(out)
}