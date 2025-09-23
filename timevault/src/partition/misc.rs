use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

use crate::errors::{Result, TvError};
use crate::partition::PartitionHandle;

pub(crate) fn ensure_writable(h: &PartitionHandle) -> Result<()> {
    if h.inner.read_only { return Err(TvError::ReadOnly); }
    Ok(())
}

pub(crate) struct Paths {
    pub part_dir: PathBuf,
    pub chunks_dir: PathBuf,
    pub manifest_path: PathBuf,
}

pub(crate) fn paths_for(h: &PartitionHandle) -> Result<Paths> {
    let part_dir = crate::store::paths::partition_dir(h.root(), h.id());
    let chunks_dir = crate::store::paths::chunks_dir(&part_dir);
    let manifest_path = crate::store::paths::partition_manifest(&part_dir);
    if !manifest_path.exists() { return Err(TvError::MissingFile { path: manifest_path }); }
    Ok(Paths { part_dir, chunks_dir, manifest_path })
}

pub(crate) fn load_meta_and_plugin(part_dir: &std::path::Path) -> Result<(crate::disk::metadata::MetadataJson, Arc<dyn crate::plugins::FormatPlugin>)> {
    let meta = crate::disk::metadata::load_metadata(&crate::store::paths::partition_metadata(part_dir))?;
    let plugin = crate::plugins::resolve_plugin(&meta.format_plugin)?;
    Ok((meta, plugin))
}

pub(crate) fn rewrite_manifest(path: &std::path::Path, lines: &[crate::disk::manifest::ManifestLine]) -> Result<()> {
    crate::disk::manifest::rewrite_manifest_atomic(path, lines)
}

pub(crate) fn delete_chunk_files(chunks_dir: &std::path::Path, ids: &[Uuid]) {
    for id in ids.iter().copied() {
        let cp = crate::store::paths::chunk_file(chunks_dir, id);
        let ip = crate::store::paths::index_file(chunks_dir, id);
        let _ = fs::remove_file(cp);
        let _ = fs::remove_file(ip);
    }
    let _ = crate::store::fsync::fsync_dir(chunks_dir);
}

pub(crate) fn refresh_runtime(h: &PartitionHandle, meta: &crate::disk::metadata::MetadataJson) -> Result<()> {
    // It might be better performance-wise to update the in-memory runtime directly, but this is simpler.
    // Purge can do a lot of things, it would be hard to keep track properly, better to just re-load the whole thing.
    // Maybe we can optimize this later.
    let rt = crate::partition::recovery::load_partition_runtime_data(h.root(), h.id(), meta)?;
    *h.inner.runtime.write() = {
        let mut r = rt.clone();
        r.cur_partition_root = h.inner.root.clone();
        r.cur_partition_id = h.inner.id;
        r
    };
    Ok(())
}

pub(crate) fn truncate_file(path: &std::path::Path, new_len: u64) -> Result<()> {
    use std::fs::OpenOptions;
    let f = OpenOptions::new().write(true).open(path)?;
    f.set_len(new_len)?;
    let _ = f.sync_all();
    Ok(())
}
