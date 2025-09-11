use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ManifestLine {
    pub chunk_id: Uuid,
    pub min_order_key: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_order_key: Option<u64>,
}

// Load the partition manifest (JSONL) into memory.
pub fn load_manifest(path: &std::path::Path) -> crate::errors::Result<Vec<ManifestLine>> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    let f = File::open(path)?;
    let rdr = BufReader::new(f);
    let mut out = Vec::new();
    for line in rdr.lines() {
        let l = line?;
        if l.trim().is_empty() { continue; }
        if let Ok(m) = serde_json::from_str::<ManifestLine>(&l) { out.push(m); }
    }
    Ok(out)
}

pub fn append_manifest_line(path: &std::path::Path, line: ManifestLine) -> crate::errors::Result<()> {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    let mut buf = serde_json::to_vec(&line)?; buf.push(b'\n'); f.write_all(&buf)?; Ok(())
}

pub fn close_manifest_line(path: &std::path::Path, rt: &crate::partition::PartitionRuntime) -> crate::errors::Result<()> {
    use std::io::Write;
    // Read existing manifest lines
    let content = std::fs::read_to_string(path).unwrap_or_default();
    let mut lines: Vec<String> = content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|s| s.to_string())
        .collect();
    let closed = ManifestLine { chunk_id: rt.cur_chunk_id.expect("chunk id"), min_order_key: rt.cur_chunk_min_order_key, max_order_key: Some(rt.cur_chunk_max_order_key) };
    let closed_json = serde_json::to_string(&closed)?;
    if lines.is_empty() {
        lines.push(closed_json);
    } else {
        // Replace last line
        if let Some(last) = lines.last_mut() { *last = closed_json; }
    }
    let mut out = lines.join("\n");
    out.push('\n');
    // Atomic-ish rewrite
    let tmp = path.with_extension("json.tmp");
    {
        let mut f = std::fs::OpenOptions::new().create(true).truncate(true).write(true).open(&tmp)?;
        f.write_all(out.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    // Best-effort fsync dir
    if let Some(dir) = path.parent() { let _ = crate::store::fsync::fsync_dir(dir); }
    Ok(())
}
