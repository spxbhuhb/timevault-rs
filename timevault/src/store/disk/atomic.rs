use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::errors::Result;

pub fn atomic_write_json<P: AsRef<Path>, T: serde::Serialize>(path: P, val: &T) -> Result<()> {
    let path = path.as_ref();
    let tmp = tmp_path(path);
    write_json(&tmp, val)?;
    fsync_file(&tmp)?;
    std::fs::rename(&tmp, path)?;
    fsync_dir(path.parent().unwrap_or(Path::new("../../../..")))?;
    Ok(())
}

fn write_json(path: &Path, val: &impl serde::Serialize) -> Result<()> {
    let mut f = OpenOptions::new().create(true).truncate(true).write(true).open(path)?;
    let s = serde_json::to_vec(val)?;
    f.write_all(&s)?;
    Ok(())
}

fn fsync_file(path: &Path) -> Result<()> {
    Ok(File::open(path)?.sync_all()?)
}
fn fsync_dir(path: &Path) -> Result<()> {
    Ok(File::open(path)?.sync_all()?)
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut p = path.to_path_buf();
    let name = p.file_name().unwrap().to_string_lossy().to_string() + ".tmp";
    p.set_file_name(name);
    p
}
