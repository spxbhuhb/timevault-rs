use crate::errors::{TvError, Result};
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::path::Path;

pub fn acquire_store_lock(root: &Path) -> Result<()> {
    let path = root.join(".timevault.write.lock");
    let f = OpenOptions::new().create(true).read(true).write(true).open(&path)?;
    if let Err(_) = f.try_lock_exclusive() { return Err(TvError::AlreadyOpen); }
    write_identity(&f)?;
    f.sync_all()?;
    Ok(())
}

fn write_identity(f: &File) -> std::io::Result<()> {
    let pid = std::process::id();
    let s = format!("pid={pid}\n");
    let mut f = f;
    f.set_len(0)?;
    use std::io::Write;
    f.write_all(s.as_bytes())
}
