use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use crate::errors::Result;

pub fn open_chunk_append(path: &Path) -> Result<File> {
    let f = OpenOptions::new().create(true).append(true).read(true).open(path)?;
    Ok(f)
}

pub fn file_len(f: &File) -> Result<u64> { Ok(f.metadata()?.len()) }

pub fn append_all(f: &mut File, buf: &[u8]) -> Result<u64> {
    let off = f.seek(SeekFrom::End(0))?;
    f.write_all(buf)?;
    Ok(off)
}
