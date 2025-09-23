use std::fs::File;
use std::path::Path;

use crate::errors::Result;

pub fn fsync_file(path: &Path) -> Result<()> { Ok(File::open(path)?.sync_all()?) }
pub fn fsync_dir(path: &Path) -> Result<()> { Ok(File::open(path)?.sync_all()?) }
