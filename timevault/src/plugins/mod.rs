use std::io::{self, Read, Seek};

#[derive(Debug, Clone, Copy)]
pub struct RecordMeta { pub offset: u64, pub len: u32, pub ts_ms: i64 }

pub trait ChunkScanner {
    fn next(&mut self) -> io::Result<Option<RecordMeta>>;
    fn seek_to(&mut self, offset: u64) -> io::Result<()>;
    fn last(&mut self) -> io::Result<Option<RecordMeta>> {
        let mut last = None;
        while let Some(m) = self.next()? { last = Some(m); }
        Ok(last)
    }
}

pub trait ReadSeek: Read + Seek {}
impl<T: Read + Seek + ?Sized> ReadSeek for T {}

pub trait FormatPlugin: Send + Sync + 'static {
    fn id(&self) -> &'static str;
    fn scanner<'a>(&self, file: &'a mut (dyn ReadSeek)) -> io::Result<Box<dyn ChunkScanner + 'a>>;
    // Encode a record from a serde value into on-disk bytes.
    fn encode(&self, ts_ms: i64, value: &serde_json::Value) -> io::Result<Vec<u8>>;
    fn valid_prefix_len(&self, _file: &mut (dyn ReadSeek)) -> io::Result<Option<u64>> { Ok(None) }
    fn dedup_eq(&self, _a: &[u8], _b: &[u8]) -> bool { false }
}

pub mod jsonl;
pub mod jsonl_struct;

use std::sync::Arc;
use crate::errors::Result;

pub fn resolve_plugin(name: &str) -> Result<Arc<dyn FormatPlugin>> {
    match name {
        "jsonl" | "json" => Ok(Arc::new(jsonl::JsonlPlugin::default())),
        "jsonl_struct" => Ok(Arc::new(jsonl_struct::JsonlStructPlugin::default())),
        other => Err(crate::errors::TvError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("unknown plugin: {}", other),
        ))),
    }
}
