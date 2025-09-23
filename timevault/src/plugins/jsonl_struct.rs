use crate::plugins::{ChunkScanner, FormatPlugin, ReadSeek, RecordMeta};
use serde::Deserialize;
use std::io::{self, BufRead, BufReader, SeekFrom};

#[derive(Default)]
pub struct JsonlStructPlugin;

impl FormatPlugin for JsonlStructPlugin {
    fn id(&self) -> &'static str { "jsonl_struct" }

    fn scanner<'a>(&self, file: &'a mut (dyn ReadSeek)) -> io::Result<Box<dyn ChunkScanner + 'a>> {
        Ok(Box::new(JsonlStructScanner::new(file)))
    }

    fn encode(&self, ts_ms: i64, value: &serde_json::Value) -> io::Result<Vec<u8>> {
        // Encode a JSONL line with structured fields alongside the timestamp.
        let mut obj = value.as_object().cloned().ok_or_else(|| io::Error::new(io::ErrorKind::Other, "expected object"))?;
        obj.insert("timestamp".to_string(), serde_json::Value::from(ts_ms));
        let mut line = serde_json::to_vec(&obj).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        line.push(b'\n');
        Ok(line)
    }
}

struct JsonlStructScanner<'a> {
    reader: BufReader<&'a mut (dyn ReadSeek)>,
    pos: u64,
}

impl<'a> JsonlStructScanner<'a> {
    fn new(file: &'a mut (dyn ReadSeek)) -> Self {
        let _ = file.seek(SeekFrom::Start(0));
        Self { reader: BufReader::new(file), pos: 0 }
    }
}

impl<'a> ChunkScanner for JsonlStructScanner<'a> {
    fn next(&mut self) -> io::Result<Option<RecordMeta>> {
        let buf = self.reader.fill_buf()?;
        if buf.is_empty() { return Ok(None); }
        let line_end = match memchr::memchr(b'\n', buf) { Some(i) => i, None => buf.len() };
        let line = &buf[..line_end];
        let json = std::str::from_utf8(line).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf8 in jsonl"))?;
        let Rec { timestamp } = serde_json::from_str::<Rec>(json)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "json"))?;
        let rec_len = line_end as u32 + 1;
        self.reader.consume(line_end + 1);
        let meta = RecordMeta { offset: self.pos, len: rec_len, ts_ms: timestamp };
        self.pos += rec_len as u64;
        Ok(Some(meta))
    }

    fn seek_to(&mut self, offset: u64) -> io::Result<()> {
        use std::io::{Read, Seek, SeekFrom};
        self.reader.seek(SeekFrom::Start(offset))?;
        self.pos = offset;
        if offset == 0 { return Ok(()); }
        self.reader.seek(SeekFrom::Start(offset - 1))?;
        let mut b = [0u8;1];
        let n = self.reader.read(&mut b)?;
        let prev_is_nl = n == 1 && b[0] == b'\n';
        if !prev_is_nl {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "seek_to offset not at line boundary"));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct Rec { timestamp: i64 }
