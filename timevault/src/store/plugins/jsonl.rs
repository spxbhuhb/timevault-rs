use crate::store::plugins::{ChunkScanner, FormatPlugin, ReadSeek, RecordMeta};
use serde::Deserialize;
use std::io::{self, BufRead, BufReader, SeekFrom};

#[derive(Default)]
pub struct JsonlPlugin;

impl FormatPlugin for JsonlPlugin {
    fn id(&self) -> &'static str {
        "jsonl"
    }

    fn scanner<'a>(&self, file: &'a mut dyn ReadSeek) -> io::Result<Box<dyn ChunkScanner + 'a>> {
        Ok(Box::new(JsonlScanner::new(file)))
    }

    fn encode(&self, ts_ms: i64, value: &serde_json::Value) -> io::Result<Vec<u8>> {
        // Encode a JSONL line with fields `timestamp` and `payload`.
        let rec = serde_json::json!({"timestamp": ts_ms, "payload": value});
        let mut line = serde_json::to_vec(&rec).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        line.push(b'\n');
        Ok(line)
    }
}

struct JsonlScanner<'a> {
    reader: BufReader<&'a mut dyn ReadSeek>,
    pos: u64,
    line_buf: Vec<u8>,
}

impl<'a> JsonlScanner<'a> {
    fn new(file: &'a mut dyn ReadSeek) -> Self {
        // Ensure starting at 0
        let _ = file.seek(SeekFrom::Start(0));
        Self {
            reader: BufReader::new(file),
            pos: 0,
            line_buf: Vec::new(),
        }
    }
}

impl<'a> ChunkScanner for JsonlScanner<'a> {
    fn next(&mut self) -> io::Result<Option<RecordMeta>> {
        self.line_buf.clear();
        let bytes_read = self.reader.read_until(b'\n', &mut self.line_buf)?;
        if bytes_read == 0 {
            return Ok(None);
        }

        let had_newline = self.line_buf.last().copied() == Some(b'\n');
        if had_newline {
            self.line_buf.pop();
            if self.line_buf.last().copied() == Some(b'\r') {
                self.line_buf.pop();
            }
        }

        let json = std::str::from_utf8(&self.line_buf).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf8 in jsonl"))?;
        let JL { timestamp } = serde_json::from_str::<JL>(json).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "json"))?;

        let rec_len = bytes_read as u32;
        let meta = RecordMeta {
            offset: self.pos,
            len: rec_len,
            ts_ms: timestamp,
        };
        self.pos += rec_len as u64;
        Ok(Some(meta))
    }

    fn seek_to(&mut self, offset: u64) -> io::Result<()> {
        use std::io::{Read, Seek, SeekFrom};
        // Position to requested offset
        self.reader.seek(SeekFrom::Start(offset))?;
        self.pos = offset;
        if offset == 0 {
            return Ok(());
        }
        // Check if previous byte is a newline; if not, we are in the middle of a line.
        self.reader.seek(SeekFrom::Start(offset - 1))?;
        let mut b = [0u8; 1];
        let n = self.reader.read(&mut b)?;
        let prev_is_nl = n == 1 && b[0] == b'\n';
        if !prev_is_nl {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "seek_to offset not at line boundary"));
        }
        Ok(())
    }
}

// Local type to avoid exposing
#[derive(Deserialize)]
struct JL {
    timestamp: i64,
}
