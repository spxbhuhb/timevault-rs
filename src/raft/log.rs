use std::io::Cursor;
use std::sync::mpsc::{Receiver, Sender, SyncSender, channel, sync_channel};
use std::thread;

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState,
    RaftLogReader, StorageError, Vote,
};
use serde::{Deserialize, Serialize};
use tracing::trace;
use crate::disk::atomic::atomic_write_json;
use crate::PartitionHandle;
use crate::errors::TvError;
use crate::partition::read::read_range_blocks;
use crate::raft::{TvrRequest, TvrConfig, TvrNodeId};
use crate::raft::paths;
use crate::raft::errors::{map_send_err, recv_map, recv_unit};

#[derive(Serialize, Deserialize, Clone)]
struct JsonlRecord {
    timestamp: i64,
    log_id: LogId<TvrNodeId>,
    kind: String,
    payload: serde_json::Value,
}


enum Op {
    // list of (order_key=index, encoded)
    Append(Vec<(u64, Vec<u8>)>, LogFlushed<TvrConfig>),
    Read(u64, u64, Sender<Result<Vec<u8>, TvError>>),
    Truncate(u64, Sender<Result<(), TvError>>),
    Purge(LogId<TvrNodeId>, Sender<Result<(), TvError>>),
    GetState(Sender<Result<LogState<TvrConfig>, TvError>>),
    SaveVote(Vote<TvrNodeId>, Sender<Result<(), TvError>>),
    ReadVote(Sender<Result<Option<Vote<TvrNodeId>>, TvError>>),
    Shutdown,
}

pub struct TvrLogAdapter {
    part: PartitionHandle,
    node_id: TvrNodeId,
    tx: SyncSender<Op>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

impl TvrLogAdapter {

    pub fn new(part: PartitionHandle, node_id: TvrNodeId) -> Self {
        let cfg = part.cfg();
        if cfg.format_plugin != "jsonl" {
            panic!("Only jsonl format is supported for now.");
        }
        let (tx, rx) = sync_channel::<Op>(1024);
        let hpart = part.clone();
        let handle = thread::spawn(move || Self::bg_loop(hpart, rx));
        Self {
            part,
            node_id,
            tx,
            thread_handle: Some(handle),
        }
    }

    fn bg_loop(part: PartitionHandle, rx: Receiver<Op>) {
        while let Ok(op) = rx.recv() {
            match op {
                Op::Append(list, cb) => {
                    trace!("bg_loop {}: append list.len={}", part.short_id(), list.len());
                    let mut first_err: Option<TvError> = None;
                    for (idx, bytes) in list {
                        if let Err(e) = part.append(idx, &bytes) {
                            first_err = Some(e);
                            break;
                        }
                    }
                    if let Some(e) = first_err {
                        let _ = cb.log_io_completed(Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e.to_string(),
                        )));
                    } else {
                        let _ = cb.log_io_completed(Ok(()));
                    }
                }
                Op::Read(a, b, tx) => {
                    trace!("bg_loop {}: read a={}, b={}", part.short_id(), a, b);
                    let r = read_range_blocks(&part, a, b);
                    tx.send(r).expect("bg_loop: failed to send Read response");
                }
                Op::Truncate(idx, tx) => {
                    trace!("bg_loop {}: truncate idx={}", part.short_id(), idx);
                    let r = part.truncate(idx);
                    tx.send(r).expect("bg_loop: failed to send Truncate response");
                }
                Op::Purge(id, tx) => {
                    trace!("bg_loop {}: purge id={:x}", part.short_id(), id.index);
                    let r = purge_with_persist(&part, &id);
                    tx.send(r).expect("bg_loop: failed to send Purge response");
                }
                Op::GetState(tx) => {
                    trace!("bg_loop {}: get state", part.short_id());
                    let r = get_state(&part);
                    tx.send(r).expect("bg_loop: failed to send GetState response");
                }
                Op::SaveVote(v, tx) => {
                    trace!("bg_loop {}: save vote", part.short_id());
                    let r = save_vote_file(&part, &v);
                    tx.send(r).expect("bg_loop: failed to send SaveVote response");
                }
                Op::ReadVote(tx) => {
                    trace!("bg_loop {}: read vote", part.short_id());
                    let r = read_vote_file(&part);
                    tx.send(r).expect("bg_loop: failed to send ReadVote response");
                }
                Op::Shutdown => {
                    trace!("bg_loop {}: shutdown", part.short_id());
                    break
                },
            }
        }
    }
}

impl Drop for TvrLogAdapter {
    fn drop(&mut self) {
        let _ = self.tx.send(Op::Shutdown);
        if let Some(h) = self.thread_handle.take() {
            let _ = h.join();
        }
    }
}

impl Clone for TvrLogAdapter {
    fn clone(&self) -> Self {
        Self {
            part: self.part.clone(),
            node_id: self.node_id.clone(),
            tx: self.tx.clone(),
            thread_handle: None,
        }
    }
}

pub(crate) fn encode_entry(e: &Entry<TvrConfig>) -> Vec<u8> {
    let (kind, val) = match &e.payload {
        EntryPayload::Blank => ("Blank".to_string(), serde_json::Value::Null),
        EntryPayload::Normal(r) => ("Normal".to_string(), r.0.clone()),
        EntryPayload::Membership(m) => (
            "Membership".to_string(),
            serde_json::to_value(m).unwrap_or(serde_json::Value::Null),
        ),
    };
    let rec = JsonlRecord {
        timestamp: e.log_id.index as i64,
        log_id: e.log_id,
        kind,
        payload: val,
    };
    let mut out = serde_json::to_vec(&rec).unwrap();
    out.push(b'\n');
    out
}

pub(crate) fn decode_entries(part : &PartitionHandle, buf: &[u8], range: std::ops::RangeInclusive<u64>) -> Vec<Entry<TvrConfig>> {
    let plugin = match crate::plugins::resolve_plugin("jsonl") {
        Ok(p) => p,
        Err(_) => return Vec::new(),
    };
    let mut cur = Cursor::new(buf);
    let mut sc = match plugin.scanner(&mut cur) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    let mut out = Vec::new();
    while let Ok(Some(meta)) = sc.next() {
        // Safety: meta.len includes the trailing newline, which serde_json tolerates.
        let start = meta.offset as usize;
        let end = start + meta.len as usize;
        if end > buf.len() { break; }
        let line = &buf[start..end];
        if let Ok(rec) = serde_json::from_slice::<JsonlRecord>(line) {
            if !range.contains(&rec.log_id.index) { continue; }
            let payload = match rec.kind.as_str() {
                "Blank" => EntryPayload::Blank,
                "Membership" => {
                    match serde_json::from_value(rec.payload) {
                        Ok(m) => EntryPayload::Membership(m),
                        Err(_) => EntryPayload::Blank,
                    }
                }
                _ => EntryPayload::Normal(TvrRequest(rec.payload)),
            };
            trace!("decode_entries {}: {} {:?}", part.short_id(), rec.log_id, payload.clone());
            out.push(Entry { log_id: rec.log_id, payload });
        }
    }
    out
}

pub(crate) fn get_state(part: &PartitionHandle) -> Result<LogState<TvrConfig>, TvError> {
    let purge = read_purge_file(part).ok().flatten();

    let last = part.last_record();
    let last_log_id;

    if let Some(last) = last {
        let rec = serde_json::from_slice::<JsonlRecord>(&*last)?;
        last_log_id = Some(rec.log_id);
    } else {
        last_log_id = purge;
    }

    trace!("get_state: last_log_id={:?}, purge={:?}", last_log_id, purge);

    Ok(LogState {
        last_purged_log_id: purge,
        last_log_id,
    })
}

pub(crate) fn purge_with_persist(part: &PartitionHandle, id: &LogId<TvrNodeId>) -> Result<(), TvError> {
    save_purge_file(part, id)?;
    part.purge(id.index)
}

pub(crate) fn save_vote_file(part: &PartitionHandle, v: &Vote<TvrNodeId>) -> Result<(), TvError> {
    atomic_write_json(paths::vote_file(part), v)
}

pub(crate) fn read_vote_file(part: &PartitionHandle) -> Result<Option<Vote<TvrNodeId>>, TvError> {
    let p = paths::vote_file(&part);
    if !p.exists() {
        return Ok(None);
    }
    let f = std::fs::File::open(p)?;
    Ok(Some(serde_json::from_reader(f)?))
}

#[derive(Serialize, Deserialize)]
struct PurgeFile {
    last_deleted: LogId<TvrNodeId>,
}

pub(crate) fn save_purge_file(part: &PartitionHandle, id: &LogId<TvrNodeId>) -> Result<(), TvError> {
    atomic_write_json(
        paths::purge_file(&part),
        &PurgeFile {
            last_deleted: id.clone(),
        },
    )
}

pub(crate) fn read_purge_file(part: &PartitionHandle) -> Result<Option<LogId<TvrNodeId>>, TvError> {
    let p = paths::purge_file(&part);
    if !p.exists() {
        return Ok(None);
    }
    let f = std::fs::File::open(p)?;
    let v: PurgeFile = serde_json::from_reader(f)?;
    Ok(Some(v.last_deleted))
}

impl RaftLogReader<TvrConfig> for TvrLogAdapter {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TvrConfig>>, StorageError<TvrNodeId>> {
        use std::ops::Bound::*;
        let start = match range.start_bound() {
            Included(a) => *a,
            Excluded(a) => a + 1,
            Unbounded => 0,
        };
        let end = match range.end_bound() {
            Included(b) => *b,
            Excluded(b) => b - 1,
            Unbounded => u64::MAX,
        };

        trace!("try_get_log_entries {}..{}", start, end);

        if start > end {
            // OpenRaft accepts ranges like 3..3 which should return an empty list.
            return Ok(Vec::new());
        }

        let (rtx, rrx) = channel();
        self.tx
            .send(Op::Read(start, end, rtx))
            .map_err(|e| map_send_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        let buf = recv_map(rrx, ErrorSubject::Logs, ErrorVerb::Read)?;
        Ok(decode_entries(&self.part, &buf, start..=end))
    }
}

impl RaftLogStorage<TvrConfig> for TvrLogAdapter {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TvrConfig>, StorageError<TvrNodeId>> {
        let (rtx, rrx) = channel();
        self.tx
            .send(Op::GetState(rtx))
            .map_err(|e| map_send_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        recv_map(rrx, ErrorSubject::Logs, ErrorVerb::Read)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<TvrNodeId>) -> Result<(), StorageError<TvrNodeId>> {
        let (rtx, rrx) = channel();
        self.tx
            .send(Op::SaveVote(vote.clone(), rtx))
            .map_err(|e| map_send_err(ErrorSubject::Vote, ErrorVerb::Write, e))?;
        recv_unit(rrx, ErrorSubject::Vote, ErrorVerb::Write)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<TvrNodeId>>, StorageError<TvrNodeId>> {
        let (rtx, rrx) = channel();
        self.tx
            .send(Op::ReadVote(rtx))
            .map_err(|e| map_send_err(ErrorSubject::Vote, ErrorVerb::Read, e))?;
        recv_map(rrx, ErrorSubject::Vote, ErrorVerb::Read)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TvrConfig>,
    ) -> Result<(), StorageError<TvrNodeId>>
    where
        I: IntoIterator<Item = Entry<TvrConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut list: Vec<(u64, Vec<u8>)> = Vec::new();
        for e in entries.into_iter() {
            trace!("append {}: {:?}", self.part.short_id(), e);
            let idx = e.log_id.index;
            let buf = encode_entry(&e);
            list.push((idx, buf));
        }
        self.tx
            .send(Op::Append(list, callback))
            .map_err(|e| map_send_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<TvrNodeId>) -> Result<(), StorageError<TvrNodeId>> {
        let (rtx, rrx) = channel();
        self.tx
            .send(Op::Truncate(log_id.index, rtx))
            .map_err(|e| map_send_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        recv_unit(rrx, ErrorSubject::Logs, ErrorVerb::Write)
    }

    async fn purge(&mut self, log_id: LogId<TvrNodeId>) -> Result<(), StorageError<TvrNodeId>> {
        let (rtx, rrx) = channel();
        self.tx
            .send(Op::Purge(log_id, rtx))
            .map_err(|e| map_send_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
        recv_unit(rrx, ErrorSubject::Store, ErrorVerb::Write)
    }
}