use std::fmt::Debug;
use std::io::Cursor;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::mpsc::{Receiver, Sender, SyncSender, channel, sync_channel};
use std::thread;

use crate::PartitionHandle;
use crate::disk::atomic::atomic_write_json;
use crate::errors::TvError;
use crate::partition::read::read_range_blocks;
use crate::raft::errors::{map_send_err, recv_map, recv_unit};
use crate::raft::paths;
use crate::raft::{TvrConfig, TvrNodeId};
use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend, RaftLogReader, StorageError, Vote};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tracing::{trace, warn};

#[derive(Serialize, Deserialize, Clone)]
struct JsonlRecord {
    timestamp: i64,
    log_id: LogId<TvrNodeId>,
    kind: String,
    payload: serde_json::Value,
}

enum Op<D, R>
where
    D: serde::Serialize + DeserializeOwned + openraft::AppData,
    R: openraft::AppDataResponse,
{
    // list of (order_key=index, encoded)
    Append(Vec<(u64, Vec<u8>)>, LogFlushed<TvrConfig<D, R>>),
    Read(u64, u64, Sender<Result<Vec<u8>, TvError>>),
    Truncate(u64, Sender<Result<(), TvError>>),
    Purge(LogId<TvrNodeId>, Sender<Result<(), TvError>>),
    GetState(Sender<Result<LogState<TvrConfig<D, R>>, TvError>>),
    SaveVote(Vote<TvrNodeId>, Sender<Result<(), TvError>>),
    ReadVote(Sender<Result<Option<Vote<TvrNodeId>>, TvError>>),
    Shutdown,
}

pub struct TvrLogReader<D, R>
where
    D: serde::Serialize + DeserializeOwned + openraft::AppData,
    R: openraft::AppDataResponse,
{
    part: PartitionHandle,
    tx: SyncSender<Op<D, R>>,
    _marker: PhantomData<(D, R)>,
}

impl<D, R> TvrLogReader<D, R>
where
    D: serde::Serialize + DeserializeOwned + openraft::AppData,
    R: openraft::AppDataResponse,
{
    fn new(part: PartitionHandle, tx: SyncSender<Op<D, R>>) -> Self {
        Self { part, tx, _marker: PhantomData }
    }
}

impl<D, R> Clone for TvrLogReader<D, R>
where
    D: serde::Serialize + DeserializeOwned + openraft::AppData,
    R: openraft::AppDataResponse,
{
    fn clone(&self) -> Self {
        Self {
            part: self.part.clone(),
            tx: self.tx.clone(),
            _marker: PhantomData,
        }
    }
}

pub struct TvrLogAdapter<D, R>
where
    D: serde::Serialize + DeserializeOwned + openraft::AppData,
    R: openraft::AppDataResponse,
{
    part: PartitionHandle,
    node_id: TvrNodeId,
    tx: SyncSender<Op<D, R>>,
    thread_handle: Option<thread::JoinHandle<()>>,
    _marker: PhantomData<(D, R)>,
}

impl<D, R> TvrLogAdapter<D, R>
where
    D: serde::Serialize + DeserializeOwned + openraft::AppData,
    R: openraft::AppDataResponse,
{
    pub fn new(part: PartitionHandle, node_id: TvrNodeId) -> Self {
        trace!("new {} {}", part.short_id(), node_id);
        let cfg = part.cfg();
        if cfg.format_plugin != "jsonl" {
            panic!("Only jsonl format is supported for now.");
        }
        let (tx, rx) = sync_channel::<Op<D, R>>(1024);
        let hpart = part.clone();
        let handle = thread::spawn(move || Self::bg_loop(hpart, rx));
        Self {
            part,
            node_id,
            tx,
            thread_handle: Some(handle),
            _marker: PhantomData,
        }
    }

    fn bg_loop(part: PartitionHandle, rx: Receiver<Op<D, R>>) {
        trace!("bg_loop {}: start", part.short_id());

        loop {
            match rx.recv() {
                Ok(op) => match op {
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
                            let _ = cb.log_io_completed(Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())));
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
                        let r = get_state::<D, R>(&part);
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
                        break;
                    }
                },
                Err(e) => {
                    warn!("bg_loop {}: channel closed unexpectedly: {}", part.short_id(), e);
                    break;
                }
            }
        }

        trace!("bg_loop {}: exit", part.short_id());
    }
}

impl<D, R> Drop for TvrLogAdapter<D, R>
where
    D: serde::Serialize + DeserializeOwned + openraft::AppData,
    R: openraft::AppDataResponse,
{
    fn drop(&mut self) {
        let _ = self.tx.send(Op::Shutdown);
        if let Some(h) = self.thread_handle.take() {
           let _ = h.join();
        }
    }
}

impl<D, R> Clone for TvrLogAdapter<D, R>
where
    D: serde::Serialize + DeserializeOwned + openraft::AppData,
    R: openraft::AppDataResponse,
{
    fn clone(&self) -> Self {
        Self {
            part: self.part.clone(),
            node_id: self.node_id.clone(),
            tx: self.tx.clone(),
            thread_handle: None,
            _marker: PhantomData,
        }
    }
}

pub(crate) fn encode_entry<D, R>(e: &Entry<TvrConfig<D, R>>) -> Vec<u8>
where
    D: serde::Serialize + DeserializeOwned + openraft::AppData,
    R: openraft::AppDataResponse,
{
    let (kind, val) = match &e.payload {
        EntryPayload::Blank => ("Blank".to_string(), serde_json::Value::Null),
        EntryPayload::Normal(r) => ("Normal".to_string(), serde_json::to_value(r).unwrap_or(serde_json::Value::Null)),
        EntryPayload::Membership(m) => ("Membership".to_string(), serde_json::to_value(m).unwrap_or(serde_json::Value::Null)),
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

pub(crate) fn decode_entries<D, R>(part: &PartitionHandle, buf: &[u8], range: std::ops::RangeInclusive<u64>) -> Vec<Entry<TvrConfig<D, R>>>
where
    D: serde::Serialize + DeserializeOwned + openraft::AppData,
    R: openraft::AppDataResponse,
{
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
        if end > buf.len() {
            break;
        }
        let line = &buf[start..end];
        if let Ok(rec) = serde_json::from_slice::<JsonlRecord>(line) {
            if !range.contains(&rec.log_id.index) {
                continue;
            }
            let payload = match rec.kind.as_str() {
                "Blank" => EntryPayload::Blank,
                "Membership" => match serde_json::from_value(rec.payload) {
                    Ok(m) => EntryPayload::Membership(m),
                    Err(_) => EntryPayload::Blank,
                },
                _ => match serde_json::from_value(rec.payload) {
                    Ok(v) => EntryPayload::Normal(v),
                    Err(_) => EntryPayload::Blank,
                },
            };
            trace!("decode_entries {}: {} {:?}", part.short_id(), rec.log_id, &payload);
            out.push(Entry { log_id: rec.log_id, payload });
        }
    }
    out
}

pub(crate) fn get_state<D, R>(part: &PartitionHandle) -> Result<LogState<TvrConfig<D, R>>, TvError>
where
    D: serde::Serialize + DeserializeOwned + openraft::AppData,
    R: openraft::AppDataResponse,
{
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

    Ok(LogState { last_purged_log_id: purge, last_log_id })
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
    atomic_write_json(paths::purge_file(&part), &PurgeFile { last_deleted: id.clone() })
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

impl<D, R> RaftLogReader<TvrConfig<D, R>> for TvrLogReader<D, R>
where
    D: serde::Serialize + DeserializeOwned + openraft::AppData,
    R: openraft::AppDataResponse,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TvrConfig<D, R>>>, StorageError<TvrNodeId>> {
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
        self.tx.send(Op::Read(start, end, rtx)).map_err(|e| map_send_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        let buf = recv_map(rrx, ErrorSubject::Logs, ErrorVerb::Read)?;
        Ok(decode_entries(&self.part, &buf, start..=end))
    }
}

impl<D, R> RaftLogReader<TvrConfig<D, R>> for TvrLogAdapter<D, R>
where
    D: DeserializeOwned + openraft::AppData + serde::Serialize,
    R: openraft::AppDataResponse,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(&mut self, range: RB) -> Result<Vec<Entry<TvrConfig<D, R>>>, StorageError<TvrNodeId>> {
        self.get_log_reader().await.try_get_log_entries(range).await // FIXME I'm not sure about this line, just added await until it started to work
    }
}

impl<D, R> RaftLogStorage<TvrConfig<D, R>> for TvrLogAdapter<D, R>
where
    D: serde::Serialize + DeserializeOwned + openraft::AppData,
    R: openraft::AppDataResponse,
{
    type LogReader = TvrLogReader<D, R>;

    async fn get_log_state(&mut self) -> Result<LogState<TvrConfig<D, R>>, StorageError<TvrNodeId>> {
        let (rtx, rrx) = channel();
        self.tx.send(Op::GetState(rtx)).map_err(|e| map_send_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        recv_map(rrx, ErrorSubject::Logs, ErrorVerb::Read)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        TvrLogReader::new(self.part.clone(), self.tx.clone())
    }

    async fn save_vote(&mut self, vote: &Vote<TvrNodeId>) -> Result<(), StorageError<TvrNodeId>> {
        let (rtx, rrx) = channel();
        self.tx.send(Op::SaveVote(vote.clone(), rtx)).map_err(|e| map_send_err(ErrorSubject::Vote, ErrorVerb::Write, e))?;
        recv_unit(rrx, ErrorSubject::Vote, ErrorVerb::Write)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<TvrNodeId>>, StorageError<TvrNodeId>> {
        let (rtx, rrx) = channel();
        self.tx.send(Op::ReadVote(rtx)).map_err(|e| map_send_err(ErrorSubject::Vote, ErrorVerb::Read, e))?;
        recv_map(rrx, ErrorSubject::Vote, ErrorVerb::Read)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TvrConfig<D, R>>) -> Result<(), StorageError<TvrNodeId>>
    where
        I: IntoIterator<Item = Entry<TvrConfig<D, R>>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut list: Vec<(u64, Vec<u8>)> = Vec::new();
        for e in entries.into_iter() {
            trace!("append {}: {:?}", self.part.short_id(), e);
            let idx = e.log_id.index;
            let buf = encode_entry(&e);
            list.push((idx, buf));
        }
        self.tx.send(Op::Append(list, callback)).map_err(|e| map_send_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<TvrNodeId>) -> Result<(), StorageError<TvrNodeId>> {
        let (rtx, rrx) = channel();
        self.tx.send(Op::Truncate(log_id.index, rtx)).map_err(|e| map_send_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        recv_unit(rrx, ErrorSubject::Logs, ErrorVerb::Write)
    }

    async fn purge(&mut self, log_id: LogId<TvrNodeId>) -> Result<(), StorageError<TvrNodeId>> {
        let (rtx, rrx) = channel();
        self.tx.send(Op::Purge(log_id, rtx)).map_err(|e| map_send_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
        recv_unit(rrx, ErrorSubject::Store, ErrorVerb::Write)
    }
}
