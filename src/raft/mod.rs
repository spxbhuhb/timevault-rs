use std::io::Cursor;
use std::sync::mpsc::{Receiver, Sender, SyncSender, channel, sync_channel};
use std::thread;

use openraft::TokioRuntime;
use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState,
    RaftLogReader, StorageError, StorageIOError, Vote,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::PartitionHandle;
use crate::partition::read::read_range_blocks;
use crate::store::paths;

// TypeConfig
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request(pub serde_json::Value);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response(pub Result<serde_json::Value, serde_json::Value>);

openraft::declare_raft_types!(pub TvConfig:
    D            = Request,
    R            = Response,
    NodeId       = Uuid,
    Node         = BasicNode,
    Entry        = Entry<TvConfig>,
    SnapshotData = Cursor<Vec<u8>>,
    AsyncRuntime = TokioRuntime,
);

#[derive(Serialize, Deserialize)]
struct JsonlRecord {
    timestamp: i64,
    log_id: LogId<Uuid>,
    kind: String,
    payload: serde_json::Value,
}

pub struct TvrLogAdapter {
    part: PartitionHandle,
    node_id: Uuid,
    tx: SyncSender<Op>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

#[derive(Debug)]
enum TvRaftError {
    Tv(crate::errors::TvError)
}

enum Op {
    // list of (order_key=index, encoded)
    Append(Vec<(u64, Vec<u8>)>, LogFlushed<TvConfig>),
    Read(u64, u64, Sender<Result<Vec<u8>, TvRaftError>>),
    Truncate(u64, Sender<Result<(), TvRaftError>>),
    Purge(LogId<Uuid>, Sender<Result<(), TvRaftError>>),
    GetState(Sender<Result<LogState<TvConfig>, TvRaftError>>),
    SaveVote(Vote<Uuid>, Sender<Result<(), TvRaftError>>),
    ReadVote(Sender<Result<Option<Vote<Uuid>>, TvRaftError>>),
    Shutdown,
}

impl TvrLogAdapter {
    
    pub fn new(part: PartitionHandle, node_id: Uuid) -> Self {
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
                    let mut first_err: Option<crate::errors::TvError> = None;
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
                    let r = read_range_blocks(&part, a, b).map_err(TvRaftError::Tv);
                    tx.send(r).expect("bg_loop: failed to send Read response");
                }
                Op::Truncate(idx, tx) => {
                    let r = part.truncate(idx).map_err(TvRaftError::Tv);
                    tx.send(r).expect("bg_loop: failed to send Truncate response");
                }
                Op::Purge(id, tx) => {
                    let r = purge_with_persist(&part, &id).map_err(TvRaftError::Tv);
                    tx.send(r).expect("bg_loop: failed to send Purge response");
                }
                Op::GetState(tx) => {
                    let r = get_state(&part).map_err(TvRaftError::Tv);
                    tx.send(r).expect("bg_loop: failed to send GetState response");
                }
                Op::SaveVote(v, tx) => {
                    let r = save_vote_file(&part, &v).map_err(TvRaftError::Tv);
                    tx.send(r).expect("bg_loop: failed to send SaveVote response");
                }
                Op::ReadVote(tx) => {
                    let r = read_vote_file(&part).map_err(TvRaftError::Tv);
                    tx.send(r).expect("bg_loop: failed to send ReadVote response");
                }
                Op::Shutdown => break,
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

// ---------- Error mapping helpers ----------
fn se_new(subject: ErrorSubject<Uuid>, verb: ErrorVerb, msg: &str) -> StorageError<Uuid> {
    StorageError::from(StorageIOError::new(
        subject,
        verb,
        AnyError::new(&std::io::Error::new(
            std::io::ErrorKind::Other,
            msg.to_string(),
        )),
    ))
}

fn map_send_err<E: std::fmt::Display>(
    subject: ErrorSubject<Uuid>,
    verb: ErrorVerb,
    e: E,
) -> StorageError<Uuid> {
    se_new(subject, verb, &e.to_string())
}

fn map_tvraft_err(
    e: TvRaftError,
    subject: ErrorSubject<Uuid>,
    verb: ErrorVerb,
) -> StorageError<Uuid> {
    match e {
        TvRaftError::Tv(te) => {
            StorageError::from(StorageIOError::new(subject, verb, AnyError::new(&te)))
        }
    }
}

fn recv_map<T>(
    rrx: Receiver<Result<T, TvRaftError>>,
    subject: ErrorSubject<Uuid>,
    verb: ErrorVerb,
) -> Result<T, StorageError<Uuid>> {
    let rsp = rrx
        .recv()
        .map_err(|e| map_send_err(subject.clone(), verb.clone(), e))?;
    rsp.map_err(|e| map_tvraft_err(e, subject, verb))
}

fn recv_unit(
    rrx: Receiver<Result<(), TvRaftError>>,
    subject: ErrorSubject<Uuid>,
    verb: ErrorVerb,
) -> Result<(), StorageError<Uuid>> {
    recv_map(rrx, subject, verb)
}

fn encode_entry(e: &Entry<TvConfig>) -> Vec<u8> {
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

fn decode_entries(buf: &[u8], range: std::ops::RangeInclusive<u64>) -> Vec<Entry<TvConfig>> {
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
                _ => EntryPayload::Normal(Request(rec.payload)),
            };
            out.push(Entry { log_id: rec.log_id, payload });
        }
    }
    out
}

fn get_state(part: &PartitionHandle) -> Result<LogState<TvConfig>, crate::errors::TvError> {
    // Recover last_purge
    let purge = read_purge_file(part).ok().flatten();
    // Read last record bytes from runtime cache if any
    let rt = part.stats();
    let _ = rt; // placeholder
    // Read last block
    let last = part.read_range(u64::MAX - 1, u64::MAX)?; // hack: get tail
    let entries = decode_entries(&last, 0..=u64::MAX);
    let last_log_id = entries.last().map(|e| e.log_id);
    Ok(LogState {
        last_purged_log_id: purge,
        last_log_id,
    })
}

fn purge_with_persist(
    part: &PartitionHandle,
    id: &LogId<Uuid>,
) -> Result<(), crate::errors::TvError> {
    save_purge_file(part, id)?;
    part.purge(id.index)
}

fn save_vote_file(part: &PartitionHandle, v: &Vote<Uuid>) -> Result<(), crate::errors::TvError> {
    let part_dir = paths::partition_dir(part.root(), part.id());
    crate::disk::atomic::atomic_write_json(paths::raft_vote_file(&part_dir), v)
}

fn read_vote_file(part: &PartitionHandle) -> Result<Option<Vote<Uuid>>, crate::errors::TvError> {
    let part_dir = paths::partition_dir(part.root(), part.id());
    let p = paths::raft_vote_file(&part_dir);
    if !p.exists() {
        return Ok(None);
    }
    let f = std::fs::File::open(p)?;
    Ok(Some(serde_json::from_reader(f)?))
}

#[derive(Serialize, Deserialize)]
struct PurgeFile {
    last_deleted: LogId<Uuid>,
}

fn save_purge_file(part: &PartitionHandle, id: &LogId<Uuid>) -> Result<(), crate::errors::TvError> {
    let part_dir = paths::partition_dir(part.root(), part.id());
    crate::disk::atomic::atomic_write_json(
        paths::raft_purge_file(&part_dir),
        &PurgeFile {
            last_deleted: id.clone(),
        },
    )
}

fn read_purge_file(part: &PartitionHandle) -> Result<Option<LogId<Uuid>>, crate::errors::TvError> {
    let part_dir = paths::partition_dir(part.root(), part.id());
    let p = paths::raft_purge_file(&part_dir);
    if !p.exists() {
        return Ok(None);
    }
    let f = std::fs::File::open(p)?;
    let v: PurgeFile = serde_json::from_reader(f)?;
    Ok(Some(v.last_deleted))
}

impl RaftLogReader<TvConfig> for TvrLogAdapter {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TvConfig>>, StorageError<Uuid>> {
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
        let (rtx, rrx) = channel();
        self.tx
            .send(Op::Read(start, end, rtx))
            .map_err(|e| map_send_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        let buf = recv_map(rrx, ErrorSubject::Logs, ErrorVerb::Read)?;
        Ok(decode_entries(&buf, start..=end))
    }
}

impl RaftLogStorage<TvConfig> for TvrLogAdapter {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TvConfig>, StorageError<Uuid>> {
        let (rtx, rrx) = channel();
        self.tx
            .send(Op::GetState(rtx))
            .map_err(|e| map_send_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        recv_map(rrx, ErrorSubject::Logs, ErrorVerb::Read)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<Uuid>) -> Result<(), StorageError<Uuid>> {
        let (rtx, rrx) = channel();
        self.tx
            .send(Op::SaveVote(vote.clone(), rtx))
            .map_err(|e| map_send_err(ErrorSubject::Vote, ErrorVerb::Write, e))?;
        recv_unit(rrx, ErrorSubject::Vote, ErrorVerb::Write)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<Uuid>>, StorageError<Uuid>> {
        let (rtx, rrx) = channel();
        self.tx
            .send(Op::ReadVote(rtx))
            .map_err(|e| map_send_err(ErrorSubject::Vote, ErrorVerb::Read, e))?;
        recv_map(rrx, ErrorSubject::Vote, ErrorVerb::Read)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TvConfig>,
    ) -> Result<(), StorageError<Uuid>>
    where
        I: IntoIterator<Item = Entry<TvConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut list: Vec<(u64, Vec<u8>)> = Vec::new();
        for e in entries.into_iter() {
            let idx = e.log_id.index;
            let buf = encode_entry(&e);
            list.push((idx, buf));
        }
        self.tx
            .send(Op::Append(list, callback))
            .map_err(|e| map_send_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<Uuid>) -> Result<(), StorageError<Uuid>> {
        let (rtx, rrx) = channel();
        self.tx
            .send(Op::Truncate(log_id.index, rtx))
            .map_err(|e| map_send_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        recv_unit(rrx, ErrorSubject::Logs, ErrorVerb::Write)
    }

    async fn purge(&mut self, log_id: LogId<Uuid>) -> Result<(), StorageError<Uuid>> {
        let (rtx, rrx) = channel();
        self.tx
            .send(Op::Purge(log_id, rtx))
            .map_err(|e| map_send_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
        recv_unit(rrx, ErrorSubject::Store, ErrorVerb::Write)
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


// Test helpers exposed for integration tests
pub fn decode_entries_for_tests(buf: &[u8], range: std::ops::RangeInclusive<u64>) -> Vec<Entry<TvConfig>> {
    decode_entries(buf, range)
}

pub fn read_purge_file_for_tests(part: &PartitionHandle) -> Result<Option<LogId<Uuid>>, crate::errors::TvError> {
    read_purge_file(part)
}
