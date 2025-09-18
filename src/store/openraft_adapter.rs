#![cfg(feature = "openraft-adapter")]

use std::path::PathBuf;
use std::sync::mpsc::{self, Sender, Receiver};
use std::thread::{self, JoinHandle};

use async_trait::async_trait;
use openraft::storage::{LogState, RaftLogReader, RaftLogStorage};
use openraft::StorageError;
use openraft::{Entry, LogId, RaftTypeConfig, Vote};
use serde::{Deserialize, Serialize};

use crate::errors::{Result, TvError};
use crate::partition::PartitionHandle;
use crate::plugins::{FormatPlugin};
use crate::store::paths;

pub struct TvRaftLog<C: RaftTypeConfig> {
    part: PartitionHandle,
    plugin: std::sync::Arc<dyn FormatPlugin>,
    tx: Sender<Op<C>>, // single queue: serialize ops
    _bg: JoinHandle<()>,
}

pub struct TvRaftLogReader<C: RaftTypeConfig> {
    part: PartitionHandle,
    plugin: std::sync::Arc<dyn FormatPlugin>,
    tx: Sender<Op<C>>,
}

#[derive(Clone)]
struct Cfg { part_dir: PathBuf }

#[derive(Debug, Serialize, Deserialize)]
struct PersistedPurge<NodeId> { last_deleted: LogId<NodeId> }

#[derive(Debug)]
enum Op<C: RaftTypeConfig> {
    Append(Vec<Entry<C>>, openraft::storage::callback::LogFlushed<C>),
    Truncate(u64, mpsc::Sender<Result<()>>),
    Purge(LogId<C::NodeId>, mpsc::Sender<Result<()>>),
    TryGet { range: std::ops::Range<u64>, tx: mpsc::Sender<Result<Vec<Entry<C>>>> },
    GetState(mpsc::Sender<Result<LogState<C>>>),
    SaveVote(Vote<C::NodeId>, mpsc::Sender<Result<()>>),
    ReadVote(mpsc::Sender<Result<Option<Vote<C::NodeId>>>>),
    Shutdown,
}

impl<C: RaftTypeConfig> TvRaftLog<C>
where C::NodeId: openraft::NodeId + Serialize + for<'de> Deserialize<'de> + Clone,
      C::Entry: Serialize + for<'de> Deserialize<'de>,
{
    pub fn new(part: PartitionHandle) -> Result<Self> {
        let plugin = crate::plugins::resolve_plugin("jsonl")?;
        let (tx, rx) = mpsc::channel();
        let cfg = Cfg { part_dir: paths::partition_dir(part.root(), part.id()) };
        let bg = Self::spawn_bg(part.clone(), plugin.clone(), cfg, rx);
        Ok(Self { part, plugin, tx, _bg: bg })
    }

    fn spawn_bg(part: PartitionHandle, plugin: std::sync::Arc<dyn FormatPlugin>, cfg: Cfg, rx: Receiver<Op<C>>) -> JoinHandle<()> {
        thread::spawn(move || {
            while let Ok(op) = rx.recv() {
                if let Op::Shutdown = op { break; }
                if let Err(e) = Self::handle_op(&part, &*plugin, &cfg, op) {
                    eprintln!("openraft adapter op error: {:?}", e);
                }
            }
        })
    }

    fn handle_op(part: &PartitionHandle, plugin: &dyn FormatPlugin, cfg: &Cfg, op: Op<C>) -> Result<()> {
        match op {
            Op::Append(entries, flushed) => { Self::do_append(part, plugin, entries)?; let _ = flushed.log_io_completed(Ok(())); }
            Op::Truncate(idx, tx) => Self::reply(tx, part.truncate(idx)),
            Op::Purge(id, tx) => Self::reply(tx, Self::do_purge(part, &cfg.part_dir, id)),
            Op::TryGet { range, tx } => Self::reply(tx, Self::do_try_get(part, plugin, range)),
            Op::GetState(tx) => Self::reply(tx, Self::do_get_state(part, plugin, &cfg.part_dir)),
            Op::SaveVote(v, tx) => Self::reply(tx, Self::persist_vote(&cfg.part_dir, &v)),
            Op::ReadVote(tx) => Self::reply(tx, Self::load_vote(&cfg.part_dir)),
            Op::Shutdown => {}
        }
        Ok(())
    }

    fn reply<T>(tx: mpsc::Sender<Result<T>>, r: Result<T>) { let _ = tx.send(r); }

    fn do_append(part: &PartitionHandle, plugin: &dyn FormatPlugin, entries: Vec<Entry<C>>) -> Result<()> {
        for ent in entries {
            let ts = ent.log_id.index as i64;
            let val = serde_json::to_value(&ent).map_err(|e| TvError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
            let buf = plugin.encode(ts, &val).map_err(TvError::Io)?;
            part.append(ent.log_id.index, &buf)?;
        }
        Ok(())
    }

    fn do_try_get(part: &PartitionHandle, plugin: &dyn FormatPlugin, range: std::ops::Range<u64>) -> Result<Vec<Entry<C>>> {
        let raw = part.read_range(range.start, range.end)?;
        Self::scan_decode(plugin, &raw, move |e: Entry<C>| {
            let idx = e.log_id.index;
            if idx >= range.start && idx < range.end { Some(e) } else { None }
        })
    }

    fn scan_decode<T, F: FnMut(T) -> Option<T>>(plugin: &dyn FormatPlugin, bytes: &[u8], mut f: F) -> Result<Vec<T>>
    where T: for<'de> Deserialize<'de> {
        use crate::plugins::ChunkScanner;
        use std::io::{Cursor, Seek};
        let mut cur = Cursor::new(bytes.to_vec());
        let mut sc = plugin.scanner(&mut cur).map_err(TvError::Io)?;
        sc.seek_to(0).map_err(TvError::Io)?;
        let mut out = Vec::new();
        while let Some(m) = sc.next().map_err(TvError::Io)? {
            let slice = &bytes[m.offset as usize..(m.offset + m.len as u64) as usize];
            let line = &slice[..slice.len()-1]; // strip newline
            let jl: serde_json::Value = serde_json::from_slice(line)?;
            let payload = &jl["payload"];
            let e: T = serde_json::from_value(payload.clone())?;
            if let Some(x) = f(e) { out.push(x); }
        }
        Ok(out)
    }

    fn do_get_state(part: &PartitionHandle, plugin: &dyn FormatPlugin, part_dir: &PathBuf) -> Result<LogState<C>> {
        let last_deleted = Self::load_purge_id::<C::NodeId>(part_dir)?;
        let last = Self::recover_last_log_id(part, plugin)?;
        let last_log_id = last.clone().or_else(|| last_deleted.clone());
        Ok(LogState { last_purged_log_id: last_deleted, last_log_id })
    }

    fn recover_last_log_id(part: &PartitionHandle, plugin: &dyn FormatPlugin) -> Result<Option<LogId<C::NodeId>>> {
        let rt = part.cfg(); // not runtime; need last record bytes from recovery cache
        let rtr = part; // no direct last bytes, read last range instead
        let max = part.stats().cur_chunk_max_order_key;
        if max == 0 { return Ok(None); }
        let raw = rtr.read_range(max, max)?; // read last block
        let mut out = Self::scan_decode::<Entry<C>, _>(plugin, &raw, |e| Some(e))?;
        Ok(out.pop().map(|e| e.log_id))
    }

    fn persist_vote(path: &PathBuf, v: &Vote<C::NodeId>) -> Result<()> {
        let p = paths::raft_vote_file(path);
        crate::disk::atomic::atomic_write_json(&p, v)
    }

    fn load_vote(path: &PathBuf) -> Result<Option<Vote<C::NodeId>>> {
        let p = paths::raft_vote_file(path);
        if !p.exists() { return Ok(None); }
        let s = std::fs::read_to_string(&p)?;
        Ok(Some(serde_json::from_str(&s)?))
    }

    fn do_purge(part: &PartitionHandle, part_dir: &PathBuf, id: LogId<C::NodeId>) -> Result<()> {
        let p = paths::raft_purge_file(part_dir);
        crate::disk::atomic::atomic_write_json(&p, &PersistedPurge { last_deleted: id.clone() })?;
        part.purge(id.index)?;
        Ok(())
    }

    fn load_purge_id<NodeId: for<'de> Deserialize<'de>>(part_dir: &PathBuf) -> Result<Option<LogId<NodeId>>> {
        let p = paths::raft_purge_file(part_dir);
        if !p.exists() { return Ok(None); }
        let s = std::fs::read_to_string(&p)?;
        let v: PersistedPurge<NodeId> = serde_json::from_str(&s)?;
        Ok(Some(v.last_deleted))
    }
}

#[async_trait]
impl<C> RaftLogStorage<C> for TvRaftLog<C>
where C: RaftTypeConfig,
      C::NodeId: openraft::NodeId + Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
      C::Entry: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    type LogReader = TvRaftLogReader<C>;

    async fn get_log_state(&mut self) -> std::result::Result<LogState<C>, StorageError<C::NodeId>> {
        let (tx, rx) = mpsc::channel();
        self.tx.send(Op::GetState(tx)).map_err(to_se)?;
        rx.recv().map_err(to_se)?.map_err(to_se)
    }

    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> std::result::Result<(), StorageError<C::NodeId>> {
        let (tx, rx) = mpsc::channel();
        self.tx.send(Op::SaveVote(vote.clone(), tx)).map_err(to_se)?;
        rx.recv().map_err(to_se)?.map_err(to_se)
    }

    async fn read_vote(&mut self) -> std::result::Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        let (tx, rx) = mpsc::channel();
        self.tx.send(Op::ReadVote(tx)).map_err(to_se)?;
        rx.recv().map_err(to_se)?.map_err(to_se)
    }

    async fn append<I>(&mut self, entries: I, cb: openraft::storage::callback::LogFlushed<C>) -> std::result::Result<(), StorageError<C::NodeId>>
    where I: IntoIterator<Item = Entry<C>> + Send {
        let vec: Vec<_> = entries.into_iter().collect();
        self.tx.send(Op::Append(vec, cb)).map_err(to_se)?;
        Ok(())
    }

    async fn truncate(&mut self, idx: u64) -> std::result::Result<(), StorageError<C::NodeId>> {
        let (tx, rx) = mpsc::channel();
        self.tx.send(Op::Truncate(idx, tx)).map_err(to_se)?;
        rx.recv().map_err(to_se)?.map_err(to_se)
    }

    async fn purge(&mut self, id: LogId<C::NodeId>) -> std::result::Result<(), StorageError<C::NodeId>> {
        let (tx, rx) = mpsc::channel();
        self.tx.send(Op::Purge(id, tx)).map_err(to_se)?;
        rx.recv().map_err(to_se)?.map_err(to_se)
    }

    async fn get_log_reader(&mut self) -> std::result::Result<Self::LogReader, StorageError<C::NodeId>> {
        Ok(TvRaftLogReader { part: self.part.clone(), plugin: self.plugin.clone(), tx: self.tx.clone() })
    }
}

#[async_trait]
impl<C> RaftLogReader<C> for TvRaftLogReader<C>
where C: RaftTypeConfig,
      C::NodeId: openraft::NodeId + Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
      C::Entry: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    async fn try_get_log_entries(&mut self, range: std::ops::Range<u64>) -> std::result::Result<Vec<Entry<C>>, StorageError<C::NodeId>> {
        let (tx, rx) = mpsc::channel();
        self.tx.send(Op::TryGet { range, tx }).map_err(to_se)?;
        rx.recv().map_err(to_se)?.map_err(to_se)
    }
}

fn to_se<N: std::fmt::Debug>(e: impl std::fmt::Debug) -> StorageError<N> {
    let io = std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e));
    StorageError::IO { source: io }
}
