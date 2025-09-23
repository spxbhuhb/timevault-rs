use std::cmp::Ordering;
use std::io::Cursor;
use std::marker::PhantomData;

use openraft::{AppData, AppDataResponse, BasicNode, Entry, RaftTypeConfig, StorageError, TokioRuntime};

mod errors;
pub mod log;
mod paths;
pub mod state;

#[cfg(test)]
mod tests;

pub struct TvrConfig<D, R> {
    _marker: PhantomData<(D, R)>,
}

impl<D, R> Copy for TvrConfig<D, R> {}

impl<D, R> Clone for TvrConfig<D, R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<D, R> Default for TvrConfig<D, R> {
    fn default() -> Self {
        Self { _marker: PhantomData }
    }
}

impl<D, R> std::fmt::Debug for TvrConfig<D, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TvrConfig<_, _>")
    }
}

impl<D, R> PartialEq for TvrConfig<D, R> {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl<D, R> Eq for TvrConfig<D, R> {}

impl<D, R> PartialOrd for TvrConfig<D, R> {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        Some(Ordering::Equal)
    }
}

impl<D, R> Ord for TvrConfig<D, R> {
    fn cmp(&self, _other: &Self) -> Ordering {
        Ordering::Equal
    }
}

impl<D, R> RaftTypeConfig for TvrConfig<D, R>
where
    D: AppData,
    R: AppDataResponse,
{
    type D = D;
    type R = R;
    type NodeId = TvrNodeId;
    type Node = BasicNode;
    type Entry = Entry<Self>;
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<Self>;
}

pub type TvRaft<C> = openraft::Raft<C>;
pub type TvrEntry<C> = Entry<C>;

pub type TvrNodeId = u64;

pub type TvrNode = BasicNode;

type StorageResult<T> = Result<T, StorageError<TvrNodeId>>;