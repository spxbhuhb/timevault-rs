use std::io::Cursor;
use openraft::{BasicNode, Entry, StorageError, TokioRuntime};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

mod storage;
mod state;
mod app;
mod paths;
mod errors;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TvrRequest(pub serde_json::Value);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TvrResponse(pub Result<serde_json::Value, serde_json::Value>);

openraft::declare_raft_types!(pub TvrConfig:
    D            = TvrRequest,
    R            = TvrResponse,
    NodeId       = Uuid,
    Node         = BasicNode,
    Entry        = Entry<TvrConfig>,
    SnapshotData = Cursor<Vec<u8>>,
    AsyncRuntime = TokioRuntime,
);

pub type TvRaft = openraft::Raft<TvrConfig>;
pub type TvrEntry = Entry<TvrConfig>;

pub type TvrNodeId = Uuid;
pub type TvrNode = BasicNode;

type StorageResult<T> = Result<T, StorageError<TvrNodeId>>;
