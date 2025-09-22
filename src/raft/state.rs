use std::fmt::Debug;
use std::io::Cursor;
use std::marker::PhantomData;

use crate::PartitionHandle;
use crate::disk::atomic::atomic_write_json;
use crate::raft::errors::se_new;
use crate::raft::{StorageResult, TvrConfig, TvrNode, TvrNodeId, paths};
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StoredMembership;
use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use openraft::{Entry, EntryPayload};
use serde::Deserialize;
use serde::Serialize;

pub type SnapshotData = Cursor<Vec<u8>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<TvrNodeId, TvrNode>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/// Stores all requests in a timevault partition.
#[derive(Debug)]
pub struct TvrPartitionStateMachine<D, R> {
    pub partition_handle: PartitionHandle,

    pub data: StateMachineData,

    /// snapshot index is not persisted in this example.
    ///
    /// It is only used as a suffix of snapshot id, and should be globally unique.
    /// In practice, using a timestamp in micro-second would be good enough.
    snapshot_idx: u64,

    _marker: PhantomData<(D, R)>,
}

impl<D, R> Clone for TvrPartitionStateMachine<D, R> {
    fn clone(&self) -> Self {
        Self {
            partition_handle: self.partition_handle.clone(),
            data: self.data.clone(),
            snapshot_idx: self.snapshot_idx,
            _marker: PhantomData,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StateMachineData {
    pub last_applied_log_id: Option<LogId<TvrNodeId>>,

    pub last_membership: StoredMembership<TvrNodeId, TvrNode>,
    // State built from applying the raft logs
}

impl<D, R> RaftSnapshotBuilder<TvrConfig<D, R>> for TvrPartitionStateMachine<D, R>
where
    D: openraft::AppData,
    R: openraft::AppDataResponse,
{
    async fn build_snapshot(&mut self) -> Result<Snapshot<TvrConfig<D, R>>, StorageError<TvrNodeId>> {
        let last_applied_log = self.data.last_applied_log_id;
        let last_membership = self.data.last_membership.clone();

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, self.snapshot_idx)
        } else {
            format!("--{}", self.snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let data = vec![];

        let snapshot = StoredSnapshot { meta: meta.clone(), data: data.clone() };

        self.set_current_snapshot_(snapshot)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl<D, R> TvrPartitionStateMachine<D, R>
where
    D: openraft::AppData,
    R: openraft::AppDataResponse,
{
    pub fn new(partition_handle: PartitionHandle) -> Result<TvrPartitionStateMachine<D, R>, StorageError<TvrNodeId>> {
        let mut sm = Self {
            partition_handle,
            data: StateMachineData {
                last_applied_log_id: None,
                last_membership: Default::default(),
            },
            snapshot_idx: 0,
            _marker: PhantomData,
        };

        let snapshot = sm.get_current_snapshot_()?;
        if let Some(snap) = snapshot {
            sm.update_state_machine_(snap)?;
        }

        Ok(sm)
    }

    fn update_state_machine_(&mut self, snapshot: StoredSnapshot) -> Result<(), StorageError<TvrNodeId>> {
        self.data.last_applied_log_id = snapshot.meta.last_log_id;
        self.data.last_membership = snapshot.meta.last_membership.clone();

        // update state machine, take care of SYNCHRONIZATION

        Ok(())
    }

    fn get_current_snapshot_(&self) -> StorageResult<Option<StoredSnapshot>> {
        let p = paths::state_file(&self.partition_handle);
        match std::fs::File::open(&p) {
            Ok(f) => {
                let r: Result<Option<StoredSnapshot>, _> = serde_json::from_reader(f);
                r.map_err(|e| se_new(ErrorSubject::Store, ErrorVerb::Read, &e.to_string()))
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(None)
                } else {
                    Err(se_new(ErrorSubject::Store, ErrorVerb::Read, &e.to_string()))
                }
            }
        }
    }

    fn set_current_snapshot_(&self, snap: StoredSnapshot) -> StorageResult<()> {
        atomic_write_json(paths::state_file(&self.partition_handle), &snap).map_err(|e| se_new(ErrorSubject::Store, ErrorVerb::Write, &e.to_string()))
    }
}

impl<D, R> RaftStateMachine<TvrConfig<D, R>> for TvrPartitionStateMachine<D, R>
where
    D: openraft::AppData,
    R: openraft::AppDataResponse + Default,
{
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId<TvrNodeId>>, StoredMembership<TvrNodeId, TvrNode>), StorageError<TvrNodeId>> {
        Ok((self.data.last_applied_log_id, self.data.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<R>, StorageError<TvrNodeId>>
    where
        I: IntoIterator<Item = Entry<TvrConfig<D, R>>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter();
        let mut replies = Vec::with_capacity(entries.size_hint().0);

        for ent in entries {
            self.data.last_applied_log_id = Some(ent.log_id);

            //let mut resp_value = None;

            match ent.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(_req) => {} // match req {
                // TvrRequest::Set { key, value } => {
                //     resp_value = Some(value.clone());
                //
                //     let mut st = self.data.kvs.write().await;
                //     st.insert(key, value);
                // }
                // },
                EntryPayload::Membership(mem) => {
                    self.data.last_membership = StoredMembership::new(Some(ent.log_id), mem);
                }
            }

            replies.push(R::default());
        }

        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.snapshot_idx += 1;
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<TvrNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(&mut self, meta: &SnapshotMeta<TvrNodeId, TvrNode>, snapshot: Box<SnapshotData>) -> Result<(), StorageError<TvrNodeId>> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        self.update_state_machine_(new_snapshot.clone())?;

        self.set_current_snapshot_(new_snapshot)?;

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TvrConfig<D, R>>>, StorageError<TvrNodeId>> {
        let x = self.get_current_snapshot_()?;
        Ok(x.map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}
