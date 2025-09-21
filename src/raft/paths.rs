use std::path::{PathBuf};
use crate::PartitionHandle;
use crate::store::paths::partition_dir;

pub(crate) fn vote_file(ph: &PartitionHandle) -> PathBuf {
    partition_dir(ph.root(), ph.id()).join("raft_vote.json")
}

pub(crate) fn purge_file(ph: &PartitionHandle) -> PathBuf {
    partition_dir(ph.root(), ph.id()).join("raft_purge.json")
}

pub(crate) fn state_file(ph: &PartitionHandle) -> PathBuf{
    partition_dir(ph.root(), ph.id()).join("raft_state.json")
}