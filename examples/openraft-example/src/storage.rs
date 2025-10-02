use std::path::Path;

use timevault::store::partition::PartitionConfig;
use timevault::store::{Store, StoreConfig};
use uuid::Uuid;

use crate::raft::AppLogStore;
use crate::TvrNodeId;

pub fn open_or_create_log_store(root: &Path, node_id: TvrNodeId) -> anyhow::Result<AppLogStore> {
    let namespace = Uuid::from_u128(0x9e2e_83b0_b6c2_4c9b_9b3b_4f1e_3a5c_7d11);
    let log_part_id = Uuid::new_v5(&namespace, format!("raft-log:{node_id}").as_bytes());
    let mut cfg = PartitionConfig::default();
    cfg.format_plugin = "jsonl".to_string();

    let log_part_dir = timevault::store::paths::partition_dir(root, log_part_id);
    let log_part = if log_part_dir.exists() { timevault::PartitionHandle::open(root.to_path_buf(), log_part_id)? } else { timevault::PartitionHandle::create(root.to_path_buf(), log_part_id, cfg.clone())? };

    Ok(AppLogStore::new(log_part, node_id))
}

pub fn open_state_store(root: &Path) -> anyhow::Result<Store> {
    let store = Store::open(root, StoreConfig { read_only: false })?;
    Ok(store)
}

