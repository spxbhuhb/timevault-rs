use crate::TvrNodeId;
use crate::Raft;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct App {
    pub id: TvrNodeId,
    pub addr: String,
    pub raft: Raft
}
