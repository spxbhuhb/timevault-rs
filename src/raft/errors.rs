use openraft::{AnyError, ErrorSubject, ErrorVerb, StorageError, StorageIOError};
use std::sync::mpsc::Receiver;

use crate::errors::TvError;
use crate::raft::TvrNodeId;

pub(crate) fn se_new(subject: ErrorSubject<TvrNodeId>, verb: ErrorVerb, msg: &str) -> StorageError<TvrNodeId> {
    StorageError::from(StorageIOError::new(
        subject,
        verb,
        AnyError::new(&std::io::Error::new(
            std::io::ErrorKind::Other,
            msg.to_string(),
        )),
    ))
}

pub(crate) fn map_send_err<E: std::fmt::Display>(
    subject: ErrorSubject<TvrNodeId>,
    verb: ErrorVerb,
    e: E,
) -> StorageError<TvrNodeId> {
    se_new(subject, verb, &e.to_string())
}

pub(crate) fn map_tv_err(
    e: TvError,
    subject: ErrorSubject<TvrNodeId>,
    verb: ErrorVerb,
) -> StorageError<TvrNodeId> {
    StorageError::from(StorageIOError::new(subject, verb, AnyError::new(&e)))
}

pub(crate) fn recv_map<T>(
    rrx: Receiver<Result<T, TvError>>,
    subject: ErrorSubject<TvrNodeId>,
    verb: ErrorVerb,
) -> Result<T, StorageError<TvrNodeId>> {
    let rsp = rrx
        .recv()
        .map_err(|e| map_send_err(subject.clone(), verb.clone(), e))?;
    rsp.map_err(|e| map_tv_err(e, subject, verb))
}

pub(crate) fn recv_unit(
    rrx: Receiver<Result<(), TvError>>,
    subject: ErrorSubject<TvrNodeId>,
    verb: ErrorVerb,
) -> Result<(), StorageError<TvrNodeId>> {
    recv_map(rrx, subject, verb)
}
