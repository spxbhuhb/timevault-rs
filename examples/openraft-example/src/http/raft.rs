use actix_web::web::{Data, Json};
use actix_web::{Responder, post};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};

use crate::app::App;
use crate::raft::AppConfig;
use crate::TvrNodeId;

#[post("/raft-vote")]
pub async fn vote(app: Data<App>, req: Json<VoteRequest<TvrNodeId>>) -> actix_web::Result<impl Responder> {
    let res = app.raft.vote(req.0).await;
    Ok(Json(res))
}

#[post("/raft-append")]
pub async fn append(app: Data<App>, req: Json<AppendEntriesRequest<AppConfig>>) -> actix_web::Result<impl Responder> {
    let res = app.raft.append_entries(req.0).await;
    Ok(Json(res))
}

#[post("/raft-snapshot")]
pub async fn snapshot(app: Data<App>, req: Json<InstallSnapshotRequest<AppConfig>>) -> actix_web::Result<impl Responder> {
    let res = app.raft.install_snapshot(req.0).await;
    Ok(Json(res))
}

