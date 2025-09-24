use crate::ValueConfig;
use crate::app::App;
use actix_web::Responder;
use actix_web::post;
use actix_web::web::Data;
use actix_web::web::Json;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::VoteRequest;
use timevault::raft::TvrNodeId;

// --- Raft communication

#[post("/raft-vote")]
pub async fn vote(app: Data<App>, req: Json<VoteRequest<TvrNodeId>>) -> actix_web::Result<impl Responder> {
    let res = app.raft.vote(req.0).await;
    Ok(Json(res))
}

#[post("/raft-append")]
pub async fn append(app: Data<App>, req: Json<AppendEntriesRequest<ValueConfig>>) -> actix_web::Result<impl Responder> {
    let res = app.raft.append_entries(req.0).await;
    Ok(Json(res))
}

#[post("/raft-snapshot")]
pub async fn snapshot(app: Data<App>, req: Json<InstallSnapshotRequest<ValueConfig>>) -> actix_web::Result<impl Responder> {
    let res = app.raft.install_snapshot(req.0).await;
    Ok(Json(res))
}
