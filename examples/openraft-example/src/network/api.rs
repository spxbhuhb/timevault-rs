use actix_web::web::{Data, Json, Path, Query};
use actix_web::{HttpResponse, Responder, get, post};
use serde::Deserialize;
use serde::Serialize;
use timevault::store::transfer::{DataTransfer, FileDownload, ManifestDownload, TransferRange};
use uuid::Uuid;

use crate::app::App;
use crate::network::transfer::StoreTransferServer;
use crate::state::{DeviceStatus, ExampleEvent};

#[post("/write")]
pub async fn write(app: Data<App>, req: Json<ExampleEvent>) -> actix_web::Result<impl Responder> {
    let response = app.raft.client_write(req.0).await;
    Ok(Json(response))
}

#[get("/read")]
pub async fn read(app: Data<App>) -> actix_web::Result<impl Responder> {
    // Ensure linearizable read: only the leader should serve reads and only after
    // establishing a linearizable barrier. If not leader, return a CheckIsLeaderError
    // so clients can forward to the leader.
    let ret = app.raft.ensure_linearizable().await;
    let res: Result<Vec<DeviceStatus>, crate::typ::RaftError<crate::typ::CheckIsLeaderError>> = match ret {
        Ok(_) => Ok(app.snapshot_devices()),
        Err(e) => Err(e),
    };
    Ok(Json(res))
}

#[get("/partitions")]
pub async fn partitions(app: Data<App>) -> actix_web::Result<impl Responder> {
    let ids = timevault::store::paths::list_partitions(&app.root).map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(Json(Ok::<_, crate::typ::RaftError>(ids)))
}

#[derive(Serialize)]
struct ManifestResponse {
    partition_id: Uuid,
    version: Option<String>,
    lines: Vec<timevault::store::disk::manifest::ManifestLine>,
}

#[derive(Serialize)]
struct RangeResponse {
    start: u64,
    end: Option<u64>,
}

#[derive(Serialize)]
struct FileResponse {
    partition_id: Uuid,
    chunk_id: u64,
    requested_range: RangeResponse,
    bytes: Vec<u8>,
    remote_len: u64,
    version: Option<String>,
}

impl From<FileDownload> for FileResponse {
    fn from(download: FileDownload) -> Self {
        Self {
            partition_id: download.partition_id,
            chunk_id: download.chunk_id,
            requested_range: RangeResponse {
                start: download.requested_range.start,
                end: download.requested_range.end,
            },
            bytes: download.bytes,
            remote_len: download.remote_len,
            version: download.version,
        }
    }
}

impl From<ManifestDownload> for ManifestResponse {
    fn from(download: ManifestDownload) -> Self {
        Self {
            partition_id: download.partition_id,
            version: download.version,
            lines: download.lines,
        }
    }
}

#[derive(Deserialize)]
struct RangeQuery {
    start: u64,
    end: Option<u64>,
}

#[get("/transfer/{partition}/manifest")]
pub async fn transfer_manifest(app: Data<App>, path: Path<(Uuid,)>) -> actix_web::Result<impl Responder> {
    let (partition,) = path.into_inner();
    let server = StoreTransferServer { root: app.root.clone() };
    let manifest = server.download_manifest(partition).map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(Json(ManifestResponse::from(manifest)))
}

#[get("/transfer/{partition}/chunk/{chunk_id}")]
pub async fn transfer_chunk(app: Data<App>, path: Path<(Uuid, u64)>, query: Query<RangeQuery>) -> actix_web::Result<impl Responder> {
    let (partition, chunk_id) = path.into_inner();
    let range = TransferRange { start: query.start, end: query.end };
    let server = StoreTransferServer { root: app.root.clone() };
    let download = server.download_chunk(partition, chunk_id, range).map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(Json(FileResponse::from(download)))
}

#[get("/transfer/{partition}/index/{chunk_id}")]
pub async fn transfer_index(app: Data<App>, path: Path<(Uuid, u64)>, query: Query<RangeQuery>) -> actix_web::Result<impl Responder> {
    let (partition, chunk_id) = path.into_inner();
    let range = TransferRange { start: query.start, end: query.end };
    let server = StoreTransferServer { root: app.root.clone() };
    let download = server.download_index(partition, chunk_id, range).map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(Json(FileResponse::from(download)))
}

#[post("/shutdown")]
pub async fn shutdown(app: Data<App>) -> actix_web::Result<impl Responder> {
    let shutdown_signal = app.take_shutdown_signal();
    let raft_result = app.raft.shutdown().await;

    if let Some(tx) = shutdown_signal {
        let _ = tx.send(());
    }

    raft_result.map_err(actix_web::error::ErrorInternalServerError)?;

    Ok(HttpResponse::Ok().finish())
}
//
// #[post("/consistent_read")]
// pub async fn consistent_read(app: Data<App>, req: Json<String>) -> actix_web::Result<impl Responder> {
//     let ret = app.raft.ensure_linearizable().await;
//
//     match ret {
//         Ok(_) => {
//             let state_machine = app.state_machine_store.state_machine.read().await;
//             let key = req.0;
//             let value = state_machine.data.get(&key).cloned();
//
//             let res: Result<String, RaftError<TvrNodeId, CheckIsLeaderError<TvrNodeId, BasicNode>>> =
//                 Ok(value.unwrap_or_default());
//             Ok(Json(res))
//         }
//         Err(e) => Ok(Json(Err(e))),
//     }
// }
