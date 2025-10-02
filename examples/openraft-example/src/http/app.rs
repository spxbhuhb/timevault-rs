use actix_web::web::{Data, Json};
use actix_web::{Responder, get, post};

use crate::app::App;
use crate::domain::{AppEvent, DeviceStatus};

#[post("/write")]
pub async fn write(app: Data<App>, req: Json<AppEvent>) -> actix_web::Result<impl Responder> {
    let response = app.raft.client_write(req.0).await;
    Ok(Json(response))
}

#[get("/read")]
pub async fn read(app: Data<App>) -> actix_web::Result<impl Responder> {
    let ret = app.raft.ensure_linearizable().await;
    let res: Result<Vec<DeviceStatus>, crate::typ::RaftError<crate::typ::CheckIsLeaderError>> = match ret {
        Ok(_) => Ok(app.snapshot_devices()),
        Err(e) => Err(e),
    };
    Ok(Json(res))
}

#[post("/shutdown")]
pub async fn shutdown(app: Data<App>) -> actix_web::Result<impl Responder> {
    let shutdown_signal = app.take_shutdown_signal();
    let raft_result = app.raft.shutdown().await;

    if let Some(tx) = shutdown_signal {
        let _ = tx.send(());
    }

    raft_result.map_err(actix_web::error::ErrorInternalServerError)?;

    Ok(actix_web::HttpResponse::Ok().finish())
}

