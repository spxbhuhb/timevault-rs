use actix_web::web;
use actix_web::web::Data;
use actix_web::Responder;
use actix_web::{get, post};
use serde_json::json;
use web::Json;

use crate::app::App;
use crate::ValueRequest;

#[post("/write")]
pub async fn write(app: Data<App>, req: Json<ValueRequest>) -> actix_web::Result<impl Responder> {
    let response = app.raft.client_write(req.0).await;
    Ok(Json(response))
}

#[get("/read")]
pub async fn read(_app: Data<App>) -> actix_web::Result<impl Responder> {
//    let state_machine = app.state_machine_store.state_machine.read().await;
//    let key = req.0;
//    let value = state_machine.data.get(&key).cloned();

//    let res: Result<String, Infallible> = Ok(value.unwrap_or_default());
    Ok(Json(json!({ "message": "Hello World!" })))
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
