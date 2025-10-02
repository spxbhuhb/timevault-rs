use std::collections::BTreeMap;
use std::collections::BTreeSet;

use actix_web::web::{Data, Json};
use actix_web::{Responder, get, post};
use openraft::BasicNode;
use openraft::RaftMetrics;
use openraft::error::Infallible;

use crate::app::App;
use crate::TvrNodeId;

#[post("/add-learner")]
pub async fn add_learner(app: Data<App>, req: Json<(TvrNodeId, String)>) -> actix_web::Result<impl Responder> {
    let node_id = req.0.0;
    let node = BasicNode { addr: req.0.1.clone() };
    let res = app.raft.add_learner(node_id, node, true).await;
    Ok(Json(res))
}

#[post("/change-membership")]
pub async fn change_membership(app: Data<App>, req: Json<BTreeSet<TvrNodeId>>) -> actix_web::Result<impl Responder> {
    let res = app.raft.change_membership(req.0, false).await;
    Ok(Json(res))
}

#[post("/init")]
pub async fn init(app: Data<App>, req: Json<Vec<(TvrNodeId, String)>>) -> actix_web::Result<impl Responder> {
    let mut nodes = BTreeMap::new();
    if req.0.is_empty() {
        nodes.insert(app.id, BasicNode { addr: app.addr.clone() });
    } else {
        for (id, addr) in req.0.into_iter() {
            nodes.insert(id, BasicNode { addr });
        }
    };
    let res = app.raft.initialize(nodes).await;
    Ok(Json(res))
}

#[get("/metrics")]
pub async fn metrics(app: Data<App>) -> actix_web::Result<impl Responder> {
    let metrics = app.raft.metrics().borrow().clone();
    let res: Result<RaftMetrics<TvrNodeId, BasicNode>, Infallible> = Ok(metrics);
    Ok(Json(res))
}

