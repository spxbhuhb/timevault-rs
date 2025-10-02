use actix_web::web::Data;
use actix_web::{HttpResponse, Responder, get};

use crate::app::App;

#[get("/ready")]
pub async fn ready(app: Data<App>) -> actix_web::Result<impl Responder> {
    if app.is_ready() {
        Ok(HttpResponse::Ok().finish())
    } else {
        Ok(HttpResponse::ServiceUnavailable().body("not ready"))
    }
}

#[get("/live")]
pub async fn live() -> actix_web::Result<impl Responder> {
    Ok(HttpResponse::Ok().finish())
}

