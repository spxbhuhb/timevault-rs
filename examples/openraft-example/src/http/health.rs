use actix_web::{HttpResponse, Responder, get};

#[get("/ready")]
pub async fn ready() -> actix_web::Result<impl Responder> {
    // HTTP-level readiness: server is up and can handle requests
    Ok(HttpResponse::Ok().finish())
}

#[get("/live")]
pub async fn live() -> actix_web::Result<impl Responder> {
    Ok(HttpResponse::Ok().finish())
}
