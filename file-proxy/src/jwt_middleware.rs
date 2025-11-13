use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::IntoResponse,
    response::Response,
};

use crate::jwt_helper::{validate_token, Claims};

pub async fn jwt_middleware(
    State(secret): State<Arc<String>>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let unauthorized = || {
        (
            StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({
                "error": "Unauthorized"
            })),
        )
            .into_response()
    };

    let auth_header = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let Some(auth) = auth_header else {
        return unauthorized();
    };

    // Expect "Bearer <token>"
    let parts: Vec<&str> = auth.split_whitespace().collect();
    if parts.len() != 2 || parts[0] != "Bearer" {
        return unauthorized();
    }
    let token = parts[1];

    match validate_token(&secret, token) {
        Ok(claims) => {
            req.extensions_mut().insert(claims);
            next.run(req).await
        }
        Err(_) => unauthorized(),
    }
}


