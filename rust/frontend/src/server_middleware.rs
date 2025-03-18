use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::{extract::Request, middleware::Next, response::Response, Json};
use chroma_error::ErrorCodes;

/// If the request does not have a `Content-Type` header, set it to `application/json`.
pub(crate) async fn default_json_content_type_middleware(mut req: Request, next: Next) -> Response {
    if req
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .is_none()
    {
        req.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/json"),
        );
    }
    next.run(req).await
}

/// Axum occasionally returns generic errors as plain text. Chroma clients expect that JSON errors will always be returned, so this middleware converts plain text errors to JSON.
///
/// Inspired by https://github.com/rust-lang/crates.io/blob/edcf93b071d3564e497c7a984fd411a760db28b5/src/middleware/cargo_compat.rs
pub(crate) async fn always_json_errors_middleware(req: Request, next: Next) -> Response {
    let res = next.run(req).await;

    let status = res.status();
    if !status.is_client_error() && !status.is_server_error() {
        return res;
    }

    let content_type = res.headers().get("content-type");
    if !matches!(content_type, Some(content_type) if content_type == "text/plain; charset=utf-8") {
        return res;
    }

    let (mut parts, body) = res.into_parts();

    // The `Json` struct is somehow not able to override these headers of the
    // `Parts` struct, so we remove them here to avoid the conflict.
    parts.headers.remove(header::CONTENT_TYPE);
    parts.headers.remove(header::CONTENT_LENGTH);

    let bytes = match axum::body::to_bytes(body, 1_000_000).await {
        Ok(bytes) => bytes,
        Err(err) => {
            tracing::error!("Failed to read response body: {}", err);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let text = match std::str::from_utf8(&bytes) {
        Ok(text) => text,
        Err(_) => {
            tracing::error!("Failed to parse response body as UTF-8");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let error_code: ErrorCodes = status.into();
    let json = serde_json::json!({ "error": error_code.name(), "message": text });
    (parts, Json(json)).into_response()
}
