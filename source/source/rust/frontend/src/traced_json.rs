use axum::body::{Body, HttpBody};
use axum::extract::{rejection::JsonRejection, FromRequest, Request};
use axum::response::IntoResponse;
use axum::{BoxError, RequestExt};
use http_body_util::BodyExt;
use tracing::Instrument;

/// TracedJson is a thin wrapper around axum::Json that allows us to trace the request body buffering
/// as well as the JSON parsing.
/// The error behavior has parity with axum::Json, but the error type is different because those types
/// are private to axum.
pub(crate) struct TracedJson<T>(pub T);

pub(crate) enum TracingJsonRejection {
    JsonRejection(JsonRejection),
    LengthLimitError,
    UnknownBodyError,
}

impl IntoResponse for TracingJsonRejection {
    fn into_response(self) -> axum::response::Response {
        match self {
            TracingJsonRejection::JsonRejection(rejection) => rejection.into_response(),
            TracingJsonRejection::LengthLimitError => axum::response::Response::builder()
                .status(axum::http::StatusCode::PAYLOAD_TOO_LARGE)
                .body(axum::body::Body::from("Payload too large"))
                // SAFETY(hammadb): This unwrap is safe because I have verified that this builder turns
                // into a valid response.
                .unwrap(),
            TracingJsonRejection::UnknownBodyError => axum::response::Response::builder()
                .status(axum::http::StatusCode::BAD_REQUEST)
                .body(axum::body::Body::from(
                    "Unknown error while buffering the request body",
                ))
                // SAFETY(hammadb): This unwrap is safe because I have verified that this builder turns
                // into a valid response.
                .unwrap(),
        }
    }
}

impl<S, T> FromRequest<S> for TracedJson<T>
where
    axum::Json<T>: FromRequest<S, Rejection = JsonRejection>,
    S: Send + Sync,
{
    type Rejection = TracingJsonRejection;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let (parts, body) = req.with_limited_body().into_parts();

        let bytes = body
            .collect()
            .instrument(tracing::debug_span!("buffering_request_body"))
            .await;
        let buffered_req = match bytes {
            Ok(bytes) => Request::from_parts(parts, Body::from(bytes.to_bytes())),
            Err(err) => {
                // two layers of boxes here because `with_limited_body`
                // wraps the `http_body_util::Limited` in a `axum_core::Body`
                // which also wraps the error type
                let box_error = match BoxError::from(err).downcast::<axum::Error>() {
                    Ok(err) => err.into_inner(),
                    Err(err) => err,
                };
                let box_error = match box_error.downcast::<axum::Error>() {
                    Ok(err) => err.into_inner(),
                    Err(err) => err,
                };
                match box_error.downcast::<http_body_util::LengthLimitError>() {
                    Ok(_) => return Err(TracingJsonRejection::LengthLimitError),
                    Err(_) => return Err(TracingJsonRejection::UnknownBodyError),
                };
            }
        };

        let buffered_req_len = buffered_req.body().size_hint().lower();
        match axum::Json::<T>::from_request(buffered_req, state)
            .instrument(tracing::debug_span!(
                "parsing_json",
                bytes =? buffered_req_len
            ))
            .await
        {
            Ok(value) => Ok(Self(value.0)),
            Err(rejection) => Err(TracingJsonRejection::JsonRejection(rejection)),
        }
    }
}
