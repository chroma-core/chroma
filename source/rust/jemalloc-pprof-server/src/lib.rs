use axum::http::StatusCode;
use axum::response::IntoResponse;
use tokio::sync::oneshot;

/// Exposes a server on the given port that serves jemalloc pprof data.
///
/// Routes:
/// - `/debug/pprof/flamegraph`: Returns a flamegraph of the heap profile.
/// - `/debug/pprof/heap`: Returns a heap profile in pprof format.
pub async fn spawn_pprof_server(port: u16, shutdown_signal: oneshot::Receiver<()>) {
    let app = axum::Router::new()
        .route(
            "/debug/pprof/flamegraph",
            axum::routing::get(handle_get_flamegraph),
        )
        .route("/debug/pprof/heap", axum::routing::get(handle_get_heap));

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                shutdown_signal.await.ok();
            })
            .await
            .unwrap();
    });
}

async fn handle_get_flamegraph() -> Result<impl IntoResponse, (StatusCode, String)> {
    let mut prof_ctl = match jemalloc_pprof::PROF_CTL.as_ref() {
        Some(ctl) => ctl.lock().await,
        None => {
            return Err((
                StatusCode::FORBIDDEN,
                "jemalloc profiling not enabled".into(),
            ));
        }
    };

    if !prof_ctl.activated() {
        return Err((StatusCode::FORBIDDEN, "heap profiling not activated".into()));
    }

    let flamegraph_svg = prof_ctl
        .dump_flamegraph()
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            axum::http::header::HeaderValue::from_static("image/svg+xml"),
        )],
        flamegraph_svg,
    ))
}

async fn handle_get_heap() -> Result<impl IntoResponse, (StatusCode, String)> {
    let mut prof_ctl = match jemalloc_pprof::PROF_CTL.as_ref() {
        Some(ctl) => ctl.lock().await,
        None => {
            return Err((
                StatusCode::FORBIDDEN,
                "jemalloc profiling not enabled".into(),
            ));
        }
    };

    if !prof_ctl.activated() {
        return Err((StatusCode::FORBIDDEN, "heap profiling not activated".into()));
    }

    let pprof = prof_ctl
        .dump_pprof()
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(pprof)
}
