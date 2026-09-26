//! Local REST daemon. Binds 127.0.0.1 only. Token-authenticated.
//! Endpoints:
//!   GET  /api/ping            → {ok, version}
//!   GET  /api/list            → snapshots
//!   GET  /api/stats           → engine stats
//!   POST /api/add             → {url, mirrors?, filename?, connections?, speed_limit?, headers?}
//!   POST /api/:id/pause | resume | cancel
//!   DELETE /api/:id           → remove (query purge=true to delete partials)
//!   GET  /api/events          → SSE event stream

use crate::parse_size;
use anyhow::Result;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use velox_core::engine::Engine;
use velox_core::types::DownloadOptions;

#[derive(Clone)]
struct ApiState {
    engine: std::sync::Arc<Engine>,
    token: String,
}

fn token_path(state_dir: &std::path::Path) -> PathBuf {
    state_dir.join("api-token")
}

fn load_or_create_token(state_dir: &std::path::Path) -> Result<String> {
    let p = token_path(state_dir);
    if let Ok(t) = std::fs::read_to_string(&p) {
        let t = t.trim().to_string();
        if t.len() >= 16 {
            return Ok(t);
        }
    }
    use rand::Rng;
    let t: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(32)
        .map(char::from)
        .collect();
    std::fs::create_dir_all(state_dir)?;
    // token file is user-only
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(&p, &t)?;
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    std::fs::write(&p, &t)?;
    Ok(t)
}

async fn auth(State(st): State<ApiState>, headers: HeaderMap) -> Result<(), (StatusCode, String)> {
    let mut got = headers
        .get("x-velox-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if got.is_empty() {
        if let Some(a) = headers.get("authorization").and_then(|a| a.to_str().ok()) {
            if let Some(b) = a.strip_prefix("Bearer ") {
                got = b.to_string();
            }
        }
    }
    if got != st.token {
        return Err((StatusCode::UNAUTHORIZED, "invalid or missing X-Velox-Token".into()));
    }
    Ok(())
}

pub async fn run(port: u16, show_token: bool, cfg: velox_core::config::EngineConfig) -> Result<()> {
    let state_dir = cfg.state_dir.clone().unwrap_or_else(|| cfg.download_dir.join(".velox-meta"));
    let token = load_or_create_token(&state_dir)?;
    let engine = Engine::new(cfg)?;

    if show_token {
        println!("API token: {token}");
    } else {
        println!("API token: stored in {}", token_path(&state_dir).display());
    }
    println!("REST API listening on http://127.0.0.1:{port}/api (localhost only)");
    println!("Browser extension: set this token + port in the Velox extension options.");

    let st = ApiState {
        engine,
        token: token.clone(),
    };

    let app = Router::new()
        .route("/api/ping", get(ping))
        .route("/api/list", get(list))
        .route("/api/stats", get(stats))
        .route("/api/add", post(add))
        .route("/api/:id/pause", post(pause))
        .route("/api/:id/resume", post(resume))
        .route("/api/:id/cancel", post(cancel))
        .route("/api/:id", delete(remove_dl))
        .route("/api/events", get(events_sse))
        .with_state(st);

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

macro_rules! guard {
    ($st:expr, $headers:expr) => {
        if let Err(e) = auth(State($st.clone()), $headers).await {
            return e.into_response();
        }
    };
}

async fn ping(State(st): State<ApiState>, headers: HeaderMap) -> Response {
    guard!(st, headers);
    Json(json!({"ok": true, "version": env!("CARGO_PKG_VERSION")})).into_response()
}

async fn list(State(st): State<ApiState>, headers: HeaderMap) -> Response {
    guard!(st, headers);
    Json(st.engine.list()).into_response()
}

async fn stats(State(st): State<ApiState>, headers: HeaderMap) -> Response {
    guard!(st, headers);
    Json(st.engine.stats()).into_response()
}

#[derive(Deserialize)]
struct AddBody {
    url: String,
    #[serde(default)]
    mirrors: Vec<String>,
    #[serde(default)]
    filename: Option<String>,
    #[serde(default)]
    connections: Option<u32>,
    #[serde(default)]
    speed_limit: Option<String>,
    #[serde(default)]
    sha256: Option<String>,
    #[serde(default)]
    headers: Vec<(String, String)>,
}

async fn add(
    State(st): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<AddBody>,
) -> Response {
    guard!(st, headers);
    let mut opts = DownloadOptions {
        filename: body.filename,
        connections: body.connections,
        mirrors: body.mirrors,
        headers: body.headers,
        verify_sha256: body.sha256,
        ..Default::default()
    };
    if let Some(sl) = &body.speed_limit {
        match parse_size(sl) {
            Ok(v) => opts.speed_limit = Some(v),
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "bad speed_limit"})),
                )
                    .into_response()
            }
        }
    }
    match st.engine.add(&body.url, opts) {
        Ok(id) => (StatusCode::CREATED, Json(json!({"id": id}))).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

fn resolve(st: &ApiState, id: &str) -> Option<velox_core::types::DownloadId> {
    let full = uuid::Uuid::parse_str(id).ok();
    st.engine.list().into_iter().find_map(|s| {
        if s.id == full.unwrap_or_default() || s.id.to_string().starts_with(id) {
            Some(s.id)
        } else {
            None
        }
    })
}

async fn pause(
    State(st): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    guard!(st, headers);
    match resolve(&st, &id) {
        Some(t) => match st.engine.pause(t) {
            Ok(()) => Json(json!({"ok": true})).into_response(),
            Err(e) => (StatusCode::CONFLICT, Json(json!({"error": e.to_string()}))).into_response(),
        },
        None => (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response(),
    }
}

async fn resume(
    State(st): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    guard!(st, headers);
    match resolve(&st, &id) {
        Some(t) => match st.engine.resume(t) {
            Ok(()) => Json(json!({"ok": true})).into_response(),
            Err(e) => (StatusCode::CONFLICT, Json(json!({"error": e.to_string()}))).into_response(),
        },
        None => (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response(),
    }
}

async fn cancel(
    State(st): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    guard!(st, headers);
    match resolve(&st, &id) {
        Some(t) => match st.engine.cancel(t) {
            Ok(()) => Json(json!({"ok": true})).into_response(),
            Err(e) => (StatusCode::CONFLICT, Json(json!({"error": e.to_string()}))).into_response(),
        },
        None => (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response(),
    }
}

async fn remove_dl(
    State(st): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    guard!(st, headers);
    match resolve(&st, &id) {
        Some(t) => {
            let purge = q.get("purge").map(|v| v == "true").unwrap_or(false);
            match st.engine.remove(t, purge, false) {
                Ok(()) => Json(json!({"ok": true})).into_response(),
                Err(e) => (StatusCode::CONFLICT, Json(json!({"error": e.to_string()}))).into_response(),
            }
        }
        None => (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response(),
    }
}

async fn events_sse(
    State(st): State<ApiState>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = auth(State(st.clone()), headers).await {
        return e.into_response();
    }
    let rx = st.engine.events();
    let stream = futures::stream::unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(ev) => {
                    let json = serde_json::to_string(&ev).unwrap_or_default();
                    return Some((Ok::<_, std::convert::Infallible>(Event::default().data(json)), rx));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => return None,
            }
        }
    });
    axum::response::Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}
