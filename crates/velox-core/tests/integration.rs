//! Hermetic integration tests: a local HTTP server exercises the full engine —
//! multi-segment downloads, pause/resume, crash recovery, retries on flaky
//! servers, speed limits, redirect hardening and integrity checks.

use axum::body::Body;
use axum::extract::{Path as AxPath, State};
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use velox_core::config::EngineConfig;
use velox_core::engine::Engine;
use velox_core::types::{DownloadOptions, DownloadStatus};

// ---------- deterministic pseudo-random content ------------------------------

pub fn gen_byte(index: u64) -> u8 {
    // xorshift-ish deterministic generator
    let mut x = index.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    (x & 0xff) as u8
}

pub fn gen_vec(size: u64) -> Vec<u8> {
    (0..size).map(gen_byte).collect()
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

// ---------- test server -------------------------------------------------------

#[derive(Clone)]
struct Ctx {
    flaky_after: Arc<AtomicU64>, // kill body after this many bytes per request
    total_hits: Arc<AtomicU64>,
}

async fn serve_file_range(
    State(ctx): State<Ctx>,
    AxPath(size): AxPath<u64>,
    req: Request<Body>,
) -> Response {
    ctx.total_hits.fetch_add(1, Ordering::Relaxed);
    let range = req
        .headers()
        .get("range")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_range);
    let (start, end) = match range {
        Some((s, e)) => (s, e.min(size - 1)),
        None => (0, size.saturating_sub(1)),
    };
    let len = end - start + 1;
    let status = if range.is_some() { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK };
    let mut headers = HeaderMap::new();
    headers.insert("accept-ranges", HeaderValue::from_static("bytes"));
    headers.insert(
        "content-type",
        HeaderValue::from_static("application/octet-stream"),
    );
    if range.is_some() {
        headers.insert(
            "content-range",
            HeaderValue::from_str(&format!("bytes {start}-{end}/{size}")).unwrap(),
        );
    }
    headers.insert("content-length", HeaderValue::from_str(&len.to_string()).unwrap());
    headers.insert("etag", HeaderValue::from_str(&format!("\"gen-{size}\"")).unwrap());

    let body = stream_range(start, len, None);
    (status, headers, Body::from_stream(body)).into_response()
}

/// Flaky server: always terminates the connection after `flaky_after` bytes.
async fn serve_flaky(
    State(ctx): State<Ctx>,
    AxPath(size): AxPath<u64>,
    req: Request<Body>,
) -> Response {
    let range = req
        .headers()
        .get("range")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_range);
    let (start, end) = match range {
        Some((s, e)) => (s, e.min(size - 1)),
        None => (0, size.saturating_sub(1)),
    };
    let len = end - start + 1;
    let kill_after = ctx.flaky_after.load(Ordering::Relaxed);
    let mut headers = HeaderMap::new();
    headers.insert("accept-ranges", HeaderValue::from_static("bytes"));
    headers.insert("content-length", HeaderValue::from_str(&len.to_string()).unwrap());
    if range.is_some() {
        headers.insert(
            "content-range",
            HeaderValue::from_str(&format!("bytes {start}-{end}/{size}")).unwrap(),
        );
    }
    let body = stream_range(start, len, Some(kill_after));
    let status = if range.is_some() { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK };
    (status, headers, Body::from_stream(body)).into_response()
}

/// Slow server: ~`bps` bytes/sec (chunk size 64KB).
async fn serve_slow(
    AxPath((size, kbps)): AxPath<(u64, u64)>,
    req: Request<Body>,
) -> Response {
    let range = req
        .headers()
        .get("range")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_range);
    let (start, end) = match range {
        Some((s, e)) => (s, e.min(size - 1)),
        None => (0, size.saturating_sub(1)),
    };
    let len = end - start + 1;
    let mut headers = HeaderMap::new();
    headers.insert("accept-ranges", HeaderValue::from_static("bytes"));
    headers.insert("content-length", HeaderValue::from_str(&len.to_string()).unwrap());
    if range.is_some() {
        headers.insert(
            "content-range",
            HeaderValue::from_str(&format!("bytes {start}-{end}/{size}")).unwrap(),
        );
    }
    let delay = std::time::Duration::from_millis((64 * 1000) / kbps.max(1));
    let body = stream_range_slow(start, len, delay);
    let status = if range.is_some() { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK };
    (status, headers, Body::from_stream(body)).into_response()
}

/// HEAD returns 405 → forces the ranged-GET probe path.
async fn head_rejects(State(ctx): State<Ctx>, AxPath(size): AxPath<u64>, req: Request<Body>) -> Response {
    if req.method() == axum::http::Method::HEAD {
        (StatusCode::METHOD_NOT_ALLOWED, "no head").into_response()
    } else {
        serve_file_range(State(ctx), AxPath(size), req).await
    }
}

async fn redirect_n(AxPath(n): AxPath<usize>) -> Response {
    if n == 0 {
        serve_final().await
    } else {
        Response::builder()
            .status(StatusCode::FOUND)
            .header("location", format!("/redirect/{}", n - 1))
            .body(Body::empty())
            .unwrap()
    }
}

async fn serve_final() -> Response {
    let data = gen_vec(64 * 1024);
    (
        [(axum::http::header::CONTENT_DISPOSITION, "attachment; filename=\"final-report.pdf\"")],
        data,
    )
        .into_response()
}

async fn evil_redirect() -> Response {
    Response::builder()
        .status(StatusCode::FOUND)
        .header("location", "ftp://evil.example.com/payload")
        .body(Body::empty())
        .unwrap()
}

fn parse_range(v: &str) -> Option<(u64, u64)> {
    let rest = v.strip_prefix("bytes=")?;
    let mut it = rest.splitn(2, '-');
    let s: u64 = it.next()?.parse().ok()?;
    let e: u64 = match it.next() {
        Some(x) if x.is_empty() => u64::MAX, // open-ended
        Some(x) => x.parse().ok()?,
        None => u64::MAX,
    };
    Some((s, e))
}

fn stream_range(start: u64, len: u64, kill_after: Option<u64>) -> impl futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> {
    futures::stream::unfold(
        (start, len, kill_after, 0u64),
        |(pos, remaining, kill, sent)| async move {
            if remaining == 0 {
                return None;
            }
            if let Some(k) = kill {
                if sent >= k {
                    return Some((Err(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "flaky!")), (pos, 0, kill, sent)));
                }
            }
            let n = remaining.min(64 * 1024);
            let data: Vec<u8> = (0..n).map(|i| gen_byte(pos + i)).collect();
            let chunk = bytes::Bytes::from(data);
            Some((Ok(chunk), (pos + n, remaining - n, kill, sent + n)))
        },
    )
}

fn stream_range_slow(
    start: u64,
    len: u64,
    delay: std::time::Duration,
) -> impl futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> {
    futures::stream::unfold((start, len, delay), |(pos, remaining, delay)| async move {
        if remaining == 0 {
            return None;
        }
        tokio::time::sleep(delay).await;
        let n = remaining.min(64 * 1024);
        let data: Vec<u8> = (0..n).map(|i| gen_byte(pos + i)).collect();
        let chunk = bytes::Bytes::from(data);
        Some((Ok(chunk), (pos + n, remaining - n, delay)))
    })
}

async fn spawn_server() -> (SocketAddr, Ctx) {
    let ctx = Ctx {
        flaky_after: Arc::new(AtomicU64::new(1024 * 1024)), // dies after 1MB
        total_hits: Arc::new(AtomicU64::new(0)),
    };
    let app = Router::new()
        .route("/file/:size", get(serve_file_range))
        .route("/flaky/:size", get(serve_flaky))
        .route("/slow/:size/:kbps", get(serve_slow))
        .route("/nohead/:size", get(head_rejects))
        .route("/redirect/:n", get(redirect_n))
        .route("/evil", get(evil_redirect))
        .with_state(ctx.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, ctx)
}

// ---------- engine test harness ----------------------------------------------

fn test_cfg(dir: &std::path::Path) -> EngineConfig {
    EngineConfig {
        download_dir: dir.to_path_buf(),
        connections_per_download: 4,
        max_concurrent_downloads: 4,
        persist_interval_ms: 500,
        compute_sha256_on_complete: false,
        ..Default::default()
    }
}

async fn wait_status(engine: &Engine, id: velox_core::types::DownloadId, target: DownloadStatus, timeout: std::time::Duration) -> velox_core::types::DownloadSnapshot {
    let start = std::time::Instant::now();
    loop {
        let snap = engine.snapshot(id).expect("snapshot");
        if snap.status == target {
            return snap;
        }
        if snap.status == DownloadStatus::Failed && target != DownloadStatus::Failed {
            panic!("download failed while waiting for {target:?}: {:?}", snap.error);
        }
        assert!(start.elapsed() < timeout, "timeout waiting {target:?}, last: {:?}", snap.status);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

// ---------- tests --------------------------------------------------------------

#[tokio::test]
async fn full_download_multi_segment_integrity() {
    let (addr, _ctx) = spawn_server().await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(test_cfg(dir.path())).unwrap();

    let size = 5 * 1024 * 1024 + 777;
    let id = engine
        .add(
            &format!("http://{addr}/file/{size}"),
            DownloadOptions::default(),
        )
        .unwrap();

    let snap = wait_status(&engine, id, DownloadStatus::Completed, std::time::Duration::from_secs(60)).await;
    assert_eq!(snap.total_size, Some(size));
    assert_eq!(snap.bytes_done, size);

    let data = std::fs::read(snap.full_path.clone()).unwrap();
    assert_eq!(data.len() as u64, size);
    let expected = gen_vec(size);
    assert_eq!(sha256_hex(&data), sha256_hex(&expected), "content must match exactly");
}

#[tokio::test]
async fn pause_resume_slow_download() {
    let (addr, _ctx) = spawn_server().await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(test_cfg(dir.path())).unwrap();

    let size = 6 * 1024 * 1024;
    let id = engine
        .add(&format!("http://{addr}/slow/{size}/2048"), DownloadOptions::default()) // ~2MB/s
        .unwrap();

    // wait for some progress
    let start = std::time::Instant::now();
    loop {
        let s = engine.snapshot(id).unwrap();
        if s.bytes_done > 512 * 1024 || s.status == DownloadStatus::Completed {
            break;
        }
        assert!(start.elapsed() < std::time::Duration::from_secs(30));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    engine.pause(id).unwrap();
    let paused = wait_status(&engine, id, DownloadStatus::Paused, std::time::Duration::from_secs(15)).await;
    assert!(paused.bytes_done > 0, "must have bytes on disk before pause");

    engine.resume(id).unwrap();
    let snap = wait_status(&engine, id, DownloadStatus::Completed, std::time::Duration::from_secs(120)).await;
    let data = std::fs::read(snap.full_path).unwrap();
    assert_eq!(sha256_hex(&data), sha256_hex(&gen_vec(size as u64)));
}

#[test]
fn crash_recovery_resumes_from_sidecar() {
    // dedicated server runtime that outlives both "processes"
    let server_rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let addr = server_rt.block_on(async { spawn_server().await.0 });
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_cfg(dir.path());

    let size = 8 * 1024 * 1024;
    let url = format!("http://{addr}/slow/{size}/4096"); // ~4MB/s

    // "process 1": start download, make progress, then hard-kill the runtime
    let id;
    {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let engine = rt.block_on(async { Engine::new(cfg.clone()).unwrap() });
        id = rt.block_on(async {
            let id = engine.add(&url, DownloadOptions::default()).unwrap();
            let start = std::time::Instant::now();
            loop {
                let s = engine.snapshot(id).unwrap();
                if s.bytes_done > 1024 * 1024 {
                    break;
                }
                assert!(start.elapsed() < std::time::Duration::from_secs(30));
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            id
        });
        // hard drop = simulated power loss
    }

    // "process 2": new engine, same state dir → auto-resume must finish the job
    {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let engine = rt.block_on(async { Engine::new(cfg).unwrap() });
        let snap = rt.block_on(async {
            let waited = std::time::Instant::now();
            loop {
                let s = engine.snapshot(id).unwrap();
                if matches!(
                    s.status,
                    DownloadStatus::Completed | DownloadStatus::Downloading | DownloadStatus::Connecting
                ) {
                    break;
                }
                assert!(waited.elapsed() < std::time::Duration::from_secs(15));
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            wait_status(&engine, id, DownloadStatus::Completed, std::time::Duration::from_secs(150)).await
        });
        let data = std::fs::read(snap.full_path).unwrap();
        assert_eq!(data.len() as u64, size);
        assert_eq!(sha256_hex(&data), sha256_hex(&gen_vec(size as u64)));
    }
}

#[tokio::test]
async fn flaky_server_retries_and_completes() {
    let (addr, _ctx) = spawn_server().await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(test_cfg(dir.path())).unwrap();

    let size = 4 * 1024 * 1024;
    let id = engine
        .add(&format!("http://{addr}/flaky/{size}"), DownloadOptions::default())
        .unwrap();
    let snap = wait_status(&engine, id, DownloadStatus::Completed, std::time::Duration::from_secs(120)).await;
    assert!(snap.retries > 0, "flaky server must have triggered retries");
    let data = std::fs::read(snap.full_path).unwrap();
    assert_eq!(sha256_hex(&data), sha256_hex(&gen_vec(size as u64)));
}

#[tokio::test]
async fn speed_limit_is_respected() {
    let (addr, _ctx) = spawn_server().await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(test_cfg(dir.path())).unwrap();

    let size = 2 * 1024 * 1024;
    let mut opts = DownloadOptions::default();
    opts.speed_limit = Some(512 * 1024); // 0.5 MB/s → ≥ ~3.5s for 2MB (burst 256K)
    let id = engine
        .add(&format!("http://{addr}/file/{size}"), opts)
        .unwrap();
    let start = std::time::Instant::now();
    let mut last = 0u64;
    loop {
        let s = engine.snapshot(id).unwrap();
        if s.status == DownloadStatus::Completed { break; }
        if s.status == DownloadStatus::Failed { panic!("failed: {:?}", s.error); }
        if s.bytes_done != last {
            eprintln!("[{}] bytes={} speed={:.0}", start.elapsed().as_secs_f64(), s.bytes_done, s.speed_bps);
            last = s.bytes_done;
        }
        assert!(start.elapsed() < std::time::Duration::from_secs(60), "timeout at {} bytes", s.bytes_done);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let elapsed = start.elapsed().as_secs_f64();
    assert!(elapsed >= 2.5, "limit must slow the transfer, elapsed={elapsed:.2}s");
    let snap = engine.snapshot(id).unwrap();
    let data = std::fs::read(snap.full_path).unwrap();
    assert_eq!(sha256_hex(&data), sha256_hex(&gen_vec(size as u64)));
}

#[tokio::test]
async fn redirects_followed_and_filename_from_content_disposition() {
    let (addr, _ctx) = spawn_server().await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(test_cfg(dir.path())).unwrap();

    let id = engine
        .add(&format!("http://{addr}/redirect/3"), DownloadOptions::default())
        .unwrap();
    let snap = wait_status(&engine, id, DownloadStatus::Completed, std::time::Duration::from_secs(30)).await;
    assert_eq!(snap.filename, "final-report.pdf");
    assert!(snap.full_path.ends_with("final-report.pdf"));
    assert_eq!(snap.total_size, Some(64 * 1024));
}

#[tokio::test]
async fn unsafe_scheme_redirect_is_blocked() {
    let (addr, _ctx) = spawn_server().await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(test_cfg(dir.path())).unwrap();

    let id = engine
        .add(&format!("http://{addr}/evil"), DownloadOptions::default())
        .unwrap();
    let snap = wait_status(&engine, id, DownloadStatus::Failed, std::time::Duration::from_secs(30)).await;
    let err = snap.error.unwrap_or_default();
    assert!(
        err.contains("scheme") || err.contains("redirect"),
        "error should mention scheme/redirect, got: {err}"
    );
    assert!(!std::path::Path::new(&snap.full_path).exists()
        || std::fs::metadata(&snap.full_path).map(|m| m.len()).unwrap_or(0) == 0);
}

#[tokio::test]
async fn head_rejected_server_still_downloads_via_ranged_get_probe() {
    let (addr, _ctx) = spawn_server().await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(test_cfg(dir.path())).unwrap();

    let size = 2 * 1024 * 1024;
    let id = engine
        .add(&format!("http://{addr}/nohead/{size}"), DownloadOptions::default())
        .unwrap();
    let snap = wait_status(&engine, id, DownloadStatus::Completed, std::time::Duration::from_secs(60)).await;
    let data = std::fs::read(snap.full_path).unwrap();
    assert_eq!(sha256_hex(&data), sha256_hex(&gen_vec(size as u64)));
}

#[tokio::test]
async fn queue_respects_max_concurrent() {
    let (addr, _ctx) = spawn_server().await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = test_cfg(dir.path());
    cfg.max_concurrent_downloads = 2;
    let engine = Engine::new(cfg).unwrap();

    let mut ids = Vec::new();
    for _ in 0..4 {
        let id = engine
            .add(&format!("http://{addr}/slow/{}/2048", 4 * 1024 * 1024), DownloadOptions::default())
            .unwrap();
        ids.push(id);
    }
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let active = engine
        .list()
        .into_iter()
        .filter(|s| s.status.is_active())
        .count();
    assert!(active <= 2, "max_concurrent=2 but {active} active");
    for id in ids {
        let _ = engine.cancel(id);
    }
}

#[tokio::test]
async fn invalid_urls_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(test_cfg(dir.path())).unwrap();
    assert!(engine.add("file:///etc/passwd", DownloadOptions::default()).is_err());
    assert!(engine.add("javascript:alert(1)", DownloadOptions::default()).is_err());
    assert!(engine.add("", DownloadOptions::default()).is_err());
    assert!(engine.add("http://a.b/\r\nX: y", DownloadOptions::default()).is_err());
}

#[tokio::test]
async fn expected_sha256_verification_detects_corruption() {
    let (addr, _ctx) = spawn_server().await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(test_cfg(dir.path())).unwrap();

    let size = 1024 * 1024;
    let mut opts = DownloadOptions::default();
    opts.verify_sha256 = Some("0".repeat(64)); // wrong hash on purpose
    let id = engine
        .add(&format!("http://{addr}/file/{size}"), opts)
        .unwrap();
    let snap = wait_status(&engine, id, DownloadStatus::Failed, std::time::Duration::from_secs(60)).await;
    assert!(
        snap.error.as_deref().unwrap_or("").contains("integrity"),
        "expected integrity failure, got {:?}",
        snap.error
    );
}
