//! Diagnostic: find where concurrent-segment writes corrupt data.

use velox_core::config::EngineConfig;
use velox_core::engine::Engine;
use velox_core::types::{DownloadOptions, DownloadStatus};

pub fn gen_byte(index: u64) -> u8 {
    let mut x = index
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    (x & 0xff) as u8
}

#[tokio::test]
async fn diagnose_corruption() {
    let (addr, _ctx) = testserver::spawn().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = EngineConfig {
        download_dir: dir.path().to_path_buf(),
        connections_per_download: 4,
        max_concurrent_downloads: 4,
        compute_sha256_on_complete: false,
        persist_interval_ms: 500,
        ..Default::default()
    };
    let engine = Engine::new(cfg).unwrap();
    let size: u64 = 5 * 1024 * 1024 + 777;
    let id = engine
        .add(&format!("http://{addr}/file/{size}"), DownloadOptions::default())
        .unwrap();
    let start = std::time::Instant::now();
    loop {
        let s = engine.snapshot(id).unwrap();
        if s.status == DownloadStatus::Completed {
            println!("completed in {:.1}s", start.elapsed().as_secs_f64());
            break;
        }
        if s.status == DownloadStatus::Failed {
            panic!("failed: {:?}", s.error);
        }
        assert!(start.elapsed() < std::time::Duration::from_secs(90));
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let snap = engine.snapshot(id).unwrap();
    let data = std::fs::read(&snap.full_path).unwrap();
    println!("segments: {:?}", snap.segments.len());
    for (i, seg) in snap.segments.iter().enumerate() {
        println!("  seg {i}: [{}, {}] written {}", seg.start, seg.end, seg.written);
    }
    // find mismatch regions
    let mut diffs = Vec::new();
    let n = data.len() as u64;
    let mut i = 0u64;
    while i < n {
        if data[i as usize] != gen_byte(i) {
            let start = i;
            while i < n && data[i as usize] != gen_byte(i) {
                i += 1;
            }
            diffs.push((start, i - 1));
        } else {
            i += 1;
        }
    }
    println!("mismatch regions: {}", diffs.len());
    for (a, b) in diffs.iter().take(20) {
        println!("  [{a}..{b}]  len={}  got={:02x?} want={:02x?}", b - a + 1, data[*a as usize], gen_byte(*a));
    }
}

mod testserver {
    use axum::body::Body;
    use axum::extract::{Path as AxPath, State};
    use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::get;
    use axum::Router;
    use std::net::SocketAddr;
    use std::sync::Arc;

    #[derive(Clone)]
    pub struct Ctx;

    pub fn gen_byte2(index: u64) -> u8 {
        crate::gen_byte(index)
    }

    fn parse_range(v: &str) -> Option<(u64, u64)> {
        let rest = v.strip_prefix("bytes=")?;
        let mut it = rest.splitn(2, '-');
        let s: u64 = it.next()?.parse().ok()?;
        let e: u64 = match it.next() {
            Some(x) if x.is_empty() => u64::MAX,
            Some(x) => x.parse().ok()?,
            None => u64::MAX,
        };
        Some((s, e))
    }

    fn stream_range(start: u64, len: u64) -> impl futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> {
        futures::stream::unfold((start, len), |(pos, remaining)| async move {
            if remaining == 0 {
                return None;
            }
            let n = remaining.min(64 * 1024);
            let data: Vec<u8> = (0..n).map(|i| gen_byte2(pos + i)).collect();
            let chunk = bytes::Bytes::from(data);
            Some((Ok(chunk), (pos + n, remaining - n)))
        })
    }

    async fn serve_file_range(
        State(_ctx): State<Ctx>,
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
        let status = if range.is_some() { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK };
        let mut headers = HeaderMap::new();
        headers.insert("accept-ranges", HeaderValue::from_static("bytes"));
        if range.is_some() {
            headers.insert(
                "content-range",
                HeaderValue::from_str(&format!("bytes {start}-{end}/{size}")).unwrap(),
            );
        }
        headers.insert("content-length", HeaderValue::from_str(&len.to_string()).unwrap());
        let body = stream_range(start, len);
        (status, headers, Body::from_stream(body)).into_response()
    }

    pub async fn spawn() -> (SocketAddr, Ctx) {
        let ctx = Ctx;
        let app = Router::new()
            .route("/file/:size", get(serve_file_range))
            .with_state(ctx.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, ctx)
    }
}
