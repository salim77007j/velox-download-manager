//! Standalone benchmark utilities: prove throughput claims with numbers.

use crate::config::EngineConfig;
use crate::error::Result;
use crate::http;
use crate::urlsafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, serde::Serialize)]
pub struct BenchResult {
    pub connections: u32,
    pub bytes: u64,
    pub elapsed_secs: f64,
    pub speed_bps: f64,
    pub speed_mbps: f64,
    pub http_version: Option<String>,
    pub server: Option<String>,
}

/// Download `bytes` from `url` using `conns` parallel ranged connections into a
/// scratch file, then delete it. Reports aggregate throughput.
pub async fn bench_download(
    cfg: &EngineConfig,
    url: &str,
    conns: u32,
    bytes: u64,
) -> Result<BenchResult> {
    let parsed = urlsafe::validate_and_normalize(url)?;
    let client = http::build_client(cfg)?;
    let probe = http::probe(&client, &parsed, &[], None).await?;
    let total = probe.total_size.unwrap_or(bytes);
    let want = bytes.min(total);
    let supports_ranges = probe.supports_ranges;

    let tmp = tempfile_path();
    let file = Arc::new(crate::storage::SparseFile::create(
        tmp.clone(),
        Some(if supports_ranges { want } else { 0 }),
    )?);

    let started = Instant::now();
    let mut tasks = Vec::new();
    let per_conn = if supports_ranges { want / u64::from(conns.max(1)) } else { want };
    for i in 0..conns.max(1) {
        let client = client.clone();
        let url = probe.final_url.clone();
        let file = file.clone();
        let rng = if supports_ranges {
            let start = per_conn * u64::from(i);
            let end = if i == conns - 1 { want - 1 } else { start + per_conn - 1 };
            Some((start.min(want.saturating_sub(1)), end.min(want.saturating_sub(1))))
        } else {
            if i > 0 {
                break; // single connection only
            }
            None
        };
        tasks.push(tokio::spawn(async move {
            let resp = http::open_range(&client, &url, rng, &[], None, None).await?;
            let mut stream = resp;
            let mut pos = rng.map(|(s, _)| s).unwrap_or(0);
            let mut written = 0u64;
            loop {
                let chunk = match tokio::time::timeout(Duration::from_secs(30), stream.chunk()).await {
                    Ok(r) => r?,
                    Err(_) => {
                        return Err(crate::error::VeloxError::Network(
                            "bench read timeout".into(),
                        ))
                    }
                };
                let Some(chunk) = chunk else { break };
                let n = chunk.len();
                file.write_at(pos, &chunk)?;
                pos += n as u64;
                written += n as u64;
                if rng.is_none() && written >= want {
                    break;
                }
            }
            Ok::<u64, crate::error::VeloxError>(written)
        }));
    }

    let mut total_written = 0u64;
    for t in tasks {
        total_written += t.await.map_err(|e| crate::error::VeloxError::Other(e.to_string()))??;
    }
    let elapsed = started.elapsed().as_secs_f64();
    let _ = file; // keep alive until now
    drop(file);
    let _ = std::fs::remove_file(&tmp);

    let speed = if elapsed > 0.0 { total_written as f64 / elapsed } else { 0.0 };
    Ok(BenchResult {
        connections: conns,
        bytes: total_written,
        elapsed_secs: elapsed,
        speed_bps: speed,
        speed_mbps: speed / (1024.0 * 1024.0),
        http_version: probe.http_version,
        server: probe.server,
    })
}

fn tempfile_path() -> std::path::PathBuf {
    let dir = std::env::temp_dir();
    dir.join(format!(
        "velox-bench-{}.tmp",
        uuid::Uuid::new_v4().simple()
    ))
}
