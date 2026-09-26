//! Velox CLI — `velox <command>`
//!
//! Commands: add, list, show, pause, resume, cancel, remove, serve, bench,
//! doctor, verify. The `serve` command exposes a token-authenticated REST API
//! on 127.0.0.1 (used by the browser extension and third-party tools).

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::Duration;
use velox_core::config::EngineConfig;
use velox_core::engine::Engine;
use velox_core::types::{DownloadOptions, DownloadStatus};

#[derive(Parser)]
#[command(
    name = "velox",
    version,
    about = "Velox — every byte, at full speed. A memory-safe hybrid download engine.",
    after_help = "Run `velox serve` to expose the local REST API for browser integration."
)]
struct Cli {
    /// Verbose logging (-v info, -vv debug, -vvv trace)
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count)]
    verbose: u8,

    /// Override download/state directory
    #[arg(long, global = true)]
    dir: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Add one or more downloads
    Add {
        /// URLs (https/http). Add mirrors with --mirror.
        urls: Vec<String>,
        /// Additional mirrors of the same file
        #[arg(long = "mirror")]
        mirrors: Vec<String>,
        /// Destination directory
        #[arg(short, long)]
        out: Option<PathBuf>,
        /// Connections per download (1-32)
        #[arg(short = 'c', long)]
        connections: Option<u32>,
        /// Per-download speed limit, e.g. 5M, 800K
        #[arg(short, long)]
        limit: Option<String>,
        /// Force output filename
        #[arg(long)]
        name: Option<String>,
        /// Expected sha256 (hex); download fails on mismatch
        #[arg(long)]
        sha256: Option<String>,
        /// Extra header (repeatable): "Key: Value"
        #[arg(long = "header")]
        headers: Vec<String>,
        /// Do not wait for completion
        #[arg(long)]
        no_wait: bool,
    },
    /// List downloads (current session + history)
    List,
    /// Show one download in detail
    Show { id: String },
    /// Pause a download (or all with --all)
    Pause { id: Option<String>, #[arg(long)] all: bool },
    /// Resume a download (or all with --all)
    Resume { id: Option<String>, #[arg(long)] all: bool },
    /// Cancel a download
    Cancel { id: String },
    /// Remove from list (add --purge to delete partial data)
    Remove {
        id: String,
        #[arg(long)]
        purge: bool,
    },
    /// Start the local daemon + REST API (for the browser extension)
    Serve {
        #[arg(long, default_value = "7654")]
        port: u16,
        #[arg(long)]
        show_token: bool,
    },
    /// Benchmark a URL: 1-connection vs N-connection throughput
    Bench {
        url: String,
        #[arg(long, default_value = "64")]
        size_mb: u64,
        #[arg(long, default_value = "8")]
        connections: u32,
    },
    /// System diagnostics: network, TLS, DNS, disk, RAM
    Doctor,
    /// Verify a completed file against its recorded sha256
    Verify {
        path: PathBuf,
        #[arg(long)]
        sha256: Option<String>,
    },
}

fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim().to_lowercase();
    let (num, mult) = if let Some(n) = s.strip_suffix("g") {
        (n, 1024 * 1024 * 1024u64)
    } else if let Some(n) = s.strip_suffix("m") {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("k") {
        (n, 1024)
    } else {
        (s.as_str(), 1)
    };
    Ok((num.trim().parse::<f64>()? as u64).saturating_mul(mult))
}

fn parse_headers(specs: &[String]) -> Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    for h in specs {
        let (k, v) = h
            .split_once(':')
            .context(format!("bad header '{h}', expected 'Key: Value'"))?;
        out.push((k.trim().to_string(), v.trim().to_string()));
    }
    Ok(out)
}

fn fmt_bytes(n: u64) -> String {
    const U: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < 4 {
        v /= 1024.0;
        u += 1;
    }
    format!("{v:.1} {}", U[u])
}

fn fmt_speed(bps: f64) -> String {
    format!("{}/s", fmt_bytes(bps as u64))
}

fn fmt_eta(secs: Option<u64>) -> String {
    match secs {
        None => "—".into(),
        Some(s) => {
            let h = s / 3600;
            let m = (s % 3600) / 60;
            let sec = s % 60;
            if h > 0 {
                format!("{h}h{m:02}m")
            } else if m > 0 {
                format!("{m}m{sec:02}s")
            } else {
                format!("{sec}s")
            }
        }
    }
}

fn status_glyph(s: DownloadStatus) -> &'static str {
    match s {
        DownloadStatus::Queued => "⏳",
        DownloadStatus::Connecting => "📡",
        DownloadStatus::Downloading => "⬇",
        DownloadStatus::Paused => "⏸",
        DownloadStatus::Verifying => "🔎",
        DownloadStatus::Completed => "✔",
        DownloadStatus::Failed => "✖",
        DownloadStatus::Cancelled => "🚫",
    }
}

fn progress_bar(pct: f64, width: usize) -> String {
    let filled = ((pct / 100.0) * width as f64).round() as usize;
    let filled = filled.min(width);
    format!("{}{}", "█".repeat(filled), "░".repeat(width - filled))
}

fn engine_cfg(dir: &Option<PathBuf>) -> EngineConfig {
    let mut cfg = EngineConfig::default();
    if let Some(d) = dir {
        cfg.download_dir = d.clone();
        cfg.state_dir = Some(d.join(".velox-meta"));
    }
    cfg
}

fn short_id(id: &velox_core::types::DownloadId) -> String {
    id.to_string()[..8].to_string()
}

async fn make_engine(dir: &Option<PathBuf>) -> Result<std::sync::Arc<Engine>> {
    let engine = Engine::new(engine_cfg(dir)).context("failed to start engine")?;
    Ok(engine)
}

async fn wait_and_report(engine: &std::sync::Arc<Engine>, ids: Vec<velox_core::types::DownloadId>) -> Result<()> {
    use velox_core::events::EngineEvent;
    let mut rx = engine.events();
    let mut done = std::collections::HashSet::new();
    let mut last_print = std::time::Instant::now();
    loop {
        if done.len() == ids.len() {
            break;
        }
        // print live progress
        if last_print.elapsed() >= Duration::from_millis(400) {
            last_print = std::time::Instant::now();
            for &id in &ids {
                if done.contains(&id) {
                    continue;
                }
                if let Ok(s) = engine.snapshot(id) {
                    let total = s.total_size.map(fmt_bytes).unwrap_or_else(|| "?".into());
                    eprintln!(
                        "{} {} {} {:.1}%  {}/{}  {}  ETA {}  conn={}{}",
                        status_glyph(s.status),
                        short_id(&s.id),
                        progress_bar(s.progress_pct, 24),
                        s.progress_pct,
                        fmt_bytes(s.bytes_done),
                        total,
                        fmt_speed(s.speed_bps),
                        fmt_eta(s.eta_secs),
                        s.active_connections,
                        s.error
                            .as_ref()
                            .map(|e| format!("  ERR: {e}"))
                            .unwrap_or_default(),
                    );
                }
            }
        }
        match rx.try_recv() {
            Ok(ev) => {
                if let EngineEvent::DownloadStateChanged { id, to, error, .. } = ev {
                    match to {
                        DownloadStatus::Completed => {
                            done.insert(id);
                            eprintln!();
                            println!("✔ {} completed", short_id(&id));
                        }
                        DownloadStatus::Failed => {
                            done.insert(id);
                            eprintln!();
                            println!("✖ {} failed: {}", short_id(&id), error.unwrap_or_default());
                        }
                        _ => {}
                    }
                }
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {}
            Err(_) => break,
        }
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    let failed = ids
        .iter()
        .filter_map(|id| engine.snapshot(*id).ok())
        .any(|s| s.status == DownloadStatus::Failed);
    if failed {
        bail!("one or more downloads failed");
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();
    let filter = match cli.verbose {
        0 => "warn,velox_core=info",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .init();

    let r = run(cli).await;
    if let Err(e) = r {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    match cli.cmd {
        Cmd::Add {
            urls,
            mirrors,
            out,
            connections,
            limit,
            name,
            sha256,
            headers,
            no_wait,
        } => {
            if urls.is_empty() {
                bail!("no URLs given");
            }
            let engine = make_engine(&cli.dir).await?;
            let mut ids = Vec::new();
            for url in &urls {
                let mut opts = DownloadOptions {
                    dest_dir: out.clone(),
                    connections,
                    verify_sha256: sha256.clone(),
                    filename: name.clone(),
                    ..Default::default()
                };
                opts.headers = parse_headers(&headers)?;
                if !mirrors.is_empty() && urls.len() == 1 {
                    opts.mirrors = mirrors.clone();
                }
                opts.speed_limit = limit.as_deref().map(parse_size).transpose()?;
                let id = engine.add(url, opts)?;
                println!("added {} → {}", short_id(&id), url);
                ids.push(id);
            }
            if !no_wait {
                wait_and_report(&engine, ids).await?;
            }
            Ok(())
        }
        Cmd::List => {
            let engine = make_engine(&cli.dir).await?;
            let snaps = engine.list();
            if snaps.is_empty() {
                println!("no downloads");
                return Ok(());
            }
            println!(
                "{:<9} {:<4} {:<26} {:>10} {:>12} {:>10} {:>8}  {}",
                "ID", "ST", "NAME", "SIZE", "DONE", "SPEED", "ETA", "STATUS"
            );
            for s in snaps {
                println!(
                    "{:<9} {:<4} {:<26} {:>10} {:>12} {:>10} {:>8}  {}{}",
                    short_id(&s.id),
                    status_glyph(s.status),
                    truncate(&s.filename, 26),
                    s.total_size.map(fmt_bytes).unwrap_or_else(|| "?".into()),
                    fmt_bytes(s.bytes_done),
                    fmt_speed(s.speed_bps),
                    fmt_eta(s.eta_secs),
                    format!("{:.0}%", s.progress_pct),
                    s.error.as_ref().map(|e| format!(" ({e})")).unwrap_or_default(),
                );
            }
            Ok(())
        }
        Cmd::Show { id } => {
            let engine = make_engine(&cli.dir).await?;
            let target = resolve_id(&engine, &id)?;
            let s = engine.snapshot(target)?;
            println!("id:          {}", s.id);
            println!("url:         {}", s.url);
            println!("file:        {}", s.full_path);
            println!("status:      {:?}", s.status);
            println!(
                "size:        {} / {} ({:.2}%)",
                fmt_bytes(s.bytes_done),
                s.total_size.map(fmt_bytes).unwrap_or_else(|| "?".into()),
                s.progress_pct
            );
            println!("speed:       {}", fmt_speed(s.speed_bps));
            println!("connections: {}", s.active_connections);
            println!("retries:     {}", s.retries);
            println!("protocol:    {}", s.protocol_used.clone().unwrap_or_else(|| "—".into()));
            if s.alt_svc_h3 {
                println!("alt-svc:     HTTP/3 advertised by server");
            }
            println!("mirrors:     {}", s.mirrors);
            if let Some(h) = &s.sha256 {
                println!("sha256:      {h}");
            }
            if let Some(e) = &s.error {
                println!("error:       {e}");
            }
            if !s.segments.is_empty() {
                println!("segments ({}):", s.segments.len());
                for (i, seg) in s.segments.iter().enumerate() {
                    let pct = if seg.total() > 0 {
                        seg.written as f64 / seg.total() as f64 * 100.0
                    } else {
                        100.0
                    };
                    println!(
                        "  [{i:>2}] {} {} ({:.0}%) mirror={}",
                        progress_bar(pct, 30),
                        fmt_bytes(seg.written),
                        pct,
                        seg.mirror
                    );
                }
            }
            Ok(())
        }
        Cmd::Pause { id, all } => {
            let engine = make_engine(&cli.dir).await?;
            if all {
                engine.pause_all();
                println!("all paused");
            } else {
                let t = resolve_id(&engine, &id.expect("id"))?;
                engine.pause(t)?;
                println!("paused {}", short_id(&t));
            }
            Ok(())
        }
        Cmd::Resume { id, all } => {
            let engine = make_engine(&cli.dir).await?;
            if all {
                engine.resume_all();
                println!("all resumed");
            } else {
                let t = resolve_id(&engine, &id.expect("id"))?;
                engine.resume(t)?;
                println!("resumed {}", short_id(&t));
            }
            Ok(())
        }
        Cmd::Cancel { id } => {
            let engine = make_engine(&cli.dir).await?;
            let t = resolve_id(&engine, &id)?;
            engine.cancel(t)?;
            println!("cancelled {}", short_id(&t));
            Ok(())
        }
        Cmd::Remove { id, purge } => {
            let engine = make_engine(&cli.dir).await?;
            let t = resolve_id(&engine, &id)?;
            engine.remove(t, purge, false)?;
            println!("removed {}", short_id(&t));
            Ok(())
        }
        Cmd::Serve { port, show_token } => {
            serve::run(port, show_token, engine_cfg(&cli.dir)).await
        }
        Cmd::Bench { url, size_mb, connections } => {
            println!("Velox benchmark: {url}");
            println!("  phase 1: single connection, {size_mb} MiB …");
            let cfg = engine_cfg(&cli.dir);
            let b1 = velox_core::bench::bench_download(&cfg, &url, 1, size_mb * 1024 * 1024).await?;
            println!(
                "  1 conn:  {:>8.2} MiB/s  ({:.2}s, {:?})",
                b1.speed_mbps, b1.elapsed_secs, b1.http_version
            );
            if connections > 1 {
                println!("  phase 2: {connections} connections, {size_mb} MiB …");
                let b2 =
                    velox_core::bench::bench_download(&cfg, &url, connections, size_mb * 1024 * 1024).await?;
                println!(
                    "  {connections:>2} conn:  {:>8.2} MiB/s  ({:.2}s, {:?})",
                    b2.speed_mbps, b2.elapsed_secs, b2.http_version
                );
                if b1.speed_mbps > 0.1 {
                    println!("  speedup: {:.2}x", b2.speed_mbps / b1.speed_mbps);
                }
            }
            Ok(())
        }
        Cmd::Doctor => doctor::run().await,
        Cmd::Verify { path, sha256 } => {
            let expect = match sha256 {
                Some(h) => h,
                None => {
                    // find sidecar meta of a completed download
                    bail!("provide --sha256 <hex> (recorded hash is shown by `velox show`)")
                }
            };
            println!("hashing {} …", path.display());
            let got = velox_core::storage::sha256_file(&path, None)?;
            if got == expect.to_lowercase() {
                println!("✔ OK  sha256 = {got}");
            } else {
                println!("✖ MISMATCH\n  expected: {expect}\n  actual:   {got}");
                std::process::exit(1);
            }
            Ok(())
        }
    }
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let t: String = s.chars().take(n - 1).collect();
        format!("{t}…")
    }
}

fn resolve_id(engine: &std::sync::Arc<Engine>, id: &str) -> Result<velox_core::types::DownloadId> {
    let full = uuid::Uuid::parse_str(id).ok();
    for s in engine.list() {
        if s.id == full.unwrap_or_default() || s.id.to_string().starts_with(id) {
            return Ok(s.id);
        }
    }
    bail!("no download matches id '{id}'");
}

mod serve;
mod doctor;
