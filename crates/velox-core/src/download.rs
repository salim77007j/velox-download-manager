//! Download supervisor: probes the source, allocates storage, spawns segment
//! workers, performs dynamic splitting, handles pause/resume/cancel, verifies
//! integrity and finalizes. One supervisor task per active download.

use crate::config::EngineConfig;
use crate::error::{Result, VeloxError};
use crate::filename;
use crate::http::{self, ProbeResult};
use crate::segment::{run_worker, WorkerCtx, WorkerOutcome};
use crate::speed::SpeedTracker;
use crate::storage::{sha256_file, Sidecar, SparseFile, PART_SUFFIX};
use crate::types::{
    DownloadId, DownloadShared, DownloadSnapshot, DownloadState, DownloadStatus, SegmentState,
};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use url::Url;

/// Control messages sent by the engine to the supervisor.
#[derive(Debug)]
pub enum Ctrl {
    Pause,
    Resume,
    Cancel,
    SetLimit(Option<u64>),
}

pub(crate) struct SupervisorDeps {
    pub client: reqwest::Client,
    pub cfg: Arc<parking_lot::RwLock<EngineConfig>>,
    pub global_limiter: Arc<crate::limiter::RateLimiter>,
    pub buffer_budget: Arc<crate::memory::BufferBudget>,
    #[allow(dead_code)]
    pub global_speed: Arc<SpeedTracker>,
    pub events: tokio::sync::broadcast::Sender<crate::events::EngineEvent>,
    pub memory: Arc<crate::memory::MemoryMonitor>,
}

pub(crate) fn part_path_for(state: &DownloadState) -> std::path::PathBuf {
    state.dest_dir.join(format!("{}{}", state.filename, PART_SUFFIX))
}

fn emit(deps: &SupervisorDeps, ev: crate::events::EngineEvent) {
    let _ = deps.events.send(ev);
}

/// Persist the sidecar using DISK-TRUE segment positions. Never mutates the
/// live claim ledger (workers own it while running).
fn persist(_deps: &SupervisorDeps, shared: &DownloadShared) {
    let part = {
        let st = shared.state.read();
        part_path_for(&st)
    };
    let snapshot = {
        let mut st = shared.state.read().clone();
        st.updated_at = chrono::Utc::now();
        let d = shared.disk_upto.lock().clone();
        for (i, seg) in st.segments.iter_mut().enumerate() {
            if let Some(pos) = d.get(i) {
                seg.written = pos.saturating_sub(seg.start).min(seg.total());
            }
        }
        st.bytes_done = st.segments.iter().map(|s| s.written).sum();
        st
    };
    tokio::task::spawn_blocking(move || {
        if let Err(e) = Sidecar::save(&part, &snapshot) {
            tracing::warn!("sidecar save failed: {e}");
        }
    });
}

fn set_status(deps: &SupervisorDeps, shared: &DownloadShared, to: DownloadStatus, err: Option<String>) {
    let (id, from) = {
        let mut st = shared.state.write();
        let from = st.status;
        st.status = to;
        st.error = err.clone();
        st.updated_at = chrono::Utc::now();
        (st.id, from)
    };
    if to == DownloadStatus::Completed {
        // finalize already renamed the part file and removed its sidecar —
        // persisting here would resurrect a meta file for a non-existent part.
        let part = {
            let st = shared.state.read();
            part_path_for(&st)
        };
        tokio::task::spawn_blocking(move || Sidecar::delete(&part));
    } else {
        persist(deps, shared);
    }
    emit(
        deps,
        crate::events::EngineEvent::DownloadStateChanged {
            id,
            from,
            to,
            error: err,
        },
    );
}

/// Build N segments covering [0, size).
pub fn split_into_segments(size: u64, n: u32, min_segment: u64) -> Vec<SegmentState> {
    let n = n.max(1);
    let n = n.min(size.max(1) as u32); // never more segments than bytes
    let n = if size / u64::from(n) < min_segment && size > min_segment {
        (size / min_segment).max(1) as u32
    } else {
        n
    };
    let chunk = size / u64::from(n);
    let mut segments = Vec::with_capacity(n as usize);
    let mut start = 0u64;
    for i in 0..n {
        let len = if i == n - 1 { size - start } else { chunk };
        segments.push(SegmentState {
            start,
            end: start + len - 1,
            written: 0,
            mirror: 0,
        });
        start += len;
    }
    segments
}

pub(crate) async fn supervise(
    shared: Arc<DownloadShared>,
    deps: Arc<SupervisorDeps>,
    mut ctrl_rx: mpsc::UnboundedReceiver<Ctrl>,
) {
    // Main lifecycle: (prepare → run) repeated for pause/resume cycles.
    let mut restarts: u32 = 0;
    loop {
        let cfg_max_retries = deps.cfg.read().max_retries;
        // ---- PREPARE ------------------------------------------------------
        set_status(&deps, &shared, DownloadStatus::Connecting, None);
        let prepare = prepare_download(&shared, &deps).await;
        match prepare {
            Ok(PrepareResult::CompleteAlready) => {
                if let Err(e) = finalize(&shared, &deps).await {
                    fail(&shared, &deps, e).await;
                    return;
                }
                return;
            }
            Ok(PrepareResult::Ready {
                file,
                conns,
                chunk_size,
                unknown_total,
            }) => {
                let run = run_phase(
                    &shared, &deps, &mut ctrl_rx, file, conns, chunk_size, unknown_total,
                )
                .await;
                match run {
                    RunResult::Completed => {
                        if let Err(e) = finalize(&shared, &deps).await {
                            fail(&shared, &deps, e).await;
                            return;
                        }
                        return;
                    }
                    RunResult::Paused => {
                        set_status(&deps, &shared, DownloadStatus::Paused, None);
                        // Stay alive; wait for Resume/Cancel from the engine.
                        loop {
                            match ctrl_rx.recv().await {
                                Some(Ctrl::Resume) => break, // outer loop restarts phase
                                Some(Ctrl::Cancel) => {
                                    set_status(&deps, &shared, DownloadStatus::Cancelled, None);
                                    return;
                                }
                                Some(Ctrl::SetLimit(l)) => shared.limiter.set_rate(l),
                                Some(Ctrl::Pause) => {}
                                None => return, // engine dropped
                            }
                        }
                    }
                    RunResult::Cancelled => {
                        set_status(&deps, &shared, DownloadStatus::Cancelled, None);
                        return;
                    }
                    RunResult::Restart => {
                        // Server ignored Range mid-stream: fall back to one
                        // single-stream attempt from byte 0.
                        restarts += 1;
                        if restarts > cfg_max_retries {
                            fail(&shared, &deps, VeloxError::NoRangeSupport).await;
                            return;
                        }
                        tracing::info!(
                            "download {}: server ignored Range → single-stream restart {restarts}",
                            shared.id
                        );
                        reset_for_single_stream(&shared);
                        continue; // re-prepare + re-run
                    }
                    RunResult::Failed(e) => {
                        fail(&shared, &deps, e).await;
                        return;
                    }
                    RunResult::EngineGone => return,
                }
            }
            Err(e) => {
                fail(&shared, &deps, e).await;
                return;
            }
        }
    }
}

async fn fail(shared: &Arc<DownloadShared>, deps: &Arc<SupervisorDeps>, e: VeloxError) {
    tracing::error!("download {}: failed: {e}", shared.id);
    set_status(deps, shared, DownloadStatus::Failed, Some(e.to_string()));
}

enum PrepareResult {
    CompleteAlready,
    Ready {
        file: Arc<SparseFile>,
        conns: u32,
        chunk_size: usize,
        unknown_total: bool,
    },
}

#[allow(dead_code)]
enum PrepareErr {
    #[allow(dead_code)]
    Fatal(VeloxError),
    #[allow(dead_code)]
    Retry(VeloxError),
}

impl From<VeloxError> for PrepareErr {
    fn from(e: VeloxError) -> Self {
        PrepareErr::Fatal(e)
    }
}

#[allow(dead_code)]
fn probe_final_http2(_client: &reqwest::Client) -> bool {
    // Kept for potential ALPN-level diagnostics; per-response version is now
    // captured directly by the probe.
    true
}

async fn prepare_download(
    shared: &Arc<DownloadShared>,
    deps: &Arc<SupervisorDeps>,
) -> std::result::Result<PrepareResult, VeloxError> {
    let cfg = deps.cfg.read().clone();
    let (url, opts) = {
        let st = shared.state.read();
        (st.url.clone(), st.options.clone())
    };
    let primary = url::Url::parse(&url).map_err(|e| VeloxError::InvalidUrl(format!("parse: {e}")))?;

    // Probe with retry/backoff (network errors are retryable).
    let probe: ProbeResult = loop {
        match http::probe(
            &deps.client,
            &primary,
            &opts.headers,
            opts.username.as_deref().zip(opts.password.as_deref()),
        )
        .await
        {
            Ok(p) => break p,
            Err(e) if !e.retryable() => return Err(e), // fatal (unsafe redirect…)
            Err(VeloxError::HttpStatus { status, .. })
                if (400..=499).contains(&status) && status != 429 =>
            {
                // permanent client error (404, 403, …): no point retrying
                return Err(VeloxError::HttpStatus {
                    status,
                    url: primary.to_string(),
                });
            }
            Err(e) => {
                let retries_now = {
                    let mut st = shared.state.write();
                    st.retries += 1;
                    st.retries
                };
                if retries_now as u32 > cfg.max_retries {
                    return Err(e);
                }
                let backoff = cfg
                    .retry_backoff_base_ms
                    .saturating_mul(2u32.saturating_pow(retries_now.min(10) as u32) as u64)
                    .min(cfg.retry_backoff_max_ms);
                tracing::warn!("probe retry #{retries_now} in {backoff}ms: {e}");
                tokio::time::sleep(Duration::from_millis(backoff)).await;
                if shared.current_token().is_cancelled() {
                    return Err(VeloxError::Cancelled);
                }
            }
        }
    };

    {
        let mut st = shared.state.write();
        st.retries = 0;
        st.alt_svc_h3 = probe.advertises_h3();
        st.protocol_used = probe.http_version.clone();
        st.content_type = probe.content_type.clone();
        if st.expected_sha256.is_none() {
            st.expected_sha256 = opts.verify_sha256.clone();
        }
    }

    // Choose final filename (only before any bytes exist).
    let desired = filename::resolve_filename(
        opts.filename.as_deref(),
        probe.content_disposition.as_deref(),
        &probe.final_url,
        probe.content_type.as_deref(),
    );
    {
        let mut st = shared.state.write();
        if st.segments.is_empty() || st.bytes_done == 0 {
            if st.filename != desired {
                // migrate part file if it exists
                let old_part = part_path_for(&st);
                st.filename = desired.clone();
                let new_part = part_path_for(&st);
                if old_part.exists() {
                    let _ = std::fs::rename(&old_part, &new_part);
                }
                Sidecar::delete(&old_part);
            }
        }
    }

    // Mirror stats + final URL bookkeeping
    {
        let mut st = shared.state.write();
        if st.mirror_stats.len() != st.mirrors.len() + 1 {
            st.init_mirror_stats();
        }
        st.url = probe.final_url.as_str().to_string();
    }

    // Fresh vs resume decision
    let (resume_ok, total, ranges) = {
        let st = shared.state.read();
        let etag_match = match (&st.etag, &probe.etag) {
            (Some(a), Some(b)) => a == b,
            (None, _) => true, // no etag recorded: rely on size
            (_, None) => st.etag.is_none(),
        };
        let size_match = st.total_size == probe.total_size;
        let has_segments = !st.segments.is_empty();
        (has_segments && etag_match && size_match, probe.total_size, probe.supports_ranges)
    };

    if total == Some(0) {
        // empty file: create + complete
        let st_path = {
            let st = shared.state.read();
            st.dest_dir.join(&st.filename)
        };
        std::fs::write(&st_path, b"")?;
        let mut st = shared.state.write();
        st.total_size = Some(0);
        st.bytes_done = 0;
        st.segments.clear();
        return Ok(PrepareResult::CompleteAlready);
    }

    // Record etag/last-modified/total
    {
        let mut st = shared.state.write();
        st.etag = probe.etag.clone();
        st.last_modified = probe.last_modified.clone();
        st.total_size = probe.total_size;
        st.supports_ranges = ranges && !opts.force_single_connection;
    }

    // Allocate / open file
    let part = {
        let st = shared.state.read();
        part_path_for(&st)
    };
    std::fs::create_dir_all(&shared.state.read().dest_dir)?;
    let file = Arc::new(SparseFile::create(part.clone(), probe.total_size)?);

    let single_mode = !ranges || opts.force_single_connection;
    if !resume_ok || single_mode {
        // Fresh start — or a no-range server: partial data cannot be resumed
        // without range support, so collapse to one stream from byte 0.
        let conns = cfg.effective_connections(opts.connections, single_mode);
        let segments = match probe.total_size {
            Some(size) => split_into_segments(size, conns, cfg.min_segment_size),
            None => vec![SegmentState { start: 0, end: u64::MAX, written: 0, mirror: 0 }],
        };
        let mut st = shared.state.write();
        st.segments = segments;
        st.bytes_done = 0;
        if single_mode {
            st.supports_ranges = false;
            if let Ok(f) = std::fs::OpenOptions::new().write(true).create(true).truncate(true)
                .open(part_path_for(&st))
            {
                drop(f); // truncated part file
            }
        }
    } else {
        // Resume: reconcile ledger with disk truth
        let mut st = shared.state.write();
        let size = probe.total_size;
        if st.segments.is_empty() {
            let conns = cfg.effective_connections(opts.connections, false);
            st.segments = match size {
                Some(s) => split_into_segments(s, conns, cfg.min_segment_size),
                None => vec![SegmentState { start: 0, end: u64::MAX, written: 0, mirror: 0 }],
            };
        }
        for seg in st.segments.iter_mut() {
            if seg.written > seg.total() {
                seg.written = seg.total();
            }
        }
        st.bytes_done = st.segments.iter().map(|s| s.written).sum();
    }

    {
        let st = shared.state.read();
        *shared.disk_upto.lock() = st.segments.iter().map(|s| s.start + s.written).collect();
    }

    // RAM-adaptive chunk size
    let (avail, _total_ram) = deps.memory.sample();
    let budget = cfg.ram_budget(avail);
    deps.buffer_budget.set_budget(budget);
    let profile = crate::memory::buffer_profile(avail, budget);
    let chunk_size = profile.chunk_size as usize;

    let unknown_total = probe.total_size.is_none();
    let conns = cfg.effective_connections(opts.connections, !ranges || opts.force_single_connection);

    Ok(PrepareResult::Ready { file, conns, chunk_size, unknown_total })
}

enum RunResult {
    Completed,
    Paused,
    Cancelled,
    Restart,
    Failed(VeloxError),
    EngineGone,
}

/// Reset a download to a single fresh stream (used when a server ignores Range).
fn reset_for_single_stream(shared: &DownloadShared) {
    let part = {
        let st = shared.state.read();
        part_path_for(&st)
    };
    if let Ok(_) = std::fs::OpenOptions::new().write(true).create(true).truncate(true).open(&part) {
        // truncated
    }
    let mut st = shared.state.write();
    let end = st.total_size.map(|s| s.saturating_sub(1)).unwrap_or(u64::MAX);
    st.segments = vec![SegmentState { start: 0, end, written: 0, mirror: 0 }];
    st.supports_ranges = false; // stop sending Range on reconnects
    st.bytes_done = 0;
    st.retries += 1;
    *shared.disk_upto.lock() = vec![0];
}

#[allow(clippy::too_many_arguments)]
async fn run_phase(
    shared: &Arc<DownloadShared>,
    deps: &Arc<SupervisorDeps>,
    ctrl_rx: &mut mpsc::UnboundedReceiver<Ctrl>,
    file: Arc<SparseFile>,
    conns: u32,
    chunk_size: usize,
    unknown_total: bool,
) -> RunResult {
    let token = shared.renew_token();
    let cfg = deps.cfg.read().clone();

    set_status(deps, shared, DownloadStatus::Downloading, None);

    let mut js: JoinSet<WorkerOutcome> = JoinSet::new();

    // initial workers
    spawn_workers(shared, deps, &file, conns, chunk_size, &token, &mut js, unknown_total).await;

    let mut persist_tick = tokio::time::interval(Duration::from_millis(cfg.persist_interval_ms.max(250)));
    persist_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let result = loop {
        tokio::select! {
            ctrl = ctrl_rx.recv() => match ctrl {
                Some(Ctrl::Pause) => {
                    token.cancel();
                    while let Some(res) = js.join_next().await { let _ = res; }
                    shared.clamp_claims_to_disk();
                    persist(deps, shared);
                    break RunResult::Paused;
                }
                Some(Ctrl::Cancel) => {
                    token.cancel();
                    while let Some(res) = js.join_next().await { let _ = res; }
                    shared.clamp_claims_to_disk();
                    persist(deps, shared);
                    break RunResult::Cancelled;
                }
                Some(Ctrl::SetLimit(l)) => shared.limiter.set_rate(l),
                Some(Ctrl::Resume) => {}
                None => {
                    token.cancel();
                    while let Some(res) = js.join_next().await { let _ = res; }
                    break RunResult::EngineGone;
                }
            },
            _ = persist_tick.tick() => {
                persist(deps, shared);
                spawn_more(shared, deps, &file, conns, chunk_size, &token, &mut js, unknown_total).await;
            },
            res = js.join_next(), if !js.is_empty() => match res {
                Some(Ok(WorkerOutcome::Done)) | Some(Ok(WorkerOutcome::DoneEof)) => {
                    spawn_more(shared, deps, &file, conns, chunk_size, &token, &mut js, unknown_total).await;
                    if all_done(shared) {
                        shared.clamp_claims_to_disk();
                        break RunResult::Completed;
                    }
                }
                Some(Ok(WorkerOutcome::Stopped)) => { /* will be re-examined via ctrl */ }
                Some(Ok(WorkerOutcome::Fatal(e))) => {
                    token.cancel();
                    while let Some(r) = js.join_next().await { let _ = r; }
                    if matches!(e, VeloxError::NoRangeSupport) {
                        break RunResult::Restart;
                    }
                    break RunResult::Failed(e);
                }
                Some(Err(join_err)) => {
                    tracing::error!("worker join error: {join_err}");
                }
                None => { /* empty set */ }
            },
            else => break RunResult::Failed(VeloxError::Other("worker pool exhausted".into())),
        }
    };
    result
}

/// Completion check uses DISK-TRUE positions, never the claim ledger.
fn all_done(shared: &DownloadShared) -> bool {
    let st = shared.state.read();
    if st.total_size == Some(0) {
        return true;
    }
    if st.segments.is_empty() {
        return false;
    }
    let d = shared.disk_upto.lock();
    st.segments.iter().enumerate().all(|(i, s)| {
        let disk = d.get(i).copied().unwrap_or(s.start);
        disk.saturating_sub(s.start) >= s.total()
    })
}

#[allow(clippy::too_many_arguments)]
async fn spawn_workers(
    shared: &Arc<DownloadShared>,
    deps: &Arc<SupervisorDeps>,
    file: &Arc<SparseFile>,
    conns: u32,
    chunk_size: usize,
    token: &CancellationToken,
    js: &mut JoinSet<WorkerOutcome>,
    _unknown_total: bool,
) {
    let cfg = deps.cfg.read().clone();
    let urls = {
        let st = shared.state.read();
        let mut urls = vec![st.url.clone()];
        urls.extend(st.mirrors.iter().cloned());
        urls
    };
    let urls: Vec<Url> = urls.iter().filter_map(|u| Url::parse(u).ok()).collect();
    if urls.is_empty() {
        return;
    }
    let target = conns as usize;
    let seg_count = shared.state.read().segments.len().min(target);
    for seg_index in 0..seg_count {
        let ctx = WorkerCtx {
            client: deps.client.clone(),
            urls: urls.clone(),
            shared: shared.clone(),
            seg_index,
            file: file.clone(),
            global_limiter: deps.global_limiter.clone(),
            io_timeout: Duration::from_millis(cfg.io_timeout_ms),
            max_retries: cfg.max_retries,
            backoff_base: Duration::from_millis(cfg.retry_backoff_base_ms),
            backoff_max: Duration::from_millis(cfg.retry_backoff_max_ms),
            chunk_size,
            token: token.clone(),
            min_segment_size: cfg.min_segment_size,
        };
        js.spawn(run_worker(ctx));
    }
}

/// Spawn additional workers via dynamic split while capacity remains.
#[allow(clippy::too_many_arguments)]
async fn spawn_more(
    shared: &Arc<DownloadShared>,
    deps: &Arc<SupervisorDeps>,
    file: &Arc<SparseFile>,
    conns: u32,
    chunk_size: usize,
    token: &CancellationToken,
    js: &mut JoinSet<WorkerOutcome>,
    unknown_total: bool,
) {
    if unknown_total || conns <= 1 {
        return;
    }
    let cfg = deps.cfg.read().clone();
    let target = conns as usize;
    loop {
        if js.len() >= target || token.is_cancelled() {
            return;
        }
        // find splittable segment: largest remaining >= 2*min_segment
        let split = {
            let mut st = shared.state.write();
            let mut best: Option<(usize, u64)> = None;
            for (i, seg) in st.segments.iter().enumerate() {
                let remaining = seg.remaining();
                if remaining >= 2 * cfg.min_segment_size {
                    if best.map(|(_, r)| remaining > r).unwrap_or(true) {
                        best = Some((i, remaining));
                    }
                }
            }
            match best {
                Some((i, _remaining)) => {
                    // SPLIT: take the tail half into a new segment.
                    let seg = &mut st.segments[i];
                    let take_tail = seg.remaining() / 2;
                    let new_start = seg.end + 1 - take_tail;
                    let new_seg = SegmentState {
                        start: new_start,
                        end: seg.end,
                        written: 0,
                        mirror: seg.mirror,
                    };
                    seg.end = new_start - 1;
                    st.segments.push(new_seg);
                    let idx = st.segments.len() - 1;
                    let pos = st.segments[i].start + st.segments[i].written;
                    (idx, pos)
                }
                None => return,
            }
        };
        let (new_index, _pos) = split;
        shared.disk_upto.lock().push(0);

        let urls = {
            let st = shared.state.read();
            let mut urls = vec![st.url.clone()];
            urls.extend(st.mirrors.iter().cloned());
            urls
        };
        let urls: Vec<Url> = urls.iter().filter_map(|u| Url::parse(u).ok()).collect();
        if urls.is_empty() {
            return;
        }
        let ctx = WorkerCtx {
            client: deps.client.clone(),
            urls,
            shared: shared.clone(),
            seg_index: new_index,
            file: file.clone(),
            global_limiter: deps.global_limiter.clone(),
            io_timeout: Duration::from_millis(cfg.io_timeout_ms),
            max_retries: cfg.max_retries,
            backoff_base: Duration::from_millis(cfg.retry_backoff_base_ms),
            backoff_max: Duration::from_millis(cfg.retry_backoff_max_ms),
            chunk_size,
            token: token.clone(),
            min_segment_size: cfg.min_segment_size,
        };
        tracing::debug!("spawned split worker for segment {new_index} (pool {}/{target})", js.len() + 1);
        js.spawn(run_worker(ctx));
    }
}

/// Verification + finalize: hash check, fsync, rename part → final.
async fn finalize(shared: &Arc<DownloadShared>, deps: &Arc<SupervisorDeps>) -> Result<()> {
    set_status(deps, shared, DownloadStatus::Verifying, None);

    let (part, final_path, expected, _compute, fsync, total) = {
        let st = shared.state.read();
        let cfg = deps.cfg.read();
        (
            part_path_for(&st),
            st.dest_dir.join(&st.filename),
            st.expected_sha256.clone(),
            cfg.compute_sha256_on_complete,
            cfg.fsync_on_complete,
            st.total_size,
        )
    };

    // Size sanity
    if let Some(t) = total {
        let actual = std::fs::metadata(&part).map(|m| m.len()).unwrap_or(0);
        if actual < t {
            return Err(VeloxError::Other(format!(
                "file truncated on disk: {actual}/{t} bytes"
            )));
        }
    }

    // Hash verification
    let cancel = shared.current_token();
    let hash = sha256_file(&part, Some(&cancel)).ok();
    if let Some(expected_hex) = &expected {
        match &hash {
            Some(h) if h != expected_hex => {
                return Err(VeloxError::IntegrityMismatch {
                    expected: expected_hex.clone(),
                    actual: h.clone(),
                });
            }
            Some(_) => {}
            None => {
                return Err(VeloxError::Other("hash computation cancelled".into()));
            }
        }
    }

    {
        let f = SparseFile::open(part.clone())?;
        f.finalize(&final_path, total, fsync)?;
    }
    Sidecar::delete(&part);

    let duration = {
        let mut st = shared.state.write();
        st.sha256 = hash;
        st.status = DownloadStatus::Completed;
        st.completed_at = Some(chrono::Utc::now());
        st.bytes_done = st.total_bytes();
        st.completed_at.unwrap().signed_duration_since(st.created_at).num_milliseconds() as f64 / 1000.0
    };
    emit(
        deps,
        crate::events::EngineEvent::DownloadCompleted {
            id: shared.id,
            sha256: shared.state.read().sha256.clone(),
            bytes: shared.state.read().bytes_done,
            duration_secs: duration,
            avg_speed_bps: if duration > 0.0 { shared.state.read().bytes_done as f64 / duration } else { 0.0 },
        },
    );
    set_status(deps, shared, DownloadStatus::Completed, None);
    Ok(())
}

/// Create the initial shared state for a new download (engine.add).
pub(crate) fn new_shared(
    id: DownloadId,
    state: DownloadState,
    buffer_budget: Arc<crate::memory::BufferBudget>,
    global_speed: Arc<SpeedTracker>,
    default_limit: Option<u64>,
) -> Arc<DownloadShared> {
    let limiter = Arc::new(crate::limiter::RateLimiter::new(
        state.options.speed_limit.or(default_limit),
        256 * 1024,
    ));
    Arc::new(DownloadShared {
        id,
        state: parking_lot::RwLock::new(state),
        speed: SpeedTracker::new(),
        global_speed,
        active: std::sync::atomic::AtomicUsize::new(0),
        buffer_budget,
        limiter,
        token: Mutex::new(CancellationToken::new()),
        disk_upto: Mutex::new(Vec::new()),
    })
}

/// Build a snapshot for UI (used by engine.list).
#[allow(dead_code)]
pub(crate) fn snapshot_of(shared: &DownloadShared) -> DownloadSnapshot {
    shared.snapshot()
}

/// Disk scanner: recover all downloads from a state dir (index + sidecars).
pub fn scan_state_dir(state_dir: &std::path::Path) -> Vec<DownloadState> {
    let mut out = Vec::new();
    let idx_path = state_dir.join("index.json");
    if let Ok(bytes) = std::fs::read(&idx_path) {
        if let Ok(list) = serde_json::from_slice::<Vec<IndexEntry>>(&bytes) {
            for entry in list {
                let part = std::path::PathBuf::from(&entry.part_path);
                if let Ok(Some(state)) = Sidecar::load(&part) {
                    out.push(state);
                } else if entry.completed {
                    // completed download, keep an entry for history
                    out.push(entry.reconstruct_state());
                }
            }
        }
    }
    out
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexEntry {
    pub id: DownloadId,
    pub part_path: String,
    pub completed: bool,
    /// minimal info for history of completed downloads
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub filename: String,
    #[serde(default)]
    pub dest_dir: String,
    #[serde(default)]
    pub total_size: Option<u64>,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl IndexEntry {
    fn reconstruct_state(&self) -> DownloadState {
        let mut st = DownloadState {
            id: self.id,
            url: self.url.clone(),
            filename: self.filename.clone(),
            dest_dir: std::path::PathBuf::from(&self.dest_dir),
            total_size: self.total_size,
            status: DownloadStatus::Completed,
            sha256: self.sha256.clone(),
            created_at: self.created_at.unwrap_or_else(chrono::Utc::now),
            completed_at: self.completed_at,
            ..Default::default()
        };
        st.bytes_done = self.total_size.unwrap_or(0);
        st
    }
}

/// Write the index file listing active downloads.
pub fn save_index(state_dir: &std::path::Path, entries: &[IndexEntry]) -> Result<()> {
    std::fs::create_dir_all(state_dir)?;
    let tmp = state_dir.join("index.json.tmp");
    let path = state_dir.join("index.json");
    let json = serde_json::to_vec_pretty(entries)?;
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&json)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

pub fn load_index(state_dir: &std::path::Path) -> Vec<IndexEntry> {
    let path = state_dir.join("index.json");
    std::fs::read(&path)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
}


/// Atomic add: append id to index (called by engine).
pub fn upsert_index(state_dir: &std::path::Path, entry: IndexEntry) -> Result<()> {
    let mut entries = load_index(state_dir);
    entries.retain(|e| e.id != entry.id);
    entries.push(entry);
    save_index(state_dir, &entries)
}
