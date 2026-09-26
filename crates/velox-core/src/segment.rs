//! Segment worker: streams one byte-segment of the download into the file at
//! absolute offsets, with retry/backoff, mirror rotation and dynamic-safety
//! invariants that make concurrent writes to the same file impossible to
//! interleave incorrectly:
//!
//!   1. CLAIM-THEN-WRITE: a worker advances `seg.written` under the state lock
//!      *before* touching the network for those bytes; a dynamic split can
//!      therefore only take ranges strictly beyond the claimed boundary.
//!   2. DISK-TRUE PERSISTENCE: sidecar snapshots use `disk_upto` (bytes actually
//!      written), never the in-memory claim ledger, so a crash at any instant
//!      resumes from a correct offset.
//!   3. CLAMPED WRITES: workers cap every write at their segment's current end;
//!      a 200 response to a ranged request at offset>0 is refused (never written).

use crate::error::{Result, VeloxError};
use crate::storage::SparseFile;
use crate::types::DownloadShared;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use url::Url;

#[derive(Debug)]
pub enum WorkerOutcome {
    /// Segment fully written.
    Done,
    /// Segment completed and it was the unknown-length tail (EOF reached).
    DoneEof,
    /// Stop because pause/cancel was requested.
    Stopped,
    /// Retries exhausted / unrecoverable.
    Fatal(VeloxError),
}

pub(crate) struct WorkerCtx {
    pub client: reqwest::Client,
    pub urls: Vec<Url>,
    pub shared: Arc<DownloadShared>,
    pub seg_index: usize,
    pub file: Arc<SparseFile>,
    pub global_limiter: Arc<crate::limiter::RateLimiter>,
    pub io_timeout: Duration,
    pub max_retries: u32,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    pub chunk_size: usize,
    pub token: CancellationToken,
    pub min_segment_size: u64,
}

impl Clone for WorkerCtx {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            urls: self.urls.clone(),
            shared: self.shared.clone(),
            seg_index: self.seg_index,
            file: self.file.clone(),
            global_limiter: self.global_limiter.clone(),
            io_timeout: self.io_timeout,
            max_retries: self.max_retries,
            backoff_base: self.backoff_base,
            backoff_max: self.backoff_max,
            chunk_size: self.chunk_size,
            token: self.token.clone(),
            min_segment_size: self.min_segment_size,
        }
    }
}

/// Stream a single segment to completion (or stop/fatal).
pub(crate) async fn run_worker(ctx: WorkerCtx) -> WorkerOutcome {
    let mut stream_pos: Option<u64> = None; // None = no connection yet; Some = socket position
    let mut stream: Option<reqwest::Response> = None;
    let mut retries: u32 = 0;
    let mut buf: Vec<u8> = vec![0u8; ctx.chunk_size];
    let mut carry: Vec<u8> = Vec::new(); // overshoot bytes already off the wire

    // Register buffer usage against the RAM-adaptive budget
    ctx.shared
        .buffer_budget
        .register(ctx.chunk_size as u64);
    ctx.shared.active.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let outcome = worker_inner(
        ctx.clone(), &mut stream, &mut stream_pos, &mut retries, &mut buf, &mut carry,
    )
    .await;

    ctx.shared
        .active
        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    ctx.shared
        .buffer_budget
        .release(ctx.chunk_size as u64);

    // Close connection explicitly
    if let Some(resp) = stream.take() {
        drop(resp);
    }
    outcome
}

async fn worker_inner(
    ctx: WorkerCtx,
    stream: &mut Option<reqwest::Response>,
    stream_pos: &mut Option<u64>,
    retries: &mut u32,
    buf: &mut Vec<u8>,
    carry: &mut Vec<u8>,
) -> WorkerOutcome {
    loop {
        if ctx.token.is_cancelled() {
            rollback_claim(&ctx);
            return WorkerOutcome::Stopped;
        }

        // ---- 1) CLAIM the next chunk (in-memory ledger) --------------------
        let (offset, want, _seg_end, unknown_total) = {
            let mut st = ctx.shared.state.write();
            let unknown = st.total_size.is_none();
            let seg = match st.segments.get_mut(ctx.seg_index) {
                Some(s) => s,
                None => return WorkerOutcome::Fatal(VeloxError::Other("segment vanished".into())),
            };
            if seg.written >= seg.total() {
                drop(st);
                return WorkerOutcome::Done;
            }
            let remaining = seg.total() - seg.written;
            let want = (ctx.chunk_size as u64).min(remaining);
            let offset = seg.start + seg.written;
            seg.written += want; // CLAIM (may be rolled back on failure)
            (offset, want, seg.end, unknown)
        };

        // ---- 2) Ensure a healthy stream whose next byte is `offset` --------
        // Invariant: with a live stream, next_byte = sock_pos - carry.len().
        let stream_aligned = match *stream_pos {
            Some(pos) => pos - carry.len() as u64 == offset,
            None => false,
        };
        if !stream_aligned {
            match open_stream(&ctx, offset).await {
                Ok(resp) => {
                    *stream = Some(resp);
                    *stream_pos = Some(offset);
                    carry.clear();
                    *retries = 0; // successful (re)connect resets the retry streak
                }
                Err(e) => {
                    rollback_claim_to(&ctx, offset);
                    carry.clear();
                    *stream_pos = None;
                    match handle_failure(&ctx, retries, e).await {
                        FailureFlow::Retry => continue,
                        FailureFlow::Stopped => return WorkerOutcome::Stopped,
                        FailureFlow::Fatal(err) => return WorkerOutcome::Fatal(err),
                    }
                }
            }
        }

        // ---- 3) Fill the claim: carry leftovers first, then network --------
        let mut got = 0usize;
        let mut read_err: Option<VeloxError> = None;
        let mut eof = false;
        while got < want as usize {
            if ctx.token.is_cancelled() {
                // Bytes already read into `buf` are valid — flush them to disk
                // so the ledger and disk stay consistent across pause/cancel.
                if got > 0 {
                    let _ = ctx.file.write_at(offset, &buf[..got]);
                    record_disk(&ctx, offset + got as u64);
                } else {
                    rollback_claim_to(&ctx, offset);
                }
                return WorkerOutcome::Stopped;
            }
            // 3a) consume carry (bytes already pulled off the wire)
            if !carry.is_empty() {
                let n = (want as usize - got).min(carry.len());
                buf[got..got + n].copy_from_slice(&carry[..n]);
                carry.drain(..n);
                got += n;
                continue;
            }
            // 3b) pull from the network
            let resp = stream.as_mut().unwrap();
            let next = tokio::time::timeout(ctx.io_timeout, resp.chunk()).await;
            match next {
                Err(_elapsed) => {
                    read_err = Some(VeloxError::Network(format!(
                        "read timeout after {}ms (segment {} @ {})",
                        ctx.io_timeout.as_millis(),
                        ctx.seg_index,
                        offset + got as u64
                    )));
                    break;
                }
                Ok(Ok(Some(chunk))) => {
                    // Throttle on the FULL wire size (we consume the whole chunk)
                    ctx.shared.limiter.acquire(chunk.len() as u32).await;
                    ctx.global_limiter.acquire(chunk.len() as u32).await;
                    let take = (want as usize - got).min(chunk.len());
                    // CRITICAL: advance the buffer cursor — each chunk lands at
                    // buf[got..got+take]; overshoot goes to the carry buffer so
                    // the SAME connection serves the next claim seamlessly.
                    buf[got..got + take].copy_from_slice(&chunk[..take]);
                    got += take;
                    if take < chunk.len() {
                        carry.extend_from_slice(&chunk[take..]);
                    }
                    // whole chunk consumed off the wire
                    if let Some(pos) = *stream_pos {
                        *stream_pos = Some(pos + chunk.len() as u64);
                    }
                }
                Ok(Ok(None)) => {
                    eof = true;
                    *stream_pos = None;
                    break;
                }
                Ok(Err(e)) => {
                    read_err = Some(VeloxError::Network(crate::error::redact(&e.to_string())));
                    break;
                }
            }
        }

        // Maintain the invariant: next_byte = sock_pos - carry.len()
        if got == want as usize {
            if let Some(pos) = *stream_pos {
                *stream_pos = Some(offset + want + carry.len() as u64);
                let _ = pos;
            }
        }

        // ---- 4) Persist what we actually have ------------------------------
        if got > 0 {
            match ctx.file.write_at(offset, &buf[..got]) {
                Ok(n) if n == got => {
                    record_disk(&ctx, offset + got as u64);
                    ctx.shared.speed.add(got as u64);
                    ctx.shared.global_speed.add(got as u64);
                    bump_mirror_stats(&ctx, offset, got as u64, false);
                    // live disk-true accounting for UI (cheap; segments <= 32)
                    let done = ctx.shared.disk_true_done();
                    ctx.shared.state.write().bytes_done = done;
                }
                Ok(n) => {
                    // short write (should not happen with write_all_at, but be safe)
                    record_disk(&ctx, offset + n as u64);
                    rollback_claim_to(&ctx, offset + n as u64);
                    carry.clear();
                    *stream_pos = None;
                    let e = VeloxError::Io(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        format!("short write {n}/{}", got),
                    ));
                    match handle_failure(&ctx, retries, e).await {
                        FailureFlow::Retry => continue,
                        FailureFlow::Stopped => return WorkerOutcome::Stopped,
                        FailureFlow::Fatal(err) => return WorkerOutcome::Fatal(err),
                    }
                }
                Err(e) => {
                    rollback_claim_to(&ctx, offset);
                    carry.clear();
                    *stream_pos = None;
                    match handle_failure(&ctx, retries, e).await {
                        FailureFlow::Retry => continue,
                        FailureFlow::Stopped => return WorkerOutcome::Stopped,
                        FailureFlow::Fatal(err) => return WorkerOutcome::Fatal(err),
                    }
                }
            }
        }

        // ---- 5) Classify the read ------------------------------------------
        if read_err.is_some() || got < want as usize {
            // stream is dead/ended — never reuse it
            *stream_pos = None;
            carry.clear();
            let eof_clean = eof && read_err.is_none();
            let current = offset + got as u64;
            if unknown_total && eof_clean {
                // Unknown-length body: EOF marks the true end of file.
                // `current` (absolute bytes consumed from 0) is the file size.
                let mut st = ctx.shared.state.write();
                let seg = &mut st.segments[ctx.seg_index];
                seg.end = current.saturating_sub(1);
                seg.written = current - seg.start;
                st.total_size = Some(current);
                drop(st);
                record_disk(&ctx, current);
                return WorkerOutcome::DoneEof;
            }
            if got > 0 {
                rollback_claim_to(&ctx, current);
            } else {
                rollback_claim_to(&ctx, offset);
            }
            let e = read_err.unwrap_or_else(|| {
                VeloxError::Network("connection closed early (segment incomplete)".into())
            });
            match handle_failure(&ctx, retries, e).await {
                FailureFlow::Retry => continue,
                FailureFlow::Stopped => return WorkerOutcome::Stopped,
                FailureFlow::Fatal(err) => return WorkerOutcome::Fatal(err),
            }
        }
        // full chunk written; loop continues (claims next chunk or finishes)
    }
}

enum FailureFlow {
    Retry,
    Stopped,
    Fatal(VeloxError),
}

async fn handle_failure(ctx: &WorkerCtx, retries: &mut u32, err: VeloxError) -> FailureFlow {
    // A server that ignores Range requests cannot be fixed by retrying.
    if matches!(err, VeloxError::NoRangeSupport) {
        return FailureFlow::Fatal(err);
    }
    // Permanent client errors: fail fast. 429 (rate limit) is retried.
    if let VeloxError::HttpStatus { status, .. } = &err {
        if (400..=499).contains(status) && *status != 429 {
            return FailureFlow::Fatal(err);
        }
    }
    // rotate mirror
    {
        let mut st = ctx.shared.state.write();
        st.retries += 1;
        let urls = (st.mirrors.len() + 1) as usize;
        if let Some(seg) = st.segments.get_mut(ctx.seg_index) {
            seg.mirror = (seg.mirror + 1) % urls.max(1);
        }
        bump_mirror_stats_in(&mut st, ctx.seg_index, err.to_string());
    }
    *retries += 1;
    if *retries > ctx.max_retries {
        return FailureFlow::Fatal(err);
    }
    let backoff = ctx
        .backoff_base
        .saturating_mul(2u32.saturating_pow(*retries - 1))
        .min(ctx.backoff_max);
    tracing::debug!(
        "segment {} retry {}/{} in {:?}: {}",
        ctx.seg_index, retries, ctx.max_retries, backoff, err
    );
    tokio::select! {
        _ = tokio::time::sleep(backoff) => FailureFlow::Retry,
        _ = ctx.token.cancelled() => FailureFlow::Stopped,
    }
}

fn rollback_claim_to(ctx: &WorkerCtx, disk_pos: u64) {
    let mut st = ctx.shared.state.write();
    if let Some(seg) = st.segments.get_mut(ctx.seg_index) {
        seg.written = disk_pos.saturating_sub(seg.start);
    }
    record_disk(ctx, disk_pos);
}

fn rollback_claim(ctx: &WorkerCtx) {
    // on cancel: clamp ledger to disk-true position
    let d = ctx.shared.disk_upto.lock();
    if let Some(pos) = d.get(ctx.seg_index) {
        let mut st = ctx.shared.state.write();
        if let Some(seg) = st.segments.get_mut(ctx.seg_index) {
            let seg_written = seg.start + seg.written;
            if seg_written > *pos {
                seg.written = pos.saturating_sub(seg.start);
            }
        }
    }
}

fn record_disk(ctx: &WorkerCtx, pos: u64) {
    let mut d = ctx.shared.disk_upto.lock();
    if let Some(slot) = d.get_mut(ctx.seg_index) {
        *slot = (*slot).max(pos);
    }
}

fn bump_mirror_stats(ctx: &WorkerCtx, _offset: u64, bytes: u64, _err: bool) {
    let mut st = ctx.shared.state.write();
    let mirror_idx = st.segments.get(ctx.seg_index).map(|s| s.mirror).unwrap_or(0);
    if let Some(ms) = st.mirror_stats_mut().get_mut(mirror_idx) {
        ms.bytes_served += bytes;
    }
}

fn bump_mirror_stats_in(st: &mut crate::types::DownloadState, seg_index: usize, err: String) {
    let mirror_idx = st.segments.get(seg_index).map(|s| s.mirror).unwrap_or(0);
    if let Some(ms) = st.mirror_stats_mut().get_mut(mirror_idx) {
        ms.errors += 1;
        let _ = err;
    }
}

/// Open a stream at `offset` with hardening:
/// - open-ended Range so stale segment ends never truncate data;
/// - 200 at offset>0 is refused (server ignored our Range);
/// - 206 Content-Range start must match `offset`.
async fn open_stream(ctx: &WorkerCtx, offset: u64) -> Result<reqwest::Response> {
    let (url, auth, headers, if_range) = {
        let st = ctx.shared.state.read();
        let mirror = st.segments.get(ctx.seg_index).map(|s| s.mirror).unwrap_or(0);
        let url_str = if mirror == 0 {
            st.url.clone()
        } else {
            st.mirrors
                .get(mirror - 1)
                .cloned()
                .unwrap_or_else(|| st.url.clone())
        };
        let url = url::Url::parse(&url_str)
            .map_err(|e| VeloxError::InvalidUrl(format!("mirror {mirror}: {e}")))?;
        (
            url,
            (
                st.options.username.clone(),
                st.options.password.clone(),
            ),
            st.options.headers.clone(),
            st.etag.clone(),
        )
    };

    let supports_ranges = {
        let st = ctx.shared.state.read();
        st.supports_ranges
    };
    let mut req = ctx.client.get(url.clone());
    if offset > 0 && supports_ranges {
        req = req.header("Range", format!("bytes={offset}-"));
        // If-Range: only useful when we know an etag; a changed file returns 200
        // which we then refuse at offset>0 → supervisor restarts cleanly.
        if let Some(etag) = &if_range {
            req = req.header("If-Range", etag);
        }
    }
    for (k, v) in &headers {
        req = req.header(k.as_str(), v.as_str());
    }
    if let (Some(u), Some(p)) = (&auth.0, &auth.1) {
        req = req.basic_auth(u.clone(), Some(p.clone()));
    }

    let resp = req.send().await?;
    let status = resp.status();
    if offset > 0 && status == reqwest::StatusCode::OK {
        // Body starts at byte 0 but we wanted `offset` — continuing would
        // corrupt the file. Recoverable only by a full single-stream restart.
        return Err(VeloxError::NoRangeSupport);
    }
    if status == reqwest::StatusCode::PARTIAL_CONTENT {
        if let Some(cr) = resp
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
        {
            // format: bytes <start>-<end>/<total>
            if let Some(start) = cr
                .split(&[' ', '-'][..])
                .find_map(|p| p.parse::<u64>().ok())
            {
                if start != offset {
                    return Err(VeloxError::Network(format!(
                        "Content-Range mismatch: asked {offset}, server sent {start}"
                    )));
                }
            }
        }
    } else if !status.is_success() {
        return Err(VeloxError::HttpStatus {
            status: status.as_u16(),
            url: crate::urlsafe::sanitize_for_display(&url),
        });
    }
    Ok(resp)
}
