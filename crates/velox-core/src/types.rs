use chrono::{DateTime, Utc};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::limiter::RateLimiter;
use crate::memory::BufferBudget;
use crate::speed::SpeedTracker;

pub type DownloadId = Uuid;

/// Shared per-download runtime state (behind Arc in the engine).
pub struct DownloadShared {
    pub id: DownloadId,
    pub state: RwLock<DownloadState>,
    /// Bytes/sec of this download.
    pub speed: SpeedTracker,
    /// Engine-wide speed tracker.
    pub global_speed: Arc<SpeedTracker>,
    /// Live segment workers attached to this download.
    pub active: AtomicUsize,
    /// RAM-adaptive in-flight buffer budget (engine-wide).
    pub buffer_budget: Arc<BufferBudget>,
    /// Per-download rate limiter.
    pub limiter: Arc<RateLimiter>,
    /// Current run's cancellation token (regenerated on every resume).
    pub token: Mutex<CancellationToken>,
    /// DISK-TRUE absolute write position per segment (workers update after
    /// each successful write; the claim ledger in `state.segments` may be
    /// ahead of it). Persisted sidecars and UI progress use this.
    pub disk_upto: Mutex<Vec<u64>>,
}

impl DownloadShared {
    pub fn active_connections(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }
    pub fn current_token(&self) -> CancellationToken {
        self.token.lock().clone()
    }
    /// Disk-true completed bytes (authoritative progress).
    pub fn disk_true_done(&self) -> u64 {
        let st = self.state.read();
        let d = self.disk_upto.lock();
        st.segments
            .iter()
            .enumerate()
            .map(|(i, seg)| {
                let disk_abs = d.get(i).copied().unwrap_or(seg.start);
                disk_abs.saturating_sub(seg.start).min(seg.total())
            })
            .sum()
    }
    /// Reconcile claim ledger down to disk truth (only safe when no workers run).
    pub fn clamp_claims_to_disk(&self) {
        let d = self.disk_upto.lock().clone();
        let mut st = self.state.write();
        for (i, seg) in st.segments.iter_mut().enumerate() {
            if let Some(pos) = d.get(i) {
                seg.written = pos.saturating_sub(seg.start).min(seg.total());
            }
        }
        st.bytes_done = st.segments.iter().map(|s| s.written).sum();
    }
    pub fn snapshot(&self) -> DownloadSnapshot {
        let mut st = self.state.read().clone();
        st.bytes_done = self.disk_true_done();
        let speed = self.speed.rate();
        DownloadSnapshot::from_state(&st, speed, self.active_connections())
    }
    /// Replace the cancellation token; returns the new one.
    pub fn renew_token(&self) -> CancellationToken {
        let t = CancellationToken::new();
        *self.token.lock() = t.clone();
        t
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownloadStatus {
    Queued,
    Connecting,
    Downloading,
    Paused,
    Verifying,
    Completed,
    Failed,
    Cancelled,
}

impl DownloadStatus {
    pub fn is_active(&self) -> bool {
        matches!(self, DownloadStatus::Connecting | DownloadStatus::Downloading | DownloadStatus::Verifying)
    }
    pub fn is_terminal(&self) -> bool {
        matches!(self, DownloadStatus::Completed | DownloadStatus::Cancelled)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentState {
    /// Absolute start offset in the output file.
    pub start: u64,
    /// Absolute end offset INCLUSIVE in the output file.
    pub end: u64,
    /// Bytes written so far for this segment (absolute offset = start + written).
    pub written: u64,
    /// Mirror index used currently (0 = primary URL).
    pub mirror: usize,
}

impl SegmentState {
    pub fn total(&self) -> u64 {
        self.end - self.start + 1
    }
    pub fn remaining(&self) -> u64 {
        self.total().saturating_sub(self.written)
    }
    #[inline]
    pub fn next_offset(&self) -> u64 {
        self.start + self.written
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MirrorStats {
    pub url: String,
    pub ttfb_ms: Option<u64>,
    pub bytes_served: u64,
    pub errors: u64,
}

/// Per-download options supplied by the user.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DownloadOptions {
    /// Destination directory. Defaults to engine config download_dir.
    pub dest_dir: Option<std::path::PathBuf>,
    /// Preferred file name (without directory).
    pub filename: Option<String>,
    /// Extra mirrors treated as the same file.
    pub mirrors: Vec<String>,
    /// Override default number of connections.
    pub connections: Option<u32>,
    /// Per-download speed limit bytes/sec.
    pub speed_limit: Option<u64>,
    /// Expected sha256 (lowercase hex). Verified after completion.
    pub verify_sha256: Option<String>,
    /// Extra request headers.
    pub headers: Vec<(String, String)>,
    /// Basic auth username.
    pub username: Option<String>,
    /// Basic auth password (never persisted in meta).
    pub password: Option<String>,
    /// Priority: higher starts first.
    pub priority: i32,
    /// Do not start before this time.
    pub not_before: Option<DateTime<Utc>>,
    /// Always single connection (streams unknown-length bodies too).
    pub force_single_connection: bool,
    /// With `torrent` feature: also start a torrent source and race it (hybrid mode).
    pub hybrid_torrent: bool,
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            dest_dir: None,
            filename: None,
            mirrors: Vec::new(),
            connections: None,
            speed_limit: None,
            verify_sha256: None,
            headers: Vec::new(),
            username: None,
            password: None,
            priority: 0,
            not_before: None,
            force_single_connection: false,
            hybrid_torrent: false,
        }
    }
}

/// Serialisable full state of a download (this is what the sidecar file stores).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DownloadState {
    pub id: DownloadId,
    pub url: String,
    pub mirrors: Vec<String>,
    pub filename: String,
    pub dest_dir: std::path::PathBuf,
    pub total_size: Option<u64>,
    pub supports_ranges: bool,
    pub status: DownloadStatus,
    pub segments: Vec<SegmentState>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub sha256: Option<String>,
    pub expected_sha256: Option<String>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub retries: u64,
    pub bytes_done: u64,
    pub content_type: Option<String>,
    pub alt_svc_h3: bool,
    pub protocol_used: Option<String>,
    pub mirror_stats: Vec<MirrorStats>,
    pub options: DownloadOptions,
}

impl Default for DownloadState {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            url: String::new(),
            mirrors: Vec::new(),
            filename: "download".into(),
            dest_dir: std::env::temp_dir(),
            total_size: None,
            supports_ranges: false,
            status: DownloadStatus::Queued,
            segments: Vec::new(),
            etag: None,
            last_modified: None,
            sha256: None,
            expected_sha256: None,
            error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            completed_at: None,
            retries: 0,
            bytes_done: 0,
            content_type: None,
            alt_svc_h3: false,
            protocol_used: None,
            mirror_stats: Vec::new(),
            options: DownloadOptions::default(),
        }
    }
}

impl DownloadState {
    pub fn mirror_stats(&self) -> &[MirrorStats] {
        &self.mirror_stats
    }

    pub fn mirror_stats_mut(&mut self) -> &mut Vec<MirrorStats> {
        &mut self.mirror_stats
    }

    /// Initialize mirror stats to (mirrors+1) entries.
    pub fn init_mirror_stats(&mut self) {
        let n = self.mirrors.len() + 1;
        self.mirror_stats = (0..n)
            .map(|i| {
                if i == 0 {
                    MirrorStats { url: self.url.clone(), ttfb_ms: None, bytes_served: 0, errors: 0 }
                } else {
                    MirrorStats { url: self.mirrors[i - 1].clone(), ttfb_ms: None, bytes_served: 0, errors: 0 }
                }
            })
            .collect();
    }

    pub fn total_bytes(&self) -> u64 {
        if let Some(t) = self.total_size {
            t
        } else {
            self.bytes_done
        }
    }
    pub fn recompute_bytes_done(&mut self) {
        self.bytes_done = self.segments.iter().map(|s| s.written).sum();
    }
}

/// Read-only snapshot for UIs.
#[derive(Debug, Clone, Serialize)]
pub struct DownloadSnapshot {
    pub id: DownloadId,
    pub url: String,
    pub filename: String,
    pub dest_dir: String,
    pub full_path: String,
    pub total_size: Option<u64>,
    pub bytes_done: u64,
    pub speed_bps: f64,
    pub eta_secs: Option<u64>,
    pub status: DownloadStatus,
    pub error: Option<String>,
    pub segments: Vec<SegmentState>,
    pub active_connections: usize,
    pub retries: u64,
    pub protocol_used: Option<String>,
    pub alt_svc_h3: bool,
    pub mirrors: usize,
    pub sha256: Option<String>,
    pub expected_sha256: Option<String>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub priority: i32,
    pub progress_pct: f64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct EngineStats {
    pub global_speed_bps: f64,
    pub active_downloads: usize,
    pub active_connections: usize,
    pub queued: usize,
    pub buffer_budget_used: u64,
    pub buffer_budget_total: u64,
    pub available_ram: u64,
    pub total_ram: u64,
    pub speed_history: Vec<f64>,
}

impl DownloadSnapshot {
    pub fn from_state(state: &DownloadState, speed: f64, connections: usize) -> Self {
        let total = state.total_bytes();
        let pct = if total > 0 {
            (state.bytes_done as f64 / total as f64 * 100.0).min(100.0)
        } else if state.status == DownloadStatus::Completed {
            100.0
        } else {
            0.0
        };
        let eta = if speed > 1.0 && total > state.bytes_done && state.status.is_active() {
            Some(((total - state.bytes_done) as f64 / speed) as u64)
        } else {
            None
        };
        Self {
            id: state.id,
            url: redact_display(&state.url),
            filename: state.filename.clone(),
            dest_dir: state.dest_dir.display().to_string(),
            full_path: state.dest_dir.join(&state.filename).display().to_string(),
            total_size: state.total_size,
            bytes_done: state.bytes_done,
            speed_bps: speed,
            eta_secs: eta,
            status: state.status,
            error: state.error.clone(),
            segments: state.segments.clone(),
            active_connections: connections,
            retries: state.retries,
            protocol_used: state.protocol_used.clone(),
            alt_svc_h3: state.alt_svc_h3,
            mirrors: state.mirrors.len() + 1,
            sha256: state.sha256.clone(),
            expected_sha256: state.expected_sha256.clone(),
            created_at: state.created_at,
            completed_at: state.completed_at,
            priority: state.options.priority,
            progress_pct: pct,
        }
    }
}

fn redact_display(url: &str) -> String {
    crate::error::redact(url)
}
