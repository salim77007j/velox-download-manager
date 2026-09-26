//! Engine events broadcast to UIs (GUI/CLI/SSE).

use crate::types::{DownloadId, DownloadStatus, EngineStats};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EngineEvent {
    DownloadAdded { id: DownloadId },
    DownloadRemoved { id: DownloadId },
    DownloadStateChanged {
        id: DownloadId,
        from: DownloadStatus,
        to: DownloadStatus,
        error: Option<String>,
    },
    DownloadCompleted {
        id: DownloadId,
        sha256: Option<String>,
        bytes: u64,
        duration_secs: f64,
        avg_speed_bps: f64,
    },
    /// 1Hz global stats tick (drives live graphs).
    ProgressTick { stats: EngineStats },
    Notice { level: String, message: String },
}
