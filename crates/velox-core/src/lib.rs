//! Velox core engine.

pub mod config;
pub mod download;
pub mod engine;
pub mod error;
pub mod events;
pub mod filename;
pub mod http;
pub mod limiter;
pub mod memory;
pub mod bench;
pub mod segment;
pub mod speed;
pub mod storage;
pub mod types;
pub mod urlsafe;

#[cfg(feature = "torrent")]
pub mod torrent;

pub use config::EngineConfig;
pub use engine::Engine;
pub use error::{Result, VeloxError};
pub use events::EngineEvent;
pub use types::{DownloadId, DownloadOptions, DownloadSnapshot, DownloadStatus, SegmentState};
