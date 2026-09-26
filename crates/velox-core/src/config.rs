use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Global engine configuration. Persisted as config.toml in the state dir.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EngineConfig {
    /// Directory where downloads are stored by default.
    pub download_dir: PathBuf,
    /// Directory holding sidecar meta files (defaults to download_dir/.velox-meta).
    pub state_dir: Option<PathBuf>,
    /// Max concurrently running downloads.
    pub max_concurrent_downloads: u32,
    /// Default connections per download.
    pub connections_per_download: u32,
    /// Hard cap of connections per download.
    pub max_connections_per_download: u32,
    /// Smallest segment we are willing to create via split (bytes).
    pub min_segment_size: u64,
    /// Global speed limit (bytes/sec). None = unlimited.
    pub global_speed_limit: Option<u64>,
    /// Default per-download speed limit (bytes/sec). None = unlimited.
    pub default_download_limit: Option<u64>,
    /// Percentage of *available* RAM the adaptive buffer pool may target (1..=80).
    pub ram_cache_percent: u32,
    /// Absolute cap for the buffer pool budget in bytes.
    pub ram_cache_cap: u64,
    /// Absolute floor for the buffer pool budget in bytes.
    pub ram_cache_floor: u64,
    /// Max retries per segment before the download fails.
    pub max_retries: u32,
    /// Base backoff for retries (ms).
    pub retry_backoff_base_ms: u64,
    /// Max backoff (ms).
    pub retry_backoff_max_ms: u64,
    /// Connect timeout (ms).
    pub connect_timeout_ms: u64,
    /// Idle read timeout per chunk (ms).
    pub io_timeout_ms: u64,
    /// Max redirects followed.
    pub max_redirects: usize,
    /// Proxy URL (http:// or socks5://). None = system default (direct).
    pub proxy: Option<String>,
    /// Bind IPv4 only. Works around broken IPv6 routes (very common on home
    /// networks and containers); disable for IPv6-only destinations.
    pub prefer_ipv4: bool,
    /// User agent.
    pub user_agent: String,
    /// Verify completed downloads by streaming hash when size is known and no expected hash given?
    pub compute_sha256_on_complete: bool,
    /// Automatically resume interrupted downloads on engine start.
    pub auto_resume: bool,
    /// fsync the data file when a download finalises (durability over speed).
    pub fsync_on_complete: bool,
    /// Rate at which sidecar meta is persisted during download (ms).
    pub persist_interval_ms: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            download_dir: default_download_dir(),
            state_dir: None,
            max_concurrent_downloads: 3,
            connections_per_download: 8,
            max_connections_per_download: 32,
            min_segment_size: 1 * 1024 * 1024,
            global_speed_limit: None,
            default_download_limit: None,
            ram_cache_percent: 20,
            ram_cache_cap: 512 * 1024 * 1024,
            ram_cache_floor: 32 * 1024 * 1024,
            max_retries: 10,
            retry_backoff_base_ms: 500,
            retry_backoff_max_ms: 30_000,
            connect_timeout_ms: 15_000,
            io_timeout_ms: 45_000,
            max_redirects: 8,
            proxy: None,
            prefer_ipv4: true,
            user_agent: format!("Velox/{}", env!("CARGO_PKG_VERSION")),
            compute_sha256_on_complete: true,
            auto_resume: true,
            fsync_on_complete: true,
            persist_interval_ms: 2000,
        }
    }
}

impl EngineConfig {
    pub fn effective_connections(&self, requested: Option<u32>, single: bool) -> u32 {
        if single {
            return 1;
        }
        let base = requested.unwrap_or(self.connections_per_download);
        base.clamp(1, self.max_connections_per_download)
    }

    /// RAM budget for in-flight buffers, derived from current available memory.
    pub fn ram_budget(&self, available_ram: u64) -> u64 {
        let pct = self.ram_cache_percent.clamp(1, 80) as f64 / 100.0;
        ((available_ram as f64 * pct) as u64)
            .clamp(self.ram_cache_floor, self.ram_cache_cap)
    }
}

pub fn default_download_dir() -> PathBuf {
    if let Some(dirs) = directories::UserDirs::new() {
        if let Some(dl) = dirs.download_dir() {
            return dl.to_path_buf();
        }
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join("Downloads")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ram_budget_respects_bounds() {
        let mut cfg = EngineConfig::default();
        cfg.ram_cache_percent = 20;
        // Tiny machine: floor applies
        assert_eq!(cfg.ram_budget(64 * 1024 * 1024), cfg.ram_cache_floor);
        // Huge machine: cap applies
        assert_eq!(cfg.ram_budget(64 * 1024 * 1024 * 1024), cfg.ram_cache_cap);
        // Middle: percentage applies (4GiB avail -> ~800MiB -> capped 512MiB)
        assert_eq!(cfg.ram_budget(4 * 1024 * 1024 * 1024), cfg.ram_cache_cap);
        cfg.ram_cache_percent = 5;
        // 4GiB * 5% = ~204MiB, between floor and cap
        let b = cfg.ram_budget(4 * 1024 * 1024 * 1024);
        assert!(b > cfg.ram_cache_floor && b < cfg.ram_cache_cap, "b={b}");
    }

    #[test]
    fn connections_clamped() {
        let cfg = EngineConfig::default();
        assert_eq!(cfg.effective_connections(Some(999), false), 32);
        assert_eq!(cfg.effective_connections(Some(0), false), 1);
        assert_eq!(cfg.effective_connections(None, true), 1);
    }
}
