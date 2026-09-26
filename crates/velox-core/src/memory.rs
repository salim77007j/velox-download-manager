//! RAM-adaptive I/O budget.
//!
//! The engine never allocates unbounded buffers. `MemoryMonitor` samples the OS
//! every few seconds and `BufferProfile` derives:
//!   * per-worker chunk buffer size
//!   * a global in-flight byte budget shared across all downloads
//! so Velox speeds up downloads on beefy machines without hurting system responsiveness.

use parking_lot::Mutex;
use std::time::{Duration, Instant};
use sysinfo::System;

pub struct MemoryMonitor {
    sys: Mutex<System>,
    last_sample: Mutex<Option<(Instant, u64, u64)>>, // (at, available, total)
}

impl MemoryMonitor {
    pub fn new() -> Self {
        let mut sys = System::new();
        sys.refresh_memory();
        let avail = sys.available_memory();
        let total = sys.total_memory();
        Self {
            sys: Mutex::new(sys),
            last_sample: Mutex::new(Some((Instant::now(), avail, total))),
        }
    }

    /// Returns (available, total) bytes, cached for 2s.
    pub fn sample(&self) -> (u64, u64) {
        {
            let cached = self.last_sample.lock();
            if let Some((at, avail, total)) = *cached {
                if at.elapsed() < Duration::from_secs(2) {
                    return (avail, total);
                }
            }
        }
        let mut sys = self.sys.lock();
        sys.refresh_memory();
        let avail = sys.available_memory();
        let total = sys.total_memory();
        *self.last_sample.lock() = Some((Instant::now(), avail, total));
        (avail, total)
    }
}

impl Default for MemoryMonitor {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferProfile {
    /// Per-worker read buffer size.
    pub chunk_size: u32,
    /// Total in-flight budget across the engine.
    pub budget: u64,
}

/// Derive the buffer profile from available RAM and the configured budget.
pub fn buffer_profile(available_ram: u64, budget: u64) -> BufferProfile {
    let gib = available_ram as f64 / (1024.0 * 1024.0 * 1024.0);
    let chunk: u32 = if gib >= 8.0 {
        4 * 1024 * 1024
    } else if gib >= 4.0 {
        2 * 1024 * 1024
    } else if gib >= 1.0 {
        1024 * 1024
    } else {
        256 * 1024
    };
    // Budget must never be smaller than a handful of chunks.
    let min_budget = (chunk as u64) * 8;
    BufferProfile {
        chunk_size: chunk,
        budget: budget.max(min_budget),
    }
}

/// Tracks in-flight buffer usage against the budget; workers register/unregister.
#[derive(Debug)]
pub struct BufferBudget {
    used: std::sync::atomic::AtomicU64,
    current: std::sync::atomic::AtomicU64,
}

impl BufferBudget {
    pub fn new(initial: u64) -> Self {
        Self {
            used: std::sync::atomic::AtomicU64::new(0),
            current: std::sync::atomic::AtomicU64::new(initial),
        }
    }

    pub fn set_budget(&self, budget: u64) {
        self.current.store(budget, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn budget(&self) -> u64 {
        self.current.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn used(&self) -> u64 {
        self.used.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// True if a worker may allocate `size` more bytes right now.
    pub fn may_allocate(&self, size: u64) -> bool {
        self.used.load(std::sync::atomic::Ordering::Relaxed) + size
            <= self.budget().saturating_mul(2) // allow modest oversubscription; budget is a target
    }

    pub fn register(&self, size: u64) {
        self.used.fetch_add(size, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn release(&self, size: u64) {
        self.used.fetch_sub(size.min(self.used()), std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_scales_with_ram() {
        let small = buffer_profile(512 * 1024 * 1024, 32 * 1024 * 1024);
        let big = buffer_profile(32 * 1024 * 1024 * 1024, 512 * 1024 * 1024);
        assert!(small.chunk_size < big.chunk_size);
        assert_eq!(small.chunk_size, 256 * 1024);
        assert_eq!(big.chunk_size, 4 * 1024 * 1024);
    }

    #[test]
    fn budget_guard() {
        let b = BufferBudget::new(1024);
        b.register(512);
        assert!(b.may_allocate(512));
        b.register(512);
        // 2x oversubscription tolerance over the target
        assert!(b.may_allocate(1024));
        assert!(!b.may_allocate(4096));
        b.release(256);
        assert_eq!(b.used(), 768);
    }

    #[test]
    fn monitor_samples() {
        let m = MemoryMonitor::new();
        let (a1, t1) = m.sample();
        let (a2, t2) = m.sample();
        assert_eq!(a1, a2); // cached
        assert_eq!(t1, t2);
        assert!(t1 > 0);
    }
}
