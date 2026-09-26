//! Token-bucket rate limiters (async-friendly, no busy waits).

use parking_lot::Mutex;
use std::time::{Duration, Instant};

struct BucketState {
    tokens: f64,
    last_refill: Instant,
}

/// A token bucket where 1 token = 1 byte. `None` rate = unlimited (no waiting).
pub struct RateLimiter {
    rate: std::sync::atomic::AtomicU64,
    burst: u64,
    state: Mutex<Option<BucketState>>, // None = unlimited mode
}

impl RateLimiter {
    pub fn new(rate_bps: Option<u64>, burst: u64) -> Self {
        let state = rate_bps.map(|_| BucketState {
            tokens: burst as f64,
            last_refill: Instant::now(),
        });
        Self {
            rate: std::sync::atomic::AtomicU64::new(rate_bps.unwrap_or(0)), // 0 = unlimited
            burst,
            state: Mutex::new(state),
        }
    }

    pub fn set_rate(&self, rate_bps: Option<u64>) {
        let mut st = self.state.lock();
        self.rate
            .store(rate_bps.unwrap_or(0), std::sync::atomic::Ordering::Relaxed);
        *st = rate_bps.map(|_| BucketState {
            tokens: self.burst as f64,
            last_refill: Instant::now(),
        });
    }

    pub fn rate(&self) -> Option<u64> {
        match self.rate.load(std::sync::atomic::Ordering::Relaxed) {
            0 => None,
            r => Some(r),
        }
    }

    /// Reserve `n` bytes. Returns after the tokens are available.
    ///
    /// Debt model: the bucket may go negative (up to -burst) so chunks larger
    /// than the burst always terminate; the next acquires pay the debt back.
    pub async fn acquire(&self, n: u32) {
        if self.rate().is_none() {
            return;
        }
        let n = n as f64;
        let wait = {
            let mut guard = self.state.lock();
            let st = match guard.as_mut() {
                Some(s) => s,
                None => return, // switched to unlimited meanwhile
            };
            let now = Instant::now();
            let elapsed = now.duration_since(st.last_refill).as_secs_f64();
            let rate = self.rate.load(std::sync::atomic::Ordering::Relaxed).max(1) as f64;
            // Debt floor is deep (32x burst): with N concurrent workers each
            // debiting a chunk, a shallow floor would FORGIVE debt and leak
            // throughput. 32x covers up to 32 in-flight chunk debits.
            let floor = -(self.burst as f64 * 32.0);
            st.tokens = ((st.tokens + elapsed * rate).min(self.burst as f64)).max(floor);
            st.last_refill = now;
            if st.tokens >= n {
                st.tokens -= n;
                Duration::ZERO
            } else {
                let wait_secs = ((n - st.tokens) / rate).min(2.0).max(0.001);
                st.tokens = (st.tokens - n).max(floor);
                Duration::from_secs_f64(wait_secs)
            }
        };
        if wait == Duration::ZERO {
            return;
        }
        tokio::time::sleep(wait).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unlimited_is_free() {
        let rl = RateLimiter::new(None, 1024);
        let t = Instant::now();
        rl.acquire(1024 * 1024).await;
        assert!(t.elapsed() < Duration::from_millis(20));
    }

    #[tokio::test]
    async fn limits_throughput() {
        // 1 MB/s, burst 256KB: transferring 1MB should take >= ~700ms
        let rl = RateLimiter::new(Some(1024 * 1024), 256 * 1024);
        let t = Instant::now();
        for _ in 0..16 {
            rl.acquire(64 * 1024).await;
        }
        let elapsed = t.elapsed();
        assert!(
            elapsed >= Duration::from_millis(600),
            "elapsed={elapsed:?}"
        );
    }

    #[tokio::test]
    async fn switch_to_unlimited_releases() {
        let rl = RateLimiter::new(Some(1024), 1024);
        rl.set_rate(None);
        let t = Instant::now();
        rl.acquire(1024 * 1024).await;
        assert!(t.elapsed() < Duration::from_millis(20));
    }
}
