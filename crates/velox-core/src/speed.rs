//! Speed measurement: per-download + global trackers with history for graphs.

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const WINDOW: Duration = Duration::from_secs(3);
const HISTORY_CAP: usize = 180; // seconds of 1Hz samples

struct Inner {
    /// cumulative byte counter
    counter: u64,
    /// (instant, cumulative) pairs inside window
    window: VecDeque<(Instant, u64)>,
    /// 1Hz rate samples for graphs
    history: VecDeque<f64>,
    last_tick: Instant,
    last_counter: u64,
    ewma: f64,
}

/// Thread-safe speed tracker. `add()` is called on every chunk; `rate()` reads.
pub struct SpeedTracker {
    inner: Mutex<Inner>,
}

impl SpeedTracker {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                counter: 0,
                window: VecDeque::new(),
                history: VecDeque::new(),
                last_tick: Instant::now(),
                last_counter: 0,
                ewma: 0.0,
            }),
        }
    }

    pub fn add(&self, n: u64) {
        let mut g = self.inner.lock();
        g.counter += n;
        let now = Instant::now();
        let counter = g.counter;
        g.window.push_back((now, counter));
        // drop old samples
        while let Some(front) = g.window.front() {
            if now.duration_since(front.0) > WINDOW {
                g.window.pop_front();
            } else {
                break;
            }
        }
    }

    /// Bytes/sec over the sliding window (>= 0).
    pub fn rate(&self) -> f64 {
        let mut g = self.inner.lock();
        let now = Instant::now();
        while let Some(front) = g.window.front() {
            if now.duration_since(front.0) > WINDOW {
                g.window.pop_front();
            } else {
                break;
            }
        }
        match g.window.front() {
            Some((start, start_count)) => {
                let dt = now.duration_since(*start).as_secs_f64();
                if dt < 0.05 {
                    g.ewma
                } else {
                    let rate = (g.counter - start_count) as f64 / dt;
                    g.ewma = if g.ewma == 0.0 {
                        rate
                    } else {
                        g.ewma * 0.7 + rate * 0.3
                    };
                    g.ewma
                }
            }
            None => {
                // idle: decay ewma to 0
                g.ewma *= 0.5;
                if g.ewma < 1.0 {
                    g.ewma = 0.0;
                }
                g.ewma
            }
        }
    }

    /// Called ~1Hz by the engine to build graph history.
    pub fn tick_history(&self) -> f64 {
        let mut g = self.inner.lock();
        let rate = {
            let now = Instant::now();
            let dt = now.duration_since(g.last_tick).as_secs_f64();
            let r = if dt > 0.01 {
                (g.counter - g.last_counter) as f64 / dt
            } else {
                0.0
            };
            g.last_tick = now;
            g.last_counter = g.counter;
            r
        };
        g.history.push_back(rate);
        while g.history.len() > HISTORY_CAP {
            g.history.pop_front();
        }
        rate
    }

    pub fn history(&self) -> Vec<f64> {
        self.inner.lock().history.iter().copied().collect()
    }
}

impl Default for SpeedTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tracks_rates() {
        let st = SpeedTracker::new();
        for _ in 0..10 {
            st.add(100_000);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let r = st.rate();
        // ~100KB every 20ms ≈ 5 MB/s
        assert!(r > 1_000_000.0, "rate={r}");
        st.tick_history();
        assert!(!st.history().is_empty());
    }

    #[tokio::test]
    async fn decays_to_zero_when_idle() {
        let st = SpeedTracker::new();
        st.add(1000);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = st.rate();
        tokio::time::sleep(Duration::from_millis(120)).await;
        let r = st.rate();
        assert!(r < 50_000.0, "should decay, got {r}");
    }
}
