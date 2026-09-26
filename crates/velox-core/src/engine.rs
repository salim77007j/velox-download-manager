//! The Velox engine: owns configuration, the HTTP client, the queue scheduler,
//! all downloads, the global rate limiter, the RAM monitor and the event bus.
//!
//! The public API is **synchronous** (safe to call from GUI threads); long work
//! happens on the internal tokio runtime the engine was created on.

use crate::config::EngineConfig;
use crate::download::{self, supervise, Ctrl, IndexEntry, SupervisorDeps};
use crate::error::{Result, VeloxError};
use crate::events::EngineEvent;
use crate::filename;
use crate::limiter::RateLimiter;
use crate::memory::{BufferBudget, MemoryMonitor};
use crate::speed::SpeedTracker;
use crate::storage::{EngineLock, Sidecar};
use crate::types::{
    DownloadId, DownloadOptions, DownloadShared, DownloadSnapshot, DownloadState, DownloadStatus,
    EngineStats,
};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

pub(crate) struct Entry {
    pub shared: Arc<DownloadShared>,
    pub ctrl: Mutex<Option<mpsc::UnboundedSender<Ctrl>>>,
}

pub(crate) struct EngineInner {
    pub cfg: Arc<RwLock<EngineConfig>>,
    pub client: reqwest::Client,
    pub entries: RwLock<HashMap<DownloadId, Arc<Entry>>>,
    pub events: broadcast::Sender<EngineEvent>,
    pub global_limiter: Arc<RateLimiter>,
    pub global_speed: Arc<SpeedTracker>,
    pub buffer_budget: Arc<BufferBudget>,
    pub memory: Arc<MemoryMonitor>,
    pub _lock: Mutex<EngineLock>,
    pub state_dir: PathBuf,
    pub queue_tx: mpsc::UnboundedSender<QueueCmd>,
}

pub(crate) enum QueueCmd {
    Consider(DownloadId),
}

pub struct Engine {
    inner: Arc<EngineInner>,
}

impl Engine {
    /// Create the engine on the current tokio runtime (multi-thread recommended).
    pub fn new(cfg: EngineConfig) -> Result<Arc<Engine>> {
        let state_dir: PathBuf = cfg
            .state_dir
            .clone()
            .unwrap_or_else(|| cfg.download_dir.join(".velox-meta"));
        std::fs::create_dir_all(&cfg.download_dir)?;
        std::fs::create_dir_all(&state_dir)?;

        let lock = EngineLock::acquire(&state_dir)?;

        // Load persisted configuration (hand-edited or written by a previous run).
        let cfg = {
            let path = state_dir.join("config.toml");
            match std::fs::read_to_string(&path) {
                Ok(txt) => match toml::from_str::<EngineConfig>(&txt) {
                    Ok(loaded) => {
                        // state_dir is authoritative (came from CLI/env)
                        EngineConfig {
                            state_dir: cfg.state_dir,
                            download_dir: if std::path::Path::new(&cfg.download_dir).exists() || cfg.download_dir != EngineConfig::default().download_dir {
                                cfg.download_dir.clone()
                            } else {
                                loaded.download_dir
                            },
                            ..loaded
                        }
                    }
                    Err(e) => {
                        tracing::warn!("config.toml unreadable ({e}); using defaults");
                        cfg
                    }
                },
                Err(_) => cfg,
            }
        };

        let client = crate::http::build_client(&cfg)?;
        let (events_tx, _) = broadcast::channel(4096);
        let global_limiter = Arc::new(RateLimiter::new(cfg.global_speed_limit, 256 * 1024));
        let global_speed = Arc::new(SpeedTracker::new());
        let memory = Arc::new(MemoryMonitor::new());
        let (avail, _) = memory.sample();
        let budget = Arc::new(BufferBudget::new(cfg.ram_budget(avail)));
        let (queue_tx, queue_rx) = mpsc::unbounded_channel::<QueueCmd>();

        let cfg_arc = Arc::new(RwLock::new(cfg));

        // persist initial config (so users can hand-edit between runs)
        {
            let dir = state_dir.clone();
            let snapshot = cfg_arc.read().clone();
            let _ = std::fs::create_dir_all(&dir);
            if !dir.join("config.toml").exists() {
                if let Ok(s) = toml::to_string_pretty(&snapshot) {
                    let _ = std::fs::write(dir.join("config.toml"), s);
                }
            }
        }

        let inner = Arc::new(EngineInner {
            cfg: cfg_arc,
            client,
            entries: RwLock::new(HashMap::new()),
            events: events_tx,
            global_limiter,
            global_speed,
            buffer_budget: budget,
            memory,
            _lock: Mutex::new(lock),
            state_dir,
            queue_tx,
        });

        // Queue scheduler
        {
            let inner2 = inner.clone();
            tokio::spawn(async move { scheduler_loop(inner2, queue_rx).await });
        }
        // 1Hz stats ticker (drives graphs + keeps sidecars fresh is done by supervisors)
        {
            let inner2 = inner.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(1000));
                loop {
                    tick.tick().await;
                    let _ = inner2.global_speed.tick_history();
                    let stats = compute_stats(&inner2);
                    let _ = inner2.events.send(EngineEvent::ProgressTick { stats });
                }
            });
        }

        let engine = Arc::new(Engine { inner });
        engine.restore();
        Ok(engine)
    }

    pub fn config(&self) -> EngineConfig {
        self.inner.cfg.read().clone()
    }

    /// Mutate live configuration; persists to config.toml and applies hot knobs.
    pub fn update_config(&self, f: impl FnOnce(&mut EngineConfig)) {
        let snapshot = {
            let mut cfg = self.inner.cfg.write();
            f(&mut cfg);
            cfg.clone()
        };
        self.inner.global_limiter.set_rate(snapshot.global_speed_limit);
        let (avail, _) = self.inner.memory.sample();
        self.inner.buffer_budget.set_budget(snapshot.ram_budget(avail));
        let dir = self.inner.state_dir.clone();
        if let Ok(s) = toml::to_string_pretty(&snapshot) {
            let tmp = dir.join("config.toml.tmp");
            if std::fs::write(&tmp, s).is_ok() {
                let _ = std::fs::rename(&tmp, dir.join("config.toml"));
            }
        }
    }

    pub fn events(&self) -> broadcast::Receiver<EngineEvent> {
        self.inner.events.subscribe()
    }

    pub fn state_dir(&self) -> &std::path::Path {
        &self.inner.state_dir
    }

    /// Add a download. Returns immediately; scheduling happens asynchronously.
    pub fn add(&self, raw_url: &str, options: DownloadOptions) -> Result<DownloadId> {
        let cfg = self.inner.cfg.read().clone();
        let url = crate::urlsafe::validate_and_normalize(raw_url)?;

        let mut mirrors = Vec::new();
        for m in &options.mirrors {
            mirrors.push(crate::urlsafe::validate_and_normalize(m)?.to_string());
        }

        // initial filename guess (supervisor refines after the probe)
        let guess = filename::resolve_filename(options.filename.as_deref(), None, &url, None);
        let dest_dir = options
            .dest_dir
            .clone()
            .unwrap_or_else(|| cfg.download_dir.clone());
        let candidate = filename::unique_path(&dest_dir, &guess);
        let file_name = candidate
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_else(|| "download.bin")
            .to_string();

        let mut state = DownloadState {
            id: DownloadId::new_v4(),
            url: url.to_string(),
            mirrors,
            filename: file_name,
            dest_dir,
            status: DownloadStatus::Queued,
            options,
            ..Default::default()
        };
        state.expected_sha256 = state.options.verify_sha256.clone();
        state.init_mirror_stats();

        let id = state.id;
        let part_path = download::part_path_for(&state);
        let shared = download::new_shared(
            id,
            state,
            self.inner.buffer_budget.clone(),
            self.inner.global_speed.clone(),
            cfg.default_download_limit,
        );

        self.inner.entries.write().insert(
            id,
            Arc::new(Entry {
                shared,
                ctrl: Mutex::new(None),
            }),
        );

        let _ = download::upsert_index(
            &self.inner.state_dir,
            IndexEntry {
                id,
                part_path: part_path.display().to_string(),
                completed: false,
                url: String::new(),
                filename: String::new(),
                dest_dir: String::new(),
                total_size: None,
                sha256: None,
                created_at: Some(chrono::Utc::now()),
                completed_at: None,
            },
        );

        let _ = self.inner.events.send(EngineEvent::DownloadAdded { id });
        let _ = self.inner.queue_tx.send(QueueCmd::Consider(id));
        Ok(id)
    }

    pub fn pause(&self, id: DownloadId) -> Result<()> {
        let entry = self.entry(id)?;
        let status = entry.shared.state.read().status;
        match status {
            DownloadStatus::Queued => {
                entry.shared.state.write().status = DownloadStatus::Paused;
                Ok(())
            }
            _ => {
                if let Some(tx) = entry.ctrl.lock().as_ref() {
                    let _ = tx.send(Ctrl::Pause);
                } else if entry.shared.state.read().status.is_active() {
                    entry.shared.state.write().status = DownloadStatus::Paused;
                }
                Ok(())
            }
        }
    }

    pub fn pause_all(&self) {
        let ids: Vec<DownloadId> = self.inner.entries.read().keys().copied().collect();
        for id in ids {
            let _ = self.pause(id);
        }
    }

    pub fn resume(&self, id: DownloadId) -> Result<()> {
        let entry = self.entry(id)?;
        let status = entry.shared.state.read().status;
        match status {
            DownloadStatus::Paused | DownloadStatus::Failed | DownloadStatus::Cancelled => {
                let alive = entry
                    .ctrl
                    .lock()
                    .as_ref()
                    .map(|t| !t.is_closed())
                    .unwrap_or(false);
                if alive && status == DownloadStatus::Paused {
                    let _ = entry
                        .ctrl
                        .lock()
                        .as_ref()
                        .unwrap()
                        .send(Ctrl::Resume);
                    let mut st = entry.shared.state.write();
                    st.status = DownloadStatus::Queued;
                    st.error = None;
                } else {
                    *entry.ctrl.lock() = None;
                    {
                        let mut st = entry.shared.state.write();
                        st.status = DownloadStatus::Queued;
                        st.error = None;
                    }
                    let _ = self.inner.queue_tx.send(QueueCmd::Consider(id));
                }
                Ok(())
            }
            DownloadStatus::Queued => Ok(()),
            other => Err(VeloxError::InvalidState(format!(
                "cannot resume from {other:?}"
            ))),
        }
    }

    pub fn resume_all(&self) {
        let ids: Vec<DownloadId> = self.inner.entries.read().keys().copied().collect();
        for id in ids {
            let _ = self.resume(id);
        }
    }

    pub fn cancel(&self, id: DownloadId) -> Result<()> {
        let entry = self.entry(id)?;
        let alive = entry
            .ctrl
            .lock()
            .as_ref()
            .map(|t| !t.is_closed())
            .unwrap_or(false);
        if alive {
            let _ = entry.ctrl.lock().as_ref().unwrap().send(Ctrl::Cancel);
        } else {
            entry.shared.state.write().status = DownloadStatus::Cancelled;
        }
        Ok(())
    }

    /// Remove from the list. `purge` deletes partial data; `delete_final` also
    /// deletes the finished file (only when completed).
    pub fn remove(&self, id: DownloadId, purge: bool, delete_final: bool) -> Result<()> {
        let entry = self.entry(id)?;
        let _ = self.cancel(id);
        let (part, final_path, completed) = {
            let st = entry.shared.state.read();
            (
                download::part_path_for(&st),
                st.dest_dir.join(&st.filename),
                st.status == DownloadStatus::Completed,
            )
        };
        self.inner.entries.write().remove(&id);
        let mut idx = download::load_index(&self.inner.state_dir);
        idx.retain(|e| e.id != id);
        let _ = download::save_index(&self.inner.state_dir, &idx);

        if purge {
            Sidecar::delete(&part);
            let _ = std::fs::remove_file(&part);
            if delete_final && completed {
                let _ = std::fs::remove_file(&final_path);
            }
        }
        let _ = self.inner.events.send(EngineEvent::DownloadRemoved { id });
        Ok(())
    }

    pub fn set_global_limit(&self, bytes_per_sec: Option<u64>) {
        self.inner.global_limiter.set_rate(bytes_per_sec);
        self.update_config(move |c| c.global_speed_limit = bytes_per_sec);
    }

    pub fn set_download_limit(&self, id: DownloadId, bytes_per_sec: Option<u64>) -> Result<()> {
        let entry = self.entry(id)?;
        entry.shared.limiter.set_rate(bytes_per_sec);
        entry.shared.state.write().options.speed_limit = bytes_per_sec;
        Ok(())
    }

    pub fn list(&self) -> Vec<DownloadSnapshot> {
        let entries = self.inner.entries.read();
        let mut v: Vec<DownloadSnapshot> = entries.values().map(|e| e.shared.snapshot()).collect();
        v.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        v
    }

    pub fn snapshot(&self, id: DownloadId) -> Result<DownloadSnapshot> {
        Ok(self.entry(id)?.shared.snapshot())
    }

    pub fn stats(&self) -> EngineStats {
        compute_stats(&self.inner)
    }

    pub fn get_state(&self, id: DownloadId) -> Result<DownloadState> {
        Ok(self.entry(id)?.shared.state.read().clone())
    }

    fn entry(&self, id: DownloadId) -> Result<Arc<Entry>> {
        self.inner
            .entries
            .read()
            .get(&id)
            .cloned()
            .ok_or_else(|| VeloxError::NotFound(id.to_string()))
    }

    /// Recover previous-session downloads from the index (crash-safe defaults).
    fn restore(&self) {
        let cfg = self.inner.cfg.read().clone();
        let index = download::load_index(&self.inner.state_dir);
        for entry in index {
            if entry.completed {
                continue; // history only
            }
            let part = PathBuf::from(&entry.part_path);
            let Ok(Some(mut state)) = Sidecar::load(&part) else {
                continue;
            };
            // If the engine died mid-run, the safest state is Paused.
            if state.status.is_active() {
                state.status = DownloadStatus::Paused;
            }
            if state.status == DownloadStatus::Verifying {
                state.status = DownloadStatus::Paused;
            }
            let id = state.id;
            let shared = download::new_shared(
                id,
                state,
                self.inner.buffer_budget.clone(),
                self.inner.global_speed.clone(),
                cfg.default_download_limit,
            );
            self.inner.entries.write().insert(
                id,
                Arc::new(Entry {
                    shared,
                    ctrl: Mutex::new(None),
                }),
            );
            if cfg.auto_resume {
                let _ = self.resume(id);
            }
        }
    }

    /// Graceful shutdown: pause everything (state is persisted by supervisors).
    pub fn shutdown(&self) {
        self.pause_all();
        std::thread::sleep(Duration::from_millis(300));
    }

    pub fn inner_stats(&self) -> (usize, usize) {
        let entries = self.inner.entries.read();
        (entries.len(), entries.values().filter(|e| e.shared.state.read().status.is_active()).count())
    }
}

fn compute_stats(inner: &EngineInner) -> EngineStats {
    let entries = inner.entries.read();
    let mut active = 0usize;
    let mut queued = 0usize;
    let mut connections = 0usize;
    for e in entries.values() {
        let st = e.shared.state.read();
        match st.status {
            DownloadStatus::Queued => queued += 1,
            s if s.is_active() => {
                active += 1;
                connections += e.shared.active_connections();
            }
            _ => {}
        }
    }
    let (avail, total) = inner.memory.sample();
    EngineStats {
        global_speed_bps: inner.global_speed.rate(),
        active_downloads: active,
        active_connections: connections,
        queued,
        buffer_budget_used: inner.buffer_budget.used(),
        buffer_budget_total: inner.buffer_budget.budget(),
        available_ram: avail,
        total_ram: total,
        speed_history: inner.global_speed.history(),
    }
}

async fn scheduler_loop(inner: Arc<EngineInner>, mut rx: mpsc::UnboundedReceiver<QueueCmd>) {
    let mut pending: Vec<DownloadId> = Vec::new();
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        tokio::select! {
            cmd = rx.recv() => match cmd {
                Some(QueueCmd::Consider(id)) => {
                    if !pending.contains(&id) {
                        pending.push(id);
                    }
                }
                None => return,
            },
            _ = tick.tick() => {},
        }
        loop {
            let max_conc = inner.cfg.read().max_concurrent_downloads as usize;
            let active = {
                let entries = inner.entries.read();
                entries
                    .values()
                    .filter(|e| e.shared.state.read().status.is_active())
                    .count()
            };
            if active >= max_conc {
                break;
            }
            let now = chrono::Utc::now();
            let pick = {
                let entries = inner.entries.read();
                let mut best: Option<(i32, chrono::DateTime<chrono::Utc>, DownloadId)> = None;
                for id in &pending {
                    if let Some(e) = entries.get(id) {
                        let st = e.shared.state.read();
                        if st.status != DownloadStatus::Queued {
                            continue;
                        }
                        if let Some(nb) = st.options.not_before {
                            if nb > now {
                                continue;
                            }
                        }
                        let key = (st.options.priority, st.created_at, *id);
                        let better = match &best {
                            None => true,
                            Some(b) => key.0 > b.0 || (key.0 == b.0 && key.1 < b.1),
                        };
                        if better {
                            best = Some(key);
                        }
                    }
                }
                best.map(|b| b.2)
            };
            match pick {
                Some(id) => {
                    pending.retain(|p| *p != id);
                    start_supervisor(&inner, id);
                }
                None => break,
            }
        }
    }
}

pub(crate) fn start_supervisor(inner: &Arc<EngineInner>, id: DownloadId) {
    let deps = Arc::new(SupervisorDeps {
        client: inner.client.clone(),
        cfg: inner.cfg.clone(),
        state_dir: inner.state_dir.clone(),
        global_limiter: inner.global_limiter.clone(),
        buffer_budget: inner.buffer_budget.clone(),
        global_speed: inner.global_speed.clone(),
        events: inner.events.clone(),
        memory: inner.memory.clone(),
    });
    let (tx, rx) = mpsc::unbounded_channel();
    if let Some(entry) = inner.entries.read().get(&id) {
        *entry.ctrl.lock() = Some(tx);
        let shared = entry.shared.clone();
        // claim the slot SYNCHRONOUSLY so the scheduler's active-count is
        // correct even before the supervisor task first polls.
        {
            let mut st = shared.state.write();
            if st.status == DownloadStatus::Queued {
                st.status = DownloadStatus::Connecting;
            }
        }
        tokio::spawn(supervise(shared, deps, rx));
    }
}
