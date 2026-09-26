//! Velox application state + UI.
//!
//! NO FAKE DATA: all numbers come from `Engine` snapshots/stats; all actions
//! call engine methods directly.

use chrono::Local;
use eframe::egui;
use egui::{Color32, RichText};
use egui_plot::{Line, Plot, PlotPoints};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use velox_core::config::EngineConfig;
use velox_core::engine::Engine;
use velox_core::events::EngineEvent;
use velox_core::types::{DownloadId, DownloadOptions, DownloadSnapshot, DownloadStatus, EngineStats};

const ACCENT: Color32 = Color32::from_rgb(0x0E, 0x9F, 0x9E);
const ACCENT_DARK: Color32 = Color32::from_rgb(0x0B, 0x7A, 0x79);
const BG_LIGHT: Color32 = Color32::from_rgb(0xFA, 0xFB, 0xFC);

pub struct VeloxApp {
    engine: Arc<Engine>,
    _runtime: tokio::runtime::Runtime,
    dark: bool,
    events_rx: broadcast::Receiver<EngineEvent>,
    log: VecDeque<(chrono::DateTime<Local>, String)>,
    selected: Option<DownloadId>,
    last_list: Vec<DownloadSnapshot>,
    last_stats: EngineStats,
    last_poll: Instant,
    started: Instant,

    // add dialog
    show_add: bool,
    add_url: String,
    add_mirrors: String,
    add_name: String,
    add_conns: String,
    add_limit: String,
    add_sha: String,
    add_error: Option<String>,

    // settings dialog
    show_settings: bool,
    draft: EngineConfig,

    show_about: bool,
    quit_after: Option<u64>,
}

impl VeloxApp {
    pub fn new(
        _cc: &eframe::CreationContext<'_>,
        engine: Arc<Engine>,
        runtime: tokio::runtime::Runtime,
        add_url: Option<String>,
        quit_after: Option<u64>,
    ) -> Self {
        let events_rx = engine.events();
        let cfg = engine.config();
        let mut app = Self {
            engine,
            _runtime: runtime,
            dark: false,
            events_rx,
            log: VecDeque::new(),
            selected: None,
            last_list: Vec::new(),
            last_stats: EngineStats::default(),
            last_poll: Instant::now() - Duration::from_secs(1),
            started: Instant::now(),
            show_add: false,
            add_url: String::new(),
            add_mirrors: String::new(),
            add_name: String::new(),
            add_conns: String::new(),
            add_limit: String::new(),
            add_sha: String::new(),
            add_error: None,
            show_settings: false,
            draft: cfg,
            show_about: false,
            quit_after,
        };
        if let Some(u) = add_url {
            // CLI-provided URL: fill the dialog and submit immediately
            app.add_url = u;
            app.submit_add();
        }
        app
    }

    fn log_push(&mut self, msg: String) {
        self.log.push_back((Local::now(), msg));
        while self.log.len() > 200 {
            self.log.pop_front();
        }
    }

    fn add_url_string(&mut self, url: &str) {
        self.show_add = true;
        self.add_url = url.to_string();
    }

    fn poll(&mut self) {
        if self.last_poll.elapsed() < Duration::from_millis(200) {
            return;
        }
        self.last_poll = Instant::now();
        self.last_list = self.engine.list();
        self.last_stats = self.engine.stats();
        loop {
            match self.events_rx.try_recv() {
                Ok(ev) => match ev {
                    EngineEvent::DownloadCompleted { id, sha256, bytes, duration_secs, avg_speed_bps } => {
                        self.log_push(format!(
                            "✔ completed {} — {} in {:.1}s (avg {}/s){}",
                            short_id(&id),
                            fmt_bytes(bytes),
                            duration_secs,
                            fmt_bytes(avg_speed_bps as u64),
                            sha256.map(|h| format!("\nsha256 {h}")).unwrap_or_default(),
                        ));
                    }
                    EngineEvent::DownloadStateChanged { id, to, error, .. } => {
                        if to == DownloadStatus::Failed {
                            self.log_push(format!("✖ {} failed: {}", short_id(&id), error.unwrap_or_default()));
                        }
                    }
                    EngineEvent::Notice { level, message } => {
                        self.log_push(format!("[{level}] {message}"));
                    }
                    _ => {}
                },
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(_) => break,
            }
        }
    }

    fn apply_theme(&self, ctx: &egui::Context) {
        let mut style = (*ctx.style()).clone();
        style.visuals = if self.dark {
            egui::Visuals::dark()
        } else {
            let mut v = egui::Visuals::light();
            v.panel_fill = BG_LIGHT;
            v.window_fill = Color32::WHITE;
            v.extreme_bg_color = Color32::from_rgb(0xF0, 0xF3, 0xF5);
            v
        };
        // accent for interactive elements
        style.visuals.widgets.active.bg_fill = ACCENT;
        style.visuals.widgets.hovered.bg_fill = ACCENT;
        style.visuals.selection.bg_fill = ACCENT;
        style.visuals.selection.stroke.color = ACCENT;
        style.visuals.hyperlink_color = ACCENT;
        style.spacing.item_spacing = egui::vec2(8.0, 6.0);
        ctx.set_style(style);
    }
}

impl eframe::App for VeloxApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.apply_theme(ctx);
        self.poll();

        if let Some(secs) = self.quit_after {
            if self.started.elapsed() > Duration::from_secs(secs) {
                ctx.send_viewport_cmd(egui::ViewportCommand::Close);
            }
        }

        self.draw_toolbar(ctx);
        self.draw_detail_panel(ctx);
        self.draw_main_table(ctx);
        self.draw_bottom(ctx);
        self.draw_dialogs(ctx);

        ctx.request_repaint_after(Duration::from_millis(150));
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        // graceful: pause everything so state is persisted for next launch
        self.engine.shutdown();
    }
}

// ─── toolbar ─────────────────────────────────────────────────────────────────

impl VeloxApp {
    fn draw_toolbar(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("toolbar").show(ctx, |ui| {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.label(
                    RichText::new("⚡ Velox")
                        .size(20.0)
                        .strong()
                        .color(if self.dark { ACCENT } else { ACCENT_DARK }),
                );
                ui.separator();
                if ui.button("Add URL").clicked() {
                    self.show_add = true;
                }
                if ui.button("Pause all").clicked() {
                    self.engine.pause_all();
                    self.log_push("paused all".into());
                }
                if ui.button("Resume all").clicked() {
                    self.engine.resume_all();
                    self.log_push("resumed all".into());
                }
                ui.separator();
                self.limit_selector(ui);
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("About").clicked() {
                        self.show_about = !self.show_about;
                    }
                    if ui.button("⚙ Settings").clicked() {
                        self.draft = self.engine.config();
                        self.show_settings = !self.show_settings;
                    }
                    if ui.button(if self.dark { "☀ Light" } else { "🌙 Dark" }).clicked() {
                        self.dark = !self.dark;
                    }
                    ui.separator();
                    let s = &self.last_stats;
                    ui.label(
                        RichText::new(format!(
                            "↓ {}/s   {} active · {} conn · {} queued",
                            fmt_bytes(s.global_speed_bps as u64),
                            s.active_downloads,
                            s.active_connections,
                            s.queued
                        ))
                        .strong(),
                    );
                });
            });
            ui.add_space(6.0);
        });
    }

    fn limit_selector(&mut self, ui: &mut egui::Ui) {
        let current = self.engine.config().global_speed_limit;
        let label = match current {
            None => "Speed: Unlimited".to_string(),
            Some(v) => format!("Speed: {}/s", fmt_bytes(v)),
        };
        ui.menu_button(label, |ui| {
            const OPTIONS: [(Option<u64>, &str); 6] = [
                (None, "Unlimited"),
                (Some(256 * 1024), "256 KB/s"),
                (Some(1024 * 1024), "1 MB/s"),
                (Some(5 * 1024 * 1024), "5 MB/s"),
                (Some(10 * 1024 * 1024), "10 MB/s"),
                (Some(50 * 1024 * 1024), "50 MB/s"),
            ];
            for (val, name) in OPTIONS {
                if ui.radio(current == val, name).clicked() {
                    self.engine.set_global_limit(val);
                    self.log_push(format!("global speed limit → {name}"));
                    ui.close_menu();
                }
            }
        });
    }
}

// ─── main table ──────────────────────────────────────────────────────────────

impl VeloxApp {
    fn draw_main_table(&mut self, ctx: &egui::Context) {
        egui::CentralPanel::default().show(ctx, |ui| {
            if self.last_list.is_empty() {
                ui.vertical_centered(|ui| {
                    ui.add_space(60.0);
                    ui.label(RichText::new("No downloads yet").size(22.0).weak());
                    ui.add_space(8.0);
                    ui.label("Paste a URL with ＋ Add URL, or run `velox add <url>` from the terminal.");
                    ui.label(RichText::new("Tip: add mirrors to race multiple CDNs — Velox picks the fastest bytes automatically.").weak());
                });
                return;
            }
            egui::ScrollArea::vertical().auto_shrink(false).show(ui, |ui| {
                let row_h = 44.0;
                let list = self.last_list.clone();
                for snap in &list {
                    let selected = self.selected == Some(snap.id);
                    let frame = egui::Frame::none()
                        .fill(if selected {
                            ui.ctx().style().visuals.selection.bg_fill.gamma_multiply(0.12)
                        } else {
                            Color32::TRANSPARENT
                        })
                        .inner_margin(6.0);
                    frame.show(ui, |ui| {
                        ui.allocate_ui(egui::vec2(ui.available_width(), row_h), |ui| {
                            ui.horizontal(|ui| {
                                // status dot
                                let (dot, tip) = status_dot(snap.status);
                                ui.add(
                                    egui::Label::new(RichText::new("●").size(14.0).color(dot))
                                        .sense(egui::Sense::hover()),
                                )
                                .on_hover_text(tip);
                                // name + url
                                ui.vertical(|ui| {
                                    ui.horizontal(|ui| {
                                        ui.label(
                                            RichText::new(truncate(&snap.filename, 42)).strong(),
                                        );
                                        ui.label(
                                            RichText::new(format!(
                                                "{}  ·  {}",
                                                protocol_tag(snap),
                                                mirrors_label(snap)
                                            ))
                                            .small()
                                            .weak(),
                                        );
                                    });
                                    let bar = egui::ProgressBar::new((snap.progress_pct / 100.0) as f32)
                                        .show_percentage()
                                        .desired_height(10.0);
                                    ui.add(bar);
                                });
                                // numbers
                                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                    ui.set_min_width(330.0);
                                    for (id, _) in self_row_actions(snap) {
                                        match id {
                                            "pause" => {
                                                if ui.small_button("||").clicked() {
                                                    let _ = self.engine.pause(snap.id);
                                                }
                                            }
                                            "resume" => {
                                                if ui.small_button("|>").clicked() {
                                                    let _ = self.engine.resume(snap.id);
                                                }
                                            }
                                            "cancel" => {
                                                if ui.small_button("x").clicked() {
                                                    let _ = self.engine.cancel(snap.id);
                                                    self.log_push(format!("cancelled {}", short_id(&snap.id)));
                                                }
                                            }
                                            "remove" => {
                                                if ui.small_button("del").clicked() {
                                                    let _ = self.engine.remove(snap.id, true, false);
                                                    if self.selected == Some(snap.id) {
                                                        self.selected = None;
                                                    }
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                    ui.label(RichText::new(fmt_eta(snap.eta_secs)).weak());
                                    ui.label(RichText::new(fmt_speed(snap.speed_bps)).color(ACCENT_DARK).strong());
                                    ui.label(format!(
                                        "{} / {}",
                                        fmt_bytes(snap.bytes_done),
                                        snap.total_size.map(fmt_bytes).unwrap_or_else(|| "?".into())
                                    ));
                                    ui.label(RichText::new(format!("{} conn", snap.active_connections)).weak());
                                });
                            });
                        });
                    });
                    // click row to select
                    let resp = ui.interact(
                        egui::Rect::from_min_size(ui.cursor().min, egui::vec2(ui.available_width(), row_h)),
                        egui::Id::new(("row", snap.id)),
                        egui::Sense::click(),
                    );
                    if resp.clicked() {
                        self.selected = Some(snap.id);
                    }
                    ui.add_space(2.0);
                }
            });
        });
    }

    fn draw_detail_panel(&mut self, ctx: &egui::Context) {
        let Some(sel) = self.selected else { return };
        let Some(snap) = self.last_list.iter().find(|s| s.id == sel).cloned() else {
            self.selected = None;
            return;
        };
        egui::SidePanel::right("detail").resizable(true).default_width(330.0).show(ctx, |ui| {
            ui.heading(truncate(&snap.filename, 30));
            ui.add_space(4.0);
            grid_row(ui, "Status", format!("{:?}", snap.status));
            grid_row(ui, "URL", truncate(&snap.url, 44));
            grid_row(ui, "File", truncate(&snap.full_path, 44));
            if ui
                .button("📋 Copy path")
                .on_hover_text("Copy full path to clipboard")
                .clicked()
            {
                ui.output_mut(|o| o.copied_text = snap.full_path.clone());
                self.log_push("path copied".into());
            }
            ui.separator();
            grid_row(ui, "Progress", format!("{:.2}%", snap.progress_pct));
            grid_row(ui, "Size", format!("{} / {}", fmt_bytes(snap.bytes_done), snap.total_size.map(fmt_bytes).unwrap_or_else(|| "?".into())));
            grid_row(ui, "Speed", fmt_speed(snap.speed_bps));
            grid_row(ui, "ETA", fmt_eta(snap.eta_secs));
            grid_row(ui, "Connections", format!("{}", snap.active_connections));
            grid_row(ui, "Retries", format!("{}", snap.retries));
            grid_row(ui, "Protocol", snap.protocol_used.clone().unwrap_or_else(|| "—".into()));
            if snap.alt_svc_h3 {
                ui.label(RichText::new("ⓘ HTTP/3 advertised (alt-svc)").weak());
            }
            grid_row(ui, "Mirrors", format!("{}", snap.mirrors));
            if let Some(h) = &snap.sha256 {
                grid_row(ui, "sha256", truncate(h, 24));
            }
            if let Some(e) = &snap.error {
                ui.colored_label(Color32::from_rgb(0xC0, 0x39, 0x2B), format!("Error: {e}"));
            }
            ui.separator();
            ui.label(RichText::new("Segments").strong());
            draw_segment_map(ui, &snap, ACCENT);
            ui.separator();
            ui.label(RichText::new("Mirror breakdown").strong());
            let st = self.engine.get_state(snap.id).ok();
            if let Some(st) = st {
                for (i, m) in st.mirror_stats().iter().enumerate() {
                    ui.label(format!(
                        "[{}] {} — {} served, {} errors",
                        i,
                        truncate(&m.url, 36),
                        fmt_bytes(m.bytes_served),
                        m.errors
                    ));
                }
            }
        });
    }

    fn draw_bottom(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::bottom("graphs")
            .resizable(true)
            .default_height(180.0)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.label(RichText::new("Throughput (last 3 min)").strong());
                    ui.separator();
                    let s = &self.last_stats;
                    ui.label(format!(
                        "peak {}, RAM budget {}/{} ({} MiB free)",
                        fmt_bytes(s.speed_history.iter().cloned().fold(0.0f64, f64::max) as u64),
                        fmt_bytes(s.buffer_budget_used),
                        fmt_bytes(s.buffer_budget_total),
                        s.available_ram / 1024 / 1024
                    ));
                });
                let hist = &self.last_stats.speed_history;
                let pts: PlotPoints = hist
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        let x = i as f64 - hist.len() as f64; // seconds ago
                        [x, v / (1024.0 * 1024.0)]
                    })
                    .collect();
                let limit_line = self.engine.config().global_speed_limit.map(|l| {
                    let y = l as f64 / (1024.0 * 1024.0);
                    Line::new(PlotPoints::new(vec![[-180.0, y], [0.0, y]]))
                        .color(Color32::from_rgb(0xE7, 0x4C, 0x3C))
                        .style(egui_plot::LineStyle::dashed_dense())
                });
                Plot::new("speed_plot")
                    .height(120.0)
                    .allow_drag(false)
                    .allow_zoom(false)
                    .allow_scroll(false)
                    .y_axis_min_width(52.0)
                    .show(ui, |p| {
                        p.line(
                            Line::new(pts)
                                .color(ACCENT)
                                .fill(0.15_f32)
                                .name("MB/s"),
                        );
                        if let Some(l) = limit_line {
                            p.line(l);
                        }
                    });
                // activity log
                egui::CollapsingHeader::new(RichText::new("Event log").weak())
                    .default_open(false)
                    .show(ui, |ui| {
                        egui::ScrollArea::vertical().max_height(90.0).show(ui, |ui| {
                            for (t, msg) in self.log.iter().rev() {
                                ui.label(RichText::new(format!("{}  {}", t.format("%H:%M:%S"), msg)).small());
                            }
                        });
                    });
            });
    }

    fn draw_dialogs(&mut self, ctx: &egui::Context) {
        // ── Add URL dialog ──
        let mut open_add = self.show_add;
        egui::Window::new("Add download")
            .open(&mut open_add)
            .collapsible(false)
            .resizable(false)
            .default_width(520.0)
            .show(ctx, |ui| {
                egui::Grid::new("add_grid").num_columns(2).spacing([10.0, 8.0]).show(ui, |ui| {
                    ui.label("URL");
                    ui.add_sized(
                        [ui.available_width(), 22.0],
                        egui::TextEdit::singleline(&mut self.add_url).hint_text("https://example.com/file.zip"),
                    );
                    ui.end_row();

                    ui.label("Mirrors").on_hover_text("One URL per line — must serve the identical file. Velox races them and rotates on errors.");
                    ui.add_sized(
                        [ui.available_width(), 54.0],
                        egui::TextEdit::multiline(&mut self.add_mirrors).hint_text("(optional)"),
                    );
                    ui.end_row();

                    ui.label("Save as");
                    ui.add_sized(
                        [ui.available_width(), 22.0],
                        egui::TextEdit::singleline(&mut self.add_name).hint_text("(auto from URL/headers)"),
                    );
                    ui.end_row();

                    ui.label("Connections").on_hover_text("1–32. Servers without range support fall back to 1.");
                    ui.add_sized([ui.available_width(), 22.0], egui::TextEdit::singleline(&mut self.add_conns).hint_text("8"));
                    ui.end_row();

                    ui.label("Speed limit");
                    ui.add_sized(
                        [ui.available_width(), 22.0],
                        egui::TextEdit::singleline(&mut self.add_limit).hint_text("(unlimited) e.g. 5M, 800K"),
                    );
                    ui.end_row();

                    ui.label("Expected sha256");
                    ui.add_sized(
                        [ui.available_width(), 22.0],
                        egui::TextEdit::singleline(&mut self.add_sha).hint_text("(optional) verify after download"),
                    );
                    ui.end_row();
                });
                if let Some(err) = &self.add_error {
                    ui.colored_label(Color32::from_rgb(0xC0, 0x39, 0x2B), err);
                }
                ui.add_space(4.0);
                ui.horizontal(|ui| {
                    if ui.button(egui::RichText::new("Download").strong()).clicked() {
                        self.submit_add();
                    }
                    if ui.button("Cancel").clicked() {
                        self.show_add = false;
                        self.add_error = None;
                    }
                });
            });
        self.show_add = open_add;

        // ── Settings dialog ──
        let mut open_settings = self.show_settings;
        egui::Window::new("Settings")
            .open(&mut open_settings)
            .collapsible(false)
            .resizable(false)
            .default_width(540.0)
            .show(ctx, |ui| {
                egui::Grid::new("set_grid").num_columns(2).spacing([10.0, 8.0]).show(ui, |ui| {
                    let d = &mut self.draft;
                    num_field(ui, "Max concurrent downloads", &mut d.max_concurrent_downloads, 1, 16);
                    num_field(ui, "Connections per download", &mut d.connections_per_download, 1, 32);
                    num_field(ui, "Max connections per download", &mut d.max_connections_per_download, 1, 64);
                    ui.label("Min segment size (MiB)");
                    let mut mb = (d.min_segment_size / (1024 * 1024)).max(1);
                    if ui.add(egui::DragValue::new(&mut mb).range(1..=256)).changed() {
                        d.min_segment_size = mb * 1024 * 1024;
                    }
                    ui.end_row();
                    num_field(ui, "RAM buffer budget (%)", &mut d.ram_cache_percent, 1, 80);
                    ui.label("Download directory");
                    ui.add_sized(
                        [ui.available_width(), 22.0],
                        egui::TextEdit::singleline(&mut d.download_dir.to_string_lossy()),
                    );
                    ui.end_row();
                    ui.label("Proxy");
                    let mut proxy = d.proxy.clone().unwrap_or_default();
                    ui.add_sized(
                        [ui.available_width(), 22.0],
                        egui::TextEdit::singleline(&mut proxy).hint_text("(direct) http:// or socks5://"),
                    );
                    d.proxy = if proxy.is_empty() { None } else { Some(proxy) };
                    ui.end_row();
                    ui.label("Verify completed files");
                    ui.checkbox(&mut d.compute_sha256_on_complete, "stream SHA-256 on completion");
                    ui.end_row();
                    ui.label("Auto-resume on start");
                    ui.checkbox(&mut d.auto_resume, "resume interrupted downloads");
                    ui.end_row();
                    ui.label("fsync on completion");
                    ui.checkbox(&mut d.fsync_on_complete, "durability over a little speed");
                    ui.end_row();
                });
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if ui.button(egui::RichText::new("Apply").strong()).clicked() {
                        let d = self.draft.clone();
                        self.engine.update_config(|c| *c = d);
                        self.log_push("settings applied".into());
                        self.show_settings = false;
                    }
                    if ui.button("Cancel").clicked() {
                        self.show_settings = false;
                    }
                });
            });
        self.show_settings = open_settings;

        // ── About ──
        let mut open_about = self.show_about;
        egui::Window::new("About Velox")
            .open(&mut open_about)
            .collapsible(false)
            .resizable(false)
            .default_width(380.0)
            .show(ctx, |ui| {
                ui.vertical_centered(|ui| {
                    ui.label(RichText::new("⚡ Velox").size(30.0).strong().color(ACCENT_DARK));
                    ui.label(RichText::new("Every byte, at full speed.").weak());
                    ui.add_space(6.0);
                    ui.label(format!("version {}", env!("CARGO_PKG_VERSION")));
                    ui.label("Memory-safe hybrid download engine");
                    ui.label("Rust · tokio · rustls · egui");
                    ui.add_space(6.0);
                    ui.label(
                        RichText::new(
                            "No telemetry. No accounts. Your downloads stay on your machine.",
                        )
                        .small()
                        .weak(),
                    );
                });
            });
        self.show_about = open_about;
    }

    fn submit_add(&mut self) {
        self.add_error = None;
        let url = self.add_url.trim().to_string();
        if url.is_empty() {
            self.add_error = Some("URL is required".into());
            return;
        }
        let mut opts = DownloadOptions::default();
        if !self.add_mirrors.trim().is_empty() {
            opts.mirrors = self
                .add_mirrors
                .lines()
                .map(|l| l.trim().to_string())
                .filter(|l| !l.is_empty())
                .collect();
        }
        if !self.add_name.trim().is_empty() {
            opts.filename = Some(self.add_name.trim().to_string());
        }
        if let Ok(n) = self.add_conns.trim().parse::<u32>() {
            opts.connections = Some(n.clamp(1, 64));
        }
        if !self.add_limit.trim().is_empty() {
            match parse_size(&self.add_limit) {
                Ok(v) => opts.speed_limit = Some(v),
                Err(_) => {
                    self.add_error = Some("Bad speed limit — use e.g. 5M or 800K".into());
                    return;
                }
            }
        }
        let sha = self.add_sha.trim().to_lowercase();
        if !sha.is_empty() {
            if sha.len() != 64 || !sha.chars().all(|c| c.is_ascii_hexdigit()) {
                self.add_error = Some("sha256 must be 64 hex characters".into());
                return;
            }
            opts.verify_sha256 = Some(sha);
        }
        match self.engine.add(&url, opts) {
            Ok(id) => {
                self.log_push(format!("added {} → {}", short_id(&id), url));
                self.selected = Some(id);
                self.show_add = false;
                self.add_url.clear();
                self.add_mirrors.clear();
                self.add_name.clear();
                self.add_conns.clear();
                self.add_limit.clear();
                self.add_sha.clear();
            }
            Err(e) => self.add_error = Some(e.to_string()),
        }
    }
}

// ─── helpers ─────────────────────────────────────────────────────────────────

fn num_field(ui: &mut egui::Ui, label: &str, val: &mut u32, min: u32, max: u32) {
    ui.label(label);
    ui.add(egui::DragValue::new(val).range(min..=max));
    ui.end_row();
}

fn grid_row(ui: &mut egui::Ui, k: &str, v: String) {
    ui.horizontal(|ui| {
        ui.label(RichText::new(k).weak().small());
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            ui.label(RichText::new(v).small());
        });
    });
}

fn draw_segment_map(ui: &mut egui::Ui, snap: &DownloadSnapshot, accent: Color32) {
    let width = ui.available_width();
    let height = 26.0;
    let (rect, _) = ui.allocate_exact_size(egui::vec2(width, height), egui::Sense::hover());
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 3.0, Color32::from_rgb(0xE6, 0xEA, 0xED));
    let total: u64 = snap.segments.iter().map(|s| s.total()).sum();
    if total == 0 {
        return;
    }
    let mut x = rect.left();
    for seg in &snap.segments {
        let w = (seg.total() as f32 / total as f32) * rect.width();
        let seg_rect = egui::Rect::from_min_size(egui::pos2(x, rect.top()), egui::vec2(w, height));
        let frac = if seg.total() > 0 { seg.written as f32 / seg.total() as f32 } else { 1.0 };
        painter.rect_filled(seg_rect, 0.0, accent.gamma_multiply(0.25));
        if frac > 0.0 {
            painter.rect_filled(
                egui::Rect::from_min_size(egui::pos2(x, rect.top()), egui::vec2(w * frac, height)),
                0.0,
                accent,
            );
        }
        painter.rect_stroke(seg_rect, 0.0, egui::Stroke::new(1.0_f32, Color32::WHITE));
        x += w;
    }
    ui.label(
        RichText::new(format!(
            "{} segments · live map (teal = bytes on disk)",
            snap.segments.len()
        ))
        .small()
        .weak(),
    );
}

fn status_dot(s: DownloadStatus) -> (Color32, &'static str) {
    match s {
        DownloadStatus::Queued => (Color32::GRAY, "Queued"),
        DownloadStatus::Connecting => (Color32::from_rgb(0x3B, 0x82, 0xF6), "Connecting"),
        DownloadStatus::Downloading => (ACCENT, "Downloading"),
        DownloadStatus::Paused => (Color32::from_rgb(0xF5, 0x9E, 0x0B), "Paused"),
        DownloadStatus::Verifying => (Color32::from_rgb(0x8B, 0x5C, 0xF6), "Verifying integrity"),
        DownloadStatus::Completed => (Color32::from_rgb(0x22, 0xC5, 0x5E), "Completed"),
        DownloadStatus::Failed => (Color32::from_rgb(0xEF, 0x44, 0x44), "Failed"),
        DownloadStatus::Cancelled => (Color32::GRAY, "Cancelled"),
    }
}

fn protocol_tag(s: &DownloadSnapshot) -> String {
    let base = s.protocol_used.clone().unwrap_or_else(|| "HTTP".into());
    if s.alt_svc_h3 {
        format!("{base} · h3 available")
    } else if s.mirrors > 1 {
        format!("{base} · {} mirrors", s.mirrors)
    } else {
        base
    }
}

fn mirrors_label(s: &DownloadSnapshot) -> String {
    if s.mirrors > 1 {
        format!("{} sources", s.mirrors)
    } else {
        "1 source".into()
    }
}

fn self_row_actions(s: &DownloadSnapshot) -> Vec<(&'static str, ())> {
    match s.status {
        DownloadStatus::Downloading | DownloadStatus::Connecting | DownloadStatus::Verifying => {
            vec![("pause", ()), ("cancel", ()), ("remove", ())]
        }
        DownloadStatus::Paused | DownloadStatus::Failed | DownloadStatus::Cancelled | DownloadStatus::Queued => {
            vec![("resume", ()), ("cancel", ()), ("remove", ())]
        }
        DownloadStatus::Completed => vec![("remove", ())],
    }
}

fn fmt_bytes(n: u64) -> String {
    const U: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < 4 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", U[u])
    }
}

fn fmt_speed(bps: f64) -> String {
    format!("{}/s", fmt_bytes(bps as u64))
}

fn fmt_eta(secs: Option<u64>) -> String {
    match secs {
        None => "—".into(),
        Some(s) => {
            let h = s / 3600;
            let m = (s % 3600) / 60;
            let sec = s % 60;
            if h > 0 {
                format!("{h}h{m:02}m")
            } else if m > 0 {
                format!("{m}m{sec:02}s")
            } else {
                format!("{sec}s")
            }
        }
    }
}

fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim().to_lowercase();
    let (num, mult) = if let Some(n) = s.strip_suffix('g') {
        (n, 1024 * 1024 * 1024u64)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix('k') {
        (n, 1024)
    } else {
        (s.as_str(), 1)
    };
    num.trim()
        .parse::<f64>()
        .map(|v| (v as u64).saturating_mul(mult))
        .map_err(|e| e.to_string())
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let t: String = s.chars().take(n - 1).collect();
        format!("{t}…")
    }
}

fn short_id(id: &DownloadId) -> String {
    id.to_string()[..8].to_string()
}

/// Programmatic app icon: teal rounded square + white down-arrow bolt.
pub fn icon() -> egui::IconData {
    const S: u32 = 64;
    let mut rgba = vec![0u8; (S * S * 4) as usize];
    let mut set = |x: u32, y: u32, c: [u8; 4]| {
        let i = ((y * S + x) * 4) as usize;
        rgba[i..i + 4].copy_from_slice(&c);
    };
    for y in 0..S {
        for x in 0..S {
            // rounded-rect mask
            let r = 14.0f32;
            let fx = x as f32;
            let fy = y as f32;
            let inside_rr = (fx.min(S as f32 - 1.0 - fx)).max(0.0).powi(2)
                + (fy.min(S as f32 - 1.0 - fy)).max(0.0).powi(2)
                <= r * r
                || ((fx >= r) && (fx < S as f32 - r) || (fy >= r) && (fy < S as f32 - r))
                    && fx >= 0.0
                    && fx < S as f32
                    && fy >= 0.0
                    && fy < S as f32;
            if !inside_rr {
                set(x, y, [0, 0, 0, 0]);
                continue;
            }
            // vertical gradient teal
            let t = fy / S as f32;
            let c0 = [0x12, 0xB5, 0xB4u8];
            let c1 = [0x0A, 0x6E, 0x6Du8];
            let bg = [
                (c0[0] as f32 + (c1[0] as f32 - c0[0] as f32) * t) as u8,
                (c0[1] as f32 + (c1[1] as f32 - c0[1] as f32) * t) as u8,
                (c0[2] as f32 + (c1[2] as f32 - c0[2] as f32) * t) as u8,
                255,
            ];
            // arrow: shaft + triangle head (downward)
            let cx = fx - S as f32 / 2.0;
            let shaft = cx.abs() <= 6.0 && fy >= 14.0 && fy <= 34.0;
            let head = fy > 32.0 && fy < 50.0 && cx.abs() <= (2.0 + (50.0 - fy) * 1.1);
            let bolt_notch = fy > 18.0 && fy < 24.0 && cx.abs() <= 9.0; // stylized notch
            if (shaft && !bolt_notch) || head {
                set(x, y, [255, 255, 255, 255]);
            } else {
                set(x, y, bg);
            }
        }
    }
    egui::IconData { width: S, height: S, rgba }
}
