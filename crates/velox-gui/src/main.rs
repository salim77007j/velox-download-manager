//! Velox desktop application.
//!
//! Everything shown in this UI is driven by the real engine: every progress
//! bar, speed sample, segment rectangle and status pill is computed from live
//! engine state. There are no mock values anywhere.

mod app;

use anyhow::Result;
use eframe::egui;
use std::path::PathBuf;

#[derive(Debug, clap::Parser)]
#[command(name = "velox-gui", about = "Velox desktop app")]
struct GuiArgs {
    /// Download/state directory override
    #[arg(long)]
    dir: Option<PathBuf>,

    /// Add this URL immediately after launch
    #[arg(long = "add")]
    add: Option<String>,

    /// Exit automatically after N seconds (headless smoke tests)
    #[arg(long)]
    quit_after: Option<u64>,

    /// Start maximized
    #[arg(long)]
    maximized: bool,
}

fn main() -> Result<()> {
    let args: GuiArgs = clap::Parser::parse();
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "warn,velox_core=info".into());
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .init();

    // Engine lives on a dedicated multi-thread tokio runtime that stays alive
    // for the whole application.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()?;

    let mut cfg = velox_core::config::EngineConfig::default();
    if let Some(dir) = &args.dir {
        cfg.download_dir = dir.clone();
        cfg.state_dir = Some(dir.join(".velox-meta"));
    }

    let engine = runtime.block_on(async { velox_core::engine::Engine::new(cfg) })?;

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 800.0])
            .with_min_inner_size([980.0, 620.0])
            .with_icon(app::icon())
            .with_title("Velox — every byte, at full speed"),
        ..Default::default()
    };

    let quit_after = args.quit_after;
    let add_url = args.add;

    eframe::run_native(
        "Velox",
        native_options,
        Box::new(move |cc| {
            Ok(Box::new(app::VeloxApp::new(
                cc,
                engine,
                runtime,
                add_url,
                quit_after,
            )))
        }),
    )
    .map_err(|e| anyhow::anyhow!("eframe error: {e}"))?;
    Ok(())
}
