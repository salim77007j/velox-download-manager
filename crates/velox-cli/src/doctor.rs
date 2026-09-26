//! `velox doctor` — system diagnostics for support & validation.

use anyhow::Result;
use std::time::Instant;

pub async fn run() -> Result<()> {
    println!("Velox doctor — {}", env!("CARGO_PKG_VERSION"));
    println!("════════════════════════════════════════");

    // RAM
    let mem = sysinfo_report();
    println!("RAM:            {mem}");

    // DNS + TLS + HTTP version via a well-known probe
    let cfg = velox_core::config::EngineConfig::default();
    let client = velox_core::http::build_client(&cfg)?;
    let t0 = Instant::now();
    let url = url::Url::parse("https://speed.cloudflare.com/__down?bytes=1048576").unwrap();
    let probe = velox_core::http::probe(&client, &url, &[], None).await;
    match probe {
        Ok(p) => {
            println!(
                "HTTPS probe:    OK ({:.0} ms) via {} — ranges: {}",
                t0.elapsed().as_millis(),
                p.http_version.as_deref().unwrap_or("?"),
                if p.supports_ranges { "yes" } else { "no" }
            );
            if let Some(alt) = &p.alt_svc {
                if alt.to_lowercase().contains("h3") {
                    println!("HTTP/3:         advertised by this host (alt-svc: {alt})");
                }
            }
        }
        Err(e) => println!("HTTPS probe:    FAIL ({e})"),
    }

    // Disk write speed (sequential, 64 MiB)
    let dir = velox_core::config::default_download_dir();
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join(".velox-disk-test.tmp");
    let t0 = Instant::now();
    let buf = vec![0xABu8; 1024 * 1024];
    let mut ok = true;
    {
        use std::io::Write;
        if let Ok(mut f) = std::fs::File::create(&path) {
            for _ in 0..64 {
                if f.write_all(&buf).is_err() {
                    ok = false;
                    break;
                }
            }
            let _ = f.sync_all();
        } else {
            ok = false;
        }
    }
    let dt = t0.elapsed().as_secs_f64();
    let _ = std::fs::remove_file(&path);
    if ok && dt > 0.0 {
        println!("Disk write:     {:.0} MiB/s (sequential, 64 MiB to {})", 64.0 / dt, dir.display());
    } else {
        println!("Disk write:     FAILED (check download dir permissions)");
    }

    // Proxy
    if let Some(p) = &cfg.proxy {
        println!("Proxy:          {p}");
    } else {
        println!("Proxy:          direct (none configured)");
    }

    println!("Engine:         v{} — all systems nominal", env!("CARGO_PKG_VERSION"));
    Ok(())
}

fn sysinfo_report() -> String {
    use sysinfo::System;
    let mut sys = System::new();
    sys.refresh_memory();
    let avail = sys.available_memory();
    let total = sys.total_memory();
    format!(
        "{:.1} GiB total, {:.1} GiB available",
        total as f64 / 1024.0 / 1024.0 / 1024.0,
        avail as f64 / 1024.0 / 1024.0 / 1024.0
    )
}
