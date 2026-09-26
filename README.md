<div align="center">

# ⚡ Velox

**Every byte, at full speed.**

A memory-safe hybrid download manager for Windows & Linux — built in Rust to
outperform classic download accelerators on speed, stability, security and UX.

`v1.0.0-rc.1` · MIT licensed · no telemetry, ever

</div>

---

## Why Velox

| | Velox | Classic accelerators |
|---|---|---|
| Memory safety | 100% safe Rust, rustls only | C/C++ stacks, OpenSSL FFI |
| Storage pipeline | Sparse prealloc + positioned writes — **no merge step** | Chunk files + final concat (double I/O) |
| Crash recovery | fsynced sidecars, ETag-guarded resume from exact offsets | Often re-downloads |
| Multi-CDN | Mirror racing + per-mirror health stats | Usually single source |
| Rate limiting | Token-bucket debt model — accurate under 32+ connections | Coarse sleep loops |
| RAM management | Adaptive I/O budget from live system memory | Fixed buffers |
| UI | GPU-accelerated egui, live throughput graph, segment maps | Dated MFC-era dialogs |
| Privacy | Zero telemetry, localhost-only API with token auth | Varies |

## Features

- **Multi-segment engine** — 1–32 parallel connections, dynamic segment stealing, per-segment retry with backoff and mirror rotation
- **Mirror racing** — add mirrors of the same file; Velox ranks them by TTFB, rotates on errors and reports per-mirror bytes served
- **Crash-safe resume** — sidecar metadata fsynced every 2 s / 8 MiB; power loss at any instant resumes exactly; ETag/If-Range guards against changed sources
- **Hybrid BitTorrent (optional)** — `--features torrent` adds librqbit and a race mode against HTTP mirrors
- **HTTP/1.1 + HTTP/2** today, **HTTP/3 detection** (alt-svc) and an experimental h3 transfer path behind `--features http3`
- **Speed limits that actually hold** — token-bucket with debt accounting, global + per-download
- **RAM-adaptive I/O** — buffer sizes and in-flight budgets scale with live available memory
- **Integrity** — optional expected-SHA256 per download, streamed SHA-256 on completion
- **Queue management** — priorities, max concurrent slots, scheduled start (`not_before`), auto-resume
- **Local REST API + SSE** — token-authenticated, loopback-only, powers the browser extension
- **Browser extension** — Chrome/Edge/Firefox (MV3): context-menu and popup capture
- **Real desktop app** — live speed graph, segment maps, mirror health, light/dark themes

## Quick start

### CLI

```bash
velox add https://example.com/big-file.iso          # download (waits, live progress)
velox add URL --mirror URL2 --connections 16        # race two CDNs
velox add URL --limit 5M --sha256 <expected>        # throttle + verify
velox list / show <id> / pause / resume / cancel / remove
velox bench https://fast-host/file --size-mb 64 --connections 8
velox doctor                                        # system diagnostics
velox serve --port 7654 --show-token                # REST API for the extension
```

### Desktop app

```bash
velox-gui                      # full UI — every pixel is driven by the real engine
velox-gui --add <url>          # start with a download queued
```

### From source

```bash
cargo build --release -p velox-gui -p velox-cli
# optional engines:
cargo build --release -p velox-gui --features velox-core/torrent
```

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full decision record.
The engine's core invariant: **claim-then-write with a disk-true ledger** —
workers claim byte ranges under a lock before fetching them, while a separate
disk-position ledger guarantees that persisted state and UI progress only ever
reflect bytes actually on disk. Crash at any instant → resume is exact.

## Validation

- 26 unit tests + 11 hermetic integration tests (local flaky/slow/range test server): pause/resume, hard-crash recovery, multi-segment integrity, retry storms, limit accuracy, redirect hardening
- Real-network runs: 8-connection download byte-identical (SHA-256) to single-stream, rate-limited runs hold the cap under concurrency
- Headless GUI smoke tests with screenshots in CI

See [REPORT.md](REPORT.md) for the latest validation report with numbers and screenshots.

## Security

- Scheme allowlist (http/https), control-character rejection
- Max 8 redirects, scheme re-validated per hop, credentials never forwarded cross-host
- Loopback-only REST API with random token (0600 file)
- No telemetry. Logs stay local. Update checks are opt-in.

See [SECURITY.md](SECURITY.md).

## Repository layout

```
crates/velox-core   # engine: segments, storage, limiters, queue, HTTP, torrent (feature)
crates/velox-cli    # velox CLI + REST daemon (serve)
crates/velox-gui    # eframe desktop app
extension/          # MV3 browser extension (Chrome/Edge/Firefox)
packaging/          # Inno Setup script, Linux .desktop, package scripts
docs/               # architecture decision record
```

## License

MIT — see [LICENSE](LICENSE).
