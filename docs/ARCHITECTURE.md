# Velox — Architecture Decision Record (v1.0.0-rc.1)

**Product:** Velox — a memory-safe, hybrid download manager for Windows & Linux.
**Positioning:** Direct competitor to IDM: faster on flaky/multi-CDN networks, safer by construction, modern UI.

## 1. Branding
- Name: **Velox** (Latin: *swift*). Tagline: *"Every byte, at full speed."*
- Original identity: clean white theme (default) + dark mode, teal/graphite accent, live speed graph, segment map, health indicators.
- No IDM code, assets, names, or designs are used anywhere.

## 2. Technology choices (and why)

| Concern | Decision | Alternatives considered |
|---|---|---|
| Language | Rust (memory safety, zero-cost async, single static binaries) | Go (GC pauses + larger runtime), C++ (unsafe) |
| Async runtime | tokio (multi-thread) | async-std (maintenance mode) |
| HTTP client | reqwest 0.12 + rustls (h1 + h2) | hyper direct (more control, more code), curl (FFI/unsafe) |
| HTTP/3 | `http3` cargo feature (reqwest/quinn, experimental) + **Alt-Svc h3 detection in default builds** | Making h3 default — rejected: pre-1.0 stack, stability > hype |
| BitTorrent | librqbit behind `torrent` feature; hybrid *race mode* (HTTP mirrors vs torrent, first to finish wins) | custom DHT (months of work, no benefit) |
| GUI | egui/eframe 0.29 (GPU-accelerated, pure Rust, Win+Linux) | Tauri (JS split-brain), GTK (build pain on Windows), Qt (licensing) |
| Charts | egui_plot | plotters (no interactivity) |
| Storage I/O | Sparse preallocation + per-segment `write_at` → **no final merge step, ever** | Chunk files + concat (IDM-style; double disk I/O) |
| Integrity | per-segment accounting + optional full-file SHA-256 verification | CRC only (weaker guarantee) |
| Persistence | Atomic JSON sidecars (tmp+fsync+rename), lockfile-protected state dir | SQLite (extra dep; JSON fine at <10k rows) |
| Rate limiting | Token-bucket (global + per-download), zero busy-wait | fixed-window (bursty) |
| RAM-adaptive I/O | sysinfo sampler → buffer size profile + bounded allocation budget | fixed buffers (wastes or starves) |

## 3. Engine architecture

```
                 ┌───────────────────────────────────────────────┐
   add(url) ───► │ Queue/Scheduler (priorities, slots, windows)  │
   mirrors?      └──────────────┬────────────────────────────────┘
                                 │ start
                 ┌───────────────▼────────────────┐
                 │ Download supervisor (per dl)   │  probe → allocate(sparse)
                 │  · ETag/If-Range validation    │  → spawn N segment workers
                 │  · pause/resume/cancel ctrl    │  → monitor → finalize/verify
                 └───┬───────────┬───────────┬────┘
                     │           │           │        retry+backoff, mirror
                [seg 0]      [seg 1] ... [seg N]      rotation, dynamic split
                     │           │           │
                     ▼           ▼           ▼
             SparseFile::write_at(offset)     ← direct positioned writes
                     │
             token-bucket limiters (global + per-dl)  ← RAM-adaptive buffers
```

Key properties:
- **No merge step:** every worker writes its byte range directly into the final file at absolute offsets.
- **Crash-safe:** sidecar meta (segment offsets, ETag, size) fsynced atomically ≥ every 2 s / 8 MiB; on restart, downloads resume from exact offsets; ETag/If-Range guards against changed sources.
- **Adaptive parallelism:** finished workers steal half of the largest remaining segment (`min_segment_size` floor).
- **Mirror racing:** multiple mirrors treated as one logical source; workers rotate mirrors on retry; probes rank by TTFB.
- **Hybrid:** with `torrent` feature, a torrent source can run against HTTP mirrors; first finisher wins, loser is cancelled (honest, useful acceleration — not marketing).

## 4. Security model (2026 threat set)
- 100% safe Rust (no `unsafe` in our crates; dependencies audited list).
- rustls only — no native TLS, no OpenSSL FFI.
- URL hardening: scheme allowlist (http/https), credential redaction in logs, control-char rejection.
- Redirect hardening: max 8 hops, scheme re-validation per hop, **Authorization stripped on cross-host redirect** (creds re-applied only via manual same-host flow).
- Sandboxed parsing: untrusted headers (Content-Disposition) parsed by a strict, fuzzable pure function; no `unsafe` decode paths.
- Local REST API: binds 127.0.0.1 only, random token auth, CORS deny — browser extension cannot be used by a random website without the token.
- Privacy: telemetry = none. Logs stay local. No phone-home (update check is a plain GET the user can disable).
- State files validated on load; corrupt meta → download restarts safely, never writes outside dest dir.

## 5. Release engineering
- CI: GitHub Actions — Ubuntu (test + release + tar.gz + .desktop), Windows (MSVC release + zip + Inno script), GUI headless smoke test with real screenshot artifact, feature-matrix check (`torrent`, `http3`).
- Installer: Inno Setup script (Windows) + portable zips; Linux tar.gz + .desktop.
- Licensing: MIT for the draft; commercial fork path documented.

## 6. Explicit non-goals (v1)
- FTP/SSH protocols (post-1.0 plugins), browser *native messaging* (REST capture is cross-browser and sandbox-safe), macOS signing.
