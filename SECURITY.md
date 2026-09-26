# Security Policy — Velox

## Supported versions

| Version | Supported |
|---------|-----------|
| 1.0.x   | ✅ |

## Security model

Velox is built to be safe under hostile input:

1. **Memory safety** — all product code is safe Rust. No `unsafe` blocks in
   `velox-core`, `velox-cli` or `velox-gui`. TLS is rustls (no C FFI).
2. **URL hardening** — only `http`/`https` schemes are accepted; URLs with
   control characters are rejected; credentials are redacted from all logs and
   error paths.
3. **Redirect hardening** — maximum 8 hops; the scheme is re-validated on every
   hop (blocks `http → file`, `https → gopher`, …); basic-auth credentials are
   applied only on the initial request, never forwarded across redirect hops.
4. **Header parsing** — `Content-Disposition` and friends are parsed by strict,
   pure functions; filenames are sanitized (path separators, control chars,
   Windows reserved device names, dot-dot collapse).
5. **Local API** — the REST daemon binds `127.0.0.1` only and requires a random
   32-char token stored with `0600` permissions. No CORS is enabled.
6. **Data files** — sidecar metadata is written atomically (tmp + fsync +
   rename). Corrupt metadata never causes writes outside the destination
   directory; affected downloads restart cleanly.
7. **Privacy** — no telemetry, no accounts, no update phone-home in v1.

## Reporting a vulnerability

Open a GitHub security advisory (Security → Report a vulnerability) or contact
the repository owner. Please include a reproduction and affected commit. We
aim to acknowledge within 72 hours.
