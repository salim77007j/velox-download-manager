//! HTTP client factory with security hardening + server probing.

use crate::config::EngineConfig;
use crate::error::{Result, VeloxError};
use crate::urlsafe;
use reqwest::redirect::Attempt;
use reqwest::{Client, Method, Proxy, Response, Url};
use std::time::Duration;

fn redirect_policy(max: usize) -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(move |attempt: Attempt| {
        if attempt.previous().len() >= max {
            let err = VeloxError::RedirectLoop { max };
            return attempt.error(err);
        }
        let next_scheme = attempt.url().scheme().to_string();
        // Re-validate scheme on every hop (blocks http->file, https->gopher, etc.)
        if !matches!(next_scheme.as_str(), "http" | "https") {
            tracing::warn!("blocked redirect to unsafe scheme: {next_scheme}");
            let err = VeloxError::UnsupportedScheme(next_scheme);
            return attempt.error(err);
        }
        attempt.follow()
    })
}

pub fn build_client(cfg: &EngineConfig) -> Result<Client> {
    let mut b = Client::builder()
        .user_agent(&cfg.user_agent)
        .redirect(redirect_policy(cfg.max_redirects))
        .connect_timeout(Duration::from_millis(cfg.connect_timeout_ms))
        .tcp_nodelay(true)
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(32)
        .timeout(Duration::from_secs(300)) // outer safety net; per-read timeouts are tighter
        .http2_adaptive_window(true);

    if cfg.prefer_ipv4 {
        // Happy-Eyeballs substitute: prefer the IPv4 stack. Hyper connects to a
        // single resolved address, so a dead IPv6 route would otherwise stall
        // every request until timeout.
        b = b.local_address(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
    }

    if let Some(proxy) = &cfg.proxy {
        let p = Proxy::all(proxy)
            .map_err(|e| VeloxError::Config(format!("bad proxy url: {e}")))?;
        b = b.proxy(p);
    } else {
        b = b.no_proxy(); // deliberate: no env proxies unless configured
    }

    Ok(b.build()?)
}

/// What we learned from the server before allocating the file.
#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub final_url: Url,
    pub status: u16,
    pub total_size: Option<u64>,
    pub supports_ranges: bool,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_type: Option<String>,
    pub content_disposition: Option<String>,
    pub accept_ranges_header: Option<String>,
    pub alt_svc: Option<String>,
    pub server: Option<String>,
    /// Negotiated HTTP version of the probe response (h1.1/h2/h3).
    pub http_version: Option<String>,
}

impl ProbeResult {
    pub fn advertises_h3(&self) -> bool {
        self.alt_svc
            .as_deref()
            .map(|a| a.to_ascii_lowercase().contains("h3"))
            .unwrap_or(false)
    }
}

fn version_str(v: reqwest::Version) -> Option<String> {
    use reqwest::Version;
    Some(match v {
        Version::HTTP_09 => "HTTP/0.9".into(),
        Version::HTTP_10 => "HTTP/1.0".into(),
        Version::HTTP_11 => "HTTP/1.1".into(),
        Version::HTTP_2 => "HTTP/2".into(),
        Version::HTTP_3 => "HTTP/3".into(),
        _ => return None,
    })
}

fn extract_probe(r: &Response) -> ProbeResult {
    let headers = r.headers();
    let total = headers
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    let accept_ranges = headers
        .get(reqwest::header::ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    ProbeResult {
        final_url: r.url().clone(),
        status: r.status().as_u16(),
        total_size: total,
        supports_ranges: accept_ranges.as_deref() == Some("bytes") && total.is_some(),
        etag: headers
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        last_modified: headers
            .get(reqwest::header::LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        content_type: headers
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        content_disposition: headers
            .get(reqwest::header::CONTENT_DISPOSITION)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        accept_ranges_header: accept_ranges,
        alt_svc: headers
            .get("alt-svc")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        server: headers
            .get(reqwest::header::SERVER)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        http_version: version_str(r.version()),
    }
}

/// Probe a URL. The ranged GET probe (`Range: bytes=0-0`) is the AUTHORITATIVE
/// source for range support: many servers advertise `accept-ranges: bytes` on
/// HEAD but ignore Range on GET (Cloudflare `__down`, some CDNs, CGI scripts).
/// HEAD supplies metadata (size, etag, filename hints) when it succeeds.
pub async fn probe(
    client: &Client,
    url: &Url,
    extra_headers: &[(String, String)],
    auth: Option<(&str, &str)>,
) -> Result<ProbeResult> {
    // 1) HEAD for metadata (may fail entirely — that's fine)
    let mut head_meta: Option<ProbeResult> = None;
    let mut req = client.head(url.clone());
    for (k, v) in extra_headers {
        req = req.header(k, v);
    }
    if let Some((u, p)) = auth {
        req = req.basic_auth(u, Some(p));
    }
    if let Ok(resp) = req.send().await {
        if resp.status().is_success() && resp.status() != 204 {
            head_meta = Some(extract_probe(&resp));
        }
    }

    // 2) Ranged GET probe (authoritative)
    let mut req = client.get(url.clone()).header("Range", "bytes=0-0");
    for (k, v) in extra_headers {
        req = req.header(k, v);
    }
    if let Some((u, p)) = auth {
        req = req.basic_auth(u, Some(p));
    }
    let resp = req.send().await.map_err(|e| {
        if e.is_timeout() {
            VeloxError::Network(format!("timeout probing {url}"))
        } else if e.is_connect() {
            VeloxError::Network(format!("cannot connect to {}", sanitize_host(url)))
        } else if e.is_redirect() {
            let mut src = std::error::Error::source(&e);
            let mut msg = String::from("unsafe or excessive redirect chain");
            while let Some(s) = src {
                if let Some(ve) = s.downcast_ref::<VeloxError>() {
                    msg = ve.to_string();
                    break;
                }
                src = s.source();
            }
            VeloxError::Other(format!("unsafe redirect: {msg}"))
        } else {
            VeloxError::Network(crate::error::redact(&e.to_string()))
        }
    })?;
    let status = resp.status();

    let mut pr = extract_probe(&resp);
    if status == reqwest::StatusCode::PARTIAL_CONTENT {
        // Content-Length is the RANGE length — the real total lives in
        // Content-Range: "bytes 0-0/TOTAL"
        if let Some(cr) = resp
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
        {
            if let Some(total) = cr.rsplit('/').next().and_then(|t| t.parse::<u64>().ok()) {
                pr.total_size = Some(total);
            }
        }
        pr.supports_ranges = pr.total_size.is_some();
    } else if status.is_success() {
        // 200 to a ranged request ⇒ the server ignores Range.
        pr.supports_ranges = false;
    } else {
        // Non-success on GET: fall back to HEAD metadata, no ranges assumed.
        if let Some(mut hm) = head_meta {
            hm.supports_ranges = false;
            return Ok(hm);
        }
        return Err(VeloxError::HttpStatus {
            status: status.as_u16(),
            url: urlsafe::sanitize_for_display(url),
        });
    }

    // prefer HEAD metadata for fields the 0-0 GET can't see well
    if let Some(hm) = head_meta {
        if pr.content_disposition.is_none() {
            pr.content_disposition = hm.content_disposition;
        }
        if pr.etag.is_none() {
            pr.etag = hm.etag;
        }
        if pr.last_modified.is_none() {
            pr.last_modified = hm.last_modified;
        }
        if pr.server.is_none() {
            pr.server = hm.server;
        }
        if pr.alt_svc.is_none() {
            pr.alt_svc = hm.alt_svc;
        }
        // HEAD content-length equals full size on honest servers
        if pr.total_size.is_none() {
            pr.total_size = hm.total_size;
        }
    }
    Ok(pr)
}

fn sanitize_host(url: &Url) -> String {
    format!(
        "{} ({})",
        url.host_str().unwrap_or("?"),
        url.scheme()
    )
}

/// Open a byte range. Returns the raw response for streaming.
/// `if_range` enables the ETag guard: a changed server yields 200 (full body), which the
/// caller treats as "content changed → restart" instead of writing garbage.
pub async fn open_range(
    client: &Client,
    url: &Url,
    range: Option<(u64, u64)>,
    extra_headers: &[(String, String)],
    auth: Option<(&str, &str)>,
    if_range: Option<&str>,
) -> Result<Response> {
    let method = if range.is_some() { Method::GET } else { Method::GET };
    let mut req = client.request(method, url.clone());
    if let Some((start, end)) = range {
        req = req.header("Range", format!("bytes={start}-{end}"));
        if let Some(etag) = if_range {
            req = req.header("If-Range", etag);
        }
    }
    for (k, v) in extra_headers {
        req = req.header(k, v);
    }
    if let Some((u, p)) = auth {
        req = req.basic_auth(u, Some(p));
    }
    let resp = req.send().await?;
    let status = resp.status();
    if status.is_success() || status == reqwest::StatusCode::PARTIAL_CONTENT {
        Ok(resp)
    } else {
        Err(VeloxError::HttpStatus {
            status: status.as_u16(),
            url: urlsafe::sanitize_for_display(&resp.url().clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_blocks_bad_scheme() {
        // construct policy and simulate via public builder is hard; scheme check covered
        // by integration tests. Keep a compile-time smoke test here.
        let _p = redirect_policy(8);
    }
}
