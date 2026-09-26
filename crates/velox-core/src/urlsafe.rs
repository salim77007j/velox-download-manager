use crate::error::{Result, VeloxError};
use url::Url;

/// Strict URL hardening.
///
/// - Scheme allowlist: http/https only (file://, ftp://, data:, javascript:, etc. are rejected).
/// - Rejects URLs containing control characters or CR/LF (request splitting).
/// - Keeps the URL parseable by reqwest and returns a canonical form.
pub fn validate_and_normalize(raw: &str) -> Result<Url> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(VeloxError::InvalidUrl("empty URL".into()));
    }
    if raw.chars().any(|c| c.is_control()) {
        return Err(VeloxError::InvalidUrl("URL contains control characters".into()));
    }
    let parsed = Url::parse(raw).map_err(|e| VeloxError::InvalidUrl(format!("parse: {e}")))?;
    let scheme = parsed.scheme().to_ascii_lowercase();
    match scheme.as_str() {
        "http" | "https" => {}
        other => return Err(VeloxError::UnsupportedScheme(other.into())),
    }
    if parsed.host_str().unwrap_or("").is_empty() {
        return Err(VeloxError::InvalidUrl("missing host".into()));
    }
    Ok(parsed)
}

/// Redact any userinfo for display/logging.
pub fn sanitize_for_display(url: &Url) -> String {
    crate::error::redact(url.as_str())
}

/// Same origin (scheme+host+port) comparison used to guard credentials across redirects.
pub fn same_origin(a: &Url, b: &Url) -> bool {
    fn port(u: &Url) -> u16 {
        u.port_or_known_default().unwrap_or(0)
    }
    a.scheme() == b.scheme()
        && a.host_str().map(|h| h.to_ascii_lowercase()) == b.host_str().map(|h| h.to_ascii_lowercase())
        && port(a) == port(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_bad_schemes() {
        for bad in [
            "file:///etc/passwd",
            "ftp://example.com/x",
            "data:text/html;base64,AAA=",
            "javascript:alert(1)",
            "gopher://x",
        ] {
            assert!(validate_and_normalize(bad).is_err(), "should reject {bad}");
        }
    }

    #[test]
    fn rejects_control_chars() {
        assert!(validate_and_normalize("http://a.b/\r\nX-Injected: 1").is_err());
        assert!(validate_and_normalize("http://a.b/\x00").is_err());
    }

    #[test]
    fn accepts_and_normalizes() {
        let u = validate_and_normalize("HTTP://Example.COM/a/../b").unwrap();
        assert_eq!(u.scheme(), "http");
        assert_eq!(u.host_str(), Some("example.com"));
        assert!(validate_and_normalize("https://example.com/f%20ile.zip").is_ok());
    }

    #[test]
    fn origin_compare() {
        let a = Url::parse("https://cdn.example.com:443/x").unwrap();
        let b = Url::parse("https://cdn.example.com/y").unwrap();
        let c = Url::parse("http://cdn.example.com/y").unwrap();
        assert!(same_origin(&a, &b));
        assert!(!same_origin(&a, &c));
    }
}
