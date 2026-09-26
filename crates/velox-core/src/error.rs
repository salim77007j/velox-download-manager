use thiserror::Error;

pub type Result<T> = std::result::Result<T, VeloxError>;

#[derive(Debug, Error)]
pub enum VeloxError {
    #[error("invalid URL: {0}")]
    InvalidUrl(String),

    #[error("unsupported URL scheme '{0}' (allowed: http, https)")]
    UnsupportedScheme(String),

    #[error("network error: {0}")]
    Network(String),

    #[error("HTTP status {status} from {url}")]
    HttpStatus { status: u16, url: String },

    #[error("server does not support range requests (needed for parallel download)")]
    NoRangeSupport,

    #[error("download interrupted at offset {offset}: {source}")]
    Interrupted {
        offset: u64,
        #[source]
        source: Box<VeloxError>,
    },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("integrity check failed: expected sha256 {expected}, got {actual}")]
    IntegrityMismatch { expected: String, actual: String },

    #[error("download not found: {0}")]
    NotFound(String),

    #[error("invalid state transition: {0}")]
    InvalidState(String),

    #[error("engine is shutting down")]
    Shutdown,

    #[error("cancelled")]
    Cancelled,

    #[error("paused")]
    Paused,

    #[error("configuration error: {0}")]
    Config(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("too many redirects ({max}) or unsafe redirect chain")]
    RedirectLoop { max: usize },

    #[error("lock error: another Velox instance is using this state directory ({0})")]
    AlreadyRunning(String),

    #[error("{0}")]
    Other(String),
}

impl From<serde_json::Error> for VeloxError {
    fn from(e: serde_json::Error) -> Self {
        VeloxError::Serialization(e.to_string())
    }
}

impl VeloxError {
    /// True for transient errors worth retrying with backoff.
    pub fn retryable(&self) -> bool {
        matches!(self, VeloxError::Network(_) | VeloxError::Io(_))
    }

    pub fn redact(s: &str) -> String {
        redact(s)
    }
}

impl From<reqwest::Error> for VeloxError {
    fn from(e: reqwest::Error) -> Self {
        VeloxError::Network(redact(e.to_string().as_str()))
    }
}

/// Redact credentials that might leak into error strings (e.g. `https://user:pass@host/`).
pub fn redact(s: &str) -> String {
    if let Some(scheme_end) = s.find("://") {
        let prefix = &s[..scheme_end + 3];
        let rest = &s[scheme_end + 3..];
        if let Some(at) = rest.find('@') {
            // only redact if there's a colon (user:pass) before any slash
            let head = &rest[..at];
            if head.contains(':') && !head.contains('/') {
                return format!("{}[redacted]@{}", prefix, &rest[at + 1..]);
            }
        }
    }
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_url_credentials() {
        let s = "error for https://user:secret@cdn.example.com/file (status 403)";
        let r = redact(s);
        assert!(!r.contains("secret"));
        assert!(r.contains("[redacted]@cdn.example.com"));
    }

    #[test]
    fn leaves_normal_urls_alone() {
        let s = "error for https://cdn.example.com/file";
        assert_eq!(redact(s), s);
    }
}
