use percent_encoding::percent_decode_str;

/// Extract a filename from a Content-Disposition header (RFC 6266 / RFC 5987).
/// Strict: never returns path separators; falls back to None when unusable.
pub fn from_content_disposition(cd: &str) -> Option<String> {
    let mut best: Option<String> = None;
    for part in cd.split(';') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("filename*=") {
            // RFC 5987: charset'lang'value
            if let Some(decoded) = decode_ext_value(v) {
                best = Some(decoded);
                break; // filename* wins
            }
        } else if let Some(v) = part.strip_prefix("filename=") {
            let v = v.trim();
            let unquoted = v
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .unwrap_or(v);
            if !unquoted.is_empty() && best.is_none() {
                best = Some(unquoted.to_string());
            }
        }
    }
    best.map(|s| sanitize_filename(&s)).filter(|s| !s.is_empty())
}

fn decode_ext_value(v: &str) -> Option<String> {
    let mut it = v.splitn(3, '\'');
    let charset = it.next()?;
    let _lang = it.next()?;
    let value = it.next()?;
    let decoded = percent_decode_str(value).decode_utf8_lossy().to_string();
    match charset.to_ascii_lowercase().as_str() {
        "utf-8" | "utf8" | "iso-8859-1" => Some(decoded),
        _ => Some(decoded), // be liberal: we decoded as UTF-8 anyway
    }
}

/// Filename from URL path (percent-decoded last segment).
pub fn from_url_path(url: &url::Url) -> Option<String> {
    let seg = url
        .path_segments()?
        .filter(|s| !s.is_empty())
        .last()?
        .to_string();
    if seg.ends_with('/') || seg.is_empty() {
        return None;
    }
    let decoded = percent_decode_str(&seg).decode_utf8_lossy().to_string();
    let cleaned = sanitize_filename(&decoded);
    if cleaned.is_empty() || cleaned == "/" {
        None
    } else {
        Some(cleaned)
    }
}

/// Remove path separators, control chars, and platform-illegal characters.
pub fn sanitize_filename(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .filter(|c| !c.is_control() && *c != '\0')
        .map(|c| match c {
            '/' | '\\' | '<' | '>' | ':' | '"' | '|' | '?' | '*' => '_',
            _ => c,
        })
        .collect();
    let mut t = cleaned.trim().trim_end_matches('.').trim_start_matches('.').to_string();
    while t.contains("..") {
        t = t.replace("..", ".");
    }
    let trimmed = t;
    // Windows reserved device names
    const RESERVED: [&str; 22] = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    let upper = trimmed.split('.').next().unwrap_or("").to_ascii_uppercase();
    if RESERVED.contains(&upper.as_str()) {
        return format!("_{trimmed}");
    }
    if trimmed.chars().count() > 200 {
        trimmed.chars().take(200).collect()
    } else {
        trimmed
    }
}

/// Guess extension from MIME type (small, practical map).
pub fn ext_for_mime(mime: &str) -> Option<&'static str> {
    let base = mime.split(';').next()?.trim().to_ascii_lowercase();
    Some(match base.as_str() {
        "application/zip" => "zip",
        "application/x-zip-compressed" => "zip",
        "application/gzip" => "gz",
        "application/x-gzip" => "gz",
        "application/x-7z-compressed" => "7z",
        "application/x-rar-compressed" | "application/vnd.rar" => "rar",
        "application/x-tar" => "tar",
        "application/x-bzip2" => "bz2",
        "application/pdf" => "pdf",
        "application/json" => "json",
        "application/xml" | "text/xml" => "xml",
        "application/octet-stream" => "bin",
        "application/iso-image" | "application/x-iso9660-image" => "iso",
        "application/vnd.android.package-archive" => "apk",
        "application/x-debian-package" => "deb",
        "application/x-rpm" => "rpm",
        "application/msword" => "doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => "docx",
        "application/vnd.microsoft.portable-executable" => "exe",
        "application/x-msdownload" => "exe",
        "application/x-msi" => "msi",
        "video/mp4" => "mp4",
        "video/x-matroska" => "mkv",
        "video/webm" => "webm",
        "audio/mpeg" => "mp3",
        "audio/ogg" => "ogg",
        "audio/flac" => "flac",
        "audio/wav" | "audio/x-wav" => "wav",
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "image/svg+xml" => "svg",
        "text/plain" => "txt",
        "text/html" => "html",
        "text/csv" => "csv",
        _ => return None,
    })
}

/// Determine a filename from (priority) user choice > Content-Disposition > URL > MIME > default.
pub fn resolve_filename(
    user: Option<&str>,
    content_disposition: Option<&str>,
    url: &url::Url,
    content_type: Option<&str>,
) -> String {
    if let Some(u) = user {
        let s = sanitize_filename(u);
        if !s.is_empty() {
            return s;
        }
    }
    if let Some(cd) = content_disposition.and_then(from_content_disposition) {
        return cd;
    }
    if let Some(p) = from_url_path(url) {
        if p.contains('.') {
            return p;
        }
        // no extension: maybe add from mime
        if let Some(ct) = content_type {
            if let Some(ext) = ext_for_mime(ct) {
                return format!("{p}.{ext}");
            }
        }
        return p;
    }
    let host = url.host_str().unwrap_or("download");
    let ext = content_type.and_then(ext_for_mime).unwrap_or("bin");
    format!("{}-download.{ext}", sanitize_filename(host).to_ascii_lowercase())
}

/// If `candidate` exists in `dir`, produce "name (1).ext", "name (2).ext", ...
pub fn unique_path(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    let first = dir.join(name);
    if !first.exists() {
        return first;
    }
    let stem;
    let ext;
    match name.rfind('.') {
        Some(idx) if idx > 0 => {
            stem = &name[..idx];
            ext = &name[idx..];
        }
        _ => {
            stem = name;
            ext = "";
        }
    }
    for n in 1..1000u32 {
        let candidate = dir.join(format!("{stem} ({n}){ext}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    dir.join(format!(
        "{stem}-{}.tmp{ext}",
        chrono::Utc::now().timestamp_millis()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    #[test]
    fn parses_simple_cd() {
        assert_eq!(
            from_content_disposition("attachment; filename=\"report.pdf\""),
            Some("report.pdf".into())
        );
    }

    #[test]
    fn parses_rfc5987() {
        assert_eq!(
            from_content_disposition("attachment; filename*=UTF-8''Na%C3%AFve%20file.txt"),
            Some("Naïve file.txt".into())
        );
    }

    #[test]
    fn strips_path_tricks() {
        let r = from_content_disposition("attachment; filename=\"../../etc/passwd\"").unwrap();
        assert!(!r.contains('/'), "must not contain separators: {r}");
        assert!(!r.contains(".."), "must not contain dot-dot: {r}");
        assert_eq!(sanitize_filename("a/b\\c:d?e"), "a_b_c_d_e");
    }

    #[test]
    fn windows_reserved() {
        assert_eq!(sanitize_filename("CON"), "_CON");
        assert_eq!(sanitize_filename("com1.txt"), "_com1.txt");
    }

    #[test]
    fn url_filename() {
        let u = Url::parse("https://cdn.example.com/files/My%20Setup.exe").unwrap();
        assert_eq!(from_url_path(&u), Some("My Setup.exe".into()));
        let u2 = Url::parse("https://cdn.example.com/").unwrap();
        assert_eq!(from_url_path(&u2), None);
    }

    #[test]
    fn unique_paths() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = unique_path(dir.path(), "a.zip");
        std::fs::write(&p1, b"x").unwrap();
        let p2 = unique_path(dir.path(), "a.zip");
        assert_eq!(p2.file_name().unwrap().to_str().unwrap(), "a (1).zip");
    }
}
