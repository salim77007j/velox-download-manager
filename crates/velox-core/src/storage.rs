//! Crash-safe storage: sparse preallocation, positioned writes, atomic sidecars.

use crate::error::{Result, VeloxError};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// The final data file is created as `<name>.veloxpart` and renamed on completion.
pub const PART_SUFFIX: &str = ".veloxpart";
pub const META_SUFFIX: &str = ".velox.meta";

/// A file opened for positioned (offset) writes. Shared between segment workers.
/// Unix: lock-free `write_all_at` (positional pwrite semantics).
/// Windows: mutex + seek (portable; positioned writes are disk-bound anyway).
pub struct SparseFile {
    #[cfg(unix)]
    file: File,
    #[cfg(windows)]
    file: parking_lot::Mutex<File>,
    pub path: PathBuf,
}

impl SparseFile {
    /// Create (or reopen) the part file, preallocating `size` bytes sparsely.
    /// Preallocation via `set_len` produces sparse files on NTFS/ext4/xfs —
    /// no data is written, so this is O(1) disk work.
    pub fn create(path: PathBuf, size: Option<u64>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?;
        if let Some(sz) = size {
            let cur = file.metadata()?.len();
            if cur < sz {
                file.set_len(sz)?;
            }
        }
        #[cfg(windows)]
        let file = parking_lot::Mutex::new(file);
        Ok(Self { file, path })
    }

    pub fn open(path: PathBuf) -> Result<Self> {
        Self::create(path, None)
    }

    /// Write buf at absolute offset. Safe for concurrent use at distinct offsets.
    pub fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        #[cfg(unix)]
        {
            write_all_at_impl(&self.file, offset, buf)
        }
        #[cfg(windows)]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = self.file.lock();
            f.seek(SeekFrom::Start(offset))?;
            f.write_all(buf)?;
            Ok(buf.len())
        }
    }

    pub fn len(&self) -> Result<u64> {
        #[cfg(unix)]
        {
            Ok(self.file.metadata()?.len())
        }
        #[cfg(windows)]
        {
            Ok(self.file.lock().metadata()?.len())
        }
    }

    /// Flush OS buffers to disk (fsync). Called on finalize and optional checkpoints.
    pub fn sync(&self) -> Result<()> {
        #[cfg(unix)]
        {
            self.file.sync_all()?;
        }
        #[cfg(windows)]
        {
            self.file.lock().sync_all()?;
        }
        Ok(())
    }

    /// Finalize: truncate to exact logical size if larger (sparse tail), fsync, rename to final path.
    pub fn finalize(self, final_path: &Path, logical_size: Option<u64>, fsync: bool) -> Result<()> {
        #[cfg(unix)]
        let file = self.file;
        #[cfg(windows)]
        let file = self.file.into_inner();
        if let Some(sz) = logical_size {
            let cur = file.metadata()?.len();
            if cur > sz {
                file.set_len(sz)?;
            }
        }
        if fsync {
            file.sync_all()?;
        }
        drop(file);
        if final_path.exists() {
            // race: someone created it while we downloaded; use unique name
            let alt = crate::filename::unique_path(
                final_path.parent().unwrap_or_else(|| Path::new(".")),
                final_path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("download"),
            );
            std::fs::rename(&self.path, &alt)?;
        } else {
            std::fs::rename(&self.path, final_path)?;
        }
        Ok(())
    }
}

#[cfg(unix)]
fn write_all_at_impl(file: &File, mut offset: u64, mut buf: &[u8]) -> Result<usize> {
    use std::os::unix::fs::FileExt;
    let total = buf.len();
    while !buf.is_empty() {
        match file.write_at(buf, offset) {
            Ok(0) => return Err(VeloxError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "write_at returned 0",
            ))),
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(total)
}

#[cfg(not(any(unix, windows)))]
compile_error!("Velox supports unix and windows targets");

/// Sidecar metadata file stored next to the part file: `<name>.veloxpart.velox.meta`
pub struct Sidecar;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidecarData {
    pub state: crate::types::DownloadState,
}

impl Sidecar {
    pub fn path_for(part_path: &Path) -> PathBuf {
        let mut s = part_path.as_os_str().to_os_string();
        s.push(META_SUFFIX);
        PathBuf::from(s)
    }

    /// Atomic save: write tmp -> fsync -> rename -> fsync dir (best effort on Windows).
    pub fn save(part_path: &Path, state: &crate::types::DownloadState) -> Result<()> {
        let meta_path = Self::path_for(part_path);
        let tmp = meta_path.with_extension("meta.tmp");
        let data = SidecarData {
            state: state.clone(),
        };
        let json = serde_json::to_vec_pretty(&data)?;
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&json)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &meta_path)?;
        sync_dir(&meta_path);
        Ok(())
    }

    pub fn load(part_path: &Path) -> Result<Option<crate::types::DownloadState>> {
        let meta_path = Self::path_for(part_path);
        match std::fs::read(&meta_path) {
            Ok(bytes) => {
                let data: SidecarData = match serde_json::from_slice(&bytes) {
                    Ok(d) => d,
                    Err(e) => {
                        tracing::warn!("corrupt sidecar {}: {e}", meta_path.display());
                        return Ok(None);
                    }
                };
                Ok(Some(data.state))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn delete(part_path: &Path) {
        let _ = std::fs::remove_file(Self::path_for(part_path));
    }
}

fn sync_dir(path: &Path) {
    #[cfg(unix)]
    {
        if let Some(dir) = path.parent() {
            if let Ok(d) = File::open(dir) {
                let _ = d.sync_all();
            }
        }
    }
}

/// Streamed SHA-256 of a file (used for verification of completed downloads).
pub fn sha256_file(path: &Path, cancelled: Option<&tokio_util::sync::CancellationToken>) -> Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        if let Some(c) = cancelled {
            if c.is_cancelled() {
                return Err(VeloxError::Cancelled);
            }
        }
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Exclusive engine lock for a state directory.
pub struct EngineLock {
    _file: File,
}

impl EngineLock {
    pub fn acquire(dir: &Path) -> Result<EngineLock> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("velox.lock");
        let file = OpenOptions::new().create(true).write(true).truncate(false).open(&path)?;
        match fs2::FileExt::try_lock_exclusive(&file) {
            Ok(()) => Ok(EngineLock { _file: file }),
            Err(_) => Err(VeloxError::AlreadyRunning(dir.display().to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positioned_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part.bin");
        let f = SparseFile::create(path.clone(), Some(1024)).unwrap();
        f.write_at(500, b"hello").unwrap();
        f.write_at(0, &[0xAA; 8]).unwrap();
        f.sync().unwrap();
        let final_path = dir.path().join("final.bin");
        f.finalize(&final_path, Some(512), true).unwrap();
        let data = std::fs::read(&final_path).unwrap();
        assert_eq!(data.len(), 512);
        assert_eq!(&data[0..8], &[0xAA; 8]);
        assert_eq!(&data[500..505], b"hello");
        // all other bytes zero (sparse)
        assert!(data[8..500].iter().all(|&b| b == 0));
        assert!(!path.exists());
    }

    #[test]
    fn sidecar_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("f.veloxpart");
        let mut state = crate::types::DownloadState::default();
        state.filename = "f".into();
        state.segments = vec![crate::types::SegmentState {
            start: 0,
            end: 99,
            written: 42,
            mirror: 0,
        }];
        Sidecar::save(&part, &state).unwrap();
        let loaded = Sidecar::load(&part).unwrap().unwrap();
        assert_eq!(loaded.segments[0].written, 42);
        Sidecar::delete(&part);
        assert!(Sidecar::load(&part).unwrap().is_none());
    }

    #[test]
    fn sha256_matches_known() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("x.txt");
        std::fs::write(&p, b"hello world").unwrap();
        let h = sha256_file(&p, None).unwrap();
        assert_eq!(
            h,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }
}
