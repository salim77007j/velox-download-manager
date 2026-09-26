//! BitTorrent source (feature = "torrent"), integrated as a hybrid race partner:
//! when `options.hybrid_torrent` is set and a magnet/torrent URL is provided as
//! a mirror, Velox starts BOTH the HTTP download and the torrent swarm; the
//! first to complete wins and the other is cancelled. This is real acceleration
//! only when both sources carry identical content (same total size is verified).

use crate::error::{Result, VeloxError};
use librqbit::{AddTorrent, AddTorrentOptions, Session, SessionOptions};
use std::path::PathBuf;
use std::sync::Arc;

pub struct TorrentJob {
    pub update_rx: tokio::sync::mpsc::UnboundedReceiver<TorrentUpdate>,
    _session: Arc<Session>,
}

pub enum TorrentUpdate {
    Progress { done: u64, total: u64 },
    Completed { total: u64 },
    Failed(String),
}

/// Start a torrent (magnet: or http(s) URL to a .torrent file) into `dest`.
pub async fn start(uri: &str, dest: PathBuf) -> Result<TorrentJob> {
    let opts = SessionOptions {
        listen_port_range: Some(42000..42100),
        enable_upnp_port_forwarding: false,
        ..Default::default()
    };
    let session = Session::new_with_opts(dest, opts)
        .await
        .map_err(|e| VeloxError::Other(format!("torrent session: {e}")))?;

    let add_opts = AddTorrentOptions {
        overwrite: true,
        ..Default::default()
    };
    let added = session
        .add_torrent(AddTorrent::from_url(uri), Some(add_opts))
        .await
        .map_err(|e| VeloxError::Other(format!("add torrent: {e}")))?;

    let handle = added.into_handle().ok_or_else(|| {
        VeloxError::Other("torrent metadata only — cannot download".into())
    })?;
    let (tx, update_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let stats = handle.stats();
            let total = stats.total_bytes;
            if stats.finished && total > 0 {
                let _ = tx.send(TorrentUpdate::Completed { total });
                break;
            }
            if let Some(err) = &stats.error {
                let _ = tx.send(TorrentUpdate::Failed(err.clone()));
                break;
            }
            if total > 0 {
                if tx.send(TorrentUpdate::Progress { done: stats.progress_bytes, total }).is_err() {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        }
    });

    Ok(TorrentJob {
        update_rx,
        _session: session,
    })
}
