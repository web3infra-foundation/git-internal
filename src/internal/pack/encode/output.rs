//! Top-level convenience: encode entries directly to a `.pack` / `.idx` file pair.
//!
//! The pack is first written to a temporary file because its final name contains the checksum,
//! which is not known until encoding completes. A background task drains the encoder's pack
//! channel while the caller-facing task performs encoding. After the pack is finalized and
//! renamed, a second writer drains the generated index bytes.

use std::path::PathBuf;

use chrono::Utc;
use tokio::io::AsyncWriteExt as TokioAsyncWriteExt;
use tokio::{fs::File, sync::mpsc};

use crate::{
    errors::GitError,
    internal::{
        metadata::{EntryMeta, MetaAttached},
        pack::entry::Entry,
    },
};

use super::PackEncoder;

/// Consume entries and write a matching `.pack`/`.idx` pair into `output_dir`.
///
/// The pack is first written to a temporary file because its final name contains the checksum,
/// which is not known until encoding completes. A background task drains the encoder's pack
/// channel while the caller-facing task performs encoding. After the pack is finalized and
/// renamed, a second writer drains the generated index bytes.
///
/// `object_number` must equal the number of entries eventually received. A `window_size` of zero
/// disables delta compression; any non-zero value selects the delta-search path. The default build
/// uses Rabin fingerprinting; builds without `diff_rabin` use Myers or Patience.
pub async fn encode_and_output_to_files(
    raw_entries_rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    object_number: usize,
    output_dir: PathBuf,
    window_size: usize,
) -> Result<(), GitError> {
    let (pack_tx, mut pack_rx) = mpsc::channel(1024);
    let (idx_tx, mut idx_rx) = mpsc::channel(1024);
    let mut pack_encoder = PackEncoder::new_with_idx(object_number, window_size, pack_tx, idx_tx);

    // The checksum-based final filename is unknown until the complete pack has been hashed.
    let now = Utc::now();
    let timestamp = now.format("%Y%m%d%H%M%S%.3f").to_string();
    let tmp_path = output_dir.join(format!("{}objects.pack.tmp", timestamp));
    let mut pack_file = File::create(&tmp_path).await?;

    // Drain pack chunks concurrently so a full channel does not stall the encoder behind file I/O.
    let pack_writer = tokio::spawn(async move {
        while let Some(chunk) = pack_rx.recv().await {
            TokioAsyncWriteExt::write_all(&mut pack_file, &chunk).await?;
        }
        TokioAsyncWriteExt::flush(&mut pack_file).await?;
        Ok::<(), GitError>(())
    });

    pack_encoder.encode(raw_entries_rx).await?;

    // Closing PackEncoder's sender ends the writer loop; wait before renaming the file.
    let pack_write_result = pack_writer
        .await
        .map_err(|e| GitError::PackEncodeError(format!("pack writer task join error: {e}")))?;
    pack_write_result?;

    let final_pack_name =
        output_dir.join(format!("pack-{}.pack", pack_encoder.final_hash.unwrap()));
    let final_idx_name = output_dir.join(format!("pack-{}.idx", pack_encoder.final_hash.unwrap()));
    tokio::fs::rename(tmp_path, &final_pack_name).await?;

    let mut idx_file = File::create(&final_idx_name).await?;
    let idx_writer = tokio::spawn(async move {
        while let Some(chunk) = idx_rx.recv().await {
            TokioAsyncWriteExt::write_all(&mut idx_file, &chunk).await?;
        }
        TokioAsyncWriteExt::flush(&mut idx_file).await?;
        Ok::<(), GitError>(())
    });

    // Index generation is deferred until pack offsets and the final pack checksum are known.
    pack_encoder.encode_idx_file().await?;

    let idx_write_result = idx_writer
        .await
        .map_err(|e| GitError::PackEncodeError(format!("idx writer task join error: {e}")))?;
    idx_write_result?;

    Ok(())
}
