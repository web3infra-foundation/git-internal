//! Streaming encoder for Git packfiles and their companion index files.
//!
//! Entries are emitted through bounded channels so compression and file I/O can
//! proceed independently. A zero-sized delta window encodes objects in parallel
//! without delta compression; a non-zero window sorts objects into likely delta
//! families and searches recent objects for an offset-delta base.

use std::{
    cmp::Ordering,
    collections::VecDeque,
    hash::{Hash, Hasher},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
};

use ahash::AHasher;
use chrono::Utc;
use flate2::write::ZlibEncoder;
use rayon::prelude::*;
use tokio::io::AsyncWriteExt as TokioAsyncWriteExt;
use tokio::{fs::File, sync::mpsc, task::JoinHandle};

use crate::delta;
use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::{
        metadata::{EntryMeta, MetaAttached},
        object::types::ObjectType,
        pack::{entry::Entry, index_entry::IndexEntry, pack_index::IdxBuilder},
    },
    time_it,
    utils::HashAlgorithm,
    zstdelta,
};

const MAX_CHAIN_LEN: usize = 50;
const MIN_DELTA_RATE: f64 = 0.5;

/// Stateful encoder for one Git packfile.
///
/// The encoder hashes the pack header and object entries, records each object's
/// pack offset for the index, and appends the resulting checksum as the pack
/// trailer.
pub struct PackEncoder {
    object_number: usize,
    process_index: usize,
    window_size: usize,
    pack_sender: Option<mpsc::Sender<Vec<u8>>>,
    idx_sender: Option<mpsc::Sender<Vec<u8>>>,
    idx_entries: Option<Vec<IndexEntry>>,
    /// Offset at which the next encoded object will begin.
    inner_offset: usize,
    /// Incremental checksum over the pack header and encoded objects.
    inner_hash: HashAlgorithm,
    final_hash: Option<ObjectHash>,
    start_encoding: bool,
    /// Whether to score every eligible object in the delta window.
    ///
    /// When false, a cheap content-similarity check removes unlikely bases
    /// before the more expensive delta calculation.
    pub disable_prefilter: bool,
}

/// Encodes entries and writes a content-addressed `.pack`/`.idx` pair.
///
/// Pack bytes are first written to a temporary file. After the pack checksum is
/// known, the file is renamed to `pack-<checksum>.pack` and the matching index is
/// generated as `pack-<checksum>.idx`.
///
/// `object_number` must equal the number of entries received. A `window_size` of
/// zero disables delta compression and enables batch-parallel encoding. With
/// the default `diff_rabin` feature, delta windows use Rabin fingerprints;
/// builds without that feature fall back to Myers/Patience delta encoding.
pub async fn encode_and_output_to_files(
    raw_entries_rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    object_number: usize,
    output_dir: PathBuf,
    window_size: usize,
) -> Result<(), GitError> {
    let (pack_tx, mut pack_rx) = mpsc::channel(1024);
    let (idx_tx, mut idx_rx) = mpsc::channel(1024);
    let mut pack_encoder = PackEncoder::new_with_idx(object_number, window_size, pack_tx, idx_tx);

    // The checksum-based final name is unavailable until encoding completes.
    let now = Utc::now();
    let timestamp = now.format("%Y%m%d%H%M%S%.3f").to_string();
    let tmp_path = output_dir.join(format!("{}objects.pack.tmp", timestamp));
    let mut pack_file = File::create(&tmp_path).await?;

    let pack_writer = tokio::spawn(async move {
        while let Some(chunk) = pack_rx.recv().await {
            TokioAsyncWriteExt::write_all(&mut pack_file, &chunk).await?;
        }
        TokioAsyncWriteExt::flush(&mut pack_file).await?;
        Ok::<(), GitError>(())
    });

    #[cfg(feature = "diff_rabin")]
    pack_encoder.encode_with_rabin(raw_entries_rx).await?;

    #[cfg(not(feature = "diff_rabin"))]
    pack_encoder.encode(raw_entries_rx).await?;

    // Closing the sender lets the writer drain all queued pack chunks.
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

    // Index generation requires the final pack checksum and object offsets.
    pack_encoder.encode_idx_file().await?;

    let idx_write_result = idx_writer
        .await
        .map_err(|e| GitError::PackEncodeError(format!("idx writer task join error: {e}")))?;
    idx_write_result?;

    Ok(())
}

/// Encodes the 12-byte pack header.
///
/// The header consists of the `PACK` signature, version 2, and a big-endian
/// 32-bit object count.
fn encode_header(object_number: usize) -> Vec<u8> {
    let mut result: Vec<u8> = vec![b'P', b'A', b'C', b'K', 0, 0, 0, 2];
    assert_ne!(object_number, 0);
    assert!(object_number <= u32::MAX as usize);
    result.append((object_number as u32).to_be_bytes().to_vec().as_mut());
    result
}

/// Encodes an OFS_DELTA base distance using Git's variable-length format.
///
/// `value` is the positive distance from the delta object's offset back to its
/// base object's offset, not an absolute pack offset.
fn encode_offset(mut value: usize) -> Vec<u8> {
    assert_ne!(value, 0, "offset can't be zero");
    let mut bytes = Vec::new();

    bytes.push((value & 0x7F) as u8);
    value >>= 7;
    while value != 0 {
        value -= 1;
        let byte = (value & 0x7F) as u8 | 0x80;
        value >>= 7;
        bytes.push(byte);
    }
    bytes.reverse();
    bytes
}

/// Encodes one complete pack entry, excluding its absolute pack offset.
///
/// `offset` is required for OFS_DELTA entries and contains the backward
/// distance to the selected base. Object data, including delta instructions,
/// is zlib-compressed after the entry header and optional base reference.
fn encode_one_object(entry: &Entry, offset: Option<usize>) -> Result<Vec<u8>, GitError> {
    let obj_data = &entry.data;
    let obj_data_len = obj_data.len();
    let obj_type_number = entry.obj_type.to_pack_type_u8()?;

    let mut encoded_data = Vec::new();

    // The first byte stores four size bits and the three-bit pack object type.
    let mut header_data = vec![(0x80 | (obj_type_number << 4)) + (obj_data_len & 0x0f) as u8];
    let mut size = obj_data_len >> 4;
    if size > 0 {
        while size > 0 {
            if size >> 7 > 0 {
                header_data.push((0x80 | size) as u8);
                size >>= 7;
            } else {
                header_data.push(size as u8);
                break;
            }
        }
    } else {
        header_data.push(0);
    }
    encoded_data.extend(header_data);

    // OFS_DELTA places its backward base distance immediately after the header.
    if entry.obj_type == ObjectType::OffsetDelta || entry.obj_type == ObjectType::OffsetZstdelta {
        let offset_data = encode_offset(offset.unwrap());
        encoded_data.extend(offset_data);
    } else if entry.obj_type == ObjectType::HashDelta {
        unreachable!("unsupported type")
    }

    // Every pack entry payload is a separate zlib stream.
    let mut inflate = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    inflate
        .write_all(obj_data)
        .expect("zlib compress should never failed");
    inflate.flush().expect("zlib flush should never failed");
    let compressed_data = inflate.finish().expect("zlib compress should never failed");
    encoded_data.extend(compressed_data);
    Ok(encoded_data)
}

/// Orders entries so likely delta families remain close in the sliding window.
///
/// Entries with paths come first, grouped by parent directory and Git's
/// `pack_name_hash`; larger objects then precede smaller objects so they can
/// serve as bases. Pointer order provides a total order for the current sort
/// invocation when all semantic keys compare equal.
fn magic_sort(a: &MetaAttached<Entry, EntryMeta>, b: &MetaAttached<Entry, EntryMeta>) -> Ordering {
    let path_a = a.meta.file_path.as_ref();
    let path_b = b.meta.file_path.as_ref();

    // Path metadata provides the strongest signal that two blobs are related.
    match (path_a, path_b) {
        (Some(pa), Some(pb)) => {
            let pa = Path::new(pa);
            let pb = Path::new(pb);

            let dir_ord = pa.parent().cmp(&pb.parent());
            if dir_ord != Ordering::Equal {
                return dir_ord;
            }

            // Raw path bytes avoid allocating a temporary UTF-8 string.
            let hash_a = delta::pack_name_hash(pa.as_os_str().as_encoded_bytes());
            let hash_b = delta::pack_name_hash(pb.as_os_str().as_encoded_bytes());
            if hash_a != hash_b {
                return hash_b.cmp(&hash_a);
            }
        }
        (Some(_), None) => return Ordering::Less,
        (None, Some(_)) => return Ordering::Greater,
        (None, None) => {}
    }

    // Larger objects are generally more useful as delta bases.
    let ord = b.inner.data.len().cmp(&a.inner.data.len());
    if ord != Ordering::Equal {
        return ord;
    }

    // Preserve a total order when all content-derived keys match.
    (a as *const MetaAttached<Entry, EntryMeta>).cmp(&(b as *const MetaAttached<Entry, EntryMeta>))
}

/// Computes a fast, non-cryptographic fingerprint for similarity filtering.
fn calc_hash(data: &[u8]) -> u64 {
    let mut hasher = AHasher::default();
    data.hash(&mut hasher);
    hasher.finish()
}

/// Checks whether two buffers share an identical prefix or suffix sample.
///
/// Matching either 128-byte sample is enough to retain the candidate for Rabin
/// delta scoring. This catches related files whose generated or license headers
/// differ while their trailing content remains the same.
#[cfg(feature = "diff_rabin")]
fn multi_point_similar(a: &[u8], b: &[u8]) -> bool {
    let min_len = a.len().min(b.len());
    if min_len < 16 {
        return false;
    }

    let head_len = 128.min(min_len);
    if calc_hash(&a[..head_len]) == calc_hash(&b[..head_len]) {
        return true;
    }

    let tail_start = min_len.saturating_sub(128);
    if calc_hash(&a[tail_start..min_len]) == calc_hash(&b[tail_start..min_len]) {
        return true;
    }

    false
}

/// Retains non-Rabin candidates whose first 128 bytes have the same fingerprint.
#[cfg(not(feature = "diff_rabin"))]
fn cheap_similar(a: &[u8], b: &[u8]) -> bool {
    let k = a.len().min(b.len()).min(128);
    if k == 0 {
        return false;
    }
    calc_hash(&a[..k]) == calc_hash(&b[..k])
}

/// Candidate delta base retained in the sliding window.
///
/// With `diff_rabin`, the source index is built lazily and reused for every
/// later target that considers this entry as a base.
#[cfg(feature = "diff_rabin")]
struct DeltaWindowEntry {
    entry: Entry,
    offset: usize,
    /// Shared source storage used when constructing the cached Rabin index.
    data_arc: Option<Arc<[u8]>>,
    rabin_index: Option<delta::RabinDeltaIndex>,
}

/// Candidate delta base retained in the sliding window.
#[cfg(not(feature = "diff_rabin"))]
struct DeltaWindowEntry {
    entry: Entry,
    offset: usize,
}

impl DeltaWindowEntry {
    fn new(entry: Entry, offset: usize) -> Self {
        Self {
            entry,
            offset,
            #[cfg(feature = "diff_rabin")]
            data_arc: None,
            #[cfg(feature = "diff_rabin")]
            rabin_index: None,
        }
    }
}

impl PackEncoder {
    pub fn new(object_number: usize, window_size: usize, sender: mpsc::Sender<Vec<u8>>) -> Self {
        PackEncoder {
            object_number,
            window_size,
            process_index: 0,
            pack_sender: Some(sender),
            idx_sender: None,
            idx_entries: None,
            inner_offset: 12,
            inner_hash: HashAlgorithm::new(),
            final_hash: None,
            start_encoding: false,
            disable_prefilter: false,
        }
    }

    pub fn new_with_idx(
        object_number: usize,
        window_size: usize,
        pack_sender: mpsc::Sender<Vec<u8>>,
        idx_sender: mpsc::Sender<Vec<u8>>,
    ) -> Self {
        PackEncoder {
            object_number,
            window_size,
            process_index: 0,
            pack_sender: Some(pack_sender),
            idx_sender: Some(idx_sender),
            idx_entries: None,
            inner_offset: 12,
            inner_hash: HashAlgorithm::new(),
            final_hash: None,
            start_encoding: false,
            disable_prefilter: false,
        }
    }

    pub fn drop_sender(&mut self) {
        self.pack_sender.take();
    }

    pub async fn send_data(&mut self, data: Vec<u8>) {
        if let Some(sender) = &self.pack_sender {
            sender.send(data).await.unwrap();
        }
    }

    /// Returns the checksum appended to the completed packfile.
    ///
    /// The checksum is unavailable until all entries have been encoded.
    pub fn get_hash(&self) -> Option<ObjectHash> {
        self.final_hash
    }

    /// Encodes all received entries and streams the resulting pack bytes.
    ///
    /// A zero-sized window uses batch-parallel, non-delta encoding. Otherwise,
    /// entries are sorted by object type and path similarity before delta-base
    /// selection. With `diff_rabin`, Rabin deltas are used; without it, the
    /// standard delta implementation and optional similarity prefilter are used.
    pub async fn encode(
        &mut self,
        entry_rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    ) -> Result<(), GitError> {
        if self.window_size == 0 {
            self.parallel_encode(entry_rx).await
        } else {
            #[cfg(feature = "diff_rabin")]
            {
                self.inner_encode(entry_rx, false, true, true).await
            }
            #[cfg(not(feature = "diff_rabin"))]
            {
                self.inner_encode(entry_rx, false, false, self.disable_prefilter).await
            }
        }
    }

    /// Encodes entries using zstdelta payloads for selected offset deltas.
    pub async fn encode_with_zstdelta(
        &mut self,
        entry_rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    ) -> Result<(), GitError> {
        self.inner_encode(entry_rx, true, false, self.disable_prefilter).await
    }

    /// Encodes entries with Rabin deltas and no similarity prefilter.
    ///
    /// Requires the `diff_rabin` feature. A zero-sized window still selects the
    /// non-delta parallel path.
    #[cfg(feature = "diff_rabin")]
    pub async fn encode_with_rabin(
        &mut self,
        entry_rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    ) -> Result<(), GitError> {
        if self.window_size == 0 {
            self.parallel_encode(entry_rx).await
        } else {
            self.inner_encode(entry_rx, false, true, true).await
        }
    }

    /// Sorts entries, runs delta-base searches, and emits objects in pack order.
    ///
    /// The candidate ordering and size constraints follow Git's documented
    /// [pack heuristics](https://github.com/git/git/blob/master/Documentation/technical/pack-heuristics.adoc).
    async fn inner_encode(
        &mut self,
        mut entry_rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
        enable_zstdelta: bool,
        enable_rabin: bool,
        disable_prefilter: bool,
    ) -> Result<(), GitError> {
        let head = encode_header(self.object_number);
        self.send_data(head.clone()).await;
        self.inner_hash.update(&head);

        if self.start_encoding {
            return Err(GitError::PackEncodeError(
                "encoding operation is already in progress".to_string(),
            ));
        }

        let mut commits: Vec<MetaAttached<Entry, EntryMeta>> = Vec::new();
        let mut trees: Vec<MetaAttached<Entry, EntryMeta>> = Vec::new();
        let mut blobs: Vec<MetaAttached<Entry, EntryMeta>> = Vec::new();
        let mut tags: Vec<MetaAttached<Entry, EntryMeta>> = Vec::new();
        while let Some(entry) = entry_rx.recv().await {
            match entry.inner.obj_type {
                ObjectType::Commit => {
                    commits.push(entry);
                }
                ObjectType::Tree => {
                    trees.push(entry);
                }
                ObjectType::Blob => {
                    blobs.push(entry);
                }
                ObjectType::Tag => {
                    tags.push(entry);
                }
                _ => {
                    return Err(GitError::PackEncodeError(format!(
                        "object type `{}` is not supported by delta-window pack encoding",
                        entry.inner.obj_type
                    )));
                }
            }
        }

        commits.sort_by(magic_sort);
        trees.sort_by(magic_sort);
        blobs.sort_by(magic_sort);
        tags.sort_by(magic_sort);
        tracing::info!(
            "numbers :  commits: {:?} trees: {:?} blobs:{:?} tag :{:?}",
            commits.len(),
            trees.len(),
            blobs.len(),
            tags.len()
        );

        // Delta windows are independent by object type. Blobs, which normally
        // dominate both count and CPU time, are further divided into contiguous
        // sorted chunks and processed by a shared worker pool.

        let num_threads = std::env::var("PACK_THREADS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(4)
            });

        // Path metadata has served its sorting purpose and is not encoded.
        let commit_entries: Vec<Entry> =
            commits.into_iter().map(|e| e.inner).collect();
        let tree_entries: Vec<Entry> = trees.into_iter().map(|e| e.inner).collect();
        let tag_entries: Vec<Entry> = tags.into_iter().map(|e| e.inner).collect();
        let mut blob_entries: Vec<Entry> = blobs.into_iter().map(|e| e.inner).collect();

        tracing::info!(
            blobs = blob_entries.len(),
            threads = num_threads,
            "splitting blob delta search"
        );

        // Commit, tree, and tag buckets are small enough for one task each.
        let mk_blk = |entries: Vec<Entry>,
                      ez: bool,
                      er: bool,
                      dp: bool|
         -> tokio::task::JoinHandle<
            Result<Vec<(Vec<u8>, IndexEntry)>, GitError>,
        > {
            tokio::task::spawn_blocking(move || {
                Self::try_as_offset_delta(entries, 10, ez, er, dp)
            })
        };

        let commit_handle = mk_blk(commit_entries, enable_zstdelta, enable_rabin, disable_prefilter);
        let tree_handle = mk_blk(tree_entries, enable_zstdelta, enable_rabin, disable_prefilter);
        let tag_handle = mk_blk(tag_entries, enable_zstdelta, enable_rabin, disable_prefilter);

        // Split the sorted blob list into contiguous, count-balanced chunks.
        // Twenty chunks per worker provide load balancing without completely
        // discarding the path/name clustering established by `magic_sort`.
        let total_blob_entries = blob_entries.len();
        let chunks_per_thread: usize = 20;
        let blob_chunk_count = if num_threads > 1 && total_blob_entries > (num_threads * 20) {
            num_threads * chunks_per_thread
        } else {
            1
        };
        let entries_per_chunk = total_blob_entries.div_ceil(blob_chunk_count);

        struct BlobChunk {
            index: usize,
            entries: Vec<Entry>,
        }

        // Build from the end so each split is O(1), then restore sort order.
        let mut chunks: Vec<BlobChunk> = Vec::with_capacity(blob_chunk_count);
        for i in 0..blob_chunk_count {
            let take = entries_per_chunk.min(blob_entries.len());
            if take == 0 {
                break;
            }
            let entries = blob_entries.split_off(blob_entries.len() - take);
            chunks.push(BlobChunk { index: i, entries });
        }
        chunks.reverse();
        for (i, c) in chunks.iter_mut().enumerate() {
            c.index = i;
        }
        let actual_chunks = chunks.len();

        // Workers pop smaller-object chunks first. Faster workers can consume
        // additional chunks before the largest-object work is reached.
        type ChunkResult = (usize, Result<Vec<(Vec<u8>, IndexEntry)>, GitError>);
        let chunk_queue: Arc<Mutex<Vec<BlobChunk>>> =
            Arc::new(Mutex::new(chunks));
        let results: Arc<Mutex<Vec<ChunkResult>>> =
            Arc::new(Mutex::new(Vec::new()));
        let blob_thread_count = num_threads.min(actual_chunks);
        tracing::info!(
            total_entries = total_blob_entries,
            chunks = actual_chunks,
            entries_per_chunk = entries_per_chunk,
            threads = blob_thread_count,
            "splitting blob delta search",
        );
        let mut blob_handles: Vec<std::thread::JoinHandle<()>> =
            Vec::with_capacity(blob_thread_count);
        for _ in 0..blob_thread_count {
            let q = Arc::clone(&chunk_queue);
            let r = Arc::clone(&results);
            let ez = enable_zstdelta;
            let er = enable_rabin;
            let dp = disable_prefilter;
            blob_handles.push(thread::spawn(move || {
                loop {
                    let chunk = q.lock().unwrap().pop();
                    match chunk {
                        None => break,
                        Some(BlobChunk { index: chunk_idx, entries }) => {
                            let res = Self::try_as_offset_delta(
                                entries, 10, ez, er, dp,
                            );
                            r.lock().unwrap().push((chunk_idx, res));
                        }
                    }
                }
            }));
        }

        // These Tokio tasks run independently of the native blob workers.
        let (commit_results, tree_results, tag_results) = tokio::try_join!(
            commit_handle, tree_handle, tag_handle
        )
        .map_err(|e| GitError::PackEncodeError(format!("Task join error: {e}")))?;

        let commit_res = commit_results?;
        let tree_res = tree_results?;
        let tag_res = tag_results?;

        // Reassemble chunk results in original sorted order before assigning
        // absolute pack offsets.
        for handle in blob_handles {
            handle
                .join()
                .map_err(|_| {
                    GitError::PackEncodeError(
                        "blob delta thread panicked".to_string(),
                    )
                })?;
        }
        let mut chunk_results = Arc::into_inner(results)
            .ok_or_else(|| {
                GitError::PackEncodeError(
                    "blob chunk results still referenced".to_string(),
                )
            })?
            .into_inner()
            .map_err(|e| {
                GitError::PackEncodeError(format!("blob results poisoned: {e}"))
            })?;
        chunk_results.sort_by_key(|(i, _)| *i);
        let mut blob_res_list: Vec<Vec<(Vec<u8>, IndexEntry)>> =
            Vec::with_capacity(chunk_results.len());
        for (_idx, res) in chunk_results {
            blob_res_list.push(res?);
        }

        let mut all_res = vec![commit_res, tree_res];
        all_res.extend(blob_res_list);
        all_res.push(tag_res);

        let mut idx_entries = Vec::new();
        for res in &mut all_res {
            for data in res {
                data.1.offset = self.inner_offset as u64;
                self.write_all_and_update(&data.0).await;
                idx_entries.push(data.1.clone());
            }
        }

        self.idx_entries = Some(idx_entries);

        // The trailer is the checksum of the header and all encoded entries;
        // the trailer bytes themselves are not included in that checksum.
        let hash_result = self.inner_hash.clone().finalize();
        self.final_hash = Some(ObjectHash::from_bytes(&hash_result).unwrap());
        self.send_data(hash_result.to_vec()).await;

        self.drop_sender();
        Ok(())
    }

    /// Encodes one sorted bucket while selecting bases from a sliding window.
    ///
    /// Each returned tuple contains the encoded pack entry and its unfinished
    /// index record. Offsets are relative to the start of this bucket and are
    /// replaced with absolute pack offsets when the bucket results are emitted.
    /// Selected bases produce OFS_DELTA entries; entries without a worthwhile
    /// base are encoded in full.
    ///
    /// zstdelta format details:
    /// <https://sapling-scm.com/docs/dev/internals/zstdelta/>
    #[cfg_attr(not(feature = "diff_rabin"), allow(unused_variables))]
    fn try_as_offset_delta(
        mut bucket: Vec<Entry>,
        window_size: usize,
        enable_zstdelta: bool,
        enable_rabin: bool,
        disable_prefilter: bool,
    ) -> Result<Vec<(Vec<u8>, IndexEntry)>, GitError> {
        let mut current_offset = 0usize;
        let mut window: VecDeque<DeltaWindowEntry> = VecDeque::with_capacity(window_size);
        let mut res: Vec<(Vec<u8>, IndexEntry)> = Vec::new();

        for entry in bucket.iter_mut() {
            let mut best_base: Option<&DeltaWindowEntry> = None;
            let mut best_rate: f64 = 0.0;

            // Rabin can cheaply measure actual delta output using a cached source
            // index. The non-Rabin path estimates savings before paying for one
            // final delta calculation against the selected base.
            #[cfg(feature = "diff_rabin")]
            {
                let trg_size = entry.data.len();

                // Search newest to oldest because adjacent sorted entries are
                // most likely to belong to the same file family.
                let pre_filtered_idxs: Vec<usize> = window
                    .iter()
                    .enumerate()
                    .rev() // most recent first (matching git's search order)
                    .filter(|(_i, try_base)| {
                        let src_size = try_base.entry.data.len();
                        try_base.entry.obj_type == entry.obj_type
                            && try_base.entry.chain_len < MAX_CHAIN_LEN
                            && try_base.entry.hash != entry.hash
                            // Reject a base more than 32 times larger than the target.
                            && trg_size >= src_size / 32
                            // A large positive size difference cannot yield enough savings.
                            && src_size.saturating_sub(trg_size.min(src_size))
                                < trg_size / 2
                            && (disable_prefilter
                                || multi_point_similar(&try_base.entry.data, &entry.data))
                    })
                    .map(|(i, _)| i)
                    .collect();

                if !pre_filtered_idxs.is_empty() {
                    // Start with a threshold that requires roughly 50% savings.
                    // Each successful candidate lowers the limit for later work.
                    let max_delta_size = trg_size.saturating_sub(trg_size / 2 + 20);
                    let mut best_delta_size = max_delta_size;
                    let mut best_idx: Option<usize> = None;

                    for &idx in &pre_filtered_idxs {
                        // Build the source index at most once while it remains in
                        // the window. Very small sources cannot be indexed.
                        if window[idx].rabin_index.is_none() {
                            // The index and window entry share source bytes through
                            // `Arc`, avoiding another source-buffer allocation.
                            let data_arc = match window[idx].data_arc.take() {
                                Some(arc) => arc,
                                None => {
                                    let arc: Arc<[u8]> =
                                        window[idx].entry.data.clone().into();
                                    arc
                                }
                            };
                            match delta::create_delta_index_arc(
                                Arc::clone(&data_arc),
                            ) {
                                Some(new_index) => {
                                    window[idx].data_arc = Some(data_arc);
                                    window[idx].rabin_index = Some(new_index);
                                }
                                None => {
                                    window[idx].data_arc = Some(data_arc);
                                    continue;
                                }
                            }
                        }

                        // End the index borrow before retaining the window entry.
                        let delta = {
                            let index = window[idx].rabin_index.as_ref().unwrap();
                            delta::encode_rabin_with_index_and_max_size(
                                index,
                                &entry.data,
                                best_delta_size,
                            )
                        };

                        if let Some(delta_data) = delta {
                            let delta_size = delta_data.len();
                            if delta_size < best_delta_size {
                                best_delta_size = delta_size;
                                best_rate = 1.0
                                    - delta_size as f64 / entry.data.len() as f64;
                                best_idx = Some(idx);
                            }
                        }
                    }

                    best_base = best_idx.map(|i| &window[i]);
                }
            }

            // Estimate savings in parallel, then build a real delta only for
            // the best candidate.
            #[cfg(not(feature = "diff_rabin"))]
            {
                let candidates: Vec<_> = window
                    .par_iter()
                    .with_min_len(3)
                    .filter_map(|try_base| {
                        if try_base.entry.obj_type != entry.obj_type
                            || try_base.entry.chain_len >= MAX_CHAIN_LEN
                            || try_base.entry.hash == entry.hash
                        {
                            return None;
                        }
                        let sym_ratio = (try_base.entry.data.len().min(entry.data.len()) as f64)
                            / (try_base.entry.data.len().max(entry.data.len()) as f64);
                        let no_prefilter = disable_prefilter;
                        if sym_ratio < 0.5
                            || (!no_prefilter
                                && !cheap_similar(&try_base.entry.data, &entry.data))
                        {
                            return None;
                        }
                        let rate = if (try_base.entry.data.len() + entry.data.len()) / 2 > 64 {
                            delta::heuristic_encode_rate_parallel(
                                &try_base.entry.data,
                                &entry.data,
                            )
                        } else {
                            delta::encode_rate(&try_base.entry.data, &entry.data)
                        };
                        if rate > MIN_DELTA_RATE {
                            Some((rate, try_base))
                        } else {
                            None
                        }
                    })
                    .collect();

                let tie_epsilon: f64 = 0.15;
                for (rate, try_base) in candidates {
                    match best_base {
                        None => {
                            best_rate = rate;
                            best_base = Some(try_base);
                        }
                        Some(best_base_ref) => {
                            let is_better = if rate > best_rate + tie_epsilon {
                                true
                            } else if (rate - best_rate).abs() <= tie_epsilon {
                                try_base.entry.chain_len > best_base_ref.entry.chain_len
                            } else {
                                false
                            };

                            if is_better {
                                best_rate = rate;
                                best_base = Some(try_base);
                            }
                        }
                    }
                }
            }

            // Require enough estimated savings to justify delta metadata and
            // future reconstruction cost.
            if best_rate < MIN_DELTA_RATE {
                best_base = None;
            }

            let mut entry_for_window = entry.clone();

            let offset = best_base.map(|best_base| {
                let delta = if enable_zstdelta {
                    entry.obj_type = ObjectType::OffsetZstdelta;
                    zstdelta::diff(&best_base.entry.data, &entry.data)
                        .map_err(|e| {
                            GitError::DeltaObjectError(format!("zstdelta diff failed: {e}"))
                        })
                        .unwrap()
                } else {
                    // Reuse the source index created during candidate scoring.
                    #[cfg(feature = "diff_rabin")]
                    if enable_rabin {
                        entry.obj_type = ObjectType::OffsetDelta;
                        delta::encode_rabin_with_index(
                            best_base.rabin_index.as_ref().unwrap(),
                            &entry.data,
                        )
                    } else {
                        entry.obj_type = ObjectType::OffsetDelta;
                        delta::encode(&best_base.entry.data, &entry.data)
                    }
                    #[cfg(not(feature = "diff_rabin"))]
                    {
                        entry.obj_type = ObjectType::OffsetDelta;
                        delta::encode(&best_base.entry.data, &entry.data)
                    }
                };
                entry.data = delta;
                entry.chain_len = best_base.entry.chain_len + 1;
                current_offset - best_base.offset
            });

            entry_for_window.chain_len = entry.chain_len;
            let obj_data = encode_one_object(entry, offset)?;
            window.push_back(DeltaWindowEntry::new(entry_for_window, current_offset));
            if window.len() > window_size {
                window.pop_front();
            }
            res.push((obj_data.clone(), IndexEntry::new(entry, 0)));
            current_offset += obj_data.len();
        }
        Ok(res)
    }

    /// Encodes full objects in ordered, Rayon-parallel batches.
    ///
    /// This path is valid only when `window_size == 0`; preserving input order
    /// allows absolute pack offsets and index entries to be assigned as each
    /// completed batch is streamed.
    pub async fn parallel_encode(
        &mut self,
        mut entry_rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    ) -> Result<(), GitError> {
        if self.window_size != 0 {
            return Err(GitError::PackEncodeError(
                "parallel encode only works when window_size == 0".to_string(),
            ));
        }

        let head = encode_header(self.object_number);
        self.send_data(head.clone()).await;
        self.inner_hash.update(&head);

        if self.start_encoding {
            return Err(GitError::PackEncodeError(
                "encoding operation is already in progress".to_string(),
            ));
        }

        let mut idx_entries = Vec::new();
        let batch_size = usize::max(1000, entry_rx.max_capacity() / 10);
        tracing::info!("encode with batch size: {}", batch_size);
        loop {
            let mut batch_entries = Vec::with_capacity(batch_size);
            time_it!("parallel encode: receive batch", {
                for _ in 0..batch_size {
                    match entry_rx.recv().await {
                        Some(entry) => {
                            if entry.inner.obj_type.is_ai_object() {
                                return Err(GitError::PackEncodeError(format!(
                                    "AI object type `{}` cannot be encoded in a pack file",
                                    entry.inner.obj_type
                                )));
                            }
                            batch_entries.push(entry.inner);
                            self.process_index += 1;
                        }
                        None => break,
                    }
                }
            });

            if batch_entries.is_empty() {
                break;
            }

            // Indexed parallel iteration preserves input order in `collect`, so
            // offsets remain aligned with the receiver's object order.
            let batch_result: Vec<Result<(Vec<u8>, IndexEntry), GitError>> =
                time_it!("parallel encode: encode batch", {
                    batch_entries
                        .par_iter()
                        .map(|entry| {
                            encode_one_object(entry, None)
                                .map(|encoded| (encoded, IndexEntry::new(entry, 0)))
                        })
                        .collect()
                });

            time_it!("parallel encode: write batch", {
                for obj_data in batch_result {
                    let mut obj_data = obj_data?;
                    obj_data.1.offset = self.inner_offset as u64;
                    self.write_all_and_update(&obj_data.0).await;
                    idx_entries.push(obj_data.1);
                }
            });
        }

        tracing::debug!("parallel encode idx entries: {:?}", idx_entries.len());
        if self.process_index != self.object_number {
            panic!(
                "not all objects are encoded, process:{}, total:{}",
                self.process_index, self.object_number
            );
        }

        // Append the checksum after hashing the header and every object entry.
        let hash_result = self.inner_hash.clone().finalize();
        self.final_hash = Some(ObjectHash::from_bytes(&hash_result).unwrap());
        self.send_data(hash_result.to_vec()).await;
        self.drop_sender();

        self.idx_entries = Some(idx_entries);
        Ok(())
    }

    /// Accounts for one encoded chunk and forwards it to the pack writer.
    async fn write_all_and_update(&mut self, data: &[u8]) {
        self.inner_hash.update(data);
        self.inner_offset += data.len();
        self.send_data(data.to_vec()).await;
    }

    async fn generate_idx_file(&mut self) -> Result<(), GitError> {
        let final_hash = self.final_hash
            .ok_or(GitError::PackEncodeError("final_hash is missing,The pack file must be generated before the index file is produced.".into()))?;
        let idx_entries = self.idx_entries.clone().ok_or(GitError::PackEncodeError(
            "The pack file must be generated before the index file is produced.".into(),
        ))?;
        let mut idx_builder = IdxBuilder::new(
            self.object_number,
            self.idx_sender.clone().unwrap(),
            final_hash,
        );
        idx_builder.write_idx(idx_entries).await?;
        Ok(())
    }

    /// Spawns pack encoding and returns a handle for the background task.
    ///
    /// This consumes the encoder. A zero-sized window uses [`Self::parallel_encode`];
    /// otherwise [`Self::encode`] performs sorted delta-window encoding.
    ///
    /// The task panics if encoding fails; callers should await the returned handle.
    pub async fn encode_async(
        mut self,
        rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    ) -> Result<JoinHandle<()>, GitError> {
        Ok(tokio::spawn(async move {
            if self.window_size == 0 {
                self.parallel_encode(rx).await.unwrap()
            } else {
                self.encode(rx).await.unwrap()
            }
        }))
    }

    /// Spawns zstdelta encoding and returns a handle for the background task.
    ///
    /// This consumes the encoder. The task panics if encoding fails.
    pub async fn encode_async_with_zstdelta(
        mut self,
        rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    ) -> Result<JoinHandle<()>, GitError> {
        Ok(tokio::spawn(async move {
            // zstdelta requires a base-selection window, so it has no full-object
            // parallel fast path.
            self.encode_with_zstdelta(rx).await.unwrap()
        }))
    }

    /// Streams the companion index after pack encoding has completed.
    ///
    /// The pack checksum and collected object offsets are required, so calling
    /// this method before successful pack encoding returns an error.
    pub async fn encode_idx_file(&mut self) -> Result<(), GitError> {
        if self.idx_sender.is_none() {
            return Err(GitError::PackEncodeError(String::from(
                "idx sender is none",
            )));
        }
        self.generate_idx_file().await?;
        // Closing the channel signals that the index writer can flush and exit.
        self.idx_sender.take();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, path::PathBuf, sync::Arc, time::Instant};

    use tempfile::tempdir;
    use tokio::sync::Mutex;

    use super::*;
    use crate::{
        hash::{HashKind, ObjectHash, set_hash_kind_for_test},
        internal::{
            object::{blob::Blob, types::ObjectType},
            pack::{
                Pack,
                test_pack_download::{PackFileGuard, download_pack_file},
                tests::init_logger,
                utils::read_offset_encoding,
            },
        },
        time_it,
    };

    /// Check if the given data is a valid pack file format by attempting to decode it.
    fn check_format(data: &Vec<u8>) {
        // Use a smaller cap on 32-bit targets to avoid usize overflow.
        let max_pack_size_u64 = if cfg!(target_pointer_width = "64") {
            6u64 * 1024 * 1024 * 1024
        } else {
            2u64 * 1024 * 1024 * 1024
        };
        let max_pack_size = usize::try_from(max_pack_size_u64).unwrap_or_else(|_| {
            panic!(
                "internal assertion failed: pack size cap {} does not fit in usize on this \
                 target; this should be unreachable given the target_pointer_width configuration",
                max_pack_size_u64
            )
        });
        let mut p = Pack::new(
            None,
            Some(max_pack_size), // 6GB on 64-bit, 2GB on 32-bit
            Some(PathBuf::from("/tmp/.cache_temp")),
            true,
        );
        let mut reader = Cursor::new(data);
        tracing::debug!("start check format");
        p.decode(&mut reader, |_| {}, None::<fn(ObjectHash)>)
            .expect("pack file format error");
    }

    #[tokio::test]
    async fn test_pack_encoder() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        async fn encode_once(window_size: usize) -> Vec<u8> {
            let (tx, mut rx) = mpsc::channel(100);
            let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1);

            // make some different objects, or decode will fail
            let str_vec = vec!["hello, word", "hello, world.", "!", "123141251251"];
            let encoder = PackEncoder::new(str_vec.len(), window_size, tx);
            encoder.encode_async(entry_rx).await.unwrap();

            for str in str_vec {
                let blob = Blob::from_content(str);
                let entry: Entry = blob.into();
                entry_tx
                    .send(MetaAttached {
                        inner: entry,
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            // assert!(encoder.get_hash().is_some());
            let mut result = Vec::new();
            while let Some(chunk) = rx.recv().await {
                result.extend(chunk);
            }
            result
        }

        // without delta
        let pack_without_delta = encode_once(0).await;
        let pack_without_delta_size = pack_without_delta.len();
        check_format(&pack_without_delta);

        // with delta
        let pack_with_delta = encode_once(4).await;
        assert!(pack_with_delta.len() <= pack_without_delta_size);
        check_format(&pack_with_delta);
    }
    #[tokio::test]
    async fn test_pack_encoder_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);

        async fn encode_once(window_size: usize) -> Vec<u8> {
            let (tx, mut rx) = mpsc::channel(100);
            let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1);

            let str_vec = vec!["hello, word", "hello, world.", "!", "123141251251"];
            let encoder = PackEncoder::new(str_vec.len(), window_size, tx);
            encoder.encode_async(entry_rx).await.unwrap();

            for s in str_vec {
                let blob = Blob::from_content(s);
                let entry: Entry = blob.into();
                entry_tx
                    .send(MetaAttached {
                        inner: entry,
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);

            let mut result = Vec::new();
            while let Some(chunk) = rx.recv().await {
                result.extend(chunk);
            }
            result
        }

        // without delta
        let pack_without_delta = encode_once(0).await;
        let pack_without_delta_size = pack_without_delta.len();
        check_format(&pack_without_delta);

        // with delta
        let pack_with_delta = encode_once(4).await;
        assert!(pack_with_delta.len() <= pack_without_delta_size);
        check_format(&pack_with_delta);
    }

    #[tokio::test]
    async fn test_pack_encoder_rejects_unencodable_ai_type_parallel() {
        let (tx, _rx) = mpsc::channel(8);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1);
        let mut encoder = PackEncoder::new(1, 0, tx);

        let mut entry: Entry = Blob::from_content("ai").into();
        entry.obj_type = ObjectType::Task;
        entry_tx
            .send(MetaAttached {
                inner: entry,
                meta: EntryMeta::new(),
            })
            .await
            .expect("send entry");
        drop(entry_tx);

        let err = encoder
            .encode(entry_rx)
            .await
            .expect_err("must reject AI pack type");
        assert!(matches!(err, GitError::PackEncodeError(_)));
    }

    #[tokio::test]
    async fn test_pack_encoder_rejects_unencodable_ai_type_delta_window() {
        let (tx, _rx) = mpsc::channel(8);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1);
        let mut encoder = PackEncoder::new(1, 10, tx);

        let mut entry: Entry = Blob::from_content("ai").into();
        entry.obj_type = ObjectType::Task;
        entry_tx
            .send(MetaAttached {
                inner: entry,
                meta: EntryMeta::new(),
            })
            .await
            .expect("send entry");
        drop(entry_tx);

        let err = encoder
            .encode(entry_rx)
            .await
            .expect_err("must reject AI pack type");
        assert!(matches!(err, GitError::PackEncodeError(_)));
    }

    async fn get_entries_for_test() -> (Arc<Mutex<Vec<Entry>>>, PackFileGuard) {
        let (source, dl_guard) = download_pack_file("encode-test-sha1.pack");

        let mut p = Pack::new(None, None, Some(PathBuf::from("/tmp/.cache_temp")), true);

        let f = std::fs::File::open(&source).unwrap();
        tracing::info!("pack file size: {}", f.metadata().unwrap().len());
        let mut reader = std::io::BufReader::new(f);
        let entries = Arc::new(Mutex::new(Vec::new()));
        let entries_clone = entries.clone();
        p.decode(
            &mut reader,
            move |entry| {
                let mut entries = entries_clone.blocking_lock();
                entries.push(entry.inner);
            },
            None::<fn(ObjectHash)>,
        )
        .unwrap();
        assert_eq!(p.number, entries.lock().await.len());
        tracing::info!("total entries: {}", p.number);
        drop(p);

        (entries, dl_guard)
    }
    async fn get_entries_for_test_sha256() -> (Arc<Mutex<Vec<Entry>>>, PackFileGuard) {
        let (source, dl_guard) = download_pack_file("encode-test-sha256.pack");

        let mut p = Pack::new(None, None, Some(PathBuf::from("/tmp/.cache_temp")), true);

        let f = std::fs::File::open(&source).unwrap();
        tracing::info!("pack file size: {}", f.metadata().unwrap().len());
        let mut reader = std::io::BufReader::new(f);
        let entries = Arc::new(Mutex::new(Vec::new()));
        let entries_clone = entries.clone();
        p.decode(
            &mut reader,
            move |entry| {
                let mut entries = entries_clone.blocking_lock();
                entries.push(entry.inner);
            },
            None::<fn(ObjectHash)>,
        )
        .unwrap();
        assert_eq!(p.number, entries.lock().await.len());
        tracing::info!("total entries: {}", p.number);
        drop(p);

        (entries, dl_guard)
    }

    #[tokio::test]
    async fn test_pack_encoder_parallel_large_file() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        init_logger();

        let start = Instant::now();
        let (entries, _dl_guard) = get_entries_for_test().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        // encode entries with parallel
        let (tx, mut rx) = mpsc::channel(1_000_000);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1_000_000);

        let mut encoder = PackEncoder::new(entries_number, 0, tx);
        tokio::spawn(async move {
            time_it!("test parallel encode", {
                encoder.parallel_encode(entry_rx).await.unwrap();
            });
        });

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }

        let pack_size = result.len();
        let compression_rate = if total_original_size > 0 {
            1.0 - (pack_size as f64 / total_original_size as f64)
        } else {
            0.0
        };

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("new pack file size: {}", result.len());
        tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
        // check format
        check_format(&result);
    }
    #[tokio::test]
    async fn test_pack_encoder_parallel_large_file_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        init_logger();

        let start = Instant::now();
        // use sha256 pack file for testing
        let (entries, _dl_guard) = get_entries_for_test_sha256().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let (tx, mut rx) = mpsc::channel(1_000_000);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1_000_000);

        let mut encoder = PackEncoder::new(entries_number, 0, tx);
        tokio::spawn(async move {
            time_it!("test parallel encode sha256", {
                encoder.parallel_encode(entry_rx).await.unwrap();
            });
        });

        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }

        let pack_size = result.len();
        let compression_rate = if total_original_size > 0 {
            1.0 - (pack_size as f64 / total_original_size as f64)
        } else {
            0.0
        };

        let duration = start.elapsed();
        tracing::info!("sha256 test executed in: {:.2?}", duration);
        tracing::info!("new pack file size: {}", result.len());
        tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
        check_format(&result);
    }

    #[tokio::test]
    async fn test_pack_encoder_large_file() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        init_logger();
        let (entries, _dl_guard) = get_entries_for_test().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let start = Instant::now();
        // encode entries
        let (tx, mut rx) = mpsc::channel(100_000);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

        let mut encoder = PackEncoder::new(entries_number, 0, tx);
        tokio::spawn(async move {
            time_it!("test encode no parallel", {
                encoder.encode(entry_rx).await.unwrap();
            });
        });

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        // // only receive data
        // while (rx.recv().await).is_some() {
        //     // do nothing
        // }

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }

        let pack_size = result.len();
        let compression_rate = if total_original_size > 0 {
            1.0 - (pack_size as f64 / total_original_size as f64)
        } else {
            0.0
        };

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("new pack file size: {}", pack_size);
        tracing::info!("original total size: {}", total_original_size);
        tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
        tracing::info!(
            "space saved: {} bytes",
            total_original_size.saturating_sub(pack_size)
        );
    }
    #[tokio::test]
    async fn test_pack_encoder_large_file_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        init_logger();
        let (entries, _dl_guard) = get_entries_for_test_sha256().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let start = Instant::now();
        // encode entries
        let (tx, mut rx) = mpsc::channel(100_000);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

        let mut encoder = PackEncoder::new(entries_number, 0, tx);
        tokio::spawn(async move {
            time_it!("test encode no parallel sha256", {
                encoder.encode(entry_rx).await.unwrap();
            });
        });

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        // // only receive data
        // while (rx.recv().await).is_some() {
        //     // do nothing
        // }

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }

        let pack_size = result.len();
        let compression_rate = if total_original_size > 0 {
            1.0 - (pack_size as f64 / total_original_size as f64)
        } else {
            0.0
        };

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("new pack file size: {}", pack_size);
        tracing::info!("original total size: {}", total_original_size);
        tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
        tracing::info!(
            "space saved: {} bytes",
            total_original_size.saturating_sub(pack_size)
        );
    }

    #[tokio::test]
    async fn test_pack_encoder_with_zstdelta() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        init_logger();
        let (entries, _dl_guard) = get_entries_for_test().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let start = Instant::now();
        let (tx, mut rx) = mpsc::channel(100_000);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

        let encoder = PackEncoder::new(entries_number, 10, tx);
        encoder.encode_async_with_zstdelta(entry_rx).await.unwrap();

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }

        let pack_size = result.len();
        let compression_rate = if total_original_size > 0 {
            1.0 - (pack_size as f64 / total_original_size as f64)
        } else {
            0.0
        };

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("new pack file size: {}", pack_size);
        tracing::info!("original total size: {}", total_original_size);
        tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
        tracing::info!(
            "space saved: {} bytes",
            total_original_size.saturating_sub(pack_size)
        );

        // check format
        check_format(&result);
    }
    #[tokio::test]
    async fn test_pack_encoder_with_zstdelta_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        init_logger();
        let (entries, _dl_guard) = get_entries_for_test_sha256().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let start = Instant::now();
        let (tx, mut rx) = mpsc::channel(100_000);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

        let encoder = PackEncoder::new(entries_number, 10, tx);
        encoder.encode_async_with_zstdelta(entry_rx).await.unwrap();

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }

        let pack_size = result.len();
        let compression_rate = if total_original_size > 0 {
            1.0 - (pack_size as f64 / total_original_size as f64)
        } else {
            0.0
        };

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("new pack file size: {}", pack_size);
        tracing::info!("original total size: {}", total_original_size);
        tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
        tracing::info!(
            "space saved: {} bytes",
            total_original_size.saturating_sub(pack_size)
        );

        // check format
        check_format(&result);
    }

    #[test]
    fn test_encode_offset() {
        // let value = 11013;
        let value = 16389;

        let data = encode_offset(value);
        println!("{data:?}");
        let mut reader = Cursor::new(data);
        let (result, _) = read_offset_encoding(&mut reader).unwrap();
        println!("result: {result}");
        assert_eq!(result, value as u64);
    }

    #[tokio::test]
    async fn test_pack_encoder_large_file_with_delta() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        init_logger();
        let (entries, _dl_guard) = get_entries_for_test().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let (tx, mut rx) = mpsc::channel(100_000);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

        let encoder = PackEncoder::new(entries_number, 10, tx);

        let start = Instant::now(); // 开始时间
        encoder.encode_async(entry_rx).await.unwrap();

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }

        let pack_size = result.len();
        let compression_rate = if total_original_size > 0 {
            1.0 - (pack_size as f64 / total_original_size as f64)
        } else {
            0.0
        };

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("new pack file size: {}", pack_size);
        tracing::info!("original total size: {}", total_original_size);
        tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
        tracing::info!(
            "space saved: {} bytes",
            total_original_size.saturating_sub(pack_size)
        );

        // check format
        check_format(&result);
    }
    #[tokio::test]
    async fn test_pack_encoder_large_file_with_delta_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        init_logger();
        let (entries, _dl_guard) = get_entries_for_test_sha256().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let (tx, mut rx) = mpsc::channel(100_000);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

        let encoder = PackEncoder::new(entries_number, 10, tx);

        let start = Instant::now(); // 开始时间
        encoder.encode_async(entry_rx).await.unwrap();

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }

        let pack_size = result.len();
        let compression_rate = if total_original_size > 0 {
            1.0 - (pack_size as f64 / total_original_size as f64)
        } else {
            0.0
        };

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("new pack file size: {}", pack_size);
        tracing::info!("original total size: {}", total_original_size);
        tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
        tracing::info!(
            "space saved: {} bytes",
            total_original_size.saturating_sub(pack_size)
        );

        // check format
        check_format(&result);
    }

    #[tokio::test]
    async fn test_pack_encoder_output_to_files() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        init_logger();
        let (entries, _dl_guard) = get_entries_for_test().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let start = Instant::now();

        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);
        // 自动创建临时目录，生命周期结束自动删除
        let dir = tempdir().unwrap();
        let path = dir.path();

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        encode_and_output_to_files(entry_rx, entries_number, path.to_path_buf(), 0)
            .await
            .unwrap();

        // 验证临时目录下生成的 pack/idx 文件
        let mut pack_file = None;
        let mut idx_file = None;
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let file_name = entry.file_name();
            tracing::info!("file name: {:?}", file_name);
            let file_name = file_name.to_string_lossy();
            if file_name.ends_with(".pack") {
                pack_file = Some(entry.path());
            } else if file_name.ends_with(".idx") {
                idx_file = Some(entry.path());
            }
        }
        let pack_file = pack_file.expect("pack file not generated");
        let idx_file = idx_file.expect("idx file not generated");
        assert!(
            pack_file.metadata().unwrap().len() > 0,
            "pack file is empty"
        );
        assert!(idx_file.metadata().unwrap().len() > 0, "idx file is empty");

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("original total size: {}", total_original_size);
    }

    #[tokio::test]
    async fn test_pack_encoder_output_to_files_with_delta() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        init_logger();
        let (entries, _dl_guard) = get_entries_for_test().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let start = Instant::now();

        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);
        // 自动创建临时目录，生命周期结束自动删除
        let dir = tempdir().unwrap();
        let path = dir.path();

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        encode_and_output_to_files(entry_rx, entries_number, path.to_path_buf(), 10)
            .await
            .unwrap();

        // 验证临时目录下生成的 pack/idx 文件
        let mut pack_file = None;
        let mut idx_file = None;
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let file_name = entry.file_name();
            tracing::info!("file name: {:?}", file_name);
            let file_name = file_name.to_string_lossy();
            if file_name.ends_with(".pack") {
                pack_file = Some(entry.path());
            } else if file_name.ends_with(".idx") {
                idx_file = Some(entry.path());
            }
        }
        let pack_file = pack_file.expect("pack file not generated");
        let idx_file = idx_file.expect("idx file not generated");
        assert!(
            pack_file.metadata().unwrap().len() > 0,
            "pack file is empty"
        );
        assert!(idx_file.metadata().unwrap().len() > 0, "idx file is empty");

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("original total size: {}", total_original_size);
    }
}
