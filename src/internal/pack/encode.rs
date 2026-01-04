//! Pack encoder capable of building streamed `.pack`/`.idx` pairs with optional delta compression,
//! windowing, and asynchronous writers.

use std::{
    cmp::Ordering,
    collections::VecDeque,
    hash::{Hash, Hasher},
    io::Write,
    path::{Path, PathBuf},
};

use ahash::AHasher;
// use libc::ungetc;
use chrono::Utc;
use flate2::write::ZlibEncoder;
use natord::compare;
use rayon::prelude::*;
//use tokio::io::AsyncWriteExt;
use tokio::io::AsyncWriteExt as TokioAsyncWriteExt;
use tokio::{fs::File, sync::mpsc, task::JoinHandle};

//use std::io as stdio;
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
const MIN_DELTA_RATE: f64 = 0.5; // minimum delta rate
//const MAX_ZSTDELTA_CHAIN_LEN: usize = 50;

/// A encoder for generating pack files with delta objects.
pub struct PackEncoder {
    //path: Option<PathBuf>,
    object_number: usize,
    process_index: usize,
    window_size: usize,
    // window: VecDeque<(Entry, usize)>, // entry and offset
    pack_sender: Option<mpsc::Sender<Vec<u8>>>,
    idx_sender: Option<mpsc::Sender<Vec<u8>>>,
    //idx_sender: Option<mpsc::Sender<Vec<u8>>>,
    idx_entries: Option<Vec<IndexEntry>>,
    inner_offset: usize,       // offset of current entry
    inner_hash: HashAlgorithm, // introduce different hash algorithm
    final_hash: Option<ObjectHash>,
    start_encoding: bool,
}

/// Encode entries into a pack, write `.pack`/`.idx` files to `output_dir`.
/// - Spawns background writers to consume pack/idx channels to avoid back-pressure.
/// - Uses `window_size` to control delta: `0` means no delta (parallel encode), otherwise enable delta window.
/// # Arguments
/// * `raw_entries_rx` - receiver providing entries with metadata
/// * `object_number` - expected total object count for the pack header
/// * `output_dir` - target directory to place the generated files
/// * `window_size` - delta window size; `0` disables delta
/// # Returns
/// * `Ok(())` on success, `GitError` on failure
pub async fn encode_and_output_to_files(
    raw_entries_rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    object_number: usize,
    output_dir: PathBuf,
    window_size: usize,
) -> Result<(), GitError> {
    let (pack_tx, mut pack_rx) = mpsc::channel(1024);
    let (idx_tx, mut idx_rx) = mpsc::channel(1024);
    let mut pack_encoder = PackEncoder::new_with_idx(object_number, window_size, pack_tx, idx_tx);

    // timestamp for temp filename
    let now = Utc::now();
    let timestamp = now.format("%Y%m%d%H%M%S%.3f").to_string(); // 例如 20251209235959.123
    let tmp_path = output_dir.join(format!("{}objects.pack.tmp", timestamp));
    let mut pack_file = File::create(&tmp_path).await?;

    let pack_writer = tokio::spawn(async move {
        while let Some(chunk) = pack_rx.recv().await {
            TokioAsyncWriteExt::write_all(&mut pack_file, &chunk).await?;
        }
        //pack_file.flush().await?;
        TokioAsyncWriteExt::flush(&mut pack_file).await?;
        Ok::<(), GitError>(())
    });

    pack_encoder.encode(raw_entries_rx).await?;

    // 等待 pack 写入完成
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
            //idx_file.write_all(&chunk).await?;
            TokioAsyncWriteExt::write_all(&mut idx_file, &chunk).await?;
        }
        //idx_file.flush().await?;
        TokioAsyncWriteExt::flush(&mut idx_file).await?;
        Ok::<(), GitError>(())
    });

    //build idx
    pack_encoder.encode_idx_file().await?;

    let idx_write_result = idx_writer
        .await
        .map_err(|e| GitError::PackEncodeError(format!("idx writer task join error: {e}")))?;
    idx_write_result?;

    Ok(())
}

/// Encode header of pack file (12 byte)<br>
/// Content: 'PACK', Version(2), number of objects
fn encode_header(object_number: usize) -> Vec<u8> {
    let mut result: Vec<u8> = vec![
        b'P', b'A', b'C', b'K', // The logotype of the Pack File
        0, 0, 0, 2, // generates version 2 only.
    ];
    assert_ne!(object_number, 0); // guarantee self.number_of_objects!=0
    assert!(object_number <= u32::MAX as usize);
    //TODO: GitError:numbers of objects should < 4G ,
    result.append((object_number as u32).to_be_bytes().to_vec().as_mut()); // to 4 bytes (network byte order aka. big-endian)
    result
}

/// Encode offset of delta object
fn encode_offset(mut value: usize) -> Vec<u8> {
    assert_ne!(value, 0, "offset can't be zero");
    let mut bytes = Vec::new();

    bytes.push((value & 0x7F) as u8);
    value >>= 7;
    while value != 0 {
        value -= 1;
        let byte = (value & 0x7F) as u8 | 0x80; // set first bit one
        value >>= 7;
        bytes.push(byte);
    }
    bytes.reverse();
    bytes
}

/// Encode one object, and update the hash
/// @offset: offset of this object if it's a delta object. For other object, it's None
fn encode_one_object(entry: &Entry, offset: Option<usize>) -> Result<Vec<u8>, GitError> {
    // try encode as delta
    let obj_data = &entry.data;
    let obj_data_len = obj_data.len();
    let obj_type_number = entry.obj_type.to_u8();

    let mut encoded_data = Vec::new();

    // **header** encoding
    let mut header_data = vec![(0x80 | (obj_type_number << 4)) + (obj_data_len & 0x0f) as u8];
    let mut size = obj_data_len >> 4; // 4 bit has been used in first byte
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

    // **offset** encoding
    if entry.obj_type == ObjectType::OffsetDelta || entry.obj_type == ObjectType::OffsetZstdelta {
        let offset_data = encode_offset(offset.unwrap());
        encoded_data.extend(offset_data);
    } else if entry.obj_type == ObjectType::HashDelta {
        unreachable!("unsupported type")
    }

    // **data** encoding, need zlib compress
    let mut inflate = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    inflate
        .write_all(obj_data)
        .expect("zlib compress should never failed");
    inflate.flush().expect("zlib flush should never failed");
    let compressed_data = inflate.finish().expect("zlib compress should never failed");
    // self.write_all_and_update(&compressed_data).await;
    encoded_data.extend(compressed_data);
    Ok(encoded_data)
}

/// Magic sort function for entries
fn magic_sort(a: &MetaAttached<Entry, EntryMeta>, b: &MetaAttached<Entry, EntryMeta>) -> Ordering {
    let path_a = a.meta.file_path.as_ref();
    let path_b = b.meta.file_path.as_ref();

    // 1. Handle path existence: entries with paths sort first
    match (path_a, path_b) {
        (Some(pa), Some(pb)) => {
            let pa = Path::new(pa);
            let pb = Path::new(pb);

            // 1. Compare parent directory paths
            let dir_ord = pa.parent().cmp(&pb.parent());
            if dir_ord != Ordering::Equal {
                return dir_ord;
            }

            // 2. Compare filenames (natural sort)
            let name_a = pa.file_name().unwrap_or_default().to_string_lossy();
            let name_b = pb.file_name().unwrap_or_default().to_string_lossy();
            let name_ord = compare(&name_a, &name_b);
            if name_ord != Ordering::Equal {
                return name_ord;
            }
        }
        (Some(_), None) => return Ordering::Less, // entries with paths sort first
        (None, Some(_)) => return Ordering::Greater, // entries without paths sort last
        (None, None) => {}
    }

    let ord = b.inner.data.len().cmp(&a.inner.data.len());
    if ord != Ordering::Equal {
        return ord;
    }

    // fallback pointer order (newest first)
    (a as *const MetaAttached<Entry, EntryMeta>).cmp(&(b as *const MetaAttached<Entry, EntryMeta>))
}

/// Calculate hash of data
fn calc_hash(data: &[u8]) -> u64 {
    let mut hasher = AHasher::default();
    data.hash(&mut hasher);
    hasher.finish()
}

/// Cheap check if two byte slices are similar by comparing their hashes of the first 128 bytes.
fn cheap_similar(a: &[u8], b: &[u8]) -> bool {
    let k = a.len().min(b.len()).min(128);
    if k == 0 {
        return false;
    }
    calc_hash(&a[..k]) == calc_hash(&b[..k])
}

impl PackEncoder {
    pub fn new(object_number: usize, window_size: usize, sender: mpsc::Sender<Vec<u8>>) -> Self {
        PackEncoder {
            object_number,
            window_size,
            process_index: 0,
            // window: VecDeque::with_capacity(window_size),
            pack_sender: Some(sender),
            idx_sender: None,
            idx_entries: None,
            inner_offset: 12, // start  after 12 bytes pack header(signature + version + object count).
            inner_hash: HashAlgorithm::new(), // introduce different hash algorithm
            final_hash: None,
            start_encoding: false,
        }
    }

    pub fn new_with_idx(
        object_number: usize,
        window_size: usize,
        pack_sender: mpsc::Sender<Vec<u8>>,
        idx_sender: mpsc::Sender<Vec<u8>>,
    ) -> Self {
        PackEncoder {
            //path: Some(path),
            object_number,
            window_size,
            process_index: 0,
            // window: VecDeque::with_capacity(window_size),
            pack_sender: Some(pack_sender),
            idx_sender: Some(idx_sender),
            idx_entries: None,
            inner_offset: 12, // start  after 12 bytes pack header(signature + version + object count).
            inner_hash: HashAlgorithm::new(), // introduce different hash algorithm
            final_hash: None,
            start_encoding: false,
        }
    }

    pub fn drop_sender(&mut self) {
        self.pack_sender.take(); // Take the sender out, dropping it
    }

    pub async fn send_data(&mut self, data: Vec<u8>) {
        if let Some(sender) = &self.pack_sender {
            sender.send(data).await.unwrap();
        }
    }

    /// Get the hash of the pack file. if the pack file is not finished, return None
    pub fn get_hash(&self) -> Option<ObjectHash> {
        self.final_hash
    }

    /// Encodes entries into a pack file with delta objects and outputs them through the specified writer.
    /// # Arguments
    /// - `rx` - A receiver channel (`mpsc::Receiver<Entry>`) from which entries to be encoded are received.
    /// # Returns
    /// Returns `Ok(())` if encoding is successful, or a `GitError` in case of failure.
    /// - Returns a `GitError` if there is a failure during the encoding process.
    /// - Returns `PackEncodeError` if an encoding operation is already in progress.
    pub async fn encode(
        &mut self,
        entry_rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    ) -> Result<(), GitError> {
        //self.inner_encode(entry_rx, false).await
        if self.window_size == 0 {
            self.parallel_encode(entry_rx).await
        } else {
            self.inner_encode(entry_rx, false).await
        }
    }

    /// Encode with zstdelta
    pub async fn encode_with_zstdelta(
        &mut self,
        entry_rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    ) -> Result<(), GitError> {
        self.inner_encode(entry_rx, true).await
    }

    /// Delta selection heuristics are based on:
    ///   https://github.com/git/git/blob/master/Documentation/technical/pack-heuristics.adoc
    async fn inner_encode(
        &mut self,
        mut entry_rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
        enable_zstdelta: bool,
    ) -> Result<(), GitError> {
        let head = encode_header(self.object_number);
        self.send_data(head.clone()).await;
        self.inner_hash.update(&head);

        // ensure only one decode can only invoke once
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
                _ => {}
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

        // parallel encoding vec with different object_type
        let (commit_results, tree_results, blob_results, tag_results) = tokio::try_join!(
            tokio::task::spawn_blocking(move || {
                Self::try_as_offset_delta(
                    commits
                        .into_iter()
                        .map(|entry_with_meta| entry_with_meta.inner)
                        .collect(),
                    10,
                    enable_zstdelta,
                )
            }),
            tokio::task::spawn_blocking(move || {
                Self::try_as_offset_delta(
                    trees
                        .into_iter()
                        .map(|entry_with_meta| entry_with_meta.inner)
                        .collect(),
                    10,
                    enable_zstdelta,
                )
            }),
            tokio::task::spawn_blocking(move || {
                Self::try_as_offset_delta(
                    blobs
                        .into_iter()
                        .map(|entry_with_meta| entry_with_meta.inner)
                        .collect(),
                    10,
                    enable_zstdelta,
                )
            }),
            tokio::task::spawn_blocking(move || {
                Self::try_as_offset_delta(
                    tags.into_iter()
                        .map(|entry_with_meta| entry_with_meta.inner)
                        .collect(),
                    10,
                    enable_zstdelta,
                )
            }),
        )
        .map_err(|e| GitError::PackEncodeError(format!("Task join error: {e}")))?;

        let commit_res = commit_results?;
        let tree_res = tree_results?;
        let blob_res = blob_results?;
        let tag_res = tag_results?;

        let mut all_res = vec![commit_res, tree_res, blob_res, tag_res];

        let mut idx_entries = Vec::new();
        for res in &mut all_res {
            for data in res {
                data.1.offset = self.inner_offset as u64;
                self.write_all_and_update(&data.0).await;
                idx_entries.push(data.1.clone());
            }
        }

        self.idx_entries = Some(idx_entries);

        // Hash signature
        let hash_result = self.inner_hash.clone().finalize();
        self.final_hash = Some(ObjectHash::from_bytes(&hash_result).unwrap());
        self.send_data(hash_result.to_vec()).await;

        self.drop_sender();
        Ok(())
    }

    /// Try to encode as delta using objects in window
    /// delta & zstdelta have been gathered here
    /// Refs: https://sapling-scm.com/docs/dev/internals/zstdelta/
    /// the sliding window was moved here
    /// # Returns
    /// - Return (Vec<Vec<u8>) if success make delta
    /// - Return (None) if didn't delta,
    fn try_as_offset_delta(
        mut bucket: Vec<Entry>,
        window_size: usize,
        enable_zstdelta: bool,
    ) -> Result<Vec<(Vec<u8>, IndexEntry)>, GitError> {
        let mut current_offset = 0usize;
        let mut window: VecDeque<(Entry, usize)> = VecDeque::with_capacity(window_size);
        let mut res: Vec<(Vec<u8>, IndexEntry)> = Vec::new();
        //let mut idx_entries: Vec<IndexEntry> = Vec::new();

        for entry in bucket.iter_mut() {
            //let entry_for_window = entry.clone();
            // 每次循环重置最佳基对象选择
            let mut best_base: Option<&(Entry, usize)> = None;
            let mut best_rate: f64 = 0.0;
            let tie_epsilon: f64 = 0.15;

            let candidates: Vec<_> = window
                .par_iter()
                .with_min_len(3)
                .filter_map(|try_base| {
                    if try_base.0.obj_type != entry.obj_type {
                        return None;
                    }

                    if try_base.0.chain_len >= MAX_CHAIN_LEN {
                        return None;
                    }

                    if try_base.0.hash == entry.hash {
                        return None;
                    }

                    let sym_ratio = (try_base.0.data.len().min(entry.data.len()) as f64)
                        / (try_base.0.data.len().max(entry.data.len()) as f64);
                    if sym_ratio < 0.5 {
                        return None;
                    }

                    if !cheap_similar(&try_base.0.data, &entry.data) {
                        return None;
                    }

                    let rate = if (try_base.0.data.len() + entry.data.len()) / 2 > 64 {
                        delta::heuristic_encode_rate_parallel(&try_base.0.data, &entry.data)
                    } else {
                        delta::encode_rate(&try_base.0.data, &entry.data)
                        // let try_delta_obj = zstdelta::diff(&try_base.0.data, &entry.data).unwrap();
                        // 1.0 - try_delta_obj.len() as f64 / entry.data.len() as f64
                    };

                    if rate > MIN_DELTA_RATE {
                        Some((rate, try_base))
                    } else {
                        None
                    }
                })
                .collect();

            for (rate, try_base) in candidates {
                match best_base {
                    None => {
                        best_rate = rate;
                        //best_base_offset = current_offset - try_base.1;
                        best_base = Some(try_base);
                    }
                    Some(best_base_ref) => {
                        let is_better = if rate > best_rate + tie_epsilon {
                            true
                        } else if (rate - best_rate).abs() <= tie_epsilon {
                            try_base.0.chain_len > best_base_ref.0.chain_len
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

            let mut entry_for_window = entry.clone();

            let offset = best_base.map(|best_base| {
                let delta = if enable_zstdelta {
                    entry.obj_type = ObjectType::OffsetZstdelta;
                    zstdelta::diff(&best_base.0.data, &entry.data)
                        .map_err(|e| {
                            GitError::DeltaObjectError(format!("zstdelta diff failed: {e}"))
                        })
                        .unwrap()
                } else {
                    entry.obj_type = ObjectType::OffsetDelta;
                    delta::encode(&best_base.0.data, &entry.data)
                };
                //entry.obj_type = ObjectType::OffsetDelta;
                entry.data = delta;
                entry.chain_len = best_base.0.chain_len + 1;
                current_offset - best_base.1
            });

            entry_for_window.chain_len = entry.chain_len;
            let obj_data = encode_one_object(entry, offset)?;
            window.push_back((entry_for_window, current_offset));
            if window.len() > window_size {
                window.pop_front();
            }
            res.push((obj_data.clone(), IndexEntry::new(entry, 0)));
            current_offset += obj_data.len();
        }
        Ok(res)
    }

    /// Parallel encode with rayon, only works when window_size == 0 (no delta)
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

        // ensure only one decode can only invoke once
        if self.start_encoding {
            return Err(GitError::PackEncodeError(
                "encoding operation is already in progress".to_string(),
            ));
        }

        let mut idx_entries = Vec::new();
        let batch_size = usize::max(1000, entry_rx.max_capacity() / 10); // A temporary value, not optimized
        tracing::info!("encode with batch size: {}", batch_size);
        loop {
            let mut batch_entries = Vec::with_capacity(batch_size);
            time_it!("parallel encode: receive batch", {
                for _ in 0..batch_size {
                    match entry_rx.recv().await {
                        Some(entry) => {
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

            // use `collect` will return result in order, refs: https://github.com/rayon-rs/rayon/issues/551#issuecomment-371657900
            let batch_result: Vec<(Vec<u8>, IndexEntry)> =
                time_it!("parallel encode: encode batch", {
                    batch_entries
                        .par_iter()
                        .map(|entry| {
                            (
                                encode_one_object(entry, None).unwrap(),
                                IndexEntry::new(entry, 0),
                            )
                        })
                        .collect()
                });

            time_it!("parallel encode: write batch", {
                for mut obj_data in batch_result {
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

        // hash signature
        let hash_result = self.inner_hash.clone().finalize();
        self.final_hash = Some(ObjectHash::from_bytes(&hash_result).unwrap());
        self.send_data(hash_result.to_vec()).await;
        self.drop_sender();

        self.idx_entries = Some(idx_entries);
        Ok(())
    }

    /// Write data to writer and update hash & offset
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

    /// async version of encode, result data will be returned by JoinHandle.
    /// It will consume PackEncoder, so you can't use it after calling this function.
    /// when window_size = 0, it executes parallel_encode which retains stream transmission
    /// when window_size = 0,it executes encode which uses magic sort and delta.
    /// It seems that all other modules rely on this api
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

    /// async version of encode_with_zstdelta, result data will be returned by JoinHandle.
    pub async fn encode_async_with_zstdelta(
        mut self,
        rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    ) -> Result<JoinHandle<()>, GitError> {
        Ok(tokio::spawn(async move {
            // Do not use parallel encode with zstdelta because it make no sense.
            self.encode_with_zstdelta(rx).await.unwrap()
        }))
    }

    /// Generate idx file after pack file has been generated
    pub async fn encode_idx_file(&mut self) -> Result<(), GitError> {
        if self.idx_sender.is_none() {
            return Err(GitError::PackEncodeError(String::from(
                "idx sender is none",
            )));
        }
        self.generate_idx_file().await?;
        // drop sender so downstream consumer can finish
        self.idx_sender.take();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{env, io::Cursor, path::PathBuf, sync::Arc, time::Instant};

    use tempfile::tempdir;
    use tokio::sync::Mutex;

    use super::*;
    use crate::{
        hash::{HashKind, ObjectHash, set_hash_kind_for_test},
        internal::{
            object::blob::Blob,
            pack::{Pack, tests::init_logger, utils::read_offset_encoding},
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
        let max_pack_size = usize::try_from(max_pack_size_u64)
            .expect("pack size cap should fit in usize for this target");
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

    async fn get_entries_for_test() -> Arc<Mutex<Vec<Entry>>> {
        let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/packs/encode-test-sha1.pack");

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

        entries
    }
    async fn get_entries_for_test_sha256() -> Arc<Mutex<Vec<Entry>>> {
        let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/packs/encode-test-sha256.pack");

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

        entries
    }

    #[tokio::test]
    async fn test_pack_encoder_parallel_large_file() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        init_logger();

        let start = Instant::now();
        let entries = get_entries_for_test().await;
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
        let entries = get_entries_for_test_sha256().await;
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
        let entries = get_entries_for_test().await;
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
        let entries = get_entries_for_test_sha256().await;
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
        let entries = get_entries_for_test().await;
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
        let entries = get_entries_for_test_sha256().await;
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
        let entries = get_entries_for_test().await;
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
        let entries = get_entries_for_test_sha256().await;
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
        let entries = get_entries_for_test().await;
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
        let entries = get_entries_for_test().await;
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
