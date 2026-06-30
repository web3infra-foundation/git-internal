//! Sliding-window delta search for pack encoding.
//!
//! For each target entry, a bounded window of previously encoded objects is searched for a suitable
//! delta base. The window is ordered newest-to-oldest (backwards from the current position), which
//! works well with the locality produced by [`super::sort::magic_sort`].
//!
//! Two delta engines are available:
//!
//! | Feature flag      | Engine            | Characteristics                    |
//! |-------------------|-------------------|------------------------------------|
//! | `diff_rabin`      | Rabin fingerprint | Lazy indexed, scores every candidate |
//! | (default)         | Myers / Patience  | Similarity pre-filter → single delta |

use std::{collections::VecDeque, sync::Arc};

#[cfg(not(feature = "diff_rabin"))]
use rayon::prelude::*;

use super::header::encode_one_object;
#[cfg(feature = "diff_rabin")]
use super::sort::multi_point_similar;
#[cfg(not(feature = "diff_rabin"))]
use super::sort::{calc_hash, cheap_similar};
use crate::{
    delta,
    errors::GitError,
    internal::{
        object::types::ObjectType,
        pack::{entry::Entry, index_entry::IndexEntry},
    },
    zstdelta,
};

const MAX_CHAIN_LEN: usize = 50;
/// A delta must save at least half of the target payload to be selected.
const MIN_DELTA_RATE: f64 = 0.5;

/// One previously encoded object that may serve as a delta base.
///
/// `offset` is relative to the beginning of the current bucket, which is sufficient to compute the
/// backwards OFS_DELTA distance. With `diff_rabin`, the source index is created lazily and reused
/// while this entry remains in the window.
#[cfg(feature = "diff_rabin")]
pub(crate) struct DeltaWindowEntry {
    pub(crate) entry: Entry,
    pub(crate) offset: usize,
    /// Shared source bytes retained when the lazy Rabin index is built.
    pub(crate) data_arc: Option<Arc<[u8]>>,
    /// Reusable lookup index for evaluating multiple targets against this base.
    pub(crate) rabin_index: Option<delta::RabinDeltaIndex>,
}

/// Candidate base state when Rabin indexing is not compiled in.
#[cfg(not(feature = "diff_rabin"))]
pub(crate) struct DeltaWindowEntry {
    pub(crate) entry: Entry,
    pub(crate) offset: usize,
}

impl DeltaWindowEntry {
    pub(crate) fn new(entry: Entry, offset: usize) -> Self {
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

impl super::PackEncoder {
    /// Encode one ordered bucket while selecting bases from a backwards sliding window.
    ///
    /// For every target entry, this function:
    ///
    /// 1. Filters previous entries by type, chain depth, size, and optional similarity checks.
    /// 2. Scores the survivors and selects the most profitable base.
    /// 3. Replaces the target payload with a Git-compatible delta stream when savings exceed
    ///    [`MIN_DELTA_RATE`]; otherwise leaves it as a base object.
    /// 4. Calls [`encode_one_object`] to add the pack-entry header and zlib compression.
    /// 5. Adds the original target bytes to the window so later entries can use them as a base.
    ///
    /// The returned `IndexEntry` offsets are placeholders. `inner_encode` assigns absolute pack
    /// offsets when it merges bucket results into the final output order.
    #[cfg_attr(not(feature = "diff_rabin"), allow(unused_variables))]
    pub(super) fn try_as_offset_delta(
        mut bucket: Vec<Entry>,
        window_size: usize,
        enable_zstdelta: bool,
        enable_rabin: bool,
        disable_prefilter: bool,
    ) -> Result<Vec<(Vec<u8>, IndexEntry)>, GitError> {
        // Offsets are bucket-local here. Their differences remain valid OFS_DELTA distances when
        // the whole bucket is later placed at any absolute pack position.
        let mut current_offset = 0usize;
        let mut window: VecDeque<DeltaWindowEntry> = VecDeque::with_capacity(window_size);
        let mut res: Vec<(Vec<u8>, IndexEntry)> = Vec::with_capacity(bucket.len());

        for entry in bucket.iter_mut() {
            // best_rate is the estimated fraction of target bytes saved by delta encoding.
            let mut best_base: Option<&DeltaWindowEntry> = None;
            let mut best_rate: f64 = 0.0;

            // The Rabin path can cheaply score candidates by producing bounded deltas from cached
            // source indexes. The non-Rabin path estimates similarity first to avoid constructing
            // an expensive Myers/Patience delta for every object in the window.
            #[cfg(feature = "diff_rabin")]
            {
                let trg_size = entry.data.len();

                // Search newest-to-oldest. After locality-oriented sorting, recent entries are
                // usually the closest relatives of the current target.
                let pre_filtered_idxs: Vec<usize> = window
                    .iter()
                    .enumerate()
                    .rev() // Match Git's preference for more recent candidate bases.
                    .filter(|(_i, try_base)| {
                        let src_size = try_base.entry.data.len();
                        try_base.entry.obj_type == entry.obj_type
                            && try_base.entry.chain_len < MAX_CHAIN_LEN
                            && try_base.entry.hash != entry.hash
                            // Reject a source more than 32 times larger than the target.
                            && trg_size >= src_size / 32
                            // A large unavoidable size difference already consumes the delta budget.
                            && src_size.saturating_sub(trg_size.min(src_size))
                                < trg_size / 2
                            // Hashing both ends catches related files with changed headers.
                            && (disable_prefilter
                                || multi_point_similar(&try_base.entry.data, &entry.data))
                    })
                    .map(|(i, _)| i)
                    .collect();

                if !pre_filtered_idxs.is_empty() {
                    // Start with the largest delta worth accepting. Each improvement lowers the
                    // bound, allowing later candidates to abort as soon as they cannot win.
                    let max_delta_size = trg_size.saturating_sub(trg_size / 2 + 20);
                    let mut best_delta_size = max_delta_size;
                    let mut best_idx: Option<usize> = None;

                    // Score using actual encoded delta length rather than a similarity proxy.
                    for &idx in &pre_filtered_idxs {
                        // Build at most one Rabin index per base while it remains in the window.
                        // Very small sources cannot form a full rolling-hash window and are skipped.
                        if window[idx].rabin_index.is_none() {
                            // The index owns an Arc to source bytes. Retaining another Arc here
                            // avoids rebuilding or copying the source for subsequent targets.
                            let data_arc = match window[idx].data_arc.take() {
                                Some(arc) => arc,
                                None => {
                                    let arc: Arc<[u8]> = window[idx].entry.data.clone().into();
                                    arc
                                }
                            };
                            match delta::create_delta_index_arc(Arc::clone(&data_arc)) {
                                Some(new_index) => {
                                    window[idx].data_arc = Some(data_arc);
                                    window[idx].rabin_index = Some(new_index);
                                }
                                None => {
                                    // Preserve ownership even though this source was not indexable.
                                    window[idx].data_arc = Some(data_arc);
                                    continue;
                                }
                            }
                        }

                        // End the temporary index borrow before retaining the winning window entry.
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
                                best_rate = 1.0 - delta_size as f64 / entry.data.len() as f64;
                                best_idx = Some(idx);
                            }
                        }
                    }

                    best_base = best_idx.map(|i| &window[i]);
                }
            }

            // Myers/Patience deltas are expensive, so this path selects a base from similarity
            // estimates and constructs the real delta only for the winner.
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
                            || (!no_prefilter && !cheap_similar(&try_base.entry.data, &entry.data))
                        {
                            return None;
                        }
                        let rate = if (try_base.entry.data.len() + entry.data.len()) / 2 > 64 {
                            delta::heuristic_encode_rate_parallel(&try_base.entry.data, &entry.data)
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

            // Keep the object whole unless the selected base clears the minimum savings threshold.
            if best_rate < MIN_DELTA_RATE {
                best_base = None;
            }

            // Preserve the target's original content for future comparisons. `entry.data` may be
            // replaced below by its delta instruction stream.
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
                    // Reuse the winning base's cached Rabin index. Calling the non-indexed helper
                    // here would rebuild the same source index for the final delta.
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
                // OFS_DELTA stores a positive backwards distance, not an absolute base offset.
                current_offset - best_base.offset
            });

            // A future child of this target must observe the updated chain depth even though its
            // window copy retains the original, fully reconstructed bytes.
            entry_for_window.chain_len = entry.chain_len;
            let obj_data = encode_one_object(entry, offset)?;
            window.push_back(DeltaWindowEntry::new(entry_for_window, current_offset));
            // Evict the oldest base once it falls outside the candidate window.
            if window.len() > window_size {
                window.pop_front();
            }
            let obj_data_len = obj_data.len();
            res.push((obj_data, IndexEntry::new(entry, 0)));
            current_offset += obj_data_len;
        }
        Ok(res)
    }
}
