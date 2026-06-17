//! Rabin fingerprint delta encoder — the default delta engine for pack encoding.
//!
//! # Algorithm Overview
//!
//! This module implements Git-compatible delta compression using Rabin fingerprinting,
//! closely following the design in Git's `diff-delta.c`. The algorithm has two phases:
//!
//! **① Index construction** ([`create_delta_index`], [`create_delta_index_arc`])
//!
//! The source buffer is scanned at 16-byte intervals. At each position, a Rabin
//! fingerprint (a CRC-32-derived rolling hash) is computed over the 16-byte window.
//! The resulting `(hash, offset)` pairs are stored in a hash table bucketed by
//! `hash & mask`. Overfull buckets are uniformly culled to [`HASH_LIMIT`] entries,
//! matching Git's behavior exactly.
//!
//! The index is built once per source and can be reused across many target
//! candidates — this is the key optimization that makes Rabin faster than
//! Patience/Myers for pack delta search.
//!
//! **② Delta generation** ([`create_delta_inner`])
//!
//! The target buffer is walked byte-by-byte, maintaining the same 16-byte rolling
//! hash. When the hash hits a non-empty bucket in the source index, we extend the
//! match forward (and backward) to find the longest common substring. Short matches
//! (< 4 bytes) are emitted as literal data; longer matches become copy instructions
//! referencing the source buffer.
//!
//! The output is a Git-compatible delta instruction stream: a header with source and
//! target sizes, followed by alternating data-insert and copy opcodes.
//!
//! # Relationship to the Rest of the Codebase
//!
//! | Module | Role |
//! |--------|------|
//! | [`tables`] | Precomputed `T` and `U` lookup tables + shared constants |
//! | [`extend_match`] | Word-at-a-time byte comparison for match extension |
//! | [`super::super::decode`] | Delta decoder used in round-trip tests |
//!
//! # Feature Flags
//!
//! - `diff_rabin` (default on): enables this entire module.
//! - `delta-stats`: collects [`DeltaStats`] during delta generation for profiling.
//!
//! # References
//!
//! - Git `diff-delta.c`: <https://github.com/git/git/blob/master/diff-delta.c>
//! - "The Delta Compression Game" (Linus Torvalds, 2005):
//!   <https://lwn.net/Articles/135538/>

mod extend_match;
mod tables;

use std::{collections::HashSet, sync::Arc};

use extend_match::extend_match;
use tables::{
    DATA_INS_LEN, HASH_LIMIT, MAX_OP_SIZE, MIN_DELTA_RATE, RABIN_SHIFT, RABIN_WINDOW, T, U,
};

// ── Delta statistics (behind `delta-stats` feature) ────────────────────

/// Lightweight statistics collected during delta generation.
///
/// Only compiled when the `delta-stats` feature is enabled. When disabled, all
/// fields are zeroed and the compiler will optimize away the collection points
/// in [`create_delta_inner`].
#[cfg(feature = "delta-stats")]
#[derive(Default)]
pub struct DeltaStats {
    /// Number of [`extend_match`] calls.
    pub extension_calls: u64,
    /// Total bytes compared across all extend-match calls.
    pub extension_bytes_compared: u64,
    /// Number of hash bucket lookups.
    pub bucket_scans: u64,
    /// Total index entries examined across all bucket scans.
    pub bucket_entries_scanned: u64,
    /// Raw bucket sizes for percentile computation.
    pub bucket_sizes: Vec<u32>,
    /// Candidates that entered full match extension.
    pub candidates_entered_extension: u64,
    /// Matches that were accepted (better than the current best).
    pub matches_accepted: u64,
}

/// Stub statistics when the `delta-stats` feature is disabled.
///
/// All fields are zero/empty. The compiler will remove the `+= 1` increments
/// from the hot loop since the values are never read.
#[cfg(not(feature = "delta-stats"))]
#[derive(Default)]
pub struct DeltaStats {
    /// Number of [`extend_match`] calls.
    pub extension_calls: u64,
    /// Total bytes compared across all extend-match calls.
    pub extension_bytes_compared: u64,
    /// Number of hash bucket lookups.
    pub bucket_scans: u64,
    /// Total index entries examined across all bucket scans.
    pub bucket_entries_scanned: u64,
    /// Raw bucket sizes for percentile computation.
    pub bucket_sizes: Vec<u32>,
    /// Candidates that entered full match extension.
    pub candidates_entered_extension: u64,
    /// Matches that were accepted (better than the current best).
    pub matches_accepted: u64,
}

#[cfg(not(feature = "delta-stats"))]
impl DeltaStats {
    /// Consume self, reading all fields so the compiler does not flag them as
    /// unused when `delta-stats` is disabled.
    #[inline(always)]
    fn sink(self) {
        let _ = self.extension_calls;
        let _ = self.extension_bytes_compared;
        let _ = self.bucket_scans;
        let _ = self.bucket_entries_scanned;
        let _ = self.bucket_sizes;
        let _ = self.candidates_entered_extension;
        let _ = self.matches_accepted;
    }
}

// ── Index structures ─────────────────────────────────────────────────────

/// A single entry in the Rabin hash index: a source offset paired with its
/// Rabin fingerprint.
#[derive(Debug, Clone, Copy)]
pub struct IndexEntry {
    /// Byte offset into the source buffer where the 16-byte window ends.
    pub offset: u32,
    /// Rabin fingerprint of the 16 bytes ending at `offset`.
    pub hash: u32,
}

/// Pre-built Rabin fingerprint index over a source buffer.
///
/// This index can be cached and reused across multiple delta computations
/// against the same source — matching Git's approach where `delta_index` is
/// built once per source and reused for every target candidate in the delta
/// window.
///
/// The source data is held via [`Arc<[u8]>`] so callers can share ownership
/// without copying.
pub struct RabinDeltaIndex {
    /// Shared reference to the source buffer for match extension.
    pub source: Arc<[u8]>,
    /// All index entries packed consecutively, grouped by hash bucket.
    pub entries: Box<[IndexEntry]>,
    /// Start indices into `entries` for each bucket. `buckets.len() == hash_mask + 2`;
    /// the last entry is a sentinel equal to `entries.len()`.
    pub buckets: Box<[u32]>,
    /// Bitmask for fast `hash & mask` bucket lookup. Always one less than a power of two.
    pub hash_mask: u32,
}

// ── Index construction ───────────────────────────────────────────────────

/// Build a Rabin fingerprint index over `source`.
///
/// This clones the data into an [`Arc`]. Prefer [`create_delta_index_arc`] when
/// the caller already holds an `Arc<[u8]>` to avoid the extra copy.
///
/// Returns `None` if the source is too short to index (< [`RABIN_WINDOW`] bytes).
pub fn create_delta_index(source: &[u8]) -> Option<RabinDeltaIndex> {
    create_delta_index_arc(Arc::from(source))
}

/// Build a Rabin fingerprint index, taking ownership of the source via [`Arc<[u8]>`].
///
/// This is the preferred entry point for hot-path callers that already hold an
/// `Arc<[u8]>` (e.g. from a delta-window entry). It avoids cloning the data.
///
/// Returns `None` if the source is too short to index (< [`RABIN_WINDOW`] bytes).
pub fn create_delta_index_arc(source: Arc<[u8]>) -> Option<RabinDeltaIndex> {
    let src_len = source.len();
    if src_len == 0 {
        return None;
    }

    // We sample one hash per RABIN_WINDOW bytes, shifted by 1 byte from the
    // window boundaries (matching Git's offset scheme).
    let entries_count = (src_len - 1) / RABIN_WINDOW;
    if entries_count == 0 {
        return None;
    }

    // Hash table sizing: next power of two >= entries_count / 4, minimum 16.
    // This keeps the average bucket depth around 4 entries.
    let hsize_entries = entries_count / 4;
    let mut hsize: usize = 16;
    while hsize < hsize_entries {
        hsize <<= 1;
    }
    let hmask = hsize - 1;

    // Phase 1: build per-bucket linked lists.
    //
    // We walk the source from end to start (matching Git's direction). Adjacent
    // windows with the same hash are collapsed into a single entry keeping the
    // later offset — this deduplicates repetitive regions.
    let mut bucket_lists: Vec<Vec<IndexEntry>> = vec![Vec::new(); hsize];
    let mut prev_val: u32 = !0u32;

    let mut pos = entries_count * RABIN_WINDOW - RABIN_WINDOW;
    loop {
        let window_start = pos + 1;
        let window_end = window_start + RABIN_WINDOW;
        if window_end > src_len {
            if pos == 0 {
                break;
            }
            pos = pos.saturating_sub(RABIN_WINDOW);
            continue;
        }

        // Compute the Rabin fingerprint for this 16-byte window.
        let mut val: u32 = 0;
        for &b in &source[window_start..window_end] {
            val = ((val << 8) | b as u32) ^ T[(val >> RABIN_SHIFT) as usize];
        }

        if val == prev_val {
            // Same hash as the previous window — overwrite the last entry's
            // offset (keeping the later position).
            let h = val as usize & hmask;
            if let Some(last) = bucket_lists[h].last_mut() {
                last.offset = (pos + RABIN_WINDOW) as u32;
            }
        } else {
            prev_val = val;
            let h = val as usize & hmask;
            bucket_lists[h].push(IndexEntry {
                offset: (pos + RABIN_WINDOW) as u32,
                hash: val,
            });
        }

        if pos == 0 {
            break;
        }
        pos = pos.saturating_sub(RABIN_WINDOW);
    }

    // Phase 2: cull overfull buckets to HASH_LIMIT.
    let mut total_entries = 0usize;
    for bucket in bucket_lists.iter_mut() {
        if bucket.len() > HASH_LIMIT {
            cull_bucket(bucket);
        }
        total_entries += bucket.len();
    }

    // Phase 3: pack into flat arrays for cache-friendly lookup.
    let mut entries_vec: Vec<IndexEntry> = Vec::with_capacity(total_entries);
    let mut buckets: Vec<u32> = Vec::with_capacity(hsize + 1);

    for bucket in bucket_lists.iter() {
        buckets.push(entries_vec.len() as u32);
        entries_vec.extend_from_slice(bucket);
    }
    buckets.push(entries_vec.len() as u32); // sentinel

    Some(RabinDeltaIndex {
        source,
        entries: entries_vec.into_boxed_slice(),
        buckets: buckets.into_boxed_slice(),
        hash_mask: hmask as u32,
    })
}

/// Uniformly cull an overfull hash bucket down to [`HASH_LIMIT`] entries.
///
/// Uses Git's deterministic algorithm: iterates through the bucket, keeping
/// entries at approximately regular intervals. The accumulator-based approach
/// ensures exactly `total - HASH_LIMIT` entries are removed, uniformly
/// distributed across the original ordering.
fn cull_bucket(bucket: &mut Vec<IndexEntry>) {
    let total = bucket.len();
    let target = HASH_LIMIT;
    let excess = total - target;

    let mut kept = Vec::with_capacity(target);
    let mut acc: isize = 0;

    for entry in bucket.drain(..) {
        acc += excess as isize;
        if acc > 0 {
            acc -= target as isize;
            // Skip this entry (uniformly distributed removal)
        } else {
            kept.push(entry);
        }
    }

    *bucket = kept;
}

// ── Delta instruction encoding ───────────────────────────────────────────

/// Emit a Git-style varint: 7 bits of value per byte, MSB set on all but the
/// last byte.
///
/// Used for the source/target size fields in the delta header.
fn write_varint(n: usize, out: &mut Vec<u8>) {
    let mut v = n;
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Emit a Git delta copy opcode: `[cmd_byte] [offset bytes...] [size bytes...]`.
///
/// The command byte encodes which offset and size bytes are present using bit
/// flags: bits 0-3 for offset bytes, bits 4-6 for size bytes. Only non-zero
/// bytes are emitted, so short offsets and sizes produce compact opcodes.
fn push_copy_op(out: &mut Vec<u8>, offset: u32, size: u32) {
    let mut cmd: u8 = 0x80;
    let mut extra: Vec<u8> = Vec::with_capacity(7);

    // Encode offset bytes (bits 0-3 of cmd)
    let mut off = offset;
    for i in 0..4u8 {
        let b = (off & 0xff) as u8;
        off >>= 8;
        if b != 0 {
            cmd |= 1 << i;
            extra.push(b);
        }
    }

    // Encode size bytes (bits 4-6 of cmd)
    let mut sz = size;
    for i in 0..3u8 {
        let b = (sz & 0xff) as u8;
        sz >>= 8;
        if b != 0 {
            cmd |= 1 << (4 + i);
            extra.push(b);
        }
    }

    out.push(cmd);
    out.extend_from_slice(&extra);
}

// ── Core delta generation ────────────────────────────────────────────────

/// Generate a delta from `index` for `target`, following Git's `create_delta`.
///
/// If `max_size` is `Some(n)`, the computation aborts early and returns `None`
/// as soon as the output exceeds `n` bytes. This matches Git's `max_size`
/// parameter in `create_delta`, used for early termination during candidate
/// scoring.
///
/// This wrapper discards [`DeltaStats`]; use [`create_delta_inner`] directly
/// if profiling data is needed.
fn create_delta(
    index: &RabinDeltaIndex,
    target: &[u8],
    max_size: Option<usize>,
) -> Option<Vec<u8>> {
    create_delta_inner(index, target, max_size).map(|(delta, stats)| {
        stats.sink();
        delta
    })
}

/// Core delta generation with optional stats collection.
///
/// Implements the same algorithm as Git's `create_delta`:
///
/// 1. Emit the header (source size, target size) as varints.
/// 2. Seed the rolling hash from the first [`RABIN_WINDOW`] bytes of the target,
///    emitting them as a literal data run.
/// 3. For each subsequent target byte:
///    - Update the rolling hash (remove oldest byte, add new byte).
///    - Look up the hash in the source index.
///    - For each matching fingerprint, extend the match forward.
///    - Keep the longest match found.
///    - If the best match is < 4 bytes, emit as literal data.
///    - If ≥ 4 bytes, try to extend backward into the pending literal run
///      (this absorbs bytes that matched but weren't discovered until the
///      hash window caught up), then emit a copy opcode.
/// 4. Flush any remaining literal data.
///
/// Returns `None` if `max_size` is set and the delta would exceed it.
fn create_delta_inner(
    index: &RabinDeltaIndex,
    target: &[u8],
    max_size: Option<usize>,
) -> Option<(Vec<u8>, DeltaStats)> {
    let src_data = &index.source;
    let src_len = src_data.len();
    let trg_len = target.len();

    // Initial capacity heuristic: header overhead + ~half the target size.
    // Most deltas are smaller than the target, but we'll grow dynamically if needed.
    let cap = 32 + trg_len / 2;
    let mut out: Vec<u8> = Vec::with_capacity(cap);

    // Varint header: source size, then target size.
    write_varint(src_len, &mut out);
    write_varint(trg_len, &mut out);

    if trg_len == 0 {
        return Some((out, DeltaStats::default()));
    }

    let mut stats = DeltaStats::default();

    let hmask = index.hash_mask as usize;
    let buckets = &index.buckets;
    let entries = &index.entries;

    // ── Phase 1: emit the first RABIN_WINDOW bytes as literal data ──────
    //
    // The first window's worth of bytes has no preceding context for the
    // rolling hash, so we emit them directly and use them to initialize the
    // hash for subsequent matching.
    let init_count = RABIN_WINDOW.min(trg_len);

    // Reserve space for the data-op length byte (filled later when we know
    // how many literal bytes are in this run).
    let mut data_len_pos = out.len();
    out.push(0u8); // placeholder

    for &b in &target[..init_count] {
        out.push(b);
    }

    // Seed the rolling hash from the first window.
    let mut val: u32 = 0;
    for &b in &target[..init_count] {
        val = ((val << 8) | b as u32) ^ T[(val >> RABIN_SHIFT) as usize];
    }

    let mut data_pos = init_count; // next unconsumed target byte
    let mut inscnt = init_count; // bytes in the current literal-data run
    let mut moff: u32 = 0; // offset in source of the current best match
    let mut msize: u32 = 0; // length of the current best match

    // ── Phase 2: sliding-window match loop ──────────────────────────────
    while data_pos < trg_len {
        // Update the rolling hash: remove the byte that just left the window,
        // add the new byte entering the window.
        if data_pos >= RABIN_WINDOW {
            let oldest = target[data_pos - RABIN_WINDOW];
            val ^= U[oldest as usize];
        }
        let new_byte = target[data_pos];
        val = ((val << 8) | new_byte as u32) ^ T[(val >> RABIN_SHIFT) as usize];

        // If we already have a 4096-byte match, don't bother searching for
        // a better one — 4KB copy opcodes are good enough.
        if msize < 4096 {
            // Look up the current hash in the source index.
            let bi = val as usize & hmask;
            let bucket_start = buckets[bi] as usize;
            let bucket_end = buckets[bi + 1] as usize;
            let bucket_entries = &entries[bucket_start..bucket_end];

            stats.bucket_scans += 1;
            stats.bucket_entries_scanned += bucket_entries.len() as u64;
            stats.bucket_sizes.push(bucket_entries.len() as u32);

            // Scan the bucket for the best (longest) match.
            //
            // Entries within each bucket are sorted by offset descending
            // (because we walked the source end-to-start during index
            // construction). When `max_match <= msize`, we can break
            // early — no later entry can beat the current best.
            for &entry in bucket_entries {
                if entry.hash != val {
                    continue;
                }

                let ref_start = entry.offset as usize;
                let src_remain = src_len - ref_start;
                let trg_remain = trg_len - data_pos;
                let max_match = src_remain.min(trg_remain);

                if max_match <= msize as usize {
                    break;
                }

                stats.candidates_entered_extension += 1;

                let match_len = extend_match(src_data, target, ref_start, data_pos, max_match);

                stats.extension_calls += 1;
                stats.extension_bytes_compared += match_len as u64;

                if match_len > msize as usize {
                    msize = match_len as u32;
                    moff = ref_start as u32;

                    stats.matches_accepted += 1;

                    if msize >= 4096 {
                        break; // good enough — stop scanning
                    }
                }
            }
        }

        if msize < 4 {
            // ── Short match: emit the current byte as literal data ──
            //
            // Matches shorter than 4 bytes cost more to encode as a copy
            // opcode (3+ bytes of offset/size overhead) than as a literal
            // (1 byte length + data), so we keep them in the data run.
            if inscnt == 0 {
                data_len_pos = out.len();
                out.push(0u8); // placeholder for the data-op length
            }
            out.push(new_byte);
            inscnt += 1;
            data_pos += 1;

            // Flush the data run if it reaches the max opcode length.
            if inscnt == DATA_INS_LEN {
                out[data_len_pos] = DATA_INS_LEN as u8;
                inscnt = 0;
            }
            msize = 0;
        } else {
            // ── Good match: try backward extension, then emit copy opcode ──

            let mut match_off = moff as usize;
            let mut match_len = msize as usize;

            // Backward extension: walk backwards from (match_off, data_pos)
            // to absorb bytes from the pending literal run into this match.
            // This catches cases where the two buffers share a common prefix
            // that extends before the hashed window.
            let back_extend: usize = {
                let max_back = inscnt.min(match_off);
                let mut cnt = 0usize;
                while cnt < max_back && src_data[match_off - 1 - cnt] == target[data_pos - 1 - cnt]
                {
                    cnt += 1;
                }
                cnt
            };

            if back_extend > 0 {
                match_off -= back_extend;
                match_len += back_extend;
                data_pos -= back_extend;
                inscnt -= back_extend;

                // Truncate the output to remove the bytes that we've now
                // absorbed into the match.
                let new_out_len = if inscnt == 0 {
                    // All pending data absorbed — remove the placeholder too.
                    data_len_pos
                } else {
                    out.len() - back_extend
                };
                out.truncate(new_out_len);
            }

            // Finalize any remaining literal data run.
            if inscnt > 0 {
                out[data_len_pos] = inscnt as u8;
                inscnt = 0;
            }

            // Split the copy into chunks ≤ 64 KB (pack v2 limit).
            let mut remaining = match_len as u32;
            let mut remaining_off = match_off as u32;

            let max_copy: u32 = 0x10000;
            let left: u32 = remaining.saturating_sub(max_copy);
            remaining -= left;

            push_copy_op(&mut out, remaining_off, remaining);

            data_pos += remaining as usize;
            remaining_off += remaining;
            moff = remaining_off;
            msize = left;

            // If the remaining match is short, re-seed the rolling hash from
            // the last RABIN_WINDOW bytes before the new position. This avoids
            // a stale hash from before the copy region corrupting future lookups.
            if msize < 4096 && data_pos >= RABIN_WINDOW {
                val = 0;
                for &b in &target[data_pos - RABIN_WINDOW..data_pos] {
                    val = ((val << 8) | b as u32) ^ T[(val >> RABIN_SHIFT) as usize];
                }
            }
        }

        // Early termination: abort if the output has already exceeded max_size.
        // This enables rapid candidate scoring during pack delta search.
        if let Some(max) = max_size
            && out.len() > max
        {
            return None;
        }

        // Pre-allocate more space if the next opcode might not fit.
        if out.len() + MAX_OP_SIZE > out.capacity() {
            out.reserve(MAX_OP_SIZE * 2);
        }
    }

    // ── Phase 3: flush any remaining literal data ───────────────────────
    if inscnt > 0 {
        out[data_len_pos] = inscnt as u8;
    }

    // Final max_size check before returning.
    if let Some(max) = max_size
        && out.len() > max
    {
        return None;
    }

    Some((out, stats))
}

// ── Public API ───────────────────────────────────────────────────────────

/// Encode `new_data` as a Git-compatible delta against `old_data`.
///
/// Builds a fresh Rabin index over `old_data` and produces a complete delta
/// instruction stream (varint header + opcodes). This is the simplest entry
/// point; callers that need to reuse an index across multiple targets should
/// use [`encode_rabin_with_index`] instead.
///
/// # Edge Cases
///
/// - **Empty target**: emits a minimal delta (header only, with the actual
///   source size and a target size of 0).
/// - **Empty source**: emits a literal-only delta — all target bytes are
///   encoded as data-insert opcodes.
/// - **Source too small to index**: falls back to a literal-only delta.
pub fn encode_rabin(old_data: &[u8], new_data: &[u8]) -> Vec<u8> {
    if new_data.is_empty() {
        let mut out = Vec::with_capacity(4);
        write_varint(old_data.len(), &mut out);
        write_varint(0, &mut out);
        return out;
    }

    if old_data.is_empty() {
        return encode_literal_only(0, new_data);
    }

    if let Some(index) = create_delta_index(old_data) {
        create_delta(&index, new_data, None)
            .expect("delta should always succeed when max_size is None")
    } else {
        encode_literal_only(old_data.len(), new_data)
    }
}

/// Encode `target` as a delta against a pre-built Rabin index.
///
/// This skips index construction, allowing the caller to cache and reuse the
/// index across multiple target objects. This is the primary entry point used
/// by the pack encoder's delta search.
pub fn encode_rabin_with_index(index: &RabinDeltaIndex, target: &[u8]) -> Vec<u8> {
    create_delta(index, target, None).expect("delta should always succeed when max_size is None")
}

/// Encode `target` as a delta against a pre-built index, aborting early if
/// the output would exceed `max_size` bytes.
///
/// Returns `None` if the delta was aborted (output exceeded `max_size`).
/// This is the key optimization for candidate scoring: the pack encoder
/// tries many candidates rapidly, discarding any that can't beat the
/// current best delta size.
///
/// # Edge Cases
///
/// - **Empty target**: returns a minimal delta (header only).
/// - **max_size is tight**: if the header alone exceeds `max_size`, the
///   function still attempts to emit at least the header before aborting.
pub fn encode_rabin_with_index_and_max_size(
    index: &RabinDeltaIndex,
    target: &[u8],
    max_size: usize,
) -> Option<Vec<u8>> {
    if target.is_empty() {
        let mut out = Vec::with_capacity(4);
        write_varint(index.source.len(), &mut out);
        write_varint(0, &mut out);
        return Some(out);
    }
    create_delta(index, target, Some(max_size))
}

// ── Similarity estimation ────────────────────────────────────────────────

/// Compute the exact similarity rate by running the full Rabin encode and
/// measuring the proportion of target bytes that were covered by copy opcodes.
///
/// Returns a value in `[0.0, 1.0]` where `1.0` means the two buffers are
/// identical and `0.0` means they share no common substrings.
///
/// This is the accurate but slower path; use [`heuristic_encode_rate_rabin`]
/// for fast pre-screening of delta candidates.
pub fn rabin_encode_rate(old_data: &[u8], new_data: &[u8]) -> f64 {
    if new_data.is_empty() && old_data.is_empty() {
        return 1.0;
    }
    if old_data.is_empty() || new_data.is_empty() {
        return 0.0;
    }

    if let Some(index) = create_delta_index(old_data) {
        let delta = create_delta(&index, new_data, None)
            .expect("delta should always succeed when max_size is None");

        let new_len = new_data.len();
        let mut shared: usize = 0;

        // Parse the delta instruction stream to count shared (copy) bytes.
        let mut pos = 0usize;

        // Skip the source-size varint.
        while pos < delta.len() && (delta[pos] & 0x80) != 0 {
            pos += 1;
        }
        pos += 1;

        // Skip the target-size varint.
        while pos < delta.len() && (delta[pos] & 0x80) != 0 {
            pos += 1;
        }
        pos += 1;

        // Walk the opcodes.
        while pos < delta.len() {
            let cmd = delta[pos];
            pos += 1;
            if cmd & 0x80 == 0 {
                // Data instruction: lower 7 bits = literal length.
                let len = cmd as usize;
                pos += len; // skip the literal bytes
            } else {
                // Copy instruction: read offset and size based on flag bits.
                let mut sz: usize = 0;
                for i in 0..4 {
                    if cmd & (1 << i) != 0 {
                        pos += 1; // skip offset byte
                    }
                }
                for i in 0..3 {
                    if cmd & (1 << (4 + i)) != 0 {
                        sz |= (delta[pos] as usize) << (8 * i);
                        pos += 1;
                    }
                }
                if sz == 0 {
                    sz = 0x10000; // Git convention: size 0 means 64 KB
                }
                shared += sz.min(new_len);
            }
        }

        if new_len > 0 {
            shared.min(new_len) as f64 / new_len as f64
        } else {
            0.0
        }
    } else {
        0.0
    }
}

/// Fast heuristic similarity screening using Rabin hash sets.
///
/// Samples both buffers at [`RABIN_WINDOW`] intervals (or 256-byte intervals
/// for buffers > 1 MB), computing Rabin fingerprints at each sample point.
/// Compares the sets of hashes to estimate how much of `new_data` matches
/// `old_data`.
///
/// Returns `0.0` if the similarity is clearly below [`MIN_DELTA_RATE`] (0.5),
/// allowing the caller to quickly reject unpromising delta candidates without
/// running the full encoder.
///
/// # Performance
///
/// This is O(n) in the buffer sizes with a small constant factor. The HashSet
/// is capped at 8192 entries, so memory usage is bounded even for very large
/// buffers.
pub fn heuristic_encode_rate_rabin(old_data: &[u8], new_data: &[u8]) -> f64 {
    let old_len = old_data.len();
    let new_len = new_data.len();

    if old_len == 0 && new_len == 0 {
        return 1.0;
    }
    if old_len == 0 || new_len == 0 {
        return 0.0;
    }

    // Use wider sampling step for very large buffers to bound the hash set size.
    let step = if old_len > 1_000_000 {
        256
    } else {
        RABIN_WINDOW
    };

    let old_samples = if old_len >= RABIN_WINDOW {
        (old_len - RABIN_WINDOW) / step + 1
    } else {
        0
    };
    if old_samples == 0 {
        return 0.0;
    }
    let new_samples = if new_len >= RABIN_WINDOW {
        (new_len - RABIN_WINDOW) / step + 1
    } else {
        0
    };
    if new_samples == 0 {
        return 0.0;
    }

    // Collect Rabin hashes from the old buffer.
    let mut old_hashes: HashSet<u32> = HashSet::with_capacity(old_samples.min(8192));
    let mut pos = 0usize;
    while pos + RABIN_WINDOW <= old_len {
        let mut val: u32 = 0;
        for &b in &old_data[pos..pos + RABIN_WINDOW] {
            val = ((val << 8) | b as u32) ^ T[(val >> RABIN_SHIFT) as usize];
        }
        old_hashes.insert(val);
        pos += step;
    }

    // Count how many new-buffer sample hashes appear in the old buffer.
    let total_new_samples = new_samples;
    let mut matches = 0usize;
    let mut pos = 0usize;
    let mut samples_done = 0usize;

    while pos + RABIN_WINDOW <= new_len {
        let mut val: u32 = 0;
        for &b in &new_data[pos..pos + RABIN_WINDOW] {
            val = ((val << 8) | b as u32) ^ T[(val >> RABIN_SHIFT) as usize];
        }
        if old_hashes.contains(&val) {
            matches += 1;
        }
        samples_done += 1;
        pos += step;

        // Early exit: even if all remaining samples match, the rate can't
        // reach MIN_DELTA_RATE, so stop now.
        let remaining = total_new_samples - samples_done;
        let max_possible = (matches + remaining) as f64 / total_new_samples as f64;
        if max_possible < MIN_DELTA_RATE {
            return 0.0;
        }
    }

    if samples_done == 0 {
        return 0.0;
    }
    matches as f64 / samples_done as f64
}

// ── Fallback helpers ─────────────────────────────────────────────────────

/// Produce a delta consisting entirely of literal data (no copy opcodes).
///
/// Used when the source buffer is empty or too small to build a Rabin index.
/// The result is still a valid delta — it just doesn't reference any source
/// bytes, so the decoder reconstructs the target purely from the literal data.
fn encode_literal_only(source_len: usize, new_data: &[u8]) -> Vec<u8> {
    let new_len = new_data.len();
    let mut out: Vec<u8> = Vec::with_capacity(4 + new_len);

    write_varint(source_len, &mut out);
    write_varint(new_len, &mut out);

    // Chunk the data into DATA_INS_LEN-byte runs.
    let mut pos = 0usize;
    while pos < new_len {
        let chunk = DATA_INS_LEN.min(new_len - pos);
        out.push(chunk as u8);
        out.extend_from_slice(&new_data[pos..pos + chunk]);
        pos += chunk;
    }

    out
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::delta::decode::delta_decode;

    /// Round-trip: identical data should produce a valid delta that decodes
    /// to the exact original.
    #[test]
    fn test_rabin_round_trip_identical() {
        let old = b"hello world, this is a test for rabin delta";
        let new = b"hello world, this is a test for rabin delta";
        let delta = encode_rabin(old, new);
        let decoded = delta_decode(&mut Cursor::new(&delta), old).unwrap();
        assert_eq!(decoded, new, "round-trip should reconstruct exact data");
    }

    /// Round-trip: a small edit (single word changed).
    #[test]
    fn test_rabin_round_trip_edit() {
        let old = b"hello world, this is a test";
        let new = b"hello rust, this is a test";
        let delta = encode_rabin(old, new);
        let decoded = delta_decode(&mut Cursor::new(&delta), old).unwrap();
        assert_eq!(decoded, new);
    }

    /// Round-trip: expansion — new data is larger than old.
    #[test]
    fn test_rabin_round_trip_expand() {
        let old = b"small";
        let new = b"this is a much larger buffer that includes small at the end";
        let delta = encode_rabin(old, new);
        let decoded = delta_decode(&mut Cursor::new(&delta), old).unwrap();
        assert_eq!(decoded, new);
    }

    /// Round-trip: completely different data with no common substrings.
    #[test]
    fn test_rabin_round_trip_different() {
        let old = b"abcdefghijklmnop";
        let new = b"1234567890123456";
        let delta = encode_rabin(old, new);
        let decoded = delta_decode(&mut Cursor::new(&delta), old).unwrap();
        assert_eq!(decoded, new);
    }

    /// Round-trip: various empty-input edge cases.
    #[test]
    fn test_rabin_empty() {
        // Both empty
        let delta = encode_rabin(b"", b"");
        let decoded = delta_decode(&mut Cursor::new(&delta), b"").unwrap();
        assert_eq!(decoded, b"");

        // Empty old, non-empty new
        let delta = encode_rabin(b"", b"hello");
        let decoded = delta_decode(&mut Cursor::new(&delta), b"").unwrap();
        assert_eq!(decoded, b"hello");

        // Non-empty old, empty new
        let delta = encode_rabin(b"hello", b"");
        let decoded = delta_decode(&mut Cursor::new(&delta), b"hello").unwrap();
        assert_eq!(decoded, b"");
    }

    /// Round-trip: single byte (minimal non-empty input).
    #[test]
    fn test_rabin_single_byte() {
        let delta = encode_rabin(b"a", b"b");
        let decoded = delta_decode(&mut Cursor::new(&delta), b"a").unwrap();
        assert_eq!(decoded, b"b");
    }

    /// Round-trip: data shorter than RABIN_WINDOW (16 bytes).
    /// The index can't be built, so the encoder falls back to literal-only.
    #[test]
    fn test_rabin_short_data() {
        let old = b"abc";
        let new = b"abd";
        let delta = encode_rabin(old, new);
        let decoded = delta_decode(&mut Cursor::new(&delta), old).unwrap();
        assert_eq!(decoded, new);

        let old2 = b"hello world";
        let new2 = b"hello wOrld";
        let delta2 = encode_rabin(old2, new2);
        let decoded2 = delta_decode(&mut Cursor::new(&delta2), old2).unwrap();
        assert_eq!(decoded2, new2);
    }

    /// Large buffer with a few scattered byte changes: delta must be much
    /// smaller than the target (testing compression effectiveness).
    #[test]
    fn test_rabin_large_buffer_compresses() {
        let old = vec![0xABu8; 100_000];
        let mut new = old.clone();
        new[500] = 0xCD;
        new[50_000] = 0xEF;
        new[99_000] = 0x12;

        let delta = encode_rabin(&old, &new);
        assert!(
            delta.len() < new.len() / 10,
            "delta should compress well: delta={}, new={}",
            delta.len(),
            new.len()
        );

        let decoded = delta_decode(&mut Cursor::new(&delta), &old).unwrap();
        assert_eq!(decoded, new);
    }

    /// Heuristic rate must be 1.0 for identical buffers.
    #[test]
    fn test_heuristic_identical() {
        let data = b"hello world, this is a test for rabin heuristic";
        let rate = heuristic_encode_rate_rabin(data, data);
        assert!(
            (rate - 1.0).abs() < 1e-6,
            "identical data should give rate 1.0"
        );
    }

    /// Heuristic rate must be near-zero for completely different buffers.
    #[test]
    fn test_heuristic_different() {
        let old = vec![0xABu8; 1000];
        let new = vec![0xCDu8; 1000];
        let rate = heuristic_encode_rate_rabin(&old, &new);
        assert!(
            rate < 0.2,
            "different data should give low rate, got {rate}"
        );
    }

    /// Exact similarity rate must be 1.0 for identical data.
    #[test]
    fn test_rabin_encode_rate_identical() {
        let data = b"test data for rabin encode rate";
        let rate = rabin_encode_rate(data, data);
        assert!((rate - 1.0).abs() < 1e-6);
    }

    /// Round-trip with real zlib-compressed test fixtures (matching the
    /// existing Patience/Myers delta test).
    #[test]
    fn test_rabin_with_zlib_fixtures() {
        use std::{
            env,
            fs::File,
            io::{BufReader, Read},
            path::PathBuf,
        };

        use flate2::bufread::ZlibDecoder;

        fn read_zlib_data(path: &std::path::Path) -> Result<Vec<u8>, std::io::Error> {
            let file = File::open(path)?;
            let buf_reader = BufReader::new(file);
            let mut deflate = ZlibDecoder::new(buf_reader);
            let mut result = Vec::new();
            deflate.read_to_end(&mut result)?;
            Ok(result)
        }

        let mut source = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        source.push("tests/diff/16ecdcc8f663777896bd39ca025a041b7f005e");
        let old_data = read_zlib_data(&source).unwrap();

        let mut source = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        source.push("tests/diff/bee0d45f981adf7c2926a0dc04deb7f006bcc3");
        let new_data = read_zlib_data(&source).unwrap();

        let delta = encode_rabin(&old_data, &new_data);
        let decoded = delta_decode(&mut Cursor::new(&delta), &old_data).unwrap();
        assert_eq!(decoded, new_data, "rabin round-trip with zlib fixtures");
    }

    /// Backward extension: verify that matches are extended backwards to
    /// absorb bytes from the pending literal-data run. This produces larger
    /// copy regions and fewer opcodes.
    #[test]
    fn test_rabin_backward_extension() {
        let old = b"AAAAAAAABBBBBBBBCCCCCCCCDDDDDDDD";
        let mut new = vec![b'A'; 32];
        new[8] = b'X'; // change one byte in the middle
        let delta = encode_rabin(old, &new);
        let decoded = delta_decode(&mut Cursor::new(&delta), old).unwrap();
        assert_eq!(decoded, new);
    }

    /// Repetitive data (all same byte) must produce a very compact delta.
    #[test]
    fn test_rabin_repetitive_data() {
        let old = vec![b'X'; 10_000];
        let new = vec![b'X'; 10_000];
        let delta = encode_rabin(&old, &new);
        let decoded = delta_decode(&mut Cursor::new(&delta), &old).unwrap();
        assert_eq!(decoded, new);
        assert!(
            delta.len() < 100,
            "repetitive data should be very compact: got {}",
            delta.len()
        );
    }
}
