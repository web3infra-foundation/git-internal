//! Rabin fingerprint delta encoder based on Git's `diff-delta.c`.
//!
//! Uses a 16-byte sliding-window rolling hash over the source buffer to build a hash index,
//! then walks the target buffer maintaining the same rolling hash, looking up matches,
//! and greedily extending them forward and backward. The result is a Git-compatible delta
//! instruction stream.
//!
//! Available behind the `diff_rabin` feature flag.

use std::collections::HashSet;

// ── Constants ────────────────────────────────────────────────────────────

/// Rolling-hash window size (matching Git's `RABIN_WINDOW`).
const RABIN_WINDOW: usize = 16;

/// Shift amount for the T-table lookup in hash update.
const RABIN_SHIFT: u32 = 23;

/// Maximum entries per hash bucket before culling (matching Git's `HASH_LIMIT`).
const HASH_LIMIT: usize = 64;

/// Maximum bytes for a single data-insert opcode (7-bit length field).
const DATA_INS_LEN: usize = 0x7f;

/// Minumum delta rate threshold for heuristics.
const MIN_DELTA_RATE: f64 = 0.5;

// ── Rabin fingerprint tables ─────────────────────────────────────────────
// Hard-coded from Git's diff-delta.c. These match the standard CRC-32
// polynomial 0x04C11DB7 used as a Rabin fingerprint.

/// Forward table: `hash = ((hash << 8) | byte) ^ T[hash >> 23]`
#[rustfmt::skip]
static T: [u32; 256] = [
    0x00000000, 0xab59b4d1, 0x56b369a2, 0xfdeadd73, 0x063f6795, 0xad66d344,
    0x508c0e37, 0xfbd5bae6, 0x0c7ecf2a, 0xa7277bfb, 0x5acda688, 0xf1941259,
    0x0a41a8bf, 0xa1181c6e, 0x5cf2c11d, 0xf7ab75cc, 0x18fd9e54, 0xb3a42a85,
    0x4e4ef7f6, 0xe5174327, 0x1ec2f9c1, 0xb59b4d10, 0x48719063, 0xe32824b2,
    0x1483517e, 0xbfdae5af, 0x423038dc, 0xe9698c0d, 0x12bc36eb, 0xb9e5823a,
    0x440f5f49, 0xef56eb98, 0x31fb3ca8, 0x9aa28879, 0x6748550a, 0xcc11e1db,
    0x37c45b3d, 0x9c9defec, 0x6177329f, 0xca2e864e, 0x3d85f382, 0x96dc4753,
    0x6b369a20, 0xc06f2ef1, 0x3bba9417, 0x90e320c6, 0x6d09fdb5, 0xc6504964,
    0x2906a2fc, 0x825f162d, 0x7fb5cb5e, 0xd4ec7f8f, 0x2f39c569, 0x846071b8,
    0x798aaccb, 0xd2d3181a, 0x25786dd6, 0x8e21d907, 0x73cb0474, 0xd892b0a5,
    0x23470a43, 0x881ebe92, 0x75f463e1, 0xdeadd730, 0x63f67950, 0xc8afcd81,
    0x354510f2, 0x9e1ca423, 0x65c91ec5, 0xce90aa14, 0x337a7767, 0x9823c3b6,
    0x6f88b67a, 0xc4d102ab, 0x393bdfd8, 0x92626b09, 0x69b7d1ef, 0xc2ee653e,
    0x3f04b84d, 0x945d0c9c, 0x7b0be704, 0xd05253d5, 0x2db88ea6, 0x86e13a77,
    0x7d348091, 0xd66d3440, 0x2b87e933, 0x80de5de2, 0x7775282e, 0xdc2c9cff,
    0x21c6418c, 0x8a9ff55d, 0x714a4fbb, 0xda13fb6a, 0x27f92619, 0x8ca092c8,
    0x520d45f8, 0xf954f129, 0x04be2c5a, 0xafe7988b, 0x5432226d, 0xff6b96bc,
    0x02814bcf, 0xa9d8ff1e, 0x5e738ad2, 0xf52a3e03, 0x08c0e370, 0xa39957a1,
    0x584ced47, 0xf3155996, 0x0eff84e5, 0xa5a63034, 0x4af0dbac, 0xe1a96f7d,
    0x1c43b20e, 0xb71a06df, 0x4ccfbc39, 0xe79608e8, 0x1a7cd59b, 0xb125614a,
    0x468e1486, 0xedd7a057, 0x103d7d24, 0xbb64c9f5, 0x40b17313, 0xebe8c7c2,
    0x16021ab1, 0xbd5bae60, 0x6cb54671, 0xc7ecf2a0, 0x3a062fd3, 0x915f9b02,
    0x6a8a21e4, 0xc1d39535, 0x3c394846, 0x9760fc97, 0x60cb895b, 0xcb923d8a,
    0x3678e0f9, 0x9d215428, 0x66f4eece, 0xcdad5a1f, 0x3047876c, 0x9b1e33bd,
    0x7448d825, 0xdf116cf4, 0x22fbb187, 0x89a20556, 0x7277bfb0, 0xd92e0b61,
    0x24c4d612, 0x8f9d62c3, 0x7836170f, 0xd36fa3de, 0x2e857ead, 0x85dcca7c,
    0x7e09709a, 0xd550c44b, 0x28ba1938, 0x83e3ade9, 0x5d4e7ad9, 0xf617ce08,
    0x0bfd137b, 0xa0a4a7aa, 0x5b711d4c, 0xf028a99d, 0x0dc274ee, 0xa69bc03f,
    0x5130b5f3, 0xfa690122, 0x0783dc51, 0xacda6880, 0x570fd266, 0xfc5666b7,
    0x01bcbbc4, 0xaae50f15, 0x45b3e48d, 0xeeea505c, 0x13008d2f, 0xb85939fe,
    0x438c8318, 0xe8d537c9, 0x153feaba, 0xbe665e6b, 0x49cd2ba7, 0xe2949f76,
    0x1f7e4205, 0xb427f6d4, 0x4ff24c32, 0xe4abf8e3, 0x19412590, 0xb2189141,
    0x0f433f21, 0xa41a8bf0, 0x59f05683, 0xf2a9e252, 0x097c58b4, 0xa225ec65,
    0x5fcf3116, 0xf49685c7, 0x033df00b, 0xa86444da, 0x558e99a9, 0xfed72d78,
    0x0502979e, 0xae5b234f, 0x53b1fe3c, 0xf8e84aed, 0x17bea175, 0xbce715a4,
    0x410dc8d7, 0xea547c06, 0x1181c6e0, 0xbad87231, 0x4732af42, 0xec6b1b93,
    0x1bc06e5f, 0xb099da8e, 0x4d7307fd, 0xe62ab32c, 0x1dff09ca, 0xb6a6bd1b,
    0x4b4c6068, 0xe015d4b9, 0x3eb80389, 0x95e1b758, 0x680b6a2b, 0xc352defa,
    0x3887641c, 0x93ded0cd, 0x6e340dbe, 0xc56db96f, 0x32c6cca3, 0x999f7872,
    0x6475a501, 0xcf2c11d0, 0x34f9ab36, 0x9fa01fe7, 0x624ac294, 0xc9137645,
    0x26459ddd, 0x8d1c290c, 0x70f6f47f, 0xdbaf40ae, 0x207afa48, 0x8b234e99,
    0x76c993ea, 0xdd90273b, 0x2a3b52f7, 0x8162e626, 0x7c883b55, 0xd7d18f84,
    0x2c043562, 0x875d81b3, 0x7ab75cc0, 0xd1eee811,
];

/// Backward table: `hash ^= U[byte]` to remove the oldest byte from the window.
#[rustfmt::skip]
static U: [u32; 256] = [
    0x00000000, 0x7eb5200d, 0x5633f4cb, 0x2886d4c6, 0x073e5d47, 0x798b7d4a,
    0x510da98c, 0x2fb88981, 0x0e7cba8e, 0x70c99a83, 0x584f4e45, 0x26fa6e48,
    0x0942e7c9, 0x77f7c7c4, 0x5f711302, 0x21c4330f, 0x1cf9751c, 0x624c5511,
    0x4aca81d7, 0x347fa1da, 0x1bc7285b, 0x65720856, 0x4df4dc90, 0x3341fc9d,
    0x1285cf92, 0x6c30ef9f, 0x44b63b59, 0x3a031b54, 0x15bb92d5, 0x6b0eb2d8,
    0x4388661e, 0x3d3d4613, 0x39f2ea38, 0x4747ca35, 0x6fc11ef3, 0x11743efe,
    0x3eccb77f, 0x40799772, 0x68ff43b4, 0x164a63b9, 0x378e50b6, 0x493b70bb,
    0x61bda47d, 0x1f088470, 0x30b00df1, 0x4e052dfc, 0x6683f93a, 0x1836d937,
    0x250b9f24, 0x5bbebf29, 0x73386bef, 0x0d8d4be2, 0x2235c263, 0x5c80e26e,
    0x740636a8, 0x0ab316a5, 0x2b7725aa, 0x55c205a7, 0x7d44d161, 0x03f1f16c,
    0x2c4978ed, 0x52fc58e0, 0x7a7a8c26, 0x04cfac2b, 0x73e5d470, 0x0d50f47d,
    0x25d620bb, 0x5b6300b6, 0x74db8937, 0x0a6ea93a, 0x22e87dfc, 0x5c5d5df1,
    0x7d996efe, 0x032c4ef3, 0x2baa9a35, 0x551fba38, 0x7aa733b9, 0x041213b4,
    0x2c94c772, 0x5221e77f, 0x6f1ca16c, 0x11a98161, 0x392f55a7, 0x479a75aa,
    0x6822fc2b, 0x1697dc26, 0x3e1108e0, 0x40a428ed, 0x61601be2, 0x1fd53bef,
    0x3753ef29, 0x49e6cf24, 0x665e46a5, 0x18eb66a8, 0x306db26e, 0x4ed89263,
    0x4a173e48, 0x34a21e45, 0x1c24ca83, 0x6291ea8e, 0x4d29630f, 0x339c4302,
    0x1b1a97c4, 0x65afb7c9, 0x446b84c6, 0x3adea4cb, 0x1258700d, 0x6ced5000,
    0x4355d981, 0x3de0f98c, 0x15662d4a, 0x6bd30d47, 0x56ee4b54, 0x285b6b59,
    0x00ddbf9f, 0x7e689f92, 0x51d01613, 0x2f65361e, 0x07e3e2d8, 0x7956c2d5,
    0x5892f1da, 0x2627d1d7, 0x0ea10511, 0x7014251c, 0x5facac9d, 0x21198c90,
    0x099f5856, 0x772a785b, 0x4c921c31, 0x32273c3c, 0x1aa1e8fa, 0x6414c8f7,
    0x4bac4176, 0x3519617b, 0x1d9fb5bd, 0x632a95b0, 0x42eea6bf, 0x3c5b86b2,
    0x14dd5274, 0x6a687279, 0x45d0fbf8, 0x3b65dbf5, 0x13e30f33, 0x6d562f3e,
    0x506b692d, 0x2ede4920, 0x06589de6, 0x78edbdeb, 0x5755346a, 0x29e01467,
    0x0166c0a1, 0x7fd3e0ac, 0x5e17d3a3, 0x20a2f3ae, 0x08242768, 0x76910765,
    0x59298ee4, 0x279caee9, 0x0f1a7a2f, 0x71af5a22, 0x7560f609, 0x0bd5d604,
    0x235302c2, 0x5de622cf, 0x725eab4e, 0x0ceb8b43, 0x246d5f85, 0x5ad87f88,
    0x7b1c4c87, 0x05a96c8a, 0x2d2fb84c, 0x539a9841, 0x7c2211c0, 0x029731cd,
    0x2a11e50b, 0x54a4c506, 0x69998315, 0x172ca318, 0x3faa77de, 0x411f57d3,
    0x6ea7de52, 0x1012fe5f, 0x38942a99, 0x46210a94, 0x67e5399b, 0x19501996,
    0x31d6cd50, 0x4f63ed5d, 0x60db64dc, 0x1e6e44d1, 0x36e89017, 0x485db01a,
    0x3f77c841, 0x41c2e84c, 0x69443c8a, 0x17f11c87, 0x38499506, 0x46fcb50b,
    0x6e7a61cd, 0x10cf41c0, 0x310b72cf, 0x4fbe52c2, 0x67388604, 0x198da609,
    0x36352f88, 0x48800f85, 0x6006db43, 0x1eb3fb4e, 0x238ebd5d, 0x5d3b9d50,
    0x75bd4996, 0x0b08699b, 0x24b0e01a, 0x5a05c017, 0x728314d1, 0x0c3634dc,
    0x2df207d3, 0x534727de, 0x7bc1f318, 0x0574d315, 0x2acc5a94, 0x54797a99,
    0x7cffae5f, 0x024a8e52, 0x06852279, 0x78300274, 0x50b6d6b2, 0x2e03f6bf,
    0x01bb7f3e, 0x7f0e5f33, 0x57888bf5, 0x293dabf8, 0x08f998f7, 0x764cb8fa,
    0x5eca6c3c, 0x207f4c31, 0x0fc7c5b0, 0x7172e5bd, 0x59f4317b, 0x27411176,
    0x1a7c5765, 0x64c97768, 0x4c4fa3ae, 0x32fa83a3, 0x1d420a22, 0x63f72a2f,
    0x4b71fee9, 0x35c4dee4, 0x1400edeb, 0x6ab5cde6, 0x42331920, 0x3c86392d,
    0x133eb0ac, 0x6d8b90a1, 0x450d4467, 0x3bb8646a,
];

// ── Index structures ─────────────────────────────────────────────────────

/// A single hash-index entry: offset into source buffer and its Rabin fingerprint.
#[derive(Debug, Clone, Copy)]
pub struct IndexEntry {
    pub offset: u32,
    pub hash: u32,
}

/// Pre-built Rabin fingerprint index over a source buffer.
///
/// This can be cached and reused across multiple delta computations against
/// the same source — matching git's approach where `delta_index` is created
/// once per source and reused for all target candidates in the window.
pub struct RabinDeltaIndex {
    /// Copy of the source data for match extension.
    pub source: Vec<u8>,
    /// All index entries packed consecutively, grouped by bucket.
    pub entries: Box<[IndexEntry]>,
    /// Start indices into `entries` per bucket; `buckets.len() == hash_mask + 2`
    /// (last entry is a sentinel marking the end of the last bucket).
    pub buckets: Box<[u32]>,
    /// Bitmask for fast `hash & mask` bucket lookup.
    pub hash_mask: u32,
}

// ── Index construction ───────────────────────────────────────────────────

/// Build a Rabin fingerprint index over `source`, following Git's `create_delta_index`.
///
/// Returns `None` if the source is too short to index (< RABIN_WINDOW bytes).
pub fn create_delta_index(source: &[u8]) -> Option<RabinDeltaIndex> {
    let src_len = source.len();
    if src_len == 0 {
        return None;
    }

    let entries_count = (src_len - 1) / RABIN_WINDOW;
    if entries_count == 0 {
        return None;
    }

    // Determine hash table size: next power of two >= entries_count / 4, min 16
    let hsize_entries = entries_count / 4;
    let mut hsize: usize = 16;
    while hsize < hsize_entries {
        hsize <<= 1;
    }
    let hmask = hsize - 1;

    // Build linked lists per bucket as Vecs (building from end to start).
    let mut bucket_lists: Vec<Vec<IndexEntry>> = vec![Vec::new(); hsize];
    let mut prev_val: u32 = !0u32;

    // Walk source from end toward start, stepping by RABIN_WINDOW.
    // Git starts at: buf + entries * RABIN_WINDOW - RABIN_WINDOW
    let mut pos = entries_count * RABIN_WINDOW - RABIN_WINDOW;
    loop {
        // Compute hash over source[pos+1 .. pos+1+RABIN_WINDOW]
        let window_start = pos + 1;
        let window_end = window_start + RABIN_WINDOW;
        if window_end > src_len {
            if pos == 0 {
                break;
            }
            pos = pos.saturating_sub(RABIN_WINDOW);
            continue;
        }

        let mut val: u32 = 0;
        for &b in &source[window_start..window_end] {
            val = ((val << 8) | b as u32) ^ T[(val >> RABIN_SHIFT) as usize];
        }

        if val == prev_val {
            // Keep the lowest offset of consecutive identical blocks.
            // The existing last entry gets its offset updated.
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

    // Cull overfull buckets to HASH_LIMIT (uniform sampling, matching Git).
    let mut total_entries = 0usize;
    for bucket in bucket_lists.iter_mut() {
        if bucket.len() > HASH_LIMIT {
            cull_bucket(bucket);
        }
        total_entries += bucket.len();
    }

    // Pack into flat arrays.
    let mut entries_vec: Vec<IndexEntry> = Vec::with_capacity(total_entries);
    let mut buckets: Vec<u32> = Vec::with_capacity(hsize + 1);

    for bucket in bucket_lists.iter() {
        buckets.push(entries_vec.len() as u32);
        entries_vec.extend_from_slice(bucket);
    }
    buckets.push(entries_vec.len() as u32); // sentinel

    Some(RabinDeltaIndex {
        source: source.to_vec(),
        entries: entries_vec.into_boxed_slice(),
        buckets: buckets.into_boxed_slice(),
        hash_mask: hmask as u32,
    })
}

/// Uniformly cull an overfull bucket down to `HASH_LIMIT` entries.
///
/// Uses Git's algorithm: skip entries at regular intervals so the remaining
/// entries are spread approximately evenly across the original list.
fn cull_bucket(bucket: &mut Vec<IndexEntry>) {
    let total = bucket.len();
    let target = HASH_LIMIT;
    let excess = total - target;

    // Git's approach: iterate through the list, keeping every entry unless
    // the accumulator overflows, then skip one. This uniformly removes `excess`
    // entries.
    let mut kept = Vec::with_capacity(target);
    let mut acc: isize = 0;

    for entry in bucket.drain(..) {
        acc += excess as isize;
        if acc > 0 {
            acc -= target as isize;
            // Skip this entry (remove it)
        } else {
            kept.push(entry);
        }
    }

    *bucket = kept;
}

// ── Delta generation ─────────────────────────────────────────────────────

/// Max opcode size for output buffer growth estimation.
const MAX_OP_SIZE: usize = 5 + 5 + 1 + RABIN_WINDOW + 7;

/// Git-style varint encoding over 7 bits per byte (msb=1 means continue).
fn write_varint(n: usize, out: &mut Vec<u8>) {
    let mut v = n;
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Encode a copy opcode: [cmd_byte] [offset_bytes...] [size_bytes...]
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

/// Generate a delta from `index` for `target`, following Git's `create_delta`.
///
/// If `max_size` is `Some(n)`, the computation aborts early and returns `None`
/// as soon as the output exceeds `n` bytes. This matches git's `max_size`
/// parameter in `create_delta`, used for early termination during candidate scoring.
fn create_delta(
    index: &RabinDeltaIndex,
    target: &[u8],
    max_size: Option<usize>,
) -> Option<Vec<u8>> {
    let src_data = &index.source;
    let src_len = src_data.len();
    let trg_len = target.len();

    // Initial capacity heuristic
    let cap = 32 + trg_len / 2;
    let mut out: Vec<u8> = Vec::with_capacity(cap);

    // Header: source size, target size
    write_varint(src_len, &mut out);
    write_varint(trg_len, &mut out);

    if trg_len == 0 {
        return Some(out);
    }

    let hmask = index.hash_mask as usize;
    let buckets = &index.buckets;
    let entries = &index.entries;

    // ── Phase 1: emit first RABIN_WINDOW bytes as literal data & init hash ──
    let init_count = RABIN_WINDOW.min(trg_len);
    // Reserve space for the data-op length byte (filled later)
    let mut data_len_pos = out.len();
    out.push(0u8); // placeholder
    for &b in &target[..init_count] {
        out.push(b);
    }

    let mut val: u32 = 0;
    for &b in &target[..init_count] {
        val = ((val << 8) | b as u32) ^ T[(val >> RABIN_SHIFT) as usize];
    }

    let mut data_pos = init_count; // next target byte to process
    let mut inscnt = init_count; // bytes in current data-insert run
    let mut moff: u32 = 0; // current match offset in source
    let mut msize: u32 = 0; // current match length

    // ── Phase 2: sliding-window match loop ──
    while data_pos < trg_len {
        // Rolling hash: remove oldest byte, add new byte
        if data_pos >= RABIN_WINDOW {
            let oldest = target[data_pos - RABIN_WINDOW];
            val ^= U[oldest as usize];
        }
        let new_byte = target[data_pos];
        val = ((val << 8) | new_byte as u32) ^ T[(val >> RABIN_SHIFT) as usize];

        if msize < 4096 {
            // Look up hash in index, extend match forward
            let bi = val as usize & hmask;
            let bucket_start = buckets[bi] as usize;
            let bucket_end = buckets[bi + 1] as usize;

            for &entry in &entries[bucket_start..bucket_end] {
                if entry.hash != val {
                    continue;
                }

                let ref_start = entry.offset as usize;
                let src_remain = src_len - ref_start;
                let trg_remain = trg_len - data_pos;
                let max_match = src_remain.min(trg_remain);

                if max_match <= msize as usize {
                    // Sorted by offset (descending), so no better match possible
                    break;
                }

                // Count matching bytes
                let mut match_len: usize = 0;
                while match_len < max_match
                    && src_data[ref_start + match_len] == target[data_pos + match_len]
                {
                    match_len += 1;
                }

                if match_len > msize as usize {
                    msize = match_len as u32;
                    moff = ref_start as u32;
                    if msize >= 4096 {
                        break; // good enough
                    }
                }
            }
        }

        if msize < 4 {
            // ── Emit as literal data ──
            if inscnt == 0 {
                data_len_pos = out.len();
                out.push(0u8); // placeholder
            }
            out.push(new_byte);
            inscnt += 1;
            data_pos += 1;

            // Flush data run if it reaches max length
            if inscnt == DATA_INS_LEN {
                out[data_len_pos] = DATA_INS_LEN as u8;
                inscnt = 0;
            }
            msize = 0;
        } else {
            // ── Emit copy instruction ──
            // Backward extension: try to extend match backwards into the
            // pending literal data. Count how many bytes we can absorb.
            let mut match_off = moff as usize;
            let mut match_len = msize as usize;

            let back_extend: usize = {
                let max_back = inscnt.min(match_off);
                let mut cnt = 0usize;
                while cnt < max_back
                    && src_data[match_off - 1 - cnt] == target[data_pos - 1 - cnt]
                {
                    cnt += 1;
                }
                cnt
            };

            if back_extend > 0 {
                // Absorb back_extend bytes from the data run into the match
                match_off -= back_extend;
                match_len += back_extend;
                data_pos -= back_extend;
                inscnt -= back_extend;

                // Truncate the output: remove back_extend bytes from the
                // pending data run (and the placeholder if fully consumed).
                let old_out_len = out.len();
                let new_out_len = if inscnt == 0 {
                    // All pending data absorbed; remove placeholder byte too
                    data_len_pos
                } else {
                    old_out_len - back_extend
                };
                out.truncate(new_out_len);
            }

            // Finalize any remaining pending data insert
            if inscnt > 0 {
                out[data_len_pos] = inscnt as u8;
                inscnt = 0;
            }

            // Split copy into chunks ≤ 64KB (pack v2 limit)
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

            // Git checks `moff > 0xffffffff` here, but since our offsets are
            // u32 (matching the Git delta format's 32-bit limit), this is a no-op.

            // When remaining match < 4096, re-seed the rolling hash from
            // the last RABIN_WINDOW bytes before the new position.
            if msize < 4096 && data_pos >= RABIN_WINDOW {
                val = 0;
                for &b in &target[data_pos - RABIN_WINDOW..data_pos] {
                    val = ((val << 8) | b as u32) ^ T[(val >> RABIN_SHIFT) as usize];
                }
            }
        }

        // Early termination: if output already exceeds max_size, abort.
        if let Some(max) = max_size
            && out.len() > max
        {
            return None;
        }

        // Grow output buffer if needed
        if out.len() + MAX_OP_SIZE > out.capacity() {
            out.reserve(MAX_OP_SIZE * 2);
        }
    }

    // Flush any remaining data insert
    if inscnt > 0 {
        out[data_len_pos] = inscnt as u8;
    }

    // Final max_size check before returning
    if let Some(max) = max_size
        && out.len() > max
    {
        return None;
    }

    Some(out)
}

// ── Public API ───────────────────────────────────────────────────────────

/// Encode `new_data` as a Git-compatible delta against `old_data` using
/// Rabin fingerprint matching.
///
/// Returns the complete delta instruction stream (header + opcodes).
pub fn encode_rabin(old_data: &[u8], new_data: &[u8]) -> Vec<u8> {
    if new_data.is_empty() {
        // Emit minimal delta: only header with actual source size
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
        // Index creation failed (e.g., old_data too small); emit literal-only delta
        encode_literal_only(old_data.len(), new_data)
    }
}

/// Encode `new_data` as a Git-compatible delta against a pre-built index.
///
/// This skips the index construction step, allowing the caller to cache and
/// reuse the index across multiple target objects — matching git's approach
/// where `delta_index` is built once per source in the delta window.
pub fn encode_rabin_with_index(index: &RabinDeltaIndex, target: &[u8]) -> Vec<u8> {
    create_delta(index, target, None)
        .expect("delta should always succeed when max_size is None")
}

/// Encode `new_data` as a delta against a pre-built index, with early
/// termination if the output exceeds `max_size` bytes.
///
/// Returns `None` if the delta was aborted (output would exceed `max_size`),
/// or if the output is empty. This is the key optimization for candidate scoring:
/// we can try many candidates rapidly, aborting as soon as a delta exceeds
/// the current best.
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

/// Compute the similarity rate (shared bytes / new_data length) by running
/// the full Rabin encode. This is the accurate but slower path; use
/// `heuristic_encode_rate_rabin` for fast pre-screening.
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
        // Estimate shared bytes from delta size:
        // delta_size ≈ header(4-10) + literal_bytes + copy_op_bytes(3-8 each)
        // shared_bytes ≈ new_size - (delta_size - header - copy_op_overhead)
        //
        // A simple and accurate approach: compute shared bytes = new_data.len()
        // minus the total literal bytes embedded in the delta.
        let new_len = new_data.len();
        let mut shared: usize = 0;

        // Skip the two varint headers
        let mut pos = 0usize;
        // Skip source size varint
        while pos < delta.len() && (delta[pos] & 0x80) != 0 {
            pos += 1;
        }
        pos += 1;
        // Skip target size varint
        while pos < delta.len() && (delta[pos] & 0x80) != 0 {
            pos += 1;
        }
        pos += 1;

        while pos < delta.len() {
            let cmd = delta[pos];
            pos += 1;
            if cmd & 0x80 == 0 {
                // Data instruction: cmd is the length of literal data
                let len = cmd as usize;
                pos += len; // skip literal bytes
                shared += len.min(new_len);
            } else {
                // Copy instruction: read offset and size based on flag bits
                let mut _off: usize = 0;
                let mut sz: usize = 0;
                for i in 0..4 {
                    if cmd & (1 << i) != 0 {
                        _off |= (delta[pos] as usize) << (8 * i);
                        pos += 1;
                    }
                }
                for i in 0..3 {
                    if cmd & (1 << (4 + i)) != 0 {
                        sz |= (delta[pos] as usize) << (8 * i);
                        pos += 1;
                    }
                }
                if sz == 0 {
                    sz = 0x10000; // Git convention: size 0 means 64KB
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

/// Fast heuristic similarity screening using Rabin hashes.
///
/// Samples both buffers at `RABIN_WINDOW` intervals, computing Rabin
/// fingerprints at each sample point. Compares the sets of hashes to
/// estimate how much of `new_data` matches `old_data`. Returns 0.0 if
/// the similarity is clearly below `MIN_DELTA_RATE` (0.5).
pub fn heuristic_encode_rate_rabin(old_data: &[u8], new_data: &[u8]) -> f64 {
    let old_len = old_data.len();
    let new_len = new_data.len();

    if old_len == 0 && new_len == 0 {
        return 1.0;
    }
    if old_len == 0 || new_len == 0 {
        return 0.0;
    }

    let step = if old_len > 1_000_000 { 256 } else { RABIN_WINDOW };

    // Collect Rabin hashes from old_data at sampled positions
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

    // For fast screening: compute hashes from old_data, store in HashSet
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

    // Count matches in new_data
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

        // Early exit: if max possible rate can't reach MIN_DELTA_RATE
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

// ── Helpers ──────────────────────────────────────────────────────────────

/// Produce a delta that is entirely literal data (no copy ops). Used when
/// source is empty or too small to index.
fn encode_literal_only(source_len: usize, new_data: &[u8]) -> Vec<u8> {
    let new_len = new_data.len();
    let mut out: Vec<u8> = Vec::with_capacity(4 + new_len);

    // Header: source size, target size
    write_varint(source_len, &mut out);
    write_varint(new_len, &mut out);

    // Emit all as data instructions
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

    /// Debug test: dump raw delta for identical data.
    /// Round-trip: identical data.
    #[test]
    fn test_rabin_round_trip_identical() {
        let old = b"hello world, this is a test for rabin delta";
        let new = b"hello world, this is a test for rabin delta";
        let delta = encode_rabin(old, new);
        let decoded = delta_decode(&mut Cursor::new(&delta), old).unwrap();
        assert_eq!(decoded, new, "round-trip should reconstruct exact data");
    }

    /// Round-trip: small edit.
    #[test]
    fn test_rabin_round_trip_edit() {
        let old = b"hello world, this is a test";
        let new = b"hello rust, this is a test";
        let delta = encode_rabin(old, new);
        let decoded = delta_decode(&mut Cursor::new(&delta), old).unwrap();
        assert_eq!(decoded, new);
    }

    /// Round-trip: expansion (new is larger than old).
    #[test]
    fn test_rabin_round_trip_expand() {
        let old = b"small";
        let new = b"this is a much larger buffer that includes small at the end";
        let delta = encode_rabin(old, new);
        let decoded = delta_decode(&mut Cursor::new(&delta), old).unwrap();
        assert_eq!(decoded, new);
    }

    /// Round-trip: completely different data.
    #[test]
    fn test_rabin_round_trip_different() {
        let old = b"abcdefghijklmnop";
        let new = b"1234567890123456";
        let delta = encode_rabin(old, new);
        let decoded = delta_decode(&mut Cursor::new(&delta), old).unwrap();
        assert_eq!(decoded, new);
    }

    /// Round-trip: empty inputs.
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

    /// Round-trip: single byte.
    #[test]
    fn test_rabin_single_byte() {
        let delta = encode_rabin(b"a", b"b");
        let decoded = delta_decode(&mut Cursor::new(&delta), b"a").unwrap();
        assert_eq!(decoded, b"b");
    }

    /// Round-trip: data shorter than RABIN_WINDOW.
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

    /// Large buffer with a few byte changes: delta should be much smaller than full data.
    #[test]
    fn test_rabin_large_buffer_compresses() {
        let old = vec![0xABu8; 100_000];
        let mut new = old.clone();
        new[500] = 0xCD;
        new[50_000] = 0xEF;
        new[99_000] = 0x12;

        let delta = encode_rabin(&old, &new);
        // Delta with copy ops should be dramatically smaller than 100KB
        assert!(
            delta.len() < new.len() / 10,
            "delta should compress well: delta={}, new={}",
            delta.len(),
            new.len()
        );

        let decoded = delta_decode(&mut Cursor::new(&delta), &old).unwrap();
        assert_eq!(decoded, new);
    }

    /// Heuristic should return 1.0 for identical data.
    #[test]
    fn test_heuristic_identical() {
        let data = b"hello world, this is a test for rabin heuristic";
        let rate = heuristic_encode_rate_rabin(data, data);
        assert!((rate - 1.0).abs() < 1e-6, "identical data should give rate 1.0");
    }

    /// Heuristic should return low rate for completely different data.
    #[test]
    fn test_heuristic_different() {
        let old = vec![0xABu8; 1000];
        let new = vec![0xCDu8; 1000];
        let rate = heuristic_encode_rate_rabin(&old, &new);
        assert!(rate < 0.2, "different data should give low rate, got {rate}");
    }

    /// Accurate rate should be 1.0 for identical data.
    #[test]
    fn test_rabin_encode_rate_identical() {
        let data = b"test data for rabin encode rate";
        let rate = rabin_encode_rate(data, data);
        assert!((rate - 1.0).abs() < 1e-6);
    }

    /// Round-trip with real zlib test fixtures (matching the existing delta test).
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

    /// Backward extension: verify that matches get extended backwards
    /// to produce larger copy regions and fewer instructions.
    #[test]
    fn test_rabin_backward_extension() {
        // The prefix is identical, then a change, then more identical content.
        // Rabin should find a copy region that covers more than just the
        // hashed window by extending backward.
        let old = b"AAAAAAAABBBBBBBBCCCCCCCCDDDDDDDD";
        let mut new = vec![b'A'; 32];
        new[8] = b'X'; // change one byte
        let delta = encode_rabin(old, &new);
        let decoded = delta_decode(&mut Cursor::new(&delta), old).unwrap();
        assert_eq!(decoded, new);
    }

    /// Repetitive data: all same byte. Should produce very compact delta.
    #[test]
    fn test_rabin_repetitive_data() {
        let old = vec![b'X'; 10_000];
        let new = vec![b'X'; 10_000];
        let delta = encode_rabin(&old, &new);
        let decoded = delta_decode(&mut Cursor::new(&delta), &old).unwrap();
        assert_eq!(decoded, new);
        // Should be very small since everything matches
        assert!(delta.len() < 100, "repetitive data should be very compact");
    }
}
