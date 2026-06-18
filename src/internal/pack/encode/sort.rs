//! Entry ordering and similarity heuristics for delta-base selection.
//!
//! The sorting function groups entries so likely delta pairs become neighbors, which maximises the
//! hit-rate of the sliding-window search. The similarity checks are cheap pre-filters that avoid
//! expensive delta computation between unrelated objects.

use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    path::Path,
};

use ahash::AHasher;

use crate::{
    delta,
    internal::{
        metadata::{EntryMeta, MetaAttached},
        pack::entry::Entry,
    },
};

/// Compute a deterministic content fingerprint for sorting entries that lack
/// path metadata.
///
/// Reads the first 8 bytes as a big-endian `u64` type signature. Files that
/// share a common header prefix (e.g. all Kubernetes YAML starting with
/// `apiVersi`) receive the same key, clustering structurally-similar objects
/// together for the delta window.
#[inline]
fn content_sort_key(data: &[u8]) -> u64 {
    let len = data.len().min(8);
    let mut key: u64 = 0;
    for &byte in &data[..len] {
        key = (key << 8) | byte as u64;
    }
    key <<= (8 - len) * 8;
    key
}

/// Order entries so likely delta pairs become neighbors.
///
/// Entries with path metadata come first. They are clustered by parent directory and Git's
/// `pack_name_hash`, then ordered by decreasing payload size so a larger object can serve as the
/// base for following smaller objects. Entries without paths are grouped by the first 8 bytes
/// (type signature) so structurally-similar files sit close, then by decreasing size.
///
/// The final pointer comparison is only a tie-breaker; it gives `sort_by` a total ordering when all
/// semantic keys are equal.
pub(crate) fn magic_sort(
    a: &MetaAttached<Entry, EntryMeta>,
    b: &MetaAttached<Entry, EntryMeta>,
) -> Ordering {
    let path_a = a.meta.file_path.as_ref();
    let path_b = b.meta.file_path.as_ref();

    // Path-aware entries carry the strongest signal for grouping revisions of the same file.
    match (path_a, path_b) {
        (Some(pa), Some(pb)) => {
            let pa = Path::new(pa);
            let pb = Path::new(pb);

            // Keep files in the same directory adjacent before considering their names.
            let dir_ord = pa.parent().cmp(&pb.parent());
            if dir_ord != Ordering::Equal {
                return dir_ord;
            }

            // The Git-compatible name hash groups similar basenames without allocating strings.
            let hash_a = delta::pack_name_hash(pa.as_os_str().as_encoded_bytes());
            let hash_b = delta::pack_name_hash(pb.as_os_str().as_encoded_bytes());
            if hash_a != hash_b {
                return hash_b.cmp(&hash_a); // Descending, matching the intended pack ordering.
            }
        }
        (Some(_), None) => return Ordering::Less,
        (None, Some(_)) => return Ordering::Greater,
        (None, None) => {
            // Cluster by the first 8 bytes (type signature) so files with the
            // same header prefix sit together. Within each cluster, larger
            // entries come first as potential delta bases.
            let key_a = content_sort_key(&a.inner.data);
            let key_b = content_sort_key(&b.inner.data);
            let ord = key_a.cmp(&key_b);
            if ord != Ordering::Equal {
                return ord;
            }
        }
    }

    // Larger entries appear first because later entries can refer backwards to them as bases.
    let ord = b.inner.data.len().cmp(&a.inner.data.len());
    if ord != Ordering::Equal {
        return ord;
    }

    // Supply a deterministic ordering within this in-memory collection when all keys tie.
    (a as *const MetaAttached<Entry, EntryMeta>).cmp(&(b as *const MetaAttached<Entry, EntryMeta>))
}

/// Hash a small sample used by the cheap candidate pre-filters.
pub(crate) fn calc_hash(data: &[u8]) -> u64 {
    let mut hasher = AHasher::default();
    data.hash(&mut hasher);
    hasher.finish()
}

/// Multi-point similarity check: compares hashes at the beginning and end
/// of both buffers. If either region matches, the buffers are likely similar
/// enough to be worth delta computation.
///
/// This is more lenient than the original `cheap_similar` (which only checked
/// the first 128 bytes), catching cases where files share content after
/// different headers (e.g., different license headers but identical code).
#[cfg(feature = "diff_rabin")]
pub(crate) fn multi_point_similar(a: &[u8], b: &[u8]) -> bool {
    let min_len = a.len().min(b.len());
    if min_len < 16 {
        return false;
    }

    // Check beginning (first 128 bytes or whole buffer)
    let head_len = 128.min(min_len);
    if calc_hash(&a[..head_len]) == calc_hash(&b[..head_len]) {
        return true;
    }

    // Check end (last 128 bytes)
    let tail_start = min_len.saturating_sub(128);
    if calc_hash(&a[tail_start..min_len]) == calc_hash(&b[tail_start..min_len]) {
        return true;
    }

    false
}

/// Cheap check if two byte slices are similar by comparing their hashes
/// of the first 128 bytes. Used only in the non-rabin path.
#[cfg(not(feature = "diff_rabin"))]
pub(crate) fn cheap_similar(a: &[u8], b: &[u8]) -> bool {
    let k = a.len().min(b.len()).min(128);
    if k == 0 {
        return false;
    }
    calc_hash(&a[..k]) == calc_hash(&b[..k])
}
