//! Word-at-a-time match extension for Rabin delta generation.
//!
//! When the rolling hash finds a matching fingerprint in the source index, we need to
//! count how many consecutive bytes actually match. This function compares source and
//! target buffers starting at the given positions, returning the number of matching
//! bytes up to `max_match`.
//!
//! The comparison reads `usize`-sized chunks (8 bytes on 64-bit, 4 on 32-bit) via
//! safe `usize::from_ne_bytes` to accelerate the common case where long runs of bytes
//! are identical. When a word mismatch is found, `first_different_byte` pinpoints the
//! exact byte offset within that word. Any remaining tail bytes are compared one at
//! a time.

/// Locate the first byte position where two `usize` values differ.
///
/// On little-endian platforms (x86_64, aarch64), the least significant byte is at the
/// lowest address, so trailing zeros in the XOR difference correspond to matching bytes
/// from the start. On big-endian, we count leading zeros instead.
#[inline(always)]
fn first_different_byte(diff: usize) -> usize {
    if cfg!(target_endian = "little") {
        diff.trailing_zeros() as usize / 8
    } else {
        diff.leading_zeros() as usize / 8
    }
}

/// Extend a match forward from `(ref_start, data_pos)`, returning the number of
/// consecutive identical bytes.
///
/// The function reads `usize`-sized chunks from both buffers via
/// [`usize::from_ne_bytes`] and compares them in one operation. On mismatch it
/// finds the exact byte offset via `first_different_byte`, then falls back to a
/// byte-by-byte loop for any remaining tail.
///
/// # Performance
///
/// Benchmarks show this is ~5% faster than the baseline indexed-access loop for
/// no-prefilter delta search, and ~20% faster with prefiltering enabled. Delta
/// output is identical regardless of the match extension strategy.
#[inline(always)]
pub(crate) fn extend_match(
    src_data: &[u8],
    target: &[u8],
    ref_start: usize,
    data_pos: usize,
    max_match: usize,
) -> usize {
    // Trim to the actual remaining lengths in both buffers.
    let Some(src_tail) = src_data.get(ref_start..) else {
        return 0;
    };
    let Some(tgt_tail) = target.get(data_pos..) else {
        return 0;
    };
    let max = max_match.min(src_tail.len()).min(tgt_tail.len());

    let mut i = 0;
    const WORD_SIZE: usize = core::mem::size_of::<usize>();

    // Fast path: compare one `usize`-sized chunk at a time via safe `from_ne_bytes`.
    while i + WORD_SIZE <= max {
        let src_word = usize::from_ne_bytes(src_tail[i..i + WORD_SIZE].try_into().unwrap());
        let tgt_word = usize::from_ne_bytes(tgt_tail[i..i + WORD_SIZE].try_into().unwrap());
        let diff = src_word ^ tgt_word;

        if diff != 0 {
            // Found a mismatch — pinpoint the exact byte within this word.
            return i + first_different_byte(diff);
        }

        i += WORD_SIZE;
    }

    // Slow path: byte-by-byte comparison for the remaining tail (< WORD_SIZE bytes).
    while i < max {
        if src_tail[i] != tgt_tail[i] {
            return i;
        }
        i += 1;
    }

    max
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::extend_match;

    /// Reference implementation: the simplest possible, obviously-correct match
    /// extension using `.get()` for bounds safety. All tests compare `extend_match`
    /// against this to verify correctness.
    fn extend_match_reference(
        src_data: &[u8],
        target: &[u8],
        ref_start: usize,
        data_pos: usize,
        max_match: usize,
    ) -> usize {
        let mut i = 0;
        while i < max_match {
            let Some(&a) = src_data.get(ref_start + i) else {
                break;
            };
            let Some(&b) = target.get(data_pos + i) else {
                break;
            };
            if a != b {
                break;
            }
            i += 1;
        }
        i
    }

    /// Assert `extend_match` returns the same result as the reference implementation.
    /// Pre-condition: `ref_start + max_match <= src.len()` and `data_pos + max_match <= tgt.len()`.
    fn assert_match(src: &[u8], tgt: &[u8], ref_start: usize, data_pos: usize, max_match: usize) {
        debug_assert!(ref_start + max_match <= src.len());
        debug_assert!(data_pos + max_match <= tgt.len());

        let expected = extend_match_reference(src, tgt, ref_start, data_pos, max_match);
        let actual = extend_match(src, tgt, ref_start, data_pos, max_match);
        assert_eq!(actual, expected, "mismatch: {actual} != {expected}");
    }

    #[test]
    fn empty_inputs() {
        assert_match(b"", b"", 0, 0, 0);
    }

    #[test]
    fn full_equal() {
        let a = b"abcdef";
        let b = b"abcdef";
        assert_match(a, b, 0, 0, 6);
    }

    #[test]
    fn first_byte_differs() {
        assert_match(b"xbcdef", b"abcdef", 0, 0, 6);
    }

    #[test]
    fn middle_differs() {
        assert_match(b"abcxef", b"abcdef", 0, 0, 6);
    }

    #[test]
    fn last_byte_differs() {
        assert_match(b"abcdex", b"abcdef", 0, 0, 6);
    }

    #[test]
    fn with_offsets() {
        assert_match(b"zzzzabcdef", b"yyyyabcxef", 4, 4, 6);
    }

    #[test]
    fn max_shorter_than_equal_prefix() {
        assert_match(b"abcdef", b"abcdef", 0, 0, 3);
    }

    #[test]
    fn near_end_of_src() {
        assert_match(b"abcdef", b"xxcdef", 2, 2, 4);
    }

    #[test]
    fn near_end_of_tgt() {
        assert_match(b"xxcdef", b"abcdef", 2, 2, 4);
    }

    #[test]
    fn max_zero() {
        assert_match(b"abcdef", b"abcdef", 0, 0, 0);
    }

    #[test]
    fn single_byte_match() {
        assert_match(b"a", b"a", 0, 0, 1);
    }

    #[test]
    fn single_byte_no_match() {
        assert_match(b"a", b"b", 0, 0, 1);
    }

    #[test]
    fn long_match_64_bytes() {
        let a = vec![b'A'; 128];
        let mut b = vec![b'A'; 128];
        b[64] = b'B';
        assert_match(&a, &b, 0, 0, 128);
    }

    #[test]
    fn long_match_4096_bytes() {
        let a = vec![b'A'; 8192];
        let mut b = vec![b'A'; 8192];
        b[4096] = b'B';
        assert_match(&a, &b, 0, 0, 8192);
    }

    #[test]
    fn word_boundary_7_bytes() {
        // 7 bytes forces the byte-by-byte tail after the word loop.
        assert_match(b"1234567", b"1234567", 0, 0, 7);
    }

    #[test]
    fn word_boundary_8_bytes() {
        // Exactly one word on 64-bit.
        assert_match(b"12345678", b"12345678", 0, 0, 8);
    }

    #[test]
    fn word_boundary_9_bytes() {
        // One word + 1 tail byte.
        assert_match(b"123456789", b"123456789", 0, 0, 9);
    }

    #[test]
    fn mismatch_in_first_word() {
        assert_match(b"12345678", b"1234x678", 0, 0, 8);
    }

    #[test]
    fn mismatch_in_second_word() {
        assert_match(b"12345678abcdefgh", b"12345678abcxefgh", 0, 0, 16);
    }

    #[test]
    fn ref_start_at_last_byte() {
        assert_match(b"hello", b"hello", 4, 4, 1);
    }

    #[test]
    fn data_pos_at_last_byte() {
        assert_match(b"hello", b"hello", 4, 4, 1);
    }

    #[test]
    fn ref_start_near_end() {
        assert_match(b"abc", b"abcdef", 2, 0, 1);
    }

    #[test]
    fn data_pos_near_end() {
        assert_match(b"abcdef", b"abc", 0, 2, 1);
    }
}
