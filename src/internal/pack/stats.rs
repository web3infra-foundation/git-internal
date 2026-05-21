//! Pack statistics helpers built on top of the existing pack decoding primitives.

use std::{
    fmt::{self, Display},
    fs,
    io::{BufRead, BufReader},
    path::Path,
};

use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::{
        object::types::ObjectType,
        pack::{
            Pack,
            cache_object::{CacheObject, CacheObjectInfo},
            utils,
            wrapper::Wrapper,
        },
    },
};

/// Object distribution and validation information collected from a pack file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackStats {
    pub total: usize,
    pub declared_total: usize,
    pub decoded_total: usize,
    pub commits: usize,
    pub trees: usize,
    pub blobs: usize,
    pub tags: usize,
    pub offset_deltas: usize,
    pub hash_deltas: usize,
    pub largest_object_size: usize,
    pub largest_object_type: Option<ObjectType>,
}

impl PackStats {
    /// Records one object type and updates the total counters.
    ///
    /// # Parameters
    /// * `object_type` - The decoded raw pack object type.
    ///
    /// # Returns
    /// This method does not return a value.
    ///
    /// # Side effects
    /// Mutates the count fields in `self`.
    pub fn record_object_type(&mut self, object_type: ObjectType) {
        // 1. Count every raw pack entry toward the total and decoded totals.
        // 2. Add the object to the matching type bucket.
        // 3. Treat zstd offset deltas as offset-style delta entries.
        self.total += 1;
        self.decoded_total += 1;
        match object_type {
            ObjectType::Commit => self.commits += 1,
            ObjectType::Tree => self.trees += 1,
            ObjectType::Blob => self.blobs += 1,
            ObjectType::Tag => self.tags += 1,
            ObjectType::OffsetDelta | ObjectType::OffsetZstdelta => self.offset_deltas += 1,
            ObjectType::HashDelta => self.hash_deltas += 1,
            _ => {}
        }
    }

    /// Returns the number of delta entries in the pack.
    ///
    /// # Parameters
    /// This method does not accept parameters.
    ///
    /// # Returns
    /// The sum of offset-style and hash-style delta entries.
    ///
    /// # Side effects
    /// This method has no side effects.
    pub fn deltas(&self) -> usize {
        self.offset_deltas + self.hash_deltas
    }

    /// Checks whether the pack header object count matches decoded objects.
    ///
    /// # Parameters
    /// This method does not accept parameters.
    ///
    /// # Returns
    /// `true` when `declared_total` equals `decoded_total`.
    ///
    /// # Side effects
    /// This method has no side effects.
    pub fn header_count_matches(&self) -> bool {
        self.declared_total == self.decoded_total
    }

    /// Records one decoded raw object and updates size-related statistics.
    ///
    /// # Parameters
    /// * `object` - The raw object returned by `Pack::decode_pack_object`.
    ///
    /// # Returns
    /// This method does not return a value.
    ///
    /// # Side effects
    /// Mutates the count and largest-object fields in `self`.
    fn record_cache_object(&mut self, object: &CacheObject) {
        // 1. Extract the raw pack object type before delta reconstruction.
        // 2. Use final expanded size for delta entries and decompressed size for base entries.
        // 3. Update counters first, then update largest-object tracking.
        let object_type = object.object_type();
        let object_size = decoded_object_size(object);
        self.record_object_type(object_type);
        if object_size > self.largest_object_size {
            self.largest_object_size = object_size;
            self.largest_object_type = Some(object_type);
        }
    }
}

impl Display for PackStats {
    /// Formats pack statistics as a human-readable report.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&format_pack_stats(self))
    }
}

/// Collects object count and type-distribution statistics from a pack file.
///
/// # Parameters
/// * `pack_path` - Path to the `.pack` file that should be scanned.
///
/// # Returns
/// A `PackStats` value containing raw pack object counts, header count information,
/// delta classification, and largest-object information.
///
/// # Side effects
/// Opens and reads the provided pack file. It does not write to disk.
pub fn collect_pack_stats(pack_path: impl AsRef<Path>) -> Result<PackStats, GitError> {
    let pack_path = pack_path.as_ref();
    let file = fs::File::open(pack_path).map_err(|error| {
        GitError::InvalidPackFile(format!(
            "failed to open pack file `{}`: {error}",
            pack_path.display()
        ))
    })?;
    let mut reader = Wrapper::new(BufReader::new(file));
    let (declared_total, _) = Pack::check_header(&mut reader)?;
    let mut stats = PackStats {
        declared_total: declared_total as usize,
        ..PackStats::default()
    };
    let mut offset = 12usize;

    // 1. Decode each raw pack entry with the existing object decoder.
    // 2. Record raw object types before normal `Pack::decode` rebuilds deltas.
    // 3. Keep the read offset in sync so the next pack entry starts correctly.
    for object_index in 0..stats.declared_total {
        let object = Pack::decode_pack_object(&mut reader, &mut offset)?.ok_or_else(|| {
            GitError::InvalidPackFile(format!("pack object #{object_index} decoded to no object"))
        })?;
        stats.record_cache_object(&object);
    }

    validate_pack_trailer(&mut reader)?;
    Ok(stats)
}

/// Formats `PackStats` as a stable text report.
///
/// # Parameters
/// * `stats` - Statistics to render.
///
/// # Returns
/// A multi-line report containing counts, percentages, and validation information.
///
/// # Side effects
/// This function has no side effects.
pub fn format_pack_stats(stats: &PackStats) -> String {
    // 1. Render header and decoded count validation first.
    // 2. Render each object bucket with a percentage of total objects.
    // 3. Render largest-object information last because it is diagnostic detail.
    let largest_object_type = stats
        .largest_object_type
        .map(|object_type| object_type.to_string())
        .unwrap_or_else(|| "none".to_string());

    [
        "Pack statistics".to_string(),
        format!("total: {}", stats.total),
        format!("declared_total: {}", stats.declared_total),
        format!("decoded_total: {}", stats.decoded_total),
        format!("header_count_matches: {}", stats.header_count_matches()),
        format!(
            "commits: {} ({})",
            stats.commits,
            percentage(stats.commits, stats.total)
        ),
        format!(
            "trees: {} ({})",
            stats.trees,
            percentage(stats.trees, stats.total)
        ),
        format!(
            "blobs: {} ({})",
            stats.blobs,
            percentage(stats.blobs, stats.total)
        ),
        format!(
            "tags: {} ({})",
            stats.tags,
            percentage(stats.tags, stats.total)
        ),
        format!(
            "offset_deltas: {} ({})",
            stats.offset_deltas,
            percentage(stats.offset_deltas, stats.total)
        ),
        format!(
            "hash_deltas: {} ({})",
            stats.hash_deltas,
            percentage(stats.hash_deltas, stats.total)
        ),
        format!(
            "deltas: {} ({})",
            stats.deltas(),
            percentage(stats.deltas(), stats.total)
        ),
        format!("largest_object_type: {largest_object_type}"),
        format!("largest_object_size: {}", stats.largest_object_size),
    ]
    .join("\n")
}

/// Returns the expanded size represented by a raw decoded pack object.
///
/// # Parameters
/// * `object` - Raw decoded pack object.
///
/// # Returns
/// The base object size or the final size declared by a delta entry.
///
/// # Side effects
/// This function has no side effects.
fn decoded_object_size(object: &CacheObject) -> usize {
    match &object.info {
        CacheObjectInfo::BaseObject(_, _) => object.data_decompressed.len(),
        CacheObjectInfo::OffsetDelta(_, final_size)
        | CacheObjectInfo::OffsetZstdelta(_, final_size)
        | CacheObjectInfo::HashDelta(_, final_size) => *final_size,
    }
}

/// Validates the pack trailer hash and end-of-file state.
///
/// # Parameters
/// * `reader` - Pack reader positioned immediately before the trailer hash.
///
/// # Returns
/// `Ok(())` when the computed pack hash matches the trailer and no extra bytes remain.
///
/// # Side effects
/// Consumes the trailer hash bytes from `reader`.
fn validate_pack_trailer<R: BufRead>(reader: &mut Wrapper<R>) -> Result<(), GitError> {
    // 1. Finalize the running hash before reading the trailer itself.
    // 2. Read the trailer hash using the currently configured hash kind.
    // 3. Reject mismatched trailer hashes and trailing garbage.
    let rendered_hash = reader.final_hash();
    let trailer_hash = ObjectHash::from_stream(reader).map_err(|error| {
        GitError::InvalidPackFile(format!("failed to read pack trailer hash: {error}"))
    })?;

    if rendered_hash != trailer_hash {
        return Err(GitError::InvalidPackFile(format!(
            "computed pack hash {rendered_hash} does not match trailer hash {trailer_hash}"
        )));
    }

    if !utils::is_eof(reader) {
        return Err(GitError::InvalidPackFile(
            "pack file has trailing data after trailer hash".to_string(),
        ));
    }

    Ok(())
}

/// Formats a count as a percentage of a total.
///
/// # Parameters
/// * `count` - Bucket count.
/// * `total` - Total object count.
///
/// # Returns
/// A percentage string with two decimal places.
///
/// # Side effects
/// This function has no side effects.
fn percentage(count: usize, total: usize) -> String {
    if total == 0 {
        return "0.00%".to_string();
    }
    format!("{:.2}%", (count as f64 / total as f64) * 100.0)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use tempfile::tempdir;

    use super::*;
    use crate::hash::{HashKind, set_hash_kind_for_test};

    /// Returns an absolute path inside the pack fixture directory.
    fn pack_fixture(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("data")
            .join("packs")
            .join(name)
    }

    #[test]
    fn collect_pack_stats_counts_small_sha1_pack() {
        // 1. Use a known valid SHA-1 pack fixture.
        // 2. Collect raw pack statistics.
        // 3. Verify header counts, bucket totals, and largest-object data.
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let stats = collect_pack_stats(pack_fixture("small-sha1.pack")).unwrap();

        assert!(stats.total > 0);
        assert_eq!(stats.declared_total, stats.decoded_total);
        assert!(stats.header_count_matches());
        assert_eq!(
            stats.total,
            stats.commits + stats.trees + stats.blobs + stats.tags + stats.deltas()
        );
        assert!(stats.largest_object_size > 0);
        assert!(stats.largest_object_type.is_some());
    }

    #[test]
    fn collect_pack_stats_distinguishes_offset_deltas() {
        // 1. Use the medium fixture, which stores offset delta entries.
        // 2. Collect stats before full delta reconstruction.
        // 3. Verify that offset-style deltas are counted separately.
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let stats = collect_pack_stats(pack_fixture("medium-sha1.pack")).unwrap();

        assert!(stats.offset_deltas > 0);
        assert_eq!(stats.deltas(), stats.offset_deltas + stats.hash_deltas);
        assert_eq!(
            stats.total,
            stats.commits + stats.trees + stats.blobs + stats.tags + stats.deltas()
        );
    }

    #[test]
    fn collect_pack_stats_rejects_missing_pack_file() {
        // 1. Build a path that does not exist.
        // 2. Run the stats collector.
        // 3. Verify that the returned error points to file opening.
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let missing_path = tempdir().unwrap().path().join("missing.pack");
        let error = collect_pack_stats(missing_path).unwrap_err();

        assert!(error.to_string().contains("failed to open pack file"));
    }

    #[test]
    fn collect_pack_stats_rejects_invalid_pack_file() {
        // 1. Create a temporary file that is not a valid pack.
        // 2. Run the stats collector.
        // 3. Verify that header validation rejects the file.
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let dir = tempdir().unwrap();
        let invalid_path = dir.path().join("invalid.pack");
        fs::write(&invalid_path, b"not a pack").unwrap();

        let error = collect_pack_stats(invalid_path).unwrap_err();
        assert!(error.to_string().contains("valid pack"));
    }

    #[test]
    fn format_pack_stats_includes_counts_percentages_and_largest_object() {
        // 1. Construct a small synthetic stats value.
        // 2. Render it through the formatting helper and Display implementation.
        // 3. Verify that the report includes counts, percentages, and largest-object fields.
        let stats = PackStats {
            total: 4,
            declared_total: 4,
            decoded_total: 4,
            commits: 1,
            trees: 1,
            blobs: 0,
            tags: 0,
            offset_deltas: 1,
            hash_deltas: 1,
            largest_object_size: 128,
            largest_object_type: Some(ObjectType::Blob),
        };

        let report = format_pack_stats(&stats);
        assert!(report.contains("total: 4"));
        assert!(report.contains("commits: 1 (25.00%)"));
        assert!(report.contains("offset_deltas: 1 (25.00%)"));
        assert!(report.contains("hash_deltas: 1 (25.00%)"));
        assert!(report.contains("deltas: 2 (50.00%)"));
        assert!(report.contains("largest_object_type: blob"));
        assert_eq!(report, stats.to_string());
    }

    #[test]
    fn header_count_matches_detects_mismatched_counts() {
        // 1. Build a stats value with inconsistent header and decoded counts.
        // 2. Check the helper reports the mismatch.
        let stats = PackStats {
            declared_total: 2,
            decoded_total: 1,
            ..PackStats::default()
        };

        assert!(!stats.header_count_matches());
    }
}
