use std::{
    fmt,
    fs::File,
    io::BufReader,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tempfile::TempDir;

use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::{object::types::ObjectType, pack::Pack},
};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PackStats {
    pub total: usize,
    pub commits: usize,
    pub trees: usize,
    pub blobs: usize,
    pub tags: usize,
    pub deltas: usize,
}

impl fmt::Display for PackStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PackStats {{ total: {}, commits: {}, trees: {}, blobs: {}, tags: {}, deltas: {} }}",
            self.total, self.commits, self.trees, self.blobs, self.tags, self.deltas
        )
    }
}

impl PackStats {
    pub fn analyze<P: AsRef<Path>>(pack_path: P) -> Result<PackStats, GitError> {
        let pack_path = pack_path.as_ref();
        if !pack_path.exists() {
            return Err(GitError::InvalidPackFile(format!(
                "Pack file not found: {}",
                pack_path.display()
            )));
        }

        let f = File::open(pack_path)
            .map_err(|e| GitError::InvalidPackFile(format!("Failed to open pack file: {e}")))?;
        let mut reader = BufReader::new(f);

        let temp_dir = TempDir::new()
            .map_err(|e| GitError::InvalidPackFile(format!("Failed to create temp dir: {e}")))?;
        let mut pack = Pack::new(None, None, Some(temp_dir.path().to_path_buf()), true);

        let stats = Arc::new(AtomicPackStats::default());
        let stats_cloned = Arc::clone(&stats);

        pack.decode(
            &mut reader,
            move |entry| {
                let obj_type = entry.inner.obj_type;
                let is_delta_in_pack = entry.meta.is_delta.unwrap_or(false);
                stats_cloned.count_object_type(obj_type, is_delta_in_pack);
            },
            None::<fn(ObjectHash)>,
        )?;

        Ok(stats.snapshot())
    }

    pub fn validate_header<P: AsRef<Path>>(pack_path: P) -> Result<u32, GitError> {
        let pack_path = pack_path.as_ref();
        if !pack_path.exists() {
            return Err(GitError::InvalidPackFile(format!(
                "Pack file not found: {}",
                pack_path.display()
            )));
        }

        let f = File::open(pack_path)
            .map_err(|e| GitError::InvalidPackFile(format!("Failed to open pack file: {e}")))?;
        let mut reader = BufReader::new(f);

        let (count, _) = Pack::check_header(&mut reader)?;
        Ok(count)
    }
}

#[derive(Default)]
struct AtomicPackStats {
    total: AtomicUsize,
    commits: AtomicUsize,
    trees: AtomicUsize,
    blobs: AtomicUsize,
    tags: AtomicUsize,
    deltas: AtomicUsize,
}

impl AtomicPackStats {
    fn count_object_type(&self, obj_type: ObjectType, is_delta_in_pack: bool) {
        self.total.fetch_add(1, Ordering::Relaxed);
        if is_delta_in_pack {
            self.deltas.fetch_add(1, Ordering::Relaxed);
            return;
        }

        match obj_type {
            ObjectType::Commit => {
                self.commits.fetch_add(1, Ordering::Relaxed);
            }
            ObjectType::Tree => {
                self.trees.fetch_add(1, Ordering::Relaxed);
            }
            ObjectType::Blob => {
                self.blobs.fetch_add(1, Ordering::Relaxed);
            }
            ObjectType::Tag => {
                self.tags.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn snapshot(&self) -> PackStats {
        PackStats {
            total: self.total.load(Ordering::Relaxed),
            commits: self.commits.load(Ordering::Relaxed),
            trees: self.trees.load(Ordering::Relaxed),
            blobs: self.blobs.load(Ordering::Relaxed),
            tags: self.tags.load(Ordering::Relaxed),
            deltas: self.deltas.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        hash::{HashKind, set_hash_kind_for_test},
        internal::pack::test_pack_download::download_pack_file,
    };

    #[test]
    fn test_analyze_small_pack_sha1() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let (pack_path, _dl_guard) = download_pack_file("small-sha1.pack");
        let stats = PackStats::analyze(pack_path).expect("Failed to analyze");

        assert!(stats.total > 0);
        assert_eq!(
            stats.total,
            stats.commits + stats.trees + stats.blobs + stats.tags + stats.deltas
        );
    }

    #[test]
    fn test_analyze_small_pack_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let (pack_path, _dl_guard) = download_pack_file("small-sha256.pack");
        let stats = PackStats::analyze(pack_path).expect("Failed to analyze");

        assert!(stats.total > 0);
        assert_eq!(
            stats.total,
            stats.commits + stats.trees + stats.blobs + stats.tags + stats.deltas
        );
    }

    #[test]
    fn test_analyze_delta_pack_sha1() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let (pack_path, _dl_guard) = download_pack_file("ref-delta-sha1.pack");
        let stats = PackStats::analyze(pack_path).expect("Failed to analyze");

        assert!(stats.total > 0);

        assert_eq!(
            stats.total,
            stats.commits + stats.trees + stats.blobs + stats.tags + stats.deltas
        );
    }

    #[test]
    fn test_analyze_delta_pack_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let (pack_path, _dl_guard) = download_pack_file("ref-delta-sha256.pack");
        let stats = PackStats::analyze(pack_path).expect("Failed to analyze");

        assert!(stats.total > 0);
        assert_eq!(
            stats.total,
            stats.commits + stats.trees + stats.blobs + stats.tags + stats.deltas
        );
    }

    #[test]
    fn test_nonexistent_file() {
        let result = PackStats::analyze("tests/data/packs/nonexistent.pack");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_pack_file() {
        use std::io::Write;

        use tempfile::NamedTempFile;

        let mut temp = NamedTempFile::new().expect("create temp file");
        temp.write_all(b"XXXX").expect("write temp file");
        temp.flush().expect("flush temp file");

        let result = PackStats::analyze(temp.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_header() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let (pack_path, _dl_guard) = download_pack_file("small-sha1.pack");
        let result = PackStats::validate_header(pack_path);
        assert!(result.is_ok());
        assert!(result.unwrap() > 0);
    }

    #[test]
    fn test_validate_header_nonexistent() {
        let result = PackStats::validate_header("tests/data/packs/nonexistent.pack");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_header_invalid_file() {
        use std::io::Write;

        use tempfile::NamedTempFile;

        let mut temp = NamedTempFile::new().expect("create temp file");
        temp.write_all(b"XX").expect("write temp file");
        temp.flush().expect("flush temp file");

        let result = PackStats::validate_header(temp.path());
        assert!(result.is_err());
    }
}
