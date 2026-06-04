use std::{
    fmt,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

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
    fn count_object_type(&mut self, obj_type: ObjectType, is_delta_in_pack: bool) {
        self.total += 1;
        if is_delta_in_pack {
            self.deltas += 1;
            return;
        }

        match obj_type {
            ObjectType::Commit => self.commits += 1,
            ObjectType::Tree => self.trees += 1,
            ObjectType::Blob => self.blobs += 1,
            ObjectType::Tag => self.tags += 1,
            _ => {}
        }
    }

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

        let tmp = PathBuf::from("./.cache_temp");
        let mut pack = Pack::new(None, None, Some(tmp), true);

        let stats = Arc::new(Mutex::new(PackStats::default()));
        let stats_cloned = Arc::clone(&stats);

        pack.decode(
            &mut reader,
            move |entry| {
                let obj_type = entry.inner.obj_type;
                let is_delta_in_pack = entry.meta.is_delta.unwrap_or(false);
                let mut guard = match stats_cloned.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                guard.count_object_type(obj_type, is_delta_in_pack);
            },
            None::<fn(ObjectHash)>,
        )?;

        let result = match stats.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        Ok(result)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::{HashKind, set_hash_kind_for_test};

    #[test]
    fn test_analyze_small_pack_sha1() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let stats =
            PackStats::analyze("tests/data/packs/small-sha1.pack").expect("Failed to analyze");

        assert!(stats.total > 0);
        assert_eq!(
            stats.total,
            stats.commits + stats.trees + stats.blobs + stats.tags + stats.deltas
        );
    }

    #[test]
    fn test_analyze_small_pack_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let stats =
            PackStats::analyze("tests/data/packs/small-sha256.pack").expect("Failed to analyze");

        assert!(stats.total > 0);
        assert_eq!(
            stats.total,
            stats.commits + stats.trees + stats.blobs + stats.tags + stats.deltas
        );
    }

    #[test]
    fn test_analyze_delta_pack_sha1() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let stats =
            PackStats::analyze("tests/data/packs/ref-delta-sha1.pack").expect("Failed to analyze");

        assert!(stats.total > 0);

        assert_eq!(
            stats.total,
            stats.commits + stats.trees + stats.blobs + stats.tags + stats.deltas
        );
    }

    #[test]
    fn test_analyze_delta_pack_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let stats = PackStats::analyze("tests/data/packs/ref-delta-sha256.pack")
            .expect("Failed to analyze");

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
        let result = PackStats::validate_header("tests/data/packs/small-sha1.pack");
        assert!(result.is_ok());
        assert!(result.unwrap() > 0);
    }
}
