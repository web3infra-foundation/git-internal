use std::{fs, path::PathBuf};

use git_internal::{
    errors::GitError,
    hash::{HashKind, set_hash_kind_for_test},
    internal::pack::{PackStats, decode_pack_stats},
};

fn pack_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/packs")
        .join(name)
}

#[test]
fn decode_pack_stats_counts_small_pack() -> Result<(), GitError> {
    let _guard = set_hash_kind_for_test(HashKind::Sha1);

    let stats = decode_pack_stats(pack_path("small-sha1.pack"))?;

    assert_eq!(
        stats,
        PackStats {
            total: 19,
            commits: 2,
            trees: 2,
            blobs: 15,
            tags: 0,
            deltas: 0,
        }
    );
    Ok(())
}

#[test]
fn decode_pack_stats_counts_delta_entries() -> Result<(), GitError> {
    let _guard = set_hash_kind_for_test(HashKind::Sha1);

    let stats = decode_pack_stats(pack_path("encode-test-sha1.pack"))?;

    assert_eq!(
        stats,
        PackStats {
            total: 5030,
            commits: 10,
            trees: 20,
            blobs: 511,
            tags: 0,
            deltas: 4489,
        }
    );
    Ok(())
}

#[test]
fn decode_pack_stats_returns_error_for_missing_path() {
    let err = decode_pack_stats(pack_path("missing.pack")).expect_err("missing file must fail");

    assert!(matches!(err, GitError::IOError(_)));
}

#[test]
fn decode_pack_stats_returns_error_for_invalid_pack_header() {
    let file = tempfile::NamedTempFile::new().expect("create temporary pack");
    fs::write(file.path(), b"NOPE").expect("write invalid pack header");

    let err = decode_pack_stats(file.path()).expect_err("invalid pack header must fail");

    assert!(matches!(err, GitError::InvalidPackHeader(_)));
}
