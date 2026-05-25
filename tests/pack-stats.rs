use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::{LazyLock, Mutex},
};

mod common;

use common::download_pack_file;
use git_internal::{
    errors::GitError,
    hash::{HashKind, set_hash_kind_for_test},
    internal::pack::{
        DecodeStatsOptions, PackStats, decode_pack_stats, decode_pack_stats_with_options,
    },
};

fn pack_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/packs")
        .join(name)
}

static CURRENT_DIR_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct CurrentDirGuard {
    original: PathBuf,
}

impl CurrentDirGuard {
    fn enter(path: &Path) -> Self {
        let original = env::current_dir().expect("read current dir");
        env::set_current_dir(path).expect("set current dir");
        Self { original }
    }
}

impl Drop for CurrentDirGuard {
    fn drop(&mut self) {
        env::set_current_dir(&self.original).expect("restore current dir");
    }
}

fn decode_pack_stats_serial(path: impl AsRef<Path>) -> Result<PackStats, GitError> {
    let _guard = CURRENT_DIR_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    decode_pack_stats(path)
}

#[test]
fn decode_pack_stats_counts_small_pack() -> Result<(), GitError> {
    let _guard = set_hash_kind_for_test(HashKind::Sha1);
    let (pack_path, _download_guard) = download_pack_file("small-sha1.pack");

    let stats = decode_pack_stats_serial(pack_path)?;

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
    let (pack_path, _download_guard) = download_pack_file("encode-test-sha1.pack");

    let stats = decode_pack_stats_serial(pack_path)?;

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
    let err =
        decode_pack_stats_serial(pack_path("missing.pack")).expect_err("missing file must fail");

    assert!(matches!(err, GitError::IOError(_)));
}

#[test]
fn decode_pack_stats_returns_error_for_invalid_pack_header() {
    let file = tempfile::NamedTempFile::new().expect("create temporary pack");
    fs::write(file.path(), b"NOPE").expect("write invalid pack header");

    let err = decode_pack_stats_serial(file.path()).expect_err("invalid pack header must fail");

    assert!(matches!(err, GitError::InvalidPackHeader(_)));
}

#[test]
fn decode_pack_stats_returns_error_when_cache_dir_cannot_be_created() {
    let file = tempfile::NamedTempFile::new().expect("create temporary pack");
    fs::write(file.path(), b"NOPE").expect("write invalid pack header");
    let pack_path = file.path().canonicalize().expect("canonicalize pack path");

    let cwd = tempfile::tempdir().expect("create temporary cwd");
    fs::write(cwd.path().join(".cache_temp"), b"not a directory").expect("block cache dir");

    let _guard = CURRENT_DIR_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _cwd = CurrentDirGuard::enter(cwd.path());

    let err = decode_pack_stats(pack_path).expect_err("cache dir creation must fail");

    match err {
        GitError::IOError(err) => {
            assert!(
                err.to_string()
                    .contains("failed to create pack cache directory")
            );
        }
        other => panic!("expected IO error, got {other:?}"),
    }
}

#[test]
fn decode_pack_stats_allows_custom_cache_dir() {
    let file = tempfile::NamedTempFile::new().expect("create temporary pack");
    fs::write(file.path(), b"NOPE").expect("write invalid pack header");
    let pack_path = file.path().canonicalize().expect("canonicalize pack path");

    let cwd = tempfile::tempdir().expect("create temporary cwd");
    fs::write(cwd.path().join(".cache_temp"), b"not a directory").expect("block cache dir");
    let cache_dir = tempfile::tempdir().expect("create custom cache dir");

    let _guard = CURRENT_DIR_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _cwd = CurrentDirGuard::enter(cwd.path());

    let err = decode_pack_stats_with_options(
        pack_path,
        DecodeStatsOptions {
            temp_path: Some(cache_dir.path().to_path_buf()),
            ..DecodeStatsOptions::default()
        },
    )
    .expect_err("invalid pack header must fail after custom cache setup");

    assert!(matches!(err, GitError::InvalidPackHeader(_)));
}
