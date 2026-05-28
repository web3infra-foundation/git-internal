//! Shared test helpers for integration tests.

use std::{
    collections::HashMap,
    io::Read,
    path::{Path, PathBuf},
    sync::{
        LazyLock, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

const BASE_URL: &str = "https://download.libra.tools/libra/development/pack";

fn download_dir() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data/packs");
    std::fs::create_dir_all(&dir).expect("create download dir");
    dir
}

static REF_COUNTS: LazyLock<Mutex<HashMap<PathBuf, &'static AtomicUsize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn lock_ref_counts() -> std::sync::MutexGuard<'static, HashMap<PathBuf, &'static AtomicUsize>> {
    REF_COUNTS.lock().unwrap_or_else(|e| e.into_inner())
}

fn acquire_ref(path: &Path) -> &'static AtomicUsize {
    let mut map = lock_ref_counts();
    let counter = map
        .entry(path.to_path_buf())
        .or_insert_with(|| Box::leak(Box::new(AtomicUsize::new(0))));
    counter.fetch_add(1, Ordering::Relaxed);
    counter
}

fn release_ref(path: &Path) -> bool {
    let map = lock_ref_counts();
    if let Some(counter) = map.get(path) {
        counter.fetch_sub(1, Ordering::Relaxed) == 1
    } else {
        true
    }
}

static DOWNLOAD_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn ensure_downloaded(filename: &str) -> PathBuf {
    let path = download_dir().join(filename);
    if path.exists() {
        return path;
    }
    let _lock = DOWNLOAD_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    if path.exists() {
        return path;
    }
    let url = format!("{BASE_URL}/{filename}");
    let mut response = ureq::get(&url)
        .call()
        .unwrap_or_else(|e| panic!("failed to download {url}: {e}"));
    let mut bytes = Vec::new();
    response
        .body_mut()
        .as_reader()
        .read_to_end(&mut bytes)
        .unwrap_or_else(|e| panic!("failed to read response body for {url}: {e}"));
    std::fs::write(&path, &bytes)
        .unwrap_or_else(|e| panic!("failed to write {}: {e}", path.display()));
    path
}

pub struct PackFileGuard {
    path: PathBuf,
}

impl Drop for PackFileGuard {
    fn drop(&mut self) {
        if release_ref(&self.path) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[allow(dead_code)]
pub fn download_pack_file(filename: &str) -> (PathBuf, PackFileGuard) {
    let path = ensure_downloaded(filename);
    if filename.ends_with(".pack") {
        let idx = filename.replace(".pack", ".idx");
        let _ = ensure_downloaded(&idx);
    } else if filename.ends_with(".idx") {
        let pack = filename.replace(".idx", ".pack");
        let _ = ensure_downloaded(&pack);
    }
    acquire_ref(&path);
    let guard = PackFileGuard { path: path.clone() };
    (path, guard)
}
