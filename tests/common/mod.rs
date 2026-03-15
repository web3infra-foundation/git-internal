//! Shared test helpers for integration tests.

use std::path::PathBuf;
use std::sync::LazyLock;

const BASE_URL: &str = "https://download.libra.tools/libra/development/pack";

fn download_dir() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data/packs");
    std::fs::create_dir_all(&dir).expect("create download dir");
    dir
}

static DOWNLOAD_LOCK: LazyLock<std::sync::Mutex<()>> = LazyLock::new(|| std::sync::Mutex::new(()));

fn ensure_downloaded(filename: &str) -> PathBuf {
    let path = download_dir().join(filename);
    if path.exists() {
        return path;
    }
    let _lock = DOWNLOAD_LOCK.lock().unwrap();
    if path.exists() {
        return path;
    }
    let url = format!("{BASE_URL}/{filename}");
    let response = ureq::get(&url).call()
        .unwrap_or_else(|e| panic!("failed to download {url}: {e}"));
    let bytes = response.into_body().read_to_vec()
        .unwrap_or_else(|e| panic!("failed to read response body for {url}: {e}"));
    std::fs::write(&path, &bytes).unwrap_or_else(|e| panic!("failed to write {}: {e}", path.display()));
    path
}

pub struct PackFileGuard {
    path: PathBuf,
}

impl Drop for PackFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
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
    let guard = PackFileGuard { path: path.clone() };
    (path, guard)
}
