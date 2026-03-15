//! Test helper: download pack files from remote on demand and clean up after use.

use std::path::PathBuf;
use std::sync::LazyLock;

const BASE_URL: &str = "https://download.libra.tools/libra/development/pack";

/// Directory for caching downloaded pack files during test runs.
fn download_dir() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data/packs");
    std::fs::create_dir_all(&dir).expect("create download dir");
    dir
}

/// A mutex to serialize downloads per filename and avoid races.
static DOWNLOAD_LOCK: LazyLock<std::sync::Mutex<()>> = LazyLock::new(|| std::sync::Mutex::new(()));

/// Download a pack/idx file if not already present, returning the local path.
fn ensure_downloaded(filename: &str) -> PathBuf {
    let path = download_dir().join(filename);
    if path.exists() {
        return path;
    }
    let _lock = DOWNLOAD_LOCK.lock().unwrap();
    // Double-check after acquiring lock.
    if path.exists() {
        return path;
    }
    let url = format!("{BASE_URL}/{filename}");
    tracing::info!("Downloading test pack file: {url}");
    let response = ureq::get(&url).call()
        .unwrap_or_else(|e| panic!("failed to download {url}: {e}"));
    let bytes = response.into_body().read_to_vec()
        .unwrap_or_else(|e| panic!("failed to read response body for {url}: {e}"));
    std::fs::write(&path, &bytes).unwrap_or_else(|e| panic!("failed to write {}: {e}", path.display()));
    tracing::info!("Downloaded {} ({} bytes)", filename, bytes.len());
    path
}

/// Guard that deletes the downloaded file when dropped.
pub struct PackFileGuard {
    path: PathBuf,
}

impl Drop for PackFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Download a pack file (and its companion .idx if the file is a .pack),
/// returning `(path, guard)`. The file is deleted when the guard is dropped.
pub fn download_pack_file(filename: &str) -> (PathBuf, PackFileGuard) {
    let path = ensure_downloaded(filename);
    // Also download the companion file (.pack ↔ .idx).
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
