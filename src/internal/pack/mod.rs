//! Pack file encoder/decoder implementations, caches, waitlists, and stream wrappers that faithfully
//! follow the [pack-format spec](https://git-scm.com/docs/pack-format).

pub mod cache;
pub mod cache_object;
pub mod channel_reader;
pub mod decode;
pub mod encode;
pub mod entry;
mod index_entry;
pub mod pack_index;
pub mod utils;
pub mod waitlist;
pub mod wrapper;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use threadpool::ThreadPool;

use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::{
        object::{ObjectTrait, types::ObjectType},
        pack::{cache::Caches, waitlist::Waitlist},
    },
};

const DEFAULT_TMP_DIR: &str = "./.cache_temp";
const DEFAULT_DECODE_STATS_MEM_LIMIT: usize = 100 * 1024 * 1024;

/// Configuration for [`decode_pack_stats_with_options`] and
/// [`Pack::decode_stats_with_options`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeStatsOptions {
    /// The number of decoder/cache threads to use, or `None` to use the number
    /// of logical CPUs.
    pub thread_num: Option<usize>,
    /// The memory cache limit in bytes, or `None` for unlimited.
    pub mem_limit: Option<usize>,
    /// Directory used for temporary pack-cache files.
    ///
    /// `None` uses the decoder default, `./.cache_temp`.
    pub temp_path: Option<PathBuf>,
    /// Whether to remove the temporary cache directory when the decoder is
    /// dropped.
    pub clean_tmp: bool,
}

impl Default for DecodeStatsOptions {
    fn default() -> Self {
        Self {
            thread_num: None,
            mem_limit: Some(DEFAULT_DECODE_STATS_MEM_LIMIT),
            temp_path: None,
            clean_tmp: true,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PackStats {
    pub total: usize,
    pub commits: usize,
    pub trees: usize,
    pub blobs: usize,
    pub tags: usize,
    pub deltas: usize,
}

#[derive(Debug, Default)]
pub(crate) struct PackStatsCounters {
    total: AtomicUsize,
    commits: AtomicUsize,
    trees: AtomicUsize,
    blobs: AtomicUsize,
    tags: AtomicUsize,
    deltas: AtomicUsize,
}

impl PackStatsCounters {
    pub(crate) fn record(&self, obj_type: ObjectType, is_delta: bool) {
        self.total.fetch_add(1, Ordering::Relaxed);

        if is_delta {
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

    pub(crate) fn snapshot(&self) -> PackStats {
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

/// Decode a pack file and return object count statistics.
///
/// This uses the current thread-local hash kind, just like [`Pack::decode`].
/// It uses [`DecodeStatsOptions::default`], which applies a 100 MiB memory
/// cache limit and the default relative temporary directory, `./.cache_temp`.
pub fn decode_pack_stats(path: impl AsRef<Path>) -> Result<PackStats, GitError> {
    Pack::decode_stats(path)
}

/// Decode a pack file and return object count statistics with explicit cache
/// and threading options.
///
/// This uses the current thread-local hash kind, just like [`Pack::decode`].
pub fn decode_pack_stats_with_options(
    path: impl AsRef<Path>,
    options: DecodeStatsOptions,
) -> Result<PackStats, GitError> {
    Pack::decode_stats_with_options(path, options)
}

/// Representation of a Git pack file in memory.
pub struct Pack {
    pub number: usize,
    pub signature: ObjectHash,
    pub objects: Vec<Box<dyn ObjectTrait>>,
    pub pool: Arc<ThreadPool>,
    pub waitlist: Arc<Waitlist>,
    pub caches: Arc<Caches>,
    pub mem_limit: Option<usize>,
    pub cache_objs_mem: Arc<AtomicUsize>,
    pub clean_tmp: bool,
}

#[cfg(test)]
pub(crate) mod test_pack_download;

#[cfg(test)]
mod tests {
    use tracing_subscriber::util::SubscriberInitExt;

    /// CAUTION: This two is same
    /// 1.
    /// tracing_subscriber::fmt().init();
    ///
    /// 2.
    /// env::set_var("RUST_LOG", "debug"); // must be set if use `fmt::init()`, or no output
    /// tracing_subscriber::fmt::init();
    pub(crate) fn init_logger() {
        let _ = tracing_subscriber::fmt::Subscriber::builder()
            .with_target(false)
            .without_time()
            .with_level(true)
            .with_max_level(tracing::Level::DEBUG)
            .finish()
            .try_init(); // avoid multi-init
    }
}
