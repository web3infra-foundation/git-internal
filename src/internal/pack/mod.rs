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

#[cfg(test)]
pub mod test_pack_download;

pub mod utils;
pub mod waitlist;
pub mod wrapper;
use std::sync::{Arc, atomic::AtomicUsize};

use threadpool::ThreadPool;

use crate::{
    hash::ObjectHash,
    internal::{
        object::ObjectTrait,
        pack::{cache::Caches, waitlist::Waitlist},
    },
};

const DEFAULT_TMP_DIR: &str = "./.cache_temp";

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PackStats {
    pub total: usize,
    pub commits: usize,
    pub trees: usize,
    pub blobs: usize,
    pub tags: usize,
    pub deltas: usize,
}

// Implement Display trait for user-friendly formatting and percentage calculation
impl std::fmt::Display for PackStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Internal closure to safely calculate percentages (preventing divide-by-zero)
        let pct = |count: usize| -> f64 {
            if self.total == 0 {
                0.0
            } else {
                (count as f64 / self.total as f64) * 100.0
            }
        };

        write!(
            f,
            "📦 Pack Decode Statistics Summary\n\
             ======================================\n\
             Total Objects: {}\n\
             - Commits: {:>6}  ({:>5.1}%)\n\
             - Trees:   {:>6}  ({:>5.1}%)\n\
             - Blobs:   {:>6}  ({:>5.1}%)\n\
             - Tags:    {:>6}  ({:>5.1}%)\n\
             - Deltas:  {:>6}  ({:>5.1}%)\n\
             ======================================",
            self.total,
            self.commits,
            pct(self.commits),
            self.trees,
            pct(self.trees),
            self.blobs,
            pct(self.blobs),
            self.tags,
            pct(self.tags),
            self.deltas,
            pct(self.deltas)
        )
    }
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
    pub stats: PackStats, // Statistics field for tracking object distribution
}

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
