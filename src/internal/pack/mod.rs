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
    path::Path,
    sync::{Arc, atomic::AtomicUsize},
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PackStats {
    pub total: usize,
    pub commits: usize,
    pub trees: usize,
    pub blobs: usize,
    pub tags: usize,
    pub deltas: usize,
}

impl PackStats {
    pub(crate) fn record(&mut self, obj_type: ObjectType, is_delta: bool) {
        self.total += 1;

        if is_delta {
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
}

/// Decode a pack file and return object count statistics.
///
/// This uses the current thread-local hash kind, just like [`Pack::decode`].
pub fn decode_pack_stats(path: impl AsRef<Path>) -> Result<PackStats, GitError> {
    Pack::decode_stats(path)
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
