//! Representation of a single `.idx` entry including precomputed CRC32 and offset extraction from
//! decoded pack metadata.

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::{
        metadata::{EntryMeta, MetaAttached},
        pack::entry::Entry,
    },
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexEntry {
    pub hash: ObjectHash,
    pub crc32: u32,
    pub offset: u64, // 64-bit because offsets may exceed 32-bit
}

impl TryFrom<&MetaAttached<Entry, EntryMeta>> for IndexEntry {
    type Error = GitError;

    fn try_from(pack_entry: &MetaAttached<Entry, EntryMeta>) -> Result<Self, GitError> {
        let offset = pack_entry
            .meta
            .pack_offset
            .ok_or(GitError::ConversionError(String::from(
                "empty offset in pack entry",
            )))?;
        // Use the CRC32 from metadata if available (calculated from compressed data),
        // otherwise fallback to calculating it from decompressed data (which is technically wrong for .idx but handles legacy cases)
        let crc32 = pack_entry
            .meta
            .crc32
            .unwrap_or_else(|| calculate_crc32(&pack_entry.inner.data));
        Ok(IndexEntry {
            hash: pack_entry.inner.hash,
            crc32,
            offset: offset as u64,
        })
    }
}

impl IndexEntry {
    pub fn new(entry: &Entry, offset: usize) -> Self {
        IndexEntry {
            hash: entry.hash,
            crc32: calculate_crc32(&entry.data),
            offset: offset as u64,
        }
    }
}

fn calculate_crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}
