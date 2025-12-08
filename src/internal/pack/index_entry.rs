use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use crate::errors::GitError;
use crate::hash::{ObjectHash, SHA1};
use crate::internal::metadata::{EntryMeta, MetaAttached};
use crate::internal::pack::entry::Entry;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexEntry {
    pub hash: ObjectHash,
    pub crc32: u32,
    pub offset: u64, // 64-bit because offsets may exceed 32-bit
}

impl TryFrom<&MetaAttached<Entry, EntryMeta>> for IndexEntry {
    type Error = GitError;

    fn try_from(pack_entry: &MetaAttached<Entry, EntryMeta>) -> Result<Self, GitError> {
        let offset = pack_entry.meta.pack_offset.ok_or(GitError::ConversionError(String::from("empty offset in pack entry")))?;
        Ok(IndexEntry {
            hash: pack_entry.inner.hash,
            crc32: calculate_crc32(&pack_entry.inner.data),
            offset: offset as u64,
        })
    }
}

impl IndexEntry {
    
}

fn calculate_crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}