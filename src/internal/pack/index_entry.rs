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

/// Git index entry corresponding to a pack entry
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
    /// Create a new IndexEntry from a pack Entry and its offset in the pack file.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        hash::{HashKind, ObjectHash, set_hash_kind_for_test},
        internal::{
            metadata::{EntryMeta, MetaAttached},
            object::types::ObjectType,
            pack::entry::Entry,
        },
    };

    /// Helper to create a test Entry with given content.
    fn create_test_entry(content: &[u8]) -> Entry {
        Entry {
            obj_type: ObjectType::Blob,
            data: content.to_vec(),
            hash: ObjectHash::new(content),
            chain_len: 0,
        }
    }

    #[test]
    fn test_index_entry_new() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let entry = create_test_entry(b"test data");
        let offset = 123;

        let index_entry = IndexEntry::new(&entry, offset);

        assert_eq!(index_entry.hash, entry.hash);
        assert_eq!(index_entry.offset, offset as u64);

        let mut hasher = Hasher::new();
        hasher.update(b"test data");
        assert_eq!(index_entry.crc32, hasher.finalize());
    }

    #[test]
    fn test_try_from_meta_attached_with_crc() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let entry = create_test_entry(b"test data");
        let meta = EntryMeta {
            pack_offset: Some(456),
            crc32: Some(0x12345678),
            ..Default::default()
        };
        let meta_attached = MetaAttached { inner: entry, meta };

        let index_entry = IndexEntry::try_from(&meta_attached).unwrap();

        assert_eq!(index_entry.hash, meta_attached.inner.hash);
        assert_eq!(index_entry.offset, 456);
        assert_eq!(index_entry.crc32, 0x12345678);
    }

    #[test]
    fn test_try_from_meta_attached_crc_fallback() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let entry_data = b"fallback crc";
        let entry = create_test_entry(entry_data);
        let meta = EntryMeta {
            pack_offset: Some(789),
            crc32: None, // CRC is not provided in meta
            ..Default::default()
        };
        let meta_attached = MetaAttached { inner: entry, meta };

        let index_entry = IndexEntry::try_from(&meta_attached).unwrap();

        assert_eq!(index_entry.hash, meta_attached.inner.hash);
        assert_eq!(index_entry.offset, 789);

        let mut hasher = Hasher::new();
        hasher.update(entry_data);
        assert_eq!(index_entry.crc32, hasher.finalize());
    }

    #[test]
    fn test_try_from_meta_attached_no_offset() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let entry = create_test_entry(b"no offset");
        let meta = EntryMeta {
            pack_offset: None, // Offset is not provided
            ..Default::default()
        };
        let meta_attached = MetaAttached { inner: entry, meta };

        let result = IndexEntry::try_from(&meta_attached);
        assert!(result.is_err());
        match result.unwrap_err() {
            GitError::ConversionError(msg) => {
                assert_eq!(msg, "empty offset in pack entry");
            }
            _ => panic!("Expected ConversionError"),
        }
    }
}
