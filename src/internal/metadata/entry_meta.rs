//! Metadata container that accompanies pack entries to track file paths, on-disk offsets, CRC32
//! checksums, and delta flags so downstream encoders/decoders can enrich responses.

/// Metadata about a pack entry.
#[derive(Debug, Clone, Default)]
pub struct EntryMeta {
    pub file_path: Option<String>,

    pub pack_id: Option<String>,

    /// Offset within the pack file
    pub pack_offset: Option<usize>,
    /// CRC32 checksum of the compressed object data (including header)
    pub crc32: Option<u32>,

    pub is_delta: Option<bool>,
}

impl EntryMeta {
    /// Create a new empty metadata container.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the file path associated with this entry.
    pub fn set_pack_id(&mut self, id: impl Into<String>) -> &mut Self {
        self.pack_id = Some(id.into());
        self
    }

    /// Set the file path associated with this entry.
    pub fn set_crc32(&mut self, crc32: u32) -> &mut Self {
        self.crc32 = Some(crc32);
        self
    }
}
