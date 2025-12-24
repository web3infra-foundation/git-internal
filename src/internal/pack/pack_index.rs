//! Builder for Git pack index (.idx) files that streams fanout tables, CRCs, offsets, and trailer
//! hashes through an async channel.

use tokio::sync::mpsc;

pub use crate::internal::pack::index_entry::IndexEntry;
use crate::{errors::GitError, hash::ObjectHash, utils::HashAlgorithm};

/// Builder for Git pack index (.idx) files that streams data through an async channel.
/// # Arguments
/// * `object_number` - Total number of objects in the pack file.
/// * `sender` - Async channel sender to stream idx data.
/// * `pack_hash` - Hash of the corresponding pack file (used in the idx trailer).
/// * `inner_hash` - Hash algorithm instance to compute the idx file hash.
pub struct IdxBuilder {
    sender: Option<mpsc::Sender<Vec<u8>>>,
    inner_hash: HashAlgorithm, //  idx trailer
    object_number: usize,
    pack_hash: ObjectHash,
}

impl IdxBuilder {
    /// Create a new IdxBuilder.
    pub fn new(object_number: usize, sender: mpsc::Sender<Vec<u8>>, pack_hash: ObjectHash) -> Self {
        Self {
            sender: Some(sender),
            inner_hash: HashAlgorithm::new(),
            object_number,
            pack_hash,
        }
    }

    /// Drop the sender to close the channel.
    pub fn drop_sender(&mut self) {
        self.sender.take(); // Take the sender out, dropping it
    }

    /// Send data through the channel and update the inner hash.
    async fn send_data(&mut self, data: Vec<u8>) -> Result<(), GitError> {
        if let Some(sender) = &self.sender {
            self.inner_hash.update(&data);
            sender.send(data).await.map_err(|e| {
                GitError::IOError(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    format!("Failed to send idx data: {e}"),
                ))
            })?;
        }
        Ok(())
    }

    /// Send data through the channel without updating the inner hash.
    async fn send_data_without_update_hash(&mut self, data: Vec<u8>) -> Result<(), GitError> {
        if let Some(sender) = &self.sender {
            sender.send(data).await.map_err(|e| {
                GitError::IOError(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    format!("Failed to send idx data: {e}"),
                ))
            })?;
        }
        Ok(())
    }

    /// send u32 value (big-endian)
    async fn send_u32(&mut self, v: u32) -> Result<(), GitError> {
        self.send_data(v.to_be_bytes().to_vec()).await
    }

    /// send u64 value (big-endian)
    async fn send_u64(&mut self, v: u64) -> Result<(), GitError> {
        self.send_data(v.to_be_bytes().to_vec()).await
    }

    /// Write the idx v2 header (Git pack index format, used for both SHA1 and SHA256).
    /// The 4-byte pack index signature: \377t0c, followed by 4-byte version number: 2.
    async fn write_header(&mut self) -> Result<(), GitError> {
        // .idx v2 header (used for both SHA1 and SHA256)
        // magic: FF 74 4F 63, version: 2
        let header: [u8; 8] = [0xFF, 0x74, 0x4F, 0x63, 0, 0, 0, 2];
        self.send_data(header.to_vec()).await
    }

    /// Write the fanout table for the index.
    async fn write_fanout(&mut self, entries: &mut [IndexEntry]) -> Result<(), GitError> {
        entries.sort_by(|a, b| a.hash.cmp(&b.hash));
        let mut fanout = [0u32; 256];
        for entry in entries.iter() {
            fanout[entry.hash.to_data()[0] as usize] += 1;
        }

        // Calculate cumulative counts
        for i in 1..fanout.len() {
            fanout[i] += fanout[i - 1];
        }

        // Send all 256 cumulative counts
        for &count in fanout.iter() {
            self.send_u32(count).await?;
        }

        Ok(())
    }

    /// Write the object names (hashes) to the index.
    async fn write_names(&mut self, entries: &Vec<IndexEntry>) -> Result<(), GitError> {
        for e in entries {
            self.send_data(e.hash.to_data().clone()).await?;
        }

        Ok(())
    }

    /// Write the CRC32 checksums for each object in the index.
    async fn write_crc32(&mut self, entries: &Vec<IndexEntry>) -> Result<(), GitError> {
        for e in entries {
            self.send_u32(e.crc32).await?;
        }

        Ok(())
    }

    /// Write the offsets for each object in the index, handling large offsets.
    async fn write_offsets(&mut self, entries: &Vec<IndexEntry>) -> Result<(), GitError> {
        let mut large = vec![];
        for e in entries {
            if e.offset <= 0x7FFF_FFFF {
                // normal 31-bit offset
                self.send_u32(e.offset as u32).await?;
            } else {
                // MSB=1 => large offset reference , a label for large offset
                let marker = 0x8000_0000 | large.len() as u32;
                self.send_u32(marker).await?;
                large.push(e.offset);
            }
        }
        for v in large {
            self.send_u64(v).await?;
        }
        Ok(())
    }

    /// Write the idx trailer containing the pack hash and idx file hash.
    async fn write_trailer(&mut self) -> Result<(), GitError> {
        // pack hash
        self.send_data_without_update_hash(self.pack_hash.to_data().clone())
            .await?;

        let idx_hash = self.inner_hash.clone().finalize();
        // idx file hash
        self.send_data(idx_hash).await?;
        Ok(())
    }

    /// Write the complete idx file by sending header, fanout, names, CRCs, offsets, and trailer.
    pub async fn write_idx(&mut self, mut entries: Vec<IndexEntry>) -> Result<(), GitError> {
        // check entries length
        if entries.len() != self.object_number {
            return Err(GitError::ConversionError(format!(
                "entries length {} != object_number {}",
                entries.len(),
                self.object_number
            )));
        }

        // write header
        self.write_header().await?;
        self.write_fanout(&mut entries).await?;
        self.write_names(&entries).await?;
        self.write_crc32(&entries).await?;
        self.write_offsets(&entries).await?;
        self.write_trailer().await?;
        self.drop_sender();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use crate::{
        errors::GitError,
        hash::ObjectHash,
        internal::pack::{index_entry::IndexEntry, pack_index::IdxBuilder},
    };

    /// construct fake sha1 hash
    fn fake_sha1(n: u8) -> ObjectHash {
        ObjectHash::Sha1([n; 20])
    }

    /// construct entries (hashes from 1, 2, 3… for fanout testing)
    fn build_entries_sha1(n: usize) -> Vec<IndexEntry> {
        (0..n)
            .map(|i| IndexEntry {
                hash: fake_sha1(i as u8),
                crc32: 0x12345678 + i as u32,
                offset: 0x10 + (i as u64) * 3,
            })
            .collect()
    }

    /// Test basic idx building for SHA1 pack index.
    #[tokio::test]
    async fn test_idx_builder_sha1_basic() -> Result<(), GitError> {
        // mock channel catcher
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(4096);

        let object_number = 3;
        let pack_hash = fake_sha1(0xAA);

        let mut builder = IdxBuilder::new(object_number, tx, pack_hash);

        let entries = build_entries_sha1(object_number);

        // execute idx write
        builder.write_idx(entries).await?;

        // collect all written byte chunks
        let mut out: Vec<u8> = Vec::new();
        while let Some(chunk) = rx.recv().await {
            out.extend_from_slice(&chunk);
        }

        // ------- assert header -------
        // .idx v2 magic: FF 74 4F 63 00000002
        assert_eq!(&out[0..8], &[0xFF, 0x74, 0x4F, 0x63, 0, 0, 0, 2]);

        // ------- fanout -------
        // fanout has 256 * 4 bytes, starting from offset 8
        let fanout_start = 8;
        let fanout_end = fanout_start + 256 * 4;
        let fanout_bytes = &out[fanout_start..fanout_end];

        // Because the first byte of the hash is 0,1,2, fanout[0]=1 fanout[1]=2 fanout[2]=3, the rest=3
        let mut fanout = [0u32; 256];
        fanout[0] = 1;
        fanout[1] = 2;
        fanout[2] = 3;
        for i in 3..256 {
            fanout[i] = 3;
        }

        for i in 0..256 {
            let idx = i * 4;
            let v = u32::from_be_bytes([
                fanout_bytes[idx],
                fanout_bytes[idx + 1],
                fanout_bytes[idx + 2],
                fanout_bytes[idx + 3],
            ]);
            assert_eq!(v, fanout[i], "fanout mismatch at index {i}");
        }

        // ------- names -------
        let names_start = fanout_end;
        let names_end = names_start + object_number * 20; // sha1 = 20 bytes
        let names_bytes = &out[names_start..names_end];

        for i in 0..object_number {
            let name = &names_bytes[i * 20..i * 20 + 20];
            assert!(name.iter().all(|b| *b == i as u8));
        }

        // ------- crc32 -------
        let crc_start = names_end;
        let crc_end = crc_start + object_number * 4;
        let crc_bytes = &out[crc_start..crc_end];

        for i in 0..object_number {
            let expected = 0x12345678 + i as u32;
            let actual = u32::from_be_bytes([
                crc_bytes[4 * i],
                crc_bytes[4 * i + 1],
                crc_bytes[4 * i + 2],
                crc_bytes[4 * i + 3],
            ]);
            assert_eq!(expected, actual);
        }

        // ------- offsets -------
        let offset_start = crc_end;
        let offset_end = offset_start + object_number * 4;
        let offsets_bytes = &out[offset_start..offset_end];

        for i in 0..object_number {
            let expected = 0x10 + (i as u64) * 3;
            let actual = u32::from_be_bytes([
                offsets_bytes[i * 4],
                offsets_bytes[i * 4 + 1],
                offsets_bytes[i * 4 + 2],
                offsets_bytes[i * 4 + 3],
            ]);
            assert_eq!(expected as u32, actual);
        }

        // ------- pack hash -------
        let trailer_pack_hash_start = offset_end;
        let trailer_pack_hash_end = trailer_pack_hash_start + 20;
        let pack_hash_bytes = &out[trailer_pack_hash_start..trailer_pack_hash_end];
        assert!(pack_hash_bytes.iter().all(|b| *b == 0xAA));

        // ------- idx hash (cannot be exactly the same as git, but should have a value) -------
        let idx_hash = &out[trailer_pack_hash_end..trailer_pack_hash_end + 20];
        assert_eq!(idx_hash.len(), 20);

        Ok(())
    }
}
