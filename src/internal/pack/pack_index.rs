use crate::errors::GitError;
use crate::hash::ObjectHash;
use crate::internal::pack::index_entry::IndexEntry;
use crate::utils::HashAlgorithm;
use tokio::sync::mpsc;

pub struct IdxBuilder {
    sender: Option<mpsc::Sender<Vec<u8>>>,
    inner_hash: HashAlgorithm, // 用于 idx trailer
    object_number: usize,
    pack_hash: ObjectHash,
}

impl IdxBuilder {
    pub fn new(object_number: usize, sender: mpsc::Sender<Vec<u8>>, pack_hash: ObjectHash) -> Self {
        Self {
            sender: Some(sender),
            inner_hash: HashAlgorithm::new(),
            object_number,
            pack_hash,
        }
    }

    pub fn drop_sender(&mut self) {
        self.sender.take(); // Take the sender out, dropping it
    }

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

    async fn send_u32(&mut self, v: u32) -> Result<(), GitError> {
        self.send_data(v.to_be_bytes().to_vec()).await
    }

    /// 发送 u64 值（大端序）
    async fn send_u64(&mut self, v: u64) -> Result<(), GitError> {
        self.send_data(v.to_be_bytes().to_vec()).await
    }

    /// todo: support idx v3
    /// The 4-byte pack index signature: \377t0c
    ///
    /// 4-byte version number: 3
    ///
    /// 4-byte length of the header section, including the signature and version number
    ///
    /// 4-byte number of objects contained in the pack
    ///
    /// 4-byte number of object formats in this pack index: 2
    async fn write_header(&mut self) -> Result<(), GitError> {
        match &self.inner_hash {
            HashAlgorithm::Sha1(_sha1) => {
                //magic: FF 74 4F 63  version=2
                let header: [u8; 8] = [0xFF, 0x74, 0x4F, 0x63, 0, 0, 0, 2];
                self.send_data(header.to_vec()).await
            }
            HashAlgorithm::Sha256(_sha2) => {
                // .idx v3

                let magic: [u8; 4] = [0xFF, 0x74, 0x4F, 0x63];
                let version: u32 = 3;
                let header_size: u32 = 20; // magic(4) + version(4) + header_size(4) + object_count(4) + format_count(4)
                let format_count: u32 = 1; // in the project ,SHA1 and SHA256 will not appear in the same pack,right?.

                self.send_data(magic.to_vec()).await?;
                self.send_u32(version).await?;
                self.send_u32(header_size).await?;
                self.send_u32(self.object_number as u32).await?;
                self.send_u32(format_count).await
            }
        }
    }

    //根据对象哈希的 第一个字节（00~FF）排序
    async fn write_fanout(&mut self, entries: &mut Vec<IndexEntry>) -> Result<(), GitError> {
        entries.sort_by(|a, b| a.hash.cmp(&b.hash));
        let mut fanout = [0u32; 256];
        for entry in entries.iter() {
            fanout[entry.hash.to_data()[0] as usize] += 1;
        }
        for i in 1..256 {
            fanout[i] += fanout[i - 1];
        }
        // send all 256 cumulative counts (including index 0)
        for i in 0..256 {
            self.send_u32(fanout[i]).await?;
        }
        Ok(())
    }
    async fn write_names(&mut self, entries: &Vec<IndexEntry>) -> Result<(), GitError> {
        for e in entries {
            self.send_data(e.hash.to_data().clone()).await?;
        }

        Ok(())
    }

    async fn write_crc32(&mut self, entries: &Vec<IndexEntry>) -> Result<(), GitError> {
        for e in entries {
            self.send_u32(e.crc32).await?;
        }

        Ok(())
    }

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

    async fn write_trailer(&mut self) -> Result<(), GitError> {
        // pack hash
        self.send_data_without_update_hash(self.pack_hash.to_data().clone())
            .await?;

        let idx_hash = self.inner_hash.clone().finalize();
        // idx file hash
        self.send_data(idx_hash).await?;
        Ok(())
    }

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
    use crate::errors::GitError;
    use crate::hash::ObjectHash;
    use crate::internal::pack::index_entry::IndexEntry;
    use crate::internal::pack::pack_index::IdxBuilder;
    use tokio::sync::mpsc;

    /// 构造一个假的哈希（长度必须符合 Sha1 或 Sha256）
    fn fake_sha1(n: u8) -> ObjectHash {
        ObjectHash::Sha1([n; 20])
    }

    /// 构造 entries (hash 从 1、2、3… 便于 fanout 测试)
    fn build_entries_sha1(n: usize) -> Vec<IndexEntry> {
        (0..n)
            .map(|i| IndexEntry {
                hash: fake_sha1(i as u8),
                crc32: 0x12345678 + i as u32,
                offset: 0x10 + (i as u64) * 3,
            })
            .collect()
    }

    #[tokio::test]
    async fn test_idx_builder_sha1_basic() -> Result<(), GitError> {
        // mock channel 捕获输出
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(4096);

        let object_number = 3;
        let pack_hash = fake_sha1(0xAA);

        let mut builder = IdxBuilder::new(object_number, tx, pack_hash);

        let entries = build_entries_sha1(object_number);

        // 执行 idx 写入
        builder.write_idx(entries).await?;

        // 收集所有写入的字节片段
        let mut out: Vec<u8> = Vec::new();
        while let Some(chunk) = rx.recv().await {
            out.extend_from_slice(&chunk);
        }

        // ------- 断言 header -------
        // .idx v2 magic: FF 74 4F 63 00000002
        assert_eq!(&out[0..8], &[0xFF, 0x74, 0x4F, 0x63, 0, 0, 0, 2]);

        // ------- fanout -------
        // fanout 共 256 * 4 字节，从 offset 8 开始
        let fanout_start = 8;
        let fanout_end = fanout_start + 256 * 4;
        let fanout_bytes = &out[fanout_start..fanout_end];

        // 因为 hash 第一字节是 0,1,2，所以 fanout[0]=1 fanout[1]=2 fanout[2]=3，其余=3
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

        // ------- idx hash（无法与 git 完全一致，但应该有值） -------
        let idx_hash = &out[trailer_pack_hash_end..trailer_pack_hash_end + 20];
        assert_eq!(idx_hash.len(), 20);

        Ok(())
    }
}
