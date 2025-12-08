use std::io::Write;
use tokio::sync::mpsc;
use crate::errors::GitError;
use crate::hash::ObjectHash;
use crate::internal::metadata::{EntryMeta, MetaAttached};
use crate::internal::pack::encode::PackEncoder;
use crate::internal::pack::entry::Entry;
use crate::internal::pack::index_entry::IndexEntry;
use crate::protocol::pack::PackGenerator;
use crate::utils::HashAlgorithm;

pub struct IdxBuilder {
    sender:Option<mpsc::Sender<Vec<u8>>>,
    inner_hash: HashAlgorithm, // 用于 idx trailer
    object_number: usize,
    pack_hash: ObjectHash,
}





impl IdxBuilder{

    pub fn new(
        object_number: usize,
        sender: mpsc::Sender<Vec<u8>>,
        pack_hash: ObjectHash,
    ) -> Self {
        Self {
            sender: Some(sender),
            inner_hash: HashAlgorithm::new(),
            object_number,
            pack_hash,
        }
    }

    async fn send_data(&mut self, data: Vec<u8>) -> Result<(), GitError> {
        if let Some(sender) = &self.sender {
            self.inner_hash.update(&data);
            sender.send(data).await.map_err(|e| {
                GitError::IOError(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    format!("Failed to send idx data: {}", e),
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
            HashAlgorithm::Sha1(sha1)=>{
                //magic: FF 74 4F 63  version=2
                let header: [u8; 8] = [0xFF, 0x74, 0x4F, 0x63, 0, 0, 0, 2];
                self.send_data(header.to_vec()).await
            }
            HashAlgorithm::Sha256(sha2)=>{
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
        entries.sort_by( |a, b| a.hash.cmp(&b.hash));
        let mut fanout = [0u32; 256];
        for entry in entries.iter() {
            fanout[entry.hash.to_data()[0] as usize] += 1;
        }
        for i in 1..256 {
            fanout[i] += fanout[i - 1];
            self.send_u32(fanout[i]).await?;
        }
        
        Ok(())
    }
    async fn write_names(
        &mut self,
        entries: &Vec<IndexEntry>,
    ) -> Result<(), GitError> {

        for e in entries {
            self.send_data(e.hash.to_data().clone()).await?;
        }

        Ok(())
    }

    async fn write_crc32(
        &mut self,
        entries: &Vec<IndexEntry>,
    ) -> Result<(), GitError> {

        for e in entries {
            self.send_u32(e.crc32).await?;
        }

        Ok(())
    }

    async fn write_offsets(
        &mut self,
        entries: &Vec<IndexEntry>,
    ) -> Result<(), GitError> {
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
        self.send_data(self.pack_hash.to_data().clone()).await?;

       let idx_hash = self.inner_hash.clone().finalize();

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
        self.write_trailer().await

    }
    
}