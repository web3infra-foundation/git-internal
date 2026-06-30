use std::{
    collections::HashSet,
    fmt,
    fs::File,
    io::{self, BufRead, BufReader, ErrorKind, Read},
    path::Path,
};

use flate2::bufread::ZlibDecoder;

use crate::{
    errors::GitError,
    hash::{ObjectHash, get_hash_kind},
    internal::pack::{Pack, utils, wrapper::Wrapper},
    utils::{CountingReader, HashAlgorithm},
};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PackStats {
    pub total: usize,
    pub commits: usize,
    pub trees: usize,
    pub blobs: usize,
    pub tags: usize,
    pub deltas: usize,
}

struct PackScan {
    stats: PackStats,
    ref_delta_bases: HashSet<ObjectHash>,
    pack_hash: ObjectHash,
}

struct PackIndexHashes {
    objects: HashSet<ObjectHash>,
    pack_hash: ObjectHash,
}

struct HashingReader<R> {
    inner: R,
    hash: HashAlgorithm,
}

impl<R> HashingReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            hash: HashAlgorithm::new(),
        }
    }

    fn current_hash(&self) -> Result<ObjectHash, GitError> {
        ObjectHash::from_bytes(&self.hash.clone().finalize())
            .map_err(|e| GitError::InvalidPackFile(format!("Read index error: {e}")))
    }
}

impl<R: Read> HashingReader<R> {
    fn read_without_hash(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }

    fn read_exact_without_hash(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.inner.read_exact(buf)
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hash.update(&buf[..n]);
        Ok(n)
    }
}

impl fmt::Display for PackStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PackStats {{ total: {}, commits: {}, trees: {}, blobs: {}, tags: {}, deltas: {} }}",
            self.total, self.commits, self.trees, self.blobs, self.tags, self.deltas
        )
    }
}

impl PackStats {
    pub fn analyze<P: AsRef<Path>>(pack_path: P) -> Result<PackStats, GitError> {
        let pack_path = pack_path.as_ref();
        if !pack_path.exists() {
            return Err(GitError::InvalidPackFile(format!(
                "Pack file not found: {}",
                pack_path.display()
            )));
        }

        let f = File::open(pack_path)
            .map_err(|e| GitError::InvalidPackFile(format!("Failed to open pack file: {e}")))?;
        let scan = Self::scan(BufReader::new(f))?;
        if !scan.ref_delta_bases.is_empty() {
            let index_hashes = Self::read_pack_index_hashes(pack_path)?.ok_or_else(|| {
                GitError::InvalidPackFile(
                    "Pack index is required to verify ref-delta bases".to_string(),
                )
            })?;
            if index_hashes.pack_hash != scan.pack_hash {
                return Err(GitError::InvalidPackFile(format!(
                    "Pack index hash {} does not match pack trailer hash {}",
                    index_hashes.pack_hash, scan.pack_hash
                )));
            }
            if let Some(base_hash) = scan
                .ref_delta_bases
                .iter()
                .find(|base_hash| !index_hashes.objects.contains(*base_hash))
            {
                return Err(GitError::InvalidPackFile(format!(
                    "Ref-delta base {base_hash} is not present in the pack index"
                )));
            }
        }

        Ok(scan.stats)
    }

    pub fn validate_header<P: AsRef<Path>>(pack_path: P) -> Result<u32, GitError> {
        let pack_path = pack_path.as_ref();
        if !pack_path.exists() {
            return Err(GitError::InvalidPackFile(format!(
                "Pack file not found: {}",
                pack_path.display()
            )));
        }

        let f = File::open(pack_path)
            .map_err(|e| GitError::InvalidPackFile(format!("Failed to open pack file: {e}")))?;
        let mut reader = BufReader::new(f);

        let (count, _) = Pack::check_header(&mut reader)?;
        Ok(count)
    }

    fn scan(reader: impl BufRead) -> Result<PackScan, GitError> {
        let mut reader = Wrapper::new(reader);
        let (object_num, header_data) = Pack::check_header(&mut reader)?;
        let mut stats = PackStats {
            total: object_num as usize,
            ..Default::default()
        };
        let first_object_offset = header_data.len();
        let mut offset = first_object_offset;
        let mut object_starts = HashSet::new();
        let mut ref_delta_bases = HashSet::new();

        for _ in 0..object_num {
            let object_start = offset;
            let (type_bits, size) = utils::read_type_and_varint_size(&mut reader, &mut offset)
                .map_err(|e| {
                    GitError::InvalidPackFile(format!("Read error at offset {offset}: {e}"))
                })?;

            stats.count_type_bits(type_bits, offset)?;

            match type_bits {
                1..=4 => drain_zlib(&mut reader, &mut offset, size)?,
                5 | 6 => {
                    let (delta_offset, consumed) = utils::read_offset_encoding(&mut reader)
                        .map_err(|e| {
                            GitError::InvalidPackFile(format!(
                                "Read offset encoding error at offset {offset}: {e}"
                            ))
                        })?;
                    let delta_offset = usize::try_from(delta_offset).map_err(|_| {
                        GitError::InvalidPackFile(format!(
                            "Offset delta at {object_start} exceeds platform limits"
                        ))
                    })?;
                    let base_offset = object_start.checked_sub(delta_offset).ok_or_else(|| {
                        GitError::InvalidPackFile(format!(
                            "Offset delta at {object_start} points before pack data"
                        ))
                    })?;
                    if delta_offset == 0
                        || base_offset < first_object_offset
                        || !object_starts.contains(&base_offset)
                    {
                        return Err(GitError::InvalidPackFile(format!(
                            "Offset delta at {object_start} does not reference an earlier object"
                        )));
                    }
                    add_to_offset(&mut offset, consumed)?;
                    drain_zlib(&mut reader, &mut offset, size)?;
                }
                7 => {
                    let base_hash = ObjectHash::from_stream(&mut reader).map_err(|e| {
                        GitError::InvalidPackFile(format!(
                            "Read hash error at offset {offset}: {e}"
                        ))
                    })?;
                    add_to_offset(&mut offset, base_hash.size())?;
                    ref_delta_bases.insert(base_hash);
                    drain_zlib(&mut reader, &mut offset, size)?;
                }
                _ => unreachable!(),
            }
            object_starts.insert(object_start);
        }

        let computed_hash = reader.final_hash();
        let trailer = ObjectHash::from_stream(&mut reader).map_err(|e| {
            GitError::InvalidPackFile(format!("Failed to read trailer hash: {e:?}"))
        })?;
        if computed_hash != trailer {
            return Err(GitError::InvalidPackFile(format!(
                "Pack trailer mismatch: computed {computed_hash}, stored {trailer}"
            )));
        }
        if !utils::is_eof(&mut reader) {
            return Err(GitError::InvalidPackFile(
                "Pack has trailing data after trailer".to_string(),
            ));
        }

        Ok(PackScan {
            stats,
            ref_delta_bases,
            pack_hash: trailer,
        })
    }

    fn read_pack_index_hashes(pack_path: &Path) -> Result<Option<PackIndexHashes>, GitError> {
        let idx_path = pack_path.with_extension("idx");
        let idx_file = match File::open(&idx_path) {
            Ok(file) => file,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(GitError::InvalidPackFile(format!(
                    "Failed to open pack index {}: {e}",
                    idx_path.display()
                )));
            }
        };
        let mut reader = HashingReader::new(BufReader::new(idx_file));

        let magic = read_be_u32(&mut reader)?;
        let version = read_be_u32(&mut reader)?;
        if magic != 0xff74_4f63 || version != 2 {
            return Err(GitError::InvalidPackFile(
                "Only pack index v2 is supported for ref-delta validation".to_string(),
            ));
        }

        let mut object_num = 0usize;
        for _ in 0..256 {
            object_num = read_be_u32(&mut reader)? as usize;
        }

        let hash_size = get_hash_kind().size();
        let mut objects = HashSet::new();
        let mut hash_buf = vec![0; hash_size];
        for _ in 0..object_num {
            reader
                .read_exact(&mut hash_buf)
                .map_err(|e| GitError::InvalidPackFile(format!("Read index error: {e}")))?;
            let hash = ObjectHash::from_bytes(&hash_buf)
                .map_err(|e| GitError::InvalidPackFile(format!("Read index error: {e}")))?;
            objects.insert(hash);
        }

        let crc_bytes = checked_mul(object_num, 4, "Pack index is too large")?;
        discard_exact(&mut reader, crc_bytes)?;

        let mut large_offset_count = 0usize;
        for _ in 0..object_num {
            let offset = read_be_u32(&mut reader)?;
            if offset & 0x8000_0000 != 0 {
                large_offset_count = large_offset_count.checked_add(1).ok_or_else(|| {
                    GitError::InvalidPackFile("Pack index is too large".to_string())
                })?;
            }
        }
        let large_offset_bytes = checked_mul(large_offset_count, 8, "Pack index is too large")?;
        discard_exact(&mut reader, large_offset_bytes)?;

        let pack_hash = ObjectHash::from_stream(&mut reader)
            .map_err(|e| GitError::InvalidPackFile(format!("Read index error: {e}")))?;
        let expected_idx_hash = reader.current_hash()?;
        let mut idx_hash_buf = vec![0; hash_size];
        reader
            .read_exact_without_hash(&mut idx_hash_buf)
            .map_err(|e| GitError::InvalidPackFile(format!("Read index error: {e}")))?;
        let idx_hash = ObjectHash::from_bytes(&idx_hash_buf)
            .map_err(|e| GitError::InvalidPackFile(format!("Read index error: {e}")))?;
        if idx_hash != expected_idx_hash {
            return Err(GitError::InvalidPackFile(format!(
                "Pack index checksum {idx_hash} does not match calculated checksum {expected_idx_hash}"
            )));
        }
        let mut trailing = [0; 1];
        if reader
            .read_without_hash(&mut trailing)
            .map_err(|e| GitError::InvalidPackFile(format!("Read index error: {e}")))?
            != 0
        {
            return Err(GitError::InvalidPackFile(
                "Pack index has trailing data after checksum".to_string(),
            ));
        }

        Ok(Some(PackIndexHashes { objects, pack_hash }))
    }

    fn count_type_bits(&mut self, type_bits: u8, offset: usize) -> Result<(), GitError> {
        match type_bits {
            1 => {
                self.commits += 1;
            }
            2 => {
                self.trees += 1;
            }
            3 => {
                self.blobs += 1;
            }
            4 => {
                self.tags += 1;
            }
            5..=7 => {
                self.deltas += 1;
            }
            _ => {
                return Err(GitError::InvalidObjectType(format!(
                    "Unknown pack type bits: {type_bits} at offset {offset}"
                )));
            }
        }
        Ok(())
    }
}

fn drain_zlib(
    reader: &mut impl BufRead,
    offset: &mut usize,
    expected_size: usize,
) -> Result<(), GitError> {
    let mut counting_reader = CountingReader::new(reader);
    let mut deflate = ZlibDecoder::new(&mut counting_reader);
    let mut remaining = expected_size;
    let mut scratch = [0; 8192];

    while remaining > 0 {
        let chunk_len = remaining.min(scratch.len());
        let bytes = deflate
            .read(&mut scratch[..chunk_len])
            .map_err(|e| GitError::InvalidPackFile(format!("Decompression error: {e}")))?;
        if bytes == 0 {
            return Err(GitError::InvalidPackFile(format!(
                "The object size is smaller than the expected size {expected_size}"
            )));
        }
        remaining -= bytes;
    }

    let mut extra = [0; 1];
    let extra_bytes = deflate
        .read(&mut extra)
        .map_err(|e| GitError::InvalidPackFile(format!("Decompression error: {e}")))?;
    if extra_bytes != 0 {
        return Err(GitError::InvalidPackFile(format!(
            "The object size exceeds the expected size {expected_size}"
        )));
    }

    let consumed = usize::try_from(counting_reader.bytes_read).map_err(|_| {
        GitError::InvalidPackFile("Compressed object size exceeds platform limits".to_string())
    })?;
    add_to_offset(offset, consumed)
}

fn add_to_offset(offset: &mut usize, consumed: usize) -> Result<(), GitError> {
    *offset = offset
        .checked_add(consumed)
        .ok_or_else(|| GitError::InvalidPackFile("Pack offset overflow".to_string()))?;
    Ok(())
}

fn read_be_u32(reader: &mut impl Read) -> Result<u32, GitError> {
    let mut buf = [0; 4];
    reader
        .read_exact(&mut buf)
        .map_err(|e| GitError::InvalidPackFile(format!("Read index error: {e}")))?;
    Ok(u32::from_be_bytes(buf))
}

fn discard_exact(reader: &mut impl Read, mut len: usize) -> Result<(), GitError> {
    let mut scratch = [0; 8192];
    while len != 0 {
        let chunk_len = len.min(scratch.len());
        reader
            .read_exact(&mut scratch[..chunk_len])
            .map_err(|e| GitError::InvalidPackFile(format!("Read index error: {e}")))?;
        len -= chunk_len;
    }
    Ok(())
}

fn checked_mul(lhs: usize, rhs: usize, message: &str) -> Result<usize, GitError> {
    lhs.checked_mul(rhs)
        .ok_or_else(|| GitError::InvalidPackFile(message.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        hash::{HashKind, set_hash_kind_for_test},
        internal::pack::test_pack_download::download_pack_file,
    };

    #[test]
    fn test_analyze_small_pack_sha1() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let (pack_path, _dl_guard) = download_pack_file("small-sha1.pack");
        let stats = PackStats::analyze(pack_path).expect("Failed to analyze");

        assert!(stats.total > 0);
        assert_eq!(
            stats.total,
            stats.commits + stats.trees + stats.blobs + stats.tags + stats.deltas
        );
    }

    #[test]
    fn test_analyze_small_pack_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let (pack_path, _dl_guard) = download_pack_file("small-sha256.pack");
        let stats = PackStats::analyze(pack_path).expect("Failed to analyze");

        assert!(stats.total > 0);
        assert_eq!(
            stats.total,
            stats.commits + stats.trees + stats.blobs + stats.tags + stats.deltas
        );
    }

    #[test]
    fn test_analyze_delta_pack_sha1() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let (pack_path, _dl_guard) = download_pack_file("ref-delta-sha1.pack");
        let stats = PackStats::analyze(pack_path).expect("Failed to analyze");

        assert!(stats.total > 0);

        assert_eq!(
            stats.total,
            stats.commits + stats.trees + stats.blobs + stats.tags + stats.deltas
        );
    }

    #[test]
    fn test_analyze_delta_pack_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        let (pack_path, _dl_guard) = download_pack_file("ref-delta-sha256.pack");
        let stats = PackStats::analyze(pack_path).expect("Failed to analyze");

        assert!(stats.total > 0);
        assert_eq!(
            stats.total,
            stats.commits + stats.trees + stats.blobs + stats.tags + stats.deltas
        );
    }

    #[test]
    fn test_nonexistent_file() {
        let result = PackStats::analyze("tests/data/packs/nonexistent.pack");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_pack_file() {
        use std::io::Write;

        use tempfile::NamedTempFile;

        let mut temp = NamedTempFile::new().expect("create temp file");
        temp.write_all(b"XXXX").expect("write temp file");
        temp.flush().expect("flush temp file");

        let result = PackStats::analyze(temp.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_analyze_rejects_trailing_data() {
        use std::{fs, io::Write};

        use tempfile::NamedTempFile;

        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let (pack_path, _dl_guard) = download_pack_file("small-sha1.pack");
        let mut bytes = fs::read(pack_path).expect("read pack fixture");
        bytes.push(0);

        let mut temp = NamedTempFile::new().expect("create temp file");
        temp.write_all(&bytes).expect("write temp file");
        temp.flush().expect("flush temp file");

        let result = PackStats::analyze(temp.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_analyze_huge_object_count_does_not_preallocate() {
        use std::io::Write;

        use tempfile::NamedTempFile;

        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"PACK");
        bytes.extend_from_slice(&2u32.to_be_bytes());
        bytes.extend_from_slice(&u32::MAX.to_be_bytes());

        let mut temp = NamedTempFile::new().expect("create temp file");
        temp.write_all(&bytes).expect("write temp file");
        temp.flush().expect("flush temp file");

        let result = PackStats::analyze(temp.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_analyze_ref_delta_requires_index() {
        use std::io::Write;

        use flate2::{Compression, write::ZlibEncoder};
        use tempfile::NamedTempFile;

        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let mut deflate = ZlibEncoder::new(Vec::new(), Compression::default());
        deflate.write_all(&[]).expect("write zlib payload");
        let compressed_delta = deflate.finish().expect("finish zlib payload");

        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"PACK");
        bytes.extend_from_slice(&2u32.to_be_bytes());
        bytes.extend_from_slice(&1u32.to_be_bytes());
        write_pack_object_header(&mut bytes, 7, 0);
        bytes.extend(std::iter::repeat_n(0, get_hash_kind().size()));
        bytes.extend_from_slice(&compressed_delta);
        append_pack_trailer(&mut bytes);

        let mut temp = NamedTempFile::new().expect("create temp file");
        temp.write_all(&bytes).expect("write temp file");
        temp.flush().expect("flush temp file");

        let result = PackStats::analyze(temp.path());
        assert!(format!("{result:?}").contains("Pack index is required"));
    }

    #[test]
    fn test_validate_header() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let (pack_path, _dl_guard) = download_pack_file("small-sha1.pack");
        let result = PackStats::validate_header(pack_path);
        assert!(result.is_ok());
        assert!(result.unwrap() > 0);
    }

    #[test]
    fn test_validate_header_nonexistent() {
        let result = PackStats::validate_header("tests/data/packs/nonexistent.pack");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_header_invalid_file() {
        use std::io::Write;

        use tempfile::NamedTempFile;

        let mut temp = NamedTempFile::new().expect("create temp file");
        temp.write_all(b"XX").expect("write temp file");
        temp.flush().expect("flush temp file");

        let result = PackStats::validate_header(temp.path());
        assert!(result.is_err());
    }

    fn write_pack_object_header(out: &mut Vec<u8>, type_bits: u8, mut size: usize) {
        let mut byte = ((type_bits & 0x07) << 4) | (size as u8 & 0x0f);
        size >>= 4;
        if size != 0 {
            byte |= 0x80;
        }
        out.push(byte);

        while size != 0 {
            let mut next = (size as u8) & 0x7f;
            size >>= 7;
            if size != 0 {
                next |= 0x80;
            }
            out.push(next);
        }
    }

    fn append_pack_trailer(bytes: &mut Vec<u8>) {
        let mut hash = HashAlgorithm::new();
        hash.update(bytes);
        let trailer = ObjectHash::from_bytes(&hash.finalize()).expect("pack hash");
        bytes.extend_from_slice(trailer.as_ref());
    }
}
