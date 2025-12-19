//! Patience/Myers-based delta encoder that emits Git-compatible instructions and exposes helpers to
//! estimate similarity rates.

use diffs::Diff;
#[cfg(feature = "diff_mydrs")]
use diffs::myers;

const DATA_INS_LEN: usize = 0x7f;
const VAR_INT_ENCODING_BITS: u8 = 7;

/// Delta operation kind: inline literal data or copy-from-base.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Optype {
    Data,
    Copy,
}

/// A single delta op: either copy `len` bytes from `begin` in base, or insert `len` bytes from `new_data`.
#[derive(Debug, Clone, Copy)]
struct DeltaOp {
    ins: Optype,
    begin: usize,
    len: usize,
}

/// Holds diff result and metadata (similarity stats) between two byte slices.
#[derive(Debug)]
pub struct DeltaDiff<'a> {
    ops: Vec<DeltaOp>,
    old_data: &'a [u8],
    new_data: &'a [u8],
    ssam: usize,
    ssam_r: f64,
}

impl<'a> DeltaDiff<'a> {
    /// Build a delta plan between two byte slices using patience or Myers (feature gated),
    /// collecting copy/insert operations and similarity stats.
    pub fn new(old_data: &'a [u8], new_data: &'a [u8]) -> Self {
        let mut delta_diff = DeltaDiff {
            ops: vec![],
            old_data,
            new_data,
            ssam: 0,
            ssam_r: 0.00,
        };

        #[cfg(feature = "diff_mydrs")]
        myers::diff(
            &mut delta_diff,
            old_data,
            0,
            old_data.len(),
            new_data,
            0,
            new_data.len(),
        )
        .unwrap();

        #[cfg(not(feature = "diff_mydrs"))]
        diffs::patience::diff(
            &mut delta_diff,
            old_data,
            0,
            old_data.len(),
            new_data,
            0,
            new_data.len(),
        )
        .unwrap();

        delta_diff
    }

    /// Encode as Git-style delta stream:
    /// `[varint old_size][varint new_size][ops...]` where each op is either
    /// - data (msb=0, lower 7 bits = literal length, followed by literal bytes)
    /// - copy (msb=1, flag bits describe which offset/size bytes are present).
    pub fn encode(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::with_capacity(self.ops.len() * 30);
        result.append(&mut write_size_encoding(self.old_data.len()));
        result.append(&mut write_size_encoding(self.new_data.len()));

        for op in &self.ops {
            result.append(&mut self.encode_op(op));
        }
        result
    }

    /// Encode a single delta op to raw bytes.
    fn encode_op(&self, op: &DeltaOp) -> Vec<u8> {
        let mut op_data = vec![];

        match op.ins {
            Optype::Data => {
                let instruct = (op.len & 0x7f) as u8;
                op_data.push(instruct);
                op_data.append(&mut self.new_data[op.begin..op.begin + op.len].to_vec());
            }

            Optype::Copy => {
                let mut instruct: u8 = 0x80;
                let mut offset = op.begin;
                let mut size = op.len;
                let mut copy_data = vec![];

                for i in 0..4 {
                    let _bit = (offset & 0xff) as u8;
                    if _bit != 0 {
                        instruct |= (1 << i) as u8;
                        copy_data.push(_bit)
                    }
                    offset >>= 8;
                }

                for i in 4..7 {
                    let _bit = (size & 0xff) as u8;
                    if _bit != 0 {
                        instruct |= (1 << i) as u8;
                        copy_data.push(_bit)
                    }
                    size >>= 8;
                }

                op_data.push(instruct);
                op_data.append(&mut copy_data);
            }
        }

        op_data
    }

    /// Return similarity rate (shared-bytes / new_data len), computed in `finish`.
    pub fn get_ssam_rate(&self) -> f64 {
        self.ssam_r
    }
}

impl Diff for DeltaDiff<'_> {
    type Error = ();

    fn equal(&mut self, _old: usize, _new: usize, _len: usize) -> Result<(), Self::Error> {
        self.ssam += _len;
        if let Some(tail) = self.ops.last_mut() {
            if tail.begin + tail.len == _old && tail.ins == Optype::Copy {
                tail.len += _len;
            } else {
                self.ops.push(DeltaOp {
                    ins: Optype::Copy,
                    begin: _old,
                    len: _len,
                });
            }
        } else {
            self.ops.push(DeltaOp {
                ins: Optype::Copy,
                begin: _old,
                len: _len,
            });
        }

        Ok(())
    }

    fn insert(&mut self, _old: usize, _new: usize, _len: usize) -> Result<(), ()> {
        let mut len = _len;
        let mut new = _new;

        if _len > DATA_INS_LEN {
            while len > DATA_INS_LEN {
                self.ops.push(DeltaOp {
                    ins: Optype::Data,
                    begin: new,
                    len: DATA_INS_LEN,
                });

                len -= DATA_INS_LEN;
                new += DATA_INS_LEN;
            }

            self.ops.push(DeltaOp {
                ins: Optype::Data,
                begin: new,
                len,
            });
        } else if let Some(tail) = self.ops.last_mut() {
            if tail.begin + tail.len == _new
                && tail.ins == Optype::Data
                && tail.len + _len < DATA_INS_LEN
            {
                tail.len += _len;
            } else {
                self.ops.push(DeltaOp {
                    ins: Optype::Data,
                    begin: new,
                    len,
                });
            }
        } else {
            self.ops.push(DeltaOp {
                ins: Optype::Data,
                begin: new,
                len,
            });
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<(), Self::Error> {
        // compute the ssam rate when finish the diff process.
        self.ssam_r = self.ssam as f64 / self.new_data.len() as f64;
        Ok(())
    }
}

/// Encode a usize as Git-style varint (7 bits per byte, msb=1 means continue).
fn write_size_encoding(number: usize) -> Vec<u8> {
    let mut num = vec![];
    let mut number = number;

    loop {
        if number >> VAR_INT_ENCODING_BITS > 0 {
            num.push((number & 0x7f) as u8 | 0x80);
        } else {
            num.push((number & 0x7f) as u8);
            break;
        }

        number >>= VAR_INT_ENCODING_BITS;
    }
    num
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        fs::File,
        io::{self, BufReader, Cursor, Read},
        path::{Path, PathBuf},
    };

    use flate2::bufread::ZlibDecoder;

    use super::DeltaDiff;
    use crate::delta::decode::delta_decode;
    /// Read and inflate a zlib-compressed file into raw bytes.
    fn read_zlib_data(path: &Path) -> Result<Vec<u8>, io::Error> {
        // open the file and create a buffered reader
        let file = File::open(path)?;
        let buf_reader = BufReader::new(file);
        let mut deflate = ZlibDecoder::new(buf_reader);
        let mut result = Vec::new();
        deflate.read_to_end(&mut result)?;
        Ok(result)
    }

    /// End-to-end encode + decode over real zlib-compressed fixtures should reconstruct new data.
    #[test]
    fn test_delta_fn() {
        let mut source = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        source.push("tests/diff/16ecdcc8f663777896bd39ca025a041b7f005e");
        let old_data = read_zlib_data(&source).unwrap();

        let mut source = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        source.push("tests/diff/bee0d45f981adf7c2926a0dc04deb7f006bcc3");
        let new_data = read_zlib_data(&source).unwrap();

        let d = DeltaDiff::new(&old_data, &new_data);
        let delta_result = d.encode();
        println!(
            "delta size: from {} to {}",
            old_data.len(),
            delta_result.len()
        );

        let mut reader = Cursor::new(&delta_result);
        let rebuild_data = delta_decode(&mut reader, &old_data).expect("delta format error");
        assert_eq!(new_data, rebuild_data);
    }
}
