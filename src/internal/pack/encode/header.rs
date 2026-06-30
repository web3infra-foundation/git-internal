//! Git pack wire-format encoding helpers.
//!
//! Low-level functions that produce the binary pack header, OFS_DELTA offset encoding, and the
//! variable-length object entry header followed by zlib-compressed payload.

use std::io::Write;

use flate2::write::ZlibEncoder;

use crate::{
    errors::GitError,
    internal::{object::types::ObjectType, pack::entry::Entry},
};

/// Build the fixed 12-byte pack header.
///
/// Layout: `b"PACK"` + version `2` as big-endian `u32` + object count as big-endian `u32`.
pub(crate) fn encode_header(object_number: usize) -> Vec<u8> {
    let mut result: Vec<u8> = vec![
        b'P', b'A', b'C', b'K', // Pack signature.
        0, 0, 0, 2, // This encoder emits pack version 2.
    ];
    assert_ne!(object_number, 0);
    assert!(object_number <= u32::MAX as usize);
    // TODO: Return GitError instead of asserting when the object count is out of range.
    result.append((object_number as u32).to_be_bytes().to_vec().as_mut());
    result
}

/// Encode the backwards distance from an OFS_DELTA entry to its base.
///
/// Git uses a most-significant-group-first, seven-bit encoding with a bias: after the first group,
/// one is subtracted before each additional group is emitted. This is not ordinary LEB128.
pub(crate) fn encode_offset(mut value: usize) -> Vec<u8> {
    assert_ne!(value, 0, "offset can't be zero");
    let mut bytes = Vec::new();

    bytes.push((value & 0x7F) as u8);
    value >>= 7;
    while value != 0 {
        value -= 1;
        let byte = (value & 0x7F) as u8 | 0x80;
        value >>= 7;
        bytes.push(byte);
    }
    bytes.reverse();
    bytes
}

/// Encode one complete pack entry.
///
/// The result contains the variable-length object header, an optional OFS_DELTA base distance, and
/// the zlib-compressed payload. For a base object, `entry.data` is the object's raw content. For a
/// delta object, it is already a Git delta instruction stream produced by the selected delta
/// engine.
///
/// `offset` is required for offset-delta types and must be `None` for base objects.
pub(crate) fn encode_one_object(entry: &Entry, offset: Option<usize>) -> Result<Vec<u8>, GitError> {
    let obj_data = &entry.data;
    let obj_data_len = obj_data.len();
    let obj_type_number = entry.obj_type.to_pack_type_u8()?;

    let mut encoded_data = Vec::new();

    // The first byte stores 4 size bits, 3 type bits, and a continuation bit. Remaining size bits
    // are emitted in seven-bit groups, least-significant group first.
    let mut header_data = vec![(0x80 | (obj_type_number << 4)) + (obj_data_len & 0x0f) as u8];
    let mut size = obj_data_len >> 4;
    if size > 0 {
        while size > 0 {
            if size >> 7 > 0 {
                header_data.push((0x80 | size) as u8);
                size >>= 7;
            } else {
                header_data.push(size as u8);
                break;
            }
        }
    } else {
        header_data.push(0);
    }
    encoded_data.extend(header_data);

    // Offset deltas identify their base by backwards distance from this entry's pack position.
    if entry.obj_type == ObjectType::OffsetDelta || entry.obj_type == ObjectType::OffsetZstdelta {
        let offset_data = encode_offset(offset.unwrap());
        encoded_data.extend(offset_data);
    } else if entry.obj_type == ObjectType::HashDelta {
        unreachable!("unsupported type")
    }

    // Git zlib-compresses both raw object payloads and delta instruction streams.
    let mut inflate = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    inflate
        .write_all(obj_data)
        .expect("zlib compress should never failed");
    inflate.flush().expect("zlib flush should never failed");
    let compressed_data = inflate.finish().expect("zlib compress should never failed");
    encoded_data.extend(compressed_data);
    Ok(encoded_data)
}
