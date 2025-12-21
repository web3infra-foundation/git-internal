//! Helper functions shared by the Git smart protocol handlers, including pkt-line parsing, pkt-line
//! encoding, subsequence scans, and response builders that honor HTTP/SSH quirks.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::types::{PKT_LINE_END_MARKER, TransportProtocol};

/// Read a packet line from the given bytes buffer
///
/// Returns a tuple of (bytes_consumed, packet_data)
///
/// This is the original simple implementation from ceres
pub fn read_pkt_line(bytes: &mut Bytes) -> (usize, Bytes) {
    if bytes.is_empty() {
        return (0, Bytes::new());
    }

    // Ensure we have at least 4 bytes for the length prefix
    if bytes.len() < 4 {
        return (0, Bytes::new());
    }

    let pkt_length = bytes.copy_to_bytes(4);
    let pkt_length_str = match core::str::from_utf8(&pkt_length) {
        Ok(s) => s,
        Err(_) => {
            tracing::warn!("Invalid UTF-8 in packet length: {:?}", pkt_length);
            return (0, Bytes::new());
        }
    };

    let pkt_length = match usize::from_str_radix(pkt_length_str, 16) {
        Ok(len) => len,
        Err(_) => {
            tracing::warn!("Invalid hex packet length: {:?}", pkt_length_str);
            return (0, Bytes::new());
        }
    };

    if pkt_length == 0 {
        return (4, Bytes::new()); // Consumed 4 bytes for the "0000" marker
    }

    if pkt_length < 4 {
        tracing::warn!("Invalid packet length: {} (must be >= 4)", pkt_length);
        return (0, Bytes::new());
    }

    let data_length = pkt_length - 4;
    if bytes.len() < data_length {
        tracing::warn!(
            "Insufficient data: need {} bytes, have {}",
            data_length,
            bytes.len()
        );
        return (0, Bytes::new());
    }

    // this operation will change the original bytes
    let pkt_line = bytes.copy_to_bytes(data_length);
    tracing::debug!("pkt line: {:?}", pkt_line);

    (pkt_length, pkt_line)
}

/// Add a packet line string to the buffer with proper length prefix
///
/// This is the original simple implementation from ceres
pub fn add_pkt_line_string(pkt_line_stream: &mut BytesMut, buf_str: String) {
    let buf_str_length = buf_str.len() + 4;
    pkt_line_stream.put(Bytes::from(format!("{buf_str_length:04x}")));
    pkt_line_stream.put(buf_str.as_bytes());
}

/// Read until whitespace and return the extracted string
///
/// This is the original implementation from ceres
pub fn read_until_white_space(bytes: &mut Bytes) -> String {
    let mut buf = Vec::new();
    while bytes.has_remaining() {
        let c = bytes.get_u8();
        if c.is_ascii_whitespace() || c == 0 {
            break;
        }
        buf.push(c);
    }
    match String::from_utf8(buf) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("Invalid UTF-8 in protocol data: {}", e);
            String::new() // Return empty string on invalid UTF-8
        }
    }
}

/// Build a smart reply packet line stream
/// 
/// This is the original simple implementation from ceres
pub fn build_smart_reply(
    transport_protocol: TransportProtocol,
    ref_list: &[String],
    service: String,
) -> BytesMut {
    let mut pkt_line_stream = BytesMut::new();
    if transport_protocol == TransportProtocol::Http {
        add_pkt_line_string(&mut pkt_line_stream, format!("# service={service}\n"));
        pkt_line_stream.put(&PKT_LINE_END_MARKER[..]);
    }

    for ref_line in ref_list {
        add_pkt_line_string(&mut pkt_line_stream, ref_line.to_string());
    }
    pkt_line_stream.put(&PKT_LINE_END_MARKER[..]);
    pkt_line_stream
}

/// Search for a subsequence in a byte slice
pub fn search_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
