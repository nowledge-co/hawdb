//! Field-tagged varint wire primitives for binary WAL record payloads.
//!
//! Record-level payloads follow the protobuf wire discipline required by
//! `COLUMNAR_CANONICAL_AND_PROJECTION_SPEC.md` §3.5.1: each field is a
//! `(field_id << 3) | wire_type` varint tag followed by a varint,
//! fixed-width, or length-delimited body. Readers skip unknown field ids by
//! wire type. The codec is hand-rolled; no code generation enters the
//! supply chain.

use crate::error::{Result, SkeinError};

/// Varint-encoded unsigned body.
pub(crate) const WIRE_TYPE_VARINT: u8 = 0;
/// Eight-byte little-endian body.
pub(crate) const WIRE_TYPE_FIXED64: u8 = 1;
/// Varint length followed by that many bytes.
pub(crate) const WIRE_TYPE_LEN: u8 = 2;

const MAX_VARINT_BYTES: usize = 10;

pub(crate) fn encode_varint_u64(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

pub(crate) fn decode_varint_u64(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    for _ in 0..MAX_VARINT_BYTES {
        let byte = *bytes
            .get(*pos)
            .ok_or_else(|| SkeinError::Storage("wire varint is truncated".to_string()))?;
        *pos += 1;
        let low = u64::from(byte & 0x7f);
        if shift == 63 && low > 1 {
            return Err(SkeinError::Storage("wire varint overflows u64".to_string()));
        }
        value |= low << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
    Err(SkeinError::Storage(
        "wire varint exceeds ten bytes".to_string(),
    ))
}

pub(crate) const fn zigzag_encode_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

pub(crate) const fn zigzag_decode_i64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

pub(crate) fn encode_tag(field_id: u32, wire_type: u8, out: &mut Vec<u8>) {
    debug_assert!(wire_type <= WIRE_TYPE_LEN);
    encode_varint_u64((u64::from(field_id) << 3) | u64::from(wire_type), out);
}

pub(crate) fn decode_tag(bytes: &[u8], pos: &mut usize) -> Result<(u32, u8)> {
    let tag = decode_varint_u64(bytes, pos)?;
    let wire_type = (tag & 0x7) as u8;
    let field_id = tag >> 3;
    let field_id = u32::try_from(field_id)
        .map_err(|_| SkeinError::Storage("wire field id overflows u32".to_string()))?;
    Ok((field_id, wire_type))
}

pub(crate) fn encode_varint_field(field_id: u32, value: u64, out: &mut Vec<u8>) {
    encode_tag(field_id, WIRE_TYPE_VARINT, out);
    encode_varint_u64(value, out);
}

pub(crate) fn encode_fixed64_field(field_id: u32, value: u64, out: &mut Vec<u8>) {
    encode_tag(field_id, WIRE_TYPE_FIXED64, out);
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn encode_len_field(field_id: u32, body: &[u8], out: &mut Vec<u8>) {
    encode_tag(field_id, WIRE_TYPE_LEN, out);
    encode_varint_u64(body.len() as u64, out);
    out.extend_from_slice(body);
}

pub(crate) fn encode_string_field(field_id: u32, value: &str, out: &mut Vec<u8>) {
    encode_len_field(field_id, value.as_bytes(), out);
}

pub(crate) fn decode_fixed64(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    let end = pos
        .checked_add(8)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| SkeinError::Storage("wire fixed64 body is truncated".to_string()))?;
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&bytes[*pos..end]);
    *pos = end;
    Ok(u64::from_le_bytes(raw))
}

pub(crate) fn decode_len_body<'a>(bytes: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
    let len = decode_varint_u64(bytes, pos)?;
    let len = usize::try_from(len).map_err(|_| {
        SkeinError::Storage("wire length-delimited body overflows usize".to_string())
    })?;
    let end = pos
        .checked_add(len)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| {
            SkeinError::Storage("wire length-delimited body is truncated".to_string())
        })?;
    let body = &bytes[*pos..end];
    *pos = end;
    Ok(body)
}

pub(crate) fn decode_string_body(bytes: &[u8], pos: &mut usize) -> Result<String> {
    let body = decode_len_body(bytes, pos)?;
    String::from_utf8(body.to_vec())
        .map_err(|error| SkeinError::Storage(format!("wire string is not valid UTF-8: {error}")))
}

/// Skips one field body of the given wire type, enabling forward-compatible
/// readers that ignore unknown field ids.
pub(crate) fn skip_field(bytes: &[u8], pos: &mut usize, wire_type: u8) -> Result<()> {
    match wire_type {
        WIRE_TYPE_VARINT => {
            decode_varint_u64(bytes, pos)?;
            Ok(())
        }
        WIRE_TYPE_FIXED64 => {
            decode_fixed64(bytes, pos)?;
            Ok(())
        }
        WIRE_TYPE_LEN => {
            decode_len_body(bytes, pos)?;
            Ok(())
        }
        _ => Err(SkeinError::Storage(format!(
            "wire type {wire_type} is not skippable"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_round_trips_boundary_values() {
        for value in [
            0u64,
            1,
            127,
            128,
            16_383,
            16_384,
            u64::from(u32::MAX),
            u64::MAX - 1,
            u64::MAX,
        ] {
            let mut out = Vec::new();
            encode_varint_u64(value, &mut out);
            let mut pos = 0;
            assert_eq!(decode_varint_u64(&out, &mut pos).unwrap(), value);
            assert_eq!(pos, out.len());
        }
    }

    #[test]
    fn varint_rejects_truncation_and_overflow() {
        let mut out = Vec::new();
        encode_varint_u64(u64::MAX, &mut out);
        let mut pos = 0;
        assert!(decode_varint_u64(&out[..out.len() - 1], &mut pos).is_err());
        // Eleven continuation bytes never terminate.
        let overlong = vec![0x80u8; 11];
        let mut pos = 0;
        assert!(decode_varint_u64(&overlong, &mut pos).is_err());
        // A tenth byte carrying more than one bit overflows u64.
        let mut overflow = vec![0xffu8; 9];
        overflow.push(0x02);
        let mut pos = 0;
        assert!(decode_varint_u64(&overflow, &mut pos).is_err());
    }

    #[test]
    fn zigzag_round_trips_signed_extremes() {
        for value in [0i64, -1, 1, -2, 2, i64::MIN, i64::MAX] {
            assert_eq!(zigzag_decode_i64(zigzag_encode_i64(value)), value);
        }
        assert_eq!(zigzag_encode_i64(0), 0);
        assert_eq!(zigzag_encode_i64(-1), 1);
        assert_eq!(zigzag_encode_i64(1), 2);
    }

    #[test]
    fn tags_round_trip_field_ids_and_wire_types() {
        for (field_id, wire_type) in [
            (1u32, WIRE_TYPE_VARINT),
            (2, WIRE_TYPE_FIXED64),
            (3, WIRE_TYPE_LEN),
            (536_870_911, WIRE_TYPE_LEN),
        ] {
            let mut out = Vec::new();
            encode_tag(field_id, wire_type, &mut out);
            let mut pos = 0;
            assert_eq!(decode_tag(&out, &mut pos).unwrap(), (field_id, wire_type));
            assert_eq!(pos, out.len());
        }
    }

    #[test]
    fn unknown_fields_are_skipped_by_wire_type() {
        let mut out = Vec::new();
        encode_varint_field(90, 300, &mut out);
        encode_fixed64_field(91, u64::MAX, &mut out);
        encode_len_field(92, b"opaque-future-field", &mut out);
        encode_varint_field(1, 7, &mut out);
        let mut pos = 0;
        let mut known = None;
        while pos < out.len() {
            let (field_id, wire_type) = decode_tag(&out, &mut pos).unwrap();
            if field_id == 1 {
                known = Some(decode_varint_u64(&out, &mut pos).unwrap());
            } else {
                skip_field(&out, &mut pos, wire_type).unwrap();
            }
        }
        assert_eq!(known, Some(7));
        assert_eq!(pos, out.len());
    }

    #[test]
    fn skip_rejects_reserved_wire_types() {
        let bytes = [0u8; 4];
        let mut pos = 0;
        assert!(skip_field(&bytes, &mut pos, 3).is_err());
        assert!(skip_field(&bytes, &mut pos, 7).is_err());
    }

    #[test]
    fn length_delimited_bodies_reject_truncation() {
        let mut out = Vec::new();
        encode_len_field(1, b"abcdef", &mut out);
        let mut pos = 0;
        let (field_id, wire_type) = decode_tag(&out, &mut pos).unwrap();
        assert_eq!((field_id, wire_type), (1, WIRE_TYPE_LEN));
        let mut short_pos = pos;
        assert!(decode_len_body(&out[..out.len() - 1], &mut short_pos).is_err());
        assert_eq!(decode_len_body(&out, &mut pos).unwrap(), b"abcdef");
    }
}
