//! Existing v1 compressed text envelope shared by checkpoint and sidecar storage.
//!
//! The facade owns file selection and admission policy. This owner preserves the
//! envelope bytes, validation order, and caller-supplied decoded byte limit.

use super::parse_u64;
use crate::DurableCompression;
use skein_core::{Result, SkeinError};
use skein_integrity::checksum_u64 as checksum_bytes;
use std::collections::BTreeSet;
use std::io::{Cursor, Read};

pub const DURABLE_COMPRESSION_HEADER: &str = "SKEIN_COMPRESSED_V1";
const DEFAULT_COMPRESSION_LEVEL: i32 = 3;

pub fn encode_durable_text(text: &str, compression: DurableCompression) -> Result<Vec<u8>> {
    match compression {
        DurableCompression::Zstd => encode_zstd_durable_text(text),
    }
}

fn encode_zstd_durable_text(text: &str) -> Result<Vec<u8>> {
    let compressed = zstd::stream::encode_all(text.as_bytes(), DEFAULT_COMPRESSION_LEVEL)
        .map_err(|error| SkeinError::Storage(format!("zstd compression failed: {error}")))?;
    let compressed_checksum = checksum_bytes(&compressed);
    let uncompressed_checksum = checksum_bytes(text.as_bytes());
    let header = format!(
        "{DURABLE_COMPRESSION_HEADER}\ncodec\tzstd\nuncompressed_checksum\t{uncompressed_checksum}\ncompressed_checksum\t{compressed_checksum}\nuncompressed_len\t{}\ncompressed_len\t{}\n\n",
        text.len(),
        compressed.len()
    );
    let mut encoded = header.into_bytes();
    encoded.extend_from_slice(&compressed);
    Ok(encoded)
}

pub fn read_durable_text_bytes(bytes: &[u8], name: &str) -> Result<String> {
    read_durable_text_bytes_with_limit(bytes, name, None)
}

pub fn read_durable_text_bytes_with_limit(
    bytes: &[u8],
    name: &str,
    max_decoded_bytes: Option<u64>,
) -> Result<String> {
    if !bytes.starts_with(DURABLE_COMPRESSION_HEADER.as_bytes()) {
        return Err(SkeinError::Storage(format!(
            "{name} is missing the V1 compressed envelope"
        )));
    }
    decode_compressed_durable_text(bytes, name, max_decoded_bytes)
}

fn decode_compressed_durable_text(
    bytes: &[u8],
    name: &str,
    max_decoded_bytes: Option<u64>,
) -> Result<String> {
    let Some(header_end) = bytes.windows(2).position(|window| window == b"\n\n") else {
        return Err(SkeinError::Storage(format!(
            "{name} compressed envelope missing header terminator"
        )));
    };
    let header = std::str::from_utf8(&bytes[..header_end]).map_err(|error| {
        SkeinError::Storage(format!(
            "{name} compressed envelope header is invalid: {error}"
        ))
    })?;
    let payload = &bytes[header_end + 2..];
    let mut codec = None;
    let mut compressed_checksum = None;
    let mut uncompressed_checksum = None;
    let mut compressed_len = None;
    let mut uncompressed_len = None;
    let mut seen_fields = BTreeSet::new();
    for line in header.lines() {
        if line == DURABLE_COMPRESSION_HEADER {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        if !seen_fields.insert(fields[0]) {
            return Err(SkeinError::Storage(format!(
                "{name} compressed envelope has duplicate field: {}",
                fields[0]
            )));
        }
        match fields.as_slice() {
            ["codec", value] => codec = Some(*value),
            ["compressed_checksum", value] => {
                compressed_checksum = Some(parse_u64(value, "compressed checksum")?);
            }
            ["uncompressed_checksum", value] => {
                uncompressed_checksum = Some(parse_u64(value, "uncompressed checksum")?);
            }
            ["compressed_len", value] => {
                compressed_len = Some(parse_usize(value, "compressed length")?);
            }
            ["uncompressed_len", value] => {
                uncompressed_len = Some(parse_usize(value, "uncompressed length")?);
            }
            _ => {
                return Err(SkeinError::Storage(format!(
                    "{name} compressed envelope has invalid header line: {line}"
                )));
            }
        }
    }
    if codec != Some("zstd") {
        return Err(SkeinError::Storage(format!(
            "{name} compressed envelope uses unsupported codec"
        )));
    }
    let expected_compressed_len = compressed_len.ok_or_else(|| {
        SkeinError::Storage(format!("{name} compressed envelope missing compressed_len"))
    })?;
    if payload.len() != expected_compressed_len {
        return Err(SkeinError::Storage(format!(
            "{name} compressed length mismatch: expected {expected_compressed_len}, got {}",
            payload.len()
        )));
    }
    let expected_compressed_checksum = compressed_checksum.ok_or_else(|| {
        SkeinError::Storage(format!(
            "{name} compressed envelope missing compressed_checksum"
        ))
    })?;
    let actual_compressed_checksum = checksum_bytes(payload);
    if actual_compressed_checksum != expected_compressed_checksum {
        return Err(SkeinError::Storage(format!(
            "{name} compressed checksum mismatch: expected {expected_compressed_checksum}, got {actual_compressed_checksum}"
        )));
    }
    let expected_uncompressed_len = uncompressed_len.ok_or_else(|| {
        SkeinError::Storage(format!(
            "{name} compressed envelope missing uncompressed_len"
        ))
    })?;
    if max_decoded_bytes.is_some_and(|limit| expected_uncompressed_len as u64 > limit) {
        return Err(SkeinError::Storage(format!(
            "{name} decoded byte limit exceeded: max_decoded_bytes={}",
            max_decoded_bytes.unwrap_or_default()
        )));
    }
    let decode_limit = max_decoded_bytes
        .unwrap_or(expected_uncompressed_len as u64)
        .min(usize::MAX as u64);
    let mut decoder = zstd::stream::read::Decoder::new(Cursor::new(payload)).map_err(|error| {
        SkeinError::Storage(format!("{name} zstd decompression failed: {error}"))
    })?;
    let initial_capacity = expected_uncompressed_len.min(8 * 1024 * 1024);
    let mut decoded = Vec::with_capacity(initial_capacity);
    decoder
        .by_ref()
        .take(decode_limit.saturating_add(1))
        .read_to_end(&mut decoded)
        .map_err(|error| {
            SkeinError::Storage(format!("{name} zstd decompression failed: {error}"))
        })?;
    if decoded.len() as u64 > decode_limit {
        return Err(SkeinError::Storage(format!(
            "{name} decoded byte limit exceeded: max_decoded_bytes={decode_limit}"
        )));
    }
    if decoded.len() != expected_uncompressed_len {
        return Err(SkeinError::Storage(format!(
            "{name} uncompressed length mismatch: expected {expected_uncompressed_len}, got {}",
            decoded.len()
        )));
    }
    let expected_uncompressed_checksum = uncompressed_checksum.ok_or_else(|| {
        SkeinError::Storage(format!(
            "{name} compressed envelope missing uncompressed_checksum"
        ))
    })?;
    let actual_uncompressed_checksum = checksum_bytes(&decoded);
    if actual_uncompressed_checksum != expected_uncompressed_checksum {
        return Err(SkeinError::Storage(format!(
            "{name} uncompressed checksum mismatch: expected {expected_uncompressed_checksum}, got {actual_uncompressed_checksum}"
        )));
    }
    String::from_utf8(decoded).map_err(|error| {
        SkeinError::Storage(format!(
            "{name} decompressed payload is not valid UTF-8: {error}"
        ))
    })
}

fn parse_usize(input: &str, name: &str) -> Result<usize> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

#[cfg(test)]
mod tests;
