//! Allocation-free compressed-envelope validation shared by search readers.

use crate::{checksum_bytes, Result, SkeinError, SEARCH_COMPRESSION_HEADER};

pub(crate) struct Envelope<'a> {
    pub(crate) payload: &'a [u8],
    pub(crate) decoded_len: usize,
    pub(crate) decoded_checksum: u64,
}

impl<'a> Envelope<'a> {
    pub(crate) fn parse(bytes: &'a [u8], limit: u64) -> Result<Self> {
        let end = bytes
            .windows(2)
            .position(|window| window == b"\n\n")
            .ok_or_else(|| error("missing header terminator"))?;
        let header =
            std::str::from_utf8(&bytes[..end]).map_err(|_| error("header is not UTF-8"))?;
        let payload = &bytes[end + 2..];
        let mut codec = None;
        let mut compressed_len = None;
        let mut compressed_checksum = None;
        let mut decoded_len = None;
        let mut decoded_checksum = None;
        let mut seen = 0u8;
        for line in header.lines() {
            if line == SEARCH_COMPRESSION_HEADER {
                continue;
            }
            let (key, value) = line
                .split_once('\t')
                .ok_or_else(|| error("invalid header line"))?;
            let bit = match key {
                "codec" => {
                    codec = Some(value);
                    1
                }
                "compressed_len" => {
                    compressed_len = Some(number(value)?);
                    2
                }
                "compressed_checksum" => {
                    compressed_checksum = Some(number(value)?);
                    4
                }
                "uncompressed_len" => {
                    decoded_len = Some(number(value)?);
                    8
                }
                "uncompressed_checksum" => {
                    decoded_checksum = Some(number(value)?);
                    16
                }
                _ => return Err(error("invalid header field")),
            };
            if seen & bit != 0 {
                return Err(error("duplicate field"));
            }
            seen |= bit;
        }
        if codec != Some("zstd") {
            return Err(error("uses unsupported codec"));
        }
        if compressed_len.ok_or_else(|| error("missing compressed_len"))? != payload.len() as u64 {
            return Err(error("compressed length mismatch"));
        }
        if compressed_checksum.ok_or_else(|| error("missing compressed_checksum"))?
            != checksum_bytes(payload)
        {
            return Err(error("compressed checksum mismatch"));
        }
        let decoded_len = decoded_len.ok_or_else(|| error("missing uncompressed_len"))?;
        if decoded_len > limit {
            return Err(SkeinError::Storage(format!("search projection uncompressed payload requires {decoded_len} bytes, exceeding {limit}")));
        }
        Ok(Self {
            payload,
            decoded_len: usize::try_from(decoded_len)
                .map_err(|_| error("uncompressed length exceeds address space"))?,
            decoded_checksum: decoded_checksum
                .ok_or_else(|| error("missing uncompressed_checksum"))?,
        })
    }
}

fn number(raw: &str) -> Result<u64> {
    raw.parse().map_err(|_| error("invalid header number"))
}

fn error(message: &str) -> SkeinError {
    SkeinError::Storage(format!("search projection compressed envelope {message}"))
}
