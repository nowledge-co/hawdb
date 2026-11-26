// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use std::io;

/// Implementation-independent memory-relevant fields of an ordinary Zstd frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub window_bytes: u64,
    pub content_bytes: Option<u64>,
    pub block_bytes: u64,
    pub skippable: bool,
}

/// Parse a complete frame header without constructing a decoder or allocating.
pub fn parse_frame_header(bytes: &[u8]) -> io::Result<FrameHeader> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid Zstd frame header");
    let magic = u32::from_le_bytes(bytes.get(..4).ok_or_else(invalid)?.try_into().unwrap());
    if magic & 0xfffffff0 == 0x184d2a50 {
        if bytes.len() < 8 {
            return Err(invalid());
        }
        return Ok(FrameHeader {
            window_bytes: 0,
            content_bytes: Some(0),
            block_bytes: 0,
            skippable: true,
        });
    }
    if magic != 0xfd2fb528 {
        return Err(invalid());
    }
    let descriptor = *bytes.get(4).ok_or_else(invalid)?;
    if descriptor & 8 != 0 {
        return Err(invalid());
    }
    let single = descriptor & 0x20 != 0;
    let dictionary = [0, 1, 2, 4][(descriptor & 3) as usize];
    let size_bytes = [usize::from(single), 2, 4, 8][(descriptor >> 6) as usize];
    let start = 5 + usize::from(!single) + dictionary;
    let field = bytes.get(start..start + size_bytes).ok_or_else(invalid)?;
    let content_bytes = if size_bytes == 0 {
        None
    } else {
        let mut value = [0; 8];
        value[..size_bytes].copy_from_slice(field);
        let value = u64::from_le_bytes(value) + if size_bytes == 2 { 256 } else { 0 };
        if value >= u64::MAX - 1 {
            return Err(invalid());
        }
        Some(value)
    };
    let window_bytes = if single {
        content_bytes.ok_or_else(invalid)?
    } else {
        let descriptor = *bytes.get(5).ok_or_else(invalid)?;
        let base = 1u64 << (10 + (descriptor >> 3));
        base + (base >> 3) * u64::from(descriptor & 7)
    };
    Ok(FrameHeader {
        window_bytes,
        content_bytes,
        block_bytes: window_bytes.min(128 * 1024),
        skippable: false,
    })
}

#[cfg(test)]
mod tests;
