// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crc32c::crc32c_append;
use sha2::{Digest, Sha256};
use std::fmt::{self, Display, Formatter};
use std::hash::Hasher;
use std::str::FromStr;

pub const SHA256_BYTES: usize = 32;
pub const SHA256_HEX_BYTES: usize = SHA256_BYTES * 2;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Crc32c(u32);

impl Crc32c {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    pub const fn as_u64(self) -> u64 {
        self.0 as u64
    }
}

impl Display for Crc32c {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Sha256Digest([u8; SHA256_BYTES]);

impl Sha256Digest {
    pub const fn from_bytes(bytes: [u8; SHA256_BYTES]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; SHA256_BYTES] {
        &self.0
    }
}

impl Display for Sha256Digest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseSha256DigestError {
    InvalidLength { actual: usize },
    InvalidHex { offset: usize },
}

impl Display for ParseSha256DigestError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { actual } => write!(
                formatter,
                "SHA-256 digest must contain {SHA256_HEX_BYTES} hexadecimal bytes, got {actual}"
            ),
            Self::InvalidHex { offset } => {
                write!(
                    formatter,
                    "SHA-256 digest contains invalid hex at byte {offset}"
                )
            }
        }
    }
}

impl std::error::Error for ParseSha256DigestError {}

impl FromStr for Sha256Digest {
    type Err = ParseSha256DigestError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != SHA256_HEX_BYTES {
            return Err(ParseSha256DigestError::InvalidLength {
                actual: value.len(),
            });
        }
        let mut bytes = [0u8; SHA256_BYTES];
        for (index, byte) in bytes.iter_mut().enumerate() {
            let offset = index * 2;
            let high = parse_hex_nibble(value.as_bytes()[offset], offset)?;
            let low = parse_hex_nibble(value.as_bytes()[offset + 1], offset + 1)?;
            *byte = (high << 4) | low;
        }
        Ok(Self(bytes))
    }
}

fn parse_hex_nibble(byte: u8, offset: usize) -> Result<u8, ParseSha256DigestError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(ParseSha256DigestError::InvalidHex { offset }),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntegrityDigest {
    pub crc32c: Crc32c,
    pub sha256: Sha256Digest,
}

#[derive(Clone)]
pub struct IntegrityHasher {
    crc32c: u32,
    sha256: Sha256,
}

impl Default for IntegrityHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl IntegrityHasher {
    pub fn new() -> Self {
        Self {
            crc32c: 0,
            sha256: Sha256::new(),
        }
    }

    #[inline]
    pub fn update(&mut self, bytes: &[u8]) {
        self.crc32c = crc32c_append(self.crc32c, bytes);
        self.sha256.update(bytes);
    }

    pub fn finish(self) -> IntegrityDigest {
        let bytes: [u8; SHA256_BYTES] = self.sha256.finalize().into();
        IntegrityDigest {
            crc32c: Crc32c(self.crc32c),
            sha256: Sha256Digest(bytes),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Crc32cHasher(u32);

impl Crc32cHasher {
    pub const fn new() -> Self {
        Self(0)
    }

    #[inline]
    pub fn update(&mut self, bytes: &[u8]) {
        self.0 = crc32c_append(self.0, bytes);
    }

    pub const fn finish_u32(self) -> u32 {
        self.0
    }

    pub const fn finish_u64(self) -> u64 {
        self.0 as u64
    }

    pub const fn finish(self) -> u64 {
        self.finish_u64()
    }
}

impl Hasher for Crc32cHasher {
    fn finish(&self) -> u64 {
        u64::from(self.0)
    }

    fn write(&mut self, bytes: &[u8]) {
        self.update(bytes);
    }
}

#[inline]
pub fn crc32c(bytes: &[u8]) -> Crc32c {
    Crc32c(::crc32c::crc32c(bytes))
}

#[inline]
pub fn checksum_u64(bytes: &[u8]) -> u64 {
    crc32c(bytes).as_u64()
}

pub fn sha256(bytes: &[u8]) -> Sha256Digest {
    let bytes: [u8; SHA256_BYTES] = Sha256::digest(bytes).into();
    Sha256Digest(bytes)
}

pub fn integrity_digest(bytes: &[u8]) -> IntegrityDigest {
    let mut hasher = IntegrityHasher::new();
    hasher.update(bytes);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_matches_castagnoli_known_vector() {
        assert_eq!(crc32c(b"123456789").get(), 0xe306_9283);
    }

    #[test]
    fn streaming_crc32c_matches_one_shot() {
        let mut hasher = Crc32cHasher::new();
        hasher.update(b"1234");
        hasher.update(b"56789");
        assert_eq!(hasher.finish_u32(), crc32c(b"123456789").get());
    }

    #[test]
    fn sha256_round_trips_through_hex() {
        let digest = sha256(b"abc");
        assert_eq!(
            digest.to_string(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(digest.to_string().parse(), Ok(digest));
    }

    #[test]
    fn combined_hasher_matches_independent_digests() {
        let payload = b"hawdb-integrity";
        let digest = integrity_digest(payload);
        assert_eq!(digest.crc32c, crc32c(payload));
        assert_eq!(digest.sha256, sha256(payload));
    }

    #[test]
    fn sha256_parser_rejects_non_canonical_input() {
        assert!(matches!(
            "00".parse::<Sha256Digest>(),
            Err(ParseSha256DigestError::InvalidLength { .. })
        ));
        let invalid = format!("{}zz", "00".repeat(31));
        assert!(matches!(
            invalid.parse::<Sha256Digest>(),
            Err(ParseSha256DigestError::InvalidHex { offset: 62 })
        ));
        assert!(matches!(
            "é".repeat(32).parse::<Sha256Digest>(),
            Err(ParseSha256DigestError::InvalidHex { offset: 0 })
        ));
    }
}
