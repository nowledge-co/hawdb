//! A single cacheable envelope for an FST and full-width term metadata.

use super::fst_validation;
use fst::Streamer;
use std::io::{self, Write};

type Result<T> = std::result::Result<T, &'static str>;
const HEADER: usize = 24;
const RECORD: usize = 32;
const CHECKSUM: usize = 4;

#[derive(Clone, Copy)]
pub(super) struct Limits {
    pub max_bytes: usize,
    pub max_terms: u32,
    pub max_key_bytes: u32,
    pub max_builder_bytes: usize,
    pub max_validation_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Metadata {
    pub df: u64,
    pub posting_offset: u64,
    pub posting_bytes: u64,
    pub skip_offset: u64,
}

impl Metadata {
    fn validate(self) -> Result<Self> {
        if self.df == 0
            || self.posting_bytes == 0
            || self
                .posting_offset
                .checked_add(self.posting_bytes)
                .is_none()
            || self.skip_offset >= self.posting_bytes
        {
            return Err("invalid term metadata");
        }
        Ok(self)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != RECORD {
            return Err("invalid metadata extent");
        }
        let read = |start| u64::from_le_bytes(bytes[start..start + 8].try_into().unwrap());
        Self {
            df: read(0),
            posting_offset: read(8),
            posting_bytes: read(16),
            skip_offset: read(24),
        }
        .validate()
    }
}

/// Conservative requested capacity for pinned fst 0.4.7 on 32/64-bit targets.
/// This is admission accounting, not a measurement of allocator overhead/RSS.
pub(super) fn builder_reservation(entries: &[(String, Metadata)], limits: Limits) -> Result<usize> {
    if entries.is_empty() || entries.len() > limits.max_terms as usize {
        return Err("dictionary term budget exceeded");
    }
    let mut key_bytes = 0usize;
    for (key, metadata) in entries {
        if key.is_empty() || key.len() > limits.max_key_bytes as usize {
            return Err("dictionary key budget exceeded");
        }
        metadata.validate()?;
        key_bytes = key_bytes
            .checked_add(key.len())
            .ok_or("dictionary key size overflow")?;
    }
    // fst allocates 10,000 two-cell registry buckets. Each cell has one owned
    // node and an address (at most 64 bytes on the supported pointer widths).
    // The variable allowance covers transition-vector growth, unfinished trie
    // nodes and previous-key copies. Caller-owned staging is admitted separately.
    // Total trie transitions cannot exceed the sum of inserted key lengths.
    let variable = key_bytes
        .checked_mul(256)
        .ok_or("builder reservation overflow")?;
    let output = limits
        .max_bytes
        .checked_mul(2)
        .ok_or("builder reservation overflow")?;
    let reservation = (20000usize * 64)
        .checked_add(variable)
        .and_then(|bytes| bytes.checked_add(output))
        .ok_or("builder reservation overflow")?;
    if reservation > limits.max_builder_bytes {
        return Err("dictionary builder budget exceeded");
    }
    Ok(reservation)
}

struct BoundedWriter<'a, F> {
    bytes: Vec<u8>,
    limit: usize,
    checkpoint: &'a mut F,
}

impl<F: FnMut() -> Result<()>> Write for BoundedWriter<'_, F> {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        (self.checkpoint)().map_err(io::Error::other)?;
        if input.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("dictionary byte budget exceeded"));
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        (self.checkpoint)().map_err(io::Error::other)
    }
}

pub(super) fn build(
    entries: &[(String, Metadata)],
    limits: Limits,
    checkpoint: &mut impl FnMut() -> Result<()>,
) -> Result<Vec<u8>> {
    checkpoint()?;
    builder_reservation(entries, limits)?;
    let prefix = entries
        .len()
        .checked_mul(RECORD)
        .and_then(|bytes| bytes.checked_add(HEADER))
        .ok_or("metadata size overflow")?;
    if limits.max_bytes > u32::MAX as usize
        || prefix
            .checked_add(36 + CHECKSUM)
            .is_none_or(|bytes| bytes > limits.max_bytes)
    {
        return Err("dictionary byte budget exceeded");
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(limits.max_bytes)
        .map_err(|_| "dictionary allocation failed")?;
    bytes.resize(HEADER, 0);
    bytes[..4].copy_from_slice(b"LXD1");
    bytes[4..8].copy_from_slice(&(entries.len() as u32).to_le_bytes());
    bytes[8..12].copy_from_slice(&(RECORD as u32).to_le_bytes());
    for (_, metadata) in entries {
        checkpoint()?;
        for value in [
            metadata.df,
            metadata.posting_offset,
            metadata.posting_bytes,
            metadata.skip_offset,
        ] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
    }
    let writer = BoundedWriter {
        bytes,
        limit: limits.max_bytes - CHECKSUM,
        checkpoint,
    };
    let mut builder = fst::MapBuilder::new(writer).map_err(|_| "dictionary builder failed")?;
    for (index, (key, _)) in entries.iter().enumerate() {
        builder
            .insert(key, (index * RECORD) as u64)
            .map_err(|_| "dictionary build failed")?;
    }
    let mut writer = builder
        .into_inner()
        .map_err(|_| "dictionary finish failed")?;
    (writer.checkpoint)()?;
    let fst_bytes = writer.bytes.len() - prefix;
    writer.bytes[12..16].copy_from_slice(&(fst_bytes as u32).to_le_bytes());
    let crc = skein_integrity::crc32c(&writer.bytes).get();
    writer.bytes.extend_from_slice(&crc.to_le_bytes());
    Ok(writer.bytes)
}

pub(super) struct Dictionary<'a> {
    map: fst::Map<&'a [u8]>,
    metadata: &'a [u8],
}

impl<'a> Dictionary<'a> {
    pub(super) fn open(
        bytes: &'a [u8],
        limits: Limits,
        checkpoint: &mut impl FnMut() -> Result<()>,
    ) -> Result<Self> {
        checkpoint()?;
        if bytes.len() < HEADER + RECORD + 36 + CHECKSUM
            || bytes.len() > limits.max_bytes
            || &bytes[..4] != b"LXD1"
            || bytes[16..HEADER].iter().any(|byte| *byte != 0)
        {
            return Err("invalid dictionary envelope");
        }
        let read = |start| u32::from_le_bytes(bytes[start..start + 4].try_into().unwrap()) as usize;
        let count = read(4);
        let fst_bytes = read(12);
        let prefix = count
            .checked_mul(RECORD)
            .and_then(|size| size.checked_add(HEADER))
            .ok_or("dictionary extent overflow")?;
        if count == 0
            || count > limits.max_terms as usize
            || read(8) != RECORD
            || prefix
                .checked_add(fst_bytes)
                .and_then(|size| size.checked_add(CHECKSUM))
                != Some(bytes.len())
        {
            return Err("invalid dictionary extent");
        }
        let payload = &bytes[..bytes.len() - CHECKSUM];
        let crc = u32::from_le_bytes(bytes[payload.len()..].try_into().unwrap());
        if skein_integrity::crc32c(payload).get() != crc {
            return Err("invalid dictionary checksum");
        }
        let metadata = &bytes[HEADER..prefix];
        let map = fst_validation::open_checked(
            &bytes[prefix..payload.len()],
            fst_validation::Limits {
                max_bytes: limits.max_bytes,
                max_scratch_bytes: limits.max_validation_bytes,
                max_nodes: limits.max_bytes,
                max_keys: count as u64,
                max_key_bytes: limits.max_key_bytes,
                max_value: (metadata.len() - RECORD) as u64,
            },
            checkpoint,
        )?;
        if map.len() != count {
            return Err("dictionary key count disagrees with records");
        }
        let mut stream = map.stream();
        let mut index = 0;
        while let Some((key, offset)) = stream.next() {
            checkpoint()?;
            if key.is_empty()
                || std::str::from_utf8(key).is_err()
                || offset != (index * RECORD) as u64
            {
                return Err("dictionary keys do not address their ordered records");
            }
            Metadata::from_bytes(&metadata[index * RECORD..(index + 1) * RECORD])?;
            index += 1;
        }
        Ok(Self { map, metadata })
    }

    pub(super) fn visit(
        &self,
        mut visitor: impl FnMut(&str, Metadata) -> Result<()>,
    ) -> Result<()> {
        let mut stream = self.map.stream();
        while let Some((key, offset)) = stream.next() {
            let key = std::str::from_utf8(key).map_err(|_| "invalid dictionary key")?;
            let start = usize::try_from(offset).map_err(|_| "metadata address overflow")?;
            let end = start
                .checked_add(RECORD)
                .ok_or("metadata address overflow")?;
            let metadata = Metadata::from_bytes(
                self.metadata
                    .get(start..end)
                    .ok_or("metadata address out of bounds")?,
            )?;
            visitor(key, metadata)?;
        }
        Ok(())
    }

    pub(super) fn get(&self, term: &str) -> Result<Option<Metadata>> {
        let Some(offset) = self.map.get(term) else {
            return Ok(None);
        };
        let start = usize::try_from(offset).map_err(|_| "metadata address overflow")?;
        let end = start
            .checked_add(RECORD)
            .ok_or("metadata address overflow")?;
        let bytes = self
            .metadata
            .get(start..end)
            .ok_or("metadata address out of bounds")?;
        Metadata::from_bytes(bytes).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> Limits {
        Limits {
            max_bytes: 4096,
            max_terms: 64,
            max_key_bytes: 64,
            max_builder_bytes: 2 * 1024 * 1024,
            max_validation_bytes: 128 * 1024,
        }
    }

    fn entries() -> Vec<(String, Metadata)> {
        ["identifier-part", "prefix-a", "prefix-b", "中国", "中文"]
            .into_iter()
            .enumerate()
            .map(|(index, term)| {
                (
                    term.to_owned(),
                    Metadata {
                        df: u64::MAX - index as u64,
                        posting_offset: u64::MAX - 1000,
                        posting_bytes: 256,
                        skip_offset: 128,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn one_envelope_retains_full_width_metadata_and_bounded_lookup() {
        let entries = entries();
        let bytes = build(&entries, limits(), &mut || Ok(())).unwrap();
        assert!(bytes.len() <= limits().max_bytes);
        assert_eq!(bytes.capacity(), limits().max_bytes);
        let dictionary = Dictionary::open(&bytes, limits(), &mut || Ok(())).unwrap();
        for (term, metadata) in entries {
            assert_eq!(dictionary.get(&term).unwrap(), Some(metadata));
        }
        assert_eq!(dictionary.get("missing").unwrap(), None);
    }

    #[test]
    fn output_and_builder_budgets_are_independent_and_cancellation_stops_build() {
        let entries = entries();
        let needed = builder_reservation(&entries, limits()).unwrap();
        assert!(needed >= 20000 * 64);
        for limit in [
            Limits {
                max_builder_bytes: needed - 1,
                ..limits()
            },
            Limits {
                max_bytes: 255,
                ..limits()
            },
            Limits {
                max_terms: 4,
                ..limits()
            },
            Limits {
                max_key_bytes: 8,
                ..limits()
            },
        ] {
            assert!(build(&entries, limit, &mut || Ok(())).is_err());
        }
        let mut checkpoints = 0;
        assert!(build(&entries, limits(), &mut || {
            checkpoints += 1;
            if checkpoints == 20 {
                Err("cancelled")
            } else {
                Ok(())
            }
        })
        .is_err());
        assert_eq!(checkpoints, 20);
    }

    #[test]
    fn every_truncation_and_rechecksummed_metadata_error_is_rejected() {
        let bytes = build(&entries(), limits(), &mut || Ok(())).unwrap();
        for length in 0..bytes.len() {
            assert!(Dictionary::open(&bytes[..length], limits(), &mut || Ok(())).is_err());
        }
        for (offset, value) in [
            (HEADER, 0),
            (HEADER + 8, u64::MAX),
            (HEADER + 16, 0),
            (HEADER + 24, 256),
        ] {
            let mut damaged = bytes.clone();
            damaged[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            let end = damaged.len() - CHECKSUM;
            let crc = skein_integrity::crc32c(&damaged[..end]).get();
            damaged[end..].copy_from_slice(&crc.to_le_bytes());
            assert!(Dictionary::open(&damaged, limits(), &mut || Ok(())).is_err());
        }
    }

    #[test]
    fn structurally_valid_fst_must_address_each_metadata_record_exactly_once() {
        let entries = entries();
        let bytes = build(&entries, limits(), &mut || Ok(())).unwrap();
        for invalid_offset in [0, 1, (RECORD * entries.len()) as u64] {
            let mut builder = fst::MapBuilder::memory();
            for (index, (term, _)) in entries.iter().enumerate() {
                let offset = if index == 1 {
                    invalid_offset
                } else {
                    (index * RECORD) as u64
                };
                builder.insert(term, offset).unwrap();
            }
            let fst = builder.into_inner().unwrap();
            let prefix = HEADER + RECORD * entries.len();
            let mut damaged = bytes[..prefix].to_vec();
            damaged[12..16].copy_from_slice(&(fst.len() as u32).to_le_bytes());
            damaged.extend_from_slice(&fst);
            let crc = skein_integrity::crc32c(&damaged).get();
            damaged.extend_from_slice(&crc.to_le_bytes());
            assert!(Dictionary::open(&damaged, limits(), &mut || Ok(())).is_err());
        }
    }
}
