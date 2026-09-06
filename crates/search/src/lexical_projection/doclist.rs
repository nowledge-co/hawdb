use super::{dictionary::Metadata, posting_codec, Digest, RemoveOnDrop};
use crate::error::{Result, SkeinError};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

const SKIP_HEADER: &[u8; 4] = b"LXS1";
const FRAME_HEADER_BYTES: usize = 12;
const SKIP_ENTRY_BYTES: u64 = 16;

fn invalid(message: &str) -> SkeinError {
    SkeinError::Storage(format!("invalid lexical doclist: {message}"))
}

fn write_bytes(writer: &mut impl Write, offset: &mut u64, bytes: &[u8]) -> Result<()> {
    let next = offset
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| invalid("offset overflow"))?;
    writer.write_all(bytes)?;
    *offset = next;
    Ok(())
}

/// Skip records spill as they are produced; a frequent term cannot grow a Vec.
pub(super) struct Writer {
    skip: File,
    _guard: RemoveOnDrop,
    start: u64,
    df: u64,
    frames: u64,
    first_skip: [u8; 16],
    last_ordinal: Option<u64>,
    spill: super::dictionary_store::SpillBudget,
    max_frame_bytes: u64,
}

impl Writer {
    pub(super) fn new(
        path: &Path,
        spill: super::dictionary_store::SpillBudget,
        max_frame_bytes: u64,
    ) -> Result<Self> {
        let skip = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        Ok(Self {
            skip,
            _guard: RemoveOnDrop::new(path.to_owned()),
            start: 0,
            df: 0,
            frames: 0,
            first_skip: [0; 16],
            last_ordinal: None,
            spill,
            max_frame_bytes,
        })
    }

    fn spill_entry(&mut self, entry: &[u8]) -> Result<()> {
        self.spill.charge(entry.len() as u64)?;
        self.skip.write_all(entry)?;
        Ok(())
    }

    pub(super) fn push_frame(
        &mut self,
        writer: &mut impl Write,
        offset: &mut u64,
        postings: &[posting_codec::Posting],
    ) -> Result<()> {
        if self.frames > 0 && !self.df.is_multiple_of(posting_codec::BLOCK_LEN as u64) {
            return Err(invalid("a short frame must terminate its doclist"));
        }
        let encoded = posting_codec::encode(postings).map_err(invalid)?;
        if encoded.len() as u64 > self.max_frame_bytes {
            return Err(invalid("frame exceeds the configured read budget"));
        }
        if self
            .last_ordinal
            .is_some_and(|previous| postings[0].ordinal <= previous)
        {
            return Err(invalid("unordered frames"));
        }
        if self.frames == 0 {
            self.start = *offset;
        }
        let last = postings.last().unwrap().ordinal;
        let mut entry = [0u8; 16];
        entry[..8].copy_from_slice(&last.to_le_bytes());
        entry[8..].copy_from_slice(&(*offset - self.start).to_le_bytes());
        if self.frames == 0 {
            self.first_skip = entry;
        } else {
            if self.frames == 1 {
                let first = self.first_skip;
                self.spill_entry(&first)?;
            }
            self.spill_entry(&entry)?;
        }
        let mut header = [0; FRAME_HEADER_BYTES];
        header[..4].copy_from_slice(&(encoded.len() as u32).to_le_bytes());
        header[4..].copy_from_slice(&super::checksum(&encoded).to_le_bytes());
        write_bytes(writer, offset, &header)?;
        write_bytes(writer, offset, &encoded)?;
        self.df = self
            .df
            .checked_add(postings.len() as u64)
            .ok_or_else(|| invalid("DF overflow"))?;
        self.frames = self
            .frames
            .checked_add(1)
            .ok_or_else(|| invalid("frame count overflow"))?;
        self.last_ordinal = Some(last);
        Ok(())
    }

    pub(super) fn finish(&mut self, writer: &mut impl Write, offset: &mut u64) -> Result<Metadata> {
        if self.df == 0 {
            return Err(invalid("empty doclist"));
        }
        let mut skip_offset = 0;
        if self.frames > 1 {
            skip_offset = *offset - self.start;
            let mut digest = Digest::new();
            let header = [SKIP_HEADER.as_slice(), &self.frames.to_le_bytes()].concat();
            digest.update(&header);
            write_bytes(writer, offset, &header)?;
            self.skip.seek(SeekFrom::Start(0))?;
            let mut remaining = self
                .frames
                .checked_mul(SKIP_ENTRY_BYTES)
                .ok_or_else(|| invalid("skip extent overflow"))?;
            let mut buffer = [0u8; 8192];
            while remaining != 0 {
                let count = remaining.min(buffer.len() as u64) as usize;
                self.skip.read_exact(&mut buffer[..count])?;
                digest.update(&buffer[..count]);
                write_bytes(writer, offset, &buffer[..count])?;
                remaining -= count as u64;
            }
            write_bytes(writer, offset, &(digest.finish() as u32).to_le_bytes())?;
        }
        let metadata = Metadata {
            df: self.df,
            posting_offset: self.start,
            posting_bytes: *offset - self.start,
            skip_offset,
        };
        self.skip.set_len(0)?;
        self.skip.seek(SeekFrom::Start(0))?;
        self.df = 0;
        self.frames = 0;
        self.last_ordinal = None;
        Ok(metadata)
    }
}

pub(super) struct Cursor {
    metadata: Metadata,
    position: u64,
    remaining: u64,
    frame: u64,
    last_ordinal: Option<u64>,
    skip_digest: Digest,
}

impl Cursor {
    pub(super) fn new(metadata: Metadata) -> Result<Self> {
        if metadata.df == 0
            || metadata.posting_bytes == 0
            || metadata
                .posting_offset
                .checked_add(metadata.posting_bytes)
                .is_none()
            || metadata.skip_offset >= metadata.posting_bytes
        {
            return Err(invalid("metadata extent"));
        }
        let frames = (metadata.df - 1) / posting_codec::BLOCK_LEN as u64 + 1;
        if frames == 1 && metadata.skip_offset != 0
            || frames > 1
                && (metadata.skip_offset == 0
                    || frames
                        .checked_mul(SKIP_ENTRY_BYTES)
                        .and_then(|bytes| bytes.checked_add(16))
                        != Some(metadata.posting_bytes - metadata.skip_offset))
        {
            return Err(invalid("skip extent"));
        }
        Ok(Self {
            metadata,
            position: 0,
            remaining: metadata.df,
            frame: 0,
            last_ordinal: None,
            skip_digest: Digest::new(),
        })
    }

    pub(super) fn next_frame(
        &mut self,
        read: &mut impl FnMut(u64, usize, Option<u64>) -> Result<Arc<[u8]>>,
    ) -> Result<Option<Vec<posting_codec::Posting>>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let metadata = self.metadata;
        let data_end = if metadata.skip_offset == 0 {
            metadata.posting_bytes
        } else {
            metadata.skip_offset
        };
        if data_end.saturating_sub(self.position) < FRAME_HEADER_BYTES as u64 {
            return Err(invalid("truncated frame header"));
        }
        let header = read(
            metadata.posting_offset + self.position,
            FRAME_HEADER_BYTES,
            None,
        )?;
        let header: [u8; FRAME_HEADER_BYTES] = header
            .as_ref()
            .try_into()
            .map_err(|_| invalid("short frame header"))?;
        let length = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        let checksum = u64::from_le_bytes(header[4..].try_into().unwrap());
        let frame_bytes = FRAME_HEADER_BYTES as u64 + length as u64;
        if length > posting_codec::MAX_BLOCK_BYTES || frame_bytes > data_end - self.position {
            return Err(invalid("frame length exceeds its extent"));
        }
        let bytes = read(
            metadata.posting_offset + self.position + FRAME_HEADER_BYTES as u64,
            length,
            Some(checksum),
        )?;
        if bytes.len() != length || super::checksum(&bytes) != checksum {
            return Err(invalid("frame checksum mismatch"));
        }
        let postings = posting_codec::decode(&bytes).map_err(invalid)?;
        let expected_count = self.remaining.min(posting_codec::BLOCK_LEN as u64);
        if postings.len() as u64 != expected_count
            || self
                .last_ordinal
                .is_some_and(|previous| previous >= postings[0].ordinal)
        {
            return Err(invalid("frame count or order"));
        }
        let last = postings.last().unwrap().ordinal;
        if metadata.skip_offset != 0 {
            let skip_start = metadata.posting_offset + metadata.skip_offset;
            if self.frame == 0 {
                let bytes = read(skip_start, 12, None)?;
                if bytes.len() != 12
                    || &bytes[..4] != SKIP_HEADER
                    || u64::from_le_bytes(bytes[4..].try_into().unwrap())
                        != (metadata.df - 1) / 128 + 1
                {
                    return Err(invalid("skip header"));
                }
                self.skip_digest.update(&bytes);
            }
            let bytes = read(skip_start + 12 + self.frame * SKIP_ENTRY_BYTES, 16, None)?;
            if bytes.len() != 16
                || u64::from_le_bytes(bytes[..8].try_into().unwrap()) != last
                || u64::from_le_bytes(bytes[8..].try_into().unwrap()) != self.position
            {
                return Err(invalid("skip record disagrees with frame"));
            }
            self.skip_digest.update(&bytes);
        }
        self.remaining -= expected_count;
        self.position += frame_bytes;
        self.frame += 1;
        self.last_ordinal = Some(last);
        if self.remaining == 0 {
            if self.position != data_end {
                return Err(invalid("trailing frame bytes"));
            }
            if metadata.skip_offset != 0 {
                let bytes = read(
                    metadata.posting_offset + metadata.posting_bytes - 4,
                    4,
                    None,
                )?;
                if bytes.len() != 4
                    || u32::from_le_bytes(bytes.as_ref().try_into().unwrap())
                        != self.skip_digest.finish() as u32
                {
                    return Err(invalid("skip checksum mismatch"));
                }
            }
        }
        Ok(Some(postings))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(bytes: &[u8], metadata: Metadata) -> Result<Vec<posting_codec::Posting>> {
        let mut cursor = Cursor::new(metadata)?;
        let mut output = Vec::new();
        while let Some(frame) = cursor.next_frame(&mut |offset, length, _| {
            let start = usize::try_from(offset).map_err(|_| invalid("test range overflow"))?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| invalid("test range overflow"))?;
            bytes
                .get(start..end)
                .map(Arc::from)
                .ok_or_else(|| invalid("test short read"))
        })? {
            output.extend(frame);
        }
        assert!(cursor
            .next_frame(&mut |_, _, _| panic!("exhausted cursor must not read"))
            .unwrap()
            .is_none());
        Ok(output)
    }

    #[test]
    fn spilled_skip_records_roundtrip_and_reject_rechecksummed_corruption() {
        let root = super::super::tests::projection_root("compact-skip-roundtrip");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("skip.tmp");
        let mut writer = Writer::new(
            &path,
            super::super::dictionary_store::SpillBudget::new(0, 4096),
            4096,
        )
        .unwrap();
        let postings = (0..257)
            .map(|index| posting_codec::Posting {
                ordinal: index * 1000,
                tf: 1,
            })
            .collect::<Vec<_>>();
        let mut bytes = Vec::new();
        let mut offset = 0;
        for frame in postings.chunks(posting_codec::BLOCK_LEN) {
            writer.push_frame(&mut bytes, &mut offset, frame).unwrap();
        }
        let metadata = writer.finish(&mut bytes, &mut offset).unwrap();
        assert_eq!(decode(&bytes, metadata).unwrap(), postings);
        for length in 0..bytes.len() {
            assert!(
                decode(&bytes[..length], metadata).is_err(),
                "accepted truncated prefix {length}"
            );
        }
        let skip = metadata.skip_offset as usize;
        for damaged in [skip, skip + 4, skip + 12, skip + 20, skip + 28, skip + 36] {
            let mut corrupt = bytes.clone();
            corrupt[damaged] ^= 1;
            let end = corrupt.len() - 4;
            let crc = super::super::checksum(&corrupt[skip..end]) as u32;
            corrupt[end..].copy_from_slice(&crc.to_le_bytes());
            assert!(
                decode(&corrupt, metadata).is_err(),
                "accepted rechecksummed skip corruption at {damaged}"
            );
        }
        let mut bad_checksum = bytes.clone();
        *bad_checksum.last_mut().unwrap() ^= 1;
        assert!(decode(&bad_checksum, metadata).is_err());
        let mut wrong_count = metadata;
        wrong_count.df += 1;
        assert!(decode(&bytes, wrong_count).is_err());

        // Reusing the temporary file must not retain the previous term's skip entries.
        let tail = [posting_codec::Posting { ordinal: 7, tf: 3 }];
        writer.push_frame(&mut bytes, &mut offset, &tail).unwrap();
        let second = writer.finish(&mut bytes, &mut offset).unwrap();
        assert_eq!(second.skip_offset, 0);
        assert_eq!(decode(&bytes, second).unwrap(), tail);
        drop(writer);
        assert!(!path.exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn partial_frames_and_skip_spill_exhaustion_fail_before_publication() {
        let root = super::super::tests::projection_root("compact-skip-budgets");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("skip.tmp");
        let budget = super::super::dictionary_store::SpillBudget::new(0, 31);
        let mut writer = Writer::new(&path, budget.clone(), 4096).unwrap();
        let mut bytes = Vec::new();
        let mut offset = 0;
        let postings = (0..129)
            .map(|ordinal| posting_codec::Posting { ordinal, tf: 1 })
            .collect::<Vec<_>>();
        writer
            .push_frame(&mut bytes, &mut offset, &postings[..128])
            .unwrap();
        let before = bytes.clone();
        assert!(writer
            .push_frame(&mut bytes, &mut offset, &postings[128..])
            .is_err());
        assert_eq!(bytes, before);
        drop(writer);
        assert!(!path.exists());
        let mut writer = Writer::new(&path, budget, 4096).unwrap();
        writer
            .push_frame(&mut bytes, &mut offset, &postings[..1])
            .unwrap();
        assert!(writer
            .push_frame(&mut bytes, &mut offset, &postings[1..2])
            .unwrap_err()
            .to_string()
            .contains("short frame"));
        drop(writer);
        assert!(!path.exists());
        let mut writer = Writer::new(
            &path,
            super::super::dictionary_store::SpillBudget::new(0, 4096),
            128,
        )
        .unwrap();
        let wide = (0..128)
            .map(|ordinal| posting_codec::Posting {
                ordinal: ordinal * (u64::from(u32::MAX) + 1),
                tf: u32::MAX,
            })
            .collect::<Vec<_>>();
        let before = bytes.clone();
        assert!(writer
            .push_frame(&mut bytes, &mut offset, &wide)
            .unwrap_err()
            .to_string()
            .contains("configured read budget"));
        assert_eq!(bytes, before);
        drop(writer);
        std::fs::remove_dir_all(root).unwrap();
    }
}
