use crate::error::{Result, SkeinError};
use crate::{checksum_bytes, decode_search_document_line, SearchDocument};
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) const SPOOL_HEADER: &[u8; 8] = b"SKNSPOL1";
pub(super) const SPOOL_FRAME_HEADER_BYTES: u64 = 16;
static GENERATION_WRITER_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(super) struct SpoolSource {
    pub(super) path: PathBuf,
    pub(super) document_count: usize,
    pub(super) max_record_bytes: u64,
}

impl SpoolSource {
    pub(super) fn scan(
        &self,
        consumer: &mut dyn FnMut(SearchDocument) -> Result<()>,
    ) -> Result<()> {
        let mut reader = BufReader::new(File::open(&self.path)?);
        let mut header = [0u8; SPOOL_HEADER.len()];
        reader.read_exact(&mut header)?;
        if &header != SPOOL_HEADER {
            return Err(SkeinError::Storage(
                "search generation spool header is invalid".to_string(),
            ));
        }
        let mut previous_id = None::<String>;
        for ordinal in 0..self.document_count {
            let mut raw_length = [0u8; 8];
            let mut raw_checksum = [0u8; 8];
            reader.read_exact(&mut raw_length).map_err(|error| {
                SkeinError::Storage(format!(
                    "search generation spool record {ordinal} has a truncated length: {error}"
                ))
            })?;
            reader.read_exact(&mut raw_checksum).map_err(|error| {
                SkeinError::Storage(format!(
                    "search generation spool record {ordinal} has a truncated checksum: {error}"
                ))
            })?;
            let length = u64::from_le_bytes(raw_length);
            if length == 0 || length > self.max_record_bytes {
                return Err(SkeinError::Storage(format!(
                    "search generation spool record {ordinal} length {length} is outside its admission"
                )));
            }
            let length = usize::try_from(length).map_err(|_| {
                SkeinError::Storage(format!(
                    "search generation spool record {ordinal} length exceeds usize"
                ))
            })?;
            let mut record = vec![0u8; length];
            reader.read_exact(&mut record).map_err(|error| {
                SkeinError::Storage(format!(
                    "search generation spool record {ordinal} is truncated: {error}"
                ))
            })?;
            let expected_checksum = u64::from_le_bytes(raw_checksum);
            let actual_checksum = checksum_bytes(&record);
            if actual_checksum != expected_checksum {
                return Err(SkeinError::Storage(format!(
                    "search generation spool record {ordinal} checksum mismatch"
                )));
            }
            let line = std::str::from_utf8(&record).map_err(|error| {
                SkeinError::Storage(format!(
                    "search generation spool record {ordinal} is not UTF-8: {error}"
                ))
            })?;
            let document = decode_search_document_line(line)?;
            if previous_id
                .as_ref()
                .is_some_and(|previous| previous >= &document.id)
            {
                return Err(SkeinError::Storage(format!(
                    "search generation spool record {ordinal} is not strictly ordered"
                )));
            }
            previous_id = Some(document.id.clone());
            consumer(document)?;
        }
        let mut trailing = [0u8; 1];
        if reader.read(&mut trailing)? != 0 {
            return Err(SkeinError::Storage(
                "search generation spool has trailing records".to_string(),
            ));
        }
        Ok(())
    }
}

pub(super) struct StageDirectory {
    pub(super) path: PathBuf,
}

impl StageDirectory {
    pub(super) fn create(root: &Path) -> Result<Self> {
        for _ in 0..64 {
            let sequence = GENERATION_WRITER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = root.join(format!(
                ".search-generation.{}.{}.stage",
                std::process::id(),
                sequence
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(SkeinError::Storage(
            "failed to allocate a unique search generation stage directory".to_string(),
        ))
    }
}

impl Drop for StageDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
