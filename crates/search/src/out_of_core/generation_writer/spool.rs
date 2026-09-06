use crate::build_control::checkpoint;
use crate::build_memory::{AdmittedDocument, BuildMemory, SPOOL_BUFFER_BYTES};
use crate::checksum_bytes;
use crate::error::{Result, SkeinError};
#[cfg(test)]
use crate::SearchDocument;
use skein_core::RuntimeTaskContext;
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
    pub(super) max_metadata_fields: usize,
    pub(super) memory: BuildMemory,
}

impl SpoolSource {
    #[cfg(test)]
    pub(super) fn scan(
        &self,
        consumer: &mut dyn FnMut(u64, SearchDocument) -> Result<()>,
    ) -> Result<()> {
        self.scan_with_context(&RuntimeTaskContext::default(), consumer)
    }

    #[cfg(test)]
    pub(super) fn scan_with_context(
        &self,
        task_context: &RuntimeTaskContext,
        consumer: &mut dyn FnMut(u64, SearchDocument) -> Result<()>,
    ) -> Result<()> {
        self.scan_admitted(task_context, &mut |ordinal, document| {
            let (document, _lease) = document.into_parts();
            consumer(ordinal, document)
        })
    }

    pub(super) fn scan_admitted(
        &self,
        task_context: &RuntimeTaskContext,
        consumer: &mut dyn FnMut(u64, AdmittedDocument) -> Result<()>,
    ) -> Result<()> {
        checkpoint(task_context)?;
        let _buffer_memory = self.memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let file = File::open(&self.path)?;
        #[cfg(test)]
        let file = read_evidence::track(file);
        let mut reader = BufReader::with_capacity(SPOOL_BUFFER_BYTES, file);
        let mut header = [0u8; SPOOL_HEADER.len()];
        reader.read_exact(&mut header)?;
        if &header != SPOOL_HEADER {
            return Err(SkeinError::Storage(
                "search generation spool header is invalid".to_string(),
            ));
        }
        let mut _previous_id_memory = None;
        let mut previous_id = None::<String>;
        for ordinal in 0..self.document_count {
            checkpoint(task_context)?;
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
            let record_memory = self.memory.spool.reserve(length)?;
            let mut record = Vec::new();
            record.try_reserve_exact(length).map_err(|error| {
                SkeinError::Storage(format!("spool record allocation failed: {error}"))
            })?;
            record.resize(length, 0);
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
            let document = self
                .memory
                .decode_document(line, self.max_metadata_fields)?;
            // The sinks need the decoded document, not its encoded spool copy.
            // Release it before lexical analysis and segment/vector buffering.
            drop(record);
            drop(record_memory);
            checkpoint(task_context)?;
            if previous_id
                .as_ref()
                .is_some_and(|previous| previous >= &document.id)
            {
                return Err(SkeinError::Storage(format!(
                    "search generation spool record {ordinal} is not strictly ordered"
                )));
            }
            let previous_id_memory = self.memory.retained.reserve(document.id.len())?;
            previous_id = Some(document.id.clone());
            _previous_id_memory = Some(previous_id_memory);
            let document_ordinal = u64::try_from(ordinal).map_err(|_| {
                SkeinError::Storage("search document ordinal exceeds u64".to_string())
            })?;
            consumer(document_ordinal, document)?;
            checkpoint(task_context)?;
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

#[cfg(test)]
pub(super) mod read_evidence {
    use super::*;
    use std::cell::{Cell, RefCell};

    thread_local! {
        static READS: Cell<(usize, u64)> = const { Cell::new((0, 0)) };
        static CANCEL_AFTER: RefCell<Option<(u64, crate::RuntimeCancellationToken)>> = const { RefCell::new(None) };
    }

    pub(in super::super) struct CancelGuard;

    impl Drop for CancelGuard {
        fn drop(&mut self) {
            CANCEL_AFTER.with(|slot| {
                slot.borrow_mut().take();
            });
        }
    }

    pub(in super::super) fn cancel_after_bytes(
        bytes: u64,
        token: crate::RuntimeCancellationToken,
    ) -> CancelGuard {
        CANCEL_AFTER.with(|slot| *slot.borrow_mut() = Some((bytes, token)));
        CancelGuard
    }

    pub(super) struct TrackedFile(File);

    pub(super) fn track(file: File) -> TrackedFile {
        READS.with(|reads| {
            let (opens, bytes) = reads.get();
            reads.set((opens + 1, bytes));
        });
        TrackedFile(file)
    }

    pub(in super::super) fn take() -> (usize, u64) {
        READS.with(|reads| reads.replace((0, 0)))
    }

    impl Read for TrackedFile {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            let count = self.0.read(output)?;
            READS.with(|reads| {
                let (opens, bytes) = reads.get();
                reads.set((opens, bytes + count as u64));
                CANCEL_AFTER.with(|slot| {
                    let mut slot = slot.borrow_mut();
                    if slot
                        .as_ref()
                        .is_some_and(|(limit, _)| bytes + count as u64 >= *limit)
                    {
                        slot.take().unwrap().1.cancel();
                    }
                });
            });
            Ok(count)
        }
    }
}
