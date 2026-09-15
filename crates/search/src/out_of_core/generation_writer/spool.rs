use crate::build_control::{checkpoint, CheckedWriter};
use crate::build_memory::{AdmittedDocument, BuildMemory, SPOOL_BUFFER_BYTES};
#[cfg(test)]
use crate::checksum_bytes;
use crate::document_encoding::DocumentEncoding;
use crate::error::{Result, SkeinError};
use crate::SearchDocument;
use skein_core::RuntimeTaskContext;
use skein_integrity::Crc32cHasher;
use std::fs::{self, File};
use std::io::{self, BufReader, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) const SPOOL_HEADER: &[u8; 8] = b"SKNSPOL1";
pub(super) const SPOOL_FRAME_HEADER_BYTES: u64 = 16;
static GENERATION_WRITER_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct FrameDigests {
    record: Crc32cHasher,
    documents: Crc32cHasher,
}

impl Write for FrameDigests {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.record.update(bytes);
        self.documents.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Append an already admitted frame without retaining its encoded payload.
#[cfg(test)]
pub(super) fn write_frame(
    output: &mut impl Write,
    encoding: &DocumentEncoding<'_>,
    documents_digest: &mut Crc32cHasher,
) -> Result<()> {
    write_frame_inner(output, encoding, documents_digest, None)
}

pub(super) fn write_frame_with_context(
    output: &mut impl Write,
    encoding: &DocumentEncoding<'_>,
    documents_digest: &mut Crc32cHasher,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<()> {
    checkpoint(task)?;
    let _scratch_memory = memory
        .spool
        .reserve(crate::document_encoding::HEX_BUFFER_BYTES)?;
    write_frame_inner(output, encoding, documents_digest, Some(task))
}

fn write_frame_inner(
    output: &mut impl Write,
    encoding: &DocumentEncoding<'_>,
    documents_digest: &mut Crc32cHasher,
    task: Option<&RuntimeTaskContext>,
) -> Result<()> {
    // The existing format places the checksum before the payload. A bounded
    // prepass keeps writes sequential without per-document seek/flush. The
    // borrowed source cannot change between the two encoding passes.
    let mut digests = FrameDigests {
        record: Crc32cHasher::new(),
        documents: *documents_digest,
    };
    encoding.write_to(&mut CheckedWriter::new(&mut digests, task))?;
    let mut output = CheckedWriter::new(output, task);
    output.write_all(&(encoding.len() as u64).to_le_bytes())?;
    output.write_all(&digests.record.finish().to_le_bytes())?;
    encoding.write_to(&mut output)?;
    // Failed or partial writes must not commit a new logical stream identity.
    *documents_digest = digests.documents;
    Ok(())
}

#[cfg(test)]
mod write_tests;

mod decoding;

pub(super) struct SpoolSource<'a> {
    pub(super) path: &'a Path,
    pub(super) document_count: usize,
    pub(super) max_record_bytes: u64,
    pub(super) max_metadata_fields: usize,
    pub(super) memory: BuildMemory,
}

impl SpoolSource<'_> {
    #[cfg(test)]
    pub(super) fn scan(
        &self,
        consumer: &mut dyn FnMut(SearchDocument) -> Result<()>,
    ) -> Result<()> {
        self.scan_with_context(&RuntimeTaskContext::default(), consumer)
    }

    #[cfg(test)]
    pub(super) fn scan_with_context(
        &self,
        task_context: &RuntimeTaskContext,
        consumer: &mut dyn FnMut(SearchDocument) -> Result<()>,
    ) -> Result<()> {
        self.scan_admitted(task_context, &mut |document| {
            let (document, _lease) = document.into_parts();
            consumer(document)
        })
    }

    pub(super) fn scan_admitted(
        &self,
        task_context: &RuntimeTaskContext,
        consumer: &mut dyn FnMut(AdmittedDocument) -> Result<()>,
    ) -> Result<()> {
        checkpoint(task_context)?;
        let _buffer_memory = self.memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let file = File::open(self.path)?;
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
            let expected_checksum = u64::from_le_bytes(raw_checksum);
            let document = decoding::read_frame_admitted(
                &mut reader,
                length,
                expected_checksum,
                ordinal,
                &self.memory,
                self.max_metadata_fields,
                task_context,
            )?;
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
            consumer(document)?;
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
    pub(super) path: super::context_memory::OwnedPath,
}

impl StageDirectory {
    pub(super) fn create(
        root: &Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        for _ in 0..64 {
            checkpoint(task)?;
            let _name_memory = memory.retained.reserve(3 * 128)?;
            let sequence = GENERATION_WRITER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let name = format!(
                ".search-generation.{}.{}.stage",
                std::process::id(),
                sequence
            );
            if name.capacity() > 128 {
                return Err(SkeinError::Execution(
                    "search stage name exceeds preflight capacity".into(),
                ));
            }
            let path =
                super::context_memory::OwnedPath::join(root, Path::new(&name), memory, task)?;
            match fs::create_dir(&path) {
                Ok(()) => {
                    let stage = Self { path };
                    checkpoint(task)?;
                    return Ok(stage);
                }
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
        static MAX_REQUEST: Cell<usize> = const { Cell::new(0) };
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

    pub(in super::super) fn take_max_request() -> usize {
        MAX_REQUEST.replace(0)
    }

    impl Read for TrackedFile {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            MAX_REQUEST.set(MAX_REQUEST.get().max(output.len()));
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
