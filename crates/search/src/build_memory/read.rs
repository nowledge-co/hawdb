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

//! Reader metadata retains capacity without retaining operation execution.

use super::{checked_add, BuildMemory, ACCOUNT_METADATA_BYTES};
use crate::{HawDBError, Result};
use hawdb_core::{RuntimeCancellationReason, RuntimeIoWaveError, RuntimeTaskContext};
use hawdb_executor::QueryMemoryLease;
use hawdb_qos::{RuntimePermit, RuntimeRetainedMemory};

/// Internal resource ownership, independent of source/publication authority.
#[doc(hidden)]
#[derive(Debug)]
pub struct ReaderMemory {
    // The query lease's account reference must die before the host reservation.
    _query: QueryMemoryLease,
    _host: ReadMemoryFunding,
}

impl hawdb_core::RuntimeMemoryPermit for ReaderMemory {
    fn bytes(&self) -> u64 {
        self._query.bytes() as u64
    }
}

impl ReaderMemory {
    pub(crate) fn shares_governor_with(&self, permit: &RuntimePermit) -> bool {
        matches!(&self._host, ReadMemoryFunding::Retained(host) if host.shares_governor_with(permit))
    }
}

enum ReadMemoryFunding {
    Retained(RuntimeRetainedMemory),
    Scoped {
        _working: Option<Box<dyn hawdb_core::RuntimeMemoryPermit>>,
    },
}
impl std::fmt::Debug for ReadMemoryFunding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Retained(_) => "retained governor capacity",
            Self::Scoped { .. } => "scoped operation capacity",
        })
    }
}
impl ReaderMemory {
    pub(crate) fn requires_execution(&self) -> bool {
        matches!(self._host, ReadMemoryFunding::Retained(_))
    }
}

/// Synchronous metadata may borrow an actual publication, but cannot mint a
/// persistent root/view or select another governor. Its accounts stay original.
#[derive(Clone, Copy)]
pub(crate) enum ReadPermit<'a> {
    Governed(&'a RuntimePermit),
    Synchronous(&'a crate::out_of_core::SearchProjectionPublishLease),
}

/// Borrow execution only while constructing a reader. Returned memory owners
/// carry neither this context, BuildMemory nor the active RuntimePermit.
pub(crate) struct ReadAdmission<'a> {
    pub(crate) memory: &'a BuildMemory,
    pub(crate) task: &'a RuntimeTaskContext,
    pub(crate) permit: ReadPermit<'a>,
}

mod query;
mod unbound;
pub(crate) use query::{io_error, QueryFileRange, QueryIoScope};

/// A bounded positioned range inside an already admitted, held native scope.
pub(crate) struct ScopedRange<'a, 'scope> {
    pub(crate) reader: &'a mut hawdb_storage::file_io::AdmittedFileRead<'scope>,
    pub(crate) offset: u64,
    pub(crate) remaining: u64,
}

impl std::io::Read for ScopedRange<'_, '_> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let count = self.remaining.min(output.len() as u64).min(8192) as usize;
        if count == 0 {
            return Ok(0);
        }
        let read = self.reader.read_at(&mut output[..count], self.offset)?;
        self.offset = self
            .offset
            .checked_add(read as u64)
            .ok_or_else(|| std::io::Error::other("admitted range offset overflow"))?;
        self.remaining -= read as u64;
        Ok(read)
    }
}

pub(crate) struct FileRange {
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) checksum: u64,
}

impl ReadAdmission<'_> {
    pub(crate) fn validate_mode(&self) -> Result<()> {
        if self.is_synchronous()
            && (self.task.has_resource_controllers()
                || self.memory.host_admission.is_some()
                || self.task.memory_reservation().is_none())
        {
            return Err(HawDBError::Execution(
                "governed cleanup requires its originating runtime permit".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn retained(&self, bytes: usize) -> Result<ReaderMemory> {
        self.validate_mode()?;
        let query = self.memory.retained.reserve(bytes)?;
        let host_bytes = checked_add(bytes, ACCOUNT_METADATA_BYTES)?;
        let host_bytes = u64::try_from(host_bytes)
            .map_err(|_| HawDBError::Execution("reader capacity exceeds u64".into()))?;
        let host = match self.permit {
            ReadPermit::Governed(permit) => ReadMemoryFunding::Retained(
                permit
                    .reserve_retained_memory(host_bytes)
                    .map_err(|error| HawDBError::Execution(error.to_string()))?,
            ),
            ReadPermit::Synchronous(_) => ReadMemoryFunding::Scoped {
                _working: self
                    .task
                    .reserve_working_memory(host_bytes)
                    .map_err(|error| HawDBError::Execution(error.to_string()))?,
            },
        };
        Ok(ReaderMemory {
            _query: query,
            _host: host,
        })
    }

    pub(crate) fn scratch(&self, bytes: usize) -> Result<QueryMemoryLease> {
        self.validate_mode()?;
        self.memory.spool.reserve(bytes)
    }

    pub(crate) fn io(
        &self,
    ) -> std::result::Result<Option<Box<dyn hawdb_core::RuntimeIoWavePermit>>, RuntimeIoWaveError>
    {
        if self.validate_mode().is_err() {
            return Err(RuntimeIoWaveError::ReservationExceeded {
                requested_slots: 1,
                reserved_slots: 0,
            });
        }
        let permit = self.task.acquire_io_wave(std::num::NonZeroUsize::MIN)?;
        if permit.is_none() && self.runtime_permit().is_some() {
            return Err(RuntimeIoWaveError::ReservationExceeded {
                requested_slots: 1,
                reserved_slots: 0,
            });
        }
        Ok(permit)
    }

    pub(crate) fn runtime_permit(&self) -> Option<&RuntimePermit> {
        match self.permit {
            ReadPermit::Governed(permit) => Some(permit),
            ReadPermit::Synchronous(_) => None,
        }
    }
    pub(crate) fn require_governor(&self) -> Result<&RuntimePermit> {
        self.runtime_permit().ok_or_else(|| {
            HawDBError::Execution(
                "synchronous cleanup cannot construct persistent root custody".into(),
            )
        })
    }
    pub(crate) fn is_synchronous(&self) -> bool {
        self.runtime_permit().is_none()
    }
    pub(crate) fn matches_publication(
        &self,
        publication: &crate::out_of_core::SearchProjectionPublishLease,
    ) -> bool {
        match self.permit {
            ReadPermit::Governed(_) => true,
            ReadPermit::Synchronous(actual) => std::ptr::eq(actual, publication),
        }
    }

    pub(crate) fn checkpoint(&self) -> std::result::Result<(), RuntimeCancellationReason> {
        self.task.checkpoint()
    }
}

#[cfg(test)]
thread_local! {
    static AFTER_FILE_OPEN: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

/// Encoded bytes die before the reservation that admitted their allocation.
pub(crate) struct ReadBytes<'execution> {
    // Scratch payload cannot escape the construction permit it borrows.
    _execution: std::marker::PhantomData<&'execution RuntimePermit>,
    value: Vec<u8>,
    _memory: Option<QueryMemoryLease>,
}

impl ReadBytes<'_> {
    pub(crate) fn unaccounted(value: Vec<u8>) -> Self {
        Self {
            value,
            _memory: None,
            _execution: std::marker::PhantomData,
        }
    }
}

impl std::ops::Deref for ReadBytes<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.value
    }
}

impl<'execution> ReadAdmission<'execution> {
    /// Charge the selected buffer before allocation, but validate a cold full
    /// object under the same I/O scope before consuming any selected bytes.
    pub(crate) fn read_file_range(
        &self,
        file: &hawdb_storage::file_io::File,
        full_length: u64,
        maximum: u64,
        range: FileRange,
    ) -> Result<ReadBytes<'execution>> {
        use std::io::Read;
        if range
            .offset
            .checked_add(range.length)
            .is_none_or(|end| end > full_length)
        {
            return Err(HawDBError::Storage(
                "reader range exceeds its artifact".into(),
            ));
        }
        let length = usize::try_from(range.length)
            .map_err(|_| HawDBError::Execution("reader range exceeds address space".into()))?;
        let memory = self.scratch(length)?;
        let mut output = ReadBytes {
            _execution: std::marker::PhantomData,
            value: Vec::new(),
            _memory: Some(memory),
        };
        self.with_read_scope(file, full_length, maximum, |reader| {
            output.value.try_reserve_exact(length).map_err(|error| {
                HawDBError::Execution(format!("reader range allocation failed: {error}"))
            })?;
            if output.value.capacity() > length {
                return Err(HawDBError::Execution(
                    "reader range exceeded admitted capacity".into(),
                ));
            }
            output.value.resize(length, 0);
            let mut input = ScopedRange {
                reader,
                offset: range.offset,
                remaining: range.length,
            };
            input.read_exact(&mut output.value)?;
            if crate::build_control::write_checksummed(
                &mut std::io::sink(),
                &output.value,
                Some(self.task),
            )? != range.checksum
            {
                return Err(HawDBError::Storage("reader range checksum mismatch".into()));
            }
            Ok(())
        })?;
        Ok(output)
    }

    pub(crate) fn require_read_limit(&self, bytes: u64, maximum: u64) -> Result<()> {
        crate::build_control::checkpoint(self.task)?;
        if bytes > maximum {
            return Err(HawDBError::Execution(
                "reader artifact exceeds its operation read limit".into(),
            ));
        }
        Ok(())
    }

    /// Admit encoded storage before opening, but allocate its buffer only after
    /// the held file and operation have passed the admitted read boundary.
    pub(crate) fn read_bound(
        &self,
        path: &std::path::Path,
        expected_len: u64,
        expected_checksum: u64,
        maximum: u64,
    ) -> Result<ReadBytes<'execution>> {
        crate::build_control::checkpoint(self.task)?;
        if expected_len > maximum {
            return Err(HawDBError::Storage(
                "reader artifact exceeds its encoded limit".into(),
            ));
        }
        let length = usize::try_from(expected_len).map_err(|_| {
            HawDBError::Storage("reader artifact length exceeds address space".into())
        })?;
        let memory = self.scratch(length)?;
        let mut output = ReadBytes {
            value: Vec::new(),
            _memory: Some(memory),
            _execution: std::marker::PhantomData,
        };
        let file = self.visit_bound(path, expected_len, expected_checksum, maximum, |chunk| {
            if output.value.capacity() == 0 {
                output.value.try_reserve_exact(length).map_err(|error| {
                    HawDBError::Execution(format!("reader artifact allocation failed: {error}"))
                })?;
                if output.value.capacity() > length {
                    return Err(HawDBError::Execution(
                        "reader artifact allocation exceeds admission".into(),
                    ));
                }
            }
            output.value.extend_from_slice(chunk);
            Ok(())
        })?;
        drop(file);
        Ok(output)
    }

    /// Stream a complete selected artifact through one admitted native lease.
    /// Visitor state must remain private until this function succeeds.
    pub(crate) fn visit_bound(
        &self,
        path: &std::path::Path,
        expected_len: u64,
        expected_checksum: u64,
        maximum: u64,
        mut visitor: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<hawdb_storage::file_io::File> {
        let _buffer = self.scratch(super::SPOOL_BUFFER_BYTES)?;
        let (file, ()) = self.with_bound_file(path, expected_len, maximum, |reader| {
            self.visit_reader_bound(reader, expected_len, expected_checksum, &mut visitor)
        })?;
        Ok(file)
    }

    pub(crate) fn visit_file_bound(
        &self,
        file: hawdb_storage::file_io::File,
        expected_len: u64,
        expected_checksum: u64,
        maximum: u64,
        mut visitor: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<hawdb_storage::file_io::File> {
        self.require_read_limit(expected_len, maximum)?;
        let _buffer = self.scratch(super::SPOOL_BUFFER_BYTES)?;
        self.with_read_scope(&file, expected_len, maximum, |reader| {
            self.visit_reader_bound(reader, expected_len, expected_checksum, &mut visitor)
        })?;
        Ok(file)
    }

    fn visit_reader_bound(
        &self,
        reader: &mut impl std::io::Read,
        expected_len: u64,
        expected_checksum: u64,
        mut visitor: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        let mut buffer = [0; super::SPOOL_BUFFER_BYTES];
        let mut checksum = hawdb_integrity::Crc32cHasher::new();
        let mut length = 0u64;
        loop {
            crate::build_control::checkpoint(self.task)?;
            let read = match reader.read(&mut buffer) {
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                result => result?,
            };
            crate::build_control::checkpoint(self.task)?;
            if read == 0 {
                break;
            }
            if read as u64 > expected_len - length {
                return Err(HawDBError::Storage(
                    "reader artifact grew during read".into(),
                ));
            }
            length += read as u64;
            checksum.update(&buffer[..read]);
            visitor(&buffer[..read])?;
        }
        if length != expected_len || checksum.finish() != expected_checksum {
            return Err(HawDBError::Storage(
                "reader artifact length or checksum mismatch".into(),
            ));
        }
        Ok(())
    }

    /// Bind length and managed cold identity; block payload checksums remain
    /// the serving reader's responsibility. Native files are not fully read.
    pub(crate) fn open_bound_length(
        &self,
        path: &std::path::Path,
        expected_len: u64,
        maximum: u64,
    ) -> Result<hawdb_storage::file_io::File> {
        self.with_bound_file(path, expected_len, maximum, |_| Ok(()))
            .map(|(file, ())| file)
    }

    /// Pin an already selected artifact while a downstream parser performs
    /// multiple file/mmap operations. Cold revalidation is admitted as well.
    #[cfg(feature = "vector-search")]
    pub(crate) fn pin_bound_file(
        &self,
        file: hawdb_storage::file_io::File,
        expected_len: u64,
        maximum: u64,
    ) -> Result<hawdb_storage::file_io::File> {
        self.require_read_limit(expected_len, maximum)?;
        let _io = self
            .io()
            .map_err(|error| HawDBError::Execution(error.to_string()))?;
        let mut control = FileReadControl {
            admission: self,
            expected_len,
            validation_memory: None,
            error: None,
        };
        let result = file.pin_admitted(&mut control);
        crate::build_control::checkpoint(self.task)?;
        if let Some(error) = control.error.take() {
            return Err(error);
        }
        let file = result?;
        if file.metadata()?.len() != expected_len {
            return Err(HawDBError::Storage(
                "reader pinned artifact length mismatch".into(),
            ));
        }
        Ok(file)
    }

    fn with_bound_file<T>(
        &self,
        path: &std::path::Path,
        expected_len: u64,
        maximum: u64,
        operation: impl FnOnce(&mut hawdb_storage::file_io::AdmittedFileRead<'_>) -> Result<T>,
    ) -> Result<(hawdb_storage::file_io::File, T)> {
        use super::reserved::native_path;
        use hawdb_storage::file_io::File;
        self.require_read_limit(expected_len, maximum)?;
        let _native = self.scratch(checked_add(native_path::bytes(path)?, 4096)?)?;
        let file = {
            let _io = self
                .io()
                .map_err(|e| HawDBError::Execution(e.to_string()))?;
            File::open(path)?
        };
        #[cfg(test)]
        if let Some(callback) = AFTER_FILE_OPEN.with(|slot| slot.replace(None)) {
            callback();
        }
        crate::build_control::checkpoint(self.task)?;
        let value = self.with_read_scope(&file, expected_len, maximum, operation)?;
        Ok((file, value))
    }

    pub(crate) fn with_read_scope<T>(
        &self,
        file: &hawdb_storage::file_io::File,
        expected_len: u64,
        maximum: u64,
        operation: impl FnOnce(&mut hawdb_storage::file_io::AdmittedFileRead<'_>) -> Result<T>,
    ) -> Result<T> {
        self.require_read_limit(expected_len, maximum)?;
        let _io = self
            .io()
            .map_err(|error| HawDBError::Execution(error.to_string()))?;
        let mut control = FileReadControl {
            admission: self,
            expected_len,
            validation_memory: None,
            error: None,
        };
        let result = file.with_admitted_read(&mut control, |reader| {
            Ok((|| -> Result<T> {
                if reader.metadata()?.len() != expected_len {
                    return Err(HawDBError::Storage(
                        "reader artifact length mismatch".into(),
                    ));
                }
                let value = operation(reader)?;
                if reader.metadata()?.len() != expected_len {
                    return Err(HawDBError::Storage(
                        "reader artifact length changed during read".into(),
                    ));
                }
                Ok(value)
            })())
        });
        crate::build_control::checkpoint(self.task)?;
        if let Some(error) = control.error.take() {
            return Err(error);
        }
        result?.0
    }
}

struct FileReadControl<'a, 'execution> {
    admission: &'a ReadAdmission<'execution>,
    expected_len: u64,
    validation_memory: Option<QueryMemoryLease>,
    error: Option<HawDBError>,
}

impl hawdb_storage::file_io::ReadAdmissionControl for FileReadControl<'_, '_> {
    fn admit_validation(&mut self, bytes: u64) -> std::io::Result<()> {
        let result = (|| {
            if bytes != self.expected_len {
                return Err(HawDBError::Storage(
                    "reader immutable identity length mismatch".into(),
                ));
            }
            self.validation_memory = Some(self.admission.scratch(64 << 10)?);
            Ok(())
        })();
        result.map_err(|error| {
            // Keep typed budget failures instead of flattening them through
            // the managed file's generic I/O result.
            self.error = Some(error);
            std::io::Error::other("reader validation admission refused")
        })
    }

    fn checkpoint(&mut self) -> std::io::Result<()> {
        crate::build_control::checkpoint(self.admission.task).map_err(std::io::Error::other)
    }
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(feature = "vector-search")]
mod vector;
