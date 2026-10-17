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

//! Allocation and I/O admission for an already opened immutable artifact.

use super::*;

/// Internal bridge used by embedded reader construction. It grants no source,
/// binding, generation, or completeness authority. Implementations must reserve
/// before returning a guard and keep retained charges live until guard Drop.
#[doc(hidden)]
pub trait ProjectionReadAdmission {
    type Retained: std::fmt::Debug + Send + Sync;
    type Scratch;
    type Io;
    fn reserve_retained(&self, bytes: usize) -> Result<Self::Retained>;
    fn reserve_scratch(&self, bytes: usize) -> Result<Self::Scratch>;
    fn acquire_io(&self) -> Result<Self::Io>;
    fn checkpoint(&self) -> Result<()>;
}

pub(super) struct Unaccounted;
impl ProjectionReadAdmission for Unaccounted {
    type Retained = ();
    type Scratch = ();
    type Io = ();
    fn reserve_retained(&self, _: usize) -> Result<()> {
        Ok(())
    }
    fn reserve_scratch(&self, _: usize) -> Result<()> {
        Ok(())
    }
    fn acquire_io(&self) -> Result<()> {
        Ok(())
    }
    fn checkpoint(&self) -> Result<()> {
        Ok(())
    }
}

pub(super) fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or_else(overflow)
}
pub(super) fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or_else(overflow)
}
fn overflow() -> ProjectionError {
    ProjectionError::CorruptArtifact("projection metadata capacity exceeds address space".into())
}

pub(super) struct DecodePlan {
    pub(super) retained: usize,
    pub(super) scratch: usize,
}

impl DecodePlan {
    pub(super) fn inspect(bytes: &[u8], admission: &impl ProjectionReadAdmission) -> Result<Self> {
        let mut containers = 0;
        let mut quoted = false;
        let mut escaped = false;
        let mut token = 0;
        let mut largest = 8;
        for chunk in bytes.chunks(stream_verify::BUFFER_BYTES) {
            admission.checkpoint()?;
            for &byte in chunk {
                if quoted {
                    token += 1;
                    if escaped {
                        escaped = false;
                    } else if byte == b'\\' {
                        escaped = true;
                    } else if byte == b'"' {
                        quoted = false;
                        largest = largest.max(token);
                        token = 0;
                    }
                } else if byte == b'"' {
                    largest = largest.max(token);
                    token = 0;
                    quoted = true;
                } else if byte.is_ascii_digit() || matches!(byte, b'-' | b'+' | b'.' | b'e' | b'E')
                {
                    token += 1;
                } else {
                    largest = largest.max(token);
                    token = 0;
                    if matches!(byte, b'{' | b'[') {
                        containers += 1;
                    }
                }
            }
        }
        largest = largest.max(token);
        // The schema has one vector of struct records. Every completed record
        // opens a map/sequence, even for compact malformed data rejected later.
        // Final geometric capacity is at most two slots per record, plus the
        // four-slot minimum. The old vector overlaps during growth in scratch.
        let vector = mul(
            add(containers, 4)?,
            std::mem::size_of::<SegmentDescriptor>(),
        )?;
        Ok(Self {
            retained: add(bytes.len(), mul(vector, 2)?)?,
            scratch: add(add(vector, mul(largest, 8)?)?, 4096)?,
        })
    }
}

pub(super) fn read_exact(
    reader: &mut impl Read,
    bytes: &mut [u8],
    admission: &impl ProjectionReadAdmission,
) -> Result<()> {
    for chunk in bytes.chunks_mut(stream_verify::BUFFER_BYTES) {
        let mut offset = 0;
        while offset < chunk.len() {
            admission.checkpoint()?;
            let result = {
                let _io = admission.acquire_io()?;
                reader.read(&mut chunk[offset..])
            };
            match result {
                Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into()),
                Ok(read) => offset += read,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct Io(Arc<AtomicUsize>);
    impl Drop for Io {
        fn drop(&mut self) {
            assert_eq!(self.0.fetch_sub(1, Ordering::SeqCst), 1);
        }
    }
    struct Stop {
        checkpoints: AtomicUsize,
        io: Arc<AtomicUsize>,
    }
    impl ProjectionReadAdmission for Stop {
        type Retained = ();
        type Scratch = ();
        type Io = Io;
        fn reserve_retained(&self, _: usize) -> Result<()> {
            Ok(())
        }
        fn reserve_scratch(&self, _: usize) -> Result<()> {
            Ok(())
        }
        fn acquire_io(&self) -> Result<Io> {
            assert_eq!(self.io.fetch_add(1, Ordering::SeqCst), 0);
            Ok(Io(self.io.clone()))
        }
        fn checkpoint(&self) -> Result<()> {
            if self.checkpoints.fetch_add(1, Ordering::SeqCst) >= 3 {
                Err(ProjectionError::Cancelled(
                    hawdb_core::RuntimeCancellationReason::Cancelled,
                ))
            } else {
                Ok(())
            }
        }
    }
    struct Interrupted {
        io: Arc<AtomicUsize>,
        reads: usize,
    }
    impl Read for Interrupted {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            assert_eq!(
                self.io.load(Ordering::SeqCst),
                1,
                "admission must span the physical read"
            );
            self.reads += 1;
            if self.reads > 8 {
                return Err(std::io::Error::other(
                    "interrupted retry ignored cancellation",
                ));
            }
            Err(std::io::Error::from(std::io::ErrorKind::Interrupted))
        }
    }
    #[test]
    fn interrupted_reads_recheck_cancellation_and_release_io() {
        let admission = Stop {
            checkpoints: AtomicUsize::new(0),
            io: Arc::new(AtomicUsize::new(0)),
        };
        let mut reader = Interrupted {
            io: admission.io.clone(),
            reads: 0,
        };
        let mut bytes = [0_u8; 8];
        assert!(matches!(
            read_exact(&mut reader, &mut bytes, &admission),
            Err(ProjectionError::Cancelled(_))
        ));
        assert_eq!(reader.reads, 3);
        assert_eq!(admission.io.load(Ordering::SeqCst), 0);
        assert_eq!(bytes, [0; 8]);
    }
}
