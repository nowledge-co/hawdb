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

//! Keep admitted spill memory and task control together across streams.

use super::{checkpoint, RuntimeTaskContext, SPILL_IO_BUFFER_BYTES};
use crate::build_memory::reserved::{native_path, Grant, ReservedMemory};
use crate::build_term::Term;
use crate::Result;
use std::io::{self, Write};
use std::path::Path;

#[cfg(test)]
thread_local! {
    static CAPTURED_IO: std::cell::Cell<[usize; 3]> = const { std::cell::Cell::new([0; 3]) };
}
#[cfg(test)]
pub(crate) fn take_captured_io() -> [usize; 3] {
    CAPTURED_IO.with(|counts| counts.replace([0; 3]))
}
#[cfg(test)]
fn record_captured_io(index: usize) {
    CAPTURED_IO.with(|counts| {
        let mut value = counts.get();
        value[index] += 1;
        counts.set(value);
    });
}

/// Shipped spill streams always retain both task control and admitted progress.
/// Codec and exact-reservation fixtures cannot construct an unchecked mode in
/// a non-test build.
#[derive(Clone)]
pub(super) enum Control {
    Admitted {
        progress: ReservedMemory,
        task: RuntimeTaskContext,
        scratch: Option<std::sync::Arc<crate::out_of_core::ScratchDirectory>>,
    },
    #[cfg(test)]
    Fixture {
        progress: Option<ReservedMemory>,
        task: Option<RuntimeTaskContext>,
    },
}

impl Control {
    pub(super) fn new(progress: ReservedMemory, task: RuntimeTaskContext) -> Self {
        Self::Admitted {
            progress,
            task,
            scratch: None,
        }
    }

    #[cfg(test)]
    pub(super) fn fixture(
        progress: Option<&ReservedMemory>,
        task: Option<&RuntimeTaskContext>,
    ) -> Self {
        Self::Fixture {
            progress: progress.cloned(),
            task: task.cloned(),
        }
    }

    pub(super) fn check(&self) -> Result<()> {
        match self {
            Self::Admitted { task, .. } => checkpoint(task),
            #[cfg(test)]
            Self::Fixture { task, .. } => task.as_ref().map_or(Ok(()), checkpoint),
        }
    }

    pub(super) fn task(&self) -> Option<&RuntimeTaskContext> {
        match self {
            Self::Admitted { task, .. } => Some(task),
            #[cfg(test)]
            Self::Fixture { task, .. } => task.as_ref(),
        }
    }

    pub(super) fn progress(&self) -> Option<&ReservedMemory> {
        match self {
            Self::Admitted { progress, .. } => Some(progress),
            #[cfg(test)]
            Self::Fixture { progress, .. } => progress.as_ref(),
        }
    }

    pub(super) fn progress_for_growth(&self) -> Option<&ReservedMemory> {
        match self {
            Self::Admitted { progress, .. } => Some(progress),
            // An exact reservation must not grow to hide an admission failure.
            #[cfg(test)]
            Self::Fixture { .. } => None,
        }
    }

    pub(super) fn reserve(&self, bytes: usize) -> Result<Option<Grant>> {
        self.progress()
            .map(|progress| progress.reserve(bytes))
            .transpose()
    }

    pub(super) fn with_path<T>(&self, path: &Path, work: impl FnOnce() -> Result<T>) -> Result<T> {
        native_path::with_scratch(self.progress(), path, work)
    }

    pub(super) fn build_term(
        &self,
        length: usize,
        build: impl FnOnce() -> Result<String>,
    ) -> Result<Term> {
        match self {
            Self::Admitted { progress, .. } => Term::build_reserved(length, progress, build),
            #[cfg(test)]
            Self::Fixture { progress, .. } => match progress {
                Some(progress) => Term::build_reserved(length, progress, build),
                None => build().map(Term::untracked),
            },
        }
    }

    pub(super) fn with_scratch(
        mut self,
        directory: std::sync::Arc<crate::out_of_core::ScratchDirectory>,
    ) -> Self {
        match &mut self {
            Self::Admitted { scratch, .. } => *scratch = Some(directory),
            #[cfg(test)]
            Self::Fixture { .. } => panic!("unchecked fixture cannot carry captured scratch"),
        }
        self
    }
    pub(super) fn captured(&self) -> bool {
        matches!(
            self,
            Self::Admitted {
                scratch: Some(_),
                ..
            }
        )
    }
    fn scratch(&self) -> Option<&std::sync::Arc<crate::out_of_core::ScratchDirectory>> {
        match self {
            Self::Admitted { scratch, .. } => scratch.as_ref(),
            #[cfg(test)]
            Self::Fixture { .. } => None,
        }
    }
    pub(super) fn open(&self, path: &Path) -> Result<crate::out_of_core::SpillFile> {
        match self.scratch() {
            Some(directory) => {
                let task = self.task().expect("captured spill task");
                let _io = task
                    .acquire_io_wave(std::num::NonZeroUsize::MIN)
                    .map_err(|error| crate::HawDBError::Execution(error.to_string()))?;
                if directory.governed() && _io.is_none() {
                    return Err(crate::HawDBError::Execution(
                        "captured spill open requires I/O admission".into(),
                    ));
                }
                Ok(crate::out_of_core::SpillFile::captured(
                    {
                        let file = directory.open(path)?;
                        #[cfg(test)]
                        record_captured_io(1);
                        file
                    },
                    task.clone(),
                    directory.governed(),
                ))
            }
            None => Ok(crate::out_of_core::SpillFile::Legacy(
                hawdb_storage::file_io::File::open(path)?,
            )),
        }
    }
    pub(super) fn create(&self, path: &Path) -> Result<crate::out_of_core::SpillFile> {
        match self.scratch() {
            Some(directory) => {
                let task = self.task().expect("captured spill task");
                let _io = task
                    .acquire_io_wave(std::num::NonZeroUsize::MIN)
                    .map_err(|error| crate::HawDBError::Execution(error.to_string()))?;
                if directory.governed() && _io.is_none() {
                    return Err(crate::HawDBError::Execution(
                        "captured spill create requires I/O admission".into(),
                    ));
                }
                Ok(crate::out_of_core::SpillFile::captured(
                    {
                        let file = directory.create(path)?;
                        #[cfg(test)]
                        record_captured_io(0);
                        file
                    },
                    task.clone(),
                    directory.governed(),
                ))
            }
            None => Ok(crate::out_of_core::SpillFile::Legacy(
                hawdb_storage::file_io::File::create(path)?,
            )),
        }
    }
    pub(super) fn remove(&self, path: &Path) -> Result<()> {
        match self.scratch() {
            Some(directory) => {
                let task = self.task().expect("captured spill task");
                let _io = task
                    .acquire_io_wave(std::num::NonZeroUsize::MIN)
                    .map_err(|error| crate::HawDBError::Execution(error.to_string()))?;
                if directory.governed() && _io.is_none() {
                    return Err(crate::HawDBError::Execution(
                        "captured spill removal requires I/O admission".into(),
                    ));
                }
                directory.remove(path)?;
                #[cfg(test)]
                record_captured_io(2);
                Ok(())
            }
            None => Ok(hawdb_storage::file_io::remove_file(path)?),
        }
    }

    pub(super) fn with_task(&self, task: RuntimeTaskContext) -> Self {
        match self {
            Self::Admitted {
                progress, scratch, ..
            } => Self::Admitted {
                progress: progress.clone(),
                task,
                scratch: scratch.clone(),
            },
            #[cfg(test)]
            Self::Fixture { progress, .. } => Self::Fixture {
                progress: progress.clone(),
                task: Some(task),
            },
        }
    }
}

pub(super) struct RecordWriter<'a, W> {
    writer: &'a mut W,
    control: &'a Control,
    remaining: usize,
}

impl<'a, W> RecordWriter<'a, W> {
    pub(super) fn new(writer: &'a mut W, control: &'a Control) -> io::Result<Self> {
        control.check().map_err(io::Error::other)?;
        Ok(Self {
            writer,
            control,
            remaining: SPILL_IO_BUFFER_BYTES,
        })
    }

    fn check(&mut self) -> io::Result<()> {
        self.control.check().map_err(io::Error::other)?;
        self.remaining = SPILL_IO_BUFFER_BYTES;
        Ok(())
    }
}

impl<W: Write> Write for RecordWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            self.check()?;
        }
        let result = self.writer.write(&bytes[..bytes.len().min(self.remaining)]);
        match result {
            Ok(written) => self.remaining -= written,
            // Repeated interrupted writes must still observe cancellation.
            Err(_) => self.remaining = 0,
        }
        result
    }

    fn flush(&mut self) -> io::Result<()> {
        self.check()?;
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests;
