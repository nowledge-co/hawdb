//! Keep admitted spill memory and task control together across streams.

use super::{checkpoint, RuntimeTaskContext, SPILL_IO_BUFFER_BYTES};
use crate::build_memory::reserved::{native_path, Grant, ReservedMemory};
use crate::build_term::Term;
use crate::Result;
use std::io::{self, Write};
use std::path::Path;

/// Shipped spill streams always retain both task control and admitted progress.
/// Codec and exact-reservation fixtures cannot construct an unchecked mode in
/// a non-test build.
#[derive(Clone)]
pub(super) enum Control {
    Admitted {
        progress: ReservedMemory,
        task: RuntimeTaskContext,
    },
    #[cfg(test)]
    Fixture {
        progress: Option<ReservedMemory>,
        task: Option<RuntimeTaskContext>,
    },
}

impl Control {
    pub(super) fn new(progress: ReservedMemory, task: RuntimeTaskContext) -> Self {
        Self::Admitted { progress, task }
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

    pub(super) fn with_task(&self, task: RuntimeTaskContext) -> Self {
        match self {
            Self::Admitted { progress, .. } => Self::new(progress.clone(), task),
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
