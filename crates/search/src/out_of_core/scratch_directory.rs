// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Private scratch namespace; labels never select an I/O target.

use crate::build_memory::read::ReadAdmission;
use crate::build_memory::{checked_add, path::OwnedPath, BuildMemory, ACCOUNT_METADATA_BYTES};
use crate::{HawDBError, Result};
use hawdb_core::{RuntimeIoWavePermit, RuntimeTaskContext};
use hawdb_executor::QueryMemoryLease;
use hawdb_qos::RuntimeRetainedMemory;
use hawdb_storage::file_io::{File, SourceDirectory};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(super) struct Funding {
    query: QueryMemoryLease,
    host: Option<RuntimeRetainedMemory>,
}
impl Funding {
    pub(super) fn new(
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
        admission: Option<&ReadAdmission<'_>>,
        bytes: usize,
        other_owned_bytes: usize,
    ) -> Result<Self> {
        if let Some(admission) = admission {
            admission.validate_mode()?;
        } else if task.has_resource_controllers() || memory.host_admission.is_some() {
            return Err(HawDBError::Execution(
                "captured scratch requires originating admission".into(),
            ));
        }
        let query = memory.retained.reserve(bytes)?;
        let host_bytes = checked_add(
            checked_add(bytes, other_owned_bytes)?,
            ACCOUNT_METADATA_BYTES,
        )?;
        let host = admission
            .and_then(ReadAdmission::runtime_permit)
            .map(|permit| {
                permit
                    .reserve_retained_memory(host_bytes as u64)
                    .map_err(|error| HawDBError::Execution(error.to_string()))
            })
            .transpose()?;
        Ok(Self { query, host })
    }
    pub(super) fn bytes(&self) -> usize {
        self.query.bytes()
    }
    pub(super) fn governed(&self) -> bool {
        self.host.is_some()
    }
}

pub(crate) struct ScratchDirectory {
    directory: SourceDirectory,
    label: OwnedPath,
    // Native custody and all owned label bytes close before their funding.
    _funding: Funding,
}
impl ScratchDirectory {
    pub(super) fn new(directory: SourceDirectory, label: OwnedPath, funding: Funding) -> Self {
        Self {
            directory,
            label,
            _funding: funding,
        }
    }
    pub(crate) fn governed(&self) -> bool {
        self._funding.governed()
    }
    pub(super) fn native(&self) -> &SourceDirectory {
        &self.directory
    }
    fn name<'a>(&self, path: &'a Path) -> Result<&'a str> {
        if path.parent() != Some(&*self.label) {
            return Err(HawDBError::Execution(
                "scratch path is outside its captured label".into(),
            ));
        }
        path.file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| HawDBError::Execution("invalid scratch component".into()))
    }
    pub(crate) fn create(&self, path: &Path) -> Result<File> {
        Ok(self
            .directory
            .create_child_new(&self.directory.operation_context(), self.name(path)?)?)
    }
    pub(crate) fn open(&self, path: &Path) -> Result<File> {
        Ok(self
            .directory
            .open_child(&self.directory.operation_context(), self.name(path)?)?)
    }
    pub(crate) fn remove(&self, path: &Path) -> Result<()> {
        Ok(self
            .directory
            .remove_child(&self.directory.operation_context(), self.name(path)?)?)
    }
}

pub(super) fn io_scope(
    task: &RuntimeTaskContext,
    governed: bool,
) -> Result<Option<Box<dyn RuntimeIoWavePermit>>> {
    let permit = task
        .acquire_io_wave(std::num::NonZeroUsize::MIN)
        .map_err(|error| HawDBError::Execution(error.to_string()))?;
    if governed && permit.is_none() {
        return Err(HawDBError::Execution(
            "governed scratch requires fresh I/O admission".into(),
        ));
    }
    Ok(permit)
}

pub(crate) enum SpillFile {
    Legacy(File),
    Captured {
        file: File,
        task: RuntimeTaskContext,
        governed: bool,
    },
}
impl SpillFile {
    pub(crate) fn captured(file: File, task: RuntimeTaskContext, governed: bool) -> Self {
        Self::Captured {
            file,
            task,
            governed,
        }
    }
    fn io(&self) -> io::Result<Option<Box<dyn RuntimeIoWavePermit>>> {
        match self {
            Self::Legacy(_) => Ok(None),
            Self::Captured { task, governed, .. } => {
                io_scope(task, *governed).map_err(io::Error::other)
            }
        }
    }
    fn file(&mut self) -> &mut File {
        match self {
            Self::Legacy(file) | Self::Captured { file, .. } => file,
        }
    }
    pub(crate) fn metadata(&self) -> io::Result<std::fs::Metadata> {
        let _io = self.io()?;
        match self {
            Self::Legacy(file) | Self::Captured { file, .. } => file.metadata(),
        }
    }
}
impl Read for SpillFile {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let _io = self.io()?;
        self.file().read(bytes)
    }
}
impl Write for SpillFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let _io = self.io()?;
        self.file().write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        let _io = self.io()?;
        self.file().flush()
    }
}
impl Seek for SpillFile {
    fn seek(&mut self, offset: SeekFrom) -> io::Result<u64> {
        let _io = self.io()?;
        self.file().seek(offset)
    }
}
