// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A query spill borrows its actual scratch namespace and original I/O wave.

use super::*;

pub(super) struct SpillFile<'a> {
    file: Option<File>,
    legacy_path: Option<PathBuf>,
    context: Option<QueryContext<'a>>,
    length: u64,
    // The borrow prevents the cleanup owner from disappearing while a file is
    // open. The enclosing query drops its I/O wave before that owner's cleanup.
    _scratch: Option<&'a ScratchDirectory>,
    _memory: Option<QueryMemoryLease>,
}

impl<'a> SpillFile<'a> {
    pub(super) fn create(
        config: &SearchOutOfCoreConfig,
        context: Option<QueryContext<'a>>,
        scratch: Option<&'a ScratchDirectory>,
        native: bool,
    ) -> Result<Self> {
        // Native queries spill only into their captured scratch. Ordinary
        // reads, admitted or not, use the reader's configured spill directory;
        // an admitted one still charges the open file to its working account.
        let captured = context.filter(|_| native || scratch.is_some());
        let (file, legacy_path, memory) = if let Some(context) = captured {
            if context.execution.is_none() {
                return Err(HawDBError::Execution(
                    "candidate spill requires current execution".into(),
                ));
            }
            let scope = context.io_scope.ok_or_else(|| {
                HawDBError::Execution("candidate spill requires original query I/O".into())
            })?;
            let directory = scratch.ok_or_else(|| {
                HawDBError::Execution("candidate spill requires original captured scratch".into())
            })?;
            const NAME: &str = "candidates";
            let memory = context.memory.reserve(add(
                size_of::<Self>(),
                directory.native().metadata_bound(NAME.len())?,
            )?)?;
            let file = scope.with_io(context.task, || {
                directory
                    .native()
                    .create_child_new(&directory.native().operation_context(), NAME)
            })?;
            (file, None, Some(memory))
        } else {
            if scratch.is_some() {
                return Err(HawDBError::Execution(
                    "captured candidate scratch requires query admission".into(),
                ));
            }
            let memory = context
                .map(|context| context.memory.reserve(size_of::<Self>()))
                .transpose()?;
            let path = unique_candidate_path(&config.spill_directory);
            let open = || {
                fs::create_dir_all(&config.spill_directory)?;
                OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .open(&path)
            };
            let mut guard = CandidateFileGuard::new(path.clone());
            let file = match context.and_then(|context| Some((context, context.io_scope?))) {
                Some((context, scope)) => scope.with_io(context.task, open)?,
                None => open()?,
            };
            guard.disarm();
            (file, Some(path), memory)
        };
        Ok(Self {
            file: Some(file),
            legacy_path,
            context,
            length: 0,
            _scratch: scratch,
            _memory: memory,
        })
    }

    pub(super) fn context(&self) -> Option<QueryContext<'a>> {
        self.context
    }

    pub(super) fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        let length = self
            .length
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| HawDBError::Execution("candidate spill length overflow".into()))?;
        let file = self.file.as_mut().expect("live candidate spill");
        if let Some(scope) = self.context.and_then(|context| context.io_scope) {
            scope.with_io(self.context.expect("scoped spill").task, || {
                file.write_all(bytes)
            })?;
        } else {
            file.write_all(bytes)?;
        }
        self.length = length;
        Ok(())
    }

    pub(super) fn sync_all(&self) -> Result<()> {
        let file = self.file.as_ref().expect("live candidate spill");
        if let Some(scope) = self.context.and_then(|context| context.io_scope) {
            scope.with_io(self.context.expect("scoped spill").task, || file.sync_all())
        } else {
            Ok(file.sync_all()?)
        }
    }

    pub(super) fn read_exact_at(&self, offset: u64, bytes: &mut [u8]) -> Result<()> {
        let file = self.file.as_ref().expect("live candidate spill");
        if let Some(context) = self.context {
            let mut range = crate::build_memory::read::QueryFileRange {
                file,
                expected_length: Some(self.length),
                offset,
                remaining: bytes.len() as u64,
                context,
                require_execution: true,
            };
            range.read_exact(bytes).map_err(io_error)
        } else {
            read_search_range(file, offset, bytes)
        }
    }
}

impl Drop for SpillFile<'_> {
    fn drop(&mut self) {
        self.file.take();
        if let Some(path) = &self.legacy_path {
            let _ = fs::remove_file(path);
        }
    }
}
