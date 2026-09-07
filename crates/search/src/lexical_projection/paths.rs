//! Private lexical build paths and run registries on the shared build root.

use super::{artifact_file, ManifestBody, MANIFEST_FILE};
use crate::build_control::checkpoint;
use crate::build_memory::path::OwnedPath;
use crate::build_memory::{checked_mul, BuildMemory};
use crate::{Result, SkeinError};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use std::mem::size_of;
use std::ops::Deref;
use std::path::Path;

const FORMAT: &str = "SKEIN_LEXICAL_MANIFEST_V1";
const LAYOUT: &str = "SKEIN_LEXICAL_COMPACT_V1";
// Both fixed filename formats, including two full-width integers, fit 128
// bytes. Cover the formatter's old and replacement allocations as well.
const NAME_PEAK: usize = 3 * 128;

pub(super) struct Names {
    format: String,
    layout: String,
    artifact: String,
    memory: QueryMemoryLease,
}

impl Names {
    pub(super) fn artifact(&self) -> &str {
        &self.artifact
    }

    pub(super) fn new(
        generation: u64,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let lease = memory
            .retained
            .reserve(NAME_PEAK + FORMAT.len() + LAYOUT.len())?;
        #[cfg(test)]
        evidence::name();
        let mut names = Self {
            format: FORMAT.to_owned(),
            layout: LAYOUT.to_owned(),
            artifact: artifact_file(generation),
            memory: lease,
        };
        checkpoint(task)?;
        let bytes = names.format.capacity() + names.layout.capacity() + names.artifact.capacity();
        if bytes > names.memory.bytes() || names.artifact.capacity() > 128 {
            return Err(SkeinError::Execution(
                "lexical manifest names exceed preflight capacity".into(),
            ));
        }
        names.memory.shrink(names.memory.bytes() - bytes);
        Ok(names)
    }

    pub(super) fn into_manifest(
        self,
        body: impl FnOnce(String, String, String) -> ManifestBody,
    ) -> Manifest {
        Manifest {
            body: body(self.format, self.layout, self.artifact),
            _names: self.memory,
        }
    }
}

pub(super) struct Manifest {
    pub(super) body: ManifestBody,
    _names: QueryMemoryLease,
}

pub(super) struct Publication {
    pub(super) artifact: OwnedPath,
    pub(super) temporary: OwnedPath,
    pub(super) manifest: OwnedPath,
    pub(super) manifest_temporary: OwnedPath,
}

impl Publication {
    pub(super) fn new(
        root: &Path,
        name: &str,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        let artifact = OwnedPath::join(root, Path::new(name), memory, task)?;
        let temporary = OwnedPath::with_extension(&artifact, "skein.tmp", memory, task)?;
        let manifest = OwnedPath::join(root, Path::new(MANIFEST_FILE), memory, task)?;
        let manifest_temporary = OwnedPath::with_extension(&manifest, "skein.tmp", memory, task)?;
        Ok(Self {
            artifact,
            temporary,
            manifest,
            manifest_temporary,
        })
    }
}

pub(super) fn run(
    root: &Path,
    generation: u64,
    sequence: usize,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<OwnedPath> {
    checkpoint(task)?;
    let _name_memory = memory.retained.reserve(NAME_PEAK)?;
    #[cfg(test)]
    evidence::name();
    let name = format!(".search-lexical.{generation}.{sequence}.tmp");
    checkpoint(task)?;
    if name.capacity() > 128 {
        return Err(SkeinError::Execution(
            "lexical run name exceeds preflight capacity".into(),
        ));
    }
    OwnedPath::join(root, Path::new(&name), memory, task)
}

// This owner moves the registry, its capacity admission and cleanup responsibility
// together. Completed merge outputs remain owned even when removing inputs fails.
pub(super) struct Runs {
    paths: Vec<OwnedPath>,
    _slots: QueryMemoryLease,
}

impl Runs {
    pub(super) fn new(memory: &BuildMemory) -> Result<Self> {
        Ok(Self {
            paths: Vec::new(),
            _slots: memory.retained.reserve(0)?,
        })
    }

    pub(super) fn reserve_one(
        &mut self,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<()> {
        checkpoint(task)?;
        if self.paths.len() < self.paths.capacity() {
            return Ok(());
        }
        let capacity = checked_mul(self.paths.capacity().max(2), 2)?;
        let slots = memory
            .retained
            .reserve(checked_mul(capacity, size_of::<OwnedPath>())?)?;
        let mut next = Self {
            paths: Vec::new(),
            _slots: slots,
        };
        #[cfg(test)]
        evidence::slots();
        next.paths
            .try_reserve_exact(capacity)
            .map_err(|_| SkeinError::Execution("lexical run registry allocation failed".into()))?;
        if next.paths.capacity() != capacity {
            return Err(SkeinError::Execution(
                "lexical run registry exceeds preflight capacity".into(),
            ));
        }
        checkpoint(task)?;
        next.paths.append(&mut self.paths);
        *self = next;
        Ok(())
    }

    pub(super) fn push_reserved(&mut self, path: OwnedPath) {
        assert!(self.paths.len() < self.paths.capacity());
        self.paths.push(path);
    }

    #[cfg(test)]
    pub(super) fn retained_bytes(&self) -> usize {
        self._slots.bytes() + self.paths.iter().map(OwnedPath::capacity).sum::<usize>()
    }
}

impl Deref for Runs {
    type Target = [OwnedPath];
    fn deref(&self) -> &Self::Target {
        &self.paths
    }
}

impl Drop for Runs {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(test)]
pub(super) mod evidence {
    use super::*;
    use std::cell::{Cell, RefCell};
    thread_local! {
        static NAMES: Cell<usize> = const { Cell::new(0) };
        static SLOTS: Cell<usize> = const { Cell::new(0) };
        static CANCEL: RefCell<Option<skein_core::RuntimeCancellationToken>> = const { RefCell::new(None) };
        static GATE: Cell<bool> = const { Cell::new(false) };
    }
    pub(super) fn name() {
        NAMES.set(NAMES.get() + 1);
        cancel();
    }
    pub(super) fn slots() {
        SLOTS.set(SLOTS.get() + 1);
        cancel();
    }
    fn cancel() {
        CANCEL.with_borrow_mut(|value| {
            if let Some(token) = value.take() {
                token.cancel();
            }
        });
    }
    pub(super) fn take() -> (usize, usize) {
        (NAMES.replace(0), SLOTS.replace(0))
    }
    pub(super) fn cancel_next(task: &RuntimeTaskContext) {
        CANCEL.with_borrow_mut(|value| *value = Some(task.cancellation().clone()));
    }
    pub(super) fn pressure_at_gate() {
        GATE.set(true);
    }
    pub(in crate::lexical_projection) fn after_gate(
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Option<QueryMemoryLease>> {
        if !GATE.replace(false) {
            return Ok(None);
        }
        task.cancellation().cancel();
        let used = memory.ledger.snapshot().used_bytes;
        let limit = task.memory_reservation().unwrap().memory_bytes() as usize;
        memory.input.reserve(limit - used).map(Some)
    }
}

#[cfg(test)]
pub(super) mod tests;
