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

//! Synchronous writer phases borrow one actual wave, never an idle reservation.

use super::*;
use hawdb_core::{RuntimeIoWaveController, RuntimeIoWaveError, RuntimeIoWavePermit};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

#[derive(Debug)]
struct State {
    active: bool,
    borrowed: bool,
    permit: Option<Box<dyn RuntimeIoWavePermit>>,
}
#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    // Controllers can survive their phase in scratch readers. Account their
    // metadata until the final clone drops, but release actual capacity early.
    _memory: QueryMemoryLease,
}
#[derive(Debug)]
struct Controller {
    shared: Arc<Shared>,
    original: RuntimeTaskContext,
}
#[derive(Debug)]
struct Borrowed(Arc<Shared>);
impl Drop for Borrowed {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.borrowed = false;
        if !state.active {
            state.permit.take();
        }
        self.0.changed.notify_all();
    }
}
impl RuntimeIoWaveController for Controller {
    fn try_acquire(
        &self,
        slots: NonZeroUsize,
        context: &RuntimeTaskContext,
    ) -> std::result::Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
        context.checkpoint().map_err(RuntimeIoWaveError::Stopped)?;
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !state.active {
            drop(state);
            return match self.original.try_acquire_io_wave(slots)? {
                hawdb_core::RuntimeIoWaveTryAcquire::Acquired(permit) => Ok(permit),
                hawdb_core::RuntimeIoWaveTryAcquire::Pending => Ok(None),
            };
        }
        if slots.get() != 1 {
            return Err(RuntimeIoWaveError::ReservationExceeded {
                requested_slots: slots.get(),
                reserved_slots: 1,
            });
        }
        if state.borrowed {
            return Ok(None);
        }
        state.borrowed = true;
        Ok(Some(Box::new(Borrowed(self.shared.clone()))))
    }
    fn acquire(
        &self,
        slots: NonZeroUsize,
        context: &RuntimeTaskContext,
    ) -> std::result::Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
        loop {
            if let Some(permit) = self.try_acquire(slots, context)? {
                return Ok(permit);
            }
            let state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if !state.active {
                drop(state);
                return self
                    .original
                    .acquire_io_wave(slots)
                    .map(|permit| permit.expect("phase had an original controller"));
            }
            let _wait = self
                .shared
                .changed
                .wait_timeout(state, Duration::from_millis(5))
                .unwrap_or_else(|error| error.into_inner());
        }
    }
}

pub(super) struct Scope {
    shared: Option<Arc<Shared>>,
    task: RuntimeTaskContext,
    cleanup: Option<RuntimeTaskContext>,
}
impl Scope {
    pub(super) fn acquire(task: &RuntimeTaskContext, memory: &BuildMemory) -> Result<Self> {
        let permit = task
            .acquire_io_wave(NonZeroUsize::MIN)
            .map_err(HawDBError::from_execution_error)?;
        let Some(permit) = permit else {
            if memory.host_admission.is_some() {
                return Err(HawDBError::Execution(
                    "governed writer requires I/O admission".into(),
                ));
            }
            return Ok(Self {
                shared: None,
                task: task.clone(),
                cleanup: None,
            });
        };
        // Cover both shared allocations, one boxed loan and Arc headers.
        let metadata = memory.retained.reserve(
            std::mem::size_of::<Shared>()
                + 2 * std::mem::size_of::<Controller>()
                + std::mem::size_of::<Borrowed>()
                + 256,
        )?;
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                active: true,
                borrowed: false,
                permit: Some(permit),
            }),
            changed: Condvar::new(),
            _memory: metadata,
        });
        let scoped = task.clone().with_io_wave_controller(Arc::new(Controller {
            shared: shared.clone(),
            original: task.clone(),
        }));
        let cleanup_original = cleanup_context(memory, task);
        let cleanup = cleanup_original
            .clone()
            .with_io_wave_controller(Arc::new(Controller {
                shared: shared.clone(),
                original: cleanup_original,
            }));
        Ok(Self {
            shared: Some(shared),
            task: scoped,
            cleanup: Some(cleanup),
        })
    }
    pub(super) fn task(&self) -> RuntimeTaskContext {
        self.task.clone()
    }
    pub(super) fn cleanup_task(&self) -> Option<&RuntimeTaskContext> {
        self.cleanup.as_ref()
    }
    pub(super) fn install(&self, writer: &mut SearchOutOfCoreGenerationWriter) {
        writer.task_context = self.task();
        self.install_stage(&mut writer.stage);
    }
    pub(super) fn install_stage(&self, stage: &mut StageDirectory) {
        if let Some(cleanup) = &self.cleanup {
            stage.set_io_task(cleanup);
        }
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        if let Some(shared) = &self.shared {
            let mut state = shared
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.active = false;
            if !state.borrowed {
                state.permit.take();
            }
            shared.changed.notify_all();
        }
    }
}

pub(super) struct WriterPhase<'a> {
    writer: &'a mut SearchOutOfCoreGenerationWriter,
    original: RuntimeTaskContext,
    _scope: Scope,
}
impl SearchOutOfCoreGenerationWriter {
    pub(super) fn io_phase(&mut self) -> Result<WriterPhase<'_>> {
        let scope = Scope::acquire(&self.task_context, &self.memory)?;
        let original = self.task_context.clone();
        scope.install(self);
        Ok(WriterPhase {
            writer: self,
            original,
            _scope: scope,
        })
    }
    pub(super) fn set_io_task(&mut self, task: RuntimeTaskContext) {
        self.stage
            .set_io_task(&cleanup_context(&self.memory, &task));
        self.task_context = task;
    }
}
impl std::ops::Deref for WriterPhase<'_> {
    type Target = SearchOutOfCoreGenerationWriter;
    fn deref(&self) -> &Self::Target {
        self.writer
    }
}
impl std::ops::DerefMut for WriterPhase<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.writer
    }
}
impl Drop for WriterPhase<'_> {
    fn drop(&mut self) {
        if self._scope.shared.is_some() {
            self.writer.set_io_task(self.original.clone());
        }
    }
}

// Unfinished input is private and discarded. BufWriter's implicit Drop flush
// would write after failed/cancelled admission, outside every explicit phase.
pub(super) struct Spool(Option<BufWriter<File>>);
impl Spool {
    pub(super) fn new(writer: BufWriter<File>) -> Self {
        Self(Some(writer))
    }
    pub(super) fn get_ref(&self) -> &File {
        self.0.as_ref().unwrap().get_ref()
    }
    pub(super) fn into_file(mut self) -> File {
        self.0.take().unwrap().into_parts().0
    }
}
impl Write for Spool {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.as_mut().unwrap().write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.as_mut().unwrap().flush()
    }
}
impl Drop for Spool {
    fn drop(&mut self) {
        if let Some(writer) = self.0.take() {
            drop(writer.into_parts());
        }
    }
}

impl std::io::Seek for Spool {
    fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
        self.0.as_mut().unwrap().seek(position)
    }
}

// Necessary private cleanup has its own cancellation lifetime while retaining
// the originating admission. Raw contexts without controllers need no grant.
pub(super) fn cleanup_context(
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> RuntimeTaskContext {
    match &memory.host_admission {
        Some(permit) => permit.bind_task_context(RuntimeTaskContext::default()),
        None if task.has_resource_controllers() => task.clone(),
        None => RuntimeTaskContext::default(),
    }
}
