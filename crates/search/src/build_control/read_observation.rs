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

//! Spool read evidence propagated across an operation-owned analyzer worker.

use std::cell::RefCell;
use std::io::Read;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Observation {
    opens: usize,
    bytes: u64,
    max_request: usize,
    cancel_after: Option<(u64, crate::RuntimeCancellationToken)>,
}

thread_local! {
    static CURRENT: RefCell<Arc<Mutex<Observation>>> = RefCell::new(Default::default());
}

// Keep each test isolated while explicitly following its analyzer worker.
pub(crate) struct Capture(Arc<Mutex<Observation>>);

pub(crate) fn capture() -> Capture {
    CURRENT.with(|slot| Capture(Arc::clone(&slot.borrow())))
}

impl Capture {
    pub(crate) fn install(self) -> Restore {
        Restore(CURRENT.with(|slot| slot.replace(self.0)))
    }
}

pub(crate) struct Restore(Arc<Mutex<Observation>>);

impl Drop for Restore {
    fn drop(&mut self) {
        CURRENT.with(|slot| *slot.borrow_mut() = Arc::clone(&self.0));
    }
}

fn observe<T>(work: impl FnOnce(&mut Observation) -> T) -> T {
    CURRENT.with(|slot| work(&mut slot.borrow().lock().unwrap()))
}

pub(crate) struct CancelGuard;

impl Drop for CancelGuard {
    fn drop(&mut self) {
        observe(|state| state.cancel_after = None);
    }
}

pub(crate) fn cancel_after_bytes(
    bytes: u64,
    token: crate::RuntimeCancellationToken,
) -> CancelGuard {
    observe(|state| state.cancel_after = Some((bytes, token)));
    CancelGuard
}

pub(crate) struct TrackedFile<R>(R);

pub(crate) fn track<R: Read>(file: R) -> TrackedFile<R> {
    observe(|state| state.opens += 1);
    TrackedFile(file)
}

pub(crate) fn track_reads<R: Read>(file: R) -> TrackedFile<R> {
    TrackedFile(file)
}

pub(crate) fn take() -> (usize, u64) {
    observe(|state| {
        (
            std::mem::take(&mut state.opens),
            std::mem::take(&mut state.bytes),
        )
    })
}

pub(crate) fn take_max_request() -> usize {
    observe(|state| std::mem::take(&mut state.max_request))
}

impl<R: Read> Read for TrackedFile<R> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        observe(|state| state.max_request = state.max_request.max(output.len()));
        let count = self.0.read(output)?;
        observe(|state| {
            state.bytes += count as u64;
            if state
                .cancel_after
                .as_ref()
                .is_some_and(|(limit, _)| state.bytes >= *limit)
            {
                state.cancel_after.take().unwrap().1.cancel();
            }
        });
        Ok(count)
    }
}
