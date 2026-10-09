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

//! Worker lifecycle evidence shared by private analyzer and entrypoint tests.

use super::STACK_BYTES;
use hawdb_executor::QueryMemoryLedger;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Observation {
    exits: Vec<(std::thread::ThreadId, usize, QueryMemoryLedger)>,
}

thread_local! {
    static OBSERVING: RefCell<Option<Arc<Mutex<Observation>>>> = const { RefCell::new(None) };
    static WORKER_EXIT: RefCell<Option<WorkerExit>> = const { RefCell::new(None) };
}

pub(super) struct WorkerExit {
    observation: Arc<Mutex<Observation>>,
    ledger: QueryMemoryLedger,
}

impl Drop for WorkerExit {
    fn drop(&mut self) {
        self.observation.lock().unwrap().exits.push((
            std::thread::current().id(),
            self.ledger.snapshot().used_bytes,
            self.ledger.clone(),
        ));
    }
}

pub(super) fn capture(ledger: &QueryMemoryLedger) -> Option<WorkerExit> {
    OBSERVING.with(|current| {
        current.borrow().as_ref().map(|observation| WorkerExit {
            observation: Arc::clone(observation),
            ledger: ledger.clone(),
        })
    })
}

pub(super) fn install(observation: Option<WorkerExit>) {
    WORKER_EXIT.with(|slot| *slot.borrow_mut() = observation);
}

pub(super) struct Observe(Arc<Mutex<Observation>>);

impl Observe {
    pub(super) fn new() -> Self {
        let observation = Arc::new(Mutex::new(Observation::default()));
        OBSERVING.with(|current| {
            assert!(current.replace(Some(Arc::clone(&observation))).is_none());
        });
        Self(observation)
    }

    pub(super) fn assert_joined(&self, workers: usize) {
        let observation = self.0.lock().unwrap();
        assert_eq!(observation.exits.len(), workers);
        for (worker, bytes, _) in &observation.exits {
            assert_ne!(*worker, std::thread::current().id());
            assert!(
                *bytes >= STACK_BYTES,
                "thread lease released before TLS exit"
            );
        }
    }

    pub(super) fn assert_released(&self) {
        for (_, _, ledger) in &self.0.lock().unwrap().exits {
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    }
}

impl Drop for Observe {
    fn drop(&mut self) {
        OBSERVING.with(|current| current.replace(None));
    }
}
