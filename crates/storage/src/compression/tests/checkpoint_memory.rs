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

use super::*;
use crate::background::CheckpointOperationError;
use hawdb_core::{RuntimeMemoryController, RuntimeMemoryError, RuntimeTaskContext};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug, Default)]
struct Controller {
    live: Arc<AtomicU64>,
    calls: AtomicUsize,
    panic_allocation: Option<usize>,
    panic_release: Option<usize>,
}

#[derive(Debug)]
struct Permit {
    live: Arc<AtomicU64>,
    bytes: u64,
    panic_release: bool,
}

impl RuntimeMemoryController for Controller {
    fn reserve(
        &self,
        bytes: u64,
        _ceiling: u64,
    ) -> Result<Box<dyn RuntimeMemoryPermit>, RuntimeMemoryError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        assert_ne!(
            self.panic_allocation,
            Some(call),
            "host allocator panic fixture"
        );
        self.live.fetch_add(bytes, Ordering::SeqCst);
        Ok(Box::new(Permit {
            live: self.live.clone(),
            bytes,
            panic_release: self.panic_release == Some(call),
        }))
    }
}

impl RuntimeMemoryPermit for Permit {
    fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        assert!(self.live.fetch_sub(self.bytes, Ordering::SeqCst) >= self.bytes);
        assert!(!self.panic_release, "host release panic fixture");
    }
}

fn encode(work: &CheckpointWorkContext) -> io::Result<Vec<u8>> {
    let mut encoder = Encoder::new_with_work_context(Vec::new(), 3, work)?;
    encoder.write_all(&[0x3f; 64 * 1024])?;
    encoder.finish()
}

#[test]
fn every_native_allocation_failure_refunds_completed_workspace_and_staging_owners() {
    let controller = Arc::new(Controller::default());
    let work = CheckpointWorkContext::new(
        RuntimeTaskContext::default().with_memory_controller(controller.clone()),
    );
    let (result, baseline) = observe(None, || encode(&work));
    assert!(result.is_ok());
    assert!(baseline.allocations >= 2);
    assert!(baseline.allocated_bytes > 1024 * 1024);
    assert_eq!(controller.live.load(Ordering::SeqCst), 0);
    for failure in 0..baseline.allocations {
        let (result, _) = observe(Some(failure), || encode(&work));
        assert!(result.is_err(), "native allocation {failure}");
        assert_eq!(
            controller.live.load(Ordering::SeqCst),
            0,
            "native allocation {failure}"
        );
    }
}

#[test]
fn a_host_memory_controller_panic_returns_a_typed_error_without_crossing_the_native_abi() {
    let controller = Arc::new(Controller {
        // Staging and opaque ownership are the first two Rust reservations;
        // the native context asks for the third inside the C callback.
        panic_allocation: Some(2),
        ..Default::default()
    });
    let work = CheckpointWorkContext::new(
        RuntimeTaskContext::default().with_memory_controller(controller.clone()),
    );
    let (error, _) = observe(None, || work.classify(encode));
    assert!(matches!(
        error,
        Err(CheckpointOperationError::Work(
            CheckpointWorkError::Allocation { .. }
        ))
    ));
    assert_eq!(controller.live.load(Ordering::SeqCst), 0);
}

#[test]
fn a_native_memory_permit_panic_frees_physical_data_and_prevents_a_successful_result() {
    let controller = Arc::new(Controller {
        panic_release: Some(2),
        ..Default::default()
    });
    let work = CheckpointWorkContext::new(
        RuntimeTaskContext::default().with_memory_controller(controller.clone()),
    );
    let (error, _) = observe(None, || work.classify(encode));
    assert!(matches!(
        error,
        Err(CheckpointOperationError::Work(
            CheckpointWorkError::Allocation { .. }
        ))
    ));
    assert_eq!(controller.live.load(Ordering::SeqCst), 0);
    // The internal encoder must reject this result even without the outer
    // checkpoint error-classification scope.
    let controller = Arc::new(Controller {
        panic_release: Some(2),
        ..Default::default()
    });
    let work = CheckpointWorkContext::new(
        RuntimeTaskContext::default().with_memory_controller(controller.clone()),
    );
    let (error, _) = observe(None, || encode(&work));
    assert!(error.is_err());
    assert_eq!(controller.live.load(Ordering::SeqCst), 0);
}

#[test]
fn decoder_native_allocations_are_admitted_and_refunded_on_each_failure() {
    let input = vec![0x3f; 300_000];
    let encoded = encode_all(input.as_slice(), 3).unwrap();
    let controller = Arc::new(Controller::default());
    let work = CheckpointWorkContext::new(
        RuntimeTaskContext::default().with_memory_controller(controller.clone()),
    );
    let decode = || -> io::Result<Vec<u8>> {
        let mut decoder = Decoder::with_buffer_and_work_context(encoded.as_slice(), &work)?;
        assert!(controller.live.load(Ordering::SeqCst) > 0);
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded)?;
        assert!(controller.live.load(Ordering::SeqCst) > 0);
        Ok(decoded)
    };
    let (result, baseline) = observe(None, decode);
    assert_eq!(result.unwrap(), input);
    assert!(baseline.allocations >= 2);
    assert_eq!(controller.live.load(Ordering::SeqCst), 0);
    for failure in 0..baseline.allocations {
        let (result, _) = observe(Some(failure), decode);
        assert!(result.is_err(), "native allocation {failure}");
        assert_eq!(controller.live.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn decoder_host_callback_panics_preserve_typed_failure_and_refund_memory() {
    let encoded = encode_all(&[0x3f; 300_000][..], 3).unwrap();
    for release in [false, true] {
        let controller = Arc::new(Controller {
            panic_allocation: (!release).then_some(1),
            panic_release: release.then_some(1),
            ..Default::default()
        });
        let work = CheckpointWorkContext::new(
            RuntimeTaskContext::default().with_memory_controller(controller.clone()),
        );
        let (result, _) = observe(None, || {
            work.classify(|scoped| -> io::Result<Vec<u8>> {
                let mut decoder =
                    Decoder::with_buffer_and_work_context(encoded.as_slice(), scoped)?;
                let mut decoded = Vec::new();
                decoder.read_to_end(&mut decoded)?;
                Ok(decoded)
            })
        });
        assert!(matches!(
            result,
            Err(CheckpointOperationError::Work(
                CheckpointWorkError::Allocation { .. }
            ))
        ));
        assert_eq!(controller.live.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn boxed_backend_state_admission_and_release_panics_fail_closed() {
    for decoder in [false, true] {
        let controller = Arc::new(Controller::default());
        let work = CheckpointWorkContext::new(
            RuntimeTaskContext::default().with_memory_controller(controller.clone()),
        );
        let boxed_state_call = if decoder {
            let context = DecompressionContext::with_work_context(Some(&work)).unwrap();
            let call = controller.calls.load(Ordering::SeqCst) - 1;
            drop(context);
            call
        } else {
            let encoder = Encoder::new_with_work_context(io::sink(), 3, &work).unwrap();
            let call = controller.calls.load(Ordering::SeqCst) - 1;
            drop(encoder);
            call
        };
        assert_eq!(controller.live.load(Ordering::SeqCst), 0);
        for release in [false, true] {
            let controller = Arc::new(Controller {
                panic_allocation: (!release).then_some(boxed_state_call),
                panic_release: release.then_some(boxed_state_call),
                ..Default::default()
            });
            let work = CheckpointWorkContext::new(
                RuntimeTaskContext::default().with_memory_controller(controller.clone()),
            );
            let (result, _) = observe(None, || {
                work.classify(|scoped| -> io::Result<()> {
                    if decoder {
                        drop(DecompressionContext::with_work_context(Some(scoped))?);
                    } else {
                        Encoder::new_with_work_context(io::sink(), 3, scoped)?.finish()?;
                    }
                    Ok(())
                })
            });
            assert!(matches!(
                result,
                Err(CheckpointOperationError::Work(
                    CheckpointWorkError::Allocation { .. }
                ))
            ));
            assert_eq!(controller.live.load(Ordering::SeqCst), 0);
        }
    }
}
