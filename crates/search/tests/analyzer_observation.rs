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

//! Compile shared test evidence without the search index or generation writer.

use hawdb_core::RuntimeCancellationToken;
use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};
use std::io::{Cursor, Read};
use std::num::NonZeroUsize;

#[path = "../src/analyzer_workspace/stack.rs"]
mod stack;
use stack::STACK_BYTES;

#[path = "../src/analyzer_workspace/observation.rs"]
mod observation;

#[path = "../src/build_control/read_observation.rs"]
mod read_observation;

#[test]
fn native_worker_exit_records_the_lease_before_parent_release() {
    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(STACK_BYTES * 2).unwrap());
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "analyzer worker evidence",
        NonZeroUsize::new(STACK_BYTES * 2).unwrap(),
    );
    let lease = account.reserve(STACK_BYTES).unwrap();
    let observe = observation::Observe::new();
    let captured = observation::capture(&ledger);
    std::thread::spawn(move || observation::install(captured))
        .join()
        .unwrap();
    observe.assert_joined(1);
    drop(lease);
    observe.assert_released();
    drop(observe);
    assert!(observation::capture(&ledger).is_none());
}

#[test]
fn read_evidence_follows_the_worker_and_restores_its_previous_scope() {
    read_observation::take();
    read_observation::take_max_request();
    let captured = read_observation::capture();
    std::thread::spawn(move || {
        let restore = captured.install();
        let mut file = read_observation::track(Cursor::new(b"abcd"));
        let mut bytes = [0; 3];
        assert_eq!(file.read(&mut bytes).unwrap(), 3);
        assert_eq!(&bytes, b"abc");
        drop(restore);
        // Reads after restoration stay in the worker's original TLS scope.
        let mut file = read_observation::track(Cursor::new(b"private"));
        assert_eq!(file.read(&mut bytes).unwrap(), 3);
        assert_eq!(read_observation::take(), (1, 3));
    })
    .join()
    .unwrap();
    assert_eq!(read_observation::take(), (1, 3));
    assert_eq!(read_observation::take_max_request(), 3);
}

#[test]
fn cancellation_observation_crosses_the_worker_and_guard_drop_disarms_it() {
    read_observation::take();
    let token = RuntimeCancellationToken::default();
    let guard = read_observation::cancel_after_bytes(2, token.clone());
    let captured = read_observation::capture();
    std::thread::spawn(move || {
        let _restore = captured.install();
        let mut file = read_observation::track_reads(Cursor::new(b"abc"));
        let mut byte = [0; 1];
        assert_eq!(file.read(&mut byte).unwrap(), 1);
        assert_eq!(file.read(&mut byte).unwrap(), 1);
    })
    .join()
    .unwrap();
    assert!(token.is_cancelled());
    assert_eq!(read_observation::take(), (0, 2));
    drop(guard);

    let token = RuntimeCancellationToken::default();
    let guard = read_observation::cancel_after_bytes(0, token.clone());
    drop(guard);
    let mut file = read_observation::track_reads(Cursor::new(b"abcd"));
    let mut bytes = [0; 4];
    assert_eq!(file.read(&mut bytes).unwrap(), 4);
    assert!(!token.is_cancelled());
    assert_eq!(read_observation::take(), (0, 4));
}
