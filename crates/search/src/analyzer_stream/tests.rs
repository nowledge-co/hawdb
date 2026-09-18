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
use crate::build_control::observation;
use crate::build_memory::BuildMemory;
use hawdb_core::{RuntimeMemoryReservation, RuntimeTaskContext};

#[test]
fn admitted_tokenizer_throttles_deadline_checks() {
    const IDENTIFIERS: usize = 4096;

    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(64 * 1024 * 1024, 0));
    let memory = BuildMemory::new(&task).unwrap();
    let text = (0..IDENTIFIERS)
        .map(|index| format!("token{index:04}"))
        .collect::<Vec<_>>()
        .join(" ");
    let mut emitted = 0usize;
    let (result, checkpoints) = observation::measure(|| {
        visit_admitted_token_list(
            &text,
            &SearchAnalyzerLexicon::default(),
            Control {
                memory: Some(&memory),
                task: Some(&task),
                workspace: None,
                checkpoint_throttle: None,
            },
            |term, _| {
                emitted += 1;
                drop(term);
                Ok(())
            },
        )
    });
    result.unwrap();
    assert!(emitted >= IDENTIFIERS);
    assert!(
        checkpoints <= IDENTIFIERS / 64 + 16,
        "emitted={emitted} checkpoints={checkpoints}"
    );
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}
