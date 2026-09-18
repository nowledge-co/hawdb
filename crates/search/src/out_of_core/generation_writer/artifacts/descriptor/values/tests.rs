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

use super::super::tests::document;
use super::*;
use hawdb_core::RuntimeMemoryReservation;

#[test]
fn escaped_json_scratch_is_bounded_by_one_token_and_is_admitted_before_visiting() {
    let task = RuntimeTaskContext::default();
    let repeated = format!("[{}]", vec![r#""\u0041""#; 32768].join(","));
    assert_eq!(json_scratch_bytes(&repeated, &task).unwrap(), 24);
    let source = document(0, &[("labels", &repeated)]);
    for budget in [535, 536] {
        let task = task
            .clone()
            .with_memory_reservation(RuntimeMemoryReservation::new(budget, 0));
        let memory = BuildMemory::new(&task).unwrap();
        let mut visited = 0;
        let result = visit_with_context(&source, "labels", &memory, &task, &mut |value| {
            assert_eq!(value, "A");
            visited += 1;
            Ok(())
        });
        assert_eq!(result.is_ok(), budget == 536);
        assert_eq!(visited, if budget == 536 { 32768 } else { 0 });
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    assert_eq!(json_scratch_bytes(r#"["plain","text"]"#, &task).unwrap(), 0);
    assert_eq!(json_scratch_bytes(r#"["\u0041"#, &task).unwrap(), 24);
}

#[test]
fn cancellation_during_json_validation_never_selects_csv_fallback() {
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task).unwrap();
    let source = document(0, &[("labels", r#"["first","second"] trailing"#)]);
    evidence::cancel_after(1, task.cancellation().clone());
    let mut visited = 0;
    let result = visit_with_context(&source, "labels", &memory, &task, &mut |_| {
        visited += 1;
        Ok(())
    });
    assert!(result.unwrap_err().to_string().contains("cancel"));
    assert_eq!(visited, 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn non_array_strings_use_csv_without_allocating_json_error_text() {
    let raw = format!("\"{}\"", "x".repeat(256 * 1024));
    let source = document(0, &[("labels", &raw)]);
    let task =
        RuntimeTaskContext::default().with_memory_reservation(RuntimeMemoryReservation::new(1, 0));
    let memory = BuildMemory::new(&task).unwrap();
    let mut visits = 0;
    visit_with_context(&source, "labels", &memory, &task, &mut |value| {
        assert_eq!(value, raw);
        visits += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(visits, 1);
    assert_eq!(memory.ledger.snapshot().peak_bytes, 0);
}
