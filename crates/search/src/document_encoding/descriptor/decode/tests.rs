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
use crate::build_memory::read::tests::governor;
use crate::build_memory::BuildMemory;
use crate::test_allocation as allocation;
use crate::{SearchSegmentDescriptorEntry, SearchSegmentFieldSummary};
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::RuntimeWorkRequest;
use std::collections::{BTreeMap, BTreeSet};

fn encoded(fields: usize, values: usize) -> String {
    let descriptor = SearchSegmentDescriptor {
        target_documents: 256,
        document_count: 1,
        segments: vec![SearchSegmentDescriptorEntry {
            segment_id: 0,
            first_document_id: "a".into(),
            last_document_id: "z".into(),
            document_count: 1,
            payload_range: None,
            metadata: (0..fields)
                .map(|field| {
                    (
                        format!("field-{field}"),
                        SearchSegmentFieldSummary {
                            present_count: 1,
                            values: (0..values)
                                .map(|value| format!("value-{value}"))
                                .collect::<BTreeSet<_>>(),
                            ..Default::default()
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        }],
    };
    let mut bytes = Vec::new();
    crate::document_encoding::DescriptorEncoding::new(&descriptor, u64::MAX)
        .unwrap()
        .write_to(&mut bytes)
        .unwrap();
    String::from_utf8(bytes).unwrap()
}

fn warm(memory: &BuildMemory) {
    for account in [&memory.input, &memory.spool, &memory.retained] {
        drop(account.reserve(1).unwrap());
    }
}

#[test]
fn descriptor_memory_survives_producer_without_holding_execution() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let text = encoded(4, 512);
    let governor = governor();
    let request = RuntimeWorkRequest::background_maintenance(16 << 20).with_io_wave_slots(1);
    let permit = governor.try_admit(request).unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let memory = BuildMemory::new(&task).unwrap();
    assert!(memory.host_admission.is_none());
    warm(&memory);
    let (descriptor, peak) = allocation::measure(|| {
        DecodedDescriptor::decode(
            &text,
            Some(&ReadAdmission {
                memory: &memory,
                task: &task,
                permit: &permit,
            }),
        )
        .unwrap()
    });
    assert_eq!(descriptor.segments[0].metadata.len(), 4);
    assert!(peak <= memory.ledger.snapshot().peak_bytes);
    assert!(allocation::live() > 0);
    drop(memory);
    drop(task);
    drop(permit);
    let snapshot = governor.snapshot();
    assert_eq!(snapshot.active_cpu_slots, 0);
    assert_eq!(snapshot.active_background_tasks, 0);
    assert_eq!(snapshot.active_background_io_slots, 0);
    assert!(snapshot.admitted_memory_bytes >= allocation::live() as u64);
    drop(governor.try_admit(request).unwrap());
    drop(descriptor);
    assert_eq!(allocation::live(), 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn descriptor_refuses_map_expansion_before_owned_metadata_allocation() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let text = encoded(8192, 1);
    assert!(text.len() < 2 << 20);
    let (legacy, peak) =
        allocation::measure(|| crate::decode_search_segment_descriptor_text(&text).unwrap());
    assert!(
        peak > 2 << 20,
        "fixture must exceed the native working budget: {peak}"
    );
    drop(legacy);
    assert_eq!(allocation::live(), 0);
    let governor = governor();
    let request = RuntimeWorkRequest::background_maintenance(2 << 20).with_io_wave_slots(1);
    let permit = governor.try_admit(request).unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    warm(&memory);
    let baseline = memory.ledger.snapshot().used_bytes;
    let host = governor.snapshot().admitted_memory_bytes;
    let (_, peak) = allocation::measure(|| {
        let result = DecodedDescriptor::decode(
            &text,
            Some(&ReadAdmission {
                memory: &memory,
                task: &task,
                permit: &permit,
            }),
        );
        assert!(matches!(result, Err(HawDBError::Execution(_))));
    });
    assert!(
        peak < 16 << 10,
        "denial must precede map/set allocation: {peak}"
    );
    assert_eq!(allocation::live(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, baseline);
    assert_eq!(governor.snapshot().admitted_memory_bytes, host);
    drop(memory);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn cancelled_descriptor_preflight_keeps_both_ledgers_unchanged() {
    let _serial = allocation::serial();
    let text = encoded(2, 2);
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(2 << 20))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    warm(&memory);
    task.cancellation().cancel();
    let baseline = memory.ledger.snapshot().used_bytes;
    let host = governor.snapshot().admitted_memory_bytes;
    let result = DecodedDescriptor::decode(
        &text,
        Some(&ReadAdmission {
            memory: &memory,
            task: &task,
            permit: &permit,
        }),
    );
    assert!(result.unwrap_err().to_string().contains("cancel"));
    assert_eq!(memory.ledger.snapshot().used_bytes, baseline);
    assert_eq!(governor.snapshot().admitted_memory_bytes, host);
}
