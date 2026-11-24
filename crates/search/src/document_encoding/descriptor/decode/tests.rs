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
                permit: crate::build_memory::read::ReadPermit::Governed(&permit),
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
                permit: crate::build_memory::read::ReadPermit::Governed(&permit),
            }),
        );
        assert!(
            matches!(result, Err(HawDBError::ResourceBudgetExceeded(ref cause))
            if cause.resource == hawdb_core::ResourceBudgetKind::MemoryBytes
                && cause.budget == (2 << 20)
                && cause.requested > cause.budget.saturating_sub(cause.used))
        );
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
            permit: crate::build_memory::read::ReadPermit::Governed(&permit),
        }),
    );
    assert!(result.unwrap_err().to_string().contains("cancel"));
    assert_eq!(memory.ledger.snapshot().used_bytes, baseline);
    assert_eq!(governor.snapshot().admitted_memory_bytes, host);
}

/// A descriptor as written before #1746: every document body is a distinct
/// value of the `content` field, beside a short per-document `title`.
fn body_descriptor(documents: usize, body_bytes: usize) -> String {
    let body = |document: usize| {
        let mut body = format!("body {document:05} ");
        body.extend(std::iter::repeat_n('x', body_bytes - body.len()));
        body
    };
    let descriptor = SearchSegmentDescriptor {
        target_documents: 128,
        document_count: documents,
        segments: vec![SearchSegmentDescriptorEntry {
            segment_id: 0,
            first_document_id: "a".into(),
            last_document_id: "z".into(),
            document_count: documents,
            payload_range: None,
            metadata: BTreeMap::from([
                (
                    "content".to_string(),
                    SearchSegmentFieldSummary {
                        present_count: documents,
                        values: (0..documents).map(body).collect(),
                        ..Default::default()
                    },
                ),
                (
                    "title".to_string(),
                    SearchSegmentFieldSummary {
                        present_count: documents,
                        values: (0..documents)
                            .map(|index| format!("title {index}"))
                            .collect(),
                        ..Default::default()
                    },
                ),
            ]),
        }],
    };
    let mut bytes = Vec::new();
    crate::document_encoding::DescriptorEncoding::new(&descriptor, u64::MAX)
        .unwrap()
        .write_to(&mut bytes)
        .unwrap();
    String::from_utf8(bytes).unwrap()
}

/// Live bytes and admitted reader memory retained by one bounded decode.
fn retained_by_decode(text: &str) -> (usize, usize, DecodedDescriptor) {
    let governor = governor();
    let request = RuntimeWorkRequest::background_maintenance(16 << 20).with_io_wave_slots(1);
    let permit = governor.try_admit(request).unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let memory = BuildMemory::new(&task).unwrap();
    warm(&memory);
    let (descriptor, live) = allocation::measure(|| {
        let before = allocation::live();
        let descriptor = DecodedDescriptor::decode(
            text,
            Some(&ReadAdmission {
                memory: &memory,
                task: &task,
                permit: crate::build_memory::read::ReadPermit::Governed(&permit),
            }),
        )
        .unwrap();
        (descriptor, allocation::live() - before)
    })
    .0;
    let admitted = memory.ledger.snapshot().used_bytes;
    (live, admitted, descriptor)
}

#[test]
fn legacy_descriptor_bodies_are_not_retained_by_the_reader() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let mut retained = Vec::new();
    for body_bytes in [512, 4096] {
        let text = body_descriptor(128, body_bytes);
        assert!(text.len() > 2 * 128 * body_bytes);
        // The unbounded decode still returns every body.
        let legacy = crate::decode_search_segment_descriptor_text(&text).unwrap();
        assert_eq!(legacy.segments[0].metadata["content"].values.len(), 128);
        assert!(!legacy.segments[0].metadata["content"].values_omitted);
        drop(legacy);

        let (live, admitted, descriptor) = retained_by_decode(&text);
        let content = &descriptor.segments[0].metadata["content"];
        assert!(content.values_omitted);
        assert!(content.values.is_empty());
        assert_eq!(content.present_count, 128);
        let title = &descriptor.segments[0].metadata["title"];
        assert!(!title.values_omitted);
        assert_eq!(title.values.len(), 128);
        // Neither the heap nor the admitted reader memory holds the bodies:
        // what remains is the 128 short titles and their set entries.
        assert!(
            live > 0 && live < 128 * 128,
            "body_bytes={body_bytes} live={live}"
        );
        assert!(
            admitted < 128 * (crate::build_memory::SET_ENTRY_BYTES + 64) + (16 << 10),
            "body_bytes={body_bytes} admitted={admitted}"
        );
        retained.push((live, admitted));
        drop(descriptor);
        assert_eq!(allocation::live(), 0);
    }
    // Retained memory does not grow with the body length. Admission differs
    // only by the decimal width of the descriptor checksum.
    assert_eq!(retained[0].0, retained[1].0);
    assert!(retained[0].1.abs_diff(retained[1].1) <= 32, "{retained:?}");
}

#[test]
fn bounded_decode_keeps_value_sets_at_the_exact_bounds() {
    let field = |values: BTreeSet<String>| {
        let descriptor = SearchSegmentDescriptor {
            target_documents: 128,
            document_count: 1,
            segments: vec![SearchSegmentDescriptorEntry {
                segment_id: 0,
                first_document_id: "a".into(),
                last_document_id: "z".into(),
                document_count: 1,
                payload_range: None,
                metadata: BTreeMap::from([(
                    "note".to_string(),
                    SearchSegmentFieldSummary {
                        present_count: 1,
                        values,
                        ..Default::default()
                    },
                )]),
            }],
        };
        let mut bytes = Vec::new();
        crate::document_encoding::DescriptorEncoding::new(&descriptor, u64::MAX)
            .unwrap()
            .write_to(&mut bytes)
            .unwrap();
        let text = String::from_utf8(bytes).unwrap();
        let decoded = DecodedDescriptor::decode(&text, None).unwrap();
        decoded.segments[0].metadata["note"].clone()
    };
    let max_value = crate::SEARCH_SEGMENT_VALUE_SUMMARY_MAX_VALUE_BYTES;
    let max_field = crate::SEARCH_SEGMENT_VALUE_SUMMARY_MAX_FIELD_BYTES;
    // One value at the value bound is kept, one byte more is omitted.
    let kept = field(BTreeSet::from(["a".repeat(max_value)]));
    assert!(!kept.values_omitted);
    assert_eq!(kept.values, BTreeSet::from(["a".repeat(max_value)]));
    let omitted = field(BTreeSet::from(["a".repeat(max_value + 1)]));
    assert!(omitted.values_omitted && omitted.values.is_empty());
    // Distinct values summing to the field bound are kept, one byte more is
    // omitted.
    let at_bound = (0..max_field / max_value)
        .map(|index| format!("{index:03}{}", "v".repeat(max_value - 3)))
        .collect::<BTreeSet<_>>();
    assert_eq!(at_bound.iter().map(String::len).sum::<usize>(), max_field);
    assert!(!field(at_bound.clone()).values_omitted);
    let mut over = at_bound;
    over.insert("x".into());
    let over = field(over);
    assert!(over.values_omitted && over.values.is_empty());
}
