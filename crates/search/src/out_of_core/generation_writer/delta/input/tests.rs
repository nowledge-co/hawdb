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
use crate::SearchProjectionKind;
use hawdb_core::RuntimeMemoryReservation;
use std::collections::BTreeMap;

fn row(index: usize) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: format!("{index:04}"),
        title: "title".into(),
        body: "body".into(),
        embedding: Some(vec![1.0, 2.0]),
        source_id: Some("source".into()),
        metadata: BTreeMap::from([
            ("kind".into(), "overwritten".into()),
            ("external_id".into(), "overwritten".into()),
            ("source_id".into(), "overwritten".into()),
            ("space_id".into(), "retained".into()),
        ]),
    }
}

#[test]
fn delta_input_admits_spare_capacities_before_conversion() {
    let task = RuntimeTaskContext::default();
    for case in 0..8 {
        let mut delta = SearchProjectionDelta {
            upserts: vec![row(1)],
            ..Default::default()
        };
        let row = &mut delta.upserts[0];
        match case {
            0 => delta.upserts.reserve_exact(4096),
            1 => delta.deletes.reserve_exact(4096),
            2 => row.external_id.reserve_exact(65536),
            3 => row.body.reserve_exact(65536),
            4 => row.title.reserve_exact(65536),
            5 => row.embedding.as_mut().unwrap().reserve_exact(65536),
            6 => row.source_id.as_mut().unwrap().reserve_exact(65536),
            _ => row
                .metadata
                .get_mut("space_id")
                .unwrap()
                .reserve_exact(65536),
        }
        let bytes = capacity_bytes(&delta, &task).unwrap();
        let limited = RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new((bytes - 1) as u64, 0));
        let memory = BuildMemory::new(&limited).unwrap();
        assert!(Input::new(delta, &memory, &limited).is_err(), "case {case}");
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn delta_conversion_matches_row_contract_and_transfers_batch_lifetime() {
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task).unwrap();
    let rows = (0..257).rev().map(row).collect::<Vec<_>>();
    let expected = rows
        .iter()
        .cloned()
        .map(SearchProjectionRow::into_document)
        .collect::<Vec<_>>();
    let mut input = Input::new(
        SearchProjectionDelta {
            upserts: rows,
            deletes: vec!["z".into(), "a".into()],
            ..Default::default()
        },
        &memory,
        &task,
    )
    .unwrap();
    assert_eq!(input.deletes, ["a", "z"]);
    for expected in expected.iter().rev().take(256) {
        let actual = input.pop_upsert();
        assert_eq!(&actual.document, expected);
    }
    let last = input.pop_upsert();
    let held = memory.ledger.snapshot().used_bytes;
    drop(input);
    assert_eq!(memory.ledger.snapshot().used_bytes, held);
    assert!(held >= document_bytes(&last).unwrap());
    assert_eq!(last.document, expected[0]);
    drop(last);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn delta_sort_matches_total_order_and_observes_cancellation() {
    let task = RuntimeTaskContext::default();
    for length in 0..200 {
        let mut values = (0..length)
            .map(|index| (index * 37) % 19)
            .collect::<Vec<_>>();
        let mut expected = values.clone();
        expected.sort_unstable();
        sort(&mut values, &task, Ord::cmp).unwrap();
        assert_eq!(values, expected);
    }
    let cancel = RuntimeTaskContext::default();
    let count = std::cell::Cell::new(0usize);
    let mut values = (0..4096).collect::<Vec<_>>();
    assert!(sort(&mut values, &cancel, |left, right| {
        count.set(count.get() + 1);
        if count.get() == 17 {
            cancel.cancellation().cancel();
        }
        left.cmp(right)
    })
    .is_err());
    assert!(count.get() < 20);
}

#[test]
fn pending_delta_owns_raw_input_before_identity_and_conversion_work() {
    let budget = 1024 * 1024;
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(budget, 0));
    let memory = BuildMemory::new(&task).unwrap();
    let mut delta = SearchProjectionDelta {
        upserts: vec![row(1)],
        ..Default::default()
    };
    delta.upserts[0].body.reserve_exact(64 * 1024);
    let bytes = capacity_bytes(&delta, &task).unwrap();
    let pending = Pending::new(delta, &memory, &task).unwrap();
    assert_eq!(memory.ledger.snapshot().used_bytes, bytes);
    let held = memory.retained.reserve(budget as usize - bytes).unwrap();
    assert!(pending.convert(&task).is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, held.bytes());
    drop(held);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}
