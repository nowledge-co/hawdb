// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::expression::KeyMemory;
use crate::{QueryMemoryClass, QueryMemoryLedger};
use hawdb_core::Catalog;

fn record(binding: &Binding) -> SpillBindingRecord {
    let mut bytes = Vec::new();
    write_u64(&mut bytes, 37).unwrap();
    write_binding(&mut bytes, binding).unwrap();
    let mut input = Cursor::new(bytes.as_slice());
    read_u64(&mut input).unwrap();
    let decoded_binding_bytes = estimate_binding_memory_bytes(&mut input).unwrap();
    assert_eq!(input.position() as usize, bytes.len());
    SpillBindingRecord {
        payload: SpillRecordPayload {
            bytes,
            _lease: None,
        },
        decoded_binding_bytes,
    }
}

#[test]
fn encoded_keys_match_binding_keys_without_materializing_wide_payloads() {
    let mut binding = Binding::values(BTreeMap::from([
        (
            "key".into(),
            Value::List(vec![
                Value::Null,
                Value::Int(7),
                Value::Map(BTreeMap::from([(
                    "name".into(),
                    Value::String("nested".into()),
                )])),
            ]),
        ),
        ("payload".into(), Value::String("x".repeat(128 * 1024))),
    ]));
    binding.nodes.insert(
        "n".into(),
        NodeRecord {
            id: NodeId(u64::MAX),
            labels: BTreeSet::from([LabelId(2)]),
            properties: BTreeMap::from([("key".into(), Value::Int(3))]),
        },
    );
    binding.relationships.insert(
        "n".into(),
        RelRecord {
            id: RelId(4),
            source: NodeId(1),
            target: NodeId(2),
            rel_type: RelTypeId(3),
            properties: BTreeMap::from([
                ("key".into(), Value::Int(4)),
                ("fallback".into(), Value::String("relationship".into())),
            ]),
        },
    );
    let keys = [
        SortKey::Column("key".into()),
        SortKey::Column("missing".into()),
        SortKey::Property {
            variable: "n".into(),
            property: "key".into(),
        },
        SortKey::Property {
            variable: "n".into(),
            property: "fallback".into(),
        },
        SortKey::Property {
            variable: "missing".into(),
            property: "key".into(),
        },
        SortKey::Id {
            variable: "n".into(),
        },
        SortKey::Id {
            variable: "missing".into(),
        },
    ];
    let items: Vec<_> = keys
        .into_iter()
        .enumerate()
        .map(|(index, key)| SortItem {
            key,
            direction: if index % 2 == 0 {
                SortDirection::Asc
            } else {
                SortDirection::Desc
            },
        })
        .collect();
    let budget = NonZeroUsize::new(4096).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::BlockingState, "encoded keys", budget);
    let memory = KeyMemory::new(&account, budget, "test", "expressions", "keys");
    let expected = memory
        .sort(&items, &Catalog::default(), &binding, None)
        .unwrap();
    let record = record(&binding);
    assert_eq!(record.ordinal().unwrap(), 37);
    assert!(record.decoded_binding_bytes() > budget.get());
    let mut tracker = OperatorMemoryTracker::with_account(budget, account);
    let actual = record.sort_keys(&items, &mut tracker, None).unwrap();
    assert_eq!(actual, expected.values);
    assert_eq!(
        tracker.used_bytes,
        crate::binding::sort_values_memory_bytes(&actual)
    );
    drop(actual);
    drop(expected);
    drop(tracker);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn encoded_key_admission_refuses_before_owning_selected_payload() {
    let binding = Binding::scalar("key", Value::String("x".repeat(8192)));
    let record = record(&binding);
    let budget = NonZeroUsize::new(256).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::BlockingState, "encoded keys", budget);
    let mut tracker = OperatorMemoryTracker::with_account(budget, account);
    let result = record.sort_keys(
        &[SortItem {
            key: SortKey::Column("key".into()),
            direction: SortDirection::Asc,
        }],
        &mut tracker,
        None,
    );
    assert!(matches!(result, Err(HawDBError::ResourceBudgetExceeded(_))));
    assert_eq!(ledger.snapshot().peak_bytes, 0);
    assert_eq!(tracker.used_bytes, 0);
}
