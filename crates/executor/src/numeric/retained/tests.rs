// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{NumericLiteral, NumericPredicate, QueryMemoryClass, QueryMemoryLedger};
use hawdb_plan_cypher::ComparisonOp;
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use hawdb_storage::NodeId;
use std::collections::{BTreeMap, BTreeSet};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn governor(bytes: u64, handles: usize) -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            result_budget_bytes: bytes,
            retained_result_handle_limit: nz(handles),
            memory_budget_bytes: Some(1 << 24),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(nz(4), None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    )
}

fn account(bytes: usize) -> (QueryMemoryLedger, QueryMemoryAccount) {
    let ledger = QueryMemoryLedger::new(nz(bytes));
    let account = ledger.account(
        QueryMemoryClass::ResultMaterialization,
        "retained",
        nz(bytes),
    );
    (ledger, account)
}

fn fragment() -> NumericFragment<'static> {
    NumericFragment {
        label: "Item",
        property: "score",
        property_type: PropertyType::Int,
        predicate: NumericPredicate::Compare(ComparisonOp::Gte),
        expected: NumericLiteral::Int(2),
        fused_operators: None,
    }
}

fn node(id: u64, value: Option<Value>) -> NodeRecord {
    NodeRecord {
        id: NodeId(id),
        labels: BTreeSet::new(),
        properties: value.map_or_else(BTreeMap::new, |value| {
            BTreeMap::from([("score".into(), value)])
        }),
    }
}

fn int_values(batch: &RetainedNumericBatch) -> &[i64] {
    match batch.values().0 {
        RetainedNumericValues::Int(values) => values,
        RetainedNumericValues::Float(_) => panic!("expected integer values"),
    }
}

#[test]
fn seal_and_retain_preserve_every_allocation_and_sparse_selection() {
    let governor = governor(64 * 1024, 8);
    let permit = governor
        .try_admit(RuntimeWorkRequest::foreground_query(0, 64 * 1024))
        .unwrap();
    let (ledger, account) = account(64 * 1024);
    let mut builder =
        RetainedNumericBuilder::new(fragment(), nz(5), true, account, &permit).unwrap();
    for (id, value) in [Some(1), None, Some(3), Some(0), Some(5)]
        .into_iter()
        .enumerate()
    {
        builder
            .push_node(&node(id as u64, value.map(Value::Int)))
            .unwrap();
    }
    let (values, identity) = builder.values();
    let values_ptr = match values {
        RetainedNumericValues::Int(values) => values.as_ptr(),
        RetainedNumericValues::Float(_) => unreachable!(),
    };
    let ids_ptr = builder.node_ids.as_ref().unwrap().as_ptr();
    let mask_ptr = builder.validity.as_ptr();
    let selection_ptr = builder.selection.as_ptr();
    let producer_ranges = builder.buffer_provenance();
    let batch = builder.seal(Some(1)).unwrap();
    let sealed_ranges = batch.buffer_provenance();
    assert_eq!(producer_ranges[..3], sealed_ranges[..3]);
    let selection = sealed_ranges[3].unwrap();
    assert_eq!(selection.identity, producer_ranges[3].unwrap().identity);
    assert_eq!(
        selection.retained_capacity_bytes,
        producer_ranges[3].unwrap().retained_capacity_bytes
    );
    assert_eq!(selection.byte_offset, 0);
    assert_eq!(selection.byte_length, 4);
    assert!(selection.retained_capacity_bytes >= 20);
    assert_eq!(int_values(&batch), &[1, 0, 3, 0, 5]);
    assert_eq!(int_values(&batch).as_ptr(), values_ptr);
    assert_eq!(batch.values().1, identity);
    assert_eq!(batch.node_ids().unwrap().0.as_ptr(), ids_ptr);
    assert_eq!(batch.validity_buffer().unwrap().0.as_ptr(), mask_ptr);
    assert_eq!(batch.selected_rows().0.as_ptr(), selection_ptr);
    assert_eq!(batch.selected_rows().0, &[2]);
    assert_eq!(batch.physical_rows(), 5);
    assert_eq!(batch.source_constructed_bytes(), 80);
    let before = governor.retained_result_snapshot();
    let view = batch.try_retain(24).unwrap();
    assert_eq!(view.buffer_provenance(), sealed_ranges);
    assert_eq!(int_values(&view).as_ptr(), values_ptr);
    assert_eq!(view.node_ids().unwrap().0.as_ptr(), ids_ptr);
    assert_eq!(view.validity_buffer().unwrap().0.as_ptr(), mask_ptr);
    assert_eq!(view.selected_rows().0.as_ptr(), selection_ptr);
    assert_eq!(view.storage.identities, batch.storage.identities);
    assert_eq!(governor.retained_result_snapshot().buffer_owners, 1);
    assert_eq!(
        governor.retained_result_snapshot().retained_bytes,
        before.retained_bytes + view.runtime.handle_bytes()
    );
    drop((batch, permit));
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(int_values(&view), &[1, 0, 3, 0, 5]);
    assert!(!view.validity().is_valid(1));
    assert_eq!(
        ledger.snapshot().used_bytes as u64,
        governor.retained_result_snapshot().retained_bytes
    );
    drop(view);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
}

#[test]
fn floating_point_bits_survive_source_and_governor_handle_destruction() {
    let (ledger, account) = account(64 * 1024);
    let bits = [
        (-0.0f64).to_bits(),
        0x7ff8_1234_5678_9abc,
        f64::INFINITY.to_bits(),
    ];
    let batch = {
        let governor = governor(64 * 1024, 8);
        let permit = governor
            .try_admit(RuntimeWorkRequest::foreground_query(0, 64 * 1024))
            .unwrap();
        let property = String::from("score");
        let fragment = NumericFragment {
            property: &property,
            property_type: PropertyType::Float,
            expected: NumericLiteral::Float(0.0),
            ..fragment()
        };
        let mut builder =
            RetainedNumericBuilder::new(fragment, nz(4), false, account, &permit).unwrap();
        for (id, bits) in bits.iter().enumerate() {
            builder
                .push_node(&node(id as u64, Some(Value::Float(f64::from_bits(*bits)))))
                .unwrap();
        }
        builder.push_node(&node(3, Some(Value::Null))).unwrap();
        builder.seal(None).unwrap()
    };
    match batch.values().0 {
        RetainedNumericValues::Float(values) => {
            assert_eq!(
                values[..3]
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
                bits
            );
            assert_eq!(values[3].to_bits(), 0);
        }
        RetainedNumericValues::Int(_) => panic!("expected float values"),
    }
    // Existing numeric comparisons use total order: -0 < +0 < +inf < +NaN.
    assert_eq!(batch.selected_rows().0, &[1, 2]);
    assert_eq!(batch.validity().valid_count(), 3);
    assert!(batch.node_ids().is_none());
    let retained = batch.try_retain(0).unwrap();
    drop(batch);
    assert_eq!(retained.physical_rows(), 4);
    drop(retained);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn nullable_masks_cover_word_boundaries_and_all_valid_needs_no_mask() {
    for missing in [None, Some(0), Some(63), Some(64), Some(65)] {
        let governor = governor(64 * 1024, 8);
        let permit = governor
            .try_admit(RuntimeWorkRequest::foreground_query(0, 64 * 1024))
            .unwrap();
        let (ledger, account) = account(64 * 1024);
        let mut builder =
            RetainedNumericBuilder::new(fragment(), nz(66), false, account, &permit).unwrap();
        for row in 0..66 {
            let value = (missing != Some(row)).then_some(Value::Int(3));
            builder.push_node(&node(row as u64, value)).unwrap();
        }
        let batch = builder.seal(None).unwrap();
        assert_eq!(
            batch.validity().valid_count(),
            66 - usize::from(missing.is_some())
        );
        assert_eq!(batch.validity_buffer().is_some(), missing.is_some());
        for row in 0..66 {
            assert_eq!(batch.validity().is_valid(row), missing != Some(row));
        }
        drop(batch);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn exact_ledger_budget_seals_without_a_second_charge_and_retain_failure_rolls_back() {
    let bytes = RetainedNumericBuilder::required_capacity_bytes(nz(1), false).unwrap()
        + RuntimeRetainedResult::owner_overhead_bytes() as usize
        + RuntimeRetainedResult::handle_overhead_bytes() as usize
        + std::mem::size_of::<RetainedNumericBatch>();
    let governor = governor(64 * 1024, 8);
    let permit = governor
        .try_admit(RuntimeWorkRequest::foreground_query(0, 64 * 1024))
        .unwrap();
    let (ledger, account) = account(bytes);
    let mut builder =
        RetainedNumericBuilder::new(fragment(), nz(1), false, account, &permit).unwrap();
    builder.push_node(&node(0, Some(Value::Int(3)))).unwrap();
    let batch = builder.seal(None).unwrap();
    assert_eq!(ledger.snapshot().used_bytes, bytes);
    assert_eq!(ledger.snapshot().peak_bytes, bytes);
    let before = governor.retained_result_snapshot();
    assert!(matches!(
        batch.try_retain(0),
        Err(RetainedNumericError::Execution(_))
    ));
    let after = governor.retained_result_snapshot();
    assert_eq!(after.retained_bytes, before.retained_bytes);
    assert_eq!(after.buffer_owners, 1);
    assert_eq!(after.view_handles, 1);
    assert_eq!(int_values(&batch), &[3]);
    drop(batch);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn handle_pressure_is_typed_and_retry_does_not_invalidate_existing_views() {
    let governor = governor(64 * 1024, 2);
    let permit = governor
        .try_admit(RuntimeWorkRequest::foreground_query(0, 64 * 1024))
        .unwrap();
    let (ledger, account) = account(64 * 1024);
    let mut builder =
        RetainedNumericBuilder::new(fragment(), nz(1), false, account, &permit).unwrap();
    builder.push_node(&node(0, Some(Value::Int(3)))).unwrap();
    let batch = builder.seal(None).unwrap();
    let view = batch.try_retain(0).unwrap();
    let before = ledger.snapshot().used_bytes;
    match batch.try_retain(0).unwrap_err() {
        RetainedNumericError::Admission(error) => assert!(error.is_retryable()),
        RetainedNumericError::Execution(_) => panic!("lost admission category"),
    }
    assert_eq!(ledger.snapshot().used_bytes, before);
    drop(view);
    let retry = batch.try_retain(0).unwrap();
    assert_eq!(int_values(&retry).as_ptr(), int_values(&batch).as_ptr());
    drop((retry, batch));
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn producer_errors_cannot_seal_a_partial_result_and_release_all_charges() {
    for corrupt in [false, true] {
        let governor = governor(64 * 1024, 8);
        let permit = governor
            .try_admit(RuntimeWorkRequest::foreground_query(0, 64 * 1024))
            .unwrap();
        let (ledger, account) = account(64 * 1024);
        let mut builder =
            RetainedNumericBuilder::new(fragment(), nz(1), false, account, &permit).unwrap();
        if corrupt {
            let error = builder
                .push_node(&node(0, Some(Value::String("x".repeat(4096)))))
                .unwrap_err();
            assert!(error.to_string().len() < 100);
        } else {
            builder.push_node(&node(0, Some(Value::Int(3)))).unwrap();
            assert!(builder.push_node(&node(1, Some(Value::Int(3)))).is_err());
        }
        assert!(builder.seal(None).is_err());
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    }
}

#[test]
fn fresh_producers_get_distinct_allocation_identities() {
    let governor = governor(64 * 1024, 8);
    let permit = governor
        .try_admit(RuntimeWorkRequest::foreground_query(0, 64 * 1024))
        .unwrap();
    let (_, account) = account(64 * 1024);
    let mut previous = None;
    for _ in 0..8 {
        let builder =
            RetainedNumericBuilder::new(fragment(), nz(1), false, account.clone(), &permit)
                .unwrap();
        let identity = builder.values().1;
        assert_ne!(previous, Some(identity));
        assert_ne!(identity.namespace, 0);
        assert_eq!(identity.generation, 0);
        previous = Some(identity);
    }
}

#[test]
fn empty_all_null_and_integer_extremes_keep_the_declared_type() {
    for values in [
        vec![],
        vec![Value::Null],
        vec![Value::Int(i64::MIN), Value::Int(i64::MAX)],
    ] {
        let governor = governor(64 * 1024, 8);
        let permit = governor
            .try_admit(RuntimeWorkRequest::foreground_query(0, 64 * 1024))
            .unwrap();
        let (ledger, account) = account(64 * 1024);
        let mut builder = RetainedNumericBuilder::new(
            fragment(),
            nz(values.len().max(1)),
            false,
            account,
            &permit,
        )
        .unwrap();
        for (row, value) in values.iter().enumerate() {
            builder
                .push_node(&node(row as u64, Some(value.clone())))
                .unwrap();
        }
        let batch = builder.seal(None).unwrap();
        assert_eq!(int_values(&batch).len(), values.len());
        for (row, value) in values.iter().enumerate() {
            if let Value::Int(expected) = value {
                assert_eq!(int_values(&batch)[row], *expected);
            } else {
                assert!(!batch.validity().is_valid(row));
            }
        }
        drop(batch);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn aggregate_byte_pressure_refuses_before_production_and_release_allows_retry() {
    let bytes = RetainedNumericBuilder::required_capacity_bytes(nz(2), false).unwrap() as u64
        + RuntimeRetainedResult::owner_overhead_bytes()
        + RuntimeRetainedResult::handle_overhead_bytes()
        + std::mem::size_of::<RetainedNumericBatch>() as u64;
    let governor = governor(bytes, 8);
    let permit = governor
        .try_admit(RuntimeWorkRequest::foreground_query(0, bytes))
        .unwrap();
    let (ledger, account) = account(bytes as usize * 2);
    let first =
        RetainedNumericBuilder::new(fragment(), nz(2), false, account.clone(), &permit).unwrap();
    let before = ledger.snapshot();
    let error = RetainedNumericBuilder::new(fragment(), nz(2), false, account.clone(), &permit)
        .unwrap_err();
    assert!(matches!(error, RetainedNumericError::Admission(error) if error.is_retryable()));
    assert_eq!(ledger.snapshot(), before);
    drop(first);
    let retry = RetainedNumericBuilder::new(fragment(), nz(2), false, account, &permit).unwrap();
    assert_eq!(governor.retained_result_snapshot().retained_bytes, bytes);
    drop(retry);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn unsupported_type_and_size_overflow_do_not_charge_the_owner() {
    let governor = governor(64 * 1024, 8);
    let permit = governor
        .try_admit(RuntimeWorkRequest::foreground_query(0, 64 * 1024))
        .unwrap();
    let (ledger, account) = account(64 * 1024);
    let unsupported = NumericFragment {
        property_type: PropertyType::String,
        ..fragment()
    };
    assert!(
        RetainedNumericBuilder::new(unsupported, nz(1), false, account.clone(), &permit).is_err()
    );
    assert!(
        RetainedNumericBuilder::new(fragment(), nz(usize::MAX), false, account, &permit).is_err()
    );
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
}
