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
use crate::ExecutionMemoryConfig;
use hawdb_core::Value;
use std::collections::BTreeMap;

mod borrowed;
mod differential;

#[test]
fn result_refusals_preserve_the_effective_budget_origin() {
    use crate::memory::enforced_result_memory_budget_with_option;
    use hawdb_core::{ResourceBudgetScope, RuntimeMemoryReservation};

    let memory = ExecutionMemoryConfig {
        query_memory_bytes: NonZeroUsize::new(16).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    for (grant, expected, origin) in [
        (None, 16, "ExecutionMemoryConfig.query_memory_bytes"),
        (
            Some(8),
            8,
            "RuntimeTaskContext.memory_reservation.result_bytes",
        ),
        (
            Some(16),
            16,
            "RuntimeTaskContext.memory_reservation.result_bytes",
        ),
        (Some(32), 16, "ExecutionMemoryConfig.query_memory_bytes"),
    ] {
        let context = grant.map(|bytes| {
            RuntimeTaskContext::default()
                .with_memory_reservation(RuntimeMemoryReservation::new(1024, bytes))
        });
        let ledger = QueryMemoryLedger::for_query(&memory, context.as_ref()).unwrap();
        let (budget, option) =
            enforced_result_memory_budget_with_option(&memory, context.as_ref()).unwrap();
        let mut calls = 0;
        let mut consumer = |_| {
            calls += 1;
            Ok(())
        };
        let mut output = QueryOutputAccumulator::new_with_budget_option(
            OutputLimits::default(),
            budget,
            &ledger,
            option,
            ConsumerMemoryMode::Retained,
            &mut consumer,
        )
        .unwrap();
        let streamed = output
            .emit(binding("larger than the result allowance"))
            .unwrap_err();
        drop(output);
        let owned = match QueryOutputAccumulator::new_owned_with_budget_option(
            OutputLimits::default(),
            budget,
            &ledger,
            option,
        ) {
            Err(error) => error,
            Ok(_) => panic!("expected owned-result admission to fail"),
        };
        for error in [streamed, owned] {
            let HawDBError::ResourceBudgetExceeded(cause) = error else {
                panic!("expected a typed result-memory refusal, got {error:?}");
            };
            assert_eq!(cause.scope, ResourceBudgetScope::Account);
            assert_eq!(cause.owner, "query result");
            assert_eq!(cause.memory_class, Some("result_materialization"));
            assert_eq!(cause.budget, expected);
            assert_eq!(cause.budget_option, origin);
        }
        assert_eq!(calls, 0);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

fn binding(value: &str) -> Binding {
    Binding::values(BTreeMap::from([(
        "value".to_string(),
        Value::String(value.to_string()),
    )]))
}

#[test]
fn stream_delivery_selects_consumer_memory_mode() {
    assert!(matches!(
        StreamDelivery::Validated.consumer_memory_mode(true),
        ConsumerMemoryMode::DeferredUntilValidated
    ));
    assert!(matches!(
        StreamDelivery::Validated.consumer_memory_mode(false),
        ConsumerMemoryMode::ReleasedAfterCall
    ));
    assert!(matches!(
        StreamDelivery::Incremental.consumer_memory_mode(true),
        ConsumerMemoryMode::ReleasedAfterCall
    ));
    assert!(matches!(
        StreamDelivery::Incremental.consumer_memory_mode(false),
        ConsumerMemoryMode::ReleasedAfterCall
    ));
}

#[test]
fn released_consumer_memory_is_not_retained_between_rows() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let limits = OutputLimits::default();
    let mut rows = 0usize;
    let mut consumer = |_| {
        rows = rows.saturating_add(1);
        Ok(())
    };
    let mut output = QueryOutputAccumulator::new(
        limits,
        memory.query_memory_bytes,
        &ledger,
        ConsumerMemoryMode::ReleasedAfterCall,
        &mut consumer,
    )
    .unwrap();

    output.emit(binding("first")).unwrap();
    output.emit(binding("second")).unwrap();

    assert_eq!(output.metrics().rows, 2);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    drop(output);
    assert_eq!(rows, 2);
}

#[test]
fn retained_consumer_memory_lives_until_accumulator_drop() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let limits = OutputLimits::default();
    let mut consumer = |_| Ok(());
    let mut output = QueryOutputAccumulator::new(
        limits,
        memory.query_memory_bytes,
        &ledger,
        ConsumerMemoryMode::Retained,
        &mut consumer,
    )
    .unwrap();

    output.emit(binding("retained")).unwrap();
    assert!(ledger.snapshot().used_bytes > 0);

    drop(output);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn output_limits_stop_before_calling_the_consumer() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let limits = OutputLimits {
        max_rows: Some(1),
        max_payload_bytes: None,
    };
    let mut rows = 0usize;
    let mut consumer = |_| {
        rows = rows.saturating_add(1);
        Ok(())
    };
    let mut output = QueryOutputAccumulator::new(
        limits,
        memory.query_memory_bytes,
        &ledger,
        ConsumerMemoryMode::DeferredUntilValidated,
        &mut consumer,
    )
    .unwrap();

    output.emit(binding("first")).unwrap();
    let error = output.emit(binding("second")).unwrap_err();

    assert!(error.to_string().contains("max_read_result_rows 1"));
    assert!(
        matches!(
            &error,
            HawDBError::ReadBudgetExceeded(cause)
                if cause.resource == hawdb_core::ReadBudgetResource::Rows && cause.limit == 1
        ),
        "row rejection lost its typed budget cause: {error}"
    );
    assert_eq!(output.metrics().rows, 1);
    drop(output);
    assert_eq!(rows, 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn bounded_consumer_flushes_only_after_validation() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let limits = OutputLimits {
        max_rows: Some(2),
        max_payload_bytes: None,
    };
    let mut rows = Vec::new();
    let mut consumer = |row| {
        rows.push(row);
        Ok(())
    };
    let mut output = QueryOutputAccumulator::new(
        limits,
        memory.query_memory_bytes,
        &ledger,
        ConsumerMemoryMode::DeferredUntilValidated,
        &mut consumer,
    )
    .unwrap();

    output.emit(binding("first")).unwrap();
    output.emit(binding("second")).unwrap();
    assert!(ledger.snapshot().used_bytes > 0);
    output.finish_delivery(None).unwrap();
    assert_eq!(ledger.snapshot().used_bytes, 0);
    drop(output);

    assert_eq!(rows.len(), 2);
}

#[test]
fn payload_limit_discards_rows_deferred_across_calls() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let first = binding("first");
    let first_payload_bytes = map_payload_bytes(&first.values);
    let limits = OutputLimits {
        max_rows: None,
        max_payload_bytes: Some(first_payload_bytes),
    };
    let mut rows = 0usize;
    let mut consumer = |_| {
        rows = rows.saturating_add(1);
        Ok(())
    };
    let mut output = QueryOutputAccumulator::new(
        limits,
        memory.query_memory_bytes,
        &ledger,
        ConsumerMemoryMode::DeferredUntilValidated,
        &mut consumer,
    )
    .unwrap();

    output.emit(first).unwrap();
    let error = output.emit(binding("second")).unwrap_err();

    assert!(error.to_string().contains(&format!(
        "max_read_result_payload_bytes {first_payload_bytes}"
    )));
    assert!(
        matches!(
            &error,
            HawDBError::ReadBudgetExceeded(cause)
                if cause.resource == hawdb_core::ReadBudgetResource::PayloadBytes
                    && cause.limit == first_payload_bytes
        ),
        "payload rejection lost its typed budget cause: {error}"
    );
    drop(output);
    assert_eq!(rows, 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn payload_limit_failure_releases_transient_result_memory() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let limits = OutputLimits {
        max_rows: None,
        max_payload_bytes: Some(0),
    };
    let mut rows = 0usize;
    let mut consumer = |_| {
        rows = rows.saturating_add(1);
        Ok(())
    };
    let mut output = QueryOutputAccumulator::new(
        limits,
        memory.query_memory_bytes,
        &ledger,
        ConsumerMemoryMode::ReleasedAfterCall,
        &mut consumer,
    )
    .unwrap();

    let error = output.emit(binding("too-large")).unwrap_err();

    assert!(error
        .to_string()
        .contains("max_read_result_payload_bytes 0"));
    assert!(
        matches!(
            &error,
            HawDBError::ReadBudgetExceeded(cause)
                if cause.resource == hawdb_core::ReadBudgetResource::PayloadBytes && cause.limit == 0
        ),
        "payload rejection lost its typed budget cause: {error}"
    );
    assert_eq!(output.metrics().rows, 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    drop(output);
    assert_eq!(rows, 0);
}

#[test]
fn compact_owned_results_admit_scalar_string_fixture_under_16_mib() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let mut output = QueryOutputAccumulator::new_owned(
        OutputLimits::default(),
        memory.batch_payload_bytes,
        &ledger,
    )
    .unwrap();
    for id in 0..100_000 {
        output
            .emit(Binding::values(Row::from([
                ("id".into(), Value::Int(id)),
                ("score".into(), Value::Float(id as f64)),
                ("title".into(), Value::String("x".repeat(24))),
            ])))
            .unwrap();
    }
    assert_eq!(output.metrics().rows, 100_000);
    assert!(ledger.snapshot().used_bytes < 13 * 1024 * 1024);
    assert!(ledger.snapshot().peak_bytes <= memory.batch_payload_bytes.get());
    let rows = output.take_owned_rows().unwrap();
    assert_eq!(rows.len(), 100_000);
    assert_eq!(
        rows.get(99_999, "id"),
        Some(hawdb_core::ValueRef::Int(99_999))
    );
    assert_eq!(
        rows.get(99_999, "title").unwrap().as_str(),
        Some("xxxxxxxxxxxxxxxxxxxxxxxx")
    );
    drop(output);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn compact_owned_results_admit_small_map_nodes_before_publication() {
    let mut emptied = Row::from([("entry".into(), Value::Null)]);
    emptied.remove("entry");
    let cases = [
        Value::Map(emptied),
        Value::Map(Row::from([("entry".into(), Value::Null)])),
        Value::List(vec![Value::Map(Row::from([("entry".into(), Value::Null)]))]),
    ];
    for value in cases {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(8192).unwrap());
        let mut output = QueryOutputAccumulator::new_owned(
            OutputLimits::default(),
            NonZeroUsize::new(1024).unwrap(),
            &ledger,
        )
        .unwrap();
        let error = output
            .emit(Binding::values(Row::from([("value".into(), value)])))
            .unwrap_err();
        assert!(error.to_string().contains("result_materialization"));
        assert_eq!(output.metrics().rows, 0);
        drop(output);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn compact_owned_results_preserve_sparse_multi_level_maps() {
    let mut values = Row::new();
    for index in 0..1000 {
        values.insert(format!("key-{index:04}"), Value::Int(index));
    }
    for index in 0..990 {
        values.remove(&format!("key-{index:04}"));
    }
    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(1024 * 1024).unwrap());
    let mut output = QueryOutputAccumulator::new_owned(
        OutputLimits::default(),
        NonZeroUsize::new(64 * 1024).unwrap(),
        &ledger,
    )
    .unwrap();
    output
        .emit(Binding::values(Row::from([(
            "value".into(),
            Value::Map(values),
        )])))
        .unwrap();
    let rows = output.take_owned_rows().unwrap();
    let Some(hawdb_core::ValueRef::Map(values)) = rows.get(0, "value") else {
        panic!("expected a retained map");
    };
    assert_eq!(values.len(), 10);
    assert_eq!(values.get("key-0990"), Some(&Value::Int(990)));
    assert_eq!(values.get("key-0999"), Some(&Value::Int(999)));
    assert!(!values.contains_key("key-0000"));
    drop(output);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn compact_owned_results_charge_spare_nested_payload_capacity() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let budget = NonZeroUsize::new(4096).unwrap();
    let mut output =
        QueryOutputAccumulator::new_owned(OutputLimits::default(), budget, &ledger).unwrap();
    let mut payload = String::with_capacity(16 * 1024);
    payload.push('x');
    let error = output
        .emit(Binding::values(Row::from([(
            "value".into(),
            Value::List(vec![Value::Map(Row::from([(
                "nested".into(),
                Value::String(payload),
            )]))]),
        )])))
        .unwrap_err();
    assert!(error.to_string().contains("result_materialization"));
    assert_eq!(output.metrics().rows, 0);
    assert!(ledger.snapshot().peak_bytes <= budget.get());
    drop(output);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn compact_owned_results_roll_back_failed_segment_boundary_payload() {
    let scalar = |id| Binding::values(Row::from([("value".into(), Value::Int(id))]));
    let control_ledger =
        QueryMemoryLedger::new(ExecutionMemoryConfig::default().query_memory_bytes);
    let mut control = QueryOutputAccumulator::new_owned(
        OutputLimits::default(),
        ExecutionMemoryConfig::default().batch_payload_bytes,
        &control_ledger,
    )
    .unwrap();
    for id in 0..257 {
        control.emit(scalar(id)).unwrap();
    }
    // Admit the same scalar path's growth overlap on every supported ABI.
    let budget = NonZeroUsize::new(control_ledger.snapshot().peak_bytes).unwrap();
    drop(control);
    assert_eq!(control_ledger.snapshot().used_bytes, 0);
    let ledger = QueryMemoryLedger::new(budget);
    let mut output =
        QueryOutputAccumulator::new_owned(OutputLimits::default(), budget, &ledger).unwrap();
    for id in 0..256 {
        output.emit(scalar(id)).unwrap();
    }
    let headroom = budget.get() - ledger.snapshot().used_bytes;
    // Incoming and retained payload overlap, in addition to segment metadata.
    let payload_capacity = headroom / 2;
    assert!(payload_capacity > 0);
    let mut failed_used_bytes = None;
    for _ in 0..2 {
        let error = output
            .emit(Binding::values(Row::from([(
                "value".into(),
                Value::Binary(Vec::with_capacity(payload_capacity)),
            )])))
            .unwrap_err();
        assert!(error.to_string().contains("result_materialization"));
        assert_eq!(output.metrics().rows, 256);
        let used_bytes = ledger.snapshot().used_bytes;
        if let Some(previous) = failed_used_bytes {
            assert_eq!(used_bytes, previous);
        }
        failed_used_bytes = Some(used_bytes);
        assert!(ledger.snapshot().peak_bytes <= budget.get());
    }
    output.emit(scalar(256)).unwrap();
    let rows = output.take_owned_rows().unwrap();
    assert_eq!(rows.len(), 257);
    for ordinal in 0..257 {
        assert_eq!(
            rows.get(ordinal, "value"),
            Some(hawdb_core::ValueRef::Int(ordinal as i64))
        );
    }
    drop(output);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn compact_owned_results_move_payload_without_cloning_it() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let mut output = QueryOutputAccumulator::new_owned(
        OutputLimits {
            max_rows: Some(1),
            max_payload_bytes: None,
        },
        memory.batch_payload_bytes,
        &ledger,
    )
    .unwrap();
    let value = String::from("retained payload");
    let pointer = value.as_ptr();
    output
        .emit(Binding::values(Row::from([(
            "value".into(),
            Value::String(value),
        )])))
        .unwrap();
    assert!(output.emit(binding("excess row")).is_err());
    assert_eq!(output.metrics().rows, 1);
    let rows = output.take_owned_rows().unwrap();
    assert_eq!(
        rows.get(0, "value").unwrap().as_str().unwrap().as_ptr(),
        pointer
    );
}

#[test]
fn compact_owned_results_charge_incoming_column_key_capacity() {
    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(4096).unwrap());
    let mut output = QueryOutputAccumulator::new_owned(
        OutputLimits::default(),
        NonZeroUsize::new(4096).unwrap(),
        &ledger,
    )
    .unwrap();
    let mut key = String::with_capacity(16 * 1024);
    key.push('x');
    assert!(output
        .emit(Binding::values(Row::from([(key, Value::Int(1))])))
        .is_err());
    assert_eq!(output.metrics().rows, 0);
    drop(output);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn declared_owned_schema_preserves_empty_rows_and_reorders_named_values() {
    let memory = ExecutionMemoryConfig::default();
    for populated in [false, true] {
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let mut output = QueryOutputAccumulator::new_owned(
            OutputLimits::default(),
            memory.query_memory_bytes,
            &ledger,
        )
        .unwrap();
        output.bind_output_schema(["z", "a"].into_iter()).unwrap();
        if populated {
            output
                .emit(Binding::values(Row::from([
                    ("a".into(), Value::String("first alphabetically".into())),
                    ("z".into(), Value::Int(7)),
                ])))
                .unwrap();
        }
        let rows = output.take_owned_rows().unwrap();
        assert_eq!(rows.schema().columns(), ["z", "a"]);
        assert_eq!(rows.len(), usize::from(populated));
        if populated {
            assert_eq!(rows.value(0, 0), Some(hawdb_core::ValueRef::Int(7)));
            assert_eq!(
                rows.value(0, 1),
                Some(hawdb_core::ValueRef::String("first alphabetically"))
            );
        }
        assert!(ledger.snapshot().used_bytes > 0);
        drop(output);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn empty_result_schema_is_admitted_before_allocating_and_releases_on_refusal() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let mut output = QueryOutputAccumulator::new_owned(
        OutputLimits::default(),
        NonZeroUsize::new(1024).unwrap(),
        &ledger,
    )
    .unwrap();
    let name = "column".repeat(1024);
    let error = output
        .bind_output_schema(std::iter::once(name.as_str()))
        .unwrap_err();
    assert!(
        matches!(error, HawDBError::ResourceBudgetExceeded(_)),
        "{error}"
    );
    assert_eq!(output.metrics().rows, 0);
    drop(output);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn declared_borrowed_schema_reorders_values_and_rejects_schema_drift_atomically() {
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let mut output = QueryOutputAccumulator::new_owned(
        OutputLimits::default(),
        memory.query_memory_bytes,
        &ledger,
    )
    .unwrap();
    output.bind_output_schema(["z", "a"].into_iter()).unwrap();
    let row = Row::from([("a".into(), Value::Int(1)), ("z".into(), Value::Int(2))]);
    output.emit_borrowed(RowRef::Map(&row)).unwrap();
    let wrong = Row::from([("a".into(), Value::Int(3)), ("other".into(), Value::Int(4))]);
    assert!(output.emit_borrowed(RowRef::Map(&wrong)).is_err());
    assert_eq!(output.metrics().rows, 1);
    let rows = output.take_owned_rows().unwrap();
    assert_eq!(rows.value(0, 0), Some(hawdb_core::ValueRef::Int(2)));
    assert_eq!(rows.value(0, 1), Some(hawdb_core::ValueRef::Int(1)));
}

#[test]
fn declared_schema_accepts_changing_input_orders_across_segments() {
    use crate::{QueryRowsBuilder, QuerySchema};
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let mut output = QueryOutputAccumulator::new_owned(
        OutputLimits::default(),
        memory.query_memory_bytes,
        &ledger,
    )
    .unwrap();
    output
        .bind_output_schema(["z", "a", "m"].into_iter())
        .unwrap();
    for index in 0..513 {
        let source = Row::from([
            ("a".into(), Value::Int(index)),
            ("m".into(), Value::Int(index + 1)),
            ("z".into(), Value::Int(index + 2)),
        ]);
        match index % 4 {
            0 => output.emit(Binding::values(source)).unwrap(),
            1 => output.emit_borrowed(RowRef::Map(&source)).unwrap(),
            _ => {
                let columns = if index % 4 == 2 {
                    ["m", "z", "a"]
                } else {
                    ["z", "a", "m"]
                };
                let mut row = QueryRowsBuilder::with_schema(
                    QuerySchema::try_new(columns.map(str::to_owned)).unwrap(),
                    1,
                );
                row.push_values(columns.map(|name| source[name].clone()))
                    .unwrap();
                let rows = row.finish();
                output
                    .emit_borrowed(RowRef::Compact(rows.row(0).unwrap()))
                    .unwrap();
            }
        }
    }
    let rows = output.take_owned_rows().unwrap();
    assert_eq!(rows.len(), 513);
    for index in 0..513 {
        assert_eq!(
            rows.value(index, 0),
            Some(hawdb_core::ValueRef::Int(index as i64 + 2))
        );
        assert_eq!(
            rows.value(index, 1),
            Some(hawdb_core::ValueRef::Int(index as i64))
        );
        assert_eq!(
            rows.value(index, 2),
            Some(hawdb_core::ValueRef::Int(index as i64 + 1))
        );
    }
    drop(output);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}
