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
use crate::background::{CheckpointOperationError, CheckpointWorkError};
use hawdb_core::RuntimeMemoryError;
use hawdb_qos::{QosTelemetryEvent, QosTelemetrySink};
use std::sync::atomic::AtomicU64;

#[derive(Debug)]
struct MemoryProbe {
    task: RuntimeTaskContext,
    ceiling: u64,
    peak: AtomicU64,
}

impl MemoryProbe {
    fn used(&self) -> u64 {
        match self.task.reserve_working_memory(self.ceiling) {
            Err(RuntimeMemoryError::ReservationExceeded {
                available_bytes, ..
            }) => self.ceiling - available_bytes,
            other => panic!("expected the real governor's available working capacity: {other:?}"),
        }
    }
}

impl QosTelemetrySink for MemoryProbe {
    fn record_qos(&self, _: QosTelemetryEvent) {
        self.peak.fetch_max(self.used(), AtomicOrdering::SeqCst);
    }
}

#[test]
fn checkpoint_units_append_capture_memory_denial_is_the_complete_first_row_array_request() {
    let state = live_state(5127);
    let governor = governor(1);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let expected = work_error(
        work.reserve_memory(5127 * std::mem::size_of::<AppendTableRow>())
            .unwrap_err(),
    );
    assert_eq!(
        state
            .checkpoint_rows_with_work_context(5127, &work)
            .unwrap_err(),
        expected
    );
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_append_capture_memory_charges_sort_overlap_and_retries_same_reservation() {
    let state = live_state(5127);
    let expected = state.checkpoint_rows(5127).unwrap();
    let ceiling = 16 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let probe = Arc::new(MemoryProbe {
        task: task.clone(),
        ceiling,
        peak: AtomicU64::new(0),
    });
    let local = scheduler();
    local.set_telemetry_sink(Some(probe.clone()));
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let captured = state
        .checkpoint_rows_with_work_context(5127, &work)
        .unwrap();
    assert_eq!(captured, expected);
    let retained = probe.used();
    let peak = probe.peak.load(AtomicOrdering::SeqCst);
    let array_bytes = (5127 * std::mem::size_of::<AppendTableRow>()) as u64;
    assert!(retained >= array_bytes);
    assert!(
        peak >= retained + 2 * array_bytes,
        "input, run and output arrays must overlap admission: peak={peak}, retained={retained}, array_bytes={array_bytes}"
    );
    drop(captured);
    assert_eq!(probe.used(), 0);
    assert!(peak < ceiling);

    // The complete measured peak fails to fit while another real allocation
    // is held. All temporary capacity must return before the same task retries.
    let held = task.reserve_working_memory(ceiling - peak).unwrap();
    let held_usage = probe.used();
    assert!(matches!(
        work.classify(|work| state.checkpoint_rows_with_work_context(5127, work)),
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { .. }
        )))
    ));
    assert_eq!(probe.used(), held_usage);
    assert_eq!(state.checkpoint_rows(5127).unwrap(), expected);
    drop(held);
    assert_eq!(probe.used(), 0);
    let retry = state
        .checkpoint_rows_with_work_context(5127, &work)
        .unwrap();
    assert_eq!(retry, expected);
    local.set_telemetry_sink(None);
    drop(work);
    drop(task);
    drop(probe);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    assert_eq!(retry, expected);
    drop(retry);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

fn wide_state() -> AppendState {
    let text_schema = schema("text");
    let mut binary_schema = schema("binary");
    binary_schema.columns[0].scalar_type = RelationalScalarType::Bytea;
    let mut wide_schema = schema("wide");
    wide_schema.columns = (0..1057)
        .map(|index| RelationalColumnSchema {
            name: format!("c{index:04}"),
            scalar_type: RelationalScalarType::BigInt,
            nullable: false,
            default: None,
        })
        .chain(std::iter::once(RelationalColumnSchema {
            name: "sequence".into(),
            scalar_type: RelationalScalarType::BigInt,
            nullable: false,
            default: None,
        }))
        .collect();
    wide_schema.partition_key = (0..1057).map(|index| format!("c{index:04}")).collect();
    let state = AppendState::default()
        .stage_transaction(
            &AppendTransaction {
                writes: [text_schema, binary_schema, wide_schema]
                    .into_iter()
                    .map(|schema| AppendWrite::CreateTable { schema })
                    .collect(),
            },
            AppendMutationLimits::default(),
        )
        .unwrap();
    state
        .stage_transaction(
            &AppendTransaction {
                writes: vec![
                    AppendWrite::Append {
                        table: "text".into(),
                        rows: vec![RelationalRow::new(vec![
                            RelationalValue::Text(format!(
                                "{}🦀{}",
                                "x".repeat(64 * 1024 - 2),
                                "界".repeat(32 * 1024 + 1)
                            )),
                            RelationalValue::BigInt(1),
                            RelationalValue::Bytea(vec![0x5a; 17]),
                        ])],
                    },
                    AppendWrite::Append {
                        table: "binary".into(),
                        rows: vec![RelationalRow::new(vec![
                            RelationalValue::Bytea(vec![0x9b; 2 * 64 * 1024 + 7]),
                            RelationalValue::BigInt(1),
                            RelationalValue::Bytea(vec![0x6b; 17]),
                        ])],
                    },
                    AppendWrite::Append {
                        table: "wide".into(),
                        rows: vec![RelationalRow::new(
                            (0..1057)
                                .map(RelationalValue::BigInt)
                                .chain(std::iter::once(RelationalValue::BigInt(1)))
                                .collect(),
                        )],
                    },
                ],
            },
            AppendMutationLimits::default(),
        )
        .unwrap()
}

#[test]
fn checkpoint_units_append_capture_memory_wide_keys_cancel_every_cpu_unit_and_retry() {
    let state = wide_state();
    let expected = state.checkpoint_rows(3).unwrap();
    assert_eq!(expected.len(), 3);
    let local = scheduler();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    let complete = state
        .checkpoint_rows_with_work_context(3, &baseline.context(local.clone()))
        .unwrap();
    assert_eq!(complete, expected);
    let units = baseline.completed.load(AtomicOrdering::SeqCst);
    assert!(units > 2 * 1057);
    baseline.assert_released(&local);
    drop(complete);
    let ceiling = 2 * 1024 * 1024;
    for stop in 1..=units {
        let governor = governor(ceiling);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(stop, AtomicOrdering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let work = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        assert_stopped(state.checkpoint_rows_with_work_context(3, &work));
        assert_eq!(probe.completed.load(AtomicOrdering::SeqCst), stop);
        probe.assert_released(&local);
        assert_eq!(probe.peak_units.load(AtomicOrdering::SeqCst), 1);
        drop(work);
        let fresh = permit.bind_task_context(RuntimeTaskContext::default());
        assert!(matches!(
            fresh.reserve_working_memory(ceiling),
            Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                if available_bytes == ceiling
        ));
        let retry_probe = Arc::new(CheckpointWorkProbe::default());
        local.set_telemetry_sink(Some(retry_probe.clone()));
        let work = CheckpointWorkContext::new(fresh.clone()).with_scheduler(local.clone());
        let retry = state.checkpoint_rows_with_work_context(3, &work).unwrap();
        assert_eq!(retry, expected);
        retry_probe.assert_released(&local);
        drop(retry);
        assert!(matches!(
            fresh.reserve_working_memory(ceiling),
            Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                if available_bytes == ceiling
        ));
        assert_eq!(state.checkpoint_rows(3).unwrap(), expected);
        local.set_telemetry_sink(None);
        drop(work);
        drop(fresh);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}
