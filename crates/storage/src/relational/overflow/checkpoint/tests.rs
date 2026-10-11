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
use crate::background::CheckpointWorkProbe;
use crate::relational::{
    ImmutableRelationalRowPage, RelationalColumnSchema, RelationalInsertMode,
    RelationalMutationLimits, RelationalRowPageEntry, RelationalRowPageId, RelationalStore,
    RelationalTransaction, RelationalWrite,
};
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler, WorkClass, WorkRequest};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_FILE: AtomicU64 = AtomicU64::new(1);

fn input_governor(bytes: u64) -> hawdb_qos::RuntimeGovernor {
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
        RuntimeResourceBudget, RuntimeResourceSnapshot,
    };
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(bytes),
            background_task_limit: Some(std::num::NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(std::num::NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    )
}

#[derive(Debug)]
struct ReferenceMemoryProbe {
    task: hawdb_core::RuntimeTaskContext,
    ceiling: u64,
    peak: AtomicU64,
}

impl ReferenceMemoryProbe {
    fn used(&self) -> u64 {
        match self.task.reserve_working_memory(self.ceiling) {
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded {
                available_bytes, ..
            }) => self.ceiling - available_bytes,
            other => panic!("expected shared governor allocation usage: {other:?}"),
        }
    }
}

impl hawdb_qos::QosTelemetrySink for ReferenceMemoryProbe {
    fn record_qos(&self, _: hawdb_qos::QosTelemetryEvent) {
        self.peak.fetch_max(self.used(), Ordering::SeqCst);
    }
}

#[test]
fn checkpoint_units_reference_capacity_charges_live_reference_work_beside_all_inputs() {
    let state = source(3);
    let delta = deltas(&state);
    let sparse = sparse(&state);
    for (state, delta) in [(&state, None), (&sparse, Some(delta.as_slice()))] {
        let expected = collect(state, delta, None).unwrap();
        let ceiling = 64 * 1024;
        let governor = input_governor(ceiling);
        let permit = governor
            .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(
                ceiling,
            ))
            .unwrap();
        let task = permit.bind_task_context(hawdb_core::RuntimeTaskContext::default());
        let probe = Arc::new(ReferenceMemoryProbe {
            task: task.clone(),
            ceiling,
            peak: AtomicU64::new(0),
        });
        let local = scheduler();
        local.set_telemetry_sink(Some(probe.clone()));
        let work = CheckpointWorkContext::new(task).with_scheduler(local.clone());
        let actual = collect(state, delta, Some(&work)).unwrap();
        assert_eq!(actual, expected);
        let retained = probe.used();
        assert!(retained > 0);
        // Every distinct reference must remain available while its complete
        // input list is allocated, even if duplicated row values share bytes.
        let reference_floor =
            (expected.len() * std::mem::size_of::<RelationalOverflowRef>()) as u64;
        assert!(
            probe.peak.load(Ordering::SeqCst) >= retained + reference_floor,
            "reference working memory disappeared from admission: peak={}, retained={retained}, reference_floor={reference_floor}",
            probe.peak.load(Ordering::SeqCst)
        );
        assert_eq!(collect(state, delta, None).unwrap(), expected);
        drop(actual);
        assert_eq!(probe.used(), 0);
        local.set_telemetry_sink(None);
        drop(work);
        drop(probe);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn checkpoint_units_reference_capacity_denies_when_only_the_retained_inputs_fit() {
    use crate::background::CheckpointOperationError;
    let state = source(3);
    let delta = deltas(&state);
    let sparse = sparse(&state);
    for (state, delta) in [(&state, None), (&sparse, Some(delta.as_slice()))] {
        let expected = collect(state, delta, None).unwrap();
        let ceiling = 64 * 1024;
        let governor = input_governor(ceiling);
        let permit = governor
            .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(
                ceiling,
            ))
            .unwrap();
        let task = permit.bind_task_context(hawdb_core::RuntimeTaskContext::default());
        let work = CheckpointWorkContext::new(task.clone());
        let output = collect(state, delta, Some(&work)).unwrap();
        let retained = match task.reserve_working_memory(ceiling) {
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded {
                available_bytes, ..
            }) => ceiling - available_bytes,
            other => panic!("expected retained capacity: {other:?}"),
        };
        drop(output);
        drop(work);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);

        let reference_floor =
            (expected.len() * std::mem::size_of::<RelationalOverflowRef>()) as u64;
        let limited = retained + reference_floor - 1;
        let governor = input_governor(limited);
        let permit = governor
            .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(
                limited,
            ))
            .unwrap();
        let task = permit.bind_task_context(hawdb_core::RuntimeTaskContext::default());
        let work = CheckpointWorkContext::new(task);
        assert!(matches!(
            work.classify(|work| collect(state, delta, Some(work))),
            Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                hawdb_core::RuntimeMemoryError::ReservationExceeded { .. }
            )))
        ));
        assert_eq!(collect(state, delta, None).unwrap(), expected);
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn checkpoint_units_reference_capacity_releases_every_cancelled_unit_and_fully_retries() {
    let state = source(4);
    let delta = deltas(&state);
    let sparse = sparse(&state);
    for (state, delta) in [(&state, None), (&sparse, Some(delta.as_slice()))] {
        let expected = collect(state, delta, None).unwrap();
        let local = scheduler();
        let baseline = Arc::new(CheckpointWorkProbe::default());
        let output = collect(state, delta, Some(&baseline.context(local.clone()))).unwrap();
        assert_eq!(output, expected);
        let units = baseline.completed.load(Ordering::SeqCst);
        assert!(units > 20);
        drop(output);
        baseline.assert_released(&local);
        for stop in 1..=units {
            let ceiling = 64 * 1024;
            let governor = input_governor(ceiling);
            let permit = governor
                .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(
                    ceiling,
                ))
                .unwrap();
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(stop, Ordering::SeqCst);
            local.set_telemetry_sink(Some(probe.clone()));
            let task = permit.bind_task_context(hawdb_core::RuntimeTaskContext::without_deadline(
                probe.cancellation.clone(),
            ));
            let work = CheckpointWorkContext::new(task).with_scheduler(local.clone());
            let error = collect(state, delta, Some(&work)).unwrap_err();
            assert!(error.to_string().contains("stopped"), "{error:?}");
            probe.assert_released(&local);
            assert_eq!(collect(state, delta, None).unwrap(), expected);
            drop(work);

            // Reuse the same real admitted reservation with a fresh execution
            // token; leaked scratch would reduce the observed available bytes.
            let fresh = permit.bind_task_context(hawdb_core::RuntimeTaskContext::default());
            assert!(matches!(
                fresh.reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded {
                    available_bytes, ..
                }) if available_bytes == ceiling
            ));
            let retry = Arc::new(CheckpointWorkProbe::default());
            local.set_telemetry_sink(Some(retry.clone()));
            let retry_work =
                CheckpointWorkContext::new(fresh.clone()).with_scheduler(local.clone());
            let output = collect(state, delta, Some(&retry_work)).unwrap();
            assert_eq!(output, expected);
            retry.assert_released(&local);
            drop(output);
            assert!(matches!(
                fresh.reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded {
                    available_bytes, ..
                }) if available_bytes == ceiling
            ));
            assert_eq!(collect(state, delta, None).unwrap(), expected);
            local.set_telemetry_sink(None);
            drop(retry_work);
            drop(fresh);
            drop(permit);
            let closed = governor.snapshot();
            assert_eq!(closed.active_cpu_slots, 0);
            assert_eq!(closed.active_background_tasks, 0);
            assert_eq!(closed.active_background_io_slots, 0);
            assert_eq!(closed.admitted_memory_bytes, 0);
        }
    }
}

#[test]
fn checkpoint_units_input_capacity_retains_the_list_after_task_close_until_last_clone() {
    let state = source(3);
    let expected = state.overflow_generation_inputs(true, 0).unwrap();
    let ceiling = 64 * 1024;
    let governor = input_governor(ceiling);
    let permit = governor
        .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(
            ceiling,
        ))
        .unwrap();
    let work = CheckpointWorkContext::new(
        permit.bind_task_context(hawdb_core::RuntimeTaskContext::default()),
    );
    let actual = state
        .overflow_generation_inputs_with_work_context(true, 0, &work)
        .unwrap();
    assert_eq!(actual, expected);
    let retained = actual.clone();
    drop(permit);
    drop(work);
    let closed = governor.snapshot();
    assert_eq!(closed.active_cpu_slots, 0);
    assert_eq!(closed.active_background_tasks, 0);
    assert_eq!(closed.active_background_io_slots, 0);
    assert_eq!(closed.admitted_memory_bytes, ceiling);
    drop(actual);
    assert_eq!(retained, expected);
    assert_eq!(state.overflow_generation_inputs(true, 0).unwrap(), expected);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    drop(retained);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_input_capacity_denies_before_allocating_a_nonempty_list() {
    use crate::background::{CheckpointOperationError, CheckpointWorkError};
    let state = source(1);
    let expected = state.overflow_generation_inputs(true, 0).unwrap();
    let governor = input_governor(1);
    let permit = governor
        .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(1))
        .unwrap();
    let work = CheckpointWorkContext::new(
        permit.bind_task_context(hawdb_core::RuntimeTaskContext::default()),
    );
    assert!(matches!(
        work.classify(|work| state.overflow_generation_inputs_with_work_context(true, 0, work)),
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            hawdb_core::RuntimeMemoryError::ReservationExceeded { .. }
        )))
    ));
    assert_eq!(state.overflow_generation_inputs(true, 0).unwrap(), expected);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_input_capacity_delta_iterator_retains_all_values_and_closed_ownership() {
    let state = source(3);
    let deltas = deltas(&state);
    let sparse = sparse(&state);
    let expected = sparse.overflow_delta_generation_inputs(&deltas).unwrap();
    let ceiling = 64 * 1024;
    let governor = input_governor(ceiling);
    let permit = governor
        .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(
            ceiling,
        ))
        .unwrap();
    let work = CheckpointWorkContext::new(
        permit.bind_task_context(hawdb_core::RuntimeTaskContext::default()),
    );
    let output = sparse
        .overflow_delta_generation_inputs_with_work_context(&deltas, &work)
        .unwrap();
    assert_eq!(output, expected);
    drop(permit);
    drop(work);
    let retained = output.clone();
    drop(output);
    let mut iterator = retained.into_iter();
    for (index, item) in expected.iter().enumerate() {
        assert_eq!(iterator.len(), expected.len() - index);
        assert_eq!(iterator.next().as_ref(), Some(item));
        assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    }
    assert_eq!(iterator.len(), 0);
    assert!(iterator.next().is_none());
    assert!(iterator.next().is_none());
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    assert_eq!(
        sparse.overflow_delta_generation_inputs(&deltas).unwrap(),
        expected
    );
    drop(iterator);
    let closed = governor.snapshot();
    assert_eq!(closed.active_cpu_slots, 0);
    assert_eq!(closed.active_background_tasks, 0);
    assert_eq!(closed.admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_input_capacity_clone_shares_admission_and_one_byte_overlap_retries() {
    use crate::background::{CheckpointOperationError, CheckpointWorkError};
    use hawdb_core::RuntimeMemoryError;
    let state = source(3);
    let expected = state.overflow_generation_inputs(true, 0).unwrap();
    let ceiling = 64 * 1024;
    let governor = input_governor(ceiling);
    let permit = governor
        .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(
            ceiling,
        ))
        .unwrap();
    let task = permit.bind_task_context(hawdb_core::RuntimeTaskContext::default());
    let probe = Arc::new(ReferenceMemoryProbe {
        task: task.clone(),
        ceiling,
        peak: AtomicU64::new(0),
    });
    let local = scheduler();
    local.set_telemetry_sink(Some(probe.clone()));
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let available = || match task.reserve_working_memory(ceiling) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => available_bytes,
        other => panic!("expected actual shared-controller usage: {other:?}"),
    };
    let first = state
        .overflow_generation_inputs_with_work_context(true, 0, &work)
        .unwrap();
    let remaining = available();
    let used = ceiling - remaining;
    let temporary = probe.peak.load(Ordering::SeqCst) - used;
    assert!(used > 0);
    assert!(temporary > 0);
    let retained = first.clone();
    assert_eq!(available(), remaining);
    drop(first);
    assert_eq!(available(), remaining);
    // Leave exactly one byte less than the measured complete next build's
    // peak, including its simultaneous references and retained inputs. The
    // pinned governor's filler allocation owns a concrete 24-byte lease.
    let filler = task
        .reserve_working_memory(ceiling - 2 * used - temporary + 1 - 24)
        .unwrap();
    assert_eq!(available(), used + temporary - 1);
    assert!(matches!(
        work.classify(|work| state.overflow_generation_inputs_with_work_context(true, 0, work)),
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { requested_bytes, available_bytes }
        ))) if requested_bytes == used && available_bytes == used - 1
    ));
    assert_eq!(retained, expected);
    assert_eq!(state.overflow_generation_inputs(true, 0).unwrap(), expected);
    drop(filler);
    let retry = state
        .overflow_generation_inputs_with_work_context(true, 0, &work)
        .unwrap();
    assert_eq!(retry, expected);
    drop(retry);
    drop(retained);
    local.set_telemetry_sink(None);
    drop(work);
    drop(probe);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    })
}

fn source(rows: usize) -> RelationalState {
    let source = RelationalStore::new(RelationalMutationLimits::default());
    let schema = |name: &str| RelationalTableSchema {
        name: name.into(),
        columns: ["id", "payload", "duplicate", "plain"]
            .map(|name| RelationalColumnSchema {
                name: name.into(),
                scalar_type: RelationalScalarType::Text,
                nullable: false,
                default: None,
            })
            .to_vec(),
        primary_key: vec!["id".into()],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
    };
    source
        .commit(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(schema("empty")),
                    RelationalWrite::CreateTable(schema("messages")),
                ],
            },
            |_, _| Ok(()),
        )
        .unwrap();
    source
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".into(),
                    mode: RelationalInsertMode::Error,
                    rows: (0..rows)
                        .map(|seed| {
                            let value = format!("value-{seed:04}-{}", "界🙂".repeat(800));
                            RelationalRow::new(vec![
                                RelationalValue::Text(format!("id-{seed:04}")),
                                RelationalValue::Text(value.clone()),
                                RelationalValue::Text(value),
                                RelationalValue::Text("inline scalar".into()),
                            ])
                        })
                        .collect(),
                }],
            },
            |_, _| Ok(()),
        )
        .unwrap();
    source.snapshot().unwrap().value().clone()
}

fn deltas(state: &RelationalState) -> Vec<RelationalRowPageTableDelta> {
    let rows = state.segments["messages"]
        .rows
        .iter()
        .map(|(key, row)| RelationalRowPageEntry {
            primary_key: key.clone(),
            row: row.clone(),
        })
        .collect::<Vec<_>>();
    let schema = state.schemas["messages"].as_ref();
    // The collector consumes row metadata, so share the same exact schema
    // identity in each page and table delta.
    let schema_digest = state.table_schema_digest("messages").unwrap().unwrap();
    let pages = rows
        .chunks(512)
        .enumerate()
        .map(|(ordinal, rows)| ImmutableRelationalRowPage {
            generation: 2,
            source_commit_epoch: 2,
            page_id: RelationalRowPageId::new(NonZeroU64::new(ordinal as u64 + 1).unwrap()),
            schema_digest,
            column_count: schema.columns.len(),
            rows: rows.to_vec(),
        })
        .collect::<Vec<_>>();
    vec![
        RelationalRowPageTableDelta {
            table: "empty".into(),
            schema: None,
            schema_digest: state.table_schema_digest("empty").unwrap().unwrap(),
            column_count: NonZeroU32::new(4).unwrap(),
            next_page_id: NonZeroU64::MIN,
            dirty_pages: Vec::new(),
            deleted_page_ids: Vec::new(),
        },
        RelationalRowPageTableDelta {
            table: "messages".into(),
            schema: Some(schema.clone()),
            schema_digest,
            column_count: NonZeroU32::new(4).unwrap(),
            next_page_id: NonZeroU64::new(pages.len() as u64 + 1).unwrap(),
            dirty_pages: pages,
            deleted_page_ids: Vec::new(),
        },
    ]
}

fn sparse(state: &RelationalState) -> RelationalState {
    let mut state = state.clone();
    state.canonical_row_metadata_only = true;
    state.materialized_rows_resident = false;
    state.materialized_index_postings_resident = false;
    state.segments.clear();
    state
}

fn collect(
    state: &RelationalState,
    deltas: Option<&[RelationalRowPageTableDelta]>,
    work: Option<&CheckpointWorkContext>,
) -> Result<crate::relational::RelationalOverflowInputs, RelationalError> {
    match (deltas, work) {
        (None, None) => state.overflow_generation_inputs(true, 0).map(Into::into),
        (None, Some(work)) => state.overflow_generation_inputs_with_work_context(true, 0, work),
        (Some(deltas), None) => state
            .overflow_delta_generation_inputs(deltas)
            .map(Into::into),
        (Some(deltas), Some(work)) => {
            state.overflow_delta_generation_inputs_with_work_context(deltas, work)
        }
    }
}

#[test]
fn checkpoint_units_overflow_input_collection_preserves_all_1025_inputs_and_inline_ownership() {
    let state = source(1025);
    let deltas = deltas(&state);
    let sparse = sparse(&state);
    for (source, delta) in [(&state, None), (&sparse, Some(deltas.as_slice()))] {
        let expected = collect(source, delta, None).unwrap();
        assert_eq!(expected.len(), 1025);
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = collect(source, delta, Some(&probe.context(local.clone()))).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
        for input in actual {
            let RelationalOverflowExtentInput::Write { reference, encoded } = input else {
                unreachable!()
            };
            let RelationalOverflowSegment::Inline(original) =
                &source.overflow_segments[&reference.digest]
            else {
                unreachable!()
            };
            assert!(Arc::ptr_eq(&encoded, original));
            assert_eq!(encoded.as_ref(), original.as_ref());
        }
        probe.assert_released(&local);
    }
}

#[test]
fn checkpoint_units_overflow_file_inputs_retain_memory_through_publication_and_clones() {
    use crate::relational::{
        decode_relational_checkpoint_file, encode_relational_checkpoint, RelationalDecodeLimits,
    };
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
        RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
    };
    let original = source(3);
    let checkpoint = encode_relational_checkpoint(2, &original).unwrap();
    let directory = std::env::temp_dir().join(format!(
        "hawdb-overflow-owned-inputs-{}-{}",
        std::process::id(),
        NEXT_FILE.fetch_add(1, Ordering::Relaxed)
    ));
    crate::file_io::create_dir_all(&directory).unwrap();
    let path = directory.join("source.hawdb");
    crate::file_io::write(&path, &checkpoint).unwrap();
    let state = decode_relational_checkpoint_file(&path, RelationalDecodeLimits::checkpoint())
        .unwrap()
        .state;
    assert_eq!(state.file_backed_overflow_segment_count(), 3);
    let expected = state.overflow_generation_inputs(false, usize::MAX).unwrap();
    let total = expected
        .iter()
        .map(|input| input.encoded_bytes().unwrap().len())
        .sum();
    let ceiling = 2 * 1024 * 1024;
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            background_task_limit: Some(std::num::NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(std::num::NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let work = CheckpointWorkContext::new(
        permit.bind_task_context(hawdb_core::RuntimeTaskContext::default()),
    );
    let actual = state
        .overflow_generation_inputs_with_work_context(false, total, &work)
        .unwrap();
    assert_eq!(actual, expected);
    assert!(actual
        .iter()
        .all(|input| matches!(input, RelationalOverflowExtentInput::CheckpointWrite { .. })));
    let retained = actual.clone();
    let config = crate::relational::RelationalOverflowPublicationConfig::default();
    let reference_directory = directory.join("reference");
    let candidate_directory = directory.join("candidate");
    let publisher = crate::relational::RelationalOverflowPublisher::new(config);
    publisher
        .persist_generation(&reference_directory, 1, 2, None, None, expected)
        .unwrap();
    let publication = publisher
        .with_work_context(&work)
        .persist_checkpoint_generation(&candidate_directory, 1, 2, None, None, actual)
        .unwrap();
    assert_eq!(publication.extent_count, 3);
    for filename in [
        crate::relational::relational_overflow_extent_file(1),
        crate::relational::relational_overflow_descriptor_file(1),
        crate::relational::relational_overflow_manifest_generation_file(1),
    ] {
        assert_eq!(
            crate::file_io::read(reference_directory.join(&filename)).unwrap(),
            crate::file_io::read(candidate_directory.join(&filename)).unwrap()
        );
    }
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    drop(work);
    let reader = crate::relational::RelationalOverflowRootReader::open_generation(
        &candidate_directory,
        1,
        config,
    )
    .unwrap();
    let mut observed = Vec::new();
    for input in &retained {
        let value = reader
            .hydrate(
                input.reference(),
                &mut crate::relational::RelationalHydrationBudget::default(),
                None,
            )
            .unwrap();
        let RelationalValue::Text(value) = value else {
            unreachable!()
        };
        observed.push(value);
    }
    observed.sort();
    assert_eq!(
        observed,
        (0..3)
            .map(|seed| format!("value-{seed:04}-{}", "界🙂".repeat(800)))
            .collect::<Vec<_>>()
    );
    assert_eq!(crate::file_io::read(&path).unwrap(), checkpoint);
    drop(retained);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(reader);
    drop(state);
    crate::file_io::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_overflow_input_collection_cancels_each_actual_unit_and_denial_then_retries() {
    let state = source(4);
    let deltas = deltas(&state);
    let sparse = sparse(&state);
    for (state, delta) in [(&state, None), (&sparse, Some(deltas.as_slice()))] {
        let expected = collect(state, delta, None).unwrap();
        let local = scheduler();
        let baseline = Arc::new(CheckpointWorkProbe::default());
        assert_eq!(
            collect(state, delta, Some(&baseline.context(local.clone()))).unwrap(),
            expected
        );
        let units = baseline.completed.load(Ordering::SeqCst);
        assert!(units > 20);
        baseline.assert_released(&local);
        for stop in 0..=units {
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(stop, Ordering::SeqCst);
            let work = probe.context(local.clone());
            let held = (stop == 0).then(|| {
                local
                    .try_start(WorkRequest::background(WorkClass::Mutation, 1))
                    .unwrap()
            });
            let error = collect(state, delta, Some(&work)).unwrap_err();
            assert!(
                error.to_string().contains(if stop == 0 {
                    "admission deferred"
                } else {
                    "stopped"
                }),
                "{error:?}"
            );
            assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
            drop(held);
            probe.assert_released(&local);
            assert_eq!(collect(state, delta, None).unwrap(), expected);
            let retry = Arc::new(CheckpointWorkProbe::default());
            assert_eq!(
                collect(state, delta, Some(&retry.context(local.clone()))).unwrap(),
                expected
            );
            retry.assert_released(&local);
        }
    }
}

#[test]
fn checkpoint_units_overflow_shared_byte_copy_initializes_every_byte_and_cancels_each_unit() {
    for length in [
        0usize,
        1,
        64 * 1024 - 1,
        64 * 1024,
        64 * 1024 + 1,
        3 * 64 * 1024 + 17,
    ] {
        let source = (0..length)
            .map(|index| ((index * 71 + index / 7) % 256) as u8)
            .collect::<Vec<_>>();
        let before = source.clone();
        let local = scheduler();
        let baseline = Arc::new(CheckpointWorkProbe::default());
        let output = baseline.context(local.clone()).arc_bytes(&source).unwrap();
        assert_eq!(output.as_ref(), source.as_slice());
        if length > 0 {
            assert_ne!(output.as_ptr(), source.as_ptr());
        }
        let units = baseline.completed.load(Ordering::SeqCst);
        assert_eq!(units, length.div_ceil(64 * 1024) + 2);
        baseline.assert_released(&local);
        for stop in 0..=units {
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(stop, Ordering::SeqCst);
            let work = probe.context(local.clone());
            let held = (stop == 0).then(|| {
                local
                    .try_start(WorkRequest::background(WorkClass::Mutation, 1))
                    .unwrap()
            });
            assert!(work
                .arc_bytes(&source)
                .unwrap_err()
                .to_string()
                .contains(if stop == 0 {
                    "admission deferred"
                } else {
                    "stopped"
                }));
            drop(held);
            probe.assert_released(&local);
            assert_eq!(source, before);
            let retry = Arc::new(CheckpointWorkProbe::default());
            assert_eq!(
                retry
                    .context(local.clone())
                    .arc_bytes(&source)
                    .unwrap()
                    .as_ref(),
                source.as_slice()
            );
            retry.assert_released(&local);
        }
    }
}

#[test]
fn checkpoint_units_overflow_input_collection_preserves_conflicts_closure_and_source_errors() {
    let original = source(4);
    let original_deltas = deltas(&original);
    let local = scheduler();
    let compare = |state: &RelationalState, deltas: Option<&[RelationalRowPageTableDelta]>| {
        let expected = collect(state, deltas, None).unwrap_err();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = collect(state, deltas, Some(&probe.context(local.clone()))).unwrap_err();
        assert_eq!(actual.to_string(), expected.to_string());
        probe.assert_released(&local);
    };
    let mut missing = original.clone();
    missing.overflow_segments.pop_first();
    compare(&missing, None);
    let mut unreachable = original.clone();
    unreachable.overflow_segments.insert(
        hawdb_integrity::integrity_digest(b"unreachable").sha256,
        RelationalOverflowSegment::Inline(Arc::from(b"unreachable".as_slice())),
    );
    compare(&unreachable, None);
    compare(&sparse(&original), None);
    compare(&original, Some(&original_deltas));
    let mut conflicting = original_deltas.clone();
    let values = conflicting[1].dirty_pages[0].rows[0].row.values();
    let mut values = values.to_vec();
    let RelationalValue::Overflow(reference) = &mut values[2] else {
        unreachable!()
    };
    reference.uncompressed_bytes += 1;
    conflicting[1].dirty_pages[0].rows[0].row = RelationalRow::new(values);
    compare(&sparse(&original), Some(&conflicting));
    let mut missing_inline = sparse(&original);
    missing_inline.overflow_segments.clear();
    let expected = collect(&missing_inline, Some(&original_deltas), None).unwrap();
    let probe = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        collect(
            &missing_inline,
            Some(&original_deltas),
            Some(&probe.context(local.clone()))
        )
        .unwrap(),
        expected
    );
    assert!(expected
        .iter()
        .all(|input| matches!(input, RelationalOverflowExtentInput::Reuse(_))));
    probe.assert_released(&local);
}

#[test]
fn checkpoint_units_overflow_input_collection_detaches_file_bytes_without_serving_cache_leases() {
    use crate::relational::{
        decode_relational_checkpoint_file, encode_relational_checkpoint, RelationalDecodeLimits,
    };
    use crate::scan::{FileSegmentRangeReader, SegmentRangeReader};

    let source = RelationalStore::new(RelationalMutationLimits::default());
    source
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                    name: "messages".into(),
                    columns: vec![
                        RelationalColumnSchema {
                            name: "id".into(),
                            scalar_type: RelationalScalarType::Text,
                            nullable: false,
                            default: None,
                        },
                        RelationalColumnSchema {
                            name: "payload".into(),
                            scalar_type: RelationalScalarType::Bytea,
                            nullable: false,
                            default: None,
                        },
                    ],
                    primary_key: vec!["id".into()],
                    unique_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    indexes: Vec::new(),
                })],
            },
            |_, _| Ok(()),
        )
        .unwrap();
    source
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".into(),
                    mode: RelationalInsertMode::Error,
                    rows: (1..=2)
                        .map(|seed| {
                            let mut random = seed * 0x1234_5678u32;
                            let bytes = (0..3 * 64 * 1024 + 17)
                                .map(|_| {
                                    random ^= random << 13;
                                    random ^= random >> 17;
                                    random ^= random << 5;
                                    random as u8
                                })
                                .collect();
                            RelationalRow::new(vec![
                                RelationalValue::Text(format!("id-{seed}")),
                                RelationalValue::Bytea(bytes),
                            ])
                        })
                        .collect(),
                }],
            },
            |_, _| Ok(()),
        )
        .unwrap();
    let checkpoint = encode_relational_checkpoint(2, source.snapshot().unwrap().value()).unwrap();
    let path = std::env::temp_dir().join(format!(
        "hawdb-overflow-input-file-{}-{}",
        std::process::id(),
        NEXT_FILE.fetch_add(1, Ordering::Relaxed)
    ));
    crate::file_io::write(&path, &checkpoint).unwrap();
    let mut state = decode_relational_checkpoint_file(&path, RelationalDecodeLimits::checkpoint())
        .unwrap()
        .state;
    assert_eq!(state.file_backed_overflow_segment_count(), 2);
    let cache = Arc::new(crate::cache::SegmentCache::new(checkpoint.len() as u64));
    let mut retained = Vec::new();
    for segment in state.overflow_segments.values_mut() {
        let RelationalOverflowSegment::FileRange { reader, range } = segment else {
            unreachable!()
        };
        let mut cached = FileSegmentRangeReader::new().with_cache(
            Arc::clone(&cache),
            crate::cache::StoreId(1),
            crate::cache::ManifestGeneration(1),
        );
        cached.register(range.artifact_id, &path);
        *reader = Arc::new(cached);
        let bytes = reader.read_range(range).unwrap();
        assert!(bytes.len() > 3 * 64 * 1024);
        retained.push(bytes);
    }
    let total = retained.iter().map(|bytes| bytes.len()).sum::<usize>();
    let expected = state.overflow_generation_inputs(false, total).unwrap();
    let before = cache.snapshot();
    assert_eq!(before.pinned_bytes, total as u64);
    let local = scheduler();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    let actual = state
        .overflow_generation_inputs_with_work_context(
            false,
            total,
            &baseline.context(local.clone()),
        )
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(cache.snapshot(), before);
    for (input, retained) in actual.iter().zip(&retained) {
        let encoded = input.encoded_bytes().unwrap();
        assert_eq!(encoded, retained.as_ref());
        assert_ne!(encoded.as_ptr(), retained.as_ptr());
    }
    let units = baseline.completed.load(Ordering::SeqCst);
    let waves = baseline.io_waves.load(Ordering::SeqCst);
    assert!(waves > 2);
    baseline.assert_released(&local);
    for (stop, io) in (1..=units)
        .map(|stop| (stop, false))
        .chain((1..=waves).map(|stop| (stop, true)))
    {
        let probe = Arc::new(CheckpointWorkProbe::default());
        if io {
            probe.cancel_on_io_wave.store(stop, Ordering::SeqCst);
        } else {
            probe.cancel_after.store(stop, Ordering::SeqCst);
        }
        let error = state
            .overflow_generation_inputs_with_work_context(
                false,
                total,
                &probe.context(local.clone()),
            )
            .unwrap_err();
        assert!(error.to_string().contains("stopped"), "{error:?}");
        probe.assert_released(&local);
        assert_eq!(cache.snapshot(), before);
        let retry = Arc::new(CheckpointWorkProbe::default());
        assert_eq!(
            state
                .overflow_generation_inputs_with_work_context(
                    false,
                    total,
                    &retry.context(local.clone())
                )
                .unwrap(),
            expected
        );
        retry.assert_released(&local);
        assert_eq!(cache.snapshot(), before);
        assert_eq!(crate::file_io::read(&path).unwrap(), checkpoint);
    }
    for budget in [0, total - 1] {
        let expected = state.overflow_generation_inputs(false, budget).unwrap_err();
        let before = cache.snapshot();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = state
            .overflow_generation_inputs_with_work_context(
                false,
                budget,
                &probe.context(local.clone()),
            )
            .unwrap_err();
        assert_eq!(actual.to_string(), expected.to_string());
        assert_eq!(cache.snapshot(), before);
        probe.assert_released(&local);
    }
    let probe = Arc::new(CheckpointWorkProbe::default());
    let reused = state
        .overflow_generation_inputs_with_work_context(true, 0, &probe.context(local.clone()))
        .unwrap();
    assert!(reused
        .iter()
        .all(|input| matches!(input, RelationalOverflowExtentInput::Reuse(_))));
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    probe.assert_released(&local);
    drop(actual);
    drop(expected);
    drop(retained);
    assert_eq!(cache.snapshot().pinned_bytes, 0);
    crate::file_io::remove_file(path).unwrap();
}
