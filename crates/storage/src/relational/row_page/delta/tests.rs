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

use super::super::demand::RelationalRowPageProjectedOverlayValue;
use super::*;
use crate::relational::{
    ImmutableRelationalRowPage, RelationalKey, RelationalOverflowConfig,
    RelationalOverflowExtentInput, RelationalOverflowPublicationConfig,
    RelationalOverflowPublisher, RelationalOverflowRootReader, RelationalRecoveryFence,
    RelationalRow, RelationalRowChange, RelationalRowChangeCapture, RelationalRowPageEntry,
    RelationalRowPageId, RelationalRowPagePublicationConfig, RelationalRowPagePublisher,
    RelationalRowPageRecoveredValue, RelationalRowPageTableDelta, RelationalScalarType,
    RelationalValue,
};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[test]
fn private_manifest_generation_preserves_existing_temporary_evidence() {
    let directory = unique_test_dir("private-unowned-manifest");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let mut builder =
        RelationalRowDeltaBuilder::new(&directory, &base, 41, None, table_metadata(), config)
            .unwrap();
    builder.advance_empty(2).unwrap();
    let manifest_path = directory.join(relational_row_delta_manifest_generation_file(
        base.manifest().generation,
        41,
    ));
    let unowned = manifest_path.with_extension("hawdb.tmp");
    fs::write(&unowned, b"unowned manifest recovery evidence").unwrap();
    let report = builder
        .finish_selected_inner(
            2,
            super::super::super::RelationalRecoverySourceIdentity::for_test(1, 2),
            None,
            None,
            false,
        )
        .unwrap();
    assert_eq!(
        fs::read(&unowned).unwrap(),
        b"unowned manifest recovery evidence"
    );
    assert_eq!(
        report.events[3],
        RelationalRowDeltaPublicationPhase::PrivateManifestPrepared
    );
    assert!(!directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE).exists());
    RelationalRowDeltaReader::open_generation_with_recovery_fence(
        &directory,
        report.generation,
        &base,
        RelationalRecoveryFence::new(
            2,
            super::super::super::RelationalRecoverySourceIdentity::for_test(1, 2),
        ),
        config,
    )
    .unwrap();
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn private_manifest_generation_rejects_existing_immutable_evidence() {
    let directory = unique_test_dir("private-existing-manifest");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let mut builder =
        RelationalRowDeltaBuilder::new(&directory, &base, 41, None, table_metadata(), config)
            .unwrap();
    builder.advance_empty(2).unwrap();
    let manifest_path = directory.join(relational_row_delta_manifest_generation_file(
        base.manifest().generation,
        41,
    ));
    fs::write(&manifest_path, b"existing immutable recovery evidence").unwrap();
    assert!(builder
        .finish_selected_inner(
            2,
            super::super::super::RelationalRecoverySourceIdentity::for_test(1, 2),
            None,
            None,
            false,
        )
        .is_err());
    assert_eq!(
        fs::read(&manifest_path).unwrap(),
        b"existing immutable recovery evidence"
    );
    assert!(!directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE).exists());
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_run_target_is_soft_but_cannot_exceed_the_hard_limit() {
    let config = RelationalRowDeltaConfig::default();
    assert!(!config.checkpoint_recommended(config.checkpoint_runs.get() - 1));
    assert!(config.checkpoint_recommended(config.checkpoint_runs.get()));

    let directory = unique_test_dir("checkpoint-run-policy");
    let base = publish_and_open_base(&directory);
    let invalid = RelationalRowDeltaConfig {
        max_runs: NonZeroUsize::new(1).unwrap(),
        checkpoint_runs: NonZeroUsize::new(2).unwrap(),
        ..config
    };
    assert!(matches!(
        RelationalRowDeltaBuilder::new(
            &directory,
            &base,
            1,
            None,
            table_metadata(),
            invalid,
        ),
        Err(RelationalRowDeltaError::Admission(message))
            if message.contains("checkpoint target")
    ));
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn immutable_runs_round_trip_across_bounded_flushes() {
    let directory = unique_test_dir("round-trip");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::new(1).unwrap(),
        ..RelationalRowDeltaConfig::default()
    };
    let mut builder =
        RelationalRowDeltaBuilder::new(&directory, &base, 1, None, table_metadata(), config)
            .unwrap();
    builder.record(2, capture(2, Some("two-v2"))).unwrap();
    builder.record(3, capture(1, None)).unwrap();
    let report = builder.finish(3, None).unwrap();
    assert_eq!(report.runs, 2);
    assert_eq!(report.entries, 2);
    assert_eq!(report.replayed_batches, 2);
    assert!(report.peak_dirty_entries <= 1);
    assert!(report.peak_dirty_bytes <= config.max_dirty_bytes.get());
    assert_eq!(report.events, COMPLETE_PUBLICATION_TRACE);

    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 3, config)
        .unwrap()
        .unwrap();
    assert_eq!(reader.manifest().generation(), report.generation);
    let mut rows = Vec::new();
    let read = reader
        .visit_entries(|table, key, value, epoch| {
            rows.push((table.to_string(), key.clone(), value.clone(), epoch));
            true
        })
        .unwrap();
    assert_eq!(read.runs_read, 2);
    assert_eq!(read.entries_visited, 2);
    assert!(!read.stopped_early);
    assert_eq!(present_text(&rows[0].2), Some("two-v2"));
    assert_eq!(rows[0].3, 2);
    assert!(matches!(
        rows[1].2,
        RelationalRowPageRecoveredValue::Deleted
    ));
    assert_eq!(rows[1].3, 3);
    let wrong_source = RelationalRecoverySourceIdentity {
        record_sequence_sha256: hawdb_integrity::Sha256Digest::from_bytes([0x5a; 32]),
        ..RelationalRecoverySourceIdentity::for_test(1, 3)
    };
    assert!(matches!(
        RelationalRowDeltaReader::open_latest_with_recovery_fence(
            &directory,
            &base,
            RelationalRecoveryFence::new(3, wrong_source),
            config,
        ),
        Err(RelationalRowDeltaError::Corrupt(_))
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn record_triggered_row_flush_denies_metadata_before_run_publication() {
    use crate::background::{CheckpointOperationError, CheckpointWorkContext, CheckpointWorkError};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let directory = unique_test_dir("record-flush-admission");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::MIN,
        ..Default::default()
    };
    let mut builder = builder(&directory, &base, 7, None, config);
    builder.record(2, capture(2, Some("two-v2"))).unwrap();
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(64),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(64))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let result =
        work.classify(|scoped| builder.record_with_work_context(3, capture(1, None), scoped));
    assert!(
        matches!(
            result,
            Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                _
            )))
        ),
        "{result:?}"
    );
    assert!(!directory
        .join(relational_row_delta_run_file(1, 7, 0))
        .exists());
    assert!(!directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE).exists());
    drop(builder);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(base);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn dirty_run_coalesces_repeated_keys_to_the_latest_epoch() {
    let directory = unique_test_dir("coalesce");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let mut builder = builder(&directory, &base, 1, None, config);
    builder.record(2, capture(2, Some("two-v2"))).unwrap();
    builder.record(3, capture(2, Some("two-v3"))).unwrap();
    let report = builder.finish(3, None).unwrap();
    assert_eq!(report.entries, 1);
    assert_eq!(report.peak_dirty_entries, 1);
    assert!(report.peak_dirty_bytes <= config.max_dirty_bytes.get());

    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 3, config)
        .unwrap()
        .unwrap();
    let mut observed = None;
    reader
        .visit_entries(|_, key, value, epoch| {
            observed = Some((key.clone(), value.clone(), epoch));
            true
        })
        .unwrap();
    let (_, value, epoch) = observed.unwrap();
    assert_eq!(present_text(&value), Some("two-v3"));
    assert_eq!(epoch, 3);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn controlled_dirty_lookup_denies_payload_copy_and_retains_successful_backing() {
    use crate::background::{
        CheckpointDecodeContext, CheckpointOperationError, CheckpointWorkContext,
        CheckpointWorkError,
    };
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    for value in [
        RelationalValue::Text("x".repeat(128 * 1024)),
        RelationalValue::Bytea(vec![7; 128 * 1024]),
    ] {
        let directory = unique_test_dir("controlled-dirty-lookup");
        let base = publish_and_open_base(&directory);
        let mut config = RelationalRowDeltaConfig::default();
        config.row_limits.max_inline_value_bytes = NonZeroUsize::new(256 * 1024).unwrap();
        let mut builder = builder(&directory, &base, 1, None, config);
        builder
            .record(
                2,
                RelationalRowChangeCapture::Captured {
                    changes: vec![RelationalRowChange {
                        table: "documents".into(),
                        primary_key: key(2),
                        row: Some(RelationalRow::new(vec![
                            RelationalValue::BigInt(2),
                            value.clone(),
                        ])),
                    }],
                    encoded_bytes: 0,
                },
            )
            .unwrap();
        for (bytes, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
            let governor = RuntimeGovernor::detect(
                RuntimeGovernorConfig {
                    memory_budget_bytes: Some(bytes),
                    ..RuntimeGovernorConfig::shared_host()
                },
                IoConcurrencyBudget::new(2, 1),
            );
            governor.pin_resources();
            let permit = governor
                .try_admit(RuntimeWorkRequest::background_maintenance(bytes))
                .unwrap();
            let work =
                CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
            let observer = crate::test_allocator::AllocationObservation::start();
            let result = work.classify(|scoped| {
                let allocations = CheckpointDecodeContext {
                    work: scoped.clone(),
                    memory: Default::default(),
                };
                let recovered = builder.lookup_staged_with_allocations(
                    "documents",
                    &key(2),
                    Some(&allocations),
                )?;
                Ok::<_, RelationalRowDeltaError>((recovered, allocations.memory.into_inner()))
            });
            let large_allocations = observer.finish();
            if succeeds {
                let ((recovered, report), memory) = result.unwrap();
                let Some(RelationalRowPageRecoveredValue::Present(row)) = recovered else {
                    panic!("expected recovered row")
                };
                assert_eq!(row.values()[1], value);
                assert_eq!(report.runs_read, 0);
                assert!(large_allocations > 0);
                let backing = row.values().as_ptr();
                let observer = crate::test_allocator::AllocationObservation::start();
                let mut budget = crate::relational::RelationalHydrationBudget::default();
                let row = crate::relational::overflow::hydrate_owned_row(
                    &crate::relational::RelationalState::default(),
                    row,
                    &mut budget,
                    Some(work.task_context()),
                    None,
                )
                .unwrap();
                assert_eq!(
                    observer.finish(),
                    0,
                    "hydration must reuse the admitted inline payload"
                );
                assert_eq!(row.values().as_ptr(), backing);
                assert_eq!(budget.hydrated_rows, 1);
                assert!(
                    matches!(work.task_context().reserve_working_memory(bytes), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes <= bytes - 128 * 1024)
                );
                drop(row);
                drop(memory);
            } else {
                assert!(
                    matches!(
                        result,
                        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                            _
                        )))
                    ),
                    "{result:?}"
                );
                assert_eq!(
                    large_allocations, 0,
                    "denial must precede the large payload copy"
                );
            }
            assert!(matches!(
                work.task_context().reserve_working_memory(bytes),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == bytes
            ));
            drop(work);
            drop(permit);
            assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
            assert_eq!(builder.visible_commit_epoch(), 2);
        }
        fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn controlled_run_lookup_denies_read_buffer_before_allocation_and_retries() {
    use crate::background::{
        CheckpointDecodeContext, CheckpointOperationError, CheckpointWorkContext,
        CheckpointWorkError,
    };
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let directory = unique_test_dir("controlled-run-buffer");
    let base = publish_and_open_base(&directory);
    let mut config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::MIN,
        ..Default::default()
    };
    config.row_limits.max_inline_value_bytes = NonZeroUsize::new(256 * 1024).unwrap();
    let mut builder = builder(&directory, &base, 1, None, config);
    let payload = "x".repeat(128 * 1024);
    builder.record(2, capture(2, Some(&payload))).unwrap();
    builder.record(3, capture(3, Some("three"))).unwrap();
    let ordinary = builder.lookup_staged("documents", &key(2)).unwrap();
    assert_eq!(ordinary.1.runs_read, 1);
    for (bytes, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(bytes),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(bytes))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = work.classify(|scoped| {
            let context = CheckpointDecodeContext {
                work: scoped.clone(),
                memory: Default::default(),
            };
            let value =
                builder.lookup_staged_with_allocations("documents", &key(2), Some(&context))?;
            Ok::<_, RelationalRowDeltaError>((value, context.memory.into_inner()))
        });
        let large_allocations = observer.finish();
        if succeeds {
            let (value, memory) = result.unwrap();
            assert_eq!(value, ordinary);
            assert!(
                matches!(work.task_context().reserve_working_memory(bytes),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                    if available_bytes <= bytes - 128 * 1024),
                "selected run payload must remain charged after the cursor closes"
            );
            drop(value);
            drop(memory);
        } else {
            assert!(
                matches!(
                    result,
                    Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                        _
                    )))
                ),
                "{result:?}"
            );
            assert_eq!(
                large_allocations, 0,
                "deny before allocating the run row buffer"
            );
        }
        assert_eq!(builder.visible_commit_epoch(), 3);
        assert!(matches!(work.task_context().reserve_working_memory(bytes),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == bytes));
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
    drop(builder);
    drop(base);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn controlled_run_lookup_admits_wide_query_and_decoded_primary_keys() {
    use crate::background::{
        CheckpointDecodeContext, CheckpointOperationError, CheckpointWorkContext,
        CheckpointWorkError,
    };
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let directory = unique_test_dir("controlled-run-key");
    let base = publish_and_open_base(&directory);
    let mut config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::MIN,
        ..Default::default()
    };
    config.row_limits.max_key_bytes = NonZeroUsize::new(256 * 1024).unwrap();
    let mut builder = builder(&directory, &base, 1, None, config);
    let wide_key = RelationalKey(vec![RelationalValue::Text("k".repeat(128 * 1024))]);
    builder
        .record(
            2,
            RelationalRowChangeCapture::Captured {
                changes: vec![RelationalRowChange {
                    table: "documents".into(),
                    primary_key: wide_key.clone(),
                    row: Some(row(2, "small payload")),
                }],
                encoded_bytes: 0,
            },
        )
        .unwrap();
    builder.record(3, capture(3, Some("three"))).unwrap();
    let ordinary = builder.lookup_staged("documents", &wide_key).unwrap();
    assert_eq!(ordinary.1.runs_read, 1);
    for (bytes, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(bytes),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(bytes))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = work.classify(|scoped| {
            let context = CheckpointDecodeContext {
                work: scoped.clone(),
                memory: Default::default(),
            };
            let value =
                builder.lookup_staged_with_allocations("documents", &wide_key, Some(&context))?;
            Ok::<_, RelationalRowDeltaError>((value, context.memory.into_inner()))
        });
        let large_allocations = observer.finish();
        if succeeds {
            let (value, memory) = result.unwrap();
            assert_eq!(value, ordinary);
            assert!(
                matches!(work.task_context().reserve_working_memory(bytes),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                    if available_bytes <= bytes - 128 * 1024),
                "decoded primary key ownership must remain charged through its selected inventory"
            );
            drop(value);
            drop(memory);
        } else {
            assert!(
                matches!(
                    result,
                    Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                        _
                    )))
                ),
                "{result:?}"
            );
            assert_eq!(
                large_allocations, 0,
                "deny before allocating the encoded lookup key"
            );
        }
        assert_eq!(builder.visible_commit_epoch(), 3);
        assert!(matches!(work.task_context().reserve_working_memory(bytes),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == bytes));
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
    drop(builder);
    drop(base);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn controlled_run_lookup_releases_skipped_payloads_under_one_budget() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let directory = unique_test_dir("controlled-run-streaming");
    let base = publish_and_open_base(&directory);
    let mut config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::new(128).unwrap(),
        max_dirty_bytes: NonZeroUsize::new(32 * 1024 * 1024).unwrap(),
        ..Default::default()
    };
    config.row_limits.max_inline_value_bytes = NonZeroUsize::new(256 * 1024).unwrap();
    let payload = "x".repeat(128 * 1024);
    let mut builder = builder(&directory, &base, 1, None, config);
    builder
        .record(
            2,
            RelationalRowChangeCapture::Captured {
                changes: (1..=128)
                    .map(|id| RelationalRowChange {
                        table: "documents".into(),
                        primary_key: key(id),
                        row: Some(row(id, &payload)),
                    })
                    .collect(),
                encoded_bytes: 0,
            },
        )
        .unwrap();
    builder.record(3, capture(129, Some("next"))).unwrap();
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(16 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(16 * 1024 * 1024))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let context = CheckpointDecodeContext {
        work: work.clone(),
        memory: Default::default(),
    };
    let (value, report) = builder
        .lookup_staged_with_allocations("documents", &key(128), Some(&context))
        .unwrap();
    assert_eq!(report.runs_read, 1);
    assert_eq!(report.entries_visited, 128);
    assert!(
        matches!(value.as_ref(), Some(RelationalRowPageRecoveredValue::Present(row)) if row.values()[1] == RelationalValue::Text(payload.clone()))
    );
    assert!(
        matches!(work.task_context().reserve_working_memory(16 * 1024 * 1024),
        Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
            if available_bytes > 15 * 1024 * 1024 && available_bytes < 16 * 1024 * 1024),
        "only the selected payload remains charged after scanning 16 MiB of skipped/selected rows"
    );
    drop(value);
    drop(context);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(builder);
    drop(base);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn repeated_dirty_wide_key_lookups_release_scratch_without_retaining_payload_inventory() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let directory = unique_test_dir("dirty-key-scratch");
    let base = publish_and_open_base(&directory);
    let mut config = RelationalRowDeltaConfig::default();
    config.row_limits.max_key_bytes = NonZeroUsize::new(256 * 1024).unwrap();
    let mut builder = builder(&directory, &base, 1, None, config);
    let wide_key = RelationalKey(vec![RelationalValue::Text("k".repeat(128 * 1024))]);
    builder
        .record(
            2,
            RelationalRowChangeCapture::Captured {
                changes: vec![RelationalRowChange {
                    table: "documents".into(),
                    primary_key: wide_key.clone(),
                    row: None,
                }],
                encoded_bytes: 0,
            },
        )
        .unwrap();
    let ceiling = 1024 * 1024;
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let context = CheckpointDecodeContext {
        work: CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default())),
        memory: Default::default(),
    };
    for _ in 0..128 {
        let (value, report) = builder
            .lookup_staged_with_allocations("documents", &wide_key, Some(&context))
            .unwrap();
        assert!(matches!(
            value,
            Some(RelationalRowPageRecoveredValue::Deleted)
        ));
        assert_eq!(report.runs_read, 0);
        assert!(
            context.memory.borrow().is_empty(),
            "lookup scratch escaped into the payload inventory"
        );
        assert!(
            matches!(context.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                if available_bytes == ceiling)
        );
    }
    drop(context);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(builder);
    drop(base);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn unpublished_lookup_prefers_dirty_values_then_newest_immutable_run() {
    let directory = unique_test_dir("unpublished-lookup");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::new(1).unwrap(),
        ..RelationalRowDeltaConfig::default()
    };
    let mut builder = builder(&directory, &base, 1, None, config);

    builder.record(2, capture(2, Some("two-v2"))).unwrap();
    let (dirty, dirty_report) = builder.lookup_staged("documents", &key(2)).unwrap();
    assert_eq!(dirty.as_ref().and_then(present_text), Some("two-v2"));
    assert_eq!(dirty_report.entries_visited, 1);
    assert_eq!(dirty_report.runs_read, 0);

    builder.record(3, capture(1, Some("one-v3"))).unwrap();
    let (flushed, flushed_report) = builder.lookup_staged("documents", &key(2)).unwrap();
    assert_eq!(flushed.as_ref().and_then(present_text), Some("two-v2"));
    assert!(flushed_report.runs_read > 0);

    builder.record(4, capture(2, None)).unwrap();
    let (deleted, deleted_report) = builder.lookup_staged("documents", &key(2)).unwrap();
    assert!(matches!(
        deleted,
        Some(RelationalRowPageRecoveredValue::Deleted)
    ));
    assert_eq!(deleted_report.runs_read, 0);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn delta_schema_must_match_the_base_root_column_count() {
    let directory = unique_test_dir("column-count-fence");
    let base = publish_and_open_base(&directory);
    let mut schemas = table_metadata();
    schemas[0].column_count = NonZeroU32::new(3).unwrap();
    assert!(matches!(
        RelationalRowDeltaBuilder::new(
            &directory,
            &base,
            1,
            None,
            schemas,
            RelationalRowDeltaConfig::default(),
        ),
        Err(RelationalRowDeltaError::Admission(message))
            if message.contains("does not match the selected row root")
    ));
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn point_lookup_selects_the_newest_value_across_immutable_runs() {
    let directory = unique_test_dir("point-lookup");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::new(1).unwrap(),
        ..RelationalRowDeltaConfig::default()
    };
    let mut builder = builder(&directory, &base, 1, None, config);
    builder.record(2, capture(2, Some("two-v2"))).unwrap();
    builder.record(3, capture(1, Some("one-v3"))).unwrap();
    builder.record(4, capture(2, Some("two-v4"))).unwrap();
    builder.record(5, capture(2, None)).unwrap();
    builder.finish(5, None).unwrap();

    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 5, config)
        .unwrap()
        .unwrap();
    let (two, two_report) = reader.lookup("documents", &key(2)).unwrap();
    assert!(matches!(
        two,
        Some(RelationalRowPageRecoveredValue::Deleted)
    ));
    assert_eq!(two_report.runs_read, 1);
    assert!(two_report.stopped_early);

    let (one, one_report) = reader.lookup("documents", &key(1)).unwrap();
    assert_eq!(present_text(one.as_ref().unwrap()), Some("one-v3"));
    assert_eq!(one_report.runs_read, 1);
    assert!(one_report.stopped_early);

    let (missing, missing_report) = reader.lookup("documents", &key(9)).unwrap();
    assert!(missing.is_none());
    assert_eq!(missing_report.runs_read, 0);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn range_lookup_prunes_runs_and_honors_exclusive_bounds() {
    let directory = unique_test_dir("range-lookup");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::new(1).unwrap(),
        ..RelationalRowDeltaConfig::default()
    };
    let mut builder = builder(&directory, &base, 1, None, config);
    builder.record(2, capture(2, Some("two"))).unwrap();
    builder.record(3, capture(4, Some("four"))).unwrap();
    builder.record(4, capture(6, Some("six"))).unwrap();
    builder.finish(4, None).unwrap();

    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 4, config)
        .unwrap()
        .unwrap();
    let mut rows = Vec::new();
    let report = reader
        .visit_range_entries(
            "documents",
            Bound::Excluded(&key(2)),
            Bound::Included(&key(4)),
            |key, value, epoch| {
                rows.push((key.clone(), present_text(value).unwrap().to_string(), epoch));
                true
            },
        )
        .unwrap();
    assert_eq!(rows, vec![(key(4), "four".to_string(), 3)]);
    assert_eq!(report.runs_read, 1);
    assert_eq!(report.entries_visited, 1);
    assert!(!report.stopped_early);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn range_lookup_stops_without_reading_irrelevant_suffix_runs() {
    let directory = unique_test_dir("range-early-stop");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::new(1).unwrap(),
        ..RelationalRowDeltaConfig::default()
    };
    let mut builder = builder(&directory, &base, 1, None, config);
    builder.record(2, capture(2, Some("two"))).unwrap();
    builder.record(3, capture(4, Some("four"))).unwrap();
    builder.record(4, capture(6, Some("six"))).unwrap();
    builder.finish(4, None).unwrap();

    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 4, config)
        .unwrap()
        .unwrap();
    let mut rows = Vec::new();
    let report = reader
        .visit_range_entries(
            "documents",
            Bound::Unbounded,
            Bound::Unbounded,
            |key, _, _| {
                rows.push(key.clone());
                false
            },
        )
        .unwrap();
    assert_eq!(rows, vec![key(2)]);
    assert_eq!(report.runs_read, 1);
    assert_eq!(report.entries_visited, 1);
    assert!(report.stopped_early);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn streaming_range_sources_bound_retained_run_files() {
    let directory = unique_test_dir("range-file-pool");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::new(1).unwrap(),
        max_range_open_files: NonZeroUsize::new(2).unwrap(),
        ..RelationalRowDeltaConfig::default()
    };
    let mut builder = builder(&directory, &base, 1, None, config);
    for (epoch, id) in (2..=5).zip(2..=5) {
        builder.record(epoch, capture(id, Some("value"))).unwrap();
    }
    builder.finish(5, None).unwrap();

    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 5, config)
        .unwrap()
        .unwrap();
    let (mut sources, report) = reader
        .range_sources(
            "documents",
            Bound::Unbounded,
            Bound::Unbounded,
            &[0],
            false,
            8,
        )
        .unwrap();
    assert_eq!(sources.len(), 4);
    assert_eq!(report.runs_read, 4);
    assert_eq!(report.peak_open_files, 0);

    let mut keys = Vec::new();
    for source in &mut sources {
        keys.push(source.next().unwrap().unwrap().0);
        assert!(source.next().unwrap().is_none());
    }
    assert_eq!(keys, vec![key(2), key(3), key(4), key(5)]);
    let mut completed_report = report;
    sources[0]
        .file_pool_snapshot()
        .unwrap()
        .apply_to(&mut completed_report);
    assert_eq!(completed_report.peak_open_files, 2);
    assert_eq!(completed_report.range_file_opens, 4);
    assert_eq!(completed_report.range_file_pool_hits, 0);
    assert_eq!(completed_report.range_file_pool_misses, 4);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn streaming_range_sources_decode_only_requested_recovery_fields() {
    let directory = unique_test_dir("range-projected-decode");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let large_body = "x".repeat(60 * 1024);
    let mut builder = builder(&directory, &base, 1, None, config);
    builder.record(2, capture(2, Some(&large_body))).unwrap();
    builder.finish(2, None).unwrap();

    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 2, config)
        .unwrap()
        .unwrap();
    let (mut sources, _) = reader
        .range_sources(
            "documents",
            Bound::Unbounded,
            Bound::Unbounded,
            &[0],
            false,
            8,
        )
        .unwrap();
    let (_, value, _) = sources[0].next().unwrap().unwrap();
    let RelationalRowPageProjectedOverlayValue::Present { fields, .. } = value else {
        panic!("recovery row must remain present");
    };
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].ordinal, 0);
    assert_eq!(fields[0].value, RelationalValue::BigInt(2));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn cumulative_run_byte_admission_precedes_run_creation() {
    let directory = unique_test_dir("artifact-admission");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_run_bytes: NonZeroU64::new(1).unwrap(),
        ..RelationalRowDeltaConfig::default()
    };
    let mut builder = builder(&directory, &base, 1, None, config);
    builder.record(2, capture(2, Some("two-v2"))).unwrap();
    assert!(matches!(
        builder.finish(2, None),
        Err(RelationalRowDeltaError::Admission(_))
    ));
    assert!(!directory
        .join(relational_row_delta_run_file(1, 1, 0))
        .exists());
    assert!(!directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE).exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn reader_enforces_the_cumulative_run_byte_limit() {
    let directory = unique_test_dir("reader-run-admission");
    let base = publish_and_open_base(&directory);
    let write_config = RelationalRowDeltaConfig::default();
    let mut builder = builder(&directory, &base, 1, None, write_config);
    builder.record(2, capture(2, Some("two-v2"))).unwrap();
    builder.finish(2, None).unwrap();

    let read_config = RelationalRowDeltaConfig {
        max_run_bytes: NonZeroU64::new(1).unwrap(),
        ..write_config
    };
    assert!(matches!(
        RelationalRowDeltaReader::open_latest(&directory, &base, 2, read_config),
        Err(RelationalRowDeltaError::Admission(message))
            if message.contains("run") && message.contains("exceeding limit")
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn builder_is_poisoned_after_a_partial_epoch_error() {
    let directory = unique_test_dir("poison");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::new(1).unwrap(),
        ..RelationalRowDeltaConfig::default()
    };
    let mut builder = builder(&directory, &base, 1, None, config);
    let error = builder
        .record(
            2,
            RelationalRowChangeCapture::Captured {
                changes: vec![change(1, None), change(2, Some("two"))],
                encoded_bytes: 0,
            },
        )
        .unwrap_err();
    assert!(matches!(error, RelationalRowDeltaError::Admission(_)));
    assert!(matches!(
        builder.advance_empty(2),
        Err(RelationalRowDeltaError::Invalidated(_))
    ));
    assert!(!directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE).exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn builder_rejects_changes_at_the_immutable_base_epoch() {
    let directory = unique_test_dir("base-epoch-change");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let mut builder = builder(&directory, &base, 1, None, config);

    assert!(matches!(
        builder.record(1, capture(2, Some("not-newer"))),
        Err(RelationalRowDeltaError::Corrupt(message))
            if message.contains("immutable base epoch")
    ));
    assert!(matches!(
        builder.finish(1, None),
        Err(RelationalRowDeltaError::Invalidated(_))
    ));
    assert!(!directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE).exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn every_pre_latest_stop_keeps_the_previous_delta_selected() {
    for phase in [
        RelationalRowDeltaPublicationPhase::CandidateRunsDurable,
        RelationalRowDeltaPublicationPhase::CandidateManifestDurable,
        RelationalRowDeltaPublicationPhase::BaseRevalidated,
    ] {
        let directory = unique_test_dir(&format!("crash-{phase:?}"));
        let base = publish_and_open_base(&directory);
        let config = RelationalRowDeltaConfig::default();
        let mut first = builder(&directory, &base, 1, None, config);
        first.record(2, capture(2, Some("two-v2"))).unwrap();
        first.finish(2, None).unwrap();
        let previous = RelationalRowDeltaGeneration {
            base_generation: 1,
            delta_generation: 1,
        };
        let mut candidate = builder(&directory, &base, 2, Some(previous), config);
        candidate.record(2, capture(3, Some("three"))).unwrap();
        candidate.record(3, capture(2, Some("two-v3"))).unwrap();
        assert!(matches!(
            candidate.finish_inner(
                3,
                RelationalRecoverySourceIdentity::for_test(1, 3),
                None,
                Some(phase),
            ),
            Err(RelationalRowDeltaError::Durability(_))
        ));
        let selected = RelationalRowDeltaReader::open_latest(&directory, &base, 2, config)
            .unwrap()
            .unwrap();
        assert_eq!(selected.manifest().generation(), previous);
        fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn stale_builder_cannot_replace_a_newer_delta_root() {
    let directory = unique_test_dir("stale");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let mut first = builder(&directory, &base, 1, None, config);
    first.record(2, capture(2, Some("two-v2"))).unwrap();
    first.finish(2, None).unwrap();
    let previous = RelationalRowDeltaGeneration {
        base_generation: 1,
        delta_generation: 1,
    };
    let mut stale = builder(&directory, &base, 2, Some(previous), config);
    stale.advance_empty(2).unwrap();
    stale.record(3, capture(2, Some("stale"))).unwrap();
    let mut winner = builder(&directory, &base, 3, Some(previous), config);
    winner.advance_empty(2).unwrap();
    winner.record(3, capture(2, Some("winner"))).unwrap();
    winner.finish(3, None).unwrap();

    assert!(matches!(
        stale.finish(3, None),
        Err(RelationalRowDeltaError::StaleGeneration {
            expected_previous: Some(expected),
            actual_previous: Some(actual),
        }) if expected == previous && actual.delta_generation == 3
    ));
    let selected = RelationalRowDeltaReader::open_latest(&directory, &base, 3, config)
        .unwrap()
        .unwrap();
    assert_eq!(selected.manifest().delta_generation, 3);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn builder_cannot_publish_after_its_row_root_becomes_stale() {
    let directory = unique_test_dir("stale-base");
    let base = publish_and_open_base(&directory);
    let row_config = RelationalRowPagePublicationConfig::default();
    let delta_config = RelationalRowDeltaConfig::default();
    let mut candidate = builder(&directory, &base, 1, None, delta_config);
    candidate.record(2, capture(2, Some("two-v2"))).unwrap();

    RelationalRowPagePublisher::new(row_config)
        .publish(
            &directory,
            2,
            2,
            Some(1),
            vec![RelationalRowPageTableDelta {
                replaces_base: false,
                last_row_id: 0,
                table: "documents".to_string(),
                schema: None,
                schema_digest: schema_digest(),
                column_count: NonZeroU32::new(2).unwrap(),
                next_page_id: NonZeroU64::new(2).unwrap(),
                dirty_pages: vec![ImmutableRelationalRowPage {
                    generation: 2,
                    source_commit_epoch: 2,
                    page_id: RelationalRowPageId::new(NonZeroU64::new(1).unwrap()),
                    schema_digest: schema_digest(),
                    column_count: 2,
                    rows: vec![RelationalRowPageEntry {
                        primary_key: key(1),
                        row: row(1, "one-v2"),
                    }],
                }],
                deleted_page_ids: Vec::new(),
            }],
        )
        .unwrap();

    assert!(matches!(
        candidate.finish(2, None),
        Err(RelationalRowDeltaError::StaleBase {
            expected,
            actual: Some(actual),
        }) if expected.generation == 1 && actual.generation == 2
    ));
    assert!(!directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE).exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn corruption_poisoning_is_demand_driven() {
    let directory = unique_test_dir("run-corrupt");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let mut corrupted_run = builder(&directory, &base, 1, None, config);
    corrupted_run.record(2, capture(2, Some("two-v2"))).unwrap();
    corrupted_run.finish(2, None).unwrap();
    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 2, config)
        .unwrap()
        .unwrap();
    let run = directory.join(relational_row_delta_run_file(1, 1, 0));
    let end = fs::metadata(&run).unwrap().len() - 1;
    flip_byte(&run, end);
    assert!(matches!(
        reader.visit_entries(|_, _, _, _| true),
        Err(RelationalRowDeltaError::Corrupt(_))
    ));
    assert!(reader.is_poisoned());
    assert!(matches!(
        reader.visit_entries(|_, _, _, _| true),
        Err(RelationalRowDeltaError::Corrupt(message)) if message.contains("poisoned")
    ));
    fs::remove_dir_all(directory).unwrap();

    let manifest_directory = unique_test_dir("manifest-corrupt");
    let base = publish_and_open_base(&manifest_directory);
    let mut candidate = builder(&manifest_directory, &base, 1, None, config);
    candidate.record(2, capture(2, Some("two-v2"))).unwrap();
    candidate.finish(2, None).unwrap();
    flip_byte(
        &manifest_directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE),
        36,
    );
    assert!(matches!(
        RelationalRowDeltaReader::open_latest(&manifest_directory, &base, 2, config),
        Err(RelationalRowDeltaError::Corrupt(_))
    ));
    fs::remove_dir_all(manifest_directory).unwrap();
}

#[test]
fn descriptor_binding_rejects_valid_entries_swapped_between_ordinals() {
    let directory = unique_test_dir("descriptor-swap");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let mut builder = builder(&directory, &base, 1, None, config);
    builder
        .record(
            2,
            RelationalRowChangeCapture::Captured {
                changes: vec![change(2, Some("two-v2")), change(3, Some("three"))],
                encoded_bytes: 0,
            },
        )
        .unwrap();
    builder.finish(2, None).unwrap();
    let run = directory.join(relational_row_delta_run_file(1, 1, 0));
    let mut encoded = fs::read(&run).unwrap();
    let descriptors = &mut encoded
        [codec::RUN_HEADER_BYTES..codec::RUN_HEADER_BYTES + 2 * codec::ENTRY_DESCRIPTOR_BYTES];
    let (left, right) = descriptors.split_at_mut(codec::ENTRY_DESCRIPTOR_BYTES);
    left.swap_with_slice(right);
    fs::write(&run, encoded).unwrap();

    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 2, config)
        .unwrap()
        .unwrap();
    assert!(matches!(
        reader.visit_entries(|_, _, _, _| true),
        Err(RelationalRowDeltaError::Corrupt(_))
    ));
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn overflow_references_allow_a_pinned_state_resolver_or_exact_root() {
    let directory = unique_test_dir("overflow");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let overflow_input = RelationalOverflowExtentInput::encode(
        RelationalScalarType::Text,
        b"large payload",
        RelationalOverflowConfig::default(),
    )
    .unwrap();
    let reference = *overflow_input.reference();
    let overflow_capture = RelationalRowChangeCapture::Captured {
        changes: vec![RelationalRowChange {
            table: "documents".to_string(),
            primary_key: key(2),
            row: Some(RelationalRow::new(vec![
                RelationalValue::BigInt(2),
                RelationalValue::Overflow(reference),
            ])),
        }],
        encoded_bytes: 0,
    };
    let mut unresolved = builder(&directory, &base, 1, None, config);
    unresolved.record(2, overflow_capture.clone()).unwrap();
    unresolved.finish(2, None).unwrap();
    let unresolved_reader = RelationalRowDeltaReader::open_latest(&directory, &base, 2, config)
        .unwrap()
        .unwrap();
    unresolved_reader.validate_overflow_root(None).unwrap();
    let mut observed = None;
    unresolved_reader
        .visit_entries(|_, _, value, _| {
            observed = Some(value.clone());
            true
        })
        .unwrap();
    assert!(matches!(
        observed,
        Some(RelationalRowPageRecoveredValue::Present(row))
            if row.values()[1] == RelationalValue::Overflow(reference)
    ));

    fs::remove_dir_all(&directory).unwrap();
    let directory = unique_test_dir("overflow-root");
    let base = publish_and_open_base(&directory);
    let overflow_input = RelationalOverflowExtentInput::encode(
        RelationalScalarType::Text,
        b"large payload",
        RelationalOverflowConfig::default(),
    )
    .unwrap();
    let reference = *overflow_input.reference();
    let overflow_capture = RelationalRowChangeCapture::Captured {
        changes: vec![RelationalRowChange {
            table: "documents".to_string(),
            primary_key: key(2),
            row: Some(RelationalRow::new(vec![
                RelationalValue::BigInt(2),
                RelationalValue::Overflow(reference),
            ])),
        }],
        encoded_bytes: 0,
    };

    RelationalOverflowPublisher::new(RelationalOverflowPublicationConfig::default())
        .publish(&directory, 1, 2, None, vec![overflow_input])
        .unwrap();
    let overflow_root = RelationalOverflowRootReader::open_latest(
        &directory,
        RelationalOverflowPublicationConfig::default(),
    )
    .unwrap()
    .unwrap();
    let mut admitted = builder(&directory, &base, 1, None, config);
    admitted.record(2, overflow_capture).unwrap();
    admitted.finish(2, Some(&overflow_root)).unwrap();
    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 2, config)
        .unwrap()
        .unwrap();
    reader.validate_overflow_root(Some(&overflow_root)).unwrap();
    let mut observed = None;
    reader
        .visit_entries(|_, _, value, _| {
            observed = Some(value.clone());
            true
        })
        .unwrap();
    assert!(matches!(
        observed,
        Some(RelationalRowPageRecoveredValue::Present(row))
            if row.values()[1] == RelationalValue::Overflow(reference)
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn empty_commit_range_publishes_a_valid_zero_run_generation() {
    let directory = unique_test_dir("empty");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let mut builder = builder(&directory, &base, 1, None, config);
    builder.advance_empty(2).unwrap();
    let report = builder.finish(2, None).unwrap();
    assert_eq!(report.runs, 0);
    assert_eq!(report.entries, 0);
    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 2, config)
        .unwrap()
        .unwrap();
    assert_eq!(
        reader
            .visit_entries(|_, _, _, _| true)
            .unwrap()
            .entries_visited,
        0
    );

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn pinned_generation_remains_readable_after_new_publication() {
    let directory = unique_test_dir("pinned");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let mut first = builder(&directory, &base, 1, None, config);
    first.record(2, capture(2, Some("two-v2"))).unwrap();
    first.finish(2, None).unwrap();
    let pinned = RelationalRowDeltaReader::open_latest(&directory, &base, 2, config)
        .unwrap()
        .unwrap();
    let previous = pinned.manifest().generation();
    let mut second = builder(&directory, &base, 2, Some(previous), config);
    second.record(2, capture(2, Some("two-v2"))).unwrap();
    second.record(3, capture(3, Some("three"))).unwrap();
    second.finish(3, None).unwrap();

    let mut pinned_text = None;
    pinned
        .visit_entries(|_, _, value, _| {
            pinned_text = present_text(value).map(str::to_string);
            true
        })
        .unwrap();
    assert_eq!(pinned_text.as_deref(), Some("two-v2"));
    assert_eq!(pinned.manifest().generation(), previous);

    fs::remove_dir_all(directory).unwrap();
}

fn builder(
    directory: &Path,
    base: &RelationalRowPageRootReader,
    delta_generation: u64,
    expected_previous: Option<RelationalRowDeltaGeneration>,
    config: RelationalRowDeltaConfig,
) -> RelationalRowDeltaBuilder {
    RelationalRowDeltaBuilder::new(
        directory,
        base,
        delta_generation,
        expected_previous,
        table_metadata(),
        config,
    )
    .unwrap()
}

fn publish_and_open_base(directory: &Path) -> RelationalRowPageRootReader {
    let config = RelationalRowPagePublicationConfig::default();
    RelationalRowPagePublisher::new(config)
        .publish(
            directory,
            1,
            1,
            None,
            vec![RelationalRowPageTableDelta {
                replaces_base: false,
                last_row_id: 0,
                table: "documents".to_string(),
                schema: Some(crate::relational::row_page::test_row_page_schema(
                    "documents",
                    2,
                )),
                schema_digest: schema_digest(),
                column_count: NonZeroU32::new(2).unwrap(),
                next_page_id: NonZeroU64::new(2).unwrap(),
                dirty_pages: vec![ImmutableRelationalRowPage {
                    generation: 1,
                    source_commit_epoch: 1,
                    page_id: RelationalRowPageId::new(NonZeroU64::new(1).unwrap()),
                    schema_digest: schema_digest(),
                    column_count: 2,
                    rows: vec![RelationalRowPageEntry {
                        primary_key: key(1),
                        row: row(1, "one"),
                    }],
                }],
                deleted_page_ids: Vec::new(),
            }],
        )
        .unwrap();
    RelationalRowPageRootReader::open_latest(directory, config)
        .unwrap()
        .unwrap()
}

fn table_metadata() -> Vec<RelationalRowDeltaTableMetadata> {
    vec![RelationalRowDeltaTableMetadata {
        last_row_id: 0,
        table: "documents".to_string(),
        schema_digest: schema_digest(),
        column_count: NonZeroU32::new(2).unwrap(),
        row_count: 1,
    }]
}

fn capture(id: i64, body: Option<&str>) -> RelationalRowChangeCapture {
    RelationalRowChangeCapture::Captured {
        changes: vec![change(id, body)],
        encoded_bytes: 0,
    }
}

fn change(id: i64, body: Option<&str>) -> RelationalRowChange {
    RelationalRowChange {
        table: "documents".to_string(),
        primary_key: key(id),
        row: body.map(|body| row(id, body)),
    }
}

fn row(id: i64, body: &str) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::BigInt(id),
        RelationalValue::Text(body.to_string()),
    ])
}

fn key(id: i64) -> RelationalKey {
    RelationalKey(vec![RelationalValue::BigInt(id)])
}

fn schema_digest() -> hawdb_integrity::Sha256Digest {
    crate::relational::row_page::test_row_page_schema_digest("documents", 2)
}

fn present_text(value: &RelationalRowPageRecoveredValue) -> Option<&str> {
    match value {
        RelationalRowPageRecoveredValue::Present(row) => match &row.values()[1] {
            RelationalValue::Text(text) => Some(text),
            _ => None,
        },
        RelationalRowPageRecoveredValue::Deleted => None,
    }
}

fn flip_byte(path: &Path, offset: u64) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0xff;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
}

fn unique_test_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "hawdb-row-delta-{label}-{}-{}",
        std::process::id(),
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

#[test]
fn controlled_record_replacements_keep_key_and_latest_payload_leases_without_accumulation() {
    use crate::background::CheckpointWorkContext;
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let directory = unique_test_dir("record-replacement-leases");
    let base = publish_and_open_base(&directory);
    let mut config = RelationalRowDeltaConfig::default();
    config.row_limits.max_key_bytes = NonZeroUsize::new(256 * 1024).unwrap();
    config.row_limits.max_inline_value_bytes = NonZeroUsize::new(256 * 1024).unwrap();
    let mut builder = builder(&directory, &base, 1, None, config);
    let target = RelationalKey(vec![RelationalValue::Text("k".repeat(128 * 1024))]);
    let ceiling = 2 * 1024 * 1024;
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let capture = || RelationalRowChangeCapture::Captured {
        changes: vec![RelationalRowChange {
            table: "documents".into(),
            primary_key: target.clone(),
            row: Some(RelationalRow::new(vec![
                RelationalValue::Text("k".repeat(128 * 1024)),
                RelationalValue::Text("v".repeat(128 * 1024)),
            ])),
        }],
        encoded_bytes: 0,
    };
    for epoch in 2..=65 {
        builder
            .record_with_work_context(epoch, capture(), &work)
            .unwrap();
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                if available_bytes <= ceiling - 384 * 1024 && available_bytes > ceiling - 416 * 1024)
        );
    }
    assert_eq!(builder.visible_commit_epoch(), 65);
    let cloned = builder.clone();
    drop(builder);
    assert!(
        matches!(work.task_context().reserve_working_memory(ceiling),
        Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
            if available_bytes <= ceiling - 384 * 1024)
    );
    let (value, report) = cloned.lookup_staged("documents", &target).unwrap();
    assert_eq!(report.runs_read, 0);
    assert!(matches!(
        value,
        Some(RelationalRowPageRecoveredValue::Present(_))
    ));
    drop(value);
    drop(cloned);
    assert!(
        matches!(work.task_context().reserve_working_memory(ceiling),
        Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
    );
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(base);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn controlled_record_denies_key_and_row_buffers_before_wide_allocations() {
    use crate::background::{CheckpointOperationError, CheckpointWorkContext, CheckpointWorkError};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    for wide_key in [false, true] {
        let directory = unique_test_dir("record-buffer-denial");
        let base = publish_and_open_base(&directory);
        let mut config = RelationalRowDeltaConfig::default();
        config.row_limits.max_key_bytes = NonZeroUsize::new(256 * 1024).unwrap();
        config.row_limits.max_inline_value_bytes = NonZeroUsize::new(256 * 1024).unwrap();
        let mut builder = builder(&directory, &base, 1, None, config);
        let target = if wide_key {
            RelationalKey(vec![RelationalValue::Text("k".repeat(128 * 1024))])
        } else {
            key(2)
        };
        let capture = RelationalRowChangeCapture::Captured {
            changes: vec![RelationalRowChange {
                table: "documents".into(),
                primary_key: target,
                row: Some(row(2, &"v".repeat(128 * 1024))),
            }],
            encoded_bytes: 0,
        };
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(4096),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(4096))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let observation = crate::test_allocator::AllocationObservation::start();
        let result = work.classify(|scoped| builder.record_with_work_context(2, capture, scoped));
        let wide_allocations = observation.finish();
        assert!(
            matches!(
                result,
                Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                    _
                )))
            ),
            "{result:?}"
        );
        assert_eq!(wide_allocations, 0);
        assert_eq!(builder.visible_commit_epoch(), 1);
        assert!(!directory
            .join(relational_row_delta_run_file(1, 1, 0))
            .exists());
        drop(builder);
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        drop(base);
        fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn dirty_backing_cow_preserves_sorted_snapshot_and_denies_unfunded_copy() {
    use crate::background::CheckpointWorkContext;
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let make_entry = |id: u8| {
        (
            RowDeltaKey {
                table_ordinal: 0,
                encoded_primary_key: std::sync::Arc::new(vec![id]),
            },
            RowDeltaValue {
                is_present: false,
                encoded_row: Vec::new().into(),
                last_modified_epoch: u64::from(id),
                charged_bytes: 1,
                _key_memory: Default::default(),
            },
        )
    };
    let mut entries = super::DirtyEntries::default();
    for id in (0..64).rev() {
        let (key, value) = make_entry(id);
        entries
            .insert(key, value, &CheckpointWorkContext::default())
            .unwrap();
    }
    let snapshot = entries.clone();
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(4096),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(4096))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let (key, mut value) = make_entry(20);
    value.last_modified_epoch = 100;
    assert!(matches!(
        entries.insert(key, value, &work),
        Err(RelationalRowDeltaError::Admission(_))
    ));
    assert_eq!(
        entries
            .iter()
            .map(|(key, _)| key.encoded_primary_key[0])
            .collect::<Vec<_>>(),
        (0..64).collect::<Vec<_>>()
    );
    let (key, mut value) = make_entry(20);
    value.last_modified_epoch = 100;
    entries
        .insert(key, value, &CheckpointWorkContext::default())
        .unwrap();
    let (key, _) = make_entry(20);
    assert_eq!(entries.get(&key).unwrap().last_modified_epoch, 100);
    assert_eq!(snapshot.get(&key).unwrap().last_modified_epoch, 20);
    let (key, value) = make_entry(64);
    entries
        .insert(key, value, &CheckpointWorkContext::default())
        .unwrap();
    let probe = std::sync::Arc::new(crate::background::CheckpointWorkProbe::default());
    probe.cancel_after.store(3, Ordering::SeqCst);
    let scheduler = hawdb_qos::LocalQosScheduler::new(hawdb_qos::LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let cancelled = probe.context(scheduler.clone());
    let (mut key, value) = make_entry(0);
    key.encoded_primary_key = std::sync::Arc::new(Vec::new());
    assert!(matches!(
        entries.insert(key, value, &cancelled),
        Err(RelationalRowDeltaError::Admission(_))
    ));
    assert_eq!(
        entries
            .iter()
            .map(|(key, _)| key.encoded_primary_key[0])
            .collect::<Vec<_>>(),
        (0..65).collect::<Vec<_>>()
    );
    probe.assert_released(&scheduler);
    entries.clear();
    assert!(entries.is_empty());
    assert_eq!(snapshot.len(), 64);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
