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
use crate::relational::RelationalColumnDefault;
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
use std::sync::atomic::Ordering;

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    })
}

fn source_rows(prepared: &PreparedCheckpoint) -> (Vec<u8>, Vec<u8>) {
    (
        fs::read(
            prepared
                .staging_path
                .join(source_scan::SOURCE_SCAN_DESCRIPTOR_FILE),
        )
        .unwrap(),
        fs::read(
            prepared
                .staging_path
                .join(source_scan::SOURCE_SCAN_PAYLOAD_FILE),
        )
        .unwrap(),
    )
}

fn discard(source: &GraphStore, prepared: PreparedCheckpoint) {
    source
        .durable
        .as_ref()
        .unwrap()
        .discard_prepared_checkpoint(prepared.generation, &prepared.staging_path)
        .unwrap();
}

struct ClearFailpoint;

impl Drop for ClearFailpoint {
    fn drop(&mut self) {
        set_checkpoint_failpoint(None);
    }
}

fn interrupted_preparation_keeps_original_source(cancel_io: bool) {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-preparation-state-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &directory,
        &mut catalog,
        DurabilityPolicy::default(),
        WalReplayConfig {
            residency_mode: StorageResidencyMode::Materialized,
            ..Default::default()
        },
    )
    .unwrap();
    for id in 0..128 {
        store
            .create_node(
                &mut catalog,
                "Source",
                BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("title".into(), Value::String(format!("source {id}"))),
                    ("body".into(), Value::String("界🙂".repeat(128))),
                ]),
            )
            .unwrap();
    }
    let identity = store.checkpoint_source_identity();
    let manifest_path = store
        .durable
        .as_ref()
        .unwrap()
        .manifest_path()
        .to_path_buf();
    let manifest = fs::read(&manifest_path).unwrap();
    let mut frame = store
        .begin_checkpoint_preparation(&catalog)
        .unwrap()
        .unwrap();
    let local = scheduler();
    let interrupted = Arc::new(CheckpointWorkProbe::default());
    if cancel_io {
        interrupted.cancel_on_io_wave.store(1, Ordering::SeqCst);
    } else {
        set_checkpoint_failpoint(Some(CheckpointPublishStage::WalPrepared));
    }
    let clear = ClearFailpoint;
    let work = interrupted.context(local.clone());
    assert!(frame.prepare_with_work_context(&work).is_err());
    drop(clear);
    interrupted.assert_released(&local);
    assert!(frame.plan.projected.is_some());
    assert!(frame.plan.source_scan.as_ref().unwrap().is_some());
    assert!(!frame.finished);
    assert_eq!(store.checkpoint_source_identity(), identity);
    assert_eq!(fs::read(&manifest_path).unwrap(), manifest);
    store.ensure_usable().unwrap();
    drop(work);

    // Later writes cannot replace the owned source or its catalog. The retry
    // must prepare the original 128-row prefix before candidate catch-up.
    store
        .create_node(
            &mut catalog,
            "Later",
            BTreeMap::from([("id".into(), Value::Int(128))]),
        )
        .unwrap();
    assert_eq!(frame.source.commit_epoch(), 128);
    assert_eq!(frame.source.node_count_for_label(None), 128);
    assert!(frame.catalog.label_id("Later").is_none());
    assert_eq!(store.commit_epoch(), 129);
    let latest_identity = store.checkpoint_source_identity();
    let wal_path = &store.durable.as_ref().unwrap().wal_path;
    let wal = fs::read(wal_path).unwrap();

    // A fresh ordinary preparation supplies complete deterministic source-row
    // bytes and the actual work count for the same captured source.
    let baseline = Arc::new(CheckpointWorkProbe::default());
    let fresh = baseline.context(local.clone());
    let prepared = frame
        .source
        .prepare_checkpoint_with_work_context(&frame.catalog, &fresh)
        .unwrap()
        .unwrap();
    let expected_rows = source_rows(&prepared);
    let fresh_units = baseline.completed.load(Ordering::SeqCst);
    baseline.assert_released(&local);
    discard(&frame.source, prepared);
    drop(fresh);

    let resumed = Arc::new(CheckpointWorkProbe::default());
    let fresh = resumed.context(local.clone());
    let prepared = frame.prepare_with_work_context(&fresh).unwrap().unwrap();
    assert_eq!(prepared.source_commit_epoch, 128);
    assert_eq!(source_rows(&prepared), expected_rows);
    assert!(
        resumed.completed.load(Ordering::SeqCst) < fresh_units,
        "completed projection/source-row planning must survive the interruption"
    );
    resumed.assert_released(&local);
    assert_eq!(store.checkpoint_source_identity(), latest_identity);
    assert_eq!(fs::read(wal_path).unwrap(), wal);
    assert_eq!(fs::read(&manifest_path).unwrap(), manifest);
    assert!(frame.finished);
    assert!(frame.prepare_with_work_context(&fresh).is_err());
    discard(&frame.source, prepared);
    drop(fresh);
    drop(frame);
    drop(store);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn preparation_state_resumes_completed_plans_after_io_cancellation_without_recapturing() {
    interrupted_preparation_keeps_original_source(true);
}

#[test]
fn preparation_state_rewrites_source_ranges_after_late_failure_and_preserves_every_row() {
    interrupted_preparation_keeps_original_source(false);
}

#[test]
fn in_memory_source_needs_no_checkpoint_preparation_state() {
    assert!(GraphStore::in_memory()
        .begin_checkpoint_preparation(&Catalog::default())
        .unwrap()
        .is_none());
}

#[test]
fn cancelled_bootstrap_keeps_completed_base_and_recovers_with_a_fresh_context() {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-preparation-bootstrap-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&directory, &mut catalog).unwrap();
    for id in 0..32 {
        store
            .create_node(
                &mut catalog,
                "Source",
                BTreeMap::from([("id".into(), Value::Int(id))]),
            )
            .unwrap();
    }
    let identity = store.checkpoint_source_identity().unwrap();
    let mut frame = store
        .begin_checkpoint_preparation(&catalog)
        .unwrap()
        .unwrap();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    let prepared = frame.prepare_with_work_context(&work).unwrap().unwrap();
    let base =
        fs::read(directory.join(format!("checkpoint.{}.hawdb", prepared.generation))).unwrap();
    let generation = prepared.generation;
    // Ownership starts before mounting. This boundary also exists when the
    // original execution is stopped after its complete prepared base returns.
    frame.bootstrap = Some(frame.source.checkpoint_candidate_from_prepared(
        &frame.catalog,
        prepared,
        &work,
    ));
    probe.cancellation.cancel();
    let error = frame
        .prepare_candidate_with_work_context(&work)
        .unwrap_err();
    assert!(matches!(error, HawDBError::RuntimeTaskStopped(_)));
    assert!(frame.finished);
    assert!(frame.bootstrap.is_some());
    probe.assert_released(&local);
    drop(work);
    store
        .create_node(
            &mut catalog,
            "Later",
            BTreeMap::from([("id".into(), Value::Int(32))]),
        )
        .unwrap();
    assert_eq!(frame.source_identity(), identity);
    assert!(frame.can_continue_from(&store));
    let resumed = Arc::new(CheckpointWorkProbe::default());
    let fresh = resumed.context(local.clone());
    let mut candidate = frame
        .prepare_candidate_with_work_context(&fresh)
        .unwrap()
        .unwrap();
    assert!(frame.bootstrap.is_none());
    assert_eq!(candidate.commit_epoch(), 32);
    assert_eq!(
        fs::read(directory.join(format!("checkpoint.{generation}.hawdb"))).unwrap(),
        base,
        "bootstrap resume must reuse the complete prepared base"
    );
    candidate
        .catch_up_with_work_context(&store, &fresh)
        .unwrap();
    catalog = store
        .publish_checkpoint_candidate(&mut candidate, None, &BTreeSet::new())
        .unwrap();
    resumed.assert_released(&local);
    assert!(
        !frame.can_continue_from(&store),
        "publication invalidates old preparation"
    );
    drop(candidate);
    drop(fresh);
    drop(frame);
    drop(store);
    let recovered = GraphStore::open(&directory, &mut catalog).unwrap();
    assert_eq!(recovered.node_count_for_label(None), 33);
    assert!(catalog.label_id("Later").is_some());
    for id in 0..33 {
        assert_eq!(
            recovered
                .node_owned(NodeId(id))
                .unwrap()
                .unwrap()
                .properties["id"],
            Value::Int(id as i64)
        );
    }
    drop(recovered);
    fs::remove_dir_all(directory).unwrap();
}

struct AppendBootstrapFixture {
    frame: CheckpointPreparation,
    store: GraphStore,
    catalog: Catalog,
    directory: PathBuf,
    wide_name: String,
    identity: Option<CheckpointSourceIdentity>,
    manifest: Vec<u8>,
    generation: u64,
    base: Vec<u8>,
}

fn append_bootstrap_fixture() -> AppendBootstrapFixture {
    append_bootstrap_fixture_with_column("s".repeat(512 * 1024), RelationalScalarType::BigInt, None)
}

fn append_bootstrap_fixture_with_column(
    wide_name: String,
    scalar_type: RelationalScalarType,
    default: Option<RelationalColumnDefault>,
) -> AppendBootstrapFixture {
    use crate::append_table::{AppendOrderMode, AppendTableSchema, AppendTransaction, AppendWrite};
    let directory = std::env::temp_dir().join(format!(
        "hawdb-bootstrap-append-memory-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&directory, &mut catalog).unwrap();
    store
        .append_transaction(AppendTransaction {
            writes: vec![AppendWrite::CreateTable {
                schema: AppendTableSchema {
                    name: "events".into(),
                    columns: ["stream", "sequence", wide_name.as_str()]
                        .into_iter()
                        .map(|name| RelationalColumnSchema {
                            name: name.into(),
                            scalar_type: if name == wide_name {
                                scalar_type
                            } else {
                                RelationalScalarType::BigInt
                            },
                            nullable: false,
                            default: if name == wide_name {
                                default.clone()
                            } else {
                                None
                            },
                        })
                        .collect(),
                    partition_key: vec!["stream".into()],
                    order_key: vec!["sequence".into()],
                    order_mode: AppendOrderMode::CallerProvided,
                },
            }],
        })
        .unwrap();
    let original_identity = store.checkpoint_source_identity();
    let original_manifest = fs::read(store.durable.as_ref().unwrap().manifest_path()).unwrap();
    let mut frame = store
        .begin_checkpoint_preparation(&catalog)
        .unwrap()
        .unwrap();
    let ordinary = crate::background::CheckpointWorkContext::default();
    let prepared = frame.prepare_with_work_context(&ordinary).unwrap().unwrap();
    let generation = prepared.generation;
    let base = fs::read(directory.join(format!("checkpoint.{generation}.hawdb"))).unwrap();
    frame.bootstrap = Some(frame.source.checkpoint_candidate_from_prepared(
        &frame.catalog,
        prepared,
        &ordinary,
    ));
    AppendBootstrapFixture {
        frame,
        store,
        catalog,
        directory,
        wide_name,
        identity: original_identity,
        manifest: original_manifest,
        generation,
        base,
    }
}

fn append_governor() -> hawdb_qos::RuntimeGovernor {
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
        RuntimeResourceBudget, RuntimeResourceSnapshot,
    };
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(4 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    governor
}

#[test]
fn bootstrap_denies_before_copying_a_wide_append_schema() {
    assert_append_bootstrap_denial(append_bootstrap_fixture());
}

#[test]
fn bootstrap_denies_before_copying_a_wide_append_default() {
    assert_append_bootstrap_denial(append_bootstrap_fixture_with_column(
        "payload".into(),
        RelationalScalarType::Bytea,
        Some(RelationalColumnDefault::Literal(RelationalValue::Bytea(
            vec![23; 512 * 1024],
        ))),
    ));
}

fn assert_append_bootstrap_denial(fixture: AppendBootstrapFixture) {
    use crate::background::{CheckpointOperationError, CheckpointWorkError};
    use hawdb_core::RuntimeTaskContext;

    let AppendBootstrapFixture {
        mut frame,
        store,
        mut catalog,
        directory,
        wide_name,
        identity: original_identity,
        manifest: original_manifest,
        generation: _,
        base: _,
    } = fixture;
    let expected_schema = store.append_table_schema("events").unwrap().clone();
    let governor = append_governor();
    let admission = governor
        .try_admit_incremental_maintenance(0, 32 * 1024, 1, RuntimeTaskContext::default())
        .unwrap();
    let local = scheduler();
    let work =
        crate::background::CheckpointWorkContext::new(admission.task_context().unwrap().clone())
            .with_scheduler(local.clone());
    let observer = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| frame.prepare_candidate_with_work_context(work));
    let copies = observer.finish();
    let denied = matches!(
        result,
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            _
        )))
    );
    let scalar_denied = matches!(&result,
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            hawdb_core::RuntimeMemoryError::ReservationExceeded {requested_bytes, available_bytes}
        ))) if *requested_bytes == 512 * 1024 && *available_bytes == 32 * 1024);
    drop(result);
    drop(frame);
    assert_eq!(store.checkpoint_source_identity(), original_identity);
    assert_eq!(
        fs::read(store.durable.as_ref().unwrap().manifest_path()).unwrap(),
        original_manifest
    );
    assert_eq!(
        store.append_table_schema("events").unwrap().columns[2].name,
        wide_name
    );
    assert_eq!(store.append_table_schema("events"), Some(&expected_schema));
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    assert_eq!(local.state().running_background_operations, 0);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(store);
    let recovered = GraphStore::open(&directory, &mut catalog).unwrap();
    assert_eq!(
        recovered.append_table_schema("events").unwrap().columns[2].name,
        wide_name
    );
    assert_eq!(
        recovered.append_table_schema("events"),
        Some(&expected_schema)
    );
    drop(recovered);
    fs::remove_dir_all(directory).unwrap();
    assert!(
        denied,
        "32 KiB bootstrap admitted a wide schema: {copies} large copies"
    );
    assert_eq!(copies, 0, "deny before copying the 512 KiB schema name");
    assert!(
        scalar_denied,
        "metadata must fit before the wide value is denied"
    );
}

#[test]
fn append_bootstrap_resumes_the_same_base_and_keeps_leases_through_the_last_snapshot() {
    use crate::background::{CheckpointOperationError, CheckpointWorkError};
    use hawdb_core::RuntimeTaskContext;

    let AppendBootstrapFixture {
        mut frame,
        mut store,
        catalog: _,
        directory,
        wide_name,
        identity,
        manifest,
        generation,
        base,
    } = append_bootstrap_fixture();
    let governor = append_governor();
    let small = governor
        .try_admit_incremental_maintenance(0, 32 * 1024, 1, RuntimeTaskContext::default())
        .unwrap();
    let local = scheduler();
    let work = crate::background::CheckpointWorkContext::new(small.task_context().unwrap().clone())
        .with_scheduler(local.clone());
    assert!(matches!(
        work.classify(|work| frame.prepare_candidate_with_work_context(work)),
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            _
        )))
    ));
    assert!(frame.finished);
    assert!(frame.bootstrap.is_some());
    assert_eq!(frame.source.checkpoint_source_identity(), identity);
    assert_eq!(store.checkpoint_source_identity(), identity);
    assert_eq!(
        fs::read(store.durable.as_ref().unwrap().manifest_path()).unwrap(),
        manifest
    );
    assert_eq!(small.memory_report().live_accounted_bytes, 0);
    drop(work);
    drop(small);

    // A fresh independently admitted execution keeps the original completed
    // base. The denied 32 KiB attempt must remain a denial under that ceiling.
    let admission = governor
        .try_admit_incremental_maintenance(0, 1024 * 1024, 1, RuntimeTaskContext::default())
        .unwrap();
    let work =
        crate::background::CheckpointWorkContext::new(admission.task_context().unwrap().clone())
            .with_scheduler(local.clone());
    let observer = crate::test_allocator::AllocationObservation::start();
    let mut candidate = frame
        .prepare_candidate_with_work_context(&work)
        .unwrap()
        .unwrap();
    assert_eq!(
        observer.finish(),
        1,
        "copy the wide schema once after admission"
    );
    assert_eq!(
        fs::read(directory.join(format!("checkpoint.{generation}.hawdb"))).unwrap(),
        base
    );
    assert!(admission.memory_report().live_accounted_bytes >= 512 * 1024);
    let mut catalog = store
        .publish_checkpoint_candidate(&mut candidate, None, &BTreeSet::new())
        .unwrap();
    let bytes = admission.memory_report().live_accounted_bytes;
    let observer = crate::test_allocator::AllocationObservation::start();
    let snapshot = store.snapshot();
    let detached = store.append_state().clone();
    assert_eq!(observer.finish(), 0);
    assert_eq!(admission.memory_report().live_accounted_bytes, bytes);
    assert_eq!(
        snapshot.append_table_schema("events").unwrap().columns[2].name,
        wide_name
    );
    drop(candidate);
    drop(frame);
    drop(store);
    assert_eq!(admission.memory_report().live_accounted_bytes, bytes);
    assert!(admission.memory_report().peak_accounted_bytes <= 1024 * 1024);
    drop(snapshot);
    assert_eq!(admission.memory_report().live_accounted_bytes, bytes);
    assert_eq!(
        detached.schema("events").unwrap().columns[2].name,
        wide_name
    );
    drop(detached);
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    assert_eq!(local.state().running_background_operations, 0);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    let recovered = GraphStore::open(&directory, &mut catalog).unwrap();
    assert_eq!(
        recovered.append_table_schema("events").unwrap().columns[2].name,
        wide_name
    );
    drop(recovered);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn repeated_bounded_preparation_attempts_finish_without_restarting_every_unit() {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-bounded-preparation-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&directory, &mut catalog).unwrap();
    for id in 0..64 {
        store
            .create_node(
                &mut catalog,
                "Source",
                BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("title".into(), Value::String(format!("source {id}"))),
                    ("body".into(), Value::String("界🙂".repeat(128))),
                ]),
            )
            .unwrap();
    }
    let identity = store.checkpoint_source_identity();
    let local = scheduler();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    let work = baseline.context(local.clone());
    let prepared = store
        .prepare_checkpoint_with_work_context(&catalog, &work)
        .unwrap()
        .unwrap();
    let expected_rows = source_rows(&prepared);
    let total_units = baseline.completed.load(Ordering::SeqCst);
    baseline.assert_released(&local);
    discard(&store, prepared);
    drop(work);
    // Keep the measured baseline quantum fixed across implementation changes.
    let quantum = 2842;
    let mut frame = store
        .begin_checkpoint_preparation(&catalog)
        .unwrap()
        .unwrap();
    let mut completed = None;
    for attempt in 1..=16 {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(quantum, Ordering::SeqCst);
        let work = probe.context(local.clone());
        let result = frame.prepare_with_work_context(&work);
        probe.assert_released(&local);
        eprintln!("bounded-preparation attempt={attempt} units={} quantum={quantum} baseline={total_units} projected={} source_scan={} finished={}",
            probe.completed.load(Ordering::SeqCst), frame.plan.projected.is_some(), frame.plan.source_scan.is_some(), frame.finished);
        assert_eq!(store.checkpoint_source_identity(), identity);
        store.ensure_usable().unwrap();
        if let Err(error) = &result {
            eprintln!("bounded-preparation attempt={attempt} error={error:?}");
        }
        if let Ok(Some(prepared)) = result {
            completed = Some(prepared);
            break;
        }
        assert!(
            probe.cancellation.is_cancelled(),
            "attempt failed without the controlled cancellation"
        );
    }
    let finished_with_bounded_attempts = completed.is_some();
    // Verify that retries did not damage the captured source even when the
    // bounded progress contract fails. This unrestricted attempt is diagnostic,
    // never evidence that bounded work eventually completes.
    let recovery = Arc::new(CheckpointWorkProbe::default());
    let work = recovery.context(local.clone());
    let prepared =
        completed.unwrap_or_else(|| frame.prepare_with_work_context(&work).unwrap().unwrap());
    assert_eq!(source_rows(&prepared), expected_rows);
    assert_eq!(prepared.source_commit_epoch, 64);
    recovery.assert_released(&local);
    discard(&store, prepared);
    drop(work);
    drop(frame);
    drop(store);
    fs::remove_dir_all(directory).unwrap();
    assert!(
        finished_with_bounded_attempts,
        "16 attempts with quantum {quantum} did not complete a {total_units}-unit preparation"
    );
}

fn retained_stage_survives_denial_then_finishes_or_is_abandoned(finish: bool) {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-retained-stage-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&directory, &mut catalog).unwrap();
    store
        .create_node(
            &mut catalog,
            "Source",
            BTreeMap::from([("id".into(), Value::Int(17))]),
        )
        .unwrap();
    let manifest_path = store
        .durable
        .as_ref()
        .unwrap()
        .manifest_path()
        .to_path_buf();
    let manifest_before = fs::read(&manifest_path).unwrap();
    let identity = store.checkpoint_source_identity();
    let mut frame = store
        .begin_checkpoint_preparation(&catalog)
        .unwrap()
        .unwrap();
    let local = scheduler();
    let mut captured = None;
    for attempt in 0..2 {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_on_io_wave.store(1, Ordering::SeqCst);
        let stopped = probe.context(local.clone());
        assert!(frame.prepare_with_work_context(&stopped).is_err());
        assert!(probe.cancellation.is_cancelled());
        probe.assert_released(&local);
        let stage = frame
            .plan
            .stage
            .as_ref()
            .expect("safe IO denial retains generation");
        if attempt == 0 {
            fs::write(stage.path.join("owned-marker"), b"retained staging").unwrap();
            assert!(frame
                .source
                .durable
                .as_ref()
                .unwrap()
                .prepare_checkpoint_staging(stage.generation)
                .is_err());
            assert_eq!(
                fs::read(stage.path.join("owned-marker")).unwrap(),
                b"retained staging"
            );
            captured = Some((stage.generation, stage.path.clone()));
        } else {
            let (generation, path) = captured.as_ref().unwrap();
            assert_eq!(stage.generation, *generation);
            assert_eq!(&stage.path, path);
            assert_eq!(
                fs::read(path.join("owned-marker")).unwrap(),
                b"retained staging"
            );
        }
        assert_eq!(store.checkpoint_source_identity(), identity);
        assert_eq!(fs::read(&manifest_path).unwrap(), manifest_before);
    }
    let (generation, path) = captured.unwrap();
    if finish {
        let fresh = crate::background::CheckpointWorkContext::default();
        let prepared = frame.prepare_with_work_context(&fresh).unwrap().unwrap();
        assert_eq!(prepared.generation, generation);
        assert_eq!(prepared.staging_path, path);
        assert!(frame.plan.stage.is_none());
        drop(frame);
        assert!(
            path.exists(),
            "prepared result owns staging after source frame drops"
        );
        discard(&store, prepared);
    } else {
        drop(frame);
    }
    assert!(
        !path.exists(),
        "abandonment or explicit discard removes owned staging"
    );
    assert_eq!(fs::read(&manifest_path).unwrap(), manifest_before);
    drop(store);
    let recovered = GraphStore::open(&directory, &mut catalog).unwrap();
    assert_eq!(recovered.node_count_for_label(None), 1);
    assert_eq!(
        recovered.node_owned(NodeId(0)).unwrap().unwrap().properties["id"],
        Value::Int(17)
    );
    drop(recovered);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn safe_io_denial_keeps_staging_identity_until_preparation_finishes() {
    retained_stage_survives_denial_then_finishes_or_is_abandoned(true);
}

#[test]
fn abandoning_denied_preparation_cleans_its_owned_staging_and_preserves_source() {
    retained_stage_survives_denial_then_finishes_or_is_abandoned(false);
}
