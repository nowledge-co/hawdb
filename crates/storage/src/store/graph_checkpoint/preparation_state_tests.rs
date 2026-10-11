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
    assert!(matches!(error, HawDBError::Storage(message) if message.contains("cancelled")));
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
