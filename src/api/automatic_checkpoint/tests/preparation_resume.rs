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
use crate::Value;
use hawdb_qos::{QosTelemetryEvent, QosTelemetryPhase, QosTelemetrySink};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug)]
struct StopPreparedSource {
    checkpoint: PathBuf,
    cancellation: hawdb_core::RuntimeCancellationToken,
    stopped: AtomicBool,
}

impl QosTelemetrySink for StopPreparedSource {
    fn record_qos(&self, event: QosTelemetryEvent) {
        if event.phase == QosTelemetryPhase::Admission
            && self.checkpoint.exists()
            && !self.stopped.swap(true, Ordering::SeqCst)
        {
            self.cancellation.cancel();
        }
    }
}

fn add(store: &mut GraphStore, catalog: &mut Catalog, id: i64, label: &str) {
    store
        .create_node(
            catalog,
            label,
            BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("body".into(), Value::String("界🙂".repeat(128))),
            ]),
        )
        .unwrap();
}

fn parked_source(
    fixture: &Fixture,
    store: &GraphStore,
    catalog: &Catalog,
    resources: &RuntimeGovernor,
    scheduler: &LocalQosScheduler,
) -> Source {
    let task = RuntimeTaskContext::default();
    let stop = Arc::new(StopPreparedSource {
        checkpoint: fixture.0.join("checkpoint.1.hawdb"),
        cancellation: task.cancellation().clone(),
        stopped: AtomicBool::new(false),
    });
    scheduler.set_telemetry_sink(Some(stop.clone()));
    let mut source = Source::capture(store, catalog);
    let mut report = PreparationReport::default();
    assert!(prepare(&mut source, scheduler, resources, &task, &mut report).is_err());
    assert!(stop.stopped.load(Ordering::SeqCst));
    assert!(!report.operation_failed);
    let preparation = source
        .preparing
        .as_ref()
        .expect("retain original preparation");
    assert_eq!(
        preparation.frame.source_identity(),
        store.checkpoint_source_identity().unwrap()
    );
    assert!(preparation.admission.runtime.task_context().is_none());
    let paused = resources.snapshot();
    assert!(paused.admitted_memory_bytes > 0);
    assert_eq!(paused.active_cpu_slots, 0);
    assert_eq!(paused.active_background_tasks, 0);
    assert_eq!(paused.active_background_io_slots, 0);
    scheduler.set_telemetry_sink(None);
    source
}

#[test]
fn preparation_survives_new_source_submission_and_resumes_original_base_before_catch_up() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    for id in 0..128 {
        add(&mut store, &mut catalog, id, "Source");
    }
    let original = store.checkpoint_source_identity().unwrap();
    let resources = retirement_governor();
    let scheduler = LocalQosScheduler::new(hawdb_qos::LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let source = parked_source(&fixture, &store, &catalog, &resources, &scheduler);
    let control = Arc::new(Control::default());
    let mut state = control.lock().unwrap();
    state.enabled = true;
    state.last_identity = Some(original);
    state.latest = Some(source);
    add(&mut store, &mut catalog, 128, "Later");
    let retired = control.submit(&mut state, &store, &catalog).unwrap();
    assert!(retired.preparing.is_none());
    let mut latest = state.latest.take().unwrap();
    assert_eq!(
        latest.preparing.as_ref().unwrap().frame.source_identity(),
        original
    );
    drop(state);
    drop(retired);
    let mut report = PreparationReport::default();
    let (mut candidate, admission) = prepare(
        &mut latest,
        &scheduler,
        &resources,
        &RuntimeTaskContext::default(),
        &mut report,
    )
    .unwrap()
    .unwrap();
    assert_eq!(report.planning_scans, 0);
    assert_eq!(report.planning_cache_hits, 1);
    assert_eq!(
        candidate.commit_epoch(),
        128,
        "base must remain the original prefix"
    );
    candidate.catch_up(&store).unwrap();
    assert_eq!(candidate.commit_epoch(), 129);
    catalog = store
        .publish_checkpoint_candidate(&mut candidate, None, &BTreeSet::new())
        .unwrap();
    assert!(catalog.label_id("Later").is_some());
    drop(candidate);
    drop(admission);
    drop(latest);
    drop(store);
    assert_eq!(resources.snapshot().admitted_memory_bytes, 0);
    let recovered = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    assert_eq!(recovered.node_count_for_label(None), 129);
    for id in 0..129 {
        let node = recovered
            .node_owned(hawdb_storage::NodeId(id))
            .unwrap()
            .unwrap();
        assert_eq!(node.properties["id"], Value::Int(id as i64));
        assert_eq!(node.properties["body"], Value::String("界🙂".repeat(128)));
    }
}

#[test]
fn manual_suspension_destroys_parked_preparation_before_reusing_its_generation() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    for id in 0..32 {
        add(&mut store, &mut catalog, id, "Source");
    }
    let resources = retirement_governor();
    let scheduler = LocalQosScheduler::new(hawdb_qos::LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let source = parked_source(&fixture, &store, &catalog, &resources, &scheduler);
    let control = Arc::new(Control::default());
    control.lock().unwrap().latest = Some(source);
    let suspension = control.suspend().unwrap();
    assert_eq!(control.lock().unwrap().phase, Phase::Idle);
    assert!(control
        .lock()
        .unwrap()
        .latest
        .as_ref()
        .unwrap()
        .preparing
        .is_none());
    assert_eq!(resources.snapshot().admitted_memory_bytes, 0);
    store.checkpoint(&catalog).unwrap();
    drop(suspension);
    drop(control);
    drop(store);
    let recovered = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    assert_eq!(recovered.node_count_for_label(None), 32);
}

#[test]
fn stopping_owner_releases_parked_preparation_while_control_observers_survive() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    for id in 0..32 {
        add(&mut store, &mut catalog, id, "Source");
    }
    let resources = retirement_governor();
    let scheduler = LocalQosScheduler::new(hawdb_qos::LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let source = parked_source(&fixture, &store, &catalog, &resources, &scheduler);
    let control = Arc::new(Control::default());
    control.lock().unwrap().latest = Some(source);
    let mut owner = Owner {
        control: control.clone(),
        worker: None,
    };
    owner.stop();
    assert!(control.lock().unwrap().latest.is_none());
    assert_eq!(control.lock().unwrap().phase, Phase::Idle);
    assert_eq!(resources.snapshot().admitted_memory_bytes, 0);
    drop(owner);
    drop(store);
    let recovered = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    assert_eq!(recovered.node_count_for_label(None), 32);
}
