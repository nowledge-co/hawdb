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
use hawdb_core::RuntimeCancellationToken;
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, QosTelemetryEvent, QosTelemetryPhase,
    QosTelemetrySink, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest, WorkClass, WorkRequest,
};
use std::sync::atomic::{AtomicUsize, Ordering};

fn resources() -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(512 * 1024 * 1024),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    )
}

fn root() -> PathBuf {
    std::env::temp_dir().join(format!(
        "hawdb-unit-qos-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ))
}

fn add(store: &mut GraphStore, catalog: &mut Catalog, id: i64) {
    store
        .create_node(
            catalog,
            "Memory",
            BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("body".into(), Value::String("p".repeat(512))),
            ]),
        )
        .unwrap();
}

#[test]
fn fresh_catch_up_keeps_local_admission_and_resumes_the_same_base_after_denial() {
    let root = root();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&root, &mut catalog).unwrap();
    for id in 0..2 {
        add(&mut store, &mut catalog, id);
    }
    let old = store.snapshot();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(4),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    });
    let governor = resources();
    let admission = governor
        .try_admit(
            RuntimeWorkRequest::background_maintenance(
                store.checkpoint_candidate_admission_bytes().unwrap(),
            )
            .with_io_wave_slots(1),
        )
        .unwrap();
    let initial = admission.bind_task_context(RuntimeTaskContext::default());
    let work =
        crate::background::CheckpointWorkContext::new(initial).with_scheduler(scheduler.clone());
    let source = store.checkpoint_source();
    let mut candidate = source
        .prepare_checkpoint_candidate_with_work_context(&catalog, &work)
        .unwrap()
        .unwrap();
    let generation = candidate.prepared.as_ref().unwrap().generation;
    let base = root.join(format!("checkpoint.{generation}.hawdb"));
    let base_bytes = fs::read(&base).unwrap();
    let private_wal = candidate
        .store
        .as_ref()
        .unwrap()
        .durable
        .as_ref()
        .unwrap()
        .wal_path
        .clone();
    let wal = fs::read(&private_wal).unwrap();
    add(&mut store, &mut catalog, 2);
    let captured = store.checkpoint_source();
    let competing = scheduler
        .try_start(WorkRequest::background(WorkClass::Mutation, 4))
        .unwrap();
    let fresh = admission.bind_task_context(RuntimeTaskContext::default());
    assert!(candidate
        .catch_up_with_task_context(&captured, &fresh)
        .is_err());
    assert!(candidate.can_continue_from(&captured));
    assert_eq!(fs::read(&base).unwrap(), base_bytes);
    assert_eq!(fs::read(&private_wal).unwrap(), wal);
    captured.ensure_usable().unwrap();
    store.ensure_usable().unwrap();
    competing.finish_with_outcome(true);
    let next = admission.bind_task_context(RuntimeTaskContext::default());
    assert_eq!(
        candidate
            .catch_up_with_task_context(&captured, &next)
            .unwrap()
            .entries,
        1
    );
    candidate.finish_catch_up().unwrap();
    assert_eq!(fs::read(&base).unwrap(), base_bytes);
    assert_eq!(scheduler.state().running_background_operations, 0);
    catalog = store
        .publish_checkpoint_candidate(&mut candidate, None, &BTreeSet::new())
        .unwrap();
    assert_eq!(store.node_count_for_label(None), 3);
    assert_eq!(old.node_count_for_label(None), 2);
    for id in 0..3 {
        assert_eq!(
            store.node_owned(NodeId(id)).unwrap().unwrap().properties["id"],
            Value::Int(id as i64)
        );
    }
    drop(candidate);
    drop(captured);
    drop(source);
    drop(old);
    drop(work);
    drop(next);
    drop(fresh);
    drop(admission);
    drop(store);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    let recovered = GraphStore::open(&root, &mut catalog).unwrap();
    assert_eq!(recovered.node_count_for_label(None), 3);
    for id in 0..3 {
        let node = recovered.node_owned(NodeId(id)).unwrap().unwrap();
        assert_eq!(node.properties["id"], Value::Int(id as i64));
        assert_eq!(node.properties["body"], Value::String("p".repeat(512)));
    }
    drop(recovered);
    fs::remove_dir_all(root).unwrap();
}

#[derive(Debug)]
struct CancelPlanning {
    calls: AtomicUsize,
    cancellation: RuntimeCancellationToken,
}

impl QosTelemetrySink for CancelPlanning {
    fn record_qos(&self, event: QosTelemetryEvent) {
        if event.phase == QosTelemetryPhase::Admission
            && self.calls.fetch_add(1, Ordering::SeqCst) + 1 == 10
        {
            assert_eq!(event.estimated_operations, 1);
            self.cancellation.cancel();
        }
    }
}

#[test]
fn planning_preserves_exact_size_and_cancellation_releases_the_last_unit() {
    let root = root();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&root, &mut catalog).unwrap();
    for id in 0..32 {
        add(&mut store, &mut catalog, id);
    }
    // Literal legacy size: node header + one label + id/body key/value cells.
    let expected = 128 * 1024 * 1024 + 16 * 32 * (32 + 4 + (2 + 8 + 16) + (4 + 512 + 16));
    assert_eq!(
        store.checkpoint_candidate_admission_bytes().unwrap(),
        expected
    );
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    });
    let work =
        crate::background::CheckpointWorkContext::default().with_scheduler(scheduler.clone());
    assert_eq!(
        store
            .checkpoint_candidate_admission_bytes_with_work_context(&work)
            .unwrap(),
        expected
    );
    let identity = store.checkpoint_source_identity();
    let cancellation = RuntimeCancellationToken::new();
    let observer = Arc::new(CancelPlanning {
        calls: AtomicUsize::new(0),
        cancellation: cancellation.clone(),
    });
    scheduler.set_telemetry_sink(Some(observer.clone()));
    let task = RuntimeTaskContext::without_deadline(cancellation);
    let cancelled =
        crate::background::CheckpointWorkContext::new(task).with_scheduler(scheduler.clone());
    assert!(store
        .checkpoint_candidate_admission_bytes_with_work_context(&cancelled)
        .is_err());
    assert_eq!(observer.calls.load(Ordering::SeqCst), 10);
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(store.checkpoint_source_identity(), identity);
    store.ensure_usable().unwrap();
    assert_eq!(
        store
            .checkpoint_candidate_admission_bytes_with_work_context(&work)
            .unwrap(),
        expected
    );
    drop(store);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn nested_graph_size_visits_every_cell_and_propagates_each_denial() {
    let value = Value::Map(BTreeMap::from([(
        "λ".into(),
        Value::List(vec![
            Value::Null,
            Value::Bool(true),
            Value::Int(i64::MAX),
            Value::Float(f64::NAN),
            Value::String("雪".into()),
            Value::Binary(vec![0; 5]),
            Value::Uuid(hawdb_core::generate_uuidv7().unwrap()),
            Value::Map(BTreeMap::from([("μ".into(), Value::Binary(vec![0; 3]))])),
        ]),
    )]));
    // UTF-8 keys, property cells, list header and all scalar variants.
    assert_eq!(crate::mutation::estimated_value_bytes(&value), 97);
    let mut visits = 0;
    let size = crate::mutation::estimated_value_bytes_with_visit(&value, &mut || {
        visits += 1;
        Ok::<_, usize>(())
    })
    .unwrap();
    assert_eq!(size, 97);
    assert_eq!(visits, 13);
    for denied in 1..=visits {
        let mut reached = 0;
        let result = crate::mutation::estimated_value_bytes_with_visit(&value, &mut || {
            reached += 1;
            if reached == denied {
                Err(denied)
            } else {
                Ok(())
            }
        });
        assert_eq!(result, Err(denied));
        assert_eq!(reached, denied);
    }
}
