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
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot,
};

#[test]
fn allocation_admitted_candidate_finishes_beyond_whole_estimate_with_a_fixed_small_owner() {
    const ROWS: i64 = 1025;
    const BUDGET: u64 = 16 * 1024 * 1024;
    const OWNER: u64 = 64 * 1024;
    let root = std::env::temp_dir().join(format!(
        "hawdb-incremental-checkpoint-memory-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&root, &mut catalog).unwrap();
    assert!(store.begin_wal_sync_group().unwrap());
    for id in 0..ROWS {
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("body".into(), Value::String("m".repeat(512))),
                ]),
            )
            .unwrap();
    }
    store.finish_wal_sync_group().unwrap();
    let old = store.snapshot();
    let source = store.checkpoint_source();
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(BUDGET),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    let whole = source.checkpoint_candidate_admission_bytes().unwrap();
    assert!(whole > BUDGET);
    let denial = governor
        .try_admit_resumable_maintenance(whole, 1, RuntimeTaskContext::default())
        .unwrap_err();
    assert!(!denial.is_retryable());
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    let mut admission = governor
        .try_admit_incremental_maintenance(OWNER, BUDGET, 1, RuntimeTaskContext::default())
        .unwrap();
    assert_eq!(governor.snapshot().admitted_memory_bytes, OWNER);
    let scheduler = LocalQosScheduler::new(LocalQosPolicy::default());
    let context =
        crate::background::CheckpointWorkContext::new(admission.task_context().unwrap().clone())
            .with_scheduler(scheduler.clone());
    let mut candidate = source
        .prepare_checkpoint_candidate_with_work_context(&catalog, &context)
        .unwrap()
        .unwrap();
    let generation = candidate.prepared.as_ref().unwrap().generation;
    let base = root.join(format!("checkpoint.{generation}.hawdb"));
    let base_bytes = fs::read(&base).unwrap();
    let prepared_charge = governor.snapshot().admitted_memory_bytes;
    let prepared_memory = admission.memory_report();
    // This materialized source shares its original COW records. Preparation
    // used and then released its admitted scratch buffers; no decoded suffix
    // allocations have entered the private runtime yet.
    assert_eq!(prepared_charge, OWNER);
    assert_eq!(prepared_memory.live_accounted_bytes, 0);
    assert!(prepared_memory.peak_accounted_bytes > 0);
    assert!(OWNER + prepared_memory.peak_accounted_bytes <= BUDGET);
    admission.pause();
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, prepared_charge);
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".into(), Value::Int(ROWS)),
                ("body".into(), Value::String("m".repeat(512))),
            ]),
        )
        .unwrap();
    let captured = store.checkpoint_source();
    admission.try_resume(RuntimeTaskContext::default()).unwrap();
    assert_eq!(
        candidate
            .catch_up_with_task_context(&captured, admission.task_context().unwrap())
            .unwrap()
            .entries,
        1
    );
    candidate.finish_catch_up().unwrap();
    assert!(admission.memory_report().live_accounted_bytes > 0);
    catalog = store
        .publish_checkpoint_candidate(&mut candidate, None, &BTreeSet::new())
        .unwrap();
    assert_eq!(fs::read(&base).unwrap(), base_bytes);
    assert_eq!(old.node_count_for_label(None), ROWS as usize);
    assert_eq!(store.node_count_for_label(None), (ROWS + 1) as usize);
    for id in 0..=ROWS {
        let node = store.node_owned(NodeId(id as u64)).unwrap().unwrap();
        assert_eq!(node.properties["id"], Value::Int(id));
        assert_eq!(node.properties["body"], Value::String("m".repeat(512)));
        if id < ROWS {
            let pinned = old.node_owned(NodeId(id as u64)).unwrap().unwrap();
            assert_eq!(pinned.properties, node.properties);
        }
    }
    assert_eq!(scheduler.state().running_background_operations, 0);
    println!("incremental-checkpoint-memory rows={ROWS} whole_estimate={whole} governor_budget={BUDGET} fixed_owner={OWNER} retained_accounted_bytes={prepared_charge}");
    drop(candidate);
    drop(captured);
    drop(context);
    drop(admission);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert!(governor.snapshot().admitted_memory_bytes > 0);
    drop(store);
    drop(source);
    drop(old);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    let recovered = GraphStore::open(&root, &mut catalog).unwrap();
    assert_eq!(recovered.node_count_for_label(None), (ROWS + 1) as usize);
    for id in 0..=ROWS {
        let node = recovered.node_owned(NodeId(id as u64)).unwrap().unwrap();
        assert_eq!(node.properties["id"], Value::Int(id));
        assert_eq!(node.properties["body"], Value::String("m".repeat(512)));
    }
    drop(recovered);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn controlled_replay_denies_before_detaching_a_wide_pinned_graph_page() {
    const OWNER: u64 = 4096;
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    let body = "索引".repeat(64 * 1024);
    let id = store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("body".into(), Value::String(body.clone())),
                ("rank".into(), Value::Int(0)),
            ]),
        )
        .unwrap();
    let pinned = store.snapshot();
    let epoch = store.commit_epoch();
    assert!(store.nodes.shares_storage_with(&pinned.nodes));
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(4 * 1024 * 1024),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    let admission = governor
        .try_admit_incremental_maintenance(OWNER, OWNER, 1, RuntimeTaskContext::default())
        .unwrap();
    let work =
        crate::background::CheckpointWorkContext::new(admission.task_context().unwrap().clone());
    let operation = WalOp::SetNodeProperty {
        id,
        property: "rank".into(),
        value: Value::Int(1),
    };
    let mut mutation_started = false;
    let observer = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| {
        store.apply_replayed_checkpoint_wal_transaction_with_boundary(
            &mut catalog,
            operation,
            work,
            &mut mutation_started,
            None,
        )
    });
    let large_allocations = observer.finish();
    assert_eq!(
        large_allocations, 0,
        "controlled WAL replay must admit pinned page copies before large allocation"
    );
    assert!(matches!(
        result,
        Err(crate::background::CheckpointOperationError::Work(
            crate::background::CheckpointWorkError::Memory(_)
        ))
    ));
    assert!(
        !mutation_started,
        "memory denial must precede transaction mutation"
    );
    assert_eq!(store.commit_epoch(), epoch);
    assert!(store.nodes.shares_storage_with(&pinned.nodes));
    for handle in [&store, &pinned] {
        let node = handle.node_owned(id).unwrap().unwrap();
        assert_eq!(node.properties["body"], Value::String(body.clone()));
        assert_eq!(node.properties["rank"], Value::Int(0));
    }
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    drop(work);
    drop(admission);
    drop(store);
    drop(pinned);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn controlled_nested_relationship_replay_denies_before_wide_pinned_page_copy() {
    const OWNER: u64 = 4096;
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    let body = "索引".repeat(64 * 1024);
    let a = store
        .create_node(&mut catalog, "Memory", BTreeMap::new())
        .unwrap();
    let b = store
        .create_node(&mut catalog, "Memory", BTreeMap::new())
        .unwrap();
    let id = store
        .create_relationship(
            &mut catalog,
            a,
            b,
            "Memory",
            BTreeMap::from([
                ("body".into(), Value::String(body.clone())),
                ("rank".into(), Value::Int(0)),
            ]),
        )
        .unwrap();
    let pinned = store.snapshot();
    let epoch = store.commit_epoch();
    assert!(store
        .relationships
        .shares_storage_with(&pinned.relationships));
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(4 * 1024 * 1024),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    let admission = governor
        .try_admit_incremental_maintenance(OWNER, OWNER, 1, RuntimeTaskContext::default())
        .unwrap();
    let work =
        crate::background::CheckpointWorkContext::new(admission.task_context().unwrap().clone());
    let operation = WalOp::Batch(vec![WalOp::Batch(vec![WalOp::SetRelationshipProperty {
        id,
        property: "rank".into(),
        value: Value::Int(1),
    }])]);
    let mut mutation_started = false;
    let observer = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| {
        store.apply_replayed_checkpoint_wal_transaction_with_boundary(
            &mut catalog,
            operation,
            work,
            &mut mutation_started,
            None,
        )
    });
    let large_allocations = observer.finish();
    assert_eq!(
        large_allocations, 0,
        "controlled WAL replay must admit pinned page copies before large allocation"
    );
    assert!(matches!(
        result,
        Err(crate::background::CheckpointOperationError::Work(
            crate::background::CheckpointWorkError::Memory(_)
        ))
    ));
    assert!(
        !mutation_started,
        "memory denial must precede transaction mutation"
    );
    assert_eq!(store.commit_epoch(), epoch);
    assert!(store
        .relationships
        .shares_storage_with(&pinned.relationships));
    for handle in [&store, &pinned] {
        let node = handle.relationship_owned(id).unwrap().unwrap();
        assert_eq!(node.properties["body"], Value::String(body.clone()));
        assert_eq!(node.properties["rank"], Value::Int(0));
    }
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    drop(work);
    drop(admission);
    drop(store);
    drop(pinned);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

fn assert_node_record_replay_denies_before_pinned_copy(
    operation: impl FnOnce(NodeId, NodeId) -> WalOp,
) {
    const OWNER: u64 = 4096;
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    let body = "索引".repeat(64 * 1024);
    let id = store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("body".into(), Value::String(body.clone())),
                ("rank".into(), Value::Int(0)),
            ]),
        )
        .unwrap();
    let pinned = store.snapshot();
    let epoch = store.commit_epoch();
    assert!(store.nodes.shares_storage_with(&pinned.nodes));
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(4 * 1024 * 1024),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    let admission = governor
        .try_admit_incremental_maintenance(OWNER, OWNER, 1, RuntimeTaskContext::default())
        .unwrap();
    let work =
        crate::background::CheckpointWorkContext::new(admission.task_context().unwrap().clone());
    let created = NodeId(id.0 + 1);
    let operation = operation(id, created);
    let mut mutation_started = false;
    let observer = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| {
        store.apply_replayed_checkpoint_wal_transaction_with_boundary(
            &mut catalog,
            operation,
            work,
            &mut mutation_started,
            None,
        )
    });
    let large_allocations = observer.finish();
    assert_eq!(
        large_allocations, 0,
        "controlled WAL replay must admit pinned page copies before large allocation"
    );
    assert!(matches!(
        result,
        Err(crate::background::CheckpointOperationError::Work(
            crate::background::CheckpointWorkError::Memory(_)
        ))
    ));
    assert!(
        !mutation_started,
        "memory denial must precede transaction mutation"
    );
    assert_eq!(store.commit_epoch(), epoch);
    assert!(store.node_owned(created).unwrap().is_none());
    assert!(store.nodes.shares_storage_with(&pinned.nodes));
    for handle in [&store, &pinned] {
        let node = handle.node_owned(id).unwrap().unwrap();
        assert_eq!(node.properties["body"], Value::String(body.clone()));
        assert_eq!(node.properties["rank"], Value::Int(0));
    }
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    drop(work);
    drop(admission);
    drop(store);
    drop(pinned);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

fn assert_relationship_record_replay_denies_before_pinned_copy(
    operation: impl FnOnce(RelId, NodeId, NodeId) -> WalOp,
) {
    const OWNER: u64 = 4096;
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    let body = "索引".repeat(64 * 1024);
    let a = store
        .create_node(&mut catalog, "Memory", BTreeMap::new())
        .unwrap();
    let b = store
        .create_node(&mut catalog, "Memory", BTreeMap::new())
        .unwrap();
    let id = store
        .create_relationship(
            &mut catalog,
            a,
            b,
            "Memory",
            BTreeMap::from([
                ("body".into(), Value::String(body.clone())),
                ("rank".into(), Value::Int(0)),
            ]),
        )
        .unwrap();
    let pinned = store.snapshot();
    let epoch = store.commit_epoch();
    assert!(store
        .relationships
        .shares_storage_with(&pinned.relationships));
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(4 * 1024 * 1024),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    let admission = governor
        .try_admit_incremental_maintenance(OWNER, OWNER, 1, RuntimeTaskContext::default())
        .unwrap();
    let work =
        crate::background::CheckpointWorkContext::new(admission.task_context().unwrap().clone());
    let operation = WalOp::Batch(vec![WalOp::Batch(vec![operation(id, a, b)])]);
    let mut mutation_started = false;
    let observer = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| {
        store.apply_replayed_checkpoint_wal_transaction_with_boundary(
            &mut catalog,
            operation,
            work,
            &mut mutation_started,
            None,
        )
    });
    let large_allocations = observer.finish();
    assert_eq!(
        large_allocations, 0,
        "controlled WAL replay must admit pinned page copies before large allocation"
    );
    assert!(matches!(
        result,
        Err(crate::background::CheckpointOperationError::Work(
            crate::background::CheckpointWorkError::Memory(_)
        ))
    ));
    assert!(
        !mutation_started,
        "memory denial must precede transaction mutation"
    );
    assert_eq!(store.commit_epoch(), epoch);
    assert!(store
        .relationships
        .shares_storage_with(&pinned.relationships));
    for handle in [&store, &pinned] {
        let node = handle.relationship_owned(id).unwrap().unwrap();
        assert_eq!(node.properties["body"], Value::String(body.clone()));
        assert_eq!(node.properties["rank"], Value::Int(0));
    }
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    drop(work);
    drop(admission);
    drop(store);
    drop(pinned);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn controlled_record_create_node_denies_before_pinned_page_copy() {
    assert_node_record_replay_denies_before_pinned_copy(|_, created| WalOp::CreateNode {
        id: created,
        label: "Memory".into(),
        properties: BTreeMap::new(),
    });
}

#[test]
fn controlled_record_delete_node_denies_before_pinned_page_copy() {
    assert_node_record_replay_denies_before_pinned_copy(|id, _| WalOp::DeleteNode { id });
}

#[test]
fn controlled_record_create_relationship_denies_before_pinned_page_copy() {
    assert_relationship_record_replay_denies_before_pinned_copy(|id, source, target| {
        WalOp::CreateRelationship {
            id: RelId(id.0 + 1),
            source,
            target,
            rel_type: "Memory".into(),
            properties: BTreeMap::new(),
        }
    });
}

#[test]
fn controlled_record_delete_relationship_denies_before_pinned_page_copy() {
    assert_relationship_record_replay_denies_before_pinned_copy(|id, _, _| {
        WalOp::DeleteRelationship { id }
    });
}

#[test]
fn controlled_unshared_node_update_borrows_index_inputs_without_copying_wide_payloads() {
    for scenario in 0..5 {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::default();
        let body = "索引".repeat(64 * 1024);
        let id = store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("body".into(), Value::String(body.clone())),
                    ("category".into(), Value::String("small".into())),
                    ("rank".into(), Value::Int(0)),
                ]),
            )
            .unwrap();
        match scenario {
            1 => {
                store
                    .create_composite_property_index(
                        &mut catalog,
                        "Memory",
                        &["rank".into(), "category".into()],
                    )
                    .unwrap();
            }
            2 => {
                store
                    .create_composite_property_index(
                        &mut catalog,
                        "Memory",
                        &["body".into(), "category".into()],
                    )
                    .unwrap();
            }
            3 => {
                store
                    .create_full_text_property_index(&mut catalog, "Memory", "rank")
                    .unwrap();
            }
            4 => {
                store
                    .create_full_text_property_index(&mut catalog, "Memory", "body")
                    .unwrap();
            }
            _ => {}
        }
        let previous_composite = store.composite_property_index.clone();
        let previous_text = store.full_text_property_index.clone();
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(4 * 1024 * 1024),
                background_task_limit: Some(NonZeroUsize::MIN),
                ..RuntimeGovernorConfig::shared_host()
            },
            RuntimeResourceSnapshot::from_parts(
                RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
                RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
            ),
            IoConcurrencyBudget::new(2, 1),
        );
        let admission = governor
            .try_admit_incremental_maintenance(4096, 4096, 1, RuntimeTaskContext::default())
            .unwrap();
        let work = crate::background::CheckpointWorkContext::new(
            admission.task_context().unwrap().clone(),
        );
        let epoch = store.commit_epoch();
        let operation = WalOp::SetNodeProperty {
            id,
            property: "rank".into(),
            value: Value::Int(1),
        };
        let mut mutation_started = false;
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = work.classify(|work| {
            store.apply_replayed_checkpoint_wal_transaction_with_boundary(
                &mut catalog,
                operation,
                work,
                &mut mutation_started,
                None,
            )
        });
        let large_allocations = observer.finish();
        assert_eq!(
            large_allocations, 0,
            "scenario {scenario}: small property updates must borrow unrelated wide payloads"
        );
        result.unwrap();
        assert!(mutation_started);
        assert_eq!(store.commit_epoch(), epoch + 1);
        let node = store.nodes.get(&id).unwrap();
        assert_eq!(node.properties["body"], Value::String(body));
        assert_eq!(node.properties["rank"], Value::Int(1));
        if scenario == 2 {
            assert_eq!(
                store.composite_property_index.iter().collect::<Vec<_>>(),
                previous_composite.iter().collect::<Vec<_>>()
            );
        }
        if scenario == 4 {
            assert_eq!(
                store.full_text_property_index.iter().collect::<Vec<_>>(),
                previous_text.iter().collect::<Vec<_>>()
            );
        }
        if scenario == 1 {
            assert_eq!(store.composite_property_index.len(), 1);
            let (_, key) = store.composite_property_index.keys().next().unwrap();
            assert_eq!(
                key,
                &vec![
                    ("rank".into(), Value::Int(1)),
                    ("category".into(), Value::String("small".into()))
                ]
            );
        }
        assert_eq!(admission.memory_report().live_accounted_bytes, 0);
        drop(work);
        drop(admission);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}
