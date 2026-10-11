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
use std::collections::BTreeMap;
use std::sync::mpsc;

fn assert_node(store: &GraphStore) {
    assert_eq!(store.node_count_for_label(None), 1);
    assert_eq!(
        store
            .node_owned(hawdb_storage::NodeId(0))
            .unwrap()
            .unwrap()
            .properties["id"],
        Value::Int(1)
    );
}

#[test]
fn projection_procedure_waits_for_publication_and_appends_to_the_adopted_wal() {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(
        &fixture.0,
        DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let suspension = db.runtime.suspend_automatic_checkpoint().unwrap().unwrap();
    db.query("CREATE (:Memory {id: 1})").unwrap();
    let parameters = BTreeMap::new();
    let prepared = db
        .prepare_runtime_query(
            "CALL project_graph('g', ['Memory'], [])".into(),
            &parameters,
        )
        .unwrap();
    assert!(
        !prepared.uses_read_snapshot(),
        "a prepared projection must use the writable runtime"
    );
    let old = db.runtime.get().unwrap().store.snapshot_for_read();
    let control = db.runtime.checkpoint_control_for_test();
    let (paused, observed) = mpsc::channel();
    let (resume, continuation) = mpsc::channel();
    let (waiting, wait_observed) = mpsc::channel();
    {
        let mut state = control.lock().unwrap();
        state.publication_probe = Some(Arc::new(OwnerPauseProbe {
            paused,
            resume: Mutex::new(continuation),
        }));
        state.frontend_wait_probe = Some(waiting);
    }
    drop(suspension);
    observed.recv_timeout(Duration::from_secs(15)).unwrap();
    assert_eq!(control.lock().unwrap().phase, Phase::Draining);
    let mut db = std::thread::scope(|scope| {
        let projection = scope.spawn(move || {
            let output = db
                .query_prepared_with_params(prepared, &parameters)
                .unwrap();
            assert_eq!(output.rows.len(), 1);
            db
        });
        assert_eq!(
            wait_observed.recv_timeout(Duration::from_secs(15)).unwrap(),
            Phase::Draining
        );
        assert_values_before_publication(&old);
        resume.send(()).unwrap();
        projection.join().unwrap()
    });
    assert!(
        db.automatic_checkpoint_report()
            .unwrap()
            .unwrap()
            .completed_checkpoints
            >= 1
    );
    assert_node(&db.runtime.get().unwrap().store);
    assert!(db
        .runtime
        .get()
        .unwrap()
        .store
        .projected_graph_definition("g")
        .is_some());
    assert_values_before_publication(&old);
    db.checkpoint().unwrap();
    assert_eq!(
        db.automatic_checkpoint_report()
            .unwrap()
            .unwrap()
            .operation_failures,
        0
    );
    drop(db);
    drop(control);
    assert_values_before_publication(&old);
    drop(old);
    let reopened = Database::open(&fixture.0).unwrap();
    assert_node(&reopened.runtime.get().unwrap().store);
    assert!(reopened
        .runtime
        .get()
        .unwrap()
        .store
        .projected_graph_definition("g")
        .is_some());
}

fn assert_values_before_publication(store: &GraphStore) {
    assert_node(store);
    assert!(store.projected_graph_definition("g").is_none());
}

#[test]
fn unfenced_read_execution_and_explain_analyze_reject_projection_wal_effects() {
    let fixture = Fixture::new();
    let mut config = DatabaseConfig::default();
    config.runtime_capabilities.background_maintenance = false;
    let mut db = Database::open_with_config(&fixture.0, config).unwrap();
    db.query("CREATE (:Memory {id: 1})").unwrap();
    let before = db.runtime.get().unwrap().store.checkpoint_source_identity();
    let parameters = BTreeMap::new();
    let memory = db.config.execution_memory.clone();
    let projection = crate::planner::PhysicalPlan::ProjectGraph {
        name: "blocked".into(),
        node_labels: vec!["Memory".into()],
        rel_types: vec![],
        relationship_predicates: BTreeMap::new(),
    };
    let nested = crate::planner::PhysicalPlan::LimitExec {
        offset: 0,
        limit: Some(1),
        input: Box::new(projection.clone()),
    };
    for plan in [&projection, &nested] {
        let error = db
            .runtime
            .get_read()
            .unwrap()
            .execute(
                crate::executor::ExecutionRequest::new(plan, &parameters, &memory),
                &mut crate::executor::NoExternalReadOperator,
            )
            .unwrap_err();
        assert!(matches!(error, HawDBError::Execution(_)));
        assert_eq!(
            db.runtime.get().unwrap().store.checkpoint_source_identity(),
            before
        );
        assert!(db
            .runtime
            .get()
            .unwrap()
            .store
            .projected_graph_definition("blocked")
            .is_none());
    }
    assert!(db
        .query("EXPLAIN ANALYZE CALL project_graph('blocked', ['Memory'], [])")
        .is_err());
    assert!(db
        .explain_analyze_query("CALL project_graph('blocked', ['Memory'], [])")
        .is_err());
    assert_eq!(
        db.runtime.get().unwrap().store.checkpoint_source_identity(),
        before
    );
    assert!(db
        .runtime
        .get()
        .unwrap()
        .store
        .projected_graph_definition("blocked")
        .is_none());
    assert_node(&db.runtime.get().unwrap().store);
    // The ordinary procedure still works and retains its public result shape.
    assert_eq!(
        db.query("CALL project_graph('allowed', ['Memory'], [])")
            .unwrap()
            .rows
            .len(),
        1
    );
    db.checkpoint().unwrap();
    drop(db);
    let reopened = Database::open(&fixture.0).unwrap();
    assert_node(&reopened.runtime.get().unwrap().store);
    assert!(reopened
        .runtime
        .get()
        .unwrap()
        .store
        .projected_graph_definition("allowed")
        .is_some());
    assert!(reopened
        .runtime
        .get()
        .unwrap()
        .store
        .projected_graph_definition("blocked")
        .is_none());
}
