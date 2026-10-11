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

#[test]
fn fixed_memory_budget_advances_two_generations_without_accumulating_unused_reservation() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    let body = "m".repeat(512);
    let add = |store: &mut GraphStore, catalog: &mut Catalog, id| {
        store
            .create_node(
                catalog,
                "Memory",
                BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("body".into(), Value::String(body.clone())),
                ]),
            )
            .unwrap();
    };
    for id in 0..32 {
        add(&mut store, &mut catalog, id);
    }
    let control = Arc::new(Control::default());
    let resources = governor(200 * 1024 * 1024);
    assert!(
        store.checkpoint_candidate_admission_bytes().unwrap()
            < resources.snapshot().limits.memory_budget_bytes
    );
    let mut owner = Owner::start(
        control.clone(),
        &store,
        &catalog,
        Arc::new(Mutex::new(ReaderPins::default())),
        &DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        },
        LocalQosScheduler::new(hawdb_qos::LocalQosPolicy::default()),
        Some(resources.clone()),
    )
    .unwrap()
    .unwrap();
    for generation in 1..=2 {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let mut state = control.lock_frontend().unwrap();
            if state.selected.is_some() {
                control.adopt(&mut state, &mut store).unwrap();
            }
            if state.report.completed_checkpoints >= generation
                && state.phase == Phase::Idle
                && state.retired.is_none()
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "fixed-memory owner stalled at generation {generation}: {:?}; resources: {:?}",
                state.report,
                resources.snapshot()
            );
            drop(state);
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(resources.snapshot().active_background_tasks, 0);
        assert_eq!(resources.snapshot().active_background_io_slots, 0);
        if generation == 1 {
            eprintln!(
                "retained memory after first generation: {}",
                resources.snapshot().admitted_memory_bytes
            );
            let mut state = control.lock_frontend().unwrap();
            add(&mut store, &mut catalog, 32);
            let retired = control.submit(&mut state, &store, &catalog);
            drop(state);
            drop(retired);
        }
    }
    owner.stop();
    for id in 0..33 {
        let node = store
            .node_owned(hawdb_storage::NodeId(id))
            .unwrap()
            .unwrap();
        assert_eq!(node.properties["id"], Value::Int(id as i64));
        assert_eq!(node.properties["body"], Value::String(body.clone()));
    }
    drop(owner);
    drop(store);
    assert_eq!(resources.snapshot().admitted_memory_bytes, 0);
    let recovered = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    assert_eq!(recovered.node_count_for_label(None), 33);
    for id in 0..33 {
        let node = recovered
            .node_owned(hawdb_storage::NodeId(id))
            .unwrap()
            .unwrap();
        assert_eq!(node.properties["id"], Value::Int(id as i64));
        assert_eq!(node.properties["body"], Value::String(body.clone()));
    }
}
