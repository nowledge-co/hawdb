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

fn add(store: &mut GraphStore, catalog: &mut Catalog, id: i64) {
    store
        .create_node(
            catalog,
            "Memory",
            BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("body".into(), Value::String("d".repeat(512))),
            ]),
        )
        .unwrap();
}

fn assert_values(store: &GraphStore, count: u64) {
    assert_eq!(store.node_count_for_label(None), count as usize);
    for id in 0..count {
        let node = store
            .node_owned(hawdb_storage::NodeId(id))
            .unwrap()
            .unwrap();
        assert_eq!(node.properties["id"], Value::Int(id as i64 + 1));
        assert_eq!(node.properties["body"], Value::String("d".repeat(512)));
    }
}

#[test]
fn cancelled_publication_cleanup_admits_writes_but_fences_manual_generation_reuse() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    add(&mut store, &mut catalog, 1);
    let old = store.snapshot_for_read();
    let governor = retirement_governor();
    let control = Arc::new(Control::default());
    let (publication_paused, publication_observed) = std::sync::mpsc::channel();
    let (publication_resume, publication_continuation) = std::sync::mpsc::channel();
    let (discard_paused, discard_observed) = std::sync::mpsc::channel();
    let (discard_resume, discard_continuation) = std::sync::mpsc::channel();
    let (manual_wait, manual_waiting) = std::sync::mpsc::channel();
    {
        let mut state = control.lock().unwrap();
        state.publication_probe = Some(Arc::new(OwnerPauseProbe {
            paused: publication_paused,
            resume: Mutex::new(publication_continuation),
        }));
        state.discard_probe = Some(Arc::new(OwnerPauseProbe {
            paused: discard_paused,
            resume: Mutex::new(discard_continuation),
        }));
        state.manual_wait_probe = Some(manual_wait);
    }
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
        Some(governor.clone()),
    )
    .unwrap()
    .unwrap();
    publication_observed
        .recv_timeout(Duration::from_secs(15))
        .unwrap();
    assert_eq!(control.lock().unwrap().phase, Phase::Draining);
    let suspension = control.suspend().unwrap();
    let (manual_ready, manual_readiness) = std::sync::mpsc::channel();
    let (manual_release, manual_continuation) = std::sync::mpsc::channel();
    let (mut store, mut catalog) = std::thread::scope(|scope| {
        let manual = scope.spawn(move || {
            suspension.wait_idle().unwrap();
            manual_ready.send(()).unwrap();
            manual_continuation
                .recv_timeout(Duration::from_secs(15))
                .unwrap();
            drop(suspension);
        });
        assert_eq!(
            manual_waiting
                .recv_timeout(Duration::from_secs(15))
                .unwrap(),
            Phase::Draining
        );
        publication_resume.send(()).unwrap();
        discard_observed
            .recv_timeout(Duration::from_secs(15))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let phase = manual_waiting
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap();
            if phase == Phase::Discarding {
                break;
            }
            assert_eq!(phase, Phase::Draining);
        }
        assert!(matches!(
            manual_readiness.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        let mut store = store;
        let mut catalog = catalog;
        {
            let mut state = control.lock_frontend().unwrap();
            assert_eq!(state.phase, Phase::Discarding);
            assert!(governor.snapshot().admitted_memory_bytes > 0);
            add(&mut store, &mut catalog, 2);
            let retired = control.submit(&mut state, &store, &catalog);
            drop(state);
            drop(retired);
        }
        assert_values(&store, 2);
        assert_values(&old, 1);
        assert!(matches!(
            manual_readiness.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        discard_resume.send(()).unwrap();
        manual_readiness
            .recv_timeout(Duration::from_secs(15))
            .unwrap();
        let resources = governor.snapshot();
        assert_eq!(resources.admitted_memory_bytes, 0);
        assert_eq!(resources.active_cpu_slots, 0);
        assert_eq!(resources.active_background_tasks, 0);
        assert_eq!(resources.active_background_io_slots, 0);
        // The manual boundary may now safely reuse the abandoned namespace.
        store.checkpoint(&catalog).unwrap();
        {
            let mut state = control.lock_frontend().unwrap();
            let retired = control.submit(&mut state, &store, &catalog);
            drop(state);
            drop(retired);
        }
        manual_release.send(()).unwrap();
        manual.join().unwrap();
        (store, catalog)
    });
    {
        let mut state = control.lock_frontend().unwrap();
        add(&mut store, &mut catalog, 3);
        let retired = control.submit(&mut state, &store, &catalog);
        drop(state);
        drop(retired);
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let state = control.lock().unwrap();
        if state.selected.is_some() {
            assert_eq!(state.report.completed_checkpoints, 1);
            assert_eq!(state.report.operation_failures, 0);
            break;
        }
        drop(state);
        assert!(
            Instant::now() < deadline,
            "automatic work must resume after manual publication"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    {
        let mut state = control.lock_frontend().unwrap();
        control.adopt(&mut state, &mut store).unwrap();
    }
    owner.stop();
    assert_values(&store, 3);
    assert_values(&old, 1);
    drop(old);
    drop(store);
    drop(owner);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    let mut catalog = Catalog::default();
    let reopened = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    assert_values(&reopened, 3);
}
