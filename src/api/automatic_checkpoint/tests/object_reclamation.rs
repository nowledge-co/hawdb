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

//! Unattended automatic checkpoints reclaim superseded immutable objects
//! through candidate publication and off-gate retirement (#1736).

use super::*;
use crate::Value;
use std::collections::BTreeMap;
use std::path::Path;

fn wait_idle_after(control: &Control, before: u64) {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(30);
    let mut state = control.lock().unwrap();
    while !(state.phase == Phase::Idle
        && state.report.completed_checkpoints > before
        && !state.report.waiting_for_handoff)
    {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "automatic checkpoint stalled: {:?}, {:?}",
            state.phase,
            state.report
        );
        state = control.changed.wait_timeout(state, remaining).unwrap().0;
    }
}

/// `(sealed roots, checkpoint artifacts)` in the project object store.
fn object_counts(root: &Path) -> (usize, usize) {
    let count = |kind: &str| {
        std::fs::read_dir(root.join("branches/objects/objects").join(kind))
            .map_or(0, |entries| entries.count())
    };
    (count("sealed-root"), count("checkpoint-artifact"))
}

fn database(root: &Path) -> Database {
    Database::open_with_config(
        root,
        DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        },
    )
    .unwrap()
}

#[test]
fn automatic_checkpoints_reclaim_superseded_objects_without_manual_checkpoint() {
    let fixture = Fixture::new();
    let mut db = database(&fixture.0);
    db.commit_epoch().unwrap();
    let control = db.runtime.checkpoint_control_for_test();
    let mut history = Vec::new();
    for generation in 0..8_i64 {
        let before = control.report().unwrap().unwrap().completed_checkpoints;
        let mut transaction = db.begin_transaction().unwrap();
        for id in generation * 8..(generation + 1) * 8 {
            transaction
                .query_with_params(
                    "CREATE (:AutomaticObject {id: $id, body: $body})",
                    &BTreeMap::from([
                        ("id".into(), Value::Int(id)),
                        ("body".into(), Value::String(format!("{id}").repeat(512))),
                    ]),
                )
                .unwrap();
        }
        transaction.commit().unwrap();
        // Only the automatic owner publishes, adopts and retires here.
        wait_idle_after(&control, before);
        history.push(object_counts(&fixture.0));
    }
    println!("automatic object reclamation history (roots, artifacts): {history:?}");
    // Retained: the selected head, the head of the frontend it retired
    // (released only after its sweep) and the catalog creation baseline.
    // Every older closure is reclaimed.
    for (round, (roots, artifacts)) in history.iter().enumerate() {
        assert!(
            *roots <= 3 && (round == 0 || *artifacts <= history[1].1 + round),
            "round {round} retained objects: {history:?}"
        );
    }
    let pressure = db.storage_pressure_snapshot().unwrap();
    assert!(!pressure.object_reclamation_retry_required, "{pressure:?}");
    assert_eq!(pressure.object_reclamation_pending_files, 0);
    assert!(pressure.object_store_files > 0);
    assert!(control.report().unwrap().unwrap().completed_checkpoints >= 8);
    drop(db);

    let mut reopened = database(&fixture.0);
    let rows = reopened
        .query("MATCH (n:AutomaticObject) RETURN n.id AS id ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 64);
    for (id, row) in rows.iter().enumerate() {
        assert_eq!(row["id"], Value::Int(id as i64));
    }
}
