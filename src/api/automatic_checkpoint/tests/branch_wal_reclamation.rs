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

//! Unattended automatic checkpoints reclaim superseded branch-private WAL
//! files through publication and off-gate retirement (#1727).

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

/// `directory -> generations` for every `wal.<generation>.hawdb` in the project.
fn wal_files(root: &Path) -> BTreeMap<PathBuf, Vec<u64>> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = BTreeMap::<PathBuf, Vec<u64>>::new();
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                pending.push(entry.path());
                continue;
            }
            let name = entry.file_name();
            if let Some(generation) = name
                .to_str()
                .and_then(|name| hawdb_storage::artifact_files::parse_generation_file(name, "wal."))
            {
                files
                    .entry(directory.strip_prefix(root).unwrap().to_path_buf())
                    .or_default()
                    .push(generation);
            }
        }
    }
    files
        .values_mut()
        .for_each(|generations| generations.sort_unstable());
    files
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
fn automatic_checkpoints_reclaim_superseded_branch_wals_without_manual_checkpoint() {
    let fixture = Fixture::new();
    let mut db = database(&fixture.0);
    db.commit_epoch().unwrap();
    let control = db.runtime.checkpoint_control_for_test();
    let mut history = Vec::new();
    for generation in 0..6_i64 {
        let before = control.report().unwrap().unwrap().completed_checkpoints;
        let mut transaction = db.begin_transaction().unwrap();
        for id in generation * 8..(generation + 1) * 8 {
            transaction
                .query_with_params(
                    "CREATE (:AutomaticWal {id: $id, body: $body})",
                    &BTreeMap::from([
                        ("id".into(), Value::Int(id)),
                        ("body".into(), Value::String("payload".repeat(256))),
                    ]),
                )
                .unwrap();
        }
        transaction.commit().unwrap();
        // Only the automatic owner publishes, adopts and retires here; no
        // manual checkpoint drives reclamation.
        wait_idle_after(&control, before);
        history.push(wal_files(&fixture.0));
    }
    // Round 0 has no previous data-root generation yet; compare totals with
    // the first steady-state round.
    let steady = history[1].values().map(Vec::len).sum::<usize>();
    for (round, files) in history.iter().enumerate() {
        let branch_wals = files
            .iter()
            .filter(|(directory, _)| directory.components().count() == 2)
            .map(|(_, generations)| generations.len())
            .collect::<Vec<_>>();
        assert!(
            files.values().all(|generations| generations.len() <= 2)
                && (round == 0 || files.values().map(Vec::len).sum::<usize>() <= steady)
                && branch_wals == [1],
            "round {round} retained WAL generations: {history:#?}"
        );
    }
    let pressure = db.storage_pressure_snapshot().unwrap();
    assert!(!pressure.generation_reclamation_retry_required);
    assert_eq!(pressure.generation_reclamation_pending_files, 0);
    assert_eq!(pressure.generation_reclamation_pending_bytes, 0);
    assert!(control.report().unwrap().unwrap().completed_checkpoints >= 6);
    drop(db);

    let mut reopened = database(&fixture.0);
    let rows = reopened
        .query("MATCH (n:AutomaticWal) RETURN n.id AS id ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 48);
    for (id, row) in rows.iter().enumerate() {
        assert_eq!(row["id"], Value::Int(id as i64));
    }
}
