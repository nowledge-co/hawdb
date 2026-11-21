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

//! Physical images of an incremental relational checkpoint (#1737). The
//! checkpoint writes only changed row-page and overflow generations, never a
//! full-row `relational.N.hawdb` file, and every image must reopen with every
//! committed row of the recovered epoch, including overflow values.

use super::*;

const ROWS: i64 = 120;

fn large(id: i64, round: i64) -> String {
    let mut state = ((id * 7919 + round) as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..6 * 1024)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + (state % 26) as u8)
        })
        .collect()
}

fn body(id: i64, round: i64) -> String {
    if id % 10 == 0 {
        large(id, round)
    } else {
        format!("body {id} round {round}")
    }
}

fn seed(database: &mut Database) {
    database
        .query_sql(
            "CREATE TABLE docs (id BIGINT PRIMARY KEY, handle TEXT NOT NULL UNIQUE, body TEXT)",
        )
        .unwrap();
    for id in 0..ROWS {
        database
            .query_sql_with_params(
                "INSERT INTO docs (id, handle, body) VALUES ($1, $2, $3)",
                &[
                    Value::Int(id),
                    Value::String(format!("handle-{id}")),
                    Value::String(body(id, 0)),
                ],
            )
            .unwrap();
    }
    database.checkpoint().unwrap();
}

/// One transaction: update a small and a large row, insert two rows (one
/// large), and delete one row.
fn change(database: &mut Database) {
    let mut transaction = database.begin_transaction().unwrap();
    for id in [7, 40] {
        transaction
            .query_sql_with_params(
                "UPDATE docs SET body = $1 WHERE id = $2",
                &[Value::String(body(id, 1)), Value::Int(id)],
            )
            .unwrap();
    }
    for id in [ROWS, ROWS + 10] {
        transaction
            .query_sql_with_params(
                "INSERT INTO docs (id, handle, body) VALUES ($1, $2, $3)",
                &[
                    Value::Int(id),
                    Value::String(format!("handle-{id}")),
                    Value::String(body(id, 1)),
                ],
            )
            .unwrap();
    }
    transaction
        .query_sql("DELETE FROM docs WHERE id = 55")
        .unwrap();
    transaction.commit().unwrap();
}

fn expected(changed: bool) -> BTreeMap<i64, String> {
    let mut rows = (0..ROWS)
        .map(|id| (id, body(id, 0)))
        .collect::<BTreeMap<_, _>>();
    if changed {
        for id in [7, 40, ROWS, ROWS + 10] {
            rows.insert(id, body(id, 1));
        }
        rows.remove(&55);
    }
    rows
}

fn assert_docs(database: &mut Database, changed: bool) {
    let rows = database
        .query_sql("SELECT id, handle, body FROM docs ORDER BY id")
        .unwrap()
        .rows;
    let expected = expected(changed);
    assert_eq!(rows.len(), expected.len());
    for (row, (id, body)) in rows.iter().zip(&expected) {
        assert_eq!(row["id"], Value::Int(*id));
        assert_eq!(row["handle"], Value::String(format!("handle-{id}")));
        assert!(
            row["body"] == Value::String(body.clone()),
            "row {id} differs"
        );
    }
    // The unique handle index is live after recovery.
    assert!(database
        .query_sql("INSERT INTO docs (id, handle, body) VALUES (9999, 'handle-1', 'dup')")
        .is_err());
}

fn assert_no_full_row_file(root: &Path) {
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            let name = path.file_name().unwrap().to_string_lossy();
            assert!(
                !name
                    .strip_prefix("relational.")
                    .and_then(|rest| rest.strip_suffix(".hawdb"))
                    .is_some_and(|generation| generation.parse::<u64>().is_ok()),
                "full-row relational file {}",
                path.display()
            );
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum RelationalCut {
    Head,
    Manifest,
}

#[test]
fn incremental_relational_checkpoint_cuts_recover_every_committed_row() {
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        for cut in [RelationalCut::Head, RelationalCut::Manifest] {
            for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
                let mut fixture = Fixture::new();
                let mut database = open(&fixture.root, durability);
                seed(&mut database);
                let covered_epoch = database.commit_epoch().unwrap();
                change(&mut database);
                let epoch = database.commit_epoch().unwrap();
                let main = database
                    .describe_branch(BranchSelector::Name("main".into()))
                    .unwrap()
                    .id;
                let directory = PathBuf::from("branches").join(main.to_string());
                fixture
                    .model
                    .observe(ObservationPoint {
                        event: IoEvent::Rename,
                        relative_path: match cut {
                            RelationalCut::Head => directory.join("branch.head"),
                            RelationalCut::Manifest => directory.join("runtime/manifest.hawdb"),
                        },
                        boundary,
                        skip_matches: 0,
                        include_descendants: false,
                        keep_last: false,
                    })
                    .unwrap();
                database.checkpoint().unwrap();
                fixture.model.capture().unwrap();
                let snapshot = fixture
                    .model
                    .take_observation()
                    .unwrap()
                    .unwrap_or_else(|| panic!("{cut:?} {boundary:?} was not observed"));
                assert_no_full_row_file(&fixture.root);
                drop(database);
                for plan in publication_fault_plans(&snapshot) {
                    let root = fixture.image(&snapshot, &plan);
                    let mut recovered = open(&root, durability);
                    let recovered_epoch = recovered.commit_epoch().unwrap();
                    if durability == DurabilityPolicy::SyncOnEveryWrite
                        || plan == snapshot.persist_all_plan()
                    {
                        assert_eq!(
                            recovered_epoch, epoch,
                            "{durability:?} {cut:?} {boundary:?} {plan:?}"
                        );
                    } else {
                        assert!(
                            recovered_epoch == covered_epoch || recovered_epoch == epoch,
                            "{durability:?} {cut:?} {boundary:?} {plan:?}"
                        );
                    }
                    let changed = recovered_epoch == epoch;
                    assert_docs(&mut recovered, changed);
                    // The recovered handle publishes its next incremental
                    // generation and reopens to the same rows.
                    recovered.checkpoint().unwrap();
                    assert_no_full_row_file(&root);
                    drop(recovered);
                    let mut reopened = open(&root, durability);
                    assert_docs(&mut reopened, changed);
                }
            }
        }
    }
}
