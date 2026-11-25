// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! #1974, #1979: a process that dies inside a checkpoint leaves exclusively created
//! temporaries of the generation that the next checkpoint reuses. From every
//! file creation and every rename of a checkpoint, the reopened project must
//! keep its committed rows and complete another checkpoint.

use super::*;
use crate::embedded_store::EmbeddedStoreHandle;
use crate::{EmbeddedStoreOpenOptions, GraphMode};
use std::time::{Duration, Instant};

// Descriptor refusals are tested elsewhere. Background maintenance may hold
// many handles beside a foreground read, so keep the strict limit out of reach.
const MAX_OPEN_FILES: usize = 256;

// Each wait is normally milliseconds; the bound only has to outlast a
// saturated disk, not measure the owner.
const PUBLICATION_DEADLINE: Duration = Duration::from_secs(180);

#[derive(Clone, Copy, Debug)]
enum Recovery {
    Database,
    Handle,
    Automatic,
}

fn seed(database: &mut Database) {
    for statement in [
        "CREATE TABLE docs (id BIGINT PRIMARY KEY, handle TEXT NOT NULL UNIQUE, body TEXT)",
        "CREATE TABLE events (stream TEXT NOT NULL, sequence BIGINT NOT NULL, payload TEXT NOT NULL) WITH (storage_mode = 'strict_append', partition_key = 'stream', order_key = 'sequence')",
    ] {
        database.query_sql(statement).unwrap();
    }
    change(database, 0);
    database.checkpoint().unwrap();
}

fn body(round: usize) -> String {
    format!("{round} body ").repeat(4096)
}

/// One transaction over graph records, a spilled property, a relationship,
/// a row with an overflow value and an append row.
fn change(database: &mut Database, round: usize) {
    let parameters = BTreeMap::from([
        ("id".to_string(), Value::Int(round as i64)),
        ("body".to_string(), Value::String(body(round))),
    ]);
    let mut transaction = database.begin_transaction().unwrap();
    transaction
        .query_with_params(
            "CREATE (:Doc {id: $id, body: $body})-[:NEXT]->(:Tail {id: $id})",
            &parameters,
        )
        .unwrap();
    transaction
        .query_sql_with_params(
            "INSERT INTO docs (id, handle, body) VALUES ($1, $2, $3)",
            &[
                Value::Int(round as i64),
                Value::String(format!("handle-{round}")),
                Value::String(body(round)),
            ],
        )
        .unwrap();
    transaction
        .query_sql_with_params(
            "INSERT INTO events (stream, sequence, payload) VALUES ('s', $1, 'event')",
            &[Value::Int(round as i64)],
        )
        .unwrap();
    transaction.commit().unwrap();
}

fn assert_rounds(database: &mut Database, rounds: usize) {
    let documents = database
        .query("MATCH (n:Doc)-[:NEXT]->(t:Tail) RETURN n.body AS body, t.id AS id ORDER BY id")
        .unwrap()
        .rows;
    let rows = database
        .query_sql("SELECT id, body FROM docs ORDER BY id")
        .unwrap()
        .rows;
    let events = database
        .query_sql("SELECT sequence FROM events WHERE stream = 's' ORDER BY sequence LIMIT 1000")
        .unwrap()
        .rows;
    assert_eq!(
        (documents.len(), rows.len(), events.len()),
        (rounds, rounds, rounds)
    );
    for round in 0..rounds {
        let body = Value::String(body(round));
        assert_eq!(documents[round]["id"], Value::Int(round as i64));
        assert!(documents[round]["body"] == body);
        assert_eq!(rows[round]["id"], Value::Int(round as i64));
        assert!(rows[round]["body"] == body);
        assert_eq!(events[round]["sequence"], Value::Int(round as i64));
    }
}

fn config() -> DatabaseConfig {
    DatabaseConfig {
        max_open_files: MAX_OPEN_FILES,
        strict_capacity_limit: true,
        ..Default::default()
    }
}

fn open_on(root: &Path, branch: Uuid, config: DatabaseConfig) -> Database {
    let mut database =
        Database::open_with_durability_and_config(root, DurabilityPolicy::SyncOnEveryWrite, config)
            .unwrap();
    database
        .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(branch)])
        .unwrap();
    database
}

/// The first checkpoint after the crash, through one of the three owners.
fn recover(root: &Path, branch: Uuid, recovery: Recovery, rounds: usize, context: &str) {
    match recovery {
        Recovery::Database => {
            let mut database = open_on(root, branch, config());
            assert_rounds(&mut database, rounds);
            database
                .checkpoint()
                .unwrap_or_else(|error| panic!("{context}: {error}"));
        }
        Recovery::Handle => {
            let mut options = EmbeddedStoreOpenOptions::with_search_projection(
                root,
                root.join("search"),
                GraphMode::WritableCutover,
            );
            options.database_config = Some(config());
            let (handle, _) = EmbeddedStoreHandle::open_with_options(options).unwrap();
            handle
                .checkpoint()
                .unwrap_or_else(|error| panic!("{context}: {error}"));
        }
        Recovery::Automatic => {
            let mut database = open_on(
                root,
                branch,
                DatabaseConfig {
                    automatic_checkpoint_max_age: Duration::from_millis(10),
                    ..config()
                },
            );
            assert_rounds(&mut database, rounds);
            database
                .query("CREATE (:Trigger {id: 1})")
                .unwrap_or_else(|error| panic!("{context}: {error}"));
            let control = database.runtime.checkpoint_control_for_test();
            let deadline = Instant::now() + PUBLICATION_DEADLINE;
            loop {
                let report = database.automatic_checkpoint_report().unwrap().unwrap();
                if report.completed_checkpoints > 0 && control.is_idle_for_test().unwrap() {
                    break;
                }
                assert!(Instant::now() < deadline, "{context}: {report:?}");
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
    let mut database = open_on(root, branch, config());
    assert_rounds(&mut database, rounds);
    let data = root.join(format!("branches/{branch}/data"));
    for entry in std::fs::read_dir(&data).unwrap() {
        let name = entry.unwrap().file_name().into_string().unwrap();
        assert!(!name.ends_with(".tmp"), "{context}: leftover {name}");
    }
}

/// Cuts one checkpoint per round at the next unseen event until a checkpoint
/// has no further event of this kind.
fn cut_every(event: IoEvent, boundary: ObservationBoundary, child: bool, automatic: bool) {
    let mut fixture = Fixture::with_max_open_files(MAX_OPEN_FILES);
    let mut database = Database::open_with_durability_and_config(
        &fixture.root,
        DurabilityPolicy::SyncOnEveryWrite,
        DatabaseConfig {
            automatic_checkpoint_max_age: if automatic {
                Duration::from_millis(10)
            } else {
                config().automatic_checkpoint_max_age
            },
            ..config()
        },
    )
    .unwrap();
    seed(&mut database);
    let mut branch = database
        .describe_branch(BranchSelector::Name("main".into()))
        .unwrap()
        .id;
    if child {
        branch = fork(&mut database, branch, "child");
        database
            .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(branch)])
            .unwrap();
    }
    let mut seen = BTreeMap::<String, usize>::new();
    let mut images = 0;
    for skip in 0.. {
        let rounds = skip + 2;
        let suspension =
            automatic.then(|| database.runtime.suspend_automatic_checkpoint().unwrap());
        let completed = database
            .automatic_checkpoint_report()
            .unwrap()
            .map_or(0, |report| report.completed_checkpoints);
        change(&mut database, skip + 1);
        fixture
            .model
            .observe(ObservationPoint {
                event,
                relative_path: PathBuf::from("branches").join(branch.to_string()),
                boundary,
                skip_matches: skip,
                include_descendants: true,
                keep_last: false,
            })
            .unwrap();
        if let Some(suspension) = suspension {
            drop(suspension);
            let control = database.runtime.checkpoint_control_for_test();
            let deadline = Instant::now() + PUBLICATION_DEADLINE;
            loop {
                let report = database.automatic_checkpoint_report().unwrap().unwrap();
                if report.completed_checkpoints > completed && control.is_idle_for_test().unwrap() {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "cut {skip} after {completed} idle={}: {report:?}",
                    control.is_idle_for_test().unwrap()
                );
                std::thread::sleep(Duration::from_millis(5));
            }
        } else {
            database.checkpoint().unwrap();
        }
        let Some(snapshot) = fixture.model.take_observation().unwrap() else {
            break;
        };
        let name = snapshot
            .observed_path()
            .and_then(|path| path.file_name())
            .unwrap()
            .to_string_lossy()
            .chars()
            .filter(|character| !character.is_ascii_digit())
            .collect::<String>();
        *seen.entry(name.clone()).or_default() += 1;
        // A killed process keeps every write; a power loss may keep none of
        // the uncovered ones.
        let recoveries: &[Recovery] = if child {
            &[Recovery::Database, Recovery::Automatic]
        } else {
            &[Recovery::Database, Recovery::Handle, Recovery::Automatic]
        };
        // The manual creation cuts pair every kind with every owner; the other
        // schedules rotate the owners to bound the number of images.
        let every_owner = event == IoEvent::CreateFile && !automatic;
        for (index, &recovery) in recoveries.iter().enumerate() {
            if !every_owner && index != skip % recoveries.len() {
                continue;
            }
            let root = fixture.image(&snapshot, &snapshot.persist_all_plan());
            recover(
                &root,
                branch,
                recovery,
                rounds,
                &format!("{event:?} {boundary:?} {name} {recovery:?}"),
            );
            images += 1;
        }
        let root = fixture.image(&snapshot, &CrashPlan::default());
        recover(
            &root,
            branch,
            Recovery::Database,
            rounds,
            &format!("{event:?} {boundary:?} {name} power loss"),
        );
        images += 1;
    }
    println!(
        "{event:?} {boundary:?} child={child} automatic={automatic}: {images} images {seen:?}"
    );
    // Every exclusively created kind must have been cut, not only present.
    let suffix = if event == IoEvent::CreateFile {
        ".tmp"
    } else {
        ""
    };
    for family in [
        "checkpoint..hawdb",
        "canonical..manifest.hawdb",
        "properties..manifest.hawdb",
        "property-index..manifest.hawdb",
        "append-.segment.hawdb",
        "append-.manifest.hawdb",
        "relational-row-pages-.pages.hawdb",
        "relational-row-pages-.manifest.hawdb",
        "relational-row-root-.descriptors.hawdb",
        "relational-row-root-.keys.hawdb",
        "relational-overflow-.extents.hawdb",
        "relational-overflow-.manifest.hawdb",
        "relational-overflow-root-.descriptors.hawdb",
    ] {
        let family = format!("{family}{suffix}");
        assert!(seen.contains_key(&family), "no {family} cut in {seen:?}");
    }
}

#[test]
fn manual_checkpoint_created_files_do_not_block_the_next_checkpoint() {
    cut_every(
        IoEvent::CreateFile,
        ObservationBoundary::After,
        false,
        false,
    );
}

#[test]
fn manual_checkpoint_unrenamed_files_do_not_block_the_next_checkpoint() {
    cut_every(IoEvent::Rename, ObservationBoundary::Before, false, false);
}

#[test]
fn child_branch_checkpoint_created_files_do_not_block_the_next_checkpoint() {
    cut_every(IoEvent::CreateFile, ObservationBoundary::After, true, false);
}

#[test]
fn automatic_checkpoint_unrenamed_files_do_not_block_the_next_checkpoint() {
    cut_every(IoEvent::Rename, ObservationBoundary::Before, false, true);
}

#[test]
fn automatic_checkpoint_created_files_do_not_block_the_next_checkpoint() {
    cut_every(IoEvent::CreateFile, ObservationBoundary::After, false, true);
}
