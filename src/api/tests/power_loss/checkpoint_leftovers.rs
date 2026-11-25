// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! #1974, #1979: a process that dies inside a checkpoint leaves exclusively created
//! temporaries of the generation that the next checkpoint reuses. From every
//! file creation and every rename of a checkpoint, the reopened project must
//! keep its committed rows and complete another checkpoint.

use super::*;
use crate::api::automatic_checkpoint::{Control, OwnerProgress};
use crate::embedded_store::EmbeddedStoreHandle;
use crate::{EmbeddedStoreOpenOptions, GraphMode};
use std::time::{Duration, Instant};

// Descriptor refusals are tested elsewhere. Background maintenance may hold
// many handles beside a foreground read, so keep the strict limit out of reach.
const MAX_OPEN_FILES: usize = 256;

// A saturated disk made a whole checkpoint take up to 39 s in #1985, always
// moving between states. No single state may last this long.
const STALL_LIMIT: Duration = Duration::from_secs(60);
// An owner that keeps changing state without ever finishing is a failure too.
const TOTAL_LIMIT: Duration = Duration::from_secs(300);

/// Distinguishes a slow owner from a stalled one by what it does, not by how
/// long the whole checkpoint takes (#1985).
struct StallCheck {
    started: Instant,
    changed: Instant,
    last: Option<OwnerProgress>,
}

impl StallCheck {
    fn new(now: Instant) -> Self {
        Self {
            started: now,
            changed: now,
            last: None,
        }
    }

    /// `current` is `None` while the state lock is held: no change is seen.
    fn observe(
        &mut self,
        now: Instant,
        current: Option<OwnerProgress>,
        stall: Duration,
        total: Duration,
    ) -> Result<(), String> {
        if current.is_some() && current != self.last {
            self.last = current;
            self.changed = now;
        }
        if now - self.changed >= stall {
            return Err(format!(
                "automatic checkpoint stalled: unchanged for {:?} at {:?}",
                now - self.changed,
                self.last
            ));
        }
        if now - self.started >= total {
            return Err(format!(
                "automatic checkpoint unfinished after {:?} at {:?}",
                now - self.started,
                self.last
            ));
        }
        Ok(())
    }
}

/// Waits until the owner has completed more than `completed` checkpoints and
/// is idle, for as long as it makes progress.
fn await_checkpoint(control: &Control, completed: u64, stall: Duration) -> Result<(), String> {
    let mut check = StallCheck::new(Instant::now());
    loop {
        let current = control
            .owner_progress_for_test()
            .map_err(|error| error.to_string())?;
        if current
            .as_ref()
            .is_some_and(|owner| owner.completed_checkpoints() > completed && owner.is_idle())
        {
            return Ok(());
        }
        check.observe(Instant::now(), current, stall, TOTAL_LIMIT)?;
        std::thread::sleep(Duration::from_millis(5));
    }
}

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
            await_checkpoint(&control, 0, STALL_LIMIT)
                .unwrap_or_else(|error| panic!("{context}: {error}"));
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
            await_checkpoint(&control, completed, STALL_LIMIT)
                .unwrap_or_else(|error| panic!("cut {skip} after {completed}: {error}"));
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

/// A fixture whose owner publishes as soon as `suspension` is released.
fn automatic_database(root: &Path) -> Database {
    Database::open_with_durability_and_config(
        root,
        DurabilityPolicy::SyncOnEveryWrite,
        DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(10),
            ..config()
        },
    )
    .unwrap()
}

#[test]
fn a_stalled_automatic_owner_is_reported_without_waiting_for_the_total_limit() {
    let fixture = Fixture::with_max_open_files(MAX_OPEN_FILES);
    let mut database = automatic_database(&fixture.root);
    seed(&mut database);
    let control = database.runtime.checkpoint_control_for_test();
    let suspension = database.runtime.suspend_automatic_checkpoint().unwrap();
    let completed = database
        .automatic_checkpoint_report()
        .unwrap()
        .unwrap()
        .completed_checkpoints;
    let pause = control.pause_idle_handoff_for_test().unwrap();
    change(&mut database, 1);
    drop(suspension);
    pause.wait(STALL_LIMIT).unwrap();
    // The owner is parked with its publication complete but not handed off.
    let error = await_checkpoint(&control, completed, Duration::from_millis(300)).unwrap_err();
    assert!(
        error.contains("stalled: unchanged") && error.contains("phase: Handoff"),
        "{error}"
    );
    // The same wait succeeds once the owner runs again.
    drop(pause);
    await_checkpoint(&control, completed, STALL_LIMIT).unwrap();
    assert_rounds(&mut database, 2);
}

#[test]
fn only_a_changing_owner_state_counts_as_progress() {
    let fixture = Fixture::with_max_open_files(MAX_OPEN_FILES);
    let mut database = automatic_database(&fixture.root);
    let control = database.runtime.checkpoint_control_for_test();
    let suspension = database.runtime.suspend_automatic_checkpoint().unwrap();
    suspension.as_ref().unwrap().wait_idle().unwrap();
    let idle = control.owner_progress_for_test().unwrap().unwrap();
    let owner = |step: u64| {
        let mut owner = idle.clone();
        owner.advance_work_for_test(step);
        Some(owner)
    };
    let (stall, total) = (Duration::from_secs(60), Duration::from_secs(300));
    let start = Instant::now();
    let at = |seconds| start + Duration::from_secs(seconds);

    // Each change renews the stall bound, which total elapsed time does not.
    let mut check = StallCheck::new(start);
    for step in 0..6 {
        check
            .observe(at(step * 50), owner(step), stall, total)
            .unwrap();
    }
    // A held state lock or an equal state renews nothing.
    check.observe(at(260), None, stall, total).unwrap();
    check.observe(at(290), owner(5), stall, total).unwrap();
    let error = check.observe(at(310), None, stall, total).unwrap_err();
    assert!(error.contains("stalled: unchanged for 60s"), "{error}");

    // Changing for ever without finishing is bounded as well.
    let mut check = StallCheck::new(start);
    for step in 0..6 {
        check
            .observe(at(step * 50), owner(step), stall, total)
            .unwrap();
    }
    let error = check.observe(at(300), owner(6), stall, total).unwrap_err();
    assert!(error.contains("unfinished after 300s"), "{error}");
}
