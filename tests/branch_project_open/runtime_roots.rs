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
use hawdb::{BranchReclamationLimits, DerivedArtifactJobStatus, QueryOutput};
use hawdb_storage::branch_head::read_branch_head;
use hawdb_storage::immutable_object::{ImmutableObjectStore, ObjectKind, ObjectReference};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

fn head_root(project: &std::path::Path, branch: hawdb::Uuid) -> ObjectReference {
    read_branch_head(
        &project
            .join("branches")
            .join(branch.to_string())
            .join("branch.head"),
    )
    .unwrap()
    .sealed_root
}

fn publish_orphan(objects: &mut ImmutableObjectStore) -> ObjectReference {
    let bytes = b"unrelated live-owner GC probe";
    let reference = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, bytes);
    objects.publish(reference, bytes).unwrap();
    reference
}

#[test]
fn historical_reader_roots_survive_new_heads_and_retire_only_after_the_reader() {
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        let project = Project::new();
        let mut database = Database::open_with_durability(&project.0, durability).unwrap();
        database.query("CREATE (:Memory {id: 'before'})").unwrap();
        database.checkpoint().unwrap();
        let branch = main_id(&database);
        let old = head_root(&project.0, branch);
        let mut reader = database.begin_read_transaction().unwrap();
        database.query("CREATE (:Memory {id: 'after'})").unwrap();
        database.checkpoint().unwrap();
        assert_ne!(old, head_root(&project.0, branch));
        let mut objects = ImmutableObjectStore::open(project.0.join("branches/objects")).unwrap();
        let orphan = publish_orphan(&mut objects);
        let report = database
            .reclaim_branch_storage(BranchReclamationLimits::default())
            .unwrap();
        assert!(!report.deferred_for_active_leases);
        assert!(objects.read(orphan).is_err());
        assert!(objects.read(old).is_ok());
        assert_eq!(
            reader
                .query("MATCH (m:Memory) RETURN m.id AS id")
                .unwrap()
                .rows[0]["id"],
            Value::String("before".into())
        );
        drop(reader);
        database
            .reclaim_branch_storage(BranchReclamationLimits::default())
            .unwrap();
        assert!(objects.read(old).is_err());
        drop(database);
        assert_eq!(values(&mut Database::open(&project.0).unwrap()).len(), 2);
    }
}

#[test]
fn pending_and_failed_jobs_pin_their_source_without_blocking_unrelated_reclamation() {
    let project = Project::new();
    let mut database = Database::open(&project.0).unwrap();
    database.query("CREATE (:Memory {id: 'source'})").unwrap();
    database.checkpoint().unwrap();
    let branch = main_id(&database);
    let old = head_root(&project.0, branch);
    let job = database.schedule_external_content_artifact_job("fixture", "parse");
    database.query("CREATE (:Memory {id: 'later'})").unwrap();
    database.checkpoint().unwrap();
    assert_ne!(old, head_root(&project.0, branch));
    let mut objects = ImmutableObjectStore::open(project.0.join("branches/objects")).unwrap();
    for failed in [false, true] {
        if failed {
            let report = database
                .run_next_external_content_artifact_job_with(|_| {
                    Err(HawDBError::Execution("retryable fixture failure".into()))
                })
                .unwrap()
                .unwrap();
            assert_eq!(report.job.status, DerivedArtifactJobStatus::Failed);
        }
        let orphan = publish_orphan(&mut objects);
        let report = database
            .reclaim_branch_storage(BranchReclamationLimits::default())
            .unwrap();
        assert!(!report.deferred_for_active_leases);
        assert!(objects.read(orphan).is_err());
        assert!(objects.read(old).is_ok());
    }
    database
        .retry_failed_external_content_artifact_job(job.id)
        .unwrap();
    let report = database
        .run_next_external_content_artifact_job_with(|_| Ok(QueryOutput::from_rows(Vec::new())))
        .unwrap()
        .unwrap();
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    database
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .unwrap();
    assert!(objects.read(old).is_err());
    assert_eq!(values(&mut database).len(), 2);
}

#[test]
fn corrupt_or_missing_live_root_declarations_prevent_every_unlink() {
    let project = Project::new();
    let mut database = Database::open(&project.0).unwrap();
    database.query("CREATE (:Memory {id: 'retained'})").unwrap();
    database.checkpoint().unwrap();
    let record = project
        .0
        .join("branches")
        .join(main_id(&database).to_string())
        .join("root-pins/roots.hawdb");
    let original = std::fs::read(&record).unwrap();
    let mut objects = ImmutableObjectStore::open(project.0.join("branches/objects")).unwrap();
    let orphan = publish_orphan(&mut objects);
    for missing in [false, true] {
        if missing {
            std::fs::remove_file(&record).unwrap();
        } else {
            std::fs::write(&record, b"damaged root declaration").unwrap();
        }
        assert!(database
            .reclaim_branch_storage(BranchReclamationLimits::default())
            .is_err());
        assert!(objects.read(orphan).is_ok());
        assert_eq!(values(&mut database).len(), 1);
        std::fs::write(&record, &original).unwrap();
    }
    database
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .unwrap();
    assert!(objects.read(orphan).is_err());
}

fn child_signal(message: &str) {
    println!("root-gc-{message}");
    std::io::stdout().flush().unwrap();
}

fn child_command() {
    use std::io::Read;
    let mut command = [0];
    std::io::stdin().read_exact(&mut command).unwrap();
}

fn await_child(output: &mut impl BufRead, expected: &str) {
    loop {
        let mut line = String::new();
        assert!(
            output.read_line(&mut line).unwrap() > 0,
            "child exited before {expected}"
        );
        if line.contains(&format!("root-gc-{expected}")) {
            return;
        }
    }
}

#[test]
fn foreign_writer_and_historical_reader_allow_exact_project_reclamation() {
    const ENV: &str = "HAWDB_ROOT_GC_TEST_PROJECT";
    if let Some(path) = std::env::var_os(ENV) {
        let mut database = Database::open(path).unwrap();
        database.query_sql("USE BRANCH worker").unwrap();
        database.query("CREATE (:Memory {id: 'first'})").unwrap();
        database.checkpoint().unwrap();
        let mut reader = database.begin_read_transaction().unwrap();
        child_signal("first");
        child_command();
        database.query("CREATE (:Memory {id: 'second'})").unwrap();
        database.checkpoint().unwrap();
        child_signal("advanced");
        child_command();
        assert_eq!(
            reader
                .query("MATCH (m:Memory) RETURN m.id")
                .unwrap()
                .rows
                .len(),
            1
        );
        drop(reader);
        child_signal("released");
        child_command();
        assert_eq!(values(&mut database).len(), 2);
        return;
    }
    let project = Project::new();
    let mut database = Database::open(&project.0).unwrap();
    database.checkpoint().unwrap();
    let revision = database.commit_epoch().unwrap();
    database
        .query_sql(&format!(
            "CREATE BRANCH worker FROM main AT REVISION {revision} REQUEST KEY 'foreign-root-gc'"
        ))
        .unwrap();
    let worker = database
        .describe_branch(BranchSelector::Name("worker".into()))
        .unwrap()
        .id;
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "runtime_roots::foreign_writer_and_historical_reader_allow_exact_project_reclamation",
            "--nocapture",
        ])
        .env(ENV, &project.0)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    await_child(&mut output, "first");
    let historical = head_root(&project.0, worker);
    child.stdin.as_mut().unwrap().write_all(b"a").unwrap();
    await_child(&mut output, "advanced");
    assert_ne!(historical, head_root(&project.0, worker));
    let mut objects = ImmutableObjectStore::open(project.0.join("branches/objects")).unwrap();
    let orphan = publish_orphan(&mut objects);
    let report = database
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .unwrap();
    assert!(!report.deferred_for_active_leases);
    assert!(objects.read(orphan).is_err());
    assert!(objects.read(historical).is_ok());
    child.stdin.as_mut().unwrap().write_all(b"r").unwrap();
    await_child(&mut output, "released");
    let report = database
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .unwrap();
    assert!(!report.deferred_for_active_leases);
    assert!(objects.read(historical).is_err());
    child.stdin.as_mut().unwrap().write_all(b"q").unwrap();
    assert!(child.wait().unwrap().success());
    database.query_sql("USE BRANCH worker").unwrap();
    assert_eq!(values(&mut database).len(), 2);
}
