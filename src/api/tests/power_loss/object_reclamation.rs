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

//! Physical images around the automatic removal of superseded immutable
//! objects after checkpoint publication (#1736). Removal follows the head
//! that supersedes them and a barrier on every head it read; no image may
//! lose a checkpointed transaction or a child's creation base, and a later
//! checkpoint reclaims anything a lost removal left behind.

use super::*;

/// Superseded sealed roots are removed only by the sweep; publication's own
/// staging cleanup never touches this directory.
fn sealed_root_directory() -> PathBuf {
    PathBuf::from("branches")
        .join("objects")
        .join("objects")
        .join("sealed-root")
}

fn head_root(root: &Path, branch: Uuid) -> hawdb_storage::immutable_object::ObjectReference {
    hawdb_storage::branch_head::read_branch_head(
        &root
            .join("branches")
            .join(branch.to_string())
            .join("branch.head"),
    )
    .unwrap()
    .sealed_root
}

fn assert_no_object_debt(database: &Database, context: &str) {
    let pressure = database.storage_pressure_snapshot().unwrap();
    assert!(
        !pressure.object_reclamation_retry_required
            && pressure.object_reclamation_pending_files == 0,
        "{context}: {pressure:?}"
    );
}

fn assert_ids(database: &mut Database, ids: &[&str], context: &str) {
    for id in ids {
        assert_eq!(
            database
                .query(&format!(
                    "MATCH (m:Memory {{id: '{id}'}}) RETURN m.id AS id"
                ))
                .unwrap()
                .rows
                .len(),
            1,
            "{context}: {id}"
        );
    }
}

/// Cut immediately before and after the first object unlink of a checkpoint
/// sweep, with a child created from an older parent root.
#[test]
fn superseded_object_removal_cuts_recover_and_reclaim_leftovers() {
    let mut images = 0;
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
            let mut fixture = Fixture::new();
            let mut database = open(&fixture.root, durability);
            let main = database
                .describe_branch(BranchSelector::Name("main".into()))
                .unwrap()
                .id;
            commit_mixed(&mut database);
            database.checkpoint().unwrap();
            let child = fork(&mut database, main, "child");
            let child_base = head_root(&fixture.root, main);
            for round in 0..3 {
                database
                    .query(&format!("CREATE (:Memory {{id: 'main-{round}'}})"))
                    .unwrap();
                database.checkpoint().unwrap();
            }
            database
                .query("CREATE (:Memory {id: 'swept-tail'})")
                .unwrap();
            let epoch = database.commit_epoch().unwrap();
            fixture
                .model
                .observe(ObservationPoint {
                    event: IoEvent::Remove,
                    relative_path: sealed_root_directory(),
                    boundary,
                    skip_matches: 0,
                    include_descendants: true,
                    keep_last: false,
                })
                .unwrap();
            database.checkpoint().unwrap();
            fixture.model.capture().unwrap();
            let snapshot = fixture
                .model
                .take_observation()
                .unwrap()
                .expect("the post-publication sweep must remove a superseded object");
            assert_no_object_debt(&database, "observed sweep");
            drop(database);
            for plan in publication_fault_plans(&snapshot) {
                let root = fixture.image(&snapshot, &plan);
                let context = format!("{durability:?} {boundary:?} {plan:?}");
                let mut recovered = open(&root, durability);
                assert_eq!(recovered.commit_epoch().unwrap(), epoch, "{context}");
                assert!(assert_atomic(&mut recovered, main), "{context}");
                assert_ids(
                    &mut recovered,
                    &["main-0", "main-1", "main-2", "swept-tail"],
                    &context,
                );
                // A lost unlink leaves only unreachable bytes; the next
                // checkpoint reclaims them without debt.
                recovered
                    .query("CREATE (:Memory {id: 'after-recovery'})")
                    .unwrap();
                recovered.checkpoint().unwrap();
                assert_no_object_debt(&recovered, &context);
                // The child admits from the parent root it was created from.
                assert_eq!(head_root(&root, child), child_base, "{context}");
                recovered
                    .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(child)])
                    .unwrap();
                assert_ids(&mut recovered, &["atomic-tail"], &context);
                assert!(recovered
                    .query("MATCH (m:Memory {id: 'swept-tail'}) RETURN m.id")
                    .unwrap()
                    .rows
                    .is_empty());
                recovered
                    .query("CREATE (:Memory {id: 'child-after-recovery'})")
                    .unwrap();
                recovered.checkpoint().unwrap();
                assert_no_object_debt(&recovered, &context);
                drop(recovered);
                let mut reopened = open(&root, durability);
                assert_eq!(reopened.commit_epoch().unwrap(), epoch + 1, "{context}");
                assert!(assert_atomic(&mut reopened, main), "{context}");
                assert_ids(&mut reopened, &["swept-tail", "after-recovery"], &context);
                images += 1;
            }
        }
    }
    println!("superseded object removal qualification: {images} physical crash images");
}

/// A process crash after the head rename but before its directory barrier
/// leaves a visible, non-durable head. The restarted process adopts it, and
/// its first checkpoint sweeps objects that only the older durable head
/// referenced. The pre-removal barrier must make the adopted head durable
/// first, so no later power loss returns to a head whose objects are gone.
#[test]
fn unsynchronized_head_rename_survives_restart_object_sweep_and_power_loss() {
    let mut images = 0;
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        let mut fixture = Fixture::new();
        let mut database = open(&fixture.root, durability);
        let main = database
            .describe_branch(BranchSelector::Name("main".into()))
            .unwrap()
            .id;
        commit_mixed(&mut database);
        database.checkpoint().unwrap();
        database
            .query("CREATE (:Memory {id: 'uncertain-head'})")
            .unwrap();
        let durable_root = head_root(&fixture.root, main);
        fixture
            .model
            .observe(ObservationPoint {
                event: IoEvent::Rename,
                relative_path: PathBuf::from("branches")
                    .join(main.to_string())
                    .join("branch.head"),
                boundary: ObservationBoundary::After,
                skip_matches: 0,
                include_descendants: false,
                keep_last: false,
            })
            .unwrap();
        database.checkpoint().unwrap();
        fixture.model.capture().unwrap();
        let renamed = fixture
            .model
            .take_observation()
            .unwrap()
            .expect("checkpoint head rename must be observed");
        drop(database);

        let restarted = fixture.image(&renamed, &renamed.persist_all_plan());
        let project =
            ProjectFileDescriptors::acquire_with_capacity_limit(&restarted, 32, true).unwrap();
        let model = PowerLossModel::resume_after_process_crash(&project, &renamed).unwrap();
        drop(project);
        assert_ne!(head_root(&restarted, main), durable_root);
        let mut database = open(&restarted, durability);
        database
            .query("CREATE (:Memory {id: 'after-restart'})")
            .unwrap();
        let epoch = database.commit_epoch().unwrap();
        model
            .observe(ObservationPoint {
                event: IoEvent::Remove,
                relative_path: sealed_root_directory(),
                boundary: ObservationBoundary::After,
                skip_matches: 0,
                include_descendants: true,
                keep_last: false,
            })
            .unwrap();
        database.checkpoint().unwrap();
        // The restart pins nothing older than the adopted head, so a second
        // checkpoint guarantees the durable root's closure is unreachable.
        database
            .query("CREATE (:Memory {id: 'second-after-restart'})")
            .unwrap();
        database.checkpoint().unwrap();
        model.capture().unwrap();
        let removed = model
            .take_observation()
            .unwrap()
            .expect("a restart checkpoint must sweep a superseded object");
        assert_no_object_debt(&database, "restart sweep");
        drop(database);
        drop(model);

        for plan in publication_fault_plans(&removed) {
            let root = fixture.image(&removed, &plan);
            let context = format!("{durability:?} {plan:?}");
            assert_ne!(
                head_root(&root, main),
                durable_root,
                "{context}: the pre-restart head returned after its objects were swept"
            );
            let mut recovered = open(&root, durability);
            assert!(recovered.commit_epoch().unwrap() >= epoch, "{context}");
            assert!(assert_atomic(&mut recovered, main), "{context}");
            assert_ids(
                &mut recovered,
                &["uncertain-head", "after-restart"],
                &context,
            );
            recovered.query("CREATE (:Memory {id: 'post'})").unwrap();
            recovered.checkpoint().unwrap();
            assert_no_object_debt(&recovered, &context);
            images += 1;
        }
        drop(restarted);
    }
    println!(
        "unsynchronized head restart object sweep qualification: {images} physical crash images"
    );
}
