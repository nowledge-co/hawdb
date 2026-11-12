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

//! Physical images around the removal of a superseded branch-private WAL
//! (#1727). The removal follows the head that selects the successor WAL; no
//! crash image may lose a checkpointed transaction, and a leftover WAL is
//! reclaimed by a later checkpoint after recovery.

use super::*;

fn branch_wal_generations(root: &Path, main: Uuid) -> Vec<u64> {
    let directory = root.join("branches").join(main.to_string());
    let mut generations = std::fs::read_dir(directory)
        .unwrap()
        .filter_map(|entry| {
            let name = entry.unwrap().file_name();
            hawdb_storage::artifact_files::parse_generation_file(name.to_str()?, "wal.")
        })
        .collect::<Vec<_>>();
    generations.sort_unstable();
    generations
}

fn selected_generation(root: &Path, main: Uuid) -> u64 {
    hawdb_storage::branch_head::read_branch_head(
        &root
            .join("branches")
            .join(main.to_string())
            .join("branch.head"),
    )
    .unwrap()
    .active_wal
    .generation
}

#[test]
fn superseded_branch_wal_removal_cuts_recover_and_reclaim_leftovers() {
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
            database
                .query("CREATE (:Memory {id: 'reclaimed-tail'})")
                .unwrap();
            let epoch = database.commit_epoch().unwrap();
            // The WAL holding the tail is superseded by the next checkpoint.
            let superseded = selected_generation(&fixture.root, main);
            fixture
                .model
                .observe(ObservationPoint {
                    event: IoEvent::Remove,
                    relative_path: PathBuf::from("branches")
                        .join(main.to_string())
                        .join(wal_generation_file(superseded)),
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
                .expect("superseded branch WAL removal must be observed");
            assert_eq!(
                branch_wal_generations(&fixture.root, main),
                vec![selected_generation(&fixture.root, main)]
            );
            drop(database);
            for plan in publication_fault_plans(&snapshot) {
                let root = fixture.image(&snapshot, &plan);
                // The selecting head was durable before the removal began, so
                // every image recovers the complete checkpointed history.
                let selected = selected_generation(&root, main);
                assert!(
                    selected > superseded,
                    "{durability:?} {boundary:?} {plan:?}"
                );
                let mut recovered = open(&root, durability);
                assert_eq!(
                    recovered.commit_epoch().unwrap(),
                    epoch,
                    "{durability:?} {boundary:?} {plan:?}"
                );
                assert!(assert_atomic(&mut recovered, main));
                assert_eq!(
                    recovered
                        .query("MATCH (m:Memory {id: 'reclaimed-tail'}) RETURN m.id AS id")
                        .unwrap()
                        .rows
                        .len(),
                    1
                );
                // A removal that did not survive leaves only an unselected
                // leftover; the next checkpoint reclaims it without debt.
                recovered
                    .query("CREATE (:Memory {id: 'after-recovery'})")
                    .unwrap();
                recovered.checkpoint().unwrap();
                assert_eq!(
                    branch_wal_generations(&root, main),
                    vec![selected_generation(&root, main)],
                    "{durability:?} {boundary:?} {plan:?}"
                );
                let pressure = recovered.storage_pressure_snapshot().unwrap();
                assert!(!pressure.generation_reclamation_retry_required);
                assert_eq!(pressure.generation_reclamation_pending_files, 0);
                drop(recovered);
                let mut reopened = open(&root, durability);
                assert_eq!(reopened.commit_epoch().unwrap(), epoch + 1);
                assert!(assert_atomic(&mut reopened, main));
                images += 1;
            }
        }
    }
    println!("superseded branch WAL removal qualification: {images} physical crash images");
}
