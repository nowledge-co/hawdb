// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn carried_graph_checkpoint_head_cuts_recover_complete_graph_and_rows() {
    let mut images = 0;
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        // Both sides of selector publication and the returned checkpoint.
        for boundary in [
            Some(ObservationBoundary::Before),
            Some(ObservationBoundary::After),
            None,
        ] {
            let mut fixture = Fixture::new();
            let mut database = open(&fixture.root, durability);
            database
                .query("CREATE (:Carry {id: 1, body: 'retained graph'})")
                .unwrap();
            database
                .query_sql("CREATE TABLE carry_rows (id INTEGER PRIMARY KEY, body TEXT)")
                .unwrap();
            database.checkpoint().unwrap();
            let baseline_epoch = database.commit_epoch().unwrap();
            let baseline_checkpoint = database
                .published_read_view()
                .unwrap()
                .physical_generation()
                .unwrap();
            let graph_generation = database
                .runtime
                .get()
                .unwrap()
                .store
                .canonical_segment_manifest()
                .unwrap()
                .generation;
            let main = database
                .describe_branch(BranchSelector::Name("main".into()))
                .unwrap()
                .id;
            database
                .query_sql("INSERT INTO carry_rows VALUES (1, 'retained row')")
                .unwrap();
            let epoch = database.commit_epoch().unwrap();
            if let Some(boundary) = boundary {
                fixture
                    .model
                    .observe(ObservationPoint {
                        event: IoEvent::Rename,
                        relative_path: PathBuf::from("branches")
                            .join(main.to_string())
                            .join("branch.head"),
                        boundary,
                        skip_matches: 0,
                        include_descendants: false,
                        keep_last: false,
                    })
                    .unwrap();
            }
            database.checkpoint().unwrap();
            assert_eq!(
                database
                    .runtime
                    .get()
                    .unwrap()
                    .store
                    .canonical_segment_manifest()
                    .unwrap()
                    .generation,
                graph_generation
            );
            assert!(
                database
                    .published_read_view()
                    .unwrap()
                    .physical_generation()
                    .unwrap()
                    > graph_generation
            );
            let acknowledged = fixture.model.capture().unwrap();
            let snapshot = if boundary.is_some() {
                fixture
                    .model
                    .take_observation()
                    .unwrap()
                    .expect("carried checkpoint must publish a head")
            } else {
                acknowledged
            };
            drop(database);
            for plan in publication_fault_plans(&snapshot) {
                let root = fixture.image(&snapshot, &plan);
                let mut recovered = open(&root, durability);
                let recovered_epoch = recovered.commit_epoch().unwrap();
                let selected_checkpoint = recovered
                    .published_read_view()
                    .unwrap()
                    .physical_generation()
                    .unwrap();
                let graph = recovered
                    .query("MATCH (n:Carry) RETURN n.id AS id, n.body AS body")
                    .unwrap()
                    .rows;
                assert_eq!(graph.len(), 1);
                assert_eq!(graph[0]["body"], Value::String("retained graph".into()));
                let rows = recovered
                    .query_sql("SELECT body FROM carry_rows WHERE id = 1")
                    .unwrap()
                    .rows;
                let must_retain = durability == DurabilityPolicy::SyncOnEveryWrite
                    || boundary.is_none()
                    || selected_checkpoint > baseline_checkpoint;
                if must_retain {
                    assert_eq!(rows.len(), 1, "{durability:?} {boundary:?} {plan:?}");
                } else {
                    assert!(
                        rows.len() <= 1,
                        "relaxed old-head recovery must be a complete prefix"
                    );
                }
                assert_eq!(recovered_epoch, baseline_epoch + rows.len() as u64);
                if let Some(row) = rows.first() {
                    assert_eq!(row["body"], Value::String("retained row".into()));
                    assert_eq!(recovered_epoch, epoch);
                }
                let recovered_row_count = rows.len();
                recovered.checkpoint().unwrap();
                drop(recovered);
                let mut reopened = open(&root, durability);
                assert_eq!(reopened.commit_epoch().unwrap(), recovered_epoch);
                assert_eq!(
                    reopened
                        .query("MATCH (n:Carry) RETURN n.id")
                        .unwrap()
                        .rows
                        .len(),
                    1
                );
                assert_eq!(
                    reopened
                        .query_sql("SELECT id FROM carry_rows")
                        .unwrap()
                        .rows
                        .len(),
                    recovered_row_count
                );
                images += 1;
            }
        }
    }
    println!("carried graph checkpoint qualification: {images} physical crash images");
}
