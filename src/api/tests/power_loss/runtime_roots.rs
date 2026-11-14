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
use crate::{BranchReclamationLimits, QueryOutput};
use hawdb_storage::branch_head::read_branch_head;
use hawdb_storage::immutable_object::{ImmutableObjectStore, ObjectKind, ObjectReference};

fn assert_completed_prefix(database: &mut Database) {
    assert_eq!(
        database
            .query_sql("SELECT payload FROM atomic_tail WHERE id = 7")
            .unwrap()
            .rows[0]["payload"],
        Value::String("whole".into())
    );
    assert_eq!(
        database
            .query_sql("SELECT payload FROM pinned_tail WHERE id = 8")
            .unwrap()
            .rows[0]["payload"],
        Value::String("published".into())
    );
    assert_eq!(
        database
            .query("MATCH (m:Memory {id: 'pinned-tail'}) RETURN m.id")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn pinned_roots_and_reclamation_cuts_preserve_complete_live_heads() {
    let mut schedules = 0;
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        let mut fixture = Fixture::new();
        let mut database = open(&fixture.root, durability);
        commit_mixed(&mut database);
        database.checkpoint().unwrap();
        let main = database
            .describe_branch(BranchSelector::Name("main".into()))
            .unwrap()
            .id;
        let head_path = fixture
            .root
            .join("branches")
            .join(main.to_string())
            .join("branch.head");
        let historical = read_branch_head(&head_path).unwrap().sealed_root;
        let reader = database.begin_read_transaction().unwrap();
        database.schedule_external_content_artifact_job("pinned-source", "parse");
        let mut transaction = database.begin_transaction().unwrap();
        transaction
            .query_sql("CREATE TABLE pinned_tail (id BIGINT PRIMARY KEY, payload TEXT)")
            .unwrap();
        transaction
            .query_sql("INSERT INTO pinned_tail (id, payload) VALUES (8, 'published')")
            .unwrap();
        transaction
            .query("CREATE (:Memory {id: 'pinned-tail'})")
            .unwrap();
        transaction.commit().unwrap();
        database.checkpoint().unwrap();
        assert_ne!(
            historical,
            read_branch_head(&head_path).unwrap().sealed_root
        );
        let mut objects =
            ImmutableObjectStore::open(fixture.root.join("branches/objects")).unwrap();
        let orphan = ObjectReference::for_bytes(
            ObjectKind::CheckpointArtifact,
            1,
            b"unrelated pinned-GC orphan",
        );
        objects
            .publish(orphan, b"unrelated pinned-GC orphan")
            .unwrap();
        let orphan_path = objects.object_path(orphan);
        let old_path = objects.object_path(historical);
        fixture
            .model
            .observe(ObservationPoint {
                event: IoEvent::Remove,
                relative_path: orphan_path
                    .strip_prefix(&fixture.root)
                    .unwrap()
                    .to_path_buf(),
                boundary: ObservationBoundary::After,
                skip_matches: 0,
                include_descendants: false,
                keep_last: false,
            })
            .unwrap();
        let report = database
            .reclaim_branch_storage(BranchReclamationLimits::default())
            .unwrap();
        assert!(!report.deferred_for_active_leases);
        assert!(report.reclaimed_objects > 0);
        assert!(objects.read(historical).is_ok());
        assert!(!orphan_path.exists());
        let while_pinned = fixture
            .model
            .take_observation()
            .unwrap()
            .expect("observe actual orphan unlink with live pins");
        assert!(reader.query_sql("SELECT * FROM pinned_tail").is_err());
        assert_eq!(
            reader
                .query_sql("SELECT payload FROM atomic_tail WHERE id = 7")
                .unwrap()
                .rows[0]["payload"],
            Value::String("whole".into())
        );
        drop(reader);
        database
            .reclaim_branch_storage(BranchReclamationLimits::default())
            .unwrap();
        assert!(
            objects.read(historical).is_ok(),
            "pending job still owns the historical root"
        );
        database
            .run_next_external_content_artifact_job_with(|_| Ok(QueryOutput::from_rows(Vec::new())))
            .unwrap()
            .unwrap();
        fixture
            .model
            .observe(ObservationPoint {
                event: IoEvent::Remove,
                relative_path: old_path.strip_prefix(&fixture.root).unwrap().to_path_buf(),
                boundary: ObservationBoundary::After,
                skip_matches: 0,
                include_descendants: false,
                keep_last: false,
            })
            .unwrap();
        database
            .reclaim_branch_storage(BranchReclamationLimits::default())
            .unwrap();
        assert!(!old_path.exists());
        let after_release = fixture
            .model
            .take_observation()
            .unwrap()
            .expect("observe historical root unlink after every pin retires");
        let complete = fixture.model.capture().unwrap();
        assert!(fixture.model.project().metrics().high_water <= 32);
        drop(database);
        assert_eq!(fixture.model.project().metrics().admitted_runtimes, 0);
        for snapshot in [&while_pinned, &after_release, &complete] {
            for plan in publication_fault_plans(snapshot) {
                schedules += 1;
                let image = fixture.image(snapshot, &plan);
                let mut recovered = open(&image, durability);
                assert_completed_prefix(&mut recovered);
                let report = recovered
                    .reclaim_branch_storage(BranchReclamationLimits::default())
                    .unwrap();
                assert!(!report.deferred_for_active_leases);
                drop(recovered);
                assert_completed_prefix(&mut open(&image, durability));
            }
        }
    }
    eprintln!("branch-root-pins-gc-power-v1 schedules={schedules}");
}
