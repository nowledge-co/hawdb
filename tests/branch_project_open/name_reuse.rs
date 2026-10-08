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

use super::{values, Project};
use hawdb::{BranchCreateRequest, BranchSelector, Database, Value};

fn reused_branch_name(
    old_id_first: bool,
) -> (Project, Database, hawdb::BranchInfo, hawdb::BranchInfo) {
    let project = Project::new();
    let mut database = Database::open(&project.0).unwrap();
    database.query("CREATE (:Memory {id: 'source'})").unwrap();
    let epoch = database.commit_epoch().unwrap();
    let keys = if old_id_first {
        ["reuse-name-first", "reuse-name-second"]
    } else {
        ["reuse-name-second", "reuse-name-first"]
    };
    let create_sql = "CREATE BRANCH NAME $1 FROM main AT REVISION $2 REQUEST KEY $3";
    let parameters = |key: &str| {
        [
            Value::String("reused".into()),
            Value::Int(epoch as i64),
            Value::String(key.into()),
        ]
    };
    let first = database
        .query_sql_with_params(create_sql, &parameters(keys[0]))
        .unwrap();
    let Value::Uuid(old_id) = first.rows[0]["branch_id"] else {
        panic!("created branch UUID")
    };
    database
        .query_sql_with_params(
            "DROP BRANCH ID $1 AT REVISION $2",
            &[
                Value::Uuid(old_id),
                first.rows[0]["metadata_revision"].clone(),
            ],
        )
        .unwrap();
    let old = database
        .describe_branch(BranchSelector::Id(old_id))
        .unwrap();
    assert_eq!(old.state, hawdb::BranchLifecycleState::Deleted);
    assert_eq!(
        database
            .describe_branch(BranchSelector::Name(old.name.clone()))
            .unwrap(),
        old
    );
    let second = database
        .query_sql_with_params(create_sql, &parameters(keys[1]))
        .unwrap();
    let Value::Uuid(new_id) = second.rows[0]["branch_id"] else {
        panic!("replacement branch UUID")
    };
    assert_ne!(old_id, new_id);
    assert_eq!(old_id < new_id, old_id_first);
    let current = database
        .describe_branch(BranchSelector::Id(new_id))
        .unwrap();
    assert_eq!(current.state, hawdb::BranchLifecycleState::Ready);
    (project, database, old, current)
}

#[test]
fn reused_branch_name_inspection_prefers_current_identity_in_both_uuid_orders() {
    for old_id_first in [true, false] {
        let (project, mut database, old, current) = reused_branch_name(old_id_first);
        let parameters = [Value::String(current.name.clone())];
        let shown = database
            .query_sql_with_params("SHOW BRANCH NAME $1", &parameters)
            .unwrap();
        assert_eq!(shown.rows[0]["branch_id"], Value::Uuid(current.id));
        assert_eq!(
            database
                .describe_branch(BranchSelector::Name(current.name.clone()))
                .unwrap(),
            current
        );
        let mut transaction = database.begin_transaction().unwrap();
        assert_eq!(
            transaction
                .query_sql_with_params("SHOW BRANCH NAME $1", &parameters)
                .unwrap()
                .rows[0]["branch_id"],
            Value::Uuid(current.id)
        );
        transaction.rollback();
        let reader = database.begin_read_transaction().unwrap();
        assert_eq!(
            reader
                .query_sql_with_params("SHOW BRANCH NAME $1", &parameters)
                .unwrap()
                .rows[0]["branch_id"],
            Value::Uuid(current.id)
        );
        drop(reader);
        let listed = database.query_sql("SHOW BRANCHES LIMIT 3").unwrap();
        assert_eq!(listed.rows.len(), 3);
        assert!(listed
            .rows
            .iter()
            .any(|row| row["branch_id"] == Value::Uuid(old.id)));
        assert!(listed
            .rows
            .iter()
            .any(|row| row["branch_id"] == Value::Uuid(current.id)));
        assert_eq!(
            database
                .describe_branch(BranchSelector::Id(old.id))
                .unwrap(),
            old
        );
        drop(database);
        let shared = Database::open(&project.0).unwrap().into_concurrent();
        assert_eq!(
            shared
                .query_sql_with_params("SHOW BRANCH NAME $1", &parameters)
                .unwrap()
                .rows[0]["branch_id"],
            Value::Uuid(current.id)
        );
    }
}

#[test]
fn reused_branch_name_selection_admits_current_identity_in_both_uuid_orders() {
    for old_id_first in [true, false] {
        let (_project, mut database, old, current) = reused_branch_name(old_id_first);
        let selected = database.query_sql("SHOW CURRENT BRANCH").unwrap();
        assert!(database
            .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(old.id)])
            .is_err());
        assert_eq!(database.query_sql("SHOW CURRENT BRANCH").unwrap(), selected);
        database
            .query_sql_with_params("USE BRANCH NAME $1", &[Value::String(current.name)])
            .unwrap();
        assert_eq!(
            database.query_sql("SHOW CURRENT BRANCH").unwrap().rows[0]["branch_id"],
            Value::Uuid(current.id)
        );
        assert_eq!(
            values(&mut database)[0]["id"],
            Value::String("source".into())
        );
    }
}

#[test]
fn reused_branch_name_create_parent_uses_current_identity_in_both_uuid_orders() {
    for old_id_first in [true, false] {
        let (_project, mut database, _old, current) = reused_branch_name(old_id_first);
        let created = database
            .query_sql_with_params(
                "CREATE BRANCH descendant FROM NAME $1 AT REVISION $2 REQUEST KEY $3",
                &[
                    Value::String(current.name.clone()),
                    Value::Int(current.source_commit_epoch as i64),
                    Value::String("reuse-name-descendant".into()),
                ],
            )
            .unwrap();
        assert_eq!(created.rows[0]["parent_id"], Value::Uuid(current.id));
        let typed = database
            .create_branch(BranchCreateRequest {
                name: Some("typed-descendant".into()),
                parent: BranchSelector::Name(current.name),
                expected_source_commit_epoch: current.source_commit_epoch,
                owner: None,
                idempotency_key: "reuse-name-typed-descendant".into(),
            })
            .unwrap();
        assert_eq!(typed.parent_id, Some(current.id));
    }
}

#[test]
fn reused_branch_name_typed_delete_targets_current_identity_in_both_uuid_orders() {
    for old_id_first in [true, false] {
        let (_project, database, old, current) = reused_branch_name(old_id_first);
        let deleted = database
            .delete_branch(BranchSelector::Name(current.name))
            .unwrap();
        assert_eq!(deleted.id, current.id);
        assert_eq!(deleted.state, hawdb::BranchLifecycleState::Deleted);
        assert_eq!(
            database
                .describe_branch(BranchSelector::Id(old.id))
                .unwrap(),
            old
        );
        assert_eq!(
            database.delete_branch(BranchSelector::Id(old.id)).unwrap(),
            old
        );
    }
}

#[test]
fn reused_branch_name_inspection_preserves_pending_delete_over_older_receipts() {
    for old_id_first in [true, false] {
        let (project, mut database, old, current) = reused_branch_name(old_id_first);
        // Leave the durable reservation unfinished, as after a lost response
        // between delete publication and facade finalization.
        hawdb_storage::branch_catalog::begin_delete_file(
            &project.0.join("branches/catalog.hawdb"),
            hawdb_storage::branch_catalog::DeleteRequest {
                id: hawdb_storage::branch_catalog::BranchId::new(current.id).unwrap(),
                expected_metadata_revision: current.metadata_revision,
            },
        )
        .unwrap();
        let pending = database
            .describe_branch(BranchSelector::Id(current.id))
            .unwrap();
        assert_eq!(pending.state, hawdb::BranchLifecycleState::Deleting);
        assert_eq!(
            database
                .describe_branch(BranchSelector::Name(current.name.clone()))
                .unwrap(),
            pending
        );
        let shown = database
            .query_sql_with_params("SHOW BRANCH NAME $1", &[Value::String(current.name)])
            .unwrap();
        assert_eq!(shown.rows[0]["branch_id"], Value::Uuid(current.id));
        assert_eq!(shown.rows[0]["state"], Value::String("deleting".into()));
        assert_eq!(
            database
                .delete_branch(BranchSelector::Id(current.id))
                .unwrap()
                .state,
            hawdb::BranchLifecycleState::Deleted
        );
        assert_eq!(
            database
                .describe_branch(BranchSelector::Id(old.id))
                .unwrap(),
            old
        );
    }
}
