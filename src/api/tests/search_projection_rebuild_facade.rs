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

#[test]
fn database_facade_background_search_projection_rebuild_uses_qos_admission() {
    let mut db = Database::new();
    for id in ["mem_1", "mem_2"] {
        db.query(&format!(
            "CREATE (:Memory {{id: '{id}', title: '{id}', content: 'background rebuild'}})"
        ))
        .unwrap();
    }
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert_projection_row(search_projection_row("old", "Old projection", "Keep me"))
        .unwrap();

    let plan = db
        .search_projection_rebuild_background_work_plan(
            &search_index,
            BackgroundWorkHint {
                active_topic: true,
                ..BackgroundWorkHint::default()
            },
        )
        .unwrap();
    assert_eq!(plan.request.class, WorkClass::Projection);
    assert_eq!(plan.request.estimated_operations, 2);

    let policy = LocalQosPolicy {
        max_background_operations: Some(1),
        ..LocalQosPolicy::default()
    };
    let error = db
        .rebuild_background_search_projection(
            &mut search_index,
            &policy,
            &LocalQosState::default(),
            SearchRebuildOptions::default(),
        )
        .unwrap_err();

    assert!(error.to_string().contains("deferred"));
    assert!(search_index.document("memory:old").is_some());
    assert!(search_index.document("memory:mem_1").is_none());
}

#[test]
fn database_facade_scheduled_search_projection_rebuild_releases_background_budget() {
    let mut db = Database::new();
    for id in ["mem_1", "mem_2"] {
        db.query(&format!(
            "CREATE (:Memory {{id: '{id}', title: '{id}', content: 'scheduled rebuild'}})"
        ))
        .unwrap();
    }
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert_projection_row(search_projection_row("old", "Old projection", "Keep me"))
        .unwrap();
    let scheduler = db.local_qos_scheduler();
    let error = db
        .rebuild_scheduled_background_search_projection(
            &mut search_index,
            SearchRebuildOptions { max_rows: Some(1) },
        )
        .unwrap_err();

    assert!(error.to_string().contains("row limit"));
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert!(search_index.document("memory:old").is_some());
    assert!(search_index.document("memory:mem_1").is_none());
}

#[test]
fn database_facade_repairs_search_projection_metadata_from_graph() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'mem_1', title: 'Graph title', content: 'graph body', source_id: 'src_1', space_id: 'team'})",
    )
    .unwrap();
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert(SearchDocument {
            id: "memory:mem_1".to_string(),
            title: "Old title".to_string(),
            content: "Old body should stay".to_string(),
            embedding: Some(vec![1.0, 0.0]),
            metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
        })
        .unwrap();

    let summary = db
        .repair_search_projection_metadata(&mut search_index, MetadataRepairOptions::default())
        .unwrap();

    assert_eq!(summary.scanned_nodes, 1);
    assert_eq!(summary.repaired_documents, 1);
    let document = search_index.document("memory:mem_1").unwrap();
    assert_eq!(document.title, "Old title");
    assert_eq!(document.content, "Old body should stay");
    assert_eq!(document.embedding, Some(vec![1.0, 0.0]));
    assert_eq!(
        document.metadata.get("kind").map(String::as_str),
        Some("memory")
    );
    assert_eq!(
        document.metadata.get("source_id").map(String::as_str),
        Some("src_1")
    );
    assert_eq!(
        document.metadata.get("space_id").map(String::as_str),
        Some("team")
    );
}

#[test]
fn database_facade_background_search_projection_metadata_repair_uses_qos_admission() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Graph title'})")
        .unwrap();
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert(SearchDocument {
            id: "memory:mem_1".to_string(),
            title: "Old title".to_string(),
            content: "Old body should stay".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
        })
        .unwrap();

    let plan = db
        .search_projection_metadata_repair_background_work_plan(
            &search_index,
            BackgroundWorkHint {
                staleness_millis: 10_000,
                staleness_ttl_millis: Some(1_000),
                ..BackgroundWorkHint::default()
            },
        )
        .unwrap();
    assert_eq!(plan.request.class, WorkClass::Projection);
    assert_eq!(plan.request.estimated_operations, 1);

    let policy = LocalQosPolicy {
        max_background_operations: Some(0),
        ..LocalQosPolicy::default()
    };
    let error = db
        .repair_background_search_projection_metadata(
            &mut search_index,
            &policy,
            &LocalQosState::default(),
            MetadataRepairOptions::default(),
            plan.request.estimated_operations,
        )
        .unwrap_err();

    assert!(error.to_string().contains("deferred"));
    assert_eq!(
        search_index
            .document("memory:mem_1")
            .unwrap()
            .metadata
            .get("kind")
            .map(String::as_str),
        Some("stale")
    );
}

#[test]
fn database_facade_scheduled_search_projection_metadata_repair_releases_background_budget() {
    let mut db = Database::new();
    for id in ["mem_1", "mem_2"] {
        db.query(&format!("CREATE (:Memory {{id: '{id}', title: '{id}'}})"))
            .unwrap();
    }
    let mut search_index = SearchIndex::in_memory();
    for id in ["mem_1", "mem_2"] {
        search_index
            .upsert(SearchDocument {
                id: format!("memory:{id}"),
                title: id.to_string(),
                content: id.to_string(),
                embedding: None,
                metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
            })
            .unwrap();
    }
    let scheduler = db.local_qos_scheduler();
    let error = db
        .repair_scheduled_background_search_projection_metadata(
            &mut search_index,
            MetadataRepairOptions { max_rows: Some(1) },
            2,
        )
        .unwrap_err();

    assert!(error.to_string().contains("row limit"));
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        search_index
            .document("memory:mem_1")
            .unwrap()
            .metadata
            .get("kind")
            .map(String::as_str),
        Some("stale")
    );
}
