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
fn retrieves_knowledge_entity_without_search_projection() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'HawDB', kind: 'database', score: 7})")
        .unwrap();

    let output = db
        .query_entity_via_cypher(&KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "entity_1".to_string(),
        })
        .unwrap();

    let entity = output.entity.expect("expected entity");
    assert_eq!(output.graph_commit_epoch, 1);
    assert_eq!(entity.node_id, 0);
    assert_eq!(entity.labels, vec!["Entity".to_string()]);
    assert_eq!(entity.external_id.as_deref(), Some("entity_1"));
    assert_eq!(
        entity.properties.get("name"),
        Some(&Value::String("HawDB".to_string()))
    );
    assert_eq!(entity.properties.get("score"), Some(&Value::Int(7)));
}

#[test]
fn scoped_knowledge_entity_filters_by_metadata() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'Scoped', source_id: 'thread_1', space_id: ''})",
    )
    .unwrap();

    let scoped = db
        .query_scoped_entity_via_cypher(&KnowledgeScopedEntityRequest {
            entity: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            metadata_filters: BTreeMap::from([
                ("source_id".to_string(), "thread_1".to_string()),
                ("space_id".to_string(), "default".to_string()),
            ]),
        })
        .unwrap();

    let entity = scoped.entity.expect("expected scoped entity");
    assert_eq!(scoped.graph_commit_epoch, 1);
    assert_eq!(entity.external_id.as_deref(), Some("memory_1"));

    let filtered = db
        .query_scoped_entity_via_cypher(&KnowledgeScopedEntityRequest {
            entity: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            metadata_filters: BTreeMap::from([("source_id".to_string(), "thread_2".to_string())]),
        })
        .unwrap();

    assert_eq!(filtered.graph_commit_epoch, 1);
    assert!(filtered.entity.is_none());
}

#[test]
fn retrieves_knowledge_entity_batch_in_request_order() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'First'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', title: 'Second'})")
        .unwrap();

    let output = db
        .query_entity_batch_via_cypher(&KnowledgeEntityBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "missing".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, 2);
    assert_eq!(output.entities.len(), 3);
    assert_eq!(output.found_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.filtered_out_count, 0);
    assert_eq!(
        output.entities[0]
            .as_ref()
            .and_then(|entity| entity.external_id.as_deref()),
        Some("memory_2")
    );
    assert!(output.entities[1].is_none());
    assert_eq!(
        output.entities[2]
            .as_ref()
            .and_then(|entity| entity.external_id.as_deref()),
        Some("memory_1")
    );
}

#[test]
fn knowledge_entity_batch_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'cache_memory_1', title: 'First'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'cache_memory_2', title: 'Second'})")
        .unwrap();
    let request = KnowledgeEntityBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "cache_memory_2".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "cache_missing".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "cache_memory_1".to_string(),
            },
        ],
    };

    let first = db.query_entity_batch_via_cypher(&request).unwrap();
    let second = db.query_entity_batch_via_cypher(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.found_count, 2);
    assert_eq!(first.missing_count, 1);
    assert_eq!(
        first.entities[0]
            .as_ref()
            .and_then(|entity| entity.external_id.as_deref()),
        Some("cache_memory_2")
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}
