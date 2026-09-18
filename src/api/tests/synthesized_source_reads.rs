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

const SYNTHESIZED_COVERAGE_QUERY: &str = "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) \
     WHERE c.is_crystal = true AND s.id IN $source_memory_ids \
     WITH c.id AS crystal_memory_id, id(c) AS crystal_node_id, \
     c.crystal_title AS crystal_title, COUNT(DISTINCT s.id) AS covered_count, \
     COLLECT(DISTINCT s.id) AS matched_source_memory_ids \
     WHERE covered_count = $required_covered_count \
     RETURN crystal_memory_id, crystal_node_id, crystal_title, covered_count, \
     matched_source_memory_ids \
     ORDER BY crystal_memory_id ASC, crystal_node_id ASC";

const SYNTHESIZED_COVERAGE_PAGE_QUERY: &str = "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) \
     WHERE c.is_crystal = true AND s.id IN $source_memory_ids \
     WITH c.id AS crystal_memory_id, id(c) AS crystal_node_id, \
     c.crystal_title AS crystal_title, COUNT(DISTINCT s.id) AS covered_count, \
     COLLECT(DISTINCT s.id) AS matched_source_memory_ids \
     WHERE covered_count = $required_covered_count \
     RETURN crystal_memory_id, crystal_node_id, crystal_title, covered_count, \
     matched_source_memory_ids \
     ORDER BY crystal_memory_id ASC, crystal_node_id ASC LIMIT $limit";

const SYNTHESIZED_CRYSTALS_QUERY: &str = "MATCH (c:Memory) \
     WHERE c.id IN $crystal_memory_ids \
     RETURN c.id AS crystal_memory_id, id(c) AS crystal_node_id \
     ORDER BY crystal_memory_id ASC";

const SYNTHESIZED_SOURCE_IDS_QUERY: &str = "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) \
     WHERE c.id IN $crystal_memory_ids \
     WITH c.id AS crystal_memory_id, COLLECT(DISTINCT s.id) AS source_memory_ids \
     RETURN crystal_memory_id, source_memory_ids ORDER BY crystal_memory_id ASC";

#[test]
fn synthesized_coverage_uses_fixed_bounded_aggregate_queries() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'crystal-a', is_crystal: true, crystal_title: 'A'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'crystal-b', is_crystal: true, crystal_title: 'B'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'source-a'})").unwrap();
    db.query("CREATE (:Memory {id: 'source-b'})").unwrap();
    db.query("MATCH (c:Memory {id: 'crystal-a'}), (s:Memory {id: 'source-a'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)")
        .unwrap();
    db.query("MATCH (c:Memory {id: 'crystal-a'}), (s:Memory {id: 'source-b'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)")
        .unwrap();
    db.query("MATCH (c:Memory {id: 'crystal-b'}), (s:Memory {id: 'source-a'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)")
        .unwrap();
    let parameters = BTreeMap::from([
        (
            "source_memory_ids".to_string(),
            Value::List(vec![
                Value::String("source-a".to_string()),
                Value::String("source-b".to_string()),
            ]),
        ),
        ("required_covered_count".to_string(), Value::Int(1)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    let all = snapshot
        .query_with_params_bounded(SYNTHESIZED_COVERAGE_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(all.rows.len(), 1);
    assert_eq!(
        all.rows[0].get("crystal_memory_id"),
        Some(&Value::String("crystal-b".to_string()))
    );
    let mut page_parameters = parameters.clone();
    page_parameters.insert("limit".to_string(), Value::Int(1));
    let first = snapshot
        .query_with_params_bounded(SYNTHESIZED_COVERAGE_PAGE_QUERY, &page_parameters, Some(1))
        .unwrap();
    let second = snapshot
        .query_with_params_bounded(SYNTHESIZED_COVERAGE_PAGE_QUERY, &page_parameters, Some(1))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 2);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 2);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);
}

#[test]
fn synthesized_source_ids_use_two_named_queries_on_one_snapshot() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'crystal-a', is_crystal: true})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'crystal-b', is_crystal: true})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'source-a'})").unwrap();
    db.query("MATCH (c:Memory {id: 'crystal-a'}), (s:Memory {id: 'source-a'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)")
        .unwrap();
    let parameters = BTreeMap::from([(
        "crystal_memory_ids".to_string(),
        Value::List(vec![
            Value::String("crystal-a".to_string()),
            Value::String("crystal-b".to_string()),
            Value::String("missing".to_string()),
        ]),
    )]);
    let mut snapshot = db.begin_read_transaction();

    let crystals = snapshot
        .query_with_params_bounded(SYNTHESIZED_CRYSTALS_QUERY, &parameters, Some(3))
        .unwrap();
    assert_eq!(crystals.rows.len(), 2);
    let sources = snapshot
        .query_with_params_bounded(SYNTHESIZED_SOURCE_IDS_QUERY, &parameters, Some(3))
        .unwrap();
    assert_eq!(sources.rows.len(), 1);
    assert_eq!(
        sources.rows[0].get("source_memory_ids"),
        Some(&Value::List(vec![Value::String("source-a".to_string())]))
    );

    db.query("MATCH (c:Memory {id: 'crystal-b'}), (s:Memory {id: 'source-a'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)")
        .unwrap();
    let pinned = snapshot
        .query_with_params_bounded(SYNTHESIZED_SOURCE_IDS_QUERY, &parameters, Some(3))
        .unwrap();
    assert_eq!(pinned.rows.len(), 1);
}
