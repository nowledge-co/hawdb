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

const ENTITY_MENTION_COUNTS_FIRST_PAGE_QUERY: &str = "MATCH (e:Entity) \
     WHERE e.id IS NOT NULL AND e.name IS NOT NULL \
     OPTIONAL MATCH (:Memory)-[r:MENTIONS]->(e) \
     WITH e, count(r) AS mention_count \
     RETURN e.id AS entity_id, id(e) AS node_id, e.name AS name, \
       e.updated_at AS updated_at, mention_count \
     ORDER BY mention_count DESC, name ASC, entity_id ASC, node_id ASC \
     LIMIT $limit";

const ENTITY_MENTION_COUNTS_CURSOR_PAGE_QUERY: &str = "MATCH (e:Entity) \
     WHERE e.id IS NOT NULL AND e.name IS NOT NULL \
     OPTIONAL MATCH (:Memory)-[r:MENTIONS]->(e) \
     WITH e, count(r) AS mention_count \
     WHERE mention_count < $after_count \
       OR (mention_count = $after_count AND e.name > $after_name) \
     RETURN e.id AS entity_id, id(e) AS node_id, e.name AS name, \
       e.updated_at AS updated_at, mention_count \
     ORDER BY mention_count DESC, name ASC, entity_id ASC, node_id ASC \
     LIMIT $limit";

#[test]
fn entity_mention_counts_use_fixed_bounded_page_queries() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'memory-a'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory-b'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity-alpha', name: 'Alpha', updated_at: 10})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-beta', name: 'Beta', updated_at: 20})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-gamma', name: 'Gamma', updated_at: 30})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-no-name'})").unwrap();
    db.query("CREATE (:Entity {name: 'No id'})").unwrap();
    db.query("MATCH (m:Memory {id: 'memory-a'}), (e:Entity {id: 'entity-beta'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-b'}), (e:Entity {id: 'entity-beta'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-a'}), (e:Entity {id: 'entity-alpha'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'entity-alpha'}), (g:Entity {id: 'entity-gamma'}) CREATE (a)-[:MENTIONS]->(g)")
        .unwrap();
    let first_page_parameters = BTreeMap::from([("limit".to_string(), Value::Int(3))]);
    let cursor_page_parameters = BTreeMap::from([
        ("after_count".to_string(), Value::Int(1)),
        ("after_name".to_string(), Value::String("Alpha".to_string())),
        ("limit".to_string(), Value::Int(1)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    db.query("CREATE (:Memory {id: 'late-memory'})").unwrap();
    db.query("MATCH (m:Memory {id: 'late-memory'}), (e:Entity {id: 'entity-alpha'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();

    let first_page = snapshot
        .query_with_params_bounded(
            ENTITY_MENTION_COUNTS_FIRST_PAGE_QUERY,
            &first_page_parameters,
            Some(3),
        )
        .unwrap();
    assert_eq!(first_page.rows.len(), 3);
    assert_eq!(
        first_page.rows[0].get("entity_id"),
        Some(&Value::String("entity-beta".to_string()))
    );
    assert_eq!(
        first_page.rows[0].get("mention_count"),
        Some(&Value::Int(2))
    );
    assert_eq!(first_page.rows[0].get("updated_at"), Some(&Value::Int(20)));
    assert_eq!(
        first_page.rows[1].get("entity_id"),
        Some(&Value::String("entity-alpha".to_string()))
    );
    assert_eq!(
        first_page.rows[1].get("mention_count"),
        Some(&Value::Int(1))
    );
    assert_eq!(
        first_page.rows[2].get("entity_id"),
        Some(&Value::String("entity-gamma".to_string()))
    );
    assert_eq!(
        first_page.rows[2].get("mention_count"),
        Some(&Value::Int(0))
    );

    let cursor_page = snapshot
        .query_with_params_bounded(
            ENTITY_MENTION_COUNTS_CURSOR_PAGE_QUERY,
            &cursor_page_parameters,
            Some(1),
        )
        .unwrap();
    assert_eq!(cursor_page.rows.len(), 1);
    assert_eq!(
        cursor_page.rows[0].get("entity_id"),
        Some(&Value::String("entity-gamma".to_string()))
    );

    let repeated = snapshot
        .query_with_params_bounded(
            ENTITY_MENTION_COUNTS_CURSOR_PAGE_QUERY,
            &cursor_page_parameters,
            Some(1),
        )
        .unwrap();
    assert_eq!(repeated, cursor_page);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 2);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 2);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);
}

#[test]
fn entity_mention_count_pages_fail_closed_on_row_budget() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity-a', name: 'Alpha'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-b', name: 'Beta'})")
        .unwrap();
    let parameters = BTreeMap::from([("limit".to_string(), Value::Int(2))]);
    let mut snapshot = db.begin_read_transaction();

    let error = snapshot
        .query_with_params_bounded(ENTITY_MENTION_COUNTS_FIRST_PAGE_QUERY, &parameters, Some(1))
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("exceeding max_read_result_rows 1"),
        "{error}"
    );
}
