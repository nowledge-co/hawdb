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

const LABEL_USAGE_QUERY: &str = "MATCH (l:Label {id: $label_id}) \
     OPTIONAL MATCH (l)<-[r:HAS_LABEL]-(n) \
     WITH l, count(r) AS usage_count \
     RETURN l.id AS label_id, id(l) AS node_id, l.name AS name, \
       l.canonical_name AS canonical_name, l.color AS color, \
       l.description AS description, l.created_at AS created_at, \
       l.updated_at AS updated_at, usage_count \
     LIMIT 1";

const CANONICAL_LABEL_USAGE_QUERY: &str = "MATCH (l:Label) \
     WHERE l.canonical_name IS NOT NULL \
     OPTIONAL MATCH (l)<-[r:HAS_LABEL]-(n) \
     WITH l, count(r) AS usage_count \
     RETURN l.id AS label_id, id(l) AS node_id, l.name AS name, \
       l.canonical_name AS canonical_name, l.color AS color, \
       l.description AS description, l.created_at AS created_at, \
       l.updated_at AS updated_at, usage_count \
     ORDER BY node_id ASC \
     LIMIT $limit";

const ALL_LABEL_USAGE_QUERY: &str = "MATCH (l:Label) \
     OPTIONAL MATCH (l)<-[r:HAS_LABEL]-(n) \
     WITH l, count(r) AS usage_count \
     RETURN l.id AS label_id, id(l) AS node_id, l.name AS name, \
       l.canonical_name AS canonical_name, l.color AS color, \
       l.description AS description, l.created_at AS created_at, \
       l.updated_at AS updated_at, usage_count \
     ORDER BY node_id ASC \
     LIMIT $limit";

#[test]
fn label_usage_reads_use_fixed_bounded_queries() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 16,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Label {id: 'alpha', name: 'Alpha', canonical_name: 'alpha', color: '#fff', description: 'Alpha label', created_at: 10, updated_at: 20})")
        .unwrap();
    db.query("CREATE (:Label {id: 'beta', name: 'Beta', canonical_name: 'beta'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'draft', name: 'Draft'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory'}), (l:Label {id: 'alpha'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query(
        "MATCH (e:Entity {id: 'entity'}), (l:Label {id: 'alpha'}) CREATE (e)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    let single_parameters =
        BTreeMap::from([("label_id".to_string(), Value::String("alpha".to_string()))]);
    let list_parameters = BTreeMap::from([("limit".to_string(), Value::Int(3))]);
    let mut snapshot = db.begin_read_transaction();

    db.query("CREATE (:Memory {id: 'late-memory'})").unwrap();
    db.query("MATCH (m:Memory {id: 'late-memory'}), (l:Label {id: 'alpha'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();

    let single = snapshot
        .query_with_params_bounded(LABEL_USAGE_QUERY, &single_parameters, Some(1))
        .unwrap();
    assert_eq!(single.rows.len(), 1);
    assert_eq!(single.rows[0].get("usage_count"), Some(&Value::Int(2)));
    assert_eq!(
        single.rows[0].get("color"),
        Some(&Value::String("#fff".to_string()))
    );
    assert_eq!(single.rows[0].get("created_at"), Some(&Value::Int(10)));

    let canonical = snapshot
        .query_with_params_bounded(CANONICAL_LABEL_USAGE_QUERY, &list_parameters, Some(3))
        .unwrap();
    assert_eq!(canonical.rows.len(), 2);
    assert_eq!(
        canonical.rows[0].get("label_id"),
        Some(&Value::String("alpha".to_string()))
    );
    assert_eq!(canonical.rows[0].get("usage_count"), Some(&Value::Int(2)));
    assert_eq!(
        canonical.rows[1].get("label_id"),
        Some(&Value::String("beta".to_string()))
    );
    assert_eq!(canonical.rows[1].get("usage_count"), Some(&Value::Int(0)));

    let all = snapshot
        .query_with_params_bounded(ALL_LABEL_USAGE_QUERY, &list_parameters, Some(3))
        .unwrap();
    assert_eq!(all.rows.len(), 3);
    assert_eq!(
        all.rows[2].get("label_id"),
        Some(&Value::String("draft".to_string()))
    );

    let repeated = snapshot
        .query_with_params_bounded(LABEL_USAGE_QUERY, &single_parameters, Some(1))
        .unwrap();
    assert_eq!(repeated, single);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 3);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 3);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);
}

#[test]
fn label_usage_returns_no_row_for_missing_label() {
    let db = Database::new();
    let parameters =
        BTreeMap::from([("label_id".to_string(), Value::String("missing".to_string()))]);
    let mut snapshot = db.begin_read_transaction();

    let output = snapshot
        .query_with_params_bounded(LABEL_USAGE_QUERY, &parameters, Some(1))
        .unwrap();

    assert!(output.rows.is_empty());
}
