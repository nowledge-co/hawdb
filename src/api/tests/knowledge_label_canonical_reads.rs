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

const CANONICAL_LOOKUP_QUERY: &str = "MATCH (l:Label) \
     WHERE l.canonical_name = $canonical_name \
     OPTIONAL MATCH (l)<-[r:HAS_LABEL]-(n) \
     WITH l, count(r) AS usage_count \
     RETURN l.id AS label_id, id(l) AS node_id, l.name AS name, \
       l.canonical_name AS canonical_name, l.color AS color, \
       l.description AS description, l.created_at AS created_at, \
       l.updated_at AS updated_at, usage_count \
     ORDER BY node_id ASC \
     LIMIT $limit";

const CANONICAL_LOOKUP_EXCLUDING_QUERY: &str = "MATCH (l:Label) \
     WHERE l.canonical_name = $canonical_name AND l.id <> $exclude_label_id \
     OPTIONAL MATCH (l)<-[r:HAS_LABEL]-(n) \
     WITH l, count(r) AS usage_count \
     RETURN l.id AS label_id, id(l) AS node_id, l.name AS name, \
       l.canonical_name AS canonical_name, l.color AS color, \
       l.description AS description, l.created_at AS created_at, \
       l.updated_at AS updated_at, usage_count \
     ORDER BY node_id ASC \
     LIMIT $limit";

const MISSING_CANONICAL_QUERY: &str = "MATCH (l:Label) \
     WHERE l.canonical_name IS NULL \
     OPTIONAL MATCH (l)<-[r:HAS_LABEL]-(n) \
     WITH l, count(r) AS usage_count \
     RETURN l.id AS label_id, id(l) AS node_id, l.name AS name, \
       l.canonical_name AS canonical_name, l.color AS color, \
       l.description AS description, l.created_at AS created_at, \
       l.updated_at AS updated_at, usage_count \
     ORDER BY node_id ASC \
     LIMIT $limit";

const MISSING_CANONICAL_EXCLUDING_QUERY: &str = "MATCH (l:Label) \
     WHERE l.canonical_name IS NULL AND l.id <> $exclude_label_id \
     OPTIONAL MATCH (l)<-[r:HAS_LABEL]-(n) \
     WITH l, count(r) AS usage_count \
     RETURN l.id AS label_id, id(l) AS node_id, l.name AS name, \
       l.canonical_name AS canonical_name, l.color AS color, \
       l.description AS description, l.created_at AS created_at, \
       l.updated_at AS updated_at, usage_count \
     ORDER BY node_id ASC \
     LIMIT $limit";

#[test]
fn label_canonical_reads_use_fixed_bounded_queries() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 16,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Label {id: 'source', name: 'Source', canonical_name: 'target'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'target-a', name: 'Target A', canonical_name: 'target', color: '#fff'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'target-b', name: 'Target B', canonical_name: 'target'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'missing-a', name: 'Missing A'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'missing-b', name: 'Missing B', canonical_name: NULL})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory'}), (l:Label {id: 'target-a'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    let lookup_parameters = BTreeMap::from([
        (
            "canonical_name".to_string(),
            Value::String("target".to_string()),
        ),
        ("limit".to_string(), Value::Int(3)),
    ]);
    let lookup_excluding_parameters = BTreeMap::from([
        (
            "canonical_name".to_string(),
            Value::String("target".to_string()),
        ),
        (
            "exclude_label_id".to_string(),
            Value::String("source".to_string()),
        ),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let missing_parameters = BTreeMap::from([("limit".to_string(), Value::Int(2))]);
    let missing_excluding_parameters = BTreeMap::from([
        (
            "exclude_label_id".to_string(),
            Value::String("missing-a".to_string()),
        ),
        ("limit".to_string(), Value::Int(1)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    db.query("CREATE (:Label {id: 'late', name: 'Late', canonical_name: 'target'})")
        .unwrap();

    let lookup = snapshot
        .query_with_params_bounded(CANONICAL_LOOKUP_QUERY, &lookup_parameters, Some(3))
        .unwrap();
    assert_eq!(lookup.rows.len(), 3);
    assert_eq!(lookup.rows[1].get("usage_count"), Some(&Value::Int(1)));

    let lookup_excluding = snapshot
        .query_with_params_bounded(
            CANONICAL_LOOKUP_EXCLUDING_QUERY,
            &lookup_excluding_parameters,
            Some(2),
        )
        .unwrap();
    assert_eq!(lookup_excluding.rows.len(), 2);
    assert_eq!(
        lookup_excluding.rows[0].get("label_id"),
        Some(&Value::String("target-a".to_string()))
    );
    assert_eq!(
        lookup_excluding.rows[0].get("color"),
        Some(&Value::String("#fff".to_string()))
    );

    let missing = snapshot
        .query_with_params_bounded(MISSING_CANONICAL_QUERY, &missing_parameters, Some(2))
        .unwrap();
    assert_eq!(missing.rows.len(), 2);

    let missing_excluding = snapshot
        .query_with_params_bounded(
            MISSING_CANONICAL_EXCLUDING_QUERY,
            &missing_excluding_parameters,
            Some(1),
        )
        .unwrap();
    assert_eq!(missing_excluding.rows.len(), 1);
    assert_eq!(
        missing_excluding.rows[0].get("label_id"),
        Some(&Value::String("missing-b".to_string()))
    );

    let repeated = snapshot
        .query_with_params_bounded(
            CANONICAL_LOOKUP_EXCLUDING_QUERY,
            &lookup_excluding_parameters,
            Some(2),
        )
        .unwrap();
    assert_eq!(repeated, lookup_excluding);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 4);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 4);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);
}
