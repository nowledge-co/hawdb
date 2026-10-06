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
use crate::HawDBError;

// Frozen migration case mem-0344: the clause grammar now preserves the
// relationship predicate that the query-catalogue parser rejected.
const FILTERED_OPTIONAL_COUNT: &str = "MATCH (memory:Memory {id: $memory_id}) \
    OPTIONAL MATCH (memory)-[edge:EVOLVES]->(:Memory) \
    WHERE edge.content_relation = 'replaces' \
    RETURN memory.is_latest, COUNT(edge)";

// Frozen migration case mem-0361: parsing is separate from atomic write admission.
const MULTIPLE_PATTERN_SET: &str = "MATCH (older:Memory {id: $older_id}), \
    (newer:Memory {id: $newer_id}) \
    SET older.is_latest = true, newer.is_latest = true";

#[test]
fn default_pipeline_optional_predicate_retains_filtering_and_null_extension() {
    let mut db = Database::new();
    assert_eq!(
        db.query(FILTERED_OPTIONAL_COUNT).unwrap_err(),
        HawDBError::Semantic("missing parameter '$memory_id'".to_string())
    );
    for query in [
        "CREATE (:Memory {id: 'source', is_latest: false})",
        "CREATE (:Memory {id: 'target', is_latest: true})",
        "CREATE (:Memory {id: 'other', is_latest: true})",
        "CREATE (:Memory {id: 'isolated', is_latest: true})",
        "MATCH (a:Memory {id: 'source'}), (b:Memory {id: 'target'}) CREATE (a)-[:EVOLVES {content_relation: 'replaces'}]->(b)",
        "MATCH (a:Memory {id: 'source'}), (b:Memory {id: 'other'}) CREATE (a)-[:EVOLVES {content_relation: 'extends'}]->(b)",
    ] {
        db.query(query).unwrap();
    }
    let mut snapshot = db.begin_read_transaction().unwrap();
    // Rebind the same cached read with different ids; an unmatched OPTIONAL
    // relationship keeps the Memory row and produces count zero.
    for (id, latest, count) in [
        ("source", false, 1),
        ("target", true, 0),
        ("isolated", true, 0),
        ("source", false, 1),
    ] {
        let params = BTreeMap::from([("memory_id".to_string(), Value::String(id.to_string()))]);
        let result = snapshot
            .query_with_params_bounded(FILTERED_OPTIONAL_COUNT, &params, Some(1))
            .unwrap();
        assert_eq!(
            result.rows,
            vec![BTreeMap::from([
                ("memory.is_latest".to_string(), Value::Bool(latest)),
                ("count(edge)".to_string(), Value::Int(count)),
            ])],
            "{id}"
        );
    }
    let params = BTreeMap::from([(
        "memory_id".to_string(),
        Value::String("missing".to_string()),
    )]);
    assert!(snapshot
        .query_with_params_bounded(FILTERED_OPTIONAL_COUNT, &params, Some(1))
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn default_pipeline_multiple_pattern_set_fails_before_mutation() {
    for populated in [false, true] {
        let mut db = Database::new();
        if populated {
            db.query("CREATE (:Memory {id: 'older', is_latest: false})")
                .unwrap();
            db.query("CREATE (:Memory {id: 'newer', is_latest: false})")
                .unwrap();
        }
        let before = db
            .query("MATCH (m:Memory) RETURN m.id, m.is_latest ORDER BY m.id")
            .unwrap()
            .rows;
        let params = BTreeMap::from([
            ("older_id".to_string(), Value::String("older".to_string())),
            ("newer_id".to_string(), Value::String("newer".to_string())),
        ]);
        for params in [&BTreeMap::new(), &params] {
            assert_eq!(
                db.query_with_params(MULTIPLE_PATTERN_SET, params)
                    .unwrap_err(),
                HawDBError::Semantic("mutation requires one bound MATCH pattern".to_string()),
                "populated={populated}"
            );
            assert_eq!(
                db.query("MATCH (m:Memory) RETURN m.id, m.is_latest ORDER BY m.id")
                    .unwrap()
                    .rows,
                before
            );
        }
    }
}
