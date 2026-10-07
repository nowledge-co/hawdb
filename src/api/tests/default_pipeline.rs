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
    for populated in [true, false] {
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
        for params in [&params, &BTreeMap::new()] {
            let result = db.query_with_params(MULTIPLE_PATTERN_SET, params);
            assert_eq!(
                db.query("MATCH (m:Memory) RETURN m.id, m.is_latest ORDER BY m.id")
                    .unwrap()
                    .rows,
                before,
                "rejected multi-pattern mutation must retain both nodes: {result:?}"
            );
            assert_eq!(
                result.unwrap_err(),
                HawDBError::Semantic("mutation requires one bound MATCH pattern".to_string()),
                "populated={populated}"
            );
        }
    }
}

fn optional_endpoint_fixture() -> Database {
    let mut db = Database::new();
    for query in [
        "CREATE (:Item {id: 'source'})",
        "CREATE (:Item {id: 'only_bad'})",
        "CREATE (:Item {id: 'isolated'})",
        "CREATE (:Item {id: 'good', keep: true})",
        "CREATE (:Item {id: 'bad', keep: false})",
        "MATCH (a:Item {id: 'source'}), (b:Item {id: 'good'}) CREATE (a)-[:REL]->(b)",
        "MATCH (a:Item {id: 'source'}), (b:Item {id: 'bad'}) CREATE (a)-[:REL]->(b)",
        "MATCH (a:Item {id: 'only_bad'}), (b:Item {id: 'bad'}) CREATE (a)-[:REL]->(b)",
    ] {
        db.query(query).unwrap();
    }
    db
}

#[test]
fn default_pipeline_optional_endpoint_properties_preserve_rows_and_null_extension() {
    let db = optional_endpoint_fixture();
    for with in ["", "WITH a ", "WITH a WITH a "] {
        let query = format!("MATCH (a:Item {{id: $id}}) {with}OPTIONAL MATCH (a)-[:REL]->(b:Item {{keep: true}}) RETURN b.id");
        let mut snapshot = db.begin_read_transaction().unwrap();
        for (id, value) in [
            ("source", Value::String("good".to_string())),
            ("only_bad", Value::Null),
            ("isolated", Value::Null),
            ("source", Value::String("good".to_string())),
        ] {
            let params = BTreeMap::from([("id".to_string(), Value::String(id.to_string()))]);
            assert_eq!(
                snapshot
                    .query_with_params_bounded(&query, &params, Some(2))
                    .unwrap()
                    .rows,
                vec![BTreeMap::from([("b.id".to_string(), value)])],
                "with={with:?}, id={id}"
            );
        }
        let params = BTreeMap::from([("id".to_string(), Value::String("missing".to_string()))]);
        assert!(snapshot
            .query_with_params_bounded(&query, &params, Some(2))
            .unwrap()
            .rows
            .is_empty());
    }
}

#[test]
fn default_pipeline_optional_endpoint_properties_preserve_counts() {
    let mut db = optional_endpoint_fixture();
    for with in ["", "WITH a ", "WITH a WITH a "] {
        let query = format!("MATCH (a:Item {{id: $id}}) {with}OPTIONAL MATCH (a)-[:REL]->(b:Item {{keep: true}}) RETURN COUNT(b)");
        for (id, count) in [("source", 1), ("only_bad", 0), ("isolated", 0)] {
            let params = BTreeMap::from([("id".to_string(), Value::String(id.to_string()))]);
            assert_eq!(
                db.query_with_params(&query, &params).unwrap().rows,
                vec![BTreeMap::from([(
                    "count(b)".to_string(),
                    Value::Int(count)
                )])],
                "with={with:?}, id={id}"
            );
        }
    }
}

#[test]
fn default_pipeline_multi_with_retains_bounded_optional_results_and_cache() {
    let mut db = Database::new();
    for query in [
        "CREATE (:Node {id: 'source'})",
        "CREATE (:Node {id: 'near'})",
        "CREATE (:Node {id: 'far'})",
        "CREATE (:Node {id: 'isolated'})",
        "MATCH (a:Node {id: 'source'}), (b:Node {id: 'near'}) CREATE (a)-[:LINK]->(b)",
        "MATCH (a:Node {id: 'near'}), (b:Node {id: 'far'}) CREATE (a)-[:LINK]->(b)",
    ] {
        db.query(query).unwrap();
    }
    let query = "MATCH (a:Node {id: $id}) WITH a WITH a OPTIONAL MATCH (a)-[:LINK*1..2]->(b:Node) RETURN b.id ORDER BY b.id";
    for (id, expected) in [
        (
            "source",
            vec![
                Value::String("far".to_string()),
                Value::String("near".to_string()),
            ],
        ),
        ("isolated", vec![Value::Null]),
        ("missing", vec![]),
    ] {
        let params = BTreeMap::from([("id".to_string(), Value::String(id.to_string()))]);
        assert_eq!(
            db.explain_query_with_params(query, &params)
                .unwrap()
                .plan_cache_lookup,
            PlanCacheLookup::Miss
        );
        assert_eq!(
            db.explain_query_with_params(query, &params)
                .unwrap()
                .plan_cache_lookup,
            PlanCacheLookup::Hit
        );
        let mut snapshot = db.begin_read_transaction().unwrap();
        let rows = snapshot
            .query_with_params_bounded(query, &params, Some(2))
            .unwrap()
            .rows;
        assert_eq!(
            rows,
            expected
                .into_iter()
                .map(|value| BTreeMap::from([("b.id".to_string(), value)]))
                .collect::<Vec<_>>(),
            "{id}"
        );
    }
    assert_eq!(db.plan_cache_stats().unwrap().entries, 3);
}

#[test]
fn default_pipeline_optional_null_source_does_not_traverse_real_node_zero() {
    let mut db = optional_endpoint_fixture();
    for with in ["", "WITH candidate ", "WITH candidate WITH candidate "] {
        let query = format!(
            "MATCH (seed:Item {{id: 'isolated'}}) \
 OPTIONAL MATCH (seed)-[:REL]->(middle) \
 MATCH (middle)-[:REL]->(candidate:Item) {with}RETURN candidate.id AS id"
        );
        let rows = db.query(&query).unwrap().rows;
        assert!(
            rows.is_empty(),
            "NULL source must not traverse real node0; WITH variant {with:?}: {rows:?}"
        );
    }
}

#[test]
fn default_pipeline_unknown_relationship_optional_preserves_null_row() {
    let mut db = optional_endpoint_fixture();
    for with in ["", "WITH candidate ", "WITH candidate WITH candidate "] {
        let query = format!(
            "MATCH (seed:Item {{id: 'isolated'}}) \
 OPTIONAL MATCH (seed)-[:ABSENT]->(candidate:Item) {with}RETURN candidate.id AS id"
        );
        assert_eq!(
            db.query(&query).unwrap().rows,
            vec![BTreeMap::from([("id".to_string(), Value::Null)])],
            "unknown type must NULL extend; WITH variant {with:?}"
        );
    }
}

#[test]
fn default_pipeline_null_transition_optional_chain_preserves_null_and_real_zero() {
    let mut db = optional_endpoint_fixture();
    for with in ["", "WITH candidate ", "WITH candidate WITH candidate "] {
        let query = format!("MATCH (seed:Item {{id: 'isolated'}}) OPTIONAL MATCH (seed)-[:REL]->(middle) OPTIONAL MATCH (middle)-[edge:REL]->(candidate:Item) {with}RETURN candidate.id AS id");
        assert_eq!(
            db.query(&query).unwrap().rows,
            vec![BTreeMap::from([("id".to_string(), Value::Null)])],
            "{with:?}"
        );
        let zero = format!("MATCH (seed:Item {{id: 'source'}}) MATCH (seed)-[:REL*0..1]->(candidate:Item) {with}RETURN candidate.id AS id ORDER BY id");
        assert_eq!(
            db.query(&zero)
                .unwrap()
                .rows
                .iter()
                .map(|r| r["id"].clone())
                .collect::<Vec<_>>(),
            vec![
                Value::String("bad".into()),
                Value::String("good".into()),
                Value::String("source".into())
            ]
        );
    }
}

#[test]
fn default_pipeline_null_transition_mixed_graph_match_and_expand() {
    let mut db = optional_endpoint_fixture();
    for query in [
        "MATCH (seed:Item {id: 'isolated'}) OPTIONAL MATCH (seed)-[:REL]->(middle) MATCH (middle)-[:REL]->(candidate:Item), (other:Item {id: 'good'}) RETURN candidate.id AS id",
        "MATCH (seed:Item {id: 'isolated'}) OPTIONAL MATCH (seed)-[:REL]->(middle:Item {keep: true}) MATCH (middle)-[:REL]->(candidate:Item) RETURN candidate.id AS id",
    ] {
        assert!(db.query(query).unwrap().rows.is_empty(), "{query}");
    }
}

fn optional_column_null_transition(indexed: bool) {
    let mut db = optional_endpoint_fixture();
    db.query("CREATE (:Item {id: 'null-key', match_key: null})")
        .unwrap();
    if indexed {
        db.query("CREATE INDEX ON :Item(id)").unwrap();
        db.query("CREATE INDEX ON :Item(match_key)").unwrap();
    }
    let missing = "MATCH (seed:Item {id: 'isolated'}) WITH 'missing' AS key OPTIONAL MATCH (middle:Item) WHERE middle.id = key MATCH (middle)-[:REL]->(candidate:Item) RETURN candidate.id AS id";
    assert!(
        db.query(missing).unwrap().rows.is_empty(),
        "indexed={indexed}"
    );
}

#[test]
fn default_pipeline_null_transition_column_lookup_fallback() {
    optional_column_null_transition(false);
}

#[test]
fn default_pipeline_null_transition_column_lookup_indexed() {
    optional_column_null_transition(true);
}

fn scalar_with_reintroduced_target(optional: bool) {
    let mut db = optional_endpoint_fixture();
    let optional = if optional { "OPTIONAL " } else { "" };
    let query = format!("MATCH (a:Item {{id: 'source'}})-[:REL]->(target:Item) WITH a.id AS key MATCH (a:Item) WHERE a.id = key {optional}MATCH (a)-[:REL]->(target:Item) RETURN target.id AS id ORDER BY id");
    assert_eq!(
        db.query(&query)
            .unwrap()
            .rows
            .iter()
            .map(|r| r["id"].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::String("bad".into()),
            Value::String("bad".into()),
            Value::String("good".into()),
            Value::String("good".into())
        ],
        "{optional:?}"
    );
    let query = "MATCH (a:Item {id: 'source'})-[:REL]->(target:Item) WITH a.id AS key MATCH (a:Item) WHERE a.id = key OPTIONAL MATCH (a)-[:ABSENT]->(target:Item) RETURN target.id AS id";
    assert_eq!(
        db.query(query).unwrap().rows,
        vec![BTreeMap::from([("id".to_string(), Value::Null)]); 2]
    );
}

#[test]
fn default_pipeline_null_transition_scalar_with_required_reintroduction() {
    scalar_with_reintroduced_target(false);
}

#[test]
fn default_pipeline_null_transition_scalar_with_optional_reintroduction() {
    scalar_with_reintroduced_target(true);
}

#[test]
fn default_pipeline_optional_review_global_count_forward_reintroduction() {
    let mut db = optional_endpoint_fixture();
    let query = "MATCH (a:Item {id: 'source'})-[:REL]->(target:Item) WITH a.id AS key MATCH (a:Item) WHERE a.id = key OPTIONAL MATCH (a)-[:REL]->(target:Item) RETURN COUNT(target) AS count";
    assert_eq!(
        db.query(query).unwrap().rows,
        vec![BTreeMap::from([("count".into(), Value::Int(4))])]
    );
}

#[test]
fn default_pipeline_optional_review_global_count_reverse_reintroduction() {
    let mut db = optional_endpoint_fixture();
    let query = "MATCH (target:Item)-[edge:REL]->(a:Item {id: 'bad'}) WITH a.id AS key MATCH (a:Item) WHERE a.id = key OPTIONAL MATCH (target:Item)-[edge:REL]->(a) RETURN COUNT(edge) AS count";
    assert_eq!(
        db.query(query).unwrap().rows,
        vec![BTreeMap::from([("count".into(), Value::Int(4))])]
    );
}

fn global_optional_bound_relationship_count(reverse: bool, missing: bool) {
    let mut db = optional_endpoint_fixture();
    let rel_type = if missing { "ABSENT" } else { "REL" };
    let query = if reverse {
        format!("MATCH (old:Item)-[edge:REL]->(a:Item {{id: 'bad'}}) OPTIONAL MATCH (target:Item)-[edge:{rel_type}]->(a) RETURN COUNT(edge) AS count")
    } else {
        let counted = if missing { "edge" } else { "target" };
        format!("MATCH (a:Item {{id: 'source'}})-[edge:REL]->(old:Item) OPTIONAL MATCH (a)-[edge:{rel_type}]->(target:Item) RETURN COUNT({counted}) AS count")
    };
    assert_eq!(
        db.query(&query).unwrap().rows,
        vec![BTreeMap::from([("count".into(), Value::Int(2))])],
        "reverse={reverse}, missing={missing}"
    );
}

#[test]
fn default_pipeline_optional_review_global_count_bound_relationship_forward() {
    global_optional_bound_relationship_count(false, false);
}

#[test]
fn default_pipeline_optional_review_global_count_bound_relationship_reverse() {
    global_optional_bound_relationship_count(true, false);
}

#[test]
fn default_pipeline_optional_review_global_count_missing_bound_relationship_forward() {
    global_optional_bound_relationship_count(false, true);
}

#[test]
fn default_pipeline_optional_review_global_count_missing_bound_relationship_reverse() {
    global_optional_bound_relationship_count(true, true);
}

#[test]
fn default_pipeline_optional_review_unknown_type_required_zero_hop() {
    let mut db = optional_endpoint_fixture();
    for with in ["", "WITH candidate ", "WITH candidate WITH candidate "] {
        let query = format!("MATCH (seed:Item {{id: 'isolated'}}) MATCH (seed)-[:ABSENT*0..1]->(candidate:Item) {with}RETURN candidate.id AS id");
        assert_eq!(
            db.query(&query).unwrap().rows,
            vec![BTreeMap::from([(
                "id".into(),
                Value::String("isolated".into())
            )])],
            "{with:?}"
        );
        let positive = format!("MATCH (seed:Item {{id: 'isolated'}}) MATCH (seed)-[:ABSENT]->(candidate:Item) {with}RETURN candidate.id AS id");
        assert!(db.query(&positive).unwrap().rows.is_empty(), "{with:?}");
    }
}

#[test]
fn default_pipeline_optional_review_unknown_type_optional_zero_hop() {
    let mut db = optional_endpoint_fixture();
    let query = "MATCH (seed:Item {id: 'isolated'}) OPTIONAL MATCH (seed)-[:ABSENT*0..1]->(candidate:Item) WITH candidate WITH candidate RETURN candidate.id AS id";
    assert_eq!(
        db.query(query).unwrap().rows,
        vec![BTreeMap::from([(
            "id".into(),
            Value::String("isolated".into())
        )])]
    );
}

fn bound_source_label_fixture() -> Database {
    let mut db = optional_endpoint_fixture();
    db.query("CREATE (:Other {id: 'other'})").unwrap();
    db
}

#[test]
fn default_pipeline_bound_source_label_required_preserves_constraint() {
    let mut db = bound_source_label_fixture();
    for label in ["Other", "Missing"] {
        for with in ["", "WITH candidate ", "WITH candidate WITH candidate "] {
            let query = format!("MATCH (seed:Item {{id: 'source'}}) MATCH (seed:{label})-[:REL]->(candidate:Item) {with}RETURN candidate.id AS id");
            assert!(
                db.query(&query).unwrap().rows.is_empty(),
                "label={label}, WITH={with:?}"
            );
        }
    }
}

#[test]
fn default_pipeline_bound_source_label_optional_preserves_null_row() {
    let mut db = bound_source_label_fixture();
    for label in ["Other", "Missing"] {
        for with in ["", "WITH candidate ", "WITH candidate WITH candidate "] {
            let query = format!("MATCH (seed:Item {{id: 'source'}}) OPTIONAL MATCH (seed:{label})-[:REL]->(candidate:Item) {with}RETURN candidate.id AS id");
            assert_eq!(
                db.query(&query).unwrap().rows,
                vec![BTreeMap::from([("id".into(), Value::Null)])],
                "label={label}, WITH={with:?}"
            );
        }
    }
}

#[test]
fn default_pipeline_bound_source_label_forward_global_count() {
    let mut db = bound_source_label_fixture();
    for label in ["Other", "Missing"] {
        for with in ["", "WITH candidate ", "WITH candidate WITH candidate "] {
            let query = format!("MATCH (seed:Item {{id: 'source'}}) OPTIONAL MATCH (seed:{label})-[:REL]->(candidate:Item) {with}RETURN COUNT(candidate) AS count");
            assert_eq!(
                db.query(&query).unwrap().rows,
                vec![BTreeMap::from([("count".into(), Value::Int(0))])],
                "label={label}, WITH={with:?}"
            );
        }
    }
}

#[test]
fn default_pipeline_bound_source_label_reverse_global_count() {
    let mut db = bound_source_label_fixture();
    for label in ["Other", "Missing"] {
        for with in ["", "WITH edge ", "WITH edge WITH edge "] {
            let query = format!("MATCH (seed:Item {{id: 'bad'}}) OPTIONAL MATCH (candidate:Item)-[edge:REL]->(seed:{label}) {with}RETURN COUNT(edge) AS count");
            assert_eq!(
                db.query(&query).unwrap().rows,
                vec![BTreeMap::from([("count".into(), Value::Int(0))])],
                "label={label}, WITH={with:?}"
            );
        }
    }
}

fn optional_null_key_lookup(indexed: bool) {
    let mut db = optional_endpoint_fixture();
    db.query("CREATE (:Item {id: 'null-key', match_key: null})")
        .unwrap();
    if indexed {
        db.query("CREATE INDEX ON :Item(match_key)").unwrap();
    }
    let query = "MATCH (seed:Item {id: 'isolated'}) OPTIONAL MATCH (seed)-[:REL]->(middle) WITH middle.id AS key OPTIONAL MATCH (candidate:Item) WHERE candidate.match_key = key RETURN candidate.id AS id";
    assert_eq!(
        db.query(query).unwrap().rows,
        vec![BTreeMap::from([("id".to_string(), Value::Null)])],
        "NULL lookup key must never equal a NULL property; indexed={indexed}"
    );
}

#[test]
fn default_pipeline_nullable_key_lookup_fallback_does_not_match_null_property() {
    optional_null_key_lookup(false);
}

#[test]
fn default_pipeline_nullable_key_lookup_indexed_does_not_match_null_property() {
    optional_null_key_lookup(true);
}
