// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn fixture(indexed: bool) -> Database {
    let mut db = Database::new();
    for query in [
        "CREATE (:Item {id: 'a', active: true})",
        "CREATE (:Item {id: 'b', active: true})",
        "CREATE (:Item {id: 'b', active: false})",
        "CREATE (:Item {id: '', active: true})",
        "CREATE (:Item {id: null})",
        "CREATE (:Item {active: true})",
    ] {
        db.query(query).unwrap();
    }
    if indexed {
        db.query("CREATE INDEX ON :Item(id)").unwrap();
    }
    db
}

fn bag(mut rows: Vec<BTreeMap<String, Value>>) -> Vec<BTreeMap<String, Value>> {
    rows.sort();
    rows
}

#[test]
fn row_bound_pattern_properties_preserve_multiplicity_nulls_and_parameter_changes() {
    for indexed in [false, true] {
        let db = fixture(indexed);
        let mut snapshot = db.begin_read_transaction().unwrap();
        for optional in [false, true] {
            for with in [
                "",
                "WITH candidate_id",
                "WITH candidate_id WITH candidate_id",
            ] {
                let prefix = if optional { "OPTIONAL MATCH" } else { "MATCH" };
                let query = format!("UNWIND $ids AS candidate_id {with} {prefix} (n:Item {{id: candidate_id}}) RETURN candidate_id, n.id");
                // Repeated same-shaped lists must read the current parameter values.
                for ids in [
                    vec![
                        Value::String("a".into()),
                        Value::String("b".into()),
                        Value::String("b".into()),
                        Value::String("missing".into()),
                        Value::Null,
                        Value::String("".into()),
                    ],
                    vec![
                        Value::String("b".into()),
                        Value::String("a".into()),
                        Value::String("missing".into()),
                        Value::String("a".into()),
                        Value::Null,
                        Value::String("".into()),
                    ],
                    vec![],
                    vec![Value::Null],
                ] {
                    let mut expected = Vec::new();
                    for id in &ids {
                        let count = match id {
                            Value::String(id) if id == "b" => 2,
                            Value::String(id) if id == "a" || id.is_empty() => 1,
                            _ => 0,
                        };
                        for _ in 0..count.max(usize::from(optional)) {
                            expected.push(BTreeMap::from([
                                ("candidate_id".into(), id.clone()),
                                (
                                    "n.id".into(),
                                    if count == 0 { Value::Null } else { id.clone() },
                                ),
                            ]));
                        }
                    }
                    let parameters = BTreeMap::from([("ids".into(), Value::List(ids))]);
                    let actual = snapshot
                        .query_with_params_bounded(&query, &parameters, Some(32))
                        .unwrap();
                    assert_eq!(
                        bag(actual.rows.into_rows()),
                        bag(expected),
                        "indexed={indexed}: {query}"
                    );
                }
            }
        }
    }
}

#[test]
fn row_bound_pattern_properties_keep_optional_filters_inside_match() {
    let mut db = fixture(false);
    for pattern in [
        "id: candidate_id, active: true",
        "active: true, id: candidate_id",
    ] {
        let query = format!("UNWIND ['b', 'a', 'missing', null] AS candidate_id OPTIONAL MATCH (n:Item {{{pattern}}}) WHERE n.id <> 'a' RETURN candidate_id, n.id");
        assert_eq!(
            bag(db.query(&query).unwrap().rows.into_rows()),
            bag(vec![
                BTreeMap::from([
                    ("candidate_id".into(), Value::String("b".into())),
                    ("n.id".into(), Value::String("b".into()))
                ]),
                BTreeMap::from([
                    ("candidate_id".into(), Value::String("a".into())),
                    ("n.id".into(), Value::Null)
                ]),
                BTreeMap::from([
                    ("candidate_id".into(), Value::String("missing".into())),
                    ("n.id".into(), Value::Null)
                ]),
                BTreeMap::from([
                    ("candidate_id".into(), Value::Null),
                    ("n.id".into(), Value::Null)
                ]),
            ])
        );
    }
}

#[test]
fn row_bound_pattern_properties_support_with_aliases_properties_and_constants() {
    let mut db = fixture(true);
    for prefix in ["MATCH", "OPTIONAL MATCH"] {
        for source in ["source.id", "candidate_id"] {
            let with = if source == "candidate_id" {
                "WITH source.id AS candidate_id"
            } else {
                "WITH source"
            };
            let query = format!("MATCH (source:Item {{id: 'a'}}) {with} {prefix} (n:Item {{id: {source}}}) RETURN n.id AS id");
            assert_eq!(
                db.query(&query).unwrap().rows,
                vec![BTreeMap::from([("id".into(), Value::String("a".into()))])]
            );
        }
        for value in ["$id", "'a'"] {
            let query = format!(
                "UNWIND [1, 1] AS duplicate {prefix} (n:Item {{id: {value}}}) RETURN n.id AS id"
            );
            let parameters = BTreeMap::from([("id".into(), Value::String("a".into()))]);
            assert_eq!(
                db.query_with_params(&query, &parameters).unwrap().rows,
                vec![BTreeMap::from([("id".into(), Value::String("a".into()))]); 2]
            );
        }
    }
    let parameters = BTreeMap::from([(
        "rows".into(),
        Value::List(vec![Value::Map(BTreeMap::from([(
            "id".into(),
            Value::String("a".into()),
        )]))]),
    )]);
    assert_eq!(
        db.query_with_params(
            "UNWIND $rows AS candidate MATCH (n:Item {id: candidate.id}) RETURN n.id AS id",
            &parameters
        )
        .unwrap()
        .rows,
        vec![BTreeMap::from([("id".into(), Value::String("a".into()))])]
    );
}

#[test]
fn row_bound_pattern_properties_validate_scope_even_with_empty_input() {
    let mut db = fixture(false);
    for (query, missing) in [
        ("UNWIND [] AS candidate_id MATCH (n:Item {id: unknown}) RETURN n", "unknown"),
        ("UNWIND [] AS candidate_id WITH 1 AS kept OPTIONAL MATCH (n:Item {id: candidate_id}) RETURN n", "candidate_id"),
        ("UNWIND [] AS candidate_id OPTIONAL MATCH (n:Item {id: unknown.id}) RETURN n", "unknown"),
    ] {
        assert!(matches!(db.query(query), Err(HawDBError::Semantic(message)) if message.contains("unknown variable") && message.contains(missing)), "{query}");
    }
    let parameters = BTreeMap::from([("ids".into(), Value::Null)]);
    assert!(db
        .query_with_params(
            "UNWIND $ids AS candidate_id OPTIONAL MATCH (n:Item {id: candidate_id}) RETURN n",
            &parameters
        )
        .unwrap()
        .rows
        .is_empty());
    let parameters = BTreeMap::from([("ids".into(), Value::Int(1))]);
    assert!(matches!(
        db.query_with_params(
            "UNWIND $ids AS candidate_id RETURN candidate_id",
            &parameters
        ),
        Err(HawDBError::Semantic(_))
    ));
    assert!(matches!(
        db.query("UNWIND [1] AS candidate_id CREATE (:Item {id: candidate_id})"),
        Err(HawDBError::Semantic(_))
    ));
}

#[test]
fn unwind_read_source_enforces_payload_and_result_budgets() {
    let mut db = Database::new_with_config(DatabaseConfig {
        execution_memory: crate::executor::ExecutionMemoryConfig {
            batch_rows: NonZeroUsize::new(2).unwrap(),
            batch_payload_bytes: NonZeroUsize::new(1024).unwrap(),
            ..crate::executor::ExecutionMemoryConfig::default()
        },
        ..DatabaseConfig::default()
    });
    let parameters = BTreeMap::from([(
        "ids".into(),
        Value::List(vec![Value::String("x".repeat(8192))]),
    )]);
    let error = db
        .query_with_params(
            "UNWIND $ids AS candidate_id RETURN candidate_id",
            &parameters,
        )
        .unwrap_err();
    assert!(
        matches!(error, HawDBError::Execution(ref message) if message.contains("batch_payload_bytes")),
        "{error}"
    );
    let mut snapshot = db.begin_read_transaction().unwrap();
    assert!(snapshot
        .query_with_params_bounded(
            "UNWIND [1, 2, 3] AS id RETURN id",
            &BTreeMap::new(),
            Some(2)
        )
        .is_err());
}

#[test]
fn row_bound_pattern_properties_constrain_expansion_targets() {
    let mut db = fixture(false);
    db.query("MATCH (a:Item {id: 'a'}), (b:Item {id: 'b'}) CREATE (a)-[:LINK]->(b)")
        .unwrap();
    let rows = db.query("UNWIND ['b', 'missing', null] AS candidate_id WITH candidate_id MATCH (a:Item {id: 'a'}) OPTIONAL MATCH (a)-[:LINK]->(n:Item {id: candidate_id, active: true}) RETURN candidate_id, n.id").unwrap().rows;
    assert_eq!(
        bag(rows.into_rows()),
        bag(vec![
            BTreeMap::from([
                ("candidate_id".into(), Value::String("b".into())),
                ("n.id".into(), Value::String("b".into()))
            ]),
            BTreeMap::from([
                ("candidate_id".into(), Value::String("missing".into())),
                ("n.id".into(), Value::Null)
            ]),
            BTreeMap::from([
                ("candidate_id".into(), Value::Null),
                ("n.id".into(), Value::Null)
            ]),
        ])
    );
}
