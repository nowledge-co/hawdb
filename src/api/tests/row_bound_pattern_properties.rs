// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn mem_thread_reference_scalar_predicate() {
    let mut db = Database::new();
    for query in [
        "CREATE (:Thread {id: 'legacy'})",
        "CREATE (:Thread {id: 'null-space', space_id: null})",
        "CREATE (:Thread {id: 'default-space', space_id: 'default'})",
        "CREATE (:Thread {id: 'other-space', space_id: 'other'})",
    ] {
        db.query(query).unwrap();
    }
    for id in ["legacy", "null-space", "default-space", "other-space"] {
        let parameters = BTreeMap::from([
            ("storage_id".into(), Value::String(id.into())),
            ("space_id".into(), Value::String("default".into())),
        ]);
        let output = db.query_with_params(
            "MATCH (t:Thread {id: $storage_id}) WHERE true AND COALESCE(t.space_id, 'default') = $space_id RETURN t.id LIMIT 1",
            &parameters,
        ).unwrap();
        let expected = if id == "other-space" {
            Vec::new()
        } else {
            vec![BTreeMap::from([("t.id".into(), Value::String(id.into()))])]
        };
        assert_eq!(output.rows, expected, "{id}");
    }
}

#[test]
fn mem_thread_reference_complete_read_pipeline() {
    let mut db = Database::new();
    for query in [
        "CREATE (:Thread {id: 'stored-a', thread_id: 'public-a'})",
        "CREATE (:Thread {id: 'stored-b', thread_id: 'ambiguous'})",
        "CREATE (:Thread {id: 'stored-c', thread_id: 'ambiguous'})",
        "CREATE (:Thread {id: 'same', thread_id: 'same'})",
        "CREATE (:ThreadIdentity {id: 'public-a', thread_node_id: 'stored-a'})",
    ] {
        db.query(query).unwrap();
    }
    let parameters = BTreeMap::from([(
        "references".into(),
        Value::List(
            ["stored-a", "public-a", "ambiguous", "missing", "same"]
                .into_iter()
                .map(|value| Value::String(value.into()))
                .collect(),
        ),
    )]);
    let output = db
        .query_with_params(
            "MATCH (t:Thread) WHERE t.thread_id IN $references OR t.id IN $references
         UNWIND [t.thread_id, t.id] AS reference
         WITH reference, t WHERE reference IN $references
         WITH reference, count(DISTINCT t.id) AS candidates
         WHERE candidates = 1
         MATCH (chosen:Thread) WHERE chosen.thread_id = reference OR chosen.id = reference
         OPTIONAL MATCH (ti:ThreadIdentity {id: reference})
         RETURN reference, candidates, chosen.id, ti.thread_node_id, ti.id, chosen.thread_id",
            &parameters,
        )
        .unwrap();
    let expected = [
        ("stored-a", "stored-a", "public-a"),
        ("public-a", "stored-a", "public-a"),
        ("same", "same", "same"),
    ]
    .into_iter()
    .map(|(reference, stored, public)| {
        BTreeMap::from([
            ("reference".into(), Value::String(reference.into())),
            ("candidates".into(), Value::Int(1)),
            ("chosen.id".into(), Value::String(stored.into())),
            ("chosen.thread_id".into(), Value::String(public.into())),
            (
                "ti.id".into(),
                if reference == "public-a" {
                    Value::String(reference.into())
                } else {
                    Value::Null
                },
            ),
            (
                "ti.thread_node_id".into(),
                if reference == "public-a" {
                    Value::String(stored.into())
                } else {
                    Value::Null
                },
            ),
        ])
    })
    .collect();
    assert_eq!(bag(output.rows.into_rows()), bag(expected));
}

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
fn read_unwind_rows_preserves_scope_multiplicity_and_null_lists() {
    let mut db = Database::new();
    for query in [
        "CREATE (:Row {id: 'a', xs: [1, 1, null]})",
        "CREATE (:Row {id: 'b', xs: []})",
        "CREATE (:Row {id: 'c', xs: null})",
        "CREATE (:Row {id: 'd', xs: [2]})",
        "CREATE (:Row {id: 'e'})",
    ] {
        db.query(query).unwrap();
    }
    let expected = vec![
        ("a", Value::Int(1)),
        ("a", Value::Int(1)),
        ("a", Value::Null),
        ("d", Value::Int(2)),
    ]
    .into_iter()
    .map(|(id, value)| {
        BTreeMap::from([
            ("id".into(), Value::String(id.into())),
            ("item".into(), value),
        ])
    })
    .collect::<Vec<_>>();
    for query in [
        "MATCH (r:Row) UNWIND r.xs AS item RETURN r.id AS id, item",
        "MATCH (r:Row) WITH r.id AS id, r.xs AS xs UNWIND xs AS item RETURN id, item",
    ] {
        assert_eq!(
            bag(db.query(query).unwrap().rows.into_rows()),
            bag(expected.clone()),
            "{query}"
        );
    }
    let rows = db
        .query("MATCH (r:Row) WITH r.id AS id UNWIND [id] AS r RETURN r")
        .unwrap()
        .rows;
    let expected = ["a", "b", "c", "d", "e"]
        .into_iter()
        .map(|id| BTreeMap::from([("r".into(), Value::String(id.into()))]))
        .collect();
    assert_eq!(bag(rows.into_rows()), bag(expected));
    let rows = db.query("UNWIND [[1, 1, null], [], null, [2]] AS xs UNWIND xs AS item UNWIND [item, item] AS duplicate RETURN duplicate").unwrap().rows;
    let expected = [
        Value::Int(1),
        Value::Int(1),
        Value::Int(1),
        Value::Int(1),
        Value::Null,
        Value::Null,
        Value::Int(2),
        Value::Int(2),
    ]
    .into_iter()
    .map(|value| BTreeMap::from([("duplicate".into(), value)]))
    .collect();
    assert_eq!(bag(rows.into_rows()), bag(expected));
}

#[test]
fn read_unwind_rows_validates_scope_before_empty_inputs_and_rejects_non_lists() {
    let mut db = Database::new();
    db.query("CREATE (:Row {id: 'a'})").unwrap();
    for (query, message) in [
        (
            "MATCH (r:Row {id: 'missing'}) UNWIND [unknown] AS item RETURN item",
            "unknown variable",
        ),
        (
            "MATCH (r:Row {id: 'missing'}) UNWIND unknown.xs AS item RETURN item",
            "unknown variable",
        ),
        ("MATCH (r:Row) UNWIND [r.id] AS r RETURN r", "already bound"),
        (
            "MATCH (r:Row) WITH r.id AS id UNWIND [id] AS id RETURN id",
            "already bound",
        ),
        (
            "UNWIND [] AS id WITH 1 AS kept UNWIND [id] AS item RETURN item",
            "unknown variable",
        ),
    ] {
        let error = db.query(query).unwrap_err();
        assert!(
            matches!(error, HawDBError::Semantic(ref reason) if reason.contains(message)),
            "{query}: {error}"
        );
    }
    let error = db
        .query("MATCH (r:Row) UNWIND r.id AS item RETURN item")
        .unwrap_err();
    assert!(
        matches!(error, HawDBError::Execution(ref message) if message.contains("UNWIND source")),
        "{error}"
    );
}

#[test]
fn read_unwind_rows_rebinds_parameters_and_preserves_result_budgets() {
    let mut db = Database::new();
    db.query("CREATE (:Row {id: 'a'})").unwrap();
    db.query("CREATE (:Row {id: 'b'})").unwrap();
    let query = "MATCH (r:Row) WHERE r.id IN $ids UNWIND $elements AS item RETURN r.id AS id, item";
    for (id, elements) in [
        (
            "a",
            Value::List(vec![Value::Int(1), Value::Int(1), Value::Null]),
        ),
        ("b", Value::List(vec![Value::Int(2)])),
        ("a", Value::List(vec![])),
        ("b", Value::Null),
        ("b", Value::List(vec![Value::Int(3)])),
    ] {
        let expected = match &elements {
            Value::List(values) => values
                .iter()
                .map(|value| {
                    BTreeMap::from([
                        ("id".into(), Value::String(id.into())),
                        ("item".into(), value.clone()),
                    ])
                })
                .collect(),
            _ => Vec::new(),
        };
        let parameters = BTreeMap::from([
            ("ids".into(), Value::List(vec![Value::String(id.into())])),
            ("elements".into(), elements),
        ]);
        assert_eq!(
            bag(db
                .query_with_params(query, &parameters)
                .unwrap()
                .rows
                .into_rows()),
            bag(expected)
        );
    }
    let mut snapshot = db.begin_read_transaction().unwrap();
    assert!(snapshot
        .query_with_params_bounded(
            "MATCH (r:Row) UNWIND [1, 2] AS item RETURN item",
            &BTreeMap::new(),
            Some(3),
        )
        .is_err());
}

#[test]
fn read_unwind_rows_admits_output_and_stops_before_unused_source_errors() {
    let mut db = Database::new_with_config(DatabaseConfig {
        execution_memory: crate::executor::ExecutionMemoryConfig {
            batch_rows: NonZeroUsize::new(2).unwrap(),
            batch_payload_bytes: NonZeroUsize::new(1024).unwrap(),
            ..crate::executor::ExecutionMemoryConfig::default()
        },
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Row {id: 'a', xs: [1]})").unwrap();
    let parameters = BTreeMap::from([(
        "elements".into(),
        Value::List(vec![Value::String("x".repeat(8192))]),
    )]);
    let error = db
        .query_with_params(
            "MATCH (r:Row {id: 'a'}) UNWIND $elements AS item RETURN item",
            &parameters,
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("intermediate row uses")
            && error.to_string().contains("batch_payload_bytes"),
        "{error}"
    );
    assert!(db
        .query_with_params(
            "MATCH (r:Row {id: 'a'}) UNWIND $elements AS item RETURN item LIMIT 0",
            &parameters,
        )
        .unwrap()
        .rows
        .is_empty());
    let sources = BTreeMap::from([(
        "sources".into(),
        Value::List(vec![
            Value::List(vec![Value::Int(1)]),
            Value::String("not-a-list".into()),
        ]),
    )]);
    assert_eq!(
        db.query_with_params(
            "UNWIND $sources AS source UNWIND source AS item RETURN item LIMIT 1",
            &sources,
        )
        .unwrap()
        .rows,
        vec![BTreeMap::from([("item".into(), Value::Int(1))])]
    );
}

#[test]
fn boolean_predicate_literals_preserve_three_valued_composition() {
    let mut db = Database::new();
    db.query("CREATE (:Item {id: 'a'})").unwrap();
    for (predicate, expected) in [
        ("true", 1),
        ("false", 0),
        ("null", 0),
        ("NOT false", 1),
        ("NOT null", 0),
        ("(true) AND true", 1),
        ("true AND false OR true", 1),
        ("true = false", 0),
        ("false OR null", 0),
        ("COALESCE(n.missing, 'a') = 'a'", 1),
        ("true AND COALESCE(n.missing, 'a') = 'a'", 1),
    ] {
        assert_eq!(
            db.query(&format!("MATCH (n:Item) WHERE {predicate} RETURN n.id"))
                .unwrap()
                .rows
                .len(),
            expected,
            "{predicate}"
        );
    }
    for predicate in ["42", "'not-bool'"] {
        assert!(db
            .query(&format!("MATCH (n:Item) WHERE {predicate} RETURN n"))
            .is_err());
    }
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

#[test]
fn read_unwind_rows_restores_native_metadata_after_with_aliases() {
    let mut db = Database::new();
    db.query("CREATE (:Row {id: 'a'})").unwrap();
    let report = db
        .explain_query("MATCH (r:Row) UNWIND [r.id] AS item RETURN item")
        .unwrap();
    assert!(report.physical_plan.explain(0).contains("UnwindRowsExec"));
    let expected = db.query("MATCH (r:Row) RETURN id(r) AS item").unwrap().rows;
    for query in [
        "MATCH (r:Row) WITH r AS alias UNWIND [alias] AS entity RETURN entity._id AS item",
        "MATCH (r:Row) WITH id(r) AS native_id UNWIND [native_id] AS item RETURN item",
    ] {
        assert_eq!(db.query(query).unwrap().rows, expected, "{query}");
    }
}
