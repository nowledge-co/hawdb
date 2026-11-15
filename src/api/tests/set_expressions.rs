// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! General expressions on the right-hand side of `MATCH ... SET` (#1791),
//! with Kùzu semantics. Expected values were checked against Ladybug.

use super::*;
use crate::{ConcurrentDatabase, ConcurrentTransactionOptions};

fn params(entries: &[(&str, Value)]) -> BTreeMap<String, Value> {
    entries
        .iter()
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect()
}

fn string(value: &str) -> Value {
    Value::String(value.into())
}

fn column(rows: &[BTreeMap<String, Value>], name: &str) -> Vec<Value> {
    rows.iter()
        .map(|row| row.get(name).cloned().unwrap_or(Value::Null))
        .collect()
}

/// Runs `statement` on every write surface: `Database` autocommit and
/// transaction, `ConcurrentDatabase` autocommit and both transaction modes.
/// Each surface starts from `setup` and must leave the same entity rows.
fn on_every_surface(
    setup: &[&str],
    statement: &str,
    parameters: &BTreeMap<String, Value>,
    read: &str,
    check: impl Fn(&str, Vec<BTreeMap<String, Value>>, Vec<BTreeMap<String, Value>>),
) {
    let fixture = || {
        let mut db = Database::new();
        for query in setup {
            db.query(query).unwrap();
        }
        db
    };
    let rows = |output: QueryOutput| output.rows.iter().map(|row| row.to_owned_row()).collect();

    let mut db = fixture();
    let output = db.query_with_params(statement, parameters).unwrap();
    check("autocommit", rows(output), rows(db.query(read).unwrap()));

    let mut db = fixture();
    let mut transaction = db.begin_transaction().unwrap();
    let output = transaction
        .query_with_params(statement, parameters)
        .unwrap();
    transaction.commit().unwrap();
    check("transaction", rows(output), rows(db.query(read).unwrap()));

    let db = ConcurrentDatabase::new(fixture());
    let output = db.query_with_params(statement, parameters).unwrap();
    check(
        "concurrent autocommit",
        rows(output),
        rows(db.query(read).unwrap()),
    );

    for options in [
        ConcurrentTransactionOptions::optimistic(),
        ConcurrentTransactionOptions::default(),
    ] {
        let db = ConcurrentDatabase::new(fixture());
        let mut transaction = db.begin_transaction(options).unwrap();
        let output = transaction
            .query_with_params(statement, parameters)
            .unwrap();
        transaction.commit().unwrap();
        check(
            "concurrent transaction",
            rows(output),
            rows(db.query(read).unwrap()),
        );
    }
}

const ENTITIES: &[&str] = &[
    "CREATE (:E {id: 'a', score: 2.0, count: 3, name: 'x', rank: 5, kind: ''})",
    "CREATE (:E {id: 'b'})",
    "CREATE (:E {id: 'c', score: 4.0, count: 7, name: 'z', rank: 1, kind: 'keep'})",
];
const READ_ENTITIES: &str = "MATCH (e:E) RETURN e.id AS id, e.score AS score, \
    e.name AS name, e.flag AS flag, e.kind AS kind, e.count AS count ORDER BY id";

/// The upsert shape: COALESCE keeps, CASE fills empty values from parameters.
#[test]
fn case_and_coalesce_fill_only_missing_values() {
    let statement = "MATCH (e:E {id: $id}) SET e.name = $name, \
        e.created = coalesce(e.created, $created), \
        e.kind = CASE WHEN e.kind IS NULL OR e.kind = '' THEN $kind ELSE e.kind END, \
        e.updated = $updated";
    for (id, kind) in [("a", "new"), ("b", "new"), ("c", "keep")] {
        on_every_surface(
            ENTITIES,
            statement,
            &params(&[
                ("id", string(id)),
                ("name", string("renamed")),
                ("created", Value::Int(10)),
                ("kind", string("new")),
                ("updated", Value::Int(20)),
            ]),
            &format!(
                "MATCH (e:E {{id: '{id}'}}) RETURN e.kind AS kind, e.created AS created, \
                 e.updated AS updated, e.name AS name"
            ),
            |surface, _, rows| {
                assert_eq!(column(&rows, "kind"), [string(kind)], "{surface} {id}");
                assert_eq!(column(&rows, "created"), [Value::Int(10)], "{surface}");
                assert_eq!(column(&rows, "updated"), [Value::Int(20)], "{surface}");
                assert_eq!(column(&rows, "name"), [string("renamed")], "{surface}");
            },
        );
    }
}

#[test]
fn float_arithmetic_promotes_integers_and_propagates_null() {
    on_every_surface(
        ENTITIES,
        "MATCH (e:E) SET e.score = e.score * 0.5 + $d RETURN e.id AS id, e.score AS score",
        &params(&[("d", Value::Int(1))]),
        READ_ENTITIES,
        |surface, returned, stored| {
            let expected = [Value::Float(2.0), Value::Null, Value::Float(3.0)];
            let mut returned = returned;
            returned.sort_by(|a, b| a.get("id").cmp(&b.get("id")));
            assert_eq!(column(&returned, "score"), expected, "{surface}");
            assert_eq!(column(&stored, "score"), expected, "{surface}");
        },
    );
    // An integer property times a float is a float; integer division stays integral.
    on_every_surface(
        ENTITIES,
        "MATCH (e:E) SET e.score = e.count * 0.5 + 1, e.count = e.count / 2",
        &BTreeMap::new(),
        READ_ENTITIES,
        |surface, _, stored| {
            assert_eq!(
                column(&stored, "score"),
                [Value::Float(2.5), Value::Null, Value::Float(4.5)],
                "{surface}"
            );
            assert_eq!(
                column(&stored, "count"),
                [Value::Int(1), Value::Null, Value::Int(3)],
                "{surface}"
            );
        },
    );
}

/// `+` concatenates strings; like Kùzu, a NULL operand reads as empty.
#[test]
fn string_concatenation_treats_a_null_operand_as_empty() {
    on_every_surface(
        ENTITIES,
        "MATCH (e:E) SET e.name = e.name + ' ' + $s",
        &params(&[("s", string("y"))]),
        READ_ENTITIES,
        |surface, _, stored| {
            assert_eq!(
                column(&stored, "name"),
                [string("x y"), string(" y"), string("z y")],
                "{surface}"
            );
        },
    );
    // The fixed `n.p + <value>` form concatenates too instead of refusing.
    let mut db = Database::new();
    db.query("CREATE (:E {id: 'a', title: 'x'})").unwrap();
    db.query("MATCH (e:E) SET e.title = e.title + 'x'").unwrap();
    let output = db.query("MATCH (e:E) RETURN e.title AS title").unwrap();
    assert_eq!(output.rows[0].get("title"), Some(&string("xx")));
    // A string in RETURN concatenates the same way.
    let output = db
        .query("MATCH (e:E) RETURN e.title + '!' AS loud, e.missing + '!' AS bare")
        .unwrap();
    assert_eq!(output.rows[0].get("loud"), Some(&string("xx!")));
    assert_eq!(output.rows[0].get("bare"), Some(&string("!")));
}

#[test]
fn comparisons_store_booleans_and_null() {
    on_every_surface(
        ENTITIES,
        "MATCH (e:E) SET e.flag = e.rank > $b",
        &params(&[("b", Value::Int(2))]),
        READ_ENTITIES,
        |surface, _, stored| {
            assert_eq!(
                column(&stored, "flag"),
                [Value::Bool(true), Value::Null, Value::Bool(false)],
                "{surface}"
            );
        },
    );
}

/// Items apply in statement order: a later item reads an earlier item's
/// result, and an item on the same property sees the previous one.
#[test]
fn later_items_read_the_values_earlier_items_wrote() {
    on_every_surface(
        ENTITIES,
        "MATCH (e:E {id: 'a'}) SET e.rank = e.rank + 1, e.copy = e.rank, \
         e.count = e.count + 1, e.count = e.count * 10 RETURN e.copy AS copy, e.count AS count",
        &BTreeMap::new(),
        "MATCH (e:E {id: 'a'}) RETURN e.rank AS rank, e.copy AS copy, e.count AS count",
        |surface, returned, stored| {
            assert_eq!(column(&returned, "copy"), [Value::Int(6)], "{surface}");
            assert_eq!(column(&returned, "count"), [Value::Int(40)], "{surface}");
            assert_eq!(column(&stored, "rank"), [Value::Int(6)], "{surface}");
            assert_eq!(column(&stored, "copy"), [Value::Int(6)], "{surface}");
        },
    );
}

/// Like Kùzu, rows of the same node read its value from before the item:
/// two rows per node still add one.
#[test]
fn rows_of_the_same_entity_read_the_value_before_the_item() {
    on_every_surface(
        &[
            "CREATE (:E {id: 'a', n: 100})",
            "CREATE (:E {id: 'b', n: 100})",
        ],
        "MATCH (a:E), (b:E) SET a.n = a.n + $one RETURN a.id AS a, a.n AS n, b.id AS b",
        &params(&[("one", Value::Float(1.0))]),
        "MATCH (e:E) RETURN e.id AS id, e.n AS n ORDER BY id",
        |surface, returned, stored| {
            assert_eq!(returned.len(), 4, "{surface}");
            assert!(
                returned
                    .iter()
                    .all(|row| row.get("n") == Some(&Value::Float(101.0))),
                "{surface}: {returned:?}"
            );
            assert_eq!(
                column(&stored, "n"),
                [Value::Float(101.0), Value::Float(101.0)],
                "{surface}"
            );
        },
    );
}

/// A relationship SET reads its endpoints and filters by an OR across them.
#[test]
fn relationship_set_reads_endpoint_properties() {
    on_every_surface(
        &[
            "CREATE (:E {id: 'a', space: 'p'})",
            "CREATE (:E {id: 'b', space: 'q', weight: 10.0})",
            "CREATE (:E {id: 'c', space: 'old'})",
            "MATCH (a:E {id: 'a'}), (b:E {id: 'b'}) CREATE (a)-[:R {space: 'old', w: 1.0}]->(b)",
            "MATCH (a:E {id: 'c'}), (b:E {id: 'b'}) CREATE (a)-[:R {space: 'old', w: 3.0}]->(b)",
            "MATCH (a:E {id: 'c'}), (b:E {id: 'a'}) CREATE (a)-[:R {space: 'keep', w: 5.0}]->(b)",
        ],
        "MATCH (a:E)-[r:R]->(b:E) WHERE (a.id IN $ids OR b.id IN $ids) AND r.space = $from \
         SET r.space = CASE WHEN a.space <> $from THEN a.space ELSE b.space END, \
         r.w = r.w * 0.5, r.total = r.w + b.weight",
        &params(&[
            ("ids", Value::List(vec![string("a"), string("c")])),
            ("from", string("old")),
        ]),
        "MATCH (a:E)-[r:R]->(b:E) RETURN a.id AS a, b.id AS b, r.space AS space, r.w AS w, \
         r.total AS total ORDER BY a, b",
        |surface, _, stored| {
            assert_eq!(
                column(&stored, "space"),
                [string("p"), string("keep"), string("q")],
                "{surface}"
            );
            assert_eq!(
                column(&stored, "w"),
                [Value::Float(0.5), Value::Float(5.0), Value::Float(1.5)],
                "{surface}"
            );
            // A later item reads the halved weight and the target's property.
            assert_eq!(
                column(&stored, "total"),
                [Value::Float(10.5), Value::Null, Value::Float(11.5)],
                "{surface}"
            );
        },
    );
}

#[test]
fn unmatched_rows_and_optional_nulls_write_nothing() {
    let mut db = Database::new();
    for query in ENTITIES {
        db.query(query).unwrap();
    }
    let epoch = db.commit_epoch().unwrap();
    let output = db
        .query("MATCH (e:E {id: 'missing'}) SET e.name = e.name + 'x' RETURN e.name AS name")
        .unwrap();
    assert!(output.rows.is_empty());
    let output = db
        .query("MATCH (e:E {id: 'missing'}) SET e.name = e.name + 'x'")
        .unwrap();
    assert!(output.rows.is_empty());
    assert_eq!(db.commit_epoch().unwrap(), epoch);
}

/// A type error in any row refuses the statement before anything is written.
#[test]
fn type_mismatch_refuses_the_whole_statement() {
    for statement in [
        "MATCH (e:E) SET e.count = e.count + 1, e.label = e.name + e.count",
        "MATCH (e:E) SET e.flag = CASE WHEN e.name THEN 1 ELSE 0 END",
        "MATCH (e:E) SET e.count = e.count / 0",
    ] {
        let mut db = Database::new();
        for query in ENTITIES {
            db.query(query).unwrap();
        }
        let before = db.query(READ_ENTITIES).unwrap().rows;
        assert!(db.query(statement).is_err(), "{statement}");
        assert_eq!(db.query(READ_ENTITIES).unwrap().rows, before, "{statement}");

        let mut transaction = db.begin_transaction().unwrap();
        transaction
            .query("MATCH (e:E {id: 'a'}) SET e.name = 'kept'")
            .unwrap();
        assert!(transaction.query(statement).is_err(), "{statement}");
        transaction.commit().unwrap();
        let rows = db
            .query("MATCH (e:E) RETURN e.count AS count, e.name AS name ORDER BY e.id")
            .unwrap()
            .rows;
        // The failed statement rolled back to its savepoint only.
        assert_eq!(rows[0].get("name"), Some(&string("kept")), "{statement}");
        assert_eq!(rows[0].get("count"), Some(&Value::Int(3)), "{statement}");
    }
}

#[test]
fn row_budget_and_schema_constraints_apply_to_computed_values() {
    let config = DatabaseConfig {
        mutation_limits: crate::store::MutationLimits {
            max_affected_rows: NonZeroUsize::new(2).unwrap(),
            ..Default::default()
        },
        ..Default::default()
    };
    let mut db = Database::new_with_config(config);
    for query in ENTITIES {
        db.query(query).unwrap();
    }
    let error = db
        .query("MATCH (e:E) SET e.name = e.name + 'x'")
        .unwrap_err();
    assert!(
        error.to_string().contains("max_mutation_affected_rows 2"),
        "{error}"
    );

    let mut db = Database::new();
    for query in ENTITIES {
        db.query(query).unwrap();
    }
    db.query("CREATE PROPERTY ON NODE TABLE E(rank) TYPE INT")
        .unwrap();
    db.query("CREATE CONSTRAINT ON :E(id) ASSERT UNIQUE")
        .unwrap();
    let before = db.query(READ_ENTITIES).unwrap().rows;
    // A float into an INT property is refused, not cast.
    assert!(db
        .query("MATCH (e:E {id: 'a'}) SET e.rank = e.rank * 0.5")
        .is_err());
    // A computed duplicate of a unique key is refused.
    assert!(db
        .query("MATCH (e:E {id: 'b'}) SET e.id = CASE WHEN e.id = 'b' THEN 'a' ELSE e.id END")
        .is_err());
    assert_eq!(db.query(READ_ENTITIES).unwrap().rows, before);
    db.query("MATCH (e:E {id: 'a'}) SET e.rank = e.rank * 2")
        .unwrap();
    let output = db
        .query("MATCH (e:E {id: 'a'}) RETURN e.rank AS rank")
        .unwrap();
    assert_eq!(output.rows[0].get("rank"), Some(&Value::Int(10)));
}

#[test]
fn computed_updates_survive_reopen_and_reach_the_search_projection() {
    let root = unique_test_dir("set_expressions_reopen");
    {
        let mut db = Database::open(&root).unwrap();
        db.query("CREATE (:Memory {id: 'm1', title: 'Old title', content: 'Old body'})")
            .unwrap();
        let request = db
            .build_search_projection_graph_delta_request_after(0, Some(4))
            .unwrap()
            .unwrap();
        let mut search_index = SearchIndex::in_memory();
        db.apply_search_projection_graph_delta(&mut search_index, request)
            .unwrap();

        db.query_with_params(
            "MATCH (m:Memory {id: 'm1'}) SET m.content = m.content + ' ' + $extra, \
             m.title = CASE WHEN m.title = '' THEN $extra ELSE m.title END",
            &params(&[("extra", string("bounded expression"))]),
        )
        .unwrap();
        let request = db
            .build_search_projection_graph_delta_request_from_freshness(&search_index, Some(4))
            .unwrap()
            .unwrap();
        assert_eq!(request.upsert_node_ids, vec![0]);
        db.apply_search_projection_graph_delta(&mut search_index, request)
            .unwrap();
        let hits = search_index
            .search("bounded expression", None, SearchMode::Text, 10)
            .unwrap();
        assert_eq!(hits[0].id, "memory:m1");
    }
    let mut db = Database::open(&root).unwrap();
    let output = db
        .query("MATCH (m:Memory {id: 'm1'}) RETURN m.content AS content")
        .unwrap();
    assert_eq!(
        output.rows[0].get("content"),
        Some(&string("Old body bounded expression"))
    );
    drop(db);
    std::fs::remove_dir_all(&root).unwrap();
}

#[test]
fn explain_shows_the_row_evaluated_set() {
    let mut db = Database::new();
    for query in ENTITIES {
        db.query(query).unwrap();
    }
    let output = db
        .query("EXPLAIN MATCH (e:E {id: 'a'}) SET e.score = e.score * 2 RETURN e.score AS s")
        .unwrap();
    let plan = format!("{:?}", output.rows);
    assert!(plan.contains("MatchSetExec"), "{plan}");
    assert!(plan.contains("e.score=expression"), "{plan}");
}
