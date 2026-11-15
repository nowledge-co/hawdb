// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `RETURN` after a mutation clause, with Kùzu semantics: the rows describe
//! the entities the statement changed, as the statement left them, and the
//! mutation and its rows commit or fail together.

use super::*;
use crate::{ConcurrentDatabase, ConcurrentTransactionOptions, QueryRequest};

fn params(entries: &[(&str, Value)]) -> BTreeMap<String, Value> {
    entries
        .iter()
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect()
}

fn string(value: &str) -> Value {
    Value::String(value.to_string())
}

fn count(db: &mut Database, query: &str) -> i64 {
    match db.query(query).unwrap().rows[0]["total"] {
        Value::Int(total) => total,
        ref other => panic!("count expected, got {other:?}"),
    }
}

fn map(value: &Value) -> &BTreeMap<String, Value> {
    match value {
        Value::Map(map) => map,
        other => panic!("map expected, got {other:?}"),
    }
}

mod create {
    use super::*;

    const CREATE_COMPLETION: &str =
        "CREATE (o:Completion {id:$id, body:$body, body_sha256:$sha}) RETURN o.id";

    #[test]
    fn node_create_returns_the_created_property() {
        let mut db = Database::new();
        let output = db
            .query_with_params(
                CREATE_COMPLETION,
                &params(&[
                    ("id", string("new-1")),
                    ("body", string("blob")),
                    ("sha", string("digest")),
                ]),
            )
            .unwrap();
        assert_eq!(output.rows.schema().columns(), &["o.id"]);
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0]["o.id"], string("new-1"));
        let stored = db
            .query("MATCH (o:Completion) RETURN o.id AS id, o.body AS body, o.body_sha256 AS sha")
            .unwrap();
        assert_eq!(stored.rows.len(), 1);
        assert_eq!(stored.rows[0]["body"], string("blob"));
        assert_eq!(stored.rows[0]["sha"], string("digest"));
    }

    #[test]
    fn node_create_returns_whole_node_aliases_expressions_and_count() {
        let mut db = Database::new();
        let output = db
            .query("CREATE (n:Item {id: 1, name: 'first', score: 2}) RETURN n, n.name AS name, n.score + 1 AS next, n.missing AS missing")
            .unwrap();
        assert_eq!(
            output.rows.schema().columns(),
            &["n", "name", "next", "missing"]
        );
        let node = map(&output.rows[0]["n"]);
        assert_eq!(node["id"], Value::Int(1));
        assert_eq!(node["name"], string("first"));
        assert_eq!(node["labels"], Value::List(vec![string("Item")]));
        assert_eq!(output.rows[0]["name"], string("first"));
        assert_eq!(output.rows[0]["next"], Value::Int(3));
        assert_eq!(output.rows[0]["missing"], Value::Null);
        // The returned node is the stored node.
        let read = db.query("MATCH (n:Item {id: 1}) RETURN n").unwrap();
        assert_eq!(read.rows[0]["n"], output.rows[0]["n"]);

        for (query, column) in [
            ("CREATE (:Item {id: 2}) RETURN count(*)", "count(*)"),
            ("CREATE (n:Item {id: 3}) RETURN count(n) AS total", "total"),
        ] {
            let output = db.query(query).unwrap();
            assert_eq!(output.rows.schema().columns(), &[column]);
            assert_eq!(output.rows[0][column], Value::Int(1));
        }
        assert_eq!(count(&mut db, "MATCH (n:Item) RETURN count(n) AS total"), 3);
    }

    #[test]
    fn create_return_sees_declared_property_defaults() {
        let mut db = Database::new();
        db.query("CREATE PROPERTY ON NODE TABLE Job(status) TYPE STRING DEFAULT 'pending'")
            .unwrap();
        db.query("CREATE PROPERTY ON RELATIONSHIP TABLE OWNS(weight) TYPE INT DEFAULT 7")
            .unwrap();
        let output = db
            .query("CREATE (j:Job {id: 'a'}) RETURN j.status AS status, j.id AS id")
            .unwrap();
        assert_eq!(output.rows[0]["status"], string("pending"));
        // An explicit value is returned instead of the default.
        let output = db
            .query("CREATE (j:Job {id: 'b', status: 'done'}) RETURN j.status AS status")
            .unwrap();
        assert_eq!(output.rows[0]["status"], string("done"));

        db.query("CREATE (:Owner {id: 'o'})").unwrap();
        let output = db
            .query("MATCH (o:Owner {id: 'o'}), (j:Job {id: 'a'}) CREATE (o)-[r:OWNS]->(j) RETURN r.weight AS weight, r")
            .unwrap();
        assert_eq!(output.rows[0]["weight"], Value::Int(7));
        assert_eq!(map(&output.rows[0]["r"])["weight"], Value::Int(7));
        let output = db
            .query("CREATE (a:Owner {id: 'p'})-[r:OWNS]->(j:Job {id: 'c'}) RETURN j.status AS status, r.weight AS weight")
            .unwrap();
        assert_eq!(output.rows[0]["status"], string("pending"));
        assert_eq!(output.rows[0]["weight"], Value::Int(7));
    }

    #[test]
    fn matched_relationship_create_returns_one_row_per_created_relationship() {
        let mut db = Database::new();
        for query in [
            "CREATE (:Memory {id: 'a'})",
            "CREATE (:Memory {id: 'b'})",
            "CREATE (:Label {id: 'x'})",
            "CREATE (:Label {id: 'y'})",
        ] {
            db.query(query).unwrap();
        }
        let single = "MATCH (a:Memory {id:$source_id}), (b:Label {id:$target_id}) CREATE (a)-[r:HAS_LABEL {weight: 1}]->(b) RETURN r";
        let output = db
            .query_with_params(
                single,
                &params(&[("source_id", string("a")), ("target_id", string("x"))]),
            )
            .unwrap();
        assert_eq!(output.rows.schema().columns(), &["r"]);
        assert_eq!(output.rows.len(), 1);
        let relationship = map(&output.rows[0]["r"]);
        assert_eq!(relationship["weight"], Value::Int(1));
        assert_eq!(relationship["type"], string("HAS_LABEL"));
        let read = db
            .query("MATCH (:Memory {id: 'a'})-[r:HAS_LABEL]->(:Label {id: 'x'}) RETURN r")
            .unwrap();
        assert_eq!(read.rows[0]["r"], output.rows[0]["r"]);

        // No matching endpoint: no row and no relationship.
        let output = db
            .query_with_params(
                single,
                &params(&[("source_id", string("a")), ("target_id", string("missing"))]),
            )
            .unwrap();
        assert!(output.rows.is_empty());
        assert_eq!(output.rows.schema().columns(), &["r"]);
        assert_eq!(
            count(
                &mut db,
                "MATCH ()-[r:HAS_LABEL]->() RETURN count(r) AS total"
            ),
            1
        );

        // Several matched pairs: one row per created relationship, with the
        // endpoints and the relationship of that row.
        let output = db
            .query("MATCH (a:Memory), (b:Label) WHERE b.id = 'y' CREATE (a)-[r:TAGGED {at: 5}]->(b) RETURN a.id AS memory, b.id AS label, r.at AS at")
            .unwrap();
        let mut rows = output
            .rows
            .iter()
            .map(|row| {
                (
                    row["memory"].clone(),
                    row["label"].clone(),
                    row["at"].clone(),
                )
            })
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| format!("{row:?}"));
        assert_eq!(
            rows,
            vec![
                (string("a"), string("y"), Value::Int(5)),
                (string("b"), string("y"), Value::Int(5)),
            ]
        );
        let output = db
            .query("MATCH (a:Memory), (b:Label) CREATE (a)-[:SEEN]->(b) RETURN count(*) AS total")
            .unwrap();
        assert_eq!(output.rows[0]["total"], Value::Int(4));
        let output = db
            .query("MATCH (a:Memory {id: 'none'}), (b:Label) CREATE (a)-[:SEEN]->(b) RETURN count(*) AS total")
            .unwrap();
        assert_eq!(output.rows[0]["total"], Value::Int(0));
        assert_eq!(
            count(&mut db, "MATCH ()-[r:SEEN]->() RETURN count(r) AS total"),
            4
        );
    }

    #[test]
    fn connected_create_returns_both_nodes_and_the_relationship() {
        let mut db = Database::new();
        let output = db
            .query("CREATE (a:Source {id: 1})-[r:LINKS {w: 2}]->(b:Target {id: 3}) RETURN a.id AS a, r.w AS w, b.id AS b, type(r) AS type")
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0]["a"], Value::Int(1));
        assert_eq!(output.rows[0]["w"], Value::Int(2));
        assert_eq!(output.rows[0]["b"], Value::Int(3));
        assert_eq!(output.rows[0]["type"], string("LINKS"));
    }

    #[test]
    fn create_return_refusals_write_nothing() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'a'})").unwrap();
        for (query, message) in [
            ("CREATE (n:Item {id: 1}) RETURN m.id", "m"),
            (
                "CREATE (n:Item {id: 1}) RETURN n.id ORDER BY n.id",
                "modifiers",
            ),
            ("CREATE (n:Item {id: 1}) RETURN n.id LIMIT 1", "modifiers"),
            ("CREATE (n:Item {id: 1}) RETURN DISTINCT n.id", "modifiers"),
            ("CREATE (n:Item {id: 1}) RETURN count(m)", "count variable"),
            (
                "MATCH (a:Memory {id: 'a'}) CREATE (a)-[:R]->(:Item {id: 1}) RETURN a.id",
                "",
            ),
        ] {
            let error = db.query(query).unwrap_err();
            assert!(
                matches!(&error, HawDBError::Semantic(text) if text.contains(message)),
                "{query}: {error:?}"
            );
        }
        assert_eq!(count(&mut db, "MATCH (n:Item) RETURN count(n) AS total"), 0);
    }

    #[test]
    fn create_return_explain_names_the_return_operator() {
        let mut db = Database::new();
        let output = db
            .query("EXPLAIN CREATE (n:Item {id: 1}) RETURN n.id")
            .unwrap();
        let plan = format!("{:?}", output.rows);
        assert!(plan.contains("MutationReturn"), "{plan}");
        assert!(plan.contains("CreateNode"), "{plan}");
        assert_eq!(count(&mut db, "MATCH (n:Item) RETURN count(n) AS total"), 0);
    }

    #[test]
    fn create_return_persists_and_replays_from_wal() {
        let path = unique_test_dir("create_return_wal_replay");
        {
            let mut db = Database::open(&path).unwrap();
            db.query("CREATE PROPERTY ON NODE TABLE Job(status) TYPE STRING DEFAULT 'pending'")
                .unwrap();
            let output = db
                .query("CREATE (j:Job {id: 'a'}) RETURN j.id AS id, j.status AS status")
                .unwrap();
            assert_eq!(output.rows[0]["status"], string("pending"));
            db.query("CREATE (:Owner {id: 'o'})").unwrap();
            let output = db
                .query("MATCH (o:Owner {id: 'o'}), (j:Job {id: 'a'}) CREATE (o)-[r:OWNS {w: 1}]->(j) RETURN r.w AS w")
                .unwrap();
            assert_eq!(output.rows[0]["w"], Value::Int(1));
        }
        {
            let mut db = Database::open(&path).unwrap();
            let output = db
                .query("MATCH (o:Owner)-[r:OWNS]->(j:Job) RETURN o.id AS owner, r.w AS w, j.status AS status")
                .unwrap();
            assert_eq!(output.rows.len(), 1);
            assert_eq!(output.rows[0]["owner"], string("o"));
            assert_eq!(output.rows[0]["w"], Value::Int(1));
            assert_eq!(output.rows[0]["status"], string("pending"));
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    const PAIR_CREATE: &str = "MATCH (a:Memory), (b:Label) CREATE (a)-[r:HAS_LABEL]->(b) RETURN a.id AS memory, b.id AS label";
    const PAIR_COUNT: &str = "MATCH ()-[r:HAS_LABEL]->() RETURN count(r) AS total";

    fn fixture() -> Database {
        let mut db = Database::new();
        for query in [
            "CREATE (:Memory {id: 'a'})",
            "CREATE (:Memory {id: 'b'})",
            "CREATE (:Label {id: 'x'})",
        ] {
            db.query(query).unwrap();
        }
        db
    }

    /// Output budgets apply to the returned rows; a refused statement leaves
    /// no relationship and keeps earlier statements.
    fn assert_output_refusal(
        mut execute: impl for<'a> FnMut(QueryRequest<'a>) -> crate::Result<QueryOutput>,
    ) {
        execute(QueryRequest::new("CREATE (:Retained {id: 1})")).unwrap();
        for limits in [
            QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: None,
            },
            QueryStreamOptions {
                max_rows: None,
                max_payload_bytes: Some(1),
            },
        ] {
            let error =
                execute(QueryRequest::new(PAIR_CREATE).with_output_limits(limits)).unwrap_err();
            assert!(
                matches!(error, HawDBError::ReadBudgetExceeded(_)),
                "{error:?}"
            );
            assert_eq!(
                execute(QueryRequest::new(PAIR_COUNT)).unwrap().rows[0]["total"],
                Value::Int(0)
            );
        }
        // One output row is enough for a count.
        let counted = execute(
            QueryRequest::new(
                "MATCH (a:Memory), (b:Label) CREATE (a)-[r:HAS_LABEL]->(b) RETURN count(*) AS total",
            )
            .with_output_limits(QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: None,
            }),
        )
        .unwrap();
        assert_eq!(counted.rows[0]["total"], Value::Int(2));
        assert_eq!(
            execute(QueryRequest::new(PAIR_CREATE)).unwrap().rows.len(),
            2
        );
        assert_eq!(
            execute(QueryRequest::new(PAIR_COUNT)).unwrap().rows[0]["total"],
            Value::Int(4)
        );
        assert_eq!(
            execute(QueryRequest::new("MATCH (n:Retained) RETURN n.id"))
                .unwrap()
                .rows
                .len(),
            1
        );
    }

    #[test]
    fn create_return_budgets_are_atomic_on_every_execution_surface() {
        let mut db = fixture();
        assert_output_refusal(|request| db.query_request(request));

        let mut db = fixture();
        let mut transaction = db.begin_transaction().unwrap();
        assert_output_refusal(|request| transaction.query_request(request));
        transaction.rollback();
        assert_eq!(count(&mut db, PAIR_COUNT), 0);

        let mut db = fixture();
        let mut transaction = db.begin_transaction().unwrap();
        assert_output_refusal(|request| transaction.query_request(request));
        transaction.commit().unwrap();
        assert_eq!(count(&mut db, PAIR_COUNT), 4);

        let database = ConcurrentDatabase::new(fixture());
        assert_output_refusal(|request| database.query_request(request));
        for options in [
            ConcurrentTransactionOptions::optimistic(),
            ConcurrentTransactionOptions::default(),
        ] {
            let database = ConcurrentDatabase::new(fixture());
            let mut transaction = database.begin_transaction(options).unwrap();
            assert_output_refusal(|request| transaction.query_request(request));
            transaction.commit().unwrap();
            assert_eq!(
                database.query(PAIR_COUNT).unwrap().rows[0]["total"],
                Value::Int(4)
            );
        }
    }

    #[test]
    fn create_return_in_a_transaction_sees_and_is_seen_by_neighbouring_statements() {
        let mut db = Database::new();
        let mut transaction = db.begin_transaction().unwrap();
        transaction.query("CREATE (:Memory {id: 'a'})").unwrap();
        let output = transaction
            .query("MATCH (m:Memory {id: 'a'}), (n:Memory {id: 'a'}) CREATE (m)-[r:SELF {k: 1}]->(n) RETURN r.k AS k")
            .unwrap();
        assert_eq!(output.rows[0]["k"], Value::Int(1));
        let output = transaction
            .query("CREATE (l:Label {id: 'x'}) RETURN l.id AS id")
            .unwrap();
        assert_eq!(output.rows[0]["id"], string("x"));
        assert_eq!(
            transaction
                .query("MATCH (l:Label) RETURN count(l) AS total")
                .unwrap()
                .rows[0]["total"],
            Value::Int(1)
        );
        transaction.rollback();
        assert_eq!(
            count(&mut db, "MATCH (l:Label) RETURN count(l) AS total"),
            0
        );
    }
}
