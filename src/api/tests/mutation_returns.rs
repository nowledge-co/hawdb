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
                "ORDER BY",
            ),
            ("CREATE (n:Item {id: 1}) RETURN DISTINCT n.id", "DISTINCT"),
            ("CREATE (n:Item {id: 1}) RETURN count(m)", "count variable"),
            (
                // The CREATE shape itself is refused before RETURN is bound.
                "MATCH (a:Memory {id: 'a'}) CREATE (a)-[:R]->(:Item {id: 1}) RETURN a.id",
                "undecorated variable reference",
            ),
            (
                // RETURN sees only the variables the CREATE pattern binds; an
                // anonymous relationship binds none.
                "MATCH (a:Memory {id: 'a'}), (b:Memory {id: 'a'}) CREATE (a)-[:R]->(b) RETURN r",
                "unknown variable 'r'",
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

mod delete {
    use super::*;

    /// The issue's guarded orphan-entity cleanup.
    const ORPHAN_DELETE: &str = "MATCH (e:Entity {id: $entity_id}) WHERE NOT (e)<-[:MENTIONS]-(:Memory) AND NOT (e)-[:RELATES_TO]-() AND NOT (e)-[:HAS_LABEL]-() AND (e.metadata IS NULL OR NOT contains(e.metadata, 'identity_profile_revision')) DETACH DELETE e RETURN e.id";

    fn entity_ids(db: &mut Database) -> Vec<Value> {
        let mut ids = db
            .query("MATCH (e:Entity) RETURN e.id AS id")
            .unwrap()
            .rows
            .iter()
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>();
        ids.sort_by_key(|value| format!("{value:?}"));
        ids
    }

    fn fixture() -> Database {
        let mut db = Database::new();
        for query in [
            "CREATE (:Entity {id: 'orphan', name: 'Orphan'})",
            "CREATE (:Entity {id: 'profiled', metadata: '{\"identity_profile_revision\": 2}'})",
            "CREATE (:Entity {id: 'mentioned'})",
            "CREATE (:Entity {id: 'related'})",
            "CREATE (:Entity {id: 'peer'})",
            "CREATE (:Memory {id: 'm'})",
            "MATCH (m:Memory {id: 'm'}), (e:Entity {id: 'mentioned'}) CREATE (m)-[:MENTIONS]->(e)",
            "MATCH (a:Entity {id: 'related'}), (b:Entity {id: 'peer'}) CREATE (a)-[:RELATES_TO {w: 3}]->(b)",
        ] {
            db.query(query).unwrap();
        }
        db
    }

    #[test]
    fn guarded_detach_delete_returns_the_deleted_entity_only_when_it_matched() {
        let mut db = fixture();
        let delete = |db: &mut Database, id: &str| {
            db.query_with_params(ORPHAN_DELETE, &params(&[("entity_id", string(id))]))
                .unwrap()
        };
        let output = delete(&mut db, "orphan");
        assert_eq!(output.rows.schema().columns(), &["e.id"]);
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0]["e.id"], string("orphan"));
        // Guarded entities and a missing id: no row, nothing deleted.
        for id in [
            "profiled",
            "mentioned",
            "related",
            "peer",
            "missing",
            "orphan",
        ] {
            let output = delete(&mut db, id);
            assert!(output.rows.is_empty(), "{id}");
            assert_eq!(output.rows.schema().columns(), &["e.id"]);
        }
        assert_eq!(
            entity_ids(&mut db),
            vec![
                string("mentioned"),
                string("peer"),
                string("profiled"),
                string("related")
            ]
        );
    }

    #[test]
    fn delete_count_counts_deleted_rows_and_is_zero_without_a_match() {
        let mut db = fixture();
        let output = db
            .query("MATCH (e:Entity {id: 'orphan'}) DETACH DELETE e RETURN count(*)")
            .unwrap();
        assert_eq!(output.rows.schema().columns(), &["count(*)"]);
        assert_eq!(output.rows[0]["count(*)"], Value::Int(1));
        let output = db
            .query("MATCH (e:Entity {id: 'orphan'}) DETACH DELETE e RETURN count(*)")
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0]["count(*)"], Value::Int(0));
        // Several rows, with incident relationships removed by DETACH.
        let output = db
            .query("MATCH (e:Entity) WHERE e.id <> 'profiled' DETACH DELETE e RETURN count(e) AS total")
            .unwrap();
        assert_eq!(output.rows[0]["total"], Value::Int(3));
        assert_eq!(entity_ids(&mut db), vec![string("profiled")]);
        assert_eq!(
            count(&mut db, "MATCH ()-[r]->() RETURN count(r) AS total"),
            0
        );
        // A plain DELETE with RETURN also counts.
        let output = db
            .query("MATCH (m:Memory) DELETE m RETURN count(*) AS total")
            .unwrap();
        assert_eq!(output.rows[0]["total"], Value::Int(1));
    }

    #[test]
    fn delete_return_reads_entities_as_they_were_before_the_delete() {
        let mut db = fixture();
        db.query("CREATE PROPERTY ON NODE TABLE Entity(status) TYPE STRING DEFAULT 'active'")
            .unwrap();
        db.query("CREATE (:Entity {id: 'late', name: 'Late'})")
            .unwrap();
        let selected = "MATCH (e:Entity) WHERE e.id = 'orphan' OR e.id = 'late'";
        let sorted = |rows: &crate::executor::QueryRows, column: &str| {
            let mut values = rows
                .iter()
                .map(|row| row[column].clone())
                .collect::<Vec<_>>();
            values.sort_by_key(|value| format!("{value:?}"));
            values
        };
        let before = db.query(&format!("{selected} RETURN e")).unwrap();
        let output = db
            .query(&format!(
                "{selected} DETACH DELETE e RETURN e, e.name AS name, e.status AS status, e.id AS id"
            ))
            .unwrap();
        assert_eq!(
            output.rows.schema().columns(),
            &["e", "name", "status", "id"]
        );
        // The returned nodes are the nodes as a read saw them before.
        assert_eq!(sorted(&output.rows, "e"), sorted(&before.rows, "e"));
        let late = output
            .rows
            .iter()
            .find(|row| row["id"] == string("late"))
            .unwrap();
        assert_eq!(late["name"], string("Late"));
        // A property DEFAULT filled at creation is part of the deleted node.
        assert_eq!(late["status"], string("active"));
        assert_eq!(
            map(&late["e"])["labels"],
            Value::List(vec![string("Entity")])
        );
        assert_eq!(
            count(&mut db, &format!("{selected} RETURN count(e) AS total")),
            0
        );
    }

    #[test]
    fn relationship_delete_returns_the_relationship_and_its_endpoints() {
        let mut db = fixture();
        db.query("MATCH (a:Entity {id: 'peer'}), (b:Entity {id: 'orphan'}) CREATE (a)-[:RELATES_TO {w: 5}]->(b)")
            .unwrap();
        let output = db
            .query("MATCH (a:Entity)-[r:RELATES_TO]->(b:Entity) WHERE a.id = 'related' DELETE r RETURN a.id AS source, r.w AS w, b.id AS target, r")
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0]["source"], string("related"));
        assert_eq!(output.rows[0]["w"], Value::Int(3));
        assert_eq!(output.rows[0]["target"], string("peer"));
        assert_eq!(map(&output.rows[0]["r"])["type"], string("RELATES_TO"));
        let output = db
            .query("MATCH (a:Entity)-[r:RELATES_TO]->(b:Entity) DELETE r RETURN count(*) AS total")
            .unwrap();
        assert_eq!(output.rows[0]["total"], Value::Int(1));
        let output = db
            .query("MATCH (a:Entity)-[r:RELATES_TO]->(b:Entity) DELETE r RETURN count(*) AS total")
            .unwrap();
        assert_eq!(output.rows[0]["total"], Value::Int(0));
        // The endpoints remain.
        assert_eq!(entity_ids(&mut db).len(), 5);
    }

    #[test]
    fn delete_return_refusals_delete_nothing() {
        let mut db = fixture();
        // A plain DELETE of a connected node fails, with or without RETURN.
        let error = db
            .query("MATCH (e:Entity {id: 'related'}) DELETE e RETURN e.id")
            .unwrap_err();
        assert!(error.to_string().contains("DETACH DELETE"), "{error:?}");
        for (query, message) in [
            (
                "MATCH (e:Entity) DETACH DELETE e RETURN e.id ORDER BY e.id",
                "ORDER BY",
            ),
            ("MATCH (e:Entity) DETACH DELETE e RETURN x.id", "x"),
            (
                "MATCH (e:Entity) DETACH DELETE e WITH e RETURN e",
                "following clauses",
            ),
            (
                "MATCH (m:Memory {id: 'm'})-[:MENTIONS]->(e:Entity) DETACH DELETE e RETURN e.id",
                "DELETE RETURN supports",
            ),
        ] {
            let error = db.query(query).unwrap_err();
            assert!(
                matches!(&error, HawDBError::Semantic(text) if text.contains(message)),
                "{query}: {error:?}"
            );
        }
        assert_eq!(entity_ids(&mut db).len(), 5);
    }

    #[test]
    fn delete_return_persists_and_replays_from_wal() {
        let path = unique_test_dir("delete_return_wal_replay");
        {
            let mut db = Database::open(&path).unwrap();
            db.query("CREATE (:Entity {id: 'a'})-[:RELATES_TO]->(:Entity {id: 'b'})")
                .unwrap();
            let output = db
                .query("MATCH (e:Entity {id: 'a'}) DETACH DELETE e RETURN e.id AS id")
                .unwrap();
            assert_eq!(output.rows[0]["id"], string("a"));
        }
        {
            let mut db = Database::open(&path).unwrap();
            assert_eq!(entity_ids(&mut db), vec![string("b")]);
            assert_eq!(
                count(&mut db, "MATCH ()-[r]->() RETURN count(r) AS total"),
                0
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn delete_return_in_a_transaction_reads_earlier_statements_and_rolls_back() {
        let mut db = fixture();
        let mut transaction = db.begin_transaction().unwrap();
        transaction
            .query("CREATE (:Entity {id: 'fresh', name: 'Fresh'})")
            .unwrap();
        let output = transaction
            .query("MATCH (e:Entity {id: 'fresh'}) DETACH DELETE e RETURN e.name AS name")
            .unwrap();
        assert_eq!(output.rows[0]["name"], string("Fresh"));
        let output = transaction
            .query_with_params(ORPHAN_DELETE, &params(&[("entity_id", string("orphan"))]))
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            transaction
                .query("MATCH (e:Entity) RETURN count(e) AS total")
                .unwrap()
                .rows[0]["total"],
            Value::Int(4)
        );
        transaction.rollback();
        assert_eq!(entity_ids(&mut db).len(), 5);
    }

    const MULTI_DELETE: &str =
        "MATCH (e:Entity) WHERE e.id = 'orphan' OR e.id = 'peer' DETACH DELETE e RETURN e.id AS id";

    fn assert_output_refusal(
        mut execute: impl for<'a> FnMut(QueryRequest<'a>) -> crate::Result<QueryOutput>,
    ) {
        execute(QueryRequest::new("CREATE (:Retained {id: 1})")).unwrap();
        let remaining =
            |execute: &mut dyn for<'a> FnMut(QueryRequest<'a>) -> crate::Result<QueryOutput>| {
                execute(QueryRequest::new(
                "MATCH (e:Entity) WHERE e.id = 'orphan' OR e.id = 'peer' RETURN count(e) AS total",
            ))
            .unwrap()
            .rows[0]["total"]
                .clone()
            };
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
                execute(QueryRequest::new(MULTI_DELETE).with_output_limits(limits)).unwrap_err();
            assert!(
                matches!(error, HawDBError::ReadBudgetExceeded(_)),
                "{error:?}"
            );
            assert_eq!(remaining(&mut execute), Value::Int(2));
        }
        let counted = execute(
            QueryRequest::new(
                "MATCH (e:Entity) WHERE e.id = 'orphan' DETACH DELETE e RETURN count(*) AS total",
            )
            .with_output_limits(QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: None,
            }),
        )
        .unwrap();
        assert_eq!(counted.rows[0]["total"], Value::Int(1));
        assert_eq!(
            execute(QueryRequest::new(MULTI_DELETE)).unwrap().rows.len(),
            1
        );
        assert_eq!(remaining(&mut execute), Value::Int(0));
        assert_eq!(
            execute(QueryRequest::new("MATCH (n:Retained) RETURN n.id"))
                .unwrap()
                .rows
                .len(),
            1
        );
    }

    #[test]
    fn delete_return_budgets_are_atomic_on_every_execution_surface() {
        let mut db = fixture();
        assert_output_refusal(|request| db.query_request(request));

        let mut db = fixture();
        let mut transaction = db.begin_transaction().unwrap();
        assert_output_refusal(|request| transaction.query_request(request));
        transaction.rollback();
        assert_eq!(entity_ids(&mut db).len(), 5);

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
                database
                    .query("MATCH (e:Entity) RETURN count(e) AS total")
                    .unwrap()
                    .rows[0]["total"],
                Value::Int(3)
            );
        }
    }
}

mod merge {
    use super::*;

    const LABEL_UPSERT: &str = "MERGE (l:Label {id: $id}) ON CREATE SET l.name = 'created', l.created_at = $ts ON MATCH SET l.name = 'matched' RETURN l.name";

    #[test]
    fn node_merge_returns_the_created_then_the_matched_node() {
        let mut db = Database::new();
        let parameters = params(&[("id", string("l1")), ("ts", Value::Int(7))]);
        let first = db.query_with_params(LABEL_UPSERT, &parameters).unwrap();
        assert_eq!(first.rows.schema().columns(), &["l.name"]);
        assert_eq!(first.rows.len(), 1);
        assert_eq!(first.rows[0]["l.name"], string("created"));
        let second = db.query_with_params(LABEL_UPSERT, &parameters).unwrap();
        assert_eq!(second.rows[0]["l.name"], string("matched"));
        // ON CREATE values stay; ON MATCH changed only the name.
        let output = db
            .query("MERGE (l:Label {id: 'l1'}) RETURN l, l.created_at AS created_at, count(*) AS ignored")
            .unwrap_err();
        assert!(matches!(output, HawDBError::Semantic(_)), "{output:?}");
        let output = db
            .query("MERGE (l:Label {id: 'l1'}) RETURN l, l.created_at AS created_at")
            .unwrap();
        assert_eq!(output.rows[0]["created_at"], Value::Int(7));
        assert_eq!(map(&output.rows[0]["l"])["name"], string("matched"));
        assert_eq!(
            count(&mut db, "MATCH (l:Label) RETURN count(l) AS total"),
            1
        );
        let output = db
            .query("MERGE (:Label {id: 'l2'}) RETURN count(*) AS total")
            .unwrap();
        assert_eq!(output.rows[0]["total"], Value::Int(1));
        assert_eq!(
            count(&mut db, "MATCH (l:Label) RETURN count(l) AS total"),
            2
        );
    }

    #[test]
    fn node_merge_return_sees_following_set_and_declared_defaults() {
        let mut db = Database::new();
        db.query("CREATE PROPERTY ON NODE TABLE GraphMeta(state) TYPE STRING DEFAULT 'fresh'")
            .unwrap();
        let query = "MERGE (m:GraphMeta {meta_id: 'main'}) SET m.applied = $applied RETURN m.applied AS applied, m.state AS state";
        let output = db
            .query_with_params(query, &params(&[("applied", Value::Bool(true))]))
            .unwrap();
        assert_eq!(output.rows[0]["applied"], Value::Bool(true));
        assert_eq!(output.rows[0]["state"], string("fresh"));
        db.query("MATCH (m:GraphMeta) SET m.state = 'used'")
            .unwrap();
        let output = db
            .query_with_params(query, &params(&[("applied", Value::Bool(false))]))
            .unwrap();
        assert_eq!(output.rows[0]["applied"], Value::Bool(false));
        assert_eq!(output.rows[0]["state"], string("used"));
    }

    fn entities() -> Database {
        let mut db = Database::new();
        for query in [
            "CREATE (:Entity {id: 'seed'})",
            "CREATE (:Entity {id: 'seed2'})",
            "CREATE (:Memory {id: 'm1'})",
            "CREATE (:Memory {id: 'm2'})",
            "CREATE (:Label {id: 'x'})",
        ] {
            db.query(query).unwrap();
        }
        db
    }

    const COUNTED_MERGE: &str = "MATCH (a:Entity {id:'seed'}), (b:Entity {id:'seed2'}) MERGE (a)-[r:RELATES_TO {relation_type: 'X'}]->(b) ON CREATE SET r.count = 1, r.origin = 'create' ON MATCH SET r.count = r.count + 1, r.touched = true RETURN r.count AS count, r.origin AS origin, r.touched AS touched";

    #[test]
    fn relationship_merge_on_match_set_applies_only_to_a_matched_relationship() {
        let mut db = entities();
        let rows = |db: &mut Database| {
            let output = db.query(COUNTED_MERGE).unwrap();
            assert_eq!(output.rows.len(), 1);
            (
                output.rows[0]["count"].clone(),
                output.rows[0]["origin"].clone(),
                output.rows[0]["touched"].clone(),
            )
        };
        assert_eq!(
            rows(&mut db),
            (Value::Int(1), string("create"), Value::Null)
        );
        assert_eq!(
            rows(&mut db),
            (Value::Int(2), string("create"), Value::Bool(true))
        );
        // Without RETURN the same MERGE also applies ON MATCH.
        db.query(&COUNTED_MERGE[..COUNTED_MERGE.find(" RETURN").unwrap()])
            .unwrap();
        assert_eq!(
            rows(&mut db),
            (Value::Int(4), string("create"), Value::Bool(true))
        );
        assert_eq!(
            count(
                &mut db,
                "MATCH ()-[r:RELATES_TO]->() RETURN count(r) AS total"
            ),
            1
        );
        // A different match key creates a second relationship; ON MATCH does
        // not run for it.
        let output = db
            .query("MATCH (a:Entity {id:'seed'}), (b:Entity {id:'seed2'}) MERGE (a)-[r:RELATES_TO {relation_type: 'Y'}]->(b) ON MATCH SET r.touched = true RETURN r.relation_type AS kind, r.touched AS touched, r")
            .unwrap();
        assert_eq!(output.rows[0]["kind"], string("Y"));
        assert_eq!(output.rows[0]["touched"], Value::Null);
        assert_eq!(map(&output.rows[0]["r"])["relation_type"], string("Y"));
    }

    const LABEL_ASSIGN: &str = "MATCH (m:Memory {id: $memory}), (l:Label {id: $label}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = 'system', r.created_at = $now, r.properties = '{}' RETURN l.id LIMIT 1";

    #[test]
    fn matched_relationship_merge_return_with_limit_is_idempotent() {
        let mut db = entities();
        let parameters = |label: &str| {
            params(&[
                ("memory", string("m1")),
                ("label", string(label)),
                ("now", Value::Int(5)),
            ])
        };
        for _ in 0..2 {
            let output = db
                .query_with_params(LABEL_ASSIGN, &parameters("x"))
                .unwrap();
            assert_eq!(output.rows.schema().columns(), &["l.id"]);
            assert_eq!(output.rows.len(), 1);
            assert_eq!(output.rows[0]["l.id"], string("x"));
        }
        let output = db
            .query_with_params(LABEL_ASSIGN, &parameters("missing"))
            .unwrap();
        assert!(output.rows.is_empty());
        let edges = db
            .query("MATCH (:Memory)-[r:HAS_LABEL]->(:Label) RETURN r.assigned_by AS by, r.created_at AS at")
            .unwrap();
        assert_eq!(edges.rows.len(), 1);
        assert_eq!(edges.rows[0]["by"], string("system"));
        assert_eq!(edges.rows[0]["at"], Value::Int(5));
    }

    #[test]
    fn relationship_merge_skip_and_limit_select_rows_but_every_row_is_merged() {
        let mut db = entities();
        let query = "MATCH (m:Memory), (l:Label {id: 'x'}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.seen = 0 ON MATCH SET r.seen = r.seen + 1 RETURN m.id AS memory, r.seen AS seen";
        let output = db.query(query).unwrap();
        assert_eq!(output.rows.len(), 2);
        assert!(output.rows.iter().all(|row| row["seen"] == Value::Int(0)));
        let output = db.query(&format!("{query} LIMIT 1")).unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0]["seen"], Value::Int(1));
        let output = db.query(&format!("{query} SKIP 1")).unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0]["seen"], Value::Int(2));
        let output = db.query(&format!("{query} SKIP 2")).unwrap();
        assert!(output.rows.is_empty());
        let seen = db
            .query("MATCH (:Memory)-[r:HAS_LABEL]->(:Label) RETURN r.seen AS seen")
            .unwrap();
        assert_eq!(seen.rows.len(), 2);
        assert!(seen.rows.iter().all(|row| row["seen"] == Value::Int(3)));
        let output = db
            .query("MATCH (m:Memory), (l:Label {id: 'x'}) MERGE (m)-[r:HAS_LABEL]->(l) RETURN count(*) AS total LIMIT 0")
            .unwrap();
        assert!(output.rows.is_empty());
    }

    #[test]
    fn merge_return_refusals_write_nothing() {
        let mut db = entities();
        for (query, message) in [
            // Only integer increments are supported in SET right-hand sides.
            (
                "MATCH (a:Entity {id:'seed'}), (b:Entity {id:'seed2'}) MERGE (a)-[r:R]->(b) ON MATCH SET r.w = r.w + 1.0 RETURN r.w",
                "integer increment",
            ),
            (
                "MATCH (a:Entity {id:'seed'}), (b:Entity {id:'seed2'}) MERGE (a)-[:R]->(b) ON MATCH SET r.w = 1",
                "relationship variable",
            ),
            (
                "UNWIND ['seed2'] AS id MATCH (a:Entity {id:'seed'}), (b:Entity {id: id}) MERGE (a)-[r:R]->(b) ON MATCH SET r.w = 1 RETURN b.id",
                "ON CREATE",
            ),
            ("MERGE (l:Label {id: 'n'}) RETURN l ORDER BY l.id", "ORDER BY"),
            ("MERGE (l:Label {id: 'n'}) RETURN DISTINCT l.id", "DISTINCT"),
            ("MERGE (l:Label {id: 'n'}) RETURN m", "m"),
            (
                "MATCH (a:Entity {id:'seed'}), (b:Entity {id:'seed2'}) MERGE (a)-[r:R]->(b) SET r.w = 1 RETURN r",
                "following SET",
            ),
        ] {
            let error = db.query(query).unwrap_err();
            assert!(
                matches!(&error, HawDBError::Semantic(text) if text.contains(message)),
                "{query}: {error:?}"
            );
        }
        assert_eq!(
            count(&mut db, "MATCH ()-[r]->() RETURN count(r) AS total"),
            0
        );
        assert_eq!(
            count(
                &mut db,
                "MATCH (l:Label {id: 'n'}) RETURN count(l) AS total"
            ),
            0
        );
    }

    #[test]
    fn merge_return_persists_and_replays_from_wal() {
        let path = unique_test_dir("merge_return_wal_replay");
        {
            let mut db = Database::open(&path).unwrap();
            db.query("CREATE (:Entity {id: 'seed'})").unwrap();
            db.query("CREATE (:Entity {id: 'seed2'})").unwrap();
            for expected in [1, 2] {
                let output = db.query(COUNTED_MERGE).unwrap();
                assert_eq!(output.rows[0]["count"], Value::Int(expected));
            }
            let output = db
                .query_with_params(
                    LABEL_UPSERT,
                    &params(&[("id", string("l")), ("ts", Value::Int(1))]),
                )
                .unwrap();
            assert_eq!(output.rows[0]["l.name"], string("created"));
        }
        {
            let mut db = Database::open(&path).unwrap();
            let output = db.query(COUNTED_MERGE).unwrap();
            assert_eq!(output.rows[0]["count"], Value::Int(3));
            let output = db
                .query_with_params(
                    LABEL_UPSERT,
                    &params(&[("id", string("l")), ("ts", Value::Int(1))]),
                )
                .unwrap();
            assert_eq!(output.rows[0]["l.name"], string("matched"));
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn merge_return_in_a_transaction_sees_earlier_statements_and_rolls_back() {
        let mut db = entities();
        let mut transaction = db.begin_transaction().unwrap();
        for expected in [1, 2, 3] {
            let output = transaction.query(COUNTED_MERGE).unwrap();
            assert_eq!(output.rows[0]["count"], Value::Int(expected));
        }
        transaction.rollback();
        assert_eq!(
            count(&mut db, "MATCH ()-[r]->() RETURN count(r) AS total"),
            0
        );
        let mut transaction = db.begin_transaction().unwrap();
        transaction.query(COUNTED_MERGE).unwrap();
        transaction.commit().unwrap();
        assert_eq!(
            db.query(COUNTED_MERGE).unwrap().rows[0]["count"],
            Value::Int(2)
        );
    }

    const PAIR_MERGE: &str = "MATCH (m:Memory), (l:Label {id: 'x'}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.seen = 0 ON MATCH SET r.seen = r.seen + 1 RETURN m.id AS memory, r";
    const SEEN_TOTAL: &str =
        "MATCH (:Memory)-[r:HAS_LABEL]->(:Label) RETURN count(r) AS total, sum(r.seen) AS seen";

    fn assert_output_refusal(
        mut execute: impl for<'a> FnMut(QueryRequest<'a>) -> crate::Result<QueryOutput>,
    ) {
        execute(QueryRequest::new(PAIR_MERGE)).unwrap();
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
                execute(QueryRequest::new(PAIR_MERGE).with_output_limits(limits)).unwrap_err();
            assert!(
                matches!(error, HawDBError::ReadBudgetExceeded(_)),
                "{error:?}"
            );
            // The refused statement applied no ON MATCH update.
            let totals = execute(QueryRequest::new(SEEN_TOTAL)).unwrap();
            assert_eq!(totals.rows[0]["total"], Value::Int(2));
            assert_eq!(totals.rows[0]["seen"], Value::Int(0));
        }
        let limited = execute(
            QueryRequest::new(&format!("{PAIR_MERGE} LIMIT 1")).with_output_limits(
                QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: None,
                },
            ),
        )
        .unwrap();
        assert_eq!(limited.rows.len(), 1);
        let totals = execute(QueryRequest::new(SEEN_TOTAL)).unwrap();
        assert_eq!(totals.rows[0]["seen"], Value::Int(2));
    }

    #[test]
    fn merge_return_budgets_are_atomic_on_every_execution_surface() {
        let mut db = entities();
        assert_output_refusal(|request| db.query_request(request));

        let mut db = entities();
        let mut transaction = db.begin_transaction().unwrap();
        assert_output_refusal(|request| transaction.query_request(request));
        transaction.rollback();
        assert_eq!(
            count(&mut db, "MATCH ()-[r]->() RETURN count(r) AS total"),
            0
        );

        let database = ConcurrentDatabase::new(entities());
        assert_output_refusal(|request| database.query_request(request));
        for options in [
            ConcurrentTransactionOptions::optimistic(),
            ConcurrentTransactionOptions::default(),
        ] {
            let database = ConcurrentDatabase::new(entities());
            let mut transaction = database.begin_transaction(options).unwrap();
            assert_output_refusal(|request| transaction.query_request(request));
            transaction.commit().unwrap();
            assert_eq!(
                database.query(SEEN_TOTAL).unwrap().rows[0]["seen"],
                Value::Int(2)
            );
        }
    }
}

mod budgets {
    use super::*;
    use std::num::NonZeroUsize;

    fn limited(rows: usize, payload_bytes: usize) -> Database {
        let mut config = DatabaseConfig::default();
        config.mutation_limits.max_result_rows = NonZeroUsize::new(rows).unwrap();
        config.mutation_limits.max_result_payload_bytes = NonZeroUsize::new(payload_bytes).unwrap();
        let mut db = Database::new_with_config(config);
        for id in ["a", "b"] {
            db.query(&format!("CREATE (:Entity {{id: '{id}'}})"))
                .unwrap();
        }
        db
    }

    fn assert_budget(error: HawDBError, option: &str) {
        let HawDBError::ResourceBudgetExceeded(error) = error else {
            panic!("expected a typed budget refusal, got {error:?}");
        };
        assert_eq!(error.budget_option, option, "{error:?}");
        assert_eq!(error.owner, "mutation RETURN rows");
    }

    const DELETE_BOTH: &str = "MATCH (e:Entity) DETACH DELETE e RETURN e.id AS id";
    const CREATE_ONE: &str = "CREATE (n:Item {id: 'a', body: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'}) RETURN n.body AS body";

    /// Mutation result limits on RETURN rows are typed budget refusals that
    /// write nothing; a count stays one row.
    fn assert_typed_refusals(
        rows_limited: &mut dyn for<'a> FnMut(QueryRequest<'a>) -> crate::Result<QueryOutput>,
        payload_limited: &mut dyn for<'a> FnMut(QueryRequest<'a>) -> crate::Result<QueryOutput>,
    ) {
        let total =
            |execute: &mut dyn for<'a> FnMut(QueryRequest<'a>) -> crate::Result<QueryOutput>,
             query: &str| {
                execute(QueryRequest::new(query)).unwrap().rows[0]["total"].clone()
            };
        assert_budget(
            rows_limited(QueryRequest::new(DELETE_BOTH)).unwrap_err(),
            "MutationLimits.max_result_rows",
        );
        assert_eq!(
            total(rows_limited, "MATCH (e:Entity) RETURN count(e) AS total"),
            Value::Int(2)
        );
        assert_budget(
            payload_limited(QueryRequest::new(CREATE_ONE)).unwrap_err(),
            "MutationLimits.max_result_payload_bytes",
        );
        assert_eq!(
            total(payload_limited, "MATCH (n:Item) RETURN count(n) AS total"),
            Value::Int(0)
        );
        let counted = rows_limited(QueryRequest::new(
            "MATCH (e:Entity) DETACH DELETE e RETURN count(*) AS total",
        ))
        .unwrap();
        assert_eq!(counted.rows[0]["total"], Value::Int(2));
    }

    /// `SET ... RETURN` (single-pattern and row-evaluated with `ORDER BY`) and
    /// `UNWIND` relationship `MERGE ... RETURN` refuse result limits with the
    /// same typed error, and write nothing (#1800).
    fn assert_set_and_merge_refusals(
        rows_limited: &mut dyn for<'a> FnMut(QueryRequest<'a>) -> crate::Result<QueryOutput>,
        payload_limited: &mut dyn for<'a> FnMut(QueryRequest<'a>) -> crate::Result<QueryOutput>,
    ) {
        let total =
            |execute: &mut dyn for<'a> FnMut(QueryRequest<'a>) -> crate::Result<QueryOutput>,
             query: &str| {
                execute(QueryRequest::new(query)).unwrap().rows[0]["total"].clone()
            };
        const SEEN: &str = "MATCH (e:Entity) WHERE e.seen = true RETURN count(e) AS total";
        for query in [
            "MATCH (e:Entity) SET e.seen = true RETURN e.id AS id",
            "MATCH (e:Entity) WITH e SET e.seen = true RETURN e.id AS id ORDER BY id",
        ] {
            assert_budget(
                rows_limited(QueryRequest::new(query)).unwrap_err(),
                "MutationLimits.max_result_rows",
            );
            assert_eq!(total(rows_limited, SEEN), Value::Int(0), "{query}");
        }
        assert_budget(
            rows_limited(QueryRequest::new(
                "MATCH (s:Entity {id: 'a'}) UNWIND ['a', 'b'] AS key MATCH (m:Entity {id: key}) MERGE (s)-[:LINK]->(m) RETURN m.id AS id",
            ))
            .unwrap_err(),
            "MutationLimits.max_result_rows",
        );
        assert_eq!(
            total(
                rows_limited,
                "MATCH ()-[r:LINK]->() RETURN count(r) AS total"
            ),
            Value::Int(0)
        );
        // A count is one row within the one-row limit.
        assert_eq!(
            total(
                rows_limited,
                "MATCH (e:Entity) SET e.counted = true RETURN count(e) AS total"
            ),
            Value::Int(2)
        );
        assert_budget(
            payload_limited(QueryRequest::new(
                "MATCH (e:Entity {id: 'a'}) SET e.body = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' RETURN e.body AS body",
            ))
            .unwrap_err(),
            "MutationLimits.max_result_payload_bytes",
        );
        assert_eq!(
            total(
                payload_limited,
                "MATCH (e:Entity) WHERE e.body IS NOT NULL RETURN count(e) AS total"
            ),
            Value::Int(0)
        );
    }

    #[test]
    fn set_and_unwind_merge_return_limits_are_typed_on_every_execution_surface() {
        let (mut rows, mut payload) = (limited(1, 1 << 20), limited(16, 64));
        assert_set_and_merge_refusals(&mut |request| rows.query_request(request), &mut |request| {
            payload.query_request(request)
        });

        let (mut rows, mut payload) = (limited(1, 1 << 20), limited(16, 64));
        let mut rows_transaction = rows.begin_transaction().unwrap();
        let mut payload_transaction = payload.begin_transaction().unwrap();
        assert_set_and_merge_refusals(
            &mut |request| rows_transaction.query_request(request),
            &mut |request| payload_transaction.query_request(request),
        );
        rows_transaction.rollback();
        payload_transaction.rollback();

        let rows = ConcurrentDatabase::new(limited(1, 1 << 20));
        let payload = ConcurrentDatabase::new(limited(16, 64));
        assert_set_and_merge_refusals(&mut |request| rows.query_request(request), &mut |request| {
            payload.query_request(request)
        });
        let rows = ConcurrentDatabase::new(limited(1, 1 << 20));
        let payload = ConcurrentDatabase::new(limited(16, 64));
        let mut rows_transaction = rows
            .begin_transaction(ConcurrentTransactionOptions::default())
            .unwrap();
        let mut payload_transaction = payload
            .begin_transaction(ConcurrentTransactionOptions::default())
            .unwrap();
        assert_set_and_merge_refusals(
            &mut |request| rows_transaction.query_request(request),
            &mut |request| payload_transaction.query_request(request),
        );
        rows_transaction.commit().unwrap();
        payload_transaction.commit().unwrap();
    }

    #[test]
    fn mutation_return_result_limits_are_typed_on_every_execution_surface() {
        let (mut rows, mut payload) = (limited(1, 1 << 20), limited(16, 64));
        assert_typed_refusals(&mut |request| rows.query_request(request), &mut |request| {
            payload.query_request(request)
        });

        let (mut rows, mut payload) = (limited(1, 1 << 20), limited(16, 64));
        let mut rows_transaction = rows.begin_transaction().unwrap();
        let mut payload_transaction = payload.begin_transaction().unwrap();
        assert_typed_refusals(
            &mut |request| rows_transaction.query_request(request),
            &mut |request| payload_transaction.query_request(request),
        );
        rows_transaction.rollback();
        payload_transaction.rollback();

        let rows = ConcurrentDatabase::new(limited(1, 1 << 20));
        let payload = ConcurrentDatabase::new(limited(16, 64));
        assert_typed_refusals(&mut |request| rows.query_request(request), &mut |request| {
            payload.query_request(request)
        });
        let rows = ConcurrentDatabase::new(limited(1, 1 << 20));
        let payload = ConcurrentDatabase::new(limited(16, 64));
        let mut rows_transaction = rows
            .begin_transaction(ConcurrentTransactionOptions::default())
            .unwrap();
        let mut payload_transaction = payload
            .begin_transaction(ConcurrentTransactionOptions::default())
            .unwrap();
        assert_typed_refusals(
            &mut |request| rows_transaction.query_request(request),
            &mut |request| payload_transaction.query_request(request),
        );
        rows_transaction.commit().unwrap();
        payload_transaction.commit().unwrap();
    }

    #[test]
    fn merge_return_result_limit_is_typed_and_applies_no_on_match_update() {
        let mut db = limited(1, 1 << 20);
        db.query("CREATE (:Label {id: 'x'})").unwrap();
        let merge = "MATCH (e:Entity), (l:Label {id: 'x'}) MERGE (e)-[r:HAS_LABEL]->(l) ON CREATE SET r.seen = 0 ON MATCH SET r.seen = r.seen + 1 RETURN r.seen AS seen";
        // Two rows exceed max_result_rows 1, so the MERGE creates nothing.
        assert_budget(
            db.query(merge).unwrap_err(),
            "MutationLimits.max_result_rows",
        );
        assert_eq!(
            count(&mut db, "MATCH ()-[r]->() RETURN count(r) AS total"),
            0
        );
        assert_eq!(db.query(&format!("{merge} LIMIT 1")).unwrap().rows.len(), 1);
        assert_budget(
            db.query(merge).unwrap_err(),
            "MutationLimits.max_result_rows",
        );
        let seen = db
            .query("MATCH ()-[r:HAS_LABEL]->() RETURN sum(r.seen) AS total")
            .unwrap();
        assert_eq!(seen.rows[0]["total"], Value::Int(0));
        let node = "MERGE (n:Item {id: 'n'}) RETURN n.id AS id";
        assert_eq!(db.query(node).unwrap().rows.len(), 1);
    }
}
