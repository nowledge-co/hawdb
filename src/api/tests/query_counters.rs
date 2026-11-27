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

//! Cypher write statement counters (`QueryOutput::counters`). Counters count
//! executed write operations, not changed data, so every case runs against an
//! in-memory database and a durable out-of-core database whose seed nodes
//! were checkpointed into the canonical base, with identical expectations.

use super::*;
use crate::QueryCounters;
use hawdb_storage::config::StorageResidencyMode;
use hawdb_storage::mutation::{GraphMutation, MutationLimits, PropertyFilter};

const SEED: &[&str] = &[
    "CREATE (:A {id: 1, x: 1, y: 2})",
    "CREATE (:A {id: 2})",
    "CREATE (:B {id: 3})",
    "MATCH (a:A {id: 2}), (b:B {id: 3}) CREATE (a)-[:R {w: 1}]->(b)",
    "MATCH (a:A {id: 2}), (b:A {id: 1}) CREATE (a)-[:R {w: 2}]->(b)",
];

/// Runs `check` on a seeded in-memory database and on a seeded durable
/// out-of-core database after a checkpoint made the seed canonical.
fn on_both(name: &str, check: impl Fn(&mut Database)) {
    let mut memory = Database::new();
    seed(&mut memory);
    check(&mut memory);

    let path = unique_test_dir(&format!("query_counters_{name}"));
    {
        let mut durable = Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: StorageResidencyMode::OutOfCore,
                ..Default::default()
            },
        )
        .unwrap();
        seed(&mut durable);
        durable.checkpoint().unwrap();
        assert!(durable.storage_residency_report().unwrap().out_of_core);
        check(&mut durable);
    }
    std::fs::remove_dir_all(&path).unwrap();
}

fn seed(db: &mut Database) {
    for statement in SEED {
        db.query(statement).unwrap();
    }
}

fn counters(db: &mut Database, query: &str) -> QueryCounters {
    db.query(query).unwrap().counters
}

fn x_of_node_one(db: &mut Database) -> Value {
    db.query("MATCH (n:A {id: 1}) RETURN n.x AS x")
        .unwrap()
        .rows
        .value(0, 0)
        .expect("node 1 exists")
        .to_owned_value()
}

#[test]
fn same_value_set_counts_one_property() {
    on_both("same_value_set", |db| {
        assert_eq!(x_of_node_one(db), Value::Int(1));
        let counters = counters(db, "MATCH (n:A {id: 1}) SET n.x = 1");
        assert_eq!(
            counters,
            QueryCounters {
                properties_set: 1,
                ..QueryCounters::default()
            }
        );
        assert!(counters.contains_updates());
        assert_eq!(x_of_node_one(db), Value::Int(1));
    });
}

#[test]
fn setting_one_property_twice_counts_two() {
    on_both("duplicate_set", |db| {
        let counters = counters(db, "MATCH (n:A {id: 1}) SET n.x = 1, n.x = 2");
        assert_eq!(counters.properties_set, 2);
        assert_eq!(x_of_node_one(db), Value::Int(2));
    });
}

#[test]
fn setting_a_property_to_null_counts_as_a_set() {
    on_both("set_null", |db| {
        let counters = counters(db, "MATCH (n:A {id: 1}) SET n.x = null");
        assert_eq!(
            counters,
            QueryCounters {
                properties_set: 1,
                ..QueryCounters::default()
            }
        );
        assert_eq!(x_of_node_one(db), Value::Null);
    });
}

#[test]
fn create_counts_its_node_label_and_each_given_property() {
    on_both("create", |db| {
        assert_eq!(
            counters(db, "CREATE (:A {x: 1, y: 2})"),
            QueryCounters {
                nodes_created: 1,
                labels_added: 1,
                properties_set: 2,
                ..QueryCounters::default()
            }
        );
        assert_eq!(
            counters(db, "CREATE (:C {id: 10})-[:S {w: 1}]->(:D {id: 11})"),
            QueryCounters {
                nodes_created: 2,
                labels_added: 2,
                relationships_created: 1,
                properties_set: 3,
                ..QueryCounters::default()
            }
        );
        assert_eq!(
            counters(
                db,
                "MATCH (a:A {id: 1}), (b:B {id: 3}) CREATE (a)-[:R {w: 5, v: 6}]->(b)"
            ),
            QueryCounters {
                relationships_created: 1,
                properties_set: 2,
                ..QueryCounters::default()
            }
        );
    });
}

fn rows(rows: &[(i64, i64)]) -> BTreeMap<String, Value> {
    let rows = rows
        .iter()
        .map(|(k, v)| {
            Value::Map(BTreeMap::from([
                ("k".to_string(), Value::Int(*k)),
                ("v".to_string(), Value::Int(*v)),
            ]))
        })
        .collect();
    BTreeMap::from([("rows".to_string(), Value::List(rows))])
}

#[test]
fn declared_defaults_are_not_counted_as_given_properties() {
    on_both("defaults", |db| {
        db.query("CREATE PROPERTY ON NODE TABLE E(p) TYPE INT DEFAULT 7")
            .unwrap();
        assert_eq!(
            counters(db, "CREATE (:E {id: 1})"),
            QueryCounters {
                nodes_created: 1,
                labels_added: 1,
                properties_set: 1,
                ..QueryCounters::default()
            }
        );
        let p = db
            .query("MATCH (n:E {id: 1}) RETURN n.p AS p")
            .unwrap()
            .rows
            .value(0, 0)
            .unwrap()
            .to_owned_value();
        assert_eq!(p, Value::Int(7));
    });
}

#[test]
fn row_batches_count_every_executed_write() {
    on_both("unwind", |db| {
        let output = db
            .query_with_params(
                "UNWIND $rows AS r CREATE (:U {k: r.k, v: r.v})",
                &rows(&[(5, 1), (5, 2), (6, 3)]),
            )
            .unwrap();
        assert_eq!(
            output.counters,
            QueryCounters {
                nodes_created: 3,
                labels_added: 3,
                properties_set: 6,
                ..QueryCounters::default()
            }
        );
        // Two rows each match both `k = 5` nodes: four executed writes, even
        // though only one value per node and property is committed.
        let output = db
            .query_with_params(
                "UNWIND $rows AS r MATCH (n:U {k: r.k}) SET n.v = r.v",
                &rows(&[(5, 7), (5, 8)]),
            )
            .unwrap();
        assert_eq!(
            output.counters,
            QueryCounters {
                properties_set: 4,
                ..QueryCounters::default()
            }
        );
    });
}

#[test]
fn row_evaluated_set_of_one_property_twice_counts_two() {
    on_both("row_evaluated_duplicate_set", |db| {
        let counters = counters(db, "MATCH (n:A {id: 1}) SET n.x = n.x + 1, n.x = n.x + 1");
        assert_eq!(
            counters,
            QueryCounters {
                properties_set: 2,
                ..QueryCounters::default()
            }
        );
    });
}

#[test]
fn relationship_property_sets_count() {
    on_both("relationship_set", |db| {
        assert_eq!(
            counters(db, "MATCH (:A {id: 2})-[r:R]->(:B {id: 3}) SET r.w = 1"),
            QueryCounters {
                properties_set: 1,
                ..QueryCounters::default()
            }
        );
        assert_eq!(
            counters(db, "MATCH (:A {id: 2})-[r:R]->() SET r.w = 7, r.v = 8").properties_set,
            4
        );
    });
}

#[test]
fn set_with_return_counts_like_set_without() {
    on_both("set_return", |db| {
        let output = db
            .query("MATCH (n:A {id: 1}) SET n.x = 1 RETURN n.x AS x")
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.counters.properties_set, 1);
    });
}

#[test]
fn detach_delete_counts_the_relationships_it_deletes() {
    on_both("detach_delete", |db| {
        assert_eq!(
            counters(db, "MATCH (n:A {id: 2}) DETACH DELETE n"),
            QueryCounters {
                nodes_deleted: 1,
                relationships_deleted: 2,
                ..QueryCounters::default()
            }
        );
        assert_eq!(
            counters(db, "MATCH (n:B {id: 3}) DELETE n"),
            QueryCounters {
                nodes_deleted: 1,
                ..QueryCounters::default()
            }
        );
    });
}

#[test]
fn relationship_delete_counts() {
    on_both("relationship_delete", |db| {
        assert_eq!(
            counters(db, "MATCH ()-[r:R]->() DELETE r"),
            QueryCounters {
                relationships_deleted: 2,
                ..QueryCounters::default()
            }
        );
    });
}

#[test]
fn merge_that_matches_counts_only_on_match_set() {
    on_both("merge_match", |db| {
        let output = db
            .query("MERGE (n:A {id: 1}) ON CREATE SET n.c = 1 ON MATCH SET n.m = 1")
            .unwrap();
        assert_eq!(
            output.counters,
            QueryCounters {
                properties_set: 1,
                ..QueryCounters::default()
            }
        );
        // A match without ON MATCH SET writes nothing: `properties_set == 0`
        // does not mean the statement matched no node.
        let output = db.query("MERGE (n:A {id: 1})").unwrap();
        assert_eq!(output.rows.len(), 1);
        assert!(!output.counters.contains_updates());
    });
}

#[test]
fn merge_that_creates_counts_pattern_and_on_create_set_properties() {
    on_both("merge_create", |db| {
        assert_eq!(
            counters(
                db,
                "MERGE (n:A {id: 9}) ON CREATE SET n.c = 1, n.d = 2 ON MATCH SET n.m = 1"
            ),
            QueryCounters {
                nodes_created: 1,
                labels_added: 1,
                properties_set: 3,
                ..QueryCounters::default()
            }
        );
        // The second row matches the node the first row created in the same
        // batch: only the first row creates, and each row's ON CREATE SET is
        // counted only where it ran.
        let output = db
            .query_with_params(
                "UNWIND $rows AS r MERGE (n:M {k: r.k}) ON CREATE SET n.v = r.v",
                &rows(&[(5, 1), (5, 2)]),
            )
            .unwrap();
        assert_eq!(
            output.counters,
            QueryCounters {
                nodes_created: 1,
                labels_added: 1,
                properties_set: 2,
                ..QueryCounters::default()
            }
        );
    });
}

#[test]
fn creating_an_existing_index_or_constraint_counts_nothing() {
    on_both("schema", |db| {
        let created = QueryCounters {
            indexes_added: 1,
            ..QueryCounters::default()
        };
        assert_eq!(counters(db, "CREATE INDEX ON :A(x)"), created);
        assert_eq!(
            counters(db, "CREATE INDEX ON :A(x)"),
            QueryCounters::default()
        );
        assert_eq!(counters(db, "CREATE RANGE INDEX ON :A(y)"), created);
        assert_eq!(
            counters(db, "CREATE RANGE INDEX ON :A(y)"),
            QueryCounters::default()
        );
        let created = QueryCounters {
            constraints_added: 1,
            ..QueryCounters::default()
        };
        assert_eq!(
            counters(db, "CREATE CONSTRAINT ON :A(id) ASSERT UNIQUE"),
            created
        );
        assert_eq!(
            counters(db, "CREATE CONSTRAINT ON :A(id) ASSERT UNIQUE"),
            QueryCounters::default()
        );
    });
}

#[test]
fn set_on_a_node_that_does_not_exist_counts_nothing() {
    on_both("missing_node", |db| {
        for query in [
            "MATCH (n) WHERE id(n) = 999999 SET n.x = 1",
            "MATCH (n:A {id: 424242}) SET n.x = 1",
            "MATCH (n:Missing) SET n.x = 1",
            "MATCH (n:A {id: 424242}) DETACH DELETE n",
        ] {
            let output = db.query(query).unwrap();
            assert_eq!(output.counters, QueryCounters::default(), "{query}");
            assert!(!output.counters.contains_updates());
        }
        assert_eq!(x_of_node_one(db), Value::Int(1));
    });
}

#[test]
fn read_only_statements_count_nothing() {
    on_both("read_only", |db| {
        for query in [
            "MATCH (n:A) RETURN n.id AS id",
            "MATCH (a)-[r:R]->(b) RETURN count(r) AS c",
            "MATCH (n:A {id: 1}) RETURN n.x AS x",
        ] {
            assert_eq!(counters(db, query), QueryCounters::default(), "{query}");
        }
    });
}

#[test]
fn transaction_statements_report_only_their_own_counters() {
    on_both("transaction", |db| {
        let mut transaction = db.begin_transaction().unwrap();
        assert_eq!(
            transaction
                .query("CREATE (:T {id: 1, v: 1})")
                .unwrap()
                .counters,
            QueryCounters {
                nodes_created: 1,
                labels_added: 1,
                properties_set: 2,
                ..QueryCounters::default()
            }
        );
        assert_eq!(
            transaction
                .query("MATCH (n:T {id: 1}) SET n.v = 1")
                .unwrap()
                .counters,
            QueryCounters {
                properties_set: 1,
                ..QueryCounters::default()
            }
        );
        assert_eq!(
            transaction
                .query("MATCH (n:A {id: 2}) DETACH DELETE n")
                .unwrap()
                .counters,
            QueryCounters {
                nodes_deleted: 1,
                relationships_deleted: 2,
                ..QueryCounters::default()
            }
        );
        assert_eq!(
            transaction
                .query("MATCH (n:T) RETURN n.v AS v")
                .unwrap()
                .counters,
            QueryCounters::default()
        );
        transaction.commit().unwrap();
        assert_eq!(
            db.query("MATCH (n:T {id: 1}) RETURN n.v AS v")
                .unwrap()
                .rows
                .len(),
            1
        );
    });
}

#[test]
fn failed_statements_return_errors_and_later_statements_count_alone() {
    on_both("failure", |db| {
        db.query("CREATE CONSTRAINT ON :A(id) ASSERT UNIQUE")
            .unwrap();
        assert!(db.query("CREATE (:A {id: 1})").is_err());
        assert_eq!(
            counters(db, "CREATE (:A {id: 100})"),
            QueryCounters {
                nodes_created: 1,
                labels_added: 1,
                properties_set: 1,
                ..QueryCounters::default()
            }
        );

        let mut transaction = db.begin_transaction().unwrap();
        transaction
            .query("MATCH (n:A {id: 1}) SET n.x = 3")
            .unwrap();
        assert!(transaction.query("CREATE (:A {id: 2})").is_err());
        assert_eq!(
            transaction
                .query("MATCH (n:A {id: 1}) SET n.y = 3")
                .unwrap()
                .counters,
            QueryCounters {
                properties_set: 1,
                ..QueryCounters::default()
            }
        );
        transaction.rollback();
    });
}

/// `SET n += {}` and label writes are not part of the Cypher dialect yet;
/// they fail as statements and carry no counters (see `QueryCounters`). The
/// mutation layer's empty assignment list, which an empty-map SET would
/// produce, matches the node and counts no property.
#[test]
fn empty_property_map_counts_nothing_and_label_writes_are_rejected() {
    on_both("empty_map", |db| {
        for query in [
            "MATCH (n:A {id: 1}) SET n += {}",
            "MATCH (n:A {id: 1}) SET n:A",
            "MATCH (n:A {id: 1}) SET n:L",
            "MATCH (n:A {id: 1}) REMOVE n:A",
        ] {
            assert!(
                matches!(db.query(query), Err(HawDBError::Parse(_))),
                "{query}"
            );
        }
        let mut runtime = db.runtime.get_mut().unwrap();
        let runtime = &mut *runtime;
        let summary = runtime
            .store
            .commit_mutations_with_limits(
                &mut runtime.catalog,
                vec![GraphMutation::SetNodeProperties {
                    label: "A".into(),
                    filter: Some(PropertyFilter::Eq {
                        property: "id".into(),
                        value: Value::Int(1),
                    }),
                    assignments: Vec::new(),
                }],
                MutationLimits::default(),
            )
            .unwrap();
        assert_eq!(summary.rows.len(), 1, "the empty SET still matches node 1");
        assert_eq!(summary.counters, QueryCounters::default());
    });
}

/// The concurrent handle commits autocommit writes through the transaction
/// kernel; its outputs carry the same per-statement counters.
#[test]
fn concurrent_database_statements_carry_counters() {
    let mut database = Database::new();
    seed(&mut database);
    let db = crate::ConcurrentDatabase::new(database);
    assert_eq!(
        db.query("MATCH (n:A {id: 1}) SET n.x = 1")
            .unwrap()
            .counters,
        QueryCounters {
            properties_set: 1,
            ..QueryCounters::default()
        }
    );
    assert_eq!(
        db.query("MATCH (n:A {id: 2}) DETACH DELETE n")
            .unwrap()
            .counters,
        QueryCounters {
            nodes_deleted: 1,
            relationships_deleted: 2,
            ..QueryCounters::default()
        }
    );
    let mut transaction = db.begin_transaction(Default::default()).unwrap();
    assert_eq!(
        transaction
            .query("CREATE (:T {id: 1})")
            .unwrap()
            .counters
            .nodes_created,
        1
    );
    assert_eq!(
        transaction
            .query("MATCH (n:T {id: 1}) SET n.v = 2")
            .unwrap()
            .counters,
        QueryCounters {
            properties_set: 1,
            ..QueryCounters::default()
        }
    );
    transaction.commit().unwrap();
    assert_eq!(
        db.query("MATCH (n:T) RETURN n.v AS v").unwrap().counters,
        QueryCounters::default()
    );
}

/// One batch that creates a node pair with a relationship and then detach
/// deletes one of the new nodes counts every executed write, although the
/// deleted records never reach the write-ahead log.
#[test]
fn batch_that_creates_then_deletes_counts_both() {
    on_both("create_then_delete", |db| {
        let summary = {
            let mut guard = db.runtime.get_mut().unwrap();
            let runtime = &mut *guard;
            runtime
                .store
                .commit_mutations_with_limits(
                    &mut runtime.catalog,
                    vec![
                        GraphMutation::MergeConnectedNodes(
                            hawdb_storage::mutation::ConnectedNodesCreate {
                                source_label: "P".into(),
                                source_properties: BTreeMap::from([("id".into(), Value::Int(9))]),
                                rel_type: "PR".into(),
                                rel_properties: BTreeMap::new(),
                                target_label: "Q".into(),
                                target_properties: BTreeMap::from([("id".into(), Value::Int(10))]),
                            },
                        ),
                        GraphMutation::DeleteNode {
                            label: "P".into(),
                            filter: Some(PropertyFilter::Eq {
                                property: "id".into(),
                                value: Value::Int(9),
                            }),
                            detach: true,
                        },
                    ],
                    MutationLimits::default(),
                )
                .unwrap()
        };
        let counters = summary.counters;
        assert_eq!(
            (
                counters.nodes_created,
                counters.nodes_deleted,
                counters.relationships_created,
                counters.relationships_deleted,
            ),
            (2, 1, 1, 1),
            "{counters:?}"
        );
        assert_eq!(
            db.query("MATCH (n:P) RETURN count(n) AS c")
                .unwrap()
                .rows
                .value(0, 0)
                .unwrap()
                .to_owned_value(),
            Value::Int(0)
        );
        assert_eq!(
            db.query("MATCH (n:Q) RETURN count(n) AS c")
                .unwrap()
                .rows
                .value(0, 0)
                .unwrap()
                .to_owned_value(),
            Value::Int(1)
        );
    });
}

/// SQL writes are not graph writes: their outputs report all-zero counters.
#[test]
fn sql_statements_report_zero_counters() {
    on_both("sql", |db| {
        for statement in [
            "CREATE TABLE records (id BIGINT PRIMARY KEY, value BIGINT)",
            "INSERT INTO records VALUES (1, 1)",
            "UPDATE records SET value = 2 WHERE id = 1",
            "DELETE FROM records WHERE id = 1",
        ] {
            assert_eq!(
                db.query_sql(statement).unwrap().counters,
                QueryCounters::default(),
                "{statement}"
            );
        }
    });
}
