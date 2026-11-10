// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `startNode(r)`/`endNode(r)` of a relationship value, and a relationship
//! value imported into a pattern, read the named relationship by id instead
//! of scanning its source's outgoing relationships (#1644). Each read is one
//! projected point read and no adjacency visit, however many relationships
//! the source has. The point read sees the statement's snapshot: resident
//! and checkpointed (canonical) records, committed but uncheckpointed
//! overlay records, records created or deleted earlier in the same
//! transaction, and canonical records shadowed by an update or a tombstone.

use super::*;
use hawdb_storage::store::{adjacency_visits_for_test, record_reads_for_test, RecordReadsForTest};

/// Outgoing LINK relationships of the hub.
const DEGREE: i64 = 3000;
const ENDPOINTS: &str = "UNWIND [$r] AS r RETURN startNode(r).id AS s, endNode(r).id AS e";
const IMPORTED: &str = "UNWIND [$r] AS r RETURN [(a)-[r]->(b) | [a.id, b.id]] AS ends";
const NO_RELATIONSHIP: &str = "names no stored relationship";

/// Hub 0 with a LINK to each of leaves 1..=DEGREE.
fn populate(db: &mut Database) {
    db.query("CREATE (:Hub {id: 0})").unwrap();
    let rows = (1..=DEGREE)
        .map(|id| Value::Map(BTreeMap::from([("id".to_string(), Value::Int(id))])))
        .collect();
    db.query_with_params(
        "UNWIND $rows AS row CREATE (:Leaf {id: row.id})",
        &BTreeMap::from([("rows".to_string(), Value::List(rows))]),
    )
    .unwrap();
    db.query("MATCH (h:Hub), (l:Leaf) CREATE (h)-[:LINK {w: 0}]->(l)")
        .unwrap();
}

fn open_out_of_core(path: &std::path::Path) -> Database {
    Database::open_with_config(
        path,
        DatabaseConfig {
            storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
            ..DatabaseConfig::default()
        },
    )
    .unwrap()
}

/// The hub, checkpointed and reopened out of core, so every record is read
/// from the canonical base.
fn checkpointed(name: &str) -> (Database, std::path::PathBuf) {
    let path = unique_test_dir(&format!(
        "relationship_value_point_reads_{name}_{}",
        std::process::id()
    ));
    {
        let mut db = Database::open(&path).unwrap();
        populate(&mut db);
        db.checkpoint().unwrap();
    }
    (open_out_of_core(&path), path)
}

fn link_query(leaf: i64) -> String {
    format!("MATCH (:Hub {{id: 0}})-[r:LINK]->(:Leaf {{id: {leaf}}}) RETURN r")
}

/// The LINK value to `leaf`.
fn link(db: &mut Database, leaf: i64) -> Value {
    let output = db.query(&link_query(leaf)).unwrap();
    assert_eq!(output.rows.len(), 1, "leaf {leaf}");
    output.rows[0]["r"].clone()
}

fn params(relationship: &Value) -> BTreeMap<String, Value> {
    BTreeMap::from([("r".to_string(), relationship.clone())])
}

fn ints(values: &[i64]) -> Vec<Value> {
    values.iter().copied().map(Value::Int).collect()
}

/// Rows of `query`, with the point reads and adjacency visits it started.
fn measure(
    query: impl FnOnce() -> Result<QueryOutput>,
) -> (Result<Vec<Vec<Value>>>, RecordReadsForTest, u64) {
    let reads = record_reads_for_test();
    let visits = adjacency_visits_for_test();
    let rows = query().map(|output| {
        output
            .rows
            .iter()
            .map(|row| row.values().cloned().collect())
            .collect()
    });
    (
        rows,
        record_reads_for_test().since(reads),
        adjacency_visits_for_test() - visits,
    )
}

/// `startNode`/`endNode` of `relationship` are `[source, target]`, read by
/// two projected point reads (one per call) without visiting adjacency.
fn assert_endpoints(
    run: impl FnOnce(&str, &BTreeMap<String, Value>) -> Result<QueryOutput>,
    relationship: &Value,
    source: i64,
    target: i64,
) {
    let (rows, reads, visits) = measure(|| run(ENDPOINTS, &params(relationship)));
    assert_eq!(rows.unwrap(), [ints(&[source, target])], "{relationship:?}");
    assert_eq!(visits, 0, "{relationship:?}");
    assert_eq!(reads.projected_relationships, 2, "{reads:?}");
    assert_eq!(reads.full_relationships, 0, "{reads:?}");
}

/// Both endpoint functions and a pattern importing `relationship` fail as
/// naming no stored relationship.
fn assert_absent(
    mut run: impl FnMut(&str, &BTreeMap<String, Value>) -> Result<QueryOutput>,
    relationship: &Value,
) {
    for query in [ENDPOINTS, "UNWIND [$r] AS r RETURN endNode(r) AS e"] {
        let error = run(query, &params(relationship)).unwrap_err().to_string();
        assert!(error.contains(NO_RELATIONSHIP), "{query}: {error}");
    }
    let error = run(IMPORTED, &params(relationship))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("bound relationship disappeared"),
        "{IMPORTED}: {error}"
    );
}

/// The value with `source_id` and `target_id` swapped.
fn reversed(relationship: &Value) -> Value {
    let Value::Map(fields) = relationship else {
        panic!("{relationship:?}")
    };
    let mut fields = fields.clone();
    let source = fields.remove("source_id").unwrap();
    let target = fields.insert("target_id".to_string(), source).unwrap();
    fields.insert("source_id".to_string(), target);
    Value::Map(fields)
}

/// The hub's last relationship is read directly, resident and from the
/// canonical base after a checkpoint; a pattern importing it reads it once.
#[test]
fn high_degree_source_reads_one_relationship() {
    let mut resident = Database::new();
    populate(&mut resident);
    let (canonical, path) = checkpointed("high_degree");
    for mut db in [resident, canonical] {
        for leaf in [1, DEGREE / 2, DEGREE] {
            let relationship = link(&mut db, leaf);
            assert_endpoints(
                |query, parameters| db.query_with_params(query, parameters),
                &relationship,
                0,
                leaf,
            );
            let (rows, reads, _) =
                measure(|| db.query_with_params(IMPORTED, &params(&relationship)));
            assert_eq!(
                rows.unwrap(),
                [vec![Value::List(vec![Value::List(ints(&[0, leaf]))])]]
            );
            // The import is one point read. Matching `(a)-[r]->(b)` with `r`
            // bound still expands the hub's relationships once; an adjacency
            // scan for the import would read up to as many again.
            assert_eq!(reads.full_relationships, DEGREE as u64 + 1, "{reads:?}");
            // Wrong endpoints and a guessed id name no relationship.
            let error = db
                .query_with_params(ENDPOINTS, &params(&reversed(&relationship)))
                .unwrap_err()
                .to_string();
            assert!(error.contains(NO_RELATIONSHIP), "{error}");
        }
    }
    let _ = std::fs::remove_dir_all(path);
}

/// Committed but uncheckpointed changes over the canonical base: a new
/// relationship, an updated canonical one and a deleted canonical one.
#[test]
fn uncheckpointed_overlay_relationships_are_read_from_the_overlay() {
    let (mut db, path) = checkpointed("overlay");
    let deleted = link(&mut db, 7);
    db.query("CREATE (:Leaf {id: 9000})").unwrap();
    db.query("MATCH (h:Hub), (l:Leaf {id: 9000}) CREATE (h)-[:LINK {w: 1}]->(l)")
        .unwrap();
    db.query("MATCH (:Hub)-[r:LINK]->(:Leaf {id: 5}) SET r.w = -5")
        .unwrap();
    db.query("MATCH (:Hub)-[r:LINK]->(:Leaf {id: 7}) DELETE r")
        .unwrap();
    for (leaf, target) in [(9000, 9000), (5, 5), (DEGREE, DEGREE)] {
        let relationship = link(&mut db, leaf);
        assert_endpoints(
            |query, parameters| db.query_with_params(query, parameters),
            &relationship,
            0,
            target,
        );
    }
    assert_absent(
        |query, parameters| db.query_with_params(query, parameters),
        &deleted,
    );
    let _ = std::fs::remove_dir_all(path);
}

/// A transaction reads its own creations and deletions; other statements
/// keep the committed snapshot.
#[test]
fn same_transaction_creations_and_deletions_are_visible() {
    let (mut db, path) = checkpointed("transaction");
    let canonical = link(&mut db, 3);
    let created = {
        let mut transaction = db.begin_transaction().unwrap();
        transaction.query("CREATE (:Leaf {id: 9001})").unwrap();
        transaction
            .query("MATCH (h:Hub), (l:Leaf {id: 9001}) CREATE (h)-[:LINK {w: 2}]->(l)")
            .unwrap();
        let created = transaction.query(&link_query(9001)).unwrap().rows[0]["r"].clone();
        assert_endpoints(
            |query, parameters| transaction.query_with_params(query, parameters),
            &created,
            0,
            9001,
        );
        let (rows, _, _) = measure(|| transaction.query_with_params(IMPORTED, &params(&created)));
        assert_eq!(
            rows.unwrap(),
            [vec![Value::List(vec![Value::List(ints(&[0, 9001]))])]]
        );
        transaction.rollback();
        created
    };
    // Rolled back, the created relationship names nothing.
    assert_absent(
        |query, parameters| db.query_with_params(query, parameters),
        &created,
    );
    // Deleted in a transaction: a canonical relationship (tombstone) and
    // one created earlier in the same transaction.
    for victim in ["canonical", "created"] {
        let mut transaction = db.begin_transaction().unwrap();
        let relationship = if victim == "canonical" {
            canonical.clone()
        } else {
            transaction.query("CREATE (:Leaf {id: 9002})").unwrap();
            transaction
                .query("MATCH (h:Hub), (l:Leaf {id: 9002}) CREATE (h)-[:LINK {w: 3}]->(l)")
                .unwrap();
            transaction.query(&link_query(9002)).unwrap().rows[0]["r"].clone()
        };
        transaction
            .query_with_params(
                "MATCH (h:Hub)-[r:LINK]->() WHERE id(r) = $id DELETE r",
                &BTreeMap::from([(
                    "id".to_string(),
                    match &relationship {
                        Value::Map(fields) => fields["_id"].clone(),
                        other => panic!("{other:?}"),
                    },
                )]),
            )
            .unwrap();
        assert_absent(
            |query, parameters| transaction.query_with_params(query, parameters),
            &relationship,
        );
        transaction.rollback();
    }
    // The canonical relationship survives the rolled-back deletion.
    assert_endpoints(
        |query, parameters| db.query_with_params(query, parameters),
        &canonical,
        0,
        3,
    );
    let _ = std::fs::remove_dir_all(path);
}
