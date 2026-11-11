// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Kùzu `label(x)` and openCypher `labels(n)` end to end (#1678): node labels
//! and relationship types in RETURN, WITH, WHERE (`IN $param`, literal lists,
//! comparisons), ORDER BY and nested expressions, NULL from OPTIONAL MATCH,
//! and the Mem identity-view statements, on resident and checkpointed
//! out-of-core stores.

use super::*;

/// `(m1:Memory)-[:MENTIONS]->(e1:Entity)-[:RELATES_TO {space_id: 's1'}]->(e2:Entity)`,
/// `(t1:Thread)-[:CONTAINS]->(m1)`, `(m1)-[:EVOLVES]->(m2:Memory)` and an
/// isolated `(p1:Person)`. `e2` carries a removed identity profile.
fn populate(db: &mut Database) {
    for statement in [
        "CREATE (:Memory {id: 'm1', title: 'first', space_id: 's1', importance: 0.7})",
        "CREATE (:Memory {id: 'm2', title: 'second', space_id: 's2'})",
        "CREATE (:Entity {id: 'e1', name: 'Ada', space_id: 's1', metadata: '{}'})",
        "CREATE (:Entity {id: 'e2', name: 'Bob', space_id: 's1', \
         metadata: '{\"identity_profile_removed\":{\"at\":1}}'})",
        "CREATE (:Thread {id: 't1', title: 'chat', space_id: 's1', message_count: 3})",
        "CREATE (:Person {id: 'p1', name: 'Pat'})",
        "MATCH (a:Memory {id: 'm1'}), (b:Entity {id: 'e1'}) \
         CREATE (a)-[:MENTIONS {strength: 0.9}]->(b)",
        "MATCH (a:Entity {id: 'e1'}), (b:Entity {id: 'e2'}) \
         CREATE (a)-[:RELATES_TO {space_id: 's1', relation_type: 'knows'}]->(b)",
        "MATCH (a:Thread {id: 't1'}), (b:Memory {id: 'm1'}) CREATE (a)-[:CONTAINS]->(b)",
        "MATCH (a:Memory {id: 'm1'}), (b:Memory {id: 'm2'}) CREATE (a)-[:EVOLVES]->(b)",
    ] {
        db.query(statement)
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
}

fn resident() -> Database {
    let mut db = Database::new();
    populate(&mut db);
    db
}

/// The same graph, checkpointed and reopened out of core, so every record is
/// read from the canonical base.
fn out_of_core(name: &str) -> (Database, std::path::PathBuf) {
    let path = unique_test_dir(&format!(
        "entity_label_functions_{name}_{}",
        std::process::id()
    ));
    {
        let mut db = Database::open(&path).unwrap();
        populate(&mut db);
        db.checkpoint().unwrap();
    }
    let db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    (db, path)
}

/// Runs `check` on the resident store and on the out-of-core store.
fn on_both_stores(name: &str, check: impl Fn(&mut Database)) {
    check(&mut resident());
    let (mut db, path) = out_of_core(name);
    check(&mut db);
    drop(db);
    let _ = std::fs::remove_dir_all(path);
}

fn strings(values: &[&str]) -> Value {
    Value::List(
        values
            .iter()
            .map(|value| Value::String((*value).into()))
            .collect(),
    )
}

fn params(entries: &[(&str, Value)]) -> BTreeMap<String, Value> {
    entries
        .iter()
        .map(|(name, value)| ((*name).to_string(), value.clone()))
        .collect()
}

/// Every row of `query` as its values in column order.
fn rows(db: &mut Database, query: &str, parameters: &BTreeMap<String, Value>) -> Vec<Vec<Value>> {
    let output = db
        .query_with_params(query, parameters)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    output
        .rows
        .iter()
        .map(|row| row.values().cloned().collect())
        .collect()
}

/// `rows`, with every value rendered as a short string for compact asserts.
fn table(db: &mut Database, query: &str, parameters: &BTreeMap<String, Value>) -> Vec<String> {
    rows(db, query, parameters)
        .into_iter()
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    Value::String(value) => value.to_string(),
                    Value::Null => "NULL".to_string(),
                    value => format!("{value:?}"),
                })
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect()
}

fn none() -> BTreeMap<String, Value> {
    BTreeMap::new()
}

#[test]
fn label_returns_node_labels_and_relationship_types() {
    on_both_stores("return", |db| {
        assert_eq!(
            table(
                db,
                "MATCH (b) RETURN b.id AS id, label(b) AS l, labels(b) AS ls ORDER BY id",
                &none()
            ),
            [
                "e1,Entity,List([String(\"Entity\")])",
                "e2,Entity,List([String(\"Entity\")])",
                "m1,Memory,List([String(\"Memory\")])",
                "m2,Memory,List([String(\"Memory\")])",
                "p1,Person,List([String(\"Person\")])",
                "t1,Thread,List([String(\"Thread\")])",
            ]
        );
        // The issue's probe: label(r) and label(b) in one RETURN.
        assert_eq!(
            table(
                db,
                "MATCH (a)-[r]->(b) RETURN a.id AS a, label(r) AS t, label(b) AS l ORDER BY a, t",
                &none()
            ),
            [
                "e1,RELATES_TO,Entity",
                "m1,EVOLVES,Memory",
                "m1,MENTIONS,Entity",
                "t1,CONTAINS,Memory",
            ]
        );
        // Case-insensitive names and nested expression positions.
        assert_eq!(
            table(
                db,
                "MATCH (b:Thread) RETURN LEFT(LABEL(b), 2) AS k, \
                 lower(label(b)) AS lo, size(labels(b)) AS n, \
                 coalesce(label(b), 'none') AS c, \
                 CASE WHEN label(b) = 'Thread' THEN 1 ELSE 0 END AS w",
                &none()
            ),
            ["Th,thread,Int(1),Thread,Int(1)"]
        );
    });
}

/// `label(r)` is the relationship type: it agrees with `type(r)` in every
/// direction and position, including as a filter.
#[test]
fn label_of_a_relationship_equals_type() {
    on_both_stores("type", |db| {
        let rows = rows(
            db,
            "MATCH (a)-[r]-(b) RETURN a.id AS a, b.id AS b, label(r) AS l, type(r) AS t",
            &none(),
        );
        assert_eq!(rows.len(), 8);
        for row in &rows {
            assert_eq!(row[2], row[3], "{row:?}");
            assert!(matches!(&row[2], Value::String(_)), "{row:?}");
        }
        let types = params(&[("types", strings(&["MENTIONS", "CONTAINS"]))]);
        let by_label = table(
            db,
            "MATCH (a)-[r]->(b) WHERE label(r) IN $types RETURN b.id AS id ORDER BY id",
            &types,
        );
        let by_type = table(
            db,
            "MATCH (a)-[r]->(b) WHERE type(r) IN $types RETURN b.id AS id ORDER BY id",
            &types,
        );
        assert_eq!(by_label, ["e1", "m1"]);
        assert_eq!(by_label, by_type);
        assert_eq!(
            table(
                db,
                "MATCH (a)-[r]->(b) WHERE type(r) = 'EVOLVES' RETURN b.id AS id",
                &none()
            ),
            ["m2"]
        );
    });
}

#[test]
fn label_filters_in_where_and_with() {
    on_both_stores("where", |db| {
        let labels = params(&[("labels", strings(&["Entity", "Thread"]))]);
        for query in [
            "MATCH (b) WHERE label(b) IN $labels RETURN b.id AS id ORDER BY id",
            "MATCH (b) WITH b, label(b) AS l WHERE l IN $labels RETURN b.id AS id ORDER BY id",
            "MATCH (b) WITH b WHERE label(b) IN $labels RETURN b.id AS id ORDER BY id",
            "MATCH (b) WHERE label(b) IN ['Entity', 'Thread'] RETURN b.id AS id ORDER BY id",
            "MATCH (b) WHERE label(b) = 'Entity' OR label(b) = 'Thread' \
             RETURN b.id AS id ORDER BY id",
            "MATCH (b) WHERE NOT label(b) IN ['Memory', 'Person'] RETURN b.id AS id ORDER BY id",
            "MATCH (b) WHERE 'Entity' IN labels(b) OR label(b) CONTAINS 'hre' \
             RETURN b.id AS id ORDER BY id",
        ] {
            assert_eq!(table(db, query, &labels), ["e1", "e2", "t1"], "{query}");
        }
        // The WITH column survives into later clauses.
        assert_eq!(
            table(
                db,
                "MATCH (b) WITH b.id AS id, label(b) AS l WHERE l <> 'Memory' \
                 RETURN l, count(*) AS n ORDER BY l",
                &none()
            ),
            ["Entity,Int(2)", "Person,Int(1)", "Thread,Int(1)"]
        );
        // No label matches, and an empty list.
        for labels in [strings(&["Nope"]), strings(&[])] {
            assert!(table(
                db,
                "MATCH (b) WHERE label(b) IN $labels RETURN b.id AS id",
                &params(&[("labels", labels)])
            )
            .is_empty());
        }
        // A NULL list parameter is refused at binding, as for a property.
        for query in [
            "MATCH (b) WHERE label(b) IN $labels RETURN b.id AS id",
            "MATCH (b) WHERE b.id IN $labels RETURN b.id AS id",
        ] {
            let error = db
                .query_with_params(query, &params(&[("labels", Value::Null)]))
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("IN predicate requires a list value, got Null"),
                "{query}: {error}"
            );
        }
        // Mixed with an id filter, as Mem's identity reads use it.
        assert_eq!(
            table(
                db,
                "MATCH (b) WHERE b.id IN $ids AND label(b) IN $labels \
                 RETURN b.id AS id, label(b) AS l ORDER BY id",
                &params(&[
                    ("ids", strings(&["e1", "m1", "p1", "t1"])),
                    ("labels", strings(&["Entity", "Memory"])),
                ])
            ),
            ["e1,Entity", "m1,Memory"]
        );
    });
}

#[test]
fn label_orders_rows() {
    on_both_stores("order", |db| {
        assert_eq!(
            table(
                db,
                "MATCH (b) RETURN b.id AS id ORDER BY label(b) DESC, b.id",
                &none()
            ),
            ["t1", "p1", "m1", "m2", "e1", "e2"]
        );
        assert_eq!(
            table(
                db,
                "MATCH (a)-[r]->(b) RETURN a.id AS a, b.id AS b ORDER BY label(r), label(b) LIMIT 2",
                &none()
            ),
            ["t1,m1", "m1,m2"]
        );
        assert_eq!(
            table(
                db,
                "MATCH (b) WITH b ORDER BY label(b), b.id LIMIT 3 RETURN b.id AS id",
                &none()
            ),
            ["e1", "e2", "m1"]
        );
    });
}

#[test]
fn label_of_null_is_null() {
    on_both_stores("null", |db| {
        assert_eq!(
            table(
                db,
                "MATCH (p:Person) OPTIONAL MATCH (p)-[r]->(c) \
                 RETURN p.id AS id, label(r) AS rl, type(r) AS t, label(c) AS cl, labels(c) AS cls",
                &none()
            ),
            ["p1,NULL,NULL,NULL,NULL"]
        );
        // An OPTIONAL row is filtered like any NULL comparison.
        assert_eq!(
            table(
                db,
                "MATCH (p:Person) OPTIONAL MATCH (p)-[r]->(c) WITH p, c \
                 WHERE label(c) IN ['Entity'] RETURN p.id AS id",
                &none()
            ),
            Vec::<String>::new()
        );
    });
}

#[test]
fn labels_of_a_relationship_and_unknown_variables_are_errors() {
    let mut db = resident();
    let error = db
        .query("MATCH ()-[r]->() RETURN labels(r) AS l")
        .unwrap_err()
        .to_string();
    assert!(error.contains("labels(r) requires a node"), "{error}");
    let error = db
        .query("MATCH (b) RETURN label(x) AS l")
        .unwrap_err()
        .to_string();
    assert!(error.contains("unknown variable 'x'"), "{error}");
}

/// The Mem identity-view statements (`nmem-graph` `identity_profiles.rs`
/// `graph_nodes`, `neighbor_rows` and `graph_edges`), verbatim.
#[test]
fn mem_identity_view_statements() {
    const NODES: &str = "MATCH (b)
         WHERE b.id IN $ids AND label(b) IN $labels
           AND (label(b) <> 'Entity' OR NOT contains(coalesce(b.metadata, ''), '\"identity_profile_removed\":{'))
         RETURN b.id, label(b),
                COALESCE(b.name, b.title, b.original_name, LEFT(b.content, 60), b.id),
                b.entity_type,
                LEFT(COALESCE(b.description, b.summary, b.content, ''), 200),
                COALESCE(b.pagerank_score, b.importance, b.confidence, 0.5),
                b.community_id, b.space_id, b.source, b.message_count,
                b.created_at, b.updated_at, b.aliases, b.metadata
         LIMIT 2001";
    const OUTGOING: &str = "MATCH (a)-[r]->(b)
             WHERE a.id IN $ids AND label(b) IN $labels
               AND (label(a) <> 'Entity' OR NOT contains(coalesce(a.metadata, ''), '\"identity_profile_removed\":{'))
               AND (label(b) <> 'Entity' OR NOT contains(coalesce(b.metadata, ''), '\"identity_profile_removed\":{'))
               AND ($space IS NULL OR b.space_id = $space)
             RETURN a.id, b.id, label(b), label(r) LIMIT 2001";
    const INCOMING: &str = "MATCH (b)-[r]->(a)
             WHERE a.id IN $ids AND label(b) IN $labels
               AND (label(a) <> 'Entity' OR NOT contains(coalesce(a.metadata, ''), '\"identity_profile_removed\":{'))
               AND (label(b) <> 'Entity' OR NOT contains(coalesce(b.metadata, ''), '\"identity_profile_removed\":{'))
               AND ($space IS NULL OR b.space_id = $space)
             RETURN a.id, b.id, label(b), label(r) LIMIT 2001";
    const EDGES: &str = "MATCH (a)-[r]->(b)
         WHERE a.id IN $ids AND b.id IN $ids
           AND ($space IS NULL OR (a.space_id = $space AND b.space_id = $space
                AND (label(r) <> 'RELATES_TO' OR r.space_id = $space)))
         RETURN a.id, b.id, label(r), COALESCE(r.strength, r.confidence, 0.5),
                r.relation_type, r.space_id
         ORDER BY a.id, b.id, label(r) LIMIT 4001";
    let labels = strings(&["Memory", "Entity", "Thread", "Source", "Skill"]);
    let sorted = |mut rows: Vec<String>| {
        rows.sort();
        rows
    };
    on_both_stores("mem", |db| {
        let ids = strings(&["m1", "m2", "e1", "e2", "t1", "p1"]);
        let nodes = rows(
            db,
            NODES,
            &params(&[("ids", ids.clone()), ("labels", labels.clone())]),
        );
        // e2's profile is removed and p1 is not a graph label.
        let mut ids_labels = nodes
            .iter()
            .map(|row| format!("{:?}/{:?}", row[0], row[1]))
            .collect::<Vec<_>>();
        ids_labels.sort();
        assert_eq!(
            ids_labels,
            [
                "String(\"e1\")/String(\"Entity\")",
                "String(\"m1\")/String(\"Memory\")",
                "String(\"m2\")/String(\"Memory\")",
                "String(\"t1\")/String(\"Thread\")",
            ]
        );
        let m1 = nodes
            .iter()
            .find(|row| row[0] == Value::String("m1".into()))
            .unwrap();
        assert_eq!(m1.len(), 14);
        assert_eq!(m1[2], Value::String("first".into()));
        assert_eq!(m1[5], Value::Float(0.7));

        let neighbors = |db: &mut Database, query: &str, space: Value| {
            sorted(table(
                db,
                query,
                &params(&[
                    ("ids", strings(&["m1", "e1"])),
                    ("labels", labels.clone()),
                    ("space", space),
                ]),
            ))
        };
        assert_eq!(
            neighbors(db, OUTGOING, Value::Null),
            ["m1,e1,Entity,MENTIONS", "m1,m2,Memory,EVOLVES"]
        );
        assert_eq!(
            neighbors(db, OUTGOING, Value::String("s1".into())),
            ["m1,e1,Entity,MENTIONS"]
        );
        assert_eq!(
            neighbors(db, INCOMING, Value::Null),
            ["e1,m1,Memory,MENTIONS", "m1,t1,Thread,CONTAINS"]
        );

        let edges = |db: &mut Database, space: Value| {
            table(
                db,
                EDGES,
                &params(&[
                    ("ids", strings(&["m1", "m2", "e1", "e2", "t1"])),
                    ("space", space),
                ]),
            )
        };
        assert_eq!(
            edges(db, Value::Null),
            [
                "e1,e2,RELATES_TO,Float(0.5),knows,s1",
                "m1,e1,MENTIONS,Float(0.9),NULL,NULL",
                "m1,m2,EVOLVES,Float(0.5),NULL,NULL",
                "t1,m1,CONTAINS,Float(0.5),NULL,NULL",
            ]
        );
        assert_eq!(
            edges(db, Value::String("s1".into())),
            [
                "e1,e2,RELATES_TO,Float(0.5),knows,s1",
                "m1,e1,MENTIONS,Float(0.9),NULL,NULL",
                "t1,m1,CONTAINS,Float(0.5),NULL,NULL",
            ]
        );
    });
}

/// A cached plan for a label filter rebinds its parameters: the second
/// execution of the same text sees the new list.
#[test]
fn cached_label_filters_rebind_parameters() {
    let mut db = resident();
    const QUERY: &str = "MATCH (b) WHERE label(b) IN $labels RETURN b.id AS id ORDER BY id";
    for (labels, expected) in [
        (strings(&["Thread"]), vec!["t1"]),
        (strings(&["Memory"]), vec!["m1", "m2"]),
        (strings(&["Person"]), vec!["p1"]),
        (strings(&["Memory", "Person"]), vec!["m1", "m2", "p1"]),
    ] {
        assert_eq!(
            table(&mut db, QUERY, &params(&[("labels", labels)])),
            expected
        );
    }
    // Lists of one length share one cached template.
    let stats = db.plan_cache_stats().unwrap();
    assert!(stats.hits >= 2, "{stats:?}");
}

#[cfg(feature = "acl")]
mod access_control {
    use super::*;
    use crate::{QueryAccessControlContext, RuntimeCapabilities, RuntimeCapability};

    /// `(m1:Memory)-[:MENTIONS]->(e1:Entity hidden)` and
    /// `(m1)-[:EVOLVES]->(m2:Memory)`, with `space_id` 'public' except on
    /// `e1`, plus a hidden `(s1:Secret)` that no visible node reaches.
    fn graph() -> Database {
        let mut db = Database::new_with_config(DatabaseConfig {
            runtime_capabilities: RuntimeCapabilities::default()
                .with(RuntimeCapability::AccessControl, true),
            ..DatabaseConfig::default()
        });
        for statement in [
            "CREATE (:Memory {id: 'm1', space_id: 'public'})",
            "CREATE (:Memory {id: 'm2', space_id: 'public'})",
            "CREATE (:Entity {id: 'e1', space_id: 'hidden'})",
            "CREATE (:Secret {id: 's1', space_id: 'hidden'})",
            "MATCH (a:Memory {id: 'm1'}), (b:Entity {id: 'e1'}) CREATE (a)-[:MENTIONS]->(b)",
            "MATCH (a:Memory {id: 'm1'}), (b:Memory {id: 'm2'}) CREATE (a)-[:EVOLVES]->(b)",
        ] {
            db.query(statement).unwrap();
        }
        db
    }

    fn scoped(db: &mut Database, query: &str, parameters: &BTreeMap<String, Value>) -> Vec<String> {
        let output = db
            .query_with_params_access_control(
                query,
                parameters,
                QueryAccessControlContext::visibility_scope(7, "space_id", "public"),
            )
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        output
            .rows
            .iter()
            .map(|row| {
                row.values()
                    .map(|value| match value {
                        Value::String(value) => value.to_string(),
                        Value::Null => "NULL".to_string(),
                        value => format!("{value:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect()
    }

    #[test]
    fn label_never_reveals_a_hidden_node() {
        let mut db = graph();
        let labels = params(&[("labels", strings(&["Memory", "Entity", "Secret"]))]);
        // The hidden nodes are never bound, so their labels never appear.
        assert_eq!(
            scoped(
                &mut db,
                "MATCH (b) WHERE label(b) IN $labels RETURN b.id AS id, label(b) AS l, labels(b) AS ls ORDER BY id",
                &labels
            ),
            [
                "m1,Memory,List([String(\"Memory\")])",
                "m2,Memory,List([String(\"Memory\")])"
            ]
        );
        assert_eq!(
            scoped(
                &mut db,
                "MATCH (b) RETURN label(b) AS l, count(*) AS n ORDER BY l",
                &none()
            ),
            ["Memory,Int(2)"]
        );
        // The Mem neighbor read never lists the hidden Entity or its edge.
        assert_eq!(
            scoped(
                &mut db,
                "MATCH (a)-[r]->(b) WHERE a.id IN $ids AND label(b) IN $labels \
                 RETURN a.id, b.id, label(b), label(r)",
                &params(&[
                    ("ids", strings(&["m1", "e1"])),
                    ("labels", strings(&["Memory", "Entity"])),
                ])
            ),
            ["m1,m2,Memory,EVOLVES"]
        );
        // An OPTIONAL MATCH that reaches only the hidden node never reads its
        // label or its relationship's type. The labelled read keeps exactly
        // the rows of the plain projection (today the scoped OPTIONAL MATCH
        // drops the row, without label() too).
        let optional = |columns: &str| {
            format!(
                "MATCH (a:Memory {{id: 'm1'}}) OPTIONAL MATCH (a)-[r:MENTIONS]->(b) RETURN {columns}"
            )
        };
        let plain = scoped(&mut db, &optional("a.id, b.id"), &none());
        let labelled = scoped(
            &mut db,
            &optional("a.id, label(r), label(b), labels(b)"),
            &none(),
        );
        assert_eq!(labelled.len(), plain.len(), "{labelled:?} {plain:?}");
        assert!(
            labelled.iter().all(|row| row == "m1,NULL,NULL,NULL"),
            "{labelled:?}"
        );
        // Without a policy, the same reads see every node.
        assert_eq!(
            table(
                &mut db,
                "MATCH (b) WHERE label(b) IN $labels RETURN b.id AS id ORDER BY id",
                &labels
            ),
            ["e1", "m1", "m2", "s1"]
        );
    }
}

/// A single-node read that needs only `id` and labels decodes only the
/// properties it names (`NodeProjectionScanExec`), as `id(n)` does.
#[test]
fn single_node_label_reads_use_the_node_projection_scan() {
    let mut db = resident();
    for (query, parameters) in [
        ("MATCH (b) RETURN b.id AS id, label(b) AS l", none()),
        (
            "MATCH (b) WHERE label(b) IN $labels RETURN b.id AS id, labels(b) AS ls",
            params(&[("labels", strings(&["Entity"]))]),
        ),
    ] {
        let plan = db
            .explain_query_with_params(query, &parameters)
            .unwrap()
            .physical_plan
            .explain(0);
        assert!(plan.contains("NodeProjectionScanExec"), "{query}:\n{plan}");
    }
    assert_eq!(
        table(
            &mut db,
            "MATCH (b) WHERE label(b) IN $labels RETURN b.id AS id, labels(b) AS ls ORDER BY id",
            &params(&[("labels", strings(&["Entity"]))])
        ),
        [
            "e1,List([String(\"Entity\")])",
            "e2,List([String(\"Entity\")])"
        ]
    );
}
