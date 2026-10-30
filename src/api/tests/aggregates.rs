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

#[test]
fn scoped_community_aggregate_accepts_case_membership() {
    let mut db = Database::new();
    for query in [
        "CREATE (:Memory {id: 'm1'})-[:MENTIONS]->(:Entity {community_id: 'alpha'})",
        "CREATE (:Memory {id: 'm2', space_id: ''})-[:MENTIONS]->(:Entity {community_id: 'alpha'})",
        "CREATE (:Memory {id: 'm3', space_id: 'team'})-[:MENTIONS]->(:Entity {community_id: 'beta'})",
        "CREATE (:Community {community_id: 'alpha', name: 'Alpha'})",
        "CREATE (:Community {community_id: 'beta', name: 'Beta'})",
    ] {
        db.query(query).unwrap();
    }
    let query = "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE (CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN $default_space ELSE m.space_id END) IN $space_ids AND e.community_id IS NOT NULL WITH e.community_id AS community_id, COUNT(DISTINCT m) AS members OPTIONAL MATCH (c:Community) WHERE c.community_id = community_id RETURN c.name AS name, members ORDER BY members DESC, community_id ASC LIMIT $limit";
    let params = BTreeMap::from([
        ("default_space".into(), Value::String("default".into())),
        (
            "space_ids".into(),
            Value::List(vec![Value::String("default".into())]),
        ),
        ("limit".into(), Value::Int(5)),
    ]);
    let result = db.query_with_params(query, &params).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].get("name"),
        Some(&Value::String("Alpha".into()))
    );
    assert_eq!(result.rows[0].get("members"), Some(&Value::Int(2)));
}

#[test]
fn with_window_precedes_count_of_groups() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {kind: 'a'})").unwrap();
    db.query("CREATE (:Entity {kind: 'b'})").unwrap();
    for offset in 0..=3 {
        for limit in 0..=3 {
            let output = db.query_with_params(
                "MATCH (e:Entity) WITH e.kind AS kind, COUNT(*) AS n SKIP $offset LIMIT $limit RETURN COUNT(*) AS groups",
                &BTreeMap::from([
                    ("offset".into(), Value::Int(offset)),
                    ("limit".into(), Value::Int(limit)),
                ]),
            ).unwrap();
            assert_eq!(output.rows.len(), 1, "offset={offset}, limit={limit}");
            assert_eq!(
                output.rows[0].get("groups"),
                Some(&Value::Int((2_i64 - offset).max(0).min(limit)))
            );
            let after_count = db.query_with_params(
                "MATCH (e:Entity) WITH e.kind AS kind, COUNT(*) AS n RETURN COUNT(*) AS groups SKIP $offset LIMIT $limit",
                &BTreeMap::from([
                    ("offset".into(), Value::Int(offset)),
                    ("limit".into(), Value::Int(limit)),
                ]),
            ).unwrap();
            assert_eq!(
                after_count.rows.len(),
                usize::from(offset == 0 && limit > 0)
            );
            if let Some(row) = after_count.rows.first() {
                assert_eq!(row.get("groups"), Some(&Value::Int(2)));
            }
        }
    }
}

#[test]
fn with_collect_preserves_distinct_nodes_with_the_same_projected_property() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'same'})-[:SYNTHESIZED_FROM]->(:Memory {id: 'left'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'same'})-[:SYNTHESIZED_FROM]->(:Memory {id: 'right'})")
        .unwrap();
    let query = "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) WHERE c.id IN $ids WITH c, COLLECT(DISTINCT s.id) AS source_ids RETURN c.id, source_ids";
    // A second identity WITH exercises the production clause-pipeline path.
    for query in [
        query.to_string(),
        query.replace(" RETURN ", " WITH c, source_ids RETURN "),
    ] {
        let output = db
            .query_with_params(
                &query,
                &BTreeMap::from([(
                    "ids".to_string(),
                    Value::List(vec![Value::String("same".to_string())]),
                )]),
            )
            .unwrap();
        assert_eq!(output.rows.len(), 2);
        let mut sources = output
            .rows
            .iter()
            .map(|row| {
                assert_eq!(row.get("c.id"), Some(&Value::String("same".into())));
                let Some(Value::List(values)) = row.get("source_ids") else {
                    panic!("missing collected sources")
                };
                assert_eq!(values.len(), 1);
                let Value::String(source) = &values[0] else {
                    panic!("invalid source id")
                };
                source.clone()
            })
            .collect::<Vec<_>>();
        sources.sort();
        assert_eq!(sources, ["left", "right"]);
    }
}

#[test]
fn post_aggregate_lookup_applies_pagination_to_expanded_rows() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'm', space_id: 's'})-[:MENTIONS]->(:Entity {community_id: 'g'})",
    )
    .unwrap();
    db.query("CREATE (:Community {community_id: 'g', name: 'a', description: 'a'})")
        .unwrap();
    db.query("CREATE (:Community {community_id: 'g', name: 'b', description: 'b'})")
        .unwrap();
    for indexed in [false, true] {
        if indexed {
            db.query("CREATE INDEX ON :Community(community_id)")
                .unwrap();
        }
        // A window written on WITH intentionally limits groups before the
        // lookup. It must remain distinct from the RETURN window below.
        let before_lookup = db.query_with_params(
            "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE m.space_id IN $space_ids AND e.community_id IS NOT NULL WITH e.community_id AS community_id, COUNT(DISTINCT m) AS memory_count ORDER BY memory_count DESC LIMIT $limit OPTIONAL MATCH (c:Community) WHERE c.community_id = community_id RETURN c.name, memory_count, c.description",
            &BTreeMap::from([
                ("space_ids".into(), Value::List(vec![Value::String("s".into())])),
                ("limit".into(), Value::Int(1)),
            ]),
        ).unwrap();
        assert_eq!(before_lookup.rows.len(), 2, "indexed={indexed}");
        for offset in 0..=3 {
            for limit in 0..=3 {
                let output = db.query_with_params(
            "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE m.space_id IN $space_ids AND e.community_id IS NOT NULL WITH e.community_id AS community_id, COUNT(DISTINCT m) AS memory_count OPTIONAL MATCH (c:Community) WHERE c.community_id = community_id RETURN c.name, memory_count, c.description ORDER BY memory_count DESC SKIP $offset LIMIT $limit",
            &BTreeMap::from([
                ("space_ids".into(), Value::List(vec![Value::String("s".into())])),
                ("offset".into(), Value::Int(offset)),
                ("limit".into(), Value::Int(limit)),
            ]),
        ).unwrap();
                assert_eq!(
                    output.rows.len(),
                    (2_i64 - offset).max(0).min(limit) as usize,
                    "indexed={indexed}, offset={offset}, limit={limit}"
                );
                for row in &output.rows {
                    assert_eq!(row.get("memory_count"), Some(&Value::Int(1)));
                }
            }
        }
    }
}

#[test]
fn with_collect_preserves_null_property_groups_and_return_aliases() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'one', title: null})-[:SYNTHESIZED_FROM]->(:Memory {id: 'left'})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'two'})-[:SYNTHESIZED_FROM]->(:Memory {id: 'right'})")
        .unwrap();
    let output = db.query("MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) WITH c, COLLECT(DISTINCT s.id) AS source_ids RETURN c.title AS title, source_ids AS sources").unwrap();
    assert_eq!(output.rows.len(), 2);
    for row in &output.rows {
        assert_eq!(row.get("title"), Some(&Value::Null));
        assert!(row.get("source_ids").is_none());
        let Some(Value::List(values)) = row.get("sources") else {
            panic!("missing aliased sources")
        };
        assert_eq!(values.len(), 1);
    }
}

#[test]
fn post_aggregate_lookup_filters_or_extends_missing_groups_before_limit() {
    let mut db = Database::new();
    for id in ["missing-1", "missing-2"] {
        db.query_with_params("CREATE (:Memory {id: $id, space_id: 's'})-[:MENTIONS]->(:Entity {community_id: 'missing'})", &BTreeMap::from([("id".into(), Value::String(id.into()))])).unwrap();
    }
    db.query("CREATE (:Memory {id: 'found', space_id: 's'})-[:MENTIONS]->(:Entity {community_id: 'found'})").unwrap();
    db.query("CREATE (:Community {community_id: 'found', name: 'found', description: 'present'})")
        .unwrap();
    for optional in [false, true] {
        let lookup = if optional { "OPTIONAL MATCH" } else { "MATCH" };
        let query = format!("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE m.space_id IN $space_ids AND e.community_id IS NOT NULL WITH e.community_id AS community_id, COUNT(DISTINCT m) AS memory_count {lookup} (c:Community) WHERE c.community_id = community_id RETURN c.name, memory_count, c.description ORDER BY memory_count DESC LIMIT $limit");
        let output = db
            .query_with_params(
                &query,
                &BTreeMap::from([
                    (
                        "space_ids".into(),
                        Value::List(vec![Value::String("s".into())]),
                    ),
                    ("limit".into(), Value::Int(1)),
                ]),
            )
            .unwrap();
        assert_eq!(output.rows.len(), 1, "optional={optional}");
        assert_eq!(
            output.rows[0].get("memory_count"),
            Some(&Value::Int(if optional { 2 } else { 1 }))
        );
        assert_eq!(
            output.rows[0].get("c.name"),
            Some(&if optional {
                Value::Null
            } else {
                Value::String("found".into())
            })
        );
    }
}

#[test]
fn count_property_ignores_missing_and_null_values() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One', deleted_at: null})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Three', deleted_at: 'now'})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN count(m.deleted_at) AS deleted")
        .unwrap();
    assert_eq!(output.rows[0].get("deleted"), Some(&Value::Int(1)));
}

#[test]
fn count_distinct_property_ignores_duplicate_missing_and_null_values() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note', status: null})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'task', status: 'open'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 4, kind: 'task', status: 'open'})")
        .unwrap();

    let output = db
        .query(
            "MATCH (m:Memory) RETURN count(DISTINCT m.kind) AS kinds, count(DISTINCT m.status) AS statuses",
        )
        .unwrap();
    assert_eq!(output.rows[0].get("kinds"), Some(&Value::Int(2)));
    assert_eq!(output.rows[0].get("statuses"), Some(&Value::Int(1)));
}

#[test]
fn avg_property_ignores_missing_null_and_non_numeric_values() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, is_crystal: false, decay_score_cached: 0.2})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, is_crystal: false, decay_score_cached: 1})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, is_crystal: false, decay_score_cached: null})")
        .unwrap();
    db.query("CREATE (:Memory {id: 4, is_crystal: false, decay_score_cached: 'skip'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 5, is_crystal: true, decay_score_cached: 10.0})")
        .unwrap();

    let output = db
        .query(
            "MATCH (m:Memory) WHERE m.is_crystal = false RETURN count(m) AS total, avg(m.decay_score_cached) AS avg_decay",
        )
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(4)));
    let Some(Value::Float(avg_decay)) = output.rows[0].get("avg_decay") else {
        panic!("expected floating point average");
    };
    assert!((avg_decay - 0.6).abs() < f64::EPSILON);

    let output = db
        .query("MATCH (m:Memory) WHERE m.is_crystal = true RETURN avg(m.missing) AS avg_decay")
        .unwrap();
    assert_eq!(output.rows[0].get("avg_decay"), Some(&Value::Null));
}

#[test]
fn max_property_ignores_missing_and_null_values() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, updated_at: 5})").unwrap();
    db.query("CREATE (:Memory {id: 2, updated_at: null})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, updated_at: 9})").unwrap();
    db.query("CREATE (:Memory {id: 4})").unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN count(m), max(m.updated_at)")
        .unwrap();
    assert_eq!(output.rows[0].get("count(m)"), Some(&Value::Int(4)));
    assert_eq!(
        output.rows[0].get("max(m.updated_at)"),
        Some(&Value::Int(9))
    );

    let output = db
        .query("MATCH (m:Missing) RETURN max(m.updated_at)")
        .unwrap();
    assert_eq!(output.rows[0].get("max(m.updated_at)"), Some(&Value::Null));
}

#[test]
fn aggregate_order_by_alias_and_limit() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN count(*) AS total ORDER BY total DESC LIMIT 1")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
}

#[test]
fn grouped_count_aggregates_by_projected_property() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'task'})").unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN m.kind AS kind, count(*) AS total ORDER BY total DESC, kind ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("kind"),
        Some(&Value::String("note".to_string()))
    );
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
    assert_eq!(
        output.rows[1].get("kind"),
        Some(&Value::String("task".to_string()))
    );
    assert_eq!(output.rows[1].get("total"), Some(&Value::Int(1)));
}

#[test]
fn aggregate_with_can_count_its_grouped_rows() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'one', thread_id: 'shared', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'two', thread_id: 'shared', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'three', thread_id: 'other', space_id: ''})")
        .unwrap();

    let output = db
        .query(
            "MATCH (t:Thread) WITH CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END AS space_id, t.thread_id AS thread_id, MAX(t.id) AS representative_id RETURN COUNT(*) AS total",
        )
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
}

#[test]
fn grouped_count_aggregates_relationship_matches() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, kind: 'note'})-[:MENTIONS {weight: 3}]->(:Entity {id: 10, name: 'Rust'})",
        )
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 2, kind: 'note'})-[:MENTIONS {weight: 1}]->(:Entity {id: 11, name: 'Kuzu'})",
        )
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 3, kind: 'task'})-[:MENTIONS {weight: 5}]->(:Entity {id: 12, name: 'Neo4j'})",
        )
        .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN m.kind AS kind, count(e) AS total ORDER BY kind ASC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("kind"),
        Some(&Value::String("note".to_string()))
    );
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
    assert_eq!(
        output.rows[1].get("kind"),
        Some(&Value::String("task".to_string()))
    );
    assert_eq!(output.rows[1].get("total"), Some(&Value::Int(1)));

    let output = db
        .query("MATCH (:Memory)-[r:MENTIONS]->(:Entity) RETURN count(*), min(r.weight)")
        .unwrap();
    assert_eq!(output.rows[0].get("count(*)"), Some(&Value::Int(3)));
    assert_eq!(output.rows[0].get("min(r.weight)"), Some(&Value::Int(1)));
}

#[test]
fn with_collect_distinct_property_returns_source_ids() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'crystal-1', is_crystal: true})-[:SYNTHESIZED_FROM]->(:Memory {id: 'source-2'})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'source-1'})").unwrap();
    db.query("MATCH (c:Memory {id: 'crystal-1'}), (s:Memory {id: 'source-1'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)")
        .unwrap();
    db.query("MATCH (c:Memory {id: 'crystal-1'}), (s:Memory {id: 'source-1'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)")
        .unwrap();
    db.query(
        "CREATE (:Memory {id: 'crystal-2', is_crystal: true})-[:SYNTHESIZED_FROM]->(:Memory {id: 'source-3'})",
    )
    .unwrap();

    let output = db
        .query_with_params(
            "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) WHERE c.id IN $ids WITH c, COLLECT(DISTINCT s.id) AS source_ids RETURN c.id, source_ids",
            &BTreeMap::from([(
                "ids".to_string(),
                Value::List(vec![Value::String("crystal-1".to_string())]),
            )]),
        )
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("c.id"),
        Some(&Value::String("crystal-1".to_string()))
    );
    assert_eq!(
        output.rows[0].get("source_ids"),
        Some(&Value::List(vec![
            Value::String("source-1".to_string()),
            Value::String("source-2".to_string()),
        ]))
    );
}

#[test]
fn with_count_and_collect_preserves_group_node_for_ordering() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity-1', community_id: 42})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-2', community_id: 42})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-1', title: 'First', importance: 0.8})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-2', title: 'Second', importance: 0.9})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-1'}), (e:Entity {id: 'entity-1'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-1'}), (e:Entity {id: 'entity-2'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-1'}), (e:Entity {id: 'entity-2'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-2'}), (e:Entity {id: 'entity-1'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (e:Entity {community_id: $community_id})<-[:MENTIONS]-(m:Memory) WITH m, COUNT(e) AS entity_count, COLLECT(DISTINCT e.id) AS entity_ids RETURN m, entity_count, entity_ids ORDER BY entity_count DESC, m.importance DESC LIMIT 1",
            &BTreeMap::from([("community_id".to_string(), Value::Int(42))]),
        )
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    let Some(Value::Map(memory)) = output.rows[0].get("m") else {
        panic!("expected grouped memory node map");
    };
    assert_eq!(
        memory.get("id"),
        Some(&Value::String("memory-1".to_string()))
    );
    assert_eq!(output.rows[0].get("entity_count"), Some(&Value::Int(3)));
    assert_eq!(
        output.rows[0].get("entity_ids"),
        Some(&Value::List(vec![
            Value::String("entity-1".to_string()),
            Value::String("entity-2".to_string()),
        ]))
    );
}

#[test]
fn counts_single_node_matches() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
    db.query("CREATE (:Entity {id: 10, name: 'Rust'})").unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN count(*) AS total")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));

    let explain = db
        .explain_query("MATCH (m:Memory) RETURN count(*) AS total")
        .unwrap();
    assert!(explain.trace.selected_plan.contains("NodeCountExec"));
    assert!(!explain.trace.selected_plan.contains("AggregateExec"));
}

#[test]
fn counts_relationship_expansion_matches() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 1, title: 'One'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 2, title: 'Two'})-[:MENTIONS]->(:Entity {id: 11, name: 'Kuzu'})",
    )
    .unwrap();

    let output = db
        .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN count(e) AS total")
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
}

#[test]
fn counts_unfiltered_relationship_types_from_the_exact_count_store() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:MENTIONS]->(:Entity {id: 10})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2})-[:MENTIONS]->(:Entity {id: 11})")
        .unwrap();
    db.query("CREATE (:Entity {id: 10})-[:RELATES_TO]->(:Entity {id: 11})")
        .unwrap();

    let output = db
        .query("MATCH ()-[r:MENTIONS]->() RETURN count(r) AS total")
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));

    let explain = db
        .explain_query("MATCH ()-[r:MENTIONS]->() RETURN count(r) AS total")
        .unwrap();
    assert!(explain
        .trace
        .selected_plan
        .contains("RelationshipCountExec"));
    assert!(!explain.trace.selected_plan.contains("AdjacencyExpandExec"));
    assert!(!explain.trace.selected_plan.contains("AggregateExec"));
}

#[test]
fn filtered_node_count_does_not_hydrate_unreferenced_payloads() {
    let execution_memory = crate::executor::ExecutionMemoryConfig {
        batch_payload_bytes: NonZeroUsize::new(512).unwrap(),
        ..crate::executor::ExecutionMemoryConfig::default()
    };
    let mut db = Database::new_with_config(DatabaseConfig {
        execution_memory,
        ..DatabaseConfig::default()
    });
    db.query(&format!(
        "CREATE (:Memory {{id: 'memory-1', content: '{}'}})",
        "x".repeat(64 * 1024)
    ))
    .unwrap();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    let cypher = "MATCH (m:Memory {id: $id}) RETURN count(m) AS total";
    let parameters = BTreeMap::from([("id".to_string(), Value::String("memory-1".to_string()))]);

    let explain = db.explain_query_with_params(cypher, &parameters).unwrap();
    let physical_plan = explain.physical_plan.explain(0);
    assert!(physical_plan.contains("AggregateExec"));
    assert!(physical_plan.contains("NodeProjectionScanExec"));
    assert!(physical_plan.contains("output=node_binding"));
    assert!(physical_plan.contains("properties=[\"id\"]"));
    assert!(!physical_plan.contains("content"));

    let output = db.query_with_params(cypher, &parameters).unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(1)));
}

#[test]
fn node_detail_neighbor_counts_use_distinct_neighbors_and_edges() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'n1', name: 'One'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'n2', name: 'Two'})")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'n1'}), (b:Entity {id: 'n2'}) CREATE (a)-[:RELATES_TO]->(b)")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'n2'}), (b:Entity {id: 'n1'}) CREATE (a)-[:MENTIONS]->(b)")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'n1'}), (b:Entity {id: 'n1'}) CREATE (a)-[:SELF]->(b)")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (n)-[r]-(neighbor)
                 WHERE n.id = $node_id
                 RETURN COUNT(DISTINCT neighbor), COUNT(r)",
            &BTreeMap::from([("node_id".to_string(), Value::String("n1".into()))]),
        )
        .unwrap();

    assert_eq!(
        output.rows[0].get("count(DISTINCT neighbor)"),
        Some(&Value::Int(2))
    );
    assert_eq!(output.rows[0].get("count(r)"), Some(&Value::Int(3)));
}

#[test]
fn converging_relationship_pattern_counts_distinct_source_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'target'})").unwrap();
    db.query("CREATE (:Memory {id: 'other-1'})").unwrap();
    db.query("CREATE (:Memory {id: 'other-2'})").unwrap();
    db.query("CREATE (:Entity {id: 'shared'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'target'}), (e:Entity {id: 'shared'}) CREATE (m)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'other-1'}), (e:Entity {id: 'shared'}) CREATE (m)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'other-2'}), (e:Entity {id: 'shared'}) CREATE (m)-[:MENTIONS]->(e)",
    )
    .unwrap();

    let output = db
        .query(
            "MATCH (other:Memory)-[:MENTIONS]->(:Entity)<-[:MENTIONS]-(m:Memory {id: 'target'})
                 WHERE other.id <> 'target'
                 RETURN other.id AS id
                 ORDER BY id ASC",
        )
        .unwrap();

    assert_eq!(
        output.rows,
        vec![
            BTreeMap::from([("id".to_string(), Value::String("other-1".to_string()))]),
            BTreeMap::from([("id".to_string(), Value::String("other-2".to_string()))]),
        ]
    );

    let output = db
        .query(
            "MATCH (other:Memory)-[:MENTIONS]->(:Entity)<-[:MENTIONS]-(m:Memory {id: 'target'})
                 WHERE other.id <> 'target'
                 RETURN COUNT(DISTINCT other)",
        )
        .unwrap();

    assert_eq!(
        output.rows[0].get("count(DISTINCT other)"),
        Some(&Value::Int(2))
    );
}

#[test]
fn grouped_evidence_preserves_all_groups_before_top_k_and_deduplicates_ids() {
    let query = "MATCH (r:EvidenceInput) WHERE r.eligible = true WITH r.group_id AS g, r.document_id AS doc, r.score AS score WITH g, MAX(score) AS best, collect_top(doc, score, $m) AS evidence RETURN g, best, evidence ORDER BY best DESC, g ASC LIMIT $k";
    let rows = [
        ("A", "a1", 100.0, true),
        ("A", "a2", 99.0, true),
        ("A", "a3", 98.0, true),
        ("A", "a4", 97.0, true),
        ("A", "a5", 96.0, true),
        ("B", "b1", 95.0, true),
        ("C", "c1", 94.0, true),
        ("A", "a2", 99.5, true),
        ("D", "d1", 1000.0, false),
    ];
    for reverse in [false, true] {
        let mut database = Database::new();
        let mut rows = rows.to_vec();
        if reverse {
            rows.reverse();
        }
        for (group, id, score, eligible) in rows {
            database.query_with_params("CREATE (:EvidenceInput {group_id: $g, document_id: $id, score: $score, eligible: $eligible})", &BTreeMap::from([
                ("g".into(), Value::String(group.into())), ("id".into(), Value::String(id.into())),
                ("score".into(), Value::Float(score)), ("eligible".into(), Value::Bool(eligible)),
            ])).unwrap();
        }
        for evidence in [1, 2, 3] {
            let parameters = BTreeMap::from([
                ("m".into(), Value::Int(evidence)),
                ("k".into(), Value::Int(3)),
            ]);
            let explain = database
                .query_with_params(&format!("EXPLAIN {query}"), &parameters)
                .unwrap();
            let Value::String(plan) = &explain.rows[0]["plan"] else {
                panic!("missing plan")
            };
            assert!(plan.contains("NodeProjectionScanExec"), "{plan}");
            assert!(!plan.contains("GraphMatchExec"), "{plan}");
            assert!(plan.contains("TopNExec"), "{plan}");
            assert!(!plan.contains("SortExec"), "{plan}");
            let output = database.query_with_params(query, &parameters).unwrap();
            assert_eq!(output.rows.len(), 3);
            for (row, group, score, documents) in [
                (&output.rows[0], "A", 100.0, vec!["a1", "a2", "a3"]),
                (&output.rows[1], "B", 95.0, vec!["b1"]),
                (&output.rows[2], "C", 94.0, vec!["c1"]),
            ] {
                assert_eq!(row["g"], Value::String(group.into()));
                assert_eq!(row["best"], Value::Float(score));
                assert_eq!(
                    row["evidence"],
                    Value::List(
                        documents
                            .into_iter()
                            .take(evidence as usize)
                            .map(|id| Value::String(id.into()))
                            .collect()
                    )
                );
            }
        }
    }
}

#[test]
fn grouped_evidence_scalar_max_empty_null_and_ties_preserve_query_semantics() {
    let mut database = Database::new();
    for score in [Value::Null, Value::Int(1), Value::Int(4), Value::Int(2)] {
        database
            .query_with_params(
                "CREATE (:ScoreInput {score: $score})",
                &BTreeMap::from([("score".into(), score)]),
            )
            .unwrap();
    }
    for (query, expected) in [
        (
            "MATCH (n:ScoreInput) WITH n.score AS score RETURN MAX(score) AS best",
            Value::Int(4),
        ),
        (
            "MATCH (n:Absent) WITH n.score AS score RETURN MAX(score) AS best",
            Value::Null,
        ),
    ] {
        assert_eq!(database.query(query).unwrap().rows[0]["best"], expected);
    }
    let empty = database.query("MATCH (n:Absent) WITH n.id AS doc, n.score AS score RETURN MAX(score) AS best, collect_top(doc, score, 2) AS evidence").unwrap();
    assert_eq!(empty.rows.len(), 1);
    assert_eq!(empty.rows[0]["best"], Value::Null);
    assert_eq!(empty.rows[0]["evidence"], Value::List(Vec::new()));
    for id in ["z", "a", "b", "a"] {
        database
            .query_with_params(
                "CREATE (:TieInput {id: $id, score: 1.0})",
                &BTreeMap::from([("id".into(), Value::String(id.into()))]),
            )
            .unwrap();
    }
    let tied = database.query("MATCH (n:TieInput) WITH n.id AS doc, n.score AS score, n.missing AS g RETURN g, MAX(score) AS best, collect_top(doc, score, 2) AS evidence").unwrap();
    assert_eq!(tied.rows.len(), 1);
    assert_eq!(tied.rows[0]["g"], Value::Null);
    assert_eq!(
        tied.rows[0]["evidence"],
        Value::List(vec![Value::String("a".into()), Value::String("b".into())])
    );
}

#[test]
fn grouped_evidence_rejects_invalid_shapes_and_never_delivers_a_successful_prefix() {
    let mut database = Database::new();
    database.query("CREATE (:One {id: 'a'})").unwrap();
    for tail in [
        "WITH 'a' AS doc, 1.0 AS score RETURN collect_top(doc, score, 0)",
        "WITH 'a' AS doc, 1.0 AS score RETURN collect_top(doc, score, -1)",
        "WITH NULL AS doc, 1.0 AS score RETURN collect_top(doc, score, 1)",
        "WITH 'a' AS doc, NULL AS score RETURN collect_top(doc, score, 1)",
        "WITH 'a' AS doc, 1 AS score RETURN collect_top(doc, score, 1)",
        "WITH 'a' AS doc, 1.0 AS score, 1.0 AS g RETURN g, collect_top(doc, score, 1)",
    ] {
        let query = format!("MATCH (n:One) {tail}");
        assert!(
            matches!(database.query(&query), Err(crate::HawDBError::Semantic(message)) if message.contains("collect_top")),
            "{query}"
        );
    }
    assert_eq!(database.query("MATCH (n:One) WITH n.id AS doc, 1.0 AS score RETURN collect_top(doc, score, 1000000) AS evidence").unwrap().rows[0]["evidence"], Value::List(vec![Value::String("a".into())]));
}

#[test]
fn grouped_evidence_matches_full_id_reference_for_mixed_keys_and_permutations() {
    let query = "MATCH (r:EvidenceInput) WHERE r.eligible = true WITH r.group_id AS g, r.document_id AS doc, r.score AS score WITH g, MAX(score) AS best, collect_top(doc, score, $m) AS evidence RETURN g, best, evidence ORDER BY best DESC, g ASC LIMIT $k";
    let mut seed = 1119u64;
    for case in 0..12 {
        let mut rows = Vec::new();
        let mut reference = BTreeMap::<Value, BTreeMap<String, f64>>::new();
        for ordinal in 0..96 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let group = match (seed >> 32) % 7 {
                0 => None,
                1 => Some(Value::Null),
                2 => Some(Value::Bool(false)),
                3 => Some(Value::Bool(true)),
                4 => Some(Value::Int(-7)),
                5 => Some(Value::String("group".into())),
                _ => Some(Value::Uuid(crate::Uuid::from_u128(7))),
            };
            let id = format!("document-{}", (seed >> 24) % 13);
            let score = if case == 0 {
                if ordinal % 2 == 0 {
                    -0.0f64
                } else {
                    0.0f64
                }
            } else {
                ((seed >> 16) % 9) as f64 / 2.0 - 3.0
            };
            let eligible = ordinal % 4 != 0;
            if eligible {
                reference
                    .entry(group.clone().unwrap_or(Value::Null))
                    .or_default()
                    .entry(id.clone())
                    .and_modify(|old| {
                        if score.total_cmp(old).is_gt() {
                            *old = score;
                        }
                    })
                    .or_insert(score);
            }
            rows.push((group, id, score, eligible));
        }
        let mut expected = reference
            .into_iter()
            .map(|(group, scores)| {
                let mut scores = scores.into_iter().collect::<Vec<_>>();
                scores.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
                (group, scores[0].1, scores)
            })
            .collect::<Vec<_>>();
        expected.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        for reverse in [false, true] {
            let mut database = Database::new();
            let mut present = Vec::new();
            let mut missing = Vec::new();
            let ordered = if reverse {
                rows.iter().rev().collect::<Vec<_>>()
            } else {
                rows.iter().collect()
            };
            for (group, id, score, eligible) in ordered {
                let mut row = BTreeMap::from([
                    ("id".into(), Value::String(id.clone())),
                    ("score".into(), Value::Float(*score)),
                    ("eligible".into(), Value::Bool(*eligible)),
                ]);
                if let Some(group) = group {
                    row.insert("g".into(), group.clone());
                    present.push(Value::Map(row));
                } else {
                    missing.push(Value::Map(row));
                }
            }
            for (fixture, statement) in [
                (present, "UNWIND $rows AS r CREATE (:EvidenceInput {group_id: r.g, document_id: r.id, score: r.score, eligible: r.eligible})"),
                (missing, "UNWIND $rows AS r CREATE (:EvidenceInput {document_id: r.id, score: r.score, eligible: r.eligible})"),
            ] {
                database.query_with_params(statement, &BTreeMap::from([("rows".into(), Value::List(fixture))])).unwrap();
            }
            for m in [1, 3, 17] {
                for k in [1, 2, 20] {
                    let parameters =
                        BTreeMap::from([("m".into(), Value::Int(m)), ("k".into(), Value::Int(k))]);
                    let actual = database.query_with_params(query, &parameters).unwrap();
                    assert_eq!(
                        actual.rows.len(),
                        expected.len().min(k as usize),
                        "case {case}, reverse {reverse}, M {m}, K {k}"
                    );
                    for (row, (group, best, documents)) in actual.rows.iter().zip(&expected) {
                        assert_eq!(&row["g"], group);
                        let Value::Float(score) = row["best"] else {
                            panic!("expected Float maximum")
                        };
                        assert_eq!(score.to_bits(), best.to_bits());
                        assert_eq!(
                            row["evidence"],
                            Value::List(
                                documents
                                    .iter()
                                    .take(m as usize)
                                    .map(|(id, _)| Value::String(id.clone()))
                                    .collect()
                            )
                        );
                    }
                }
            }
        }
    }
}

fn sum_expression_fixture() -> Database {
    let mut db = Database::new();
    for (g, x) in [
        ("a", "1"),
        ("a", "2"),
        ("a", "3"),
        ("b", "4"),
        ("b", "null"),
    ] {
        db.query(&format!("CREATE (:Num {{g: '{g}', x: {x}}})"))
            .unwrap();
    }
    db
}

#[test]
fn sum_and_expression_aggregates_follow_opencypher_semantics() {
    let mut db = sum_expression_fixture();
    let output = db
        .query(
            "MATCH (n:Num) RETURN sum(n.x) AS s, sum(CASE WHEN n.x > 1 THEN 1 ELSE 0 END) AS big, count(DISTINCT n.x % 2) AS parities, avg(n.x * 2) AS avg2, min(n.x + 1) AS lo, max(-n.x) AS neg, collect(DISTINCT n.x % 3) AS residues, count(n.x * 1) AS counted, sum(DISTINCT n.x % 2) AS odd",
        )
        .unwrap();
    assert_eq!(
        output.schema().columns(),
        ["s", "big", "parities", "avg2", "lo", "neg", "residues", "counted", "odd"]
    );
    assert_eq!(output.rows.len(), 1);
    let row = &output.rows[0];
    assert_eq!(row.get("s"), Some(&Value::Int(10)));
    // A NULL x makes the CASE condition unknown, so its ELSE branch counts 0.
    assert_eq!(row.get("big"), Some(&Value::Int(3)));
    assert_eq!(row.get("parities"), Some(&Value::Int(2)));
    assert_eq!(row.get("avg2"), Some(&Value::Float(5.0)));
    assert_eq!(row.get("lo"), Some(&Value::Int(2)));
    assert_eq!(row.get("neg"), Some(&Value::Int(-1)));
    assert_eq!(
        row.get("residues"),
        Some(&Value::List(vec![
            Value::Int(0),
            Value::Int(1),
            Value::Int(2)
        ]))
    );
    assert_eq!(row.get("counted"), Some(&Value::Int(4)));
    assert_eq!(row.get("odd"), Some(&Value::Int(1)));

    let output = db
        .query("MATCH (n:Num) RETURN sum(n.x * 0.5) AS half, sum(n.missing) AS none")
        .unwrap();
    assert_eq!(output.rows[0].get("half"), Some(&Value::Float(5.0)));
    assert_eq!(output.rows[0].get("none"), Some(&Value::Int(0)));
}

#[test]
fn grouped_sum_and_unaliased_expression_aggregates() {
    let mut db = sum_expression_fixture();
    let output = db
        .query("MATCH (n:Num) RETURN n.g AS g, sum(n.x) AS s ORDER BY g")
        .unwrap();
    assert_eq!(output.schema().columns(), ["g", "s"]);
    let rows = output
        .rows
        .iter()
        .map(|row| (row.get("g").cloned(), row.get("s").cloned()))
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            (Some(Value::String("a".into())), Some(Value::Int(6))),
            (Some(Value::String("b".into())), Some(Value::Int(4))),
        ]
    );

    let output = db
        .query("MATCH (n:Num) RETURN sum(n.x * 2), count(DISTINCT n.x % 2)")
        .unwrap();
    assert_eq!(
        output.schema().columns(),
        ["sum(n.x * 2)", "count(DISTINCT n.x % 2)"]
    );
    assert_eq!(output.rows[0].get("sum(n.x * 2)"), Some(&Value::Int(20)));
}

#[test]
fn sum_and_avg_over_empty_input() {
    let mut db = sum_expression_fixture();
    let output = db
        .query(
            "MATCH (n:Missing) RETURN sum(n.x) AS s, sum(n.x * 2) AS s2, avg(n.x * 2) AS a, count(n.x + 1) AS c, collect(n.x + 1) AS xs",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    let row = &output.rows[0];
    assert_eq!(row.get("s"), Some(&Value::Int(0)));
    assert_eq!(row.get("s2"), Some(&Value::Int(0)));
    assert_eq!(row.get("a"), Some(&Value::Null));
    assert_eq!(row.get("c"), Some(&Value::Int(0)));
    assert_eq!(row.get("xs"), Some(&Value::List(Vec::new())));
}

#[test]
fn expression_aggregates_read_with_pipeline_columns() {
    let mut db = sum_expression_fixture();
    let output = db
        .query("MATCH (n:Num) WITH n.g AS g, n.x AS x RETURN g, sum(x * 2) AS s ORDER BY g")
        .unwrap();
    assert_eq!(output.schema().columns(), ["g", "s"]);
    let sums = output
        .rows
        .iter()
        .map(|row| row.get("s").cloned())
        .collect::<Vec<_>>();
    assert_eq!(sums, [Some(Value::Int(12)), Some(Value::Int(8))]);

    // Re-running with a new parameter value must rebind the cached plan.
    for min in [2, 4, 2] {
        let parameters = BTreeMap::from([("min".to_string(), Value::Int(min))]);
        let output = db
            .query_with_params(
                "MATCH (n:Num) WITH n.g AS g, sum(CASE WHEN n.x >= $min THEN 1 ELSE 0 END) AS hits RETURN g, hits ORDER BY g",
                &parameters,
            )
            .unwrap();
        let hits = output
            .rows
            .iter()
            .map(|row| row.get("hits").cloned())
            .collect::<Vec<_>>();
        let expected = if min == 2 { [2, 1] } else { [0, 1] };
        assert_eq!(
            hits,
            expected.map(|value| Some(Value::Int(value))),
            "min={min}"
        );
    }
}

#[test]
fn sum_rejects_overflow_non_numeric_and_nested_aggregates() {
    let mut db = Database::new();
    db.query(&format!("CREATE (:Big {{x: {}}})", i64::MAX))
        .unwrap();
    db.query("CREATE (:Big {x: 1})").unwrap();
    db.query("CREATE (:Text {x: 'a'})").unwrap();
    let error = db.query("MATCH (n:Big) RETURN sum(n.x) AS s").unwrap_err();
    assert!(error.to_string().contains("overflow"), "{error}");
    let error = db.query("MATCH (n:Text) RETURN sum(n.x) AS s").unwrap_err();
    assert!(error.to_string().contains("numeric"), "{error}");
    let error = db
        .query("MATCH (n:Big) RETURN sum(count(n)) AS s")
        .unwrap_err();
    assert!(
        error.to_string().contains("cannot contain aggregate"),
        "{error}"
    );
}

#[test]
fn unary_minus_binds_tighter_than_arithmetic_in_projections() {
    let mut db = sum_expression_fixture();
    let output = db
        .query(
            "MATCH (n:Num {x: 2}) RETURN -n.x * 2 AS a, n.x - -1 AS b, - 1 AS c, -(n.x) AS d, -n.x + 3 AS e",
        )
        .unwrap();
    let row = &output.rows[0];
    assert_eq!(row.get("a"), Some(&Value::Int(-4)));
    assert_eq!(row.get("b"), Some(&Value::Int(3)));
    assert_eq!(row.get("c"), Some(&Value::Int(-1)));
    assert_eq!(row.get("d"), Some(&Value::Int(-2)));
    assert_eq!(row.get("e"), Some(&Value::Int(1)));
    assert!(db.query("MATCH (n:Num {x: 2}) RETURN -'a' AS f").is_err());
}
