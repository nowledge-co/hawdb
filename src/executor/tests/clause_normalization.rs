use super::*;

#[test]
fn structural_read_normalization_preserves_generic_rows_and_pruning() {
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    let mut nodes = Vec::new();
    for id in 0..4 {
        nodes.push(
            store
                .create_node(
                    &mut catalog,
                    "Node",
                    BTreeMap::from([
                        ("id".into(), Value::Int(id)),
                        ("group".into(), Value::Int(id % 2)),
                        (
                            "score".into(),
                            if id == 3 {
                                Value::Null
                            } else {
                                Value::Int(id * 2)
                            },
                        ),
                        ("unused".into(), Value::String("x".repeat(16 * 1024))),
                    ]),
                )
                .unwrap(),
        );
    }
    for (index, (source, target)) in [(0, 1), (0, 1), (0, 2), (1, 2), (2, 0), (2, 2)]
        .into_iter()
        .enumerate()
    {
        store
            .create_relationship(
                &mut catalog,
                nodes[source],
                nodes[target],
                "LINK",
                BTreeMap::from([("weight".into(), Value::Int(index as i64))]),
            )
            .unwrap();
    }
    let parameters = BTreeMap::from([("source".into(), Value::Int(0))]);
    for query in [
        "MATCH (n:Node) WHERE n.id >= $source RETURN n.id, n.score",
        "MATCH (n:Node {id: $source}) RETURN n",
        "MATCH (a:Node {id: $source})-[r:LINK]->(b:Node {group: 1}) RETURN a.id, b.id, r.weight",
        "MATCH (a:Node)<-[r:LINK]-(b:Node) WHERE r.weight = 1 RETURN a.id, b.id",
        "MATCH (a:Node)-[r:LINK]-(b:Node) WHERE a.id = $source RETURN a.id, b.id, r.weight",
        "MATCH (a:Node {id: $source})-[:LINK*1..2]->(b:Node) RETURN b.id",
        "MATCH (n:Node) RETURN COUNT(n) AS count, MAX(n.score) AS maximum",
        "MATCH (n:Node) RETURN n.group AS bucket, COUNT(DISTINCT n.score) AS count, MAX(n.score) AS maximum",
        "MATCH (n:Node) RETURN COUNT(n) AS count, n.group AS bucket",
        "MATCH (n:Node) WITH n AS anchor, COUNT(n) AS count MATCH (anchor)-[:LINK]->(m:Node) RETURN anchor.id, count, m.id",
        "MATCH (n:Node) RETURN n.id AS id ORDER BY n.score DESC LIMIT 2",
        "MATCH (a:Node)-[:LINK]->(b:Node)-[:LINK]->(a) RETURN a.id, b.id",
        "MATCH (a:Node {id: $source}), (b:Node {group: 1}) RETURN a.id, b.id",
        "MATCH (a:Node {id: $source}) MATCH (b:Node {group: 1}) WHERE a.id < b.id RETURN a.id, b.id",
        "MATCH (n:Node) WITH n.id AS previous MATCH (n:Node {group: 1}) RETURN previous, n.id",
        "MATCH (n:Node) WITH n.group AS bucket, COUNT(n) AS count RETURN bucket, count ORDER BY count DESC, bucket ASC LIMIT 2",
        "MATCH (n:Node) WITH n AS item, COUNT(n) AS count RETURN item.id, count ORDER BY count DESC, item.score DESC SKIP 1 LIMIT 2",
        "MATCH (n:Node) WITH n AS item, COUNT(n) AS count ORDER BY count DESC, COALESCE(item.score, -1) DESC LIMIT 2 RETURN item.id, count",
        "MATCH (n:Node {id: $source}) OPTIONAL MATCH (n)-[:MISSING]->(other:Node) RETURN n.id, COUNT(other) AS missing_count",
    ] {
        let generic = hawdb_plan::plan_pipeline_query(query, &parameters).unwrap();
        let normalized = hawdb_plan::plan_normalized_pipeline_query(query, &parameters).unwrap();
        let mut outputs = Vec::new();
        for logical in [generic, normalized] {
            let physical = hawdb_optimizer::CascadesOptimizer::default().optimize(&logical);
            let mut rows = execute(&physical, &mut catalog, &mut store).unwrap_or_else(|error| panic!("{query}: {error}"));
            if !query.contains("ORDER BY") {
                rows.sort_by_key(|row| format!("{row:?}"));
            }
            outputs.push(rows);
        }
        assert_eq!(outputs[0], outputs[1], "{query}");
    }
    let logical = hawdb_plan::plan_normalized_pipeline_query(
        "MATCH (n:Node {id: $source}) RETURN n.id AS id",
        &parameters,
    )
    .unwrap();
    let physical = hawdb_optimizer::CascadesOptimizer::default().optimize(&logical);
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(2048).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(1024).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let output = execute_with_output_limits_profile_and_external_and_memory(
        &physical,
        &mut catalog,
        &mut store,
        &parameters,
        &mut NoExternalReadOperator,
        None,
        None,
        &memory,
    )
    .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(0)));
    let query = "MATCH (n:Node) WITH n.group AS bucket, COUNT(n) AS count RETURN bucket, 10 / (1 - bucket) AS risky ORDER BY bucket LIMIT 1";
    for binder in [
        hawdb_plan::plan_pipeline_query,
        hawdb_plan::plan_normalized_pipeline_query,
    ] {
        let logical = binder(query, &BTreeMap::new()).unwrap();
        let physical = hawdb_optimizer::CascadesOptimizer::default().optimize(&logical);
        assert!(
            execute(&physical, &mut catalog, &mut store).is_err(),
            "projection errors must not disappear beyond LIMIT"
        );
    }
}

#[test]
fn projected_entities_preserve_user_properties_that_share_metadata_names() {
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    let root = store
        .create_node(
            &mut catalog,
            "Node",
            BTreeMap::from([
                ("id".into(), Value::Int(1)),
                ("_id".into(), Value::Int(91)),
                ("labels".into(), Value::String("user-labels".into())),
            ]),
        )
        .unwrap();
    let other = store
        .create_node(
            &mut catalog,
            "Node",
            BTreeMap::from([("id".into(), Value::Int(2)), ("_id".into(), Value::Null)]),
        )
        .unwrap();
    let edge = store
        .create_relationship(
            &mut catalog,
            root,
            other,
            "LINK",
            BTreeMap::from([
                ("_id".into(), Value::Int(72)),
                ("type".into(), Value::String("user-type".into())),
                ("source_id".into(), Value::String("user-source".into())),
            ]),
        )
        .unwrap();
    let query = "MATCH (a:Node {id: 1})-[e:LINK]->(b:Node) WITH a AS node, e AS edge, COUNT(b) AS count RETURN node._id AS user_id, node.labels AS user_labels, id(node) AS native_id, edge._id AS edge_id, edge.type AS user_type, type(edge) AS native_type, edge.source_id AS user_source, id(edge) AS native_edge_id, count";
    for binder in [
        hawdb_plan::plan_pipeline_query,
        hawdb_plan::plan_normalized_pipeline_query,
    ] {
        let logical = binder(query, &BTreeMap::new()).unwrap();
        let physical = hawdb_optimizer::CascadesOptimizer::default().optimize(&logical);
        let rows = execute(&physical, &mut catalog, &mut store).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            BTreeMap::from([
                ("user_id".into(), Value::Int(91)),
                ("user_labels".into(), Value::String("user-labels".into())),
                ("native_id".into(), Value::Int(root.0 as i64)),
                ("edge_id".into(), Value::Int(72)),
                ("user_type".into(), Value::String("user-type".into())),
                ("native_type".into(), Value::String("LINK".into())),
                ("user_source".into(), Value::String("user-source".into())),
                ("native_edge_id".into(), Value::Int(edge.0 as i64)),
                ("count".into(), Value::Int(1)),
            ])
        );
        let logical = binder(
            "MATCH (n:Node) WITH n AS item RETURN COUNT(item._id) AS count",
            &BTreeMap::new(),
        )
        .unwrap();
        let physical = hawdb_optimizer::CascadesOptimizer::default().optimize(&logical);
        let rows = execute(&physical, &mut catalog, &mut store).unwrap();
        assert_eq!(rows[0]["count"], Value::Int(1));
    }
}
