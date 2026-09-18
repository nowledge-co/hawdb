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
    ] {
        let generic = skein_plan::plan_pipeline_query(query, &parameters).unwrap();
        let normalized = skein_plan::plan_normalized_pipeline_query(query, &parameters).unwrap();
        let mut outputs = Vec::new();
        for logical in [generic, normalized] {
            let physical = skein_optimizer::CascadesOptimizer::default().optimize(&logical);
            let mut rows = execute(&physical, &mut catalog, &mut store).unwrap_or_else(|error| panic!("{query}: {error}"));
            rows.sort_by_key(|row| format!("{row:?}"));
            outputs.push(rows);
        }
        assert_eq!(outputs[0], outputs[1], "{query}");
    }
    let logical = skein_plan::plan_normalized_pipeline_query(
        "MATCH (n:Node {id: $source}) RETURN n.id AS id",
        &parameters,
    )
    .unwrap();
    let physical = skein_optimizer::CascadesOptimizer::default().optimize(&logical);
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
}
