use super::*;

fn run(
    store: &mut GraphStore,
    catalog: &mut Catalog,
    query: &str,
    parameters: BTreeMap<String, Value>,
) -> Result<Vec<Row>> {
    let logical = hawdb_plan::plan_pipeline_query(query, &parameters)?;
    let physical = hawdb_optimizer::CascadesOptimizer::default().optimize(&logical);
    execute(&physical, catalog, store)
}

#[test]
fn clause_mutations_preserve_atomic_failure_parameters_and_recovery() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("skein-clause-mutations-{nonce}"));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    for (id, score) in [(1, 10), (2, i64::MAX), (3, 0)] {
        run(
            &mut store,
            &mut catalog,
            "MERGE (item:Item {id: $id}) ON CREATE SET item.score = $score",
            BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("score".into(), Value::Int(score)),
            ]),
        )
        .unwrap();
    }
    let error = run(&mut store, &mut catalog, "MATCH (item:Item) SET item.note = 'must not commit', item.score = item.score + 1 RETURN COUNT(item) AS changed", BTreeMap::new()).unwrap_err();
    assert!(error.to_string().contains("overflow"), "{error}");
    let rows = run(&mut store, &mut catalog, "MATCH (item:Item) RETURN item.id AS id, item.score AS score, item.note AS note ORDER BY id", BTreeMap::new()).unwrap();
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|row| row.get("note") == Some(&Value::Null)));
    assert_eq!(rows[0].get("score"), Some(&Value::Int(10)));
    assert_eq!(rows[1].get("score"), Some(&Value::Int(i64::MAX)));
    let rows = run(
        &mut store,
        &mut catalog,
        "MATCH (item:Item {id: $id}) SET item.score = item.score + 1 RETURN item.score AS score",
        BTreeMap::from([("id".into(), Value::Int(1))]),
    )
    .unwrap();
    assert_eq!(rows[0].get("score"), Some(&Value::Int(11)));
    for weight in [3, 99] {
        run(&mut store, &mut catalog, "MATCH (left:Item {id: 1}), (right:Item {id: 2}) MERGE (left)-[edge:LINK]->(right) ON CREATE SET edge.weight = $weight", BTreeMap::from([("weight".into(), Value::Int(weight))])).unwrap();
    }
    let rows = run(
        &mut store,
        &mut catalog,
        "MATCH (left:Item)-[edge:LINK]->(right:Item) RETURN edge.weight AS weight",
        BTreeMap::new(),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("weight"), Some(&Value::Int(3)));
    for query in [
        "MATCH (left:Item)-[edge:LINK]->(right:Item) WHERE left.id = 1 SET edge.weight = 7, edge.tag = 'copied'",
        "MATCH (left:Item {id: 1})-[original:LINK]->(right:Item) MERGE (left)-[copy:COPY]->(right) ON CREATE SET copy.weight = original.weight",
        "MATCH (left:Item {id: 1})-[:LINK]->(old:Item) MATCH (replacement:Item {id: 3}) MERGE (left)-[edge:ALIAS]->(replacement) ON CREATE SET edge.weight = 11",
    ] {
        run(&mut store, &mut catalog, query, BTreeMap::new()).unwrap();
    }
    drop(store);
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    let rows = run(&mut store, &mut catalog, "MATCH (left:Item)-[edge:COPY]->(right:Item) RETURN left.score AS score, edge.weight AS weight, right.id AS target", BTreeMap::new()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("score"), Some(&Value::Int(11)));
    assert_eq!(rows[0].get("weight"), Some(&Value::Int(7)));
    assert_eq!(rows[0].get("target"), Some(&Value::Int(2)));
    let rows = run(
        &mut store,
        &mut catalog,
        "MATCH (left:Item)-[:ALIAS]->(target:Item) RETURN target.id AS id",
        BTreeMap::new(),
    )
    .unwrap();
    assert_eq!(rows[0].get("id"), Some(&Value::Int(3)));
    run(
        &mut store,
        &mut catalog,
        "MATCH (left:Item)-[edge:COPY]->(right:Item) WHERE left.id = 1 DELETE edge",
        BTreeMap::new(),
    )
    .unwrap();
    store.checkpoint(&catalog).unwrap();
    drop(store);
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    let rows = run(
        &mut store,
        &mut catalog,
        "MATCH (left:Item)-[:COPY]->(right:Item) RETURN COUNT(left) AS count",
        BTreeMap::new(),
    )
    .unwrap();
    assert_eq!(rows[0].get("count"), Some(&Value::Int(0)));
    let rows = run(
        &mut store,
        &mut catalog,
        "MATCH (item:Item) RETURN item.note AS note",
        BTreeMap::new(),
    )
    .unwrap();
    assert!(rows.iter().all(|row| row.get("note") == Some(&Value::Null)));
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn unwind_merge_matches_sequential_bootstrap_rows_and_recovers() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let batch_path = std::env::temp_dir().join(format!("hawdb-unwind-batch-{nonce}"));
    let sequential_path = std::env::temp_dir().join(format!("hawdb-unwind-sequential-{nonce}"));
    let rows = vec![
        BTreeMap::from([
            ("id".into(), Value::String("entity-1".into())),
            ("name".into(), Value::String("First".into())),
            ("rank".into(), Value::Int(3)),
        ]),
        BTreeMap::from([
            ("id".into(), Value::String("entity-2".into())),
            ("name".into(), Value::String("Second".into())),
            ("rank".into(), Value::Int(7)),
        ]),
    ];
    let mut batch_catalog = Catalog::default();
    let mut batch_store = GraphStore::open(&batch_path, &mut batch_catalog).unwrap();
    run(
        &mut batch_store,
        &mut batch_catalog,
        "UNWIND $rows AS row MERGE (entity:Entity {id: row.id}) ON CREATE SET entity.name = row.name, entity.rank = row.rank",
        BTreeMap::from([(
            "rows".into(),
            Value::List(rows.iter().cloned().map(Value::Map).collect()),
        )]),
    )
    .unwrap();
    drop(batch_store);

    let mut sequential_catalog = Catalog::default();
    let mut sequential_store = GraphStore::open(&sequential_path, &mut sequential_catalog).unwrap();
    for row in &rows {
        run(
            &mut sequential_store,
            &mut sequential_catalog,
            "MERGE (entity:Entity {id: $id}) ON CREATE SET entity.name = $name, entity.rank = $rank",
            row.clone(),
        )
        .unwrap();
    }
    let query = "MATCH (entity:Entity) RETURN entity.id AS id, entity.name AS name, entity.rank AS rank ORDER BY id";
    let expected = run(
        &mut sequential_store,
        &mut sequential_catalog,
        query,
        BTreeMap::new(),
    )
    .unwrap();
    drop(sequential_store);

    let mut batch_catalog = Catalog::default();
    let mut batch_store = GraphStore::open(&batch_path, &mut batch_catalog).unwrap();
    let actual = run(&mut batch_store, &mut batch_catalog, query, BTreeMap::new()).unwrap();
    assert_eq!(actual, expected);
    drop(batch_store);
    std::fs::remove_dir_all(batch_path).unwrap();
    std::fs::remove_dir_all(sequential_path).unwrap();
}
