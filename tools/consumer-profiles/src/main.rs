#[cfg(feature = "database")]
fn database_workload() {
    use hawdb::{Database, RuntimeCapability, SearchIndex, SearchMode, Value};
    use std::collections::BTreeMap;

    let mut db = Database::new();
    let name = std::env::args().nth(1).unwrap_or_else(|| "consumer".into());
    let parameters = BTreeMap::from([("name".into(), Value::String(name.clone()))]);
    db.query_with_params("CREATE (:Probe {name: $name})", &parameters)
        .unwrap();
    db.query("CREATE (:Other {name: 'target'})").unwrap();
    db.query("MATCH (a:Probe), (b:Other) CREATE (a)-[:LINK]->(b)")
        .unwrap();
    let result = db
        .query("MATCH (a:Probe)-[:LINK]->(b:Other) RETURN a.name AS name, b.name AS target")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0]["name"], Value::String(name));
    assert_eq!(result.rows[0]["target"], Value::String("target".into()));
    db.query_sql("CREATE TABLE items (id BIGINT PRIMARY KEY)")
        .unwrap();
    db.query_sql("INSERT INTO items (id) VALUES (7)").unwrap();
    let result = db.query_sql("SELECT id FROM items").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0]["id"], Value::Int(7));

    let capabilities = hawdb::compiled_runtime_capabilities();
    assert!(!capabilities.is_enabled(RuntimeCapability::AccessControl));
    assert_eq!(
        capabilities.is_enabled(RuntimeCapability::FullTextSearch),
        cfg!(feature = "text")
    );
    assert_eq!(
        capabilities.is_enabled(RuntimeCapability::VectorSearch),
        cfg!(feature = "vector")
    );
    assert_eq!(
        capabilities.is_enabled(RuntimeCapability::GraphAnalytics),
        cfg!(feature = "full")
    );
    assert_eq!(
        capabilities.is_enabled(RuntimeCapability::BackgroundMaintenance),
        cfg!(feature = "full")
    );

    let index = SearchIndex::in_memory();
    #[cfg(not(feature = "text"))]
    assert!(matches!(
        index.search("graph", None, SearchMode::Text, 1),
        Err(hawdb::HawDBError::CapabilityUnavailable {
            capability: RuntimeCapability::FullTextSearch
        })
    ));
    #[cfg(not(feature = "vector"))]
    assert!(matches!(
        index.search("", Some(&[1.0, 0.0]), SearchMode::Vector, 1),
        Err(hawdb::HawDBError::CapabilityUnavailable {
            capability: RuntimeCapability::VectorSearch
        })
    ));

    #[cfg(feature = "text")]
    {
        let mut index = index;
        index
            .upsert(hawdb::SearchDocument {
                id: "one".into(),
                title: "graph database".into(),
                content: "embedded graph".into(),
                embedding: cfg!(feature = "vector").then(|| vec![1.0, 0.0]),
                metadata: BTreeMap::new(),
            })
            .unwrap();
        let hits = index.search("graph", None, SearchMode::Text, 1).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "one");
        #[cfg(feature = "vector")]
        {
            let hits = index
                .search("", Some(&[1.0, 0.0]), SearchMode::Vector, 1)
                .unwrap();
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0].id, "one");
        }
    }
    #[cfg(feature = "full")]
    {
        db.query("CALL project_graph('G', ['Probe', 'Other'], ['LINK'])")
            .unwrap();
        assert_eq!(
            db.query("CALL page_rank('G') RETURN node, pagerank_score")
                .unwrap()
                .rows
                .len(),
            2
        );
        db.run_scheduled_background_schema_maintenance(1).unwrap();
    }
    println!(
        "graph/sql-ok text={} vector={} full={}",
        cfg!(feature = "text"),
        cfg!(feature = "vector"),
        cfg!(feature = "full")
    );
}

fn main() {
    #[cfg(feature = "runtime")]
    {
        use hawdb::{
            IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeResourceSnapshot,
            TokioRuntimeAdapter, TokioRuntimeConfig,
        };
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig::default(),
            RuntimeResourceSnapshot::detect(),
            IoConcurrencyBudget::new(1, 1),
        );
        let runtime = TokioRuntimeAdapter::owned(governor, TokioRuntimeConfig::default()).unwrap();
        runtime
            .block_on(runtime.execute_blocking(
                hawdb::RuntimeWorkRequest::foreground_query(1024 * 1024, 1024),
                hawdb::RuntimeTaskContext::default(),
                |_| {
                    database_workload();
                    Ok::<_, hawdb::HawDBError>(())
                },
            ))
            .unwrap()
            .unwrap();
    }
    #[cfg(all(feature = "database", not(feature = "runtime")))]
    database_workload();
    #[cfg(not(feature = "database"))]
    println!("empty-host-ok");
}

// Keep Cargo and Bazel qualification on the same facade-only workload.
#[cfg(all(test, feature = "database"))]
#[test]
fn consumer_profile_workload() {
    main();
}
