use super::super::relational_query_limits_with_payload;
use super::*;

#[test]
fn relational_index_read_row_budget_matches_intermediate_limit() {
    let config = DatabaseConfig {
        max_read_result_rows: Some(4_097),
        ..DatabaseConfig::default()
    };

    let limits = relational_query_limits_with_payload(&config, None, None);

    assert_eq!(limits.max_intermediate_rows, 4_097);
    assert_eq!(limits.index_read.max_rows.get(), 4_097);
    assert_eq!(
        limits.index_read.max_pages.get(),
        4_097usize
            .saturating_mul(skein_storage::DEFAULT_RELATIONAL_INDEX_READ_TREE_HEIGHT as usize)
    );
    assert_eq!(
        limits.index_read.max_bytes.get(),
        limits
            .index_read
            .max_pages
            .get()
            .saturating_mul(skein_storage::DEFAULT_IMMUTABLE_INDEX_PAGE_BYTES)
    );
    assert_eq!(
        limits.index_read.max_file_bytes,
        config.max_relational_index_read_bytes.get()
    );
}

#[test]
fn numeric_scan_filter_project_is_default_morsel_eligible() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Item").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Item(score) TYPE INT")
        .unwrap();

    let admission = db
        .runtime_admission_plan(
            "MATCH (n:Item) WHERE n.score >= 10 RETURN n.score AS score",
            &BTreeMap::new(),
        )
        .unwrap();

    assert!(admission.parallel_execution_eligible);
    assert_eq!(admission.max_parallelism, 1);
}

#[test]
fn vector_seed_admission_uses_optimizer_resource_contract() {
    let db = Database::new();
    let parameters = BTreeMap::from([(
        "embedding".to_string(),
        Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
    )]);

    let admission = db
        .runtime_admission_plan(
            "CALL vector_search($embedding, topK := 4) RETURN id, score",
            &parameters,
        )
        .unwrap();

    assert!(admission.parallel_execution_eligible);
    assert_eq!(
        admission.max_parallelism,
        crate::executor::MAX_MORSEL_PARALLELISM
    );
    assert!(
        admission.estimated_memory_bytes
            >= u64::try_from(db.config.execution_memory.blocking_operator_bytes.get())
                .unwrap()
                .saturating_mul(3)
    );
}

#[test]
fn parameterized_create_and_index_seek_execute_end_to_end() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    db.query_with_params(
        "CREATE (:Memory {id: $id, title: $title})",
        &BTreeMap::from([
            ("id".to_string(), Value::Int(42)),
            (
                "title".to_string(),
                Value::String("Parameterized memory".to_string()),
            ),
        ]),
    )
    .unwrap();
    for id in 100..116 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}, title: 'Parameterized extra {id}'}})"
        ))
        .unwrap();
    }

    let params = BTreeMap::from([("id".to_string(), Value::Int(42))]);
    let explain = db
        .explain_query_with_params(
            "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title",
            &params,
        )
        .unwrap();
    assert!(explain.trace.selected_plan.contains("IndexNodeSeek"));

    let output = db
        .query_with_params(
            "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title",
            &params,
        )
        .unwrap();
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Parameterized memory".to_string()))
    );
}

#[test]
fn durable_source_filter_uses_segment_scan_through_query_runtime() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("skein-source-query-runtime-{nonce}"));
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Source {id: 'source-alpha', space_id: 'alpha', source_type: 'file'})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source-beta', space_id: 'beta', source_type: 'file'})")
        .unwrap();
    db.checkpoint().unwrap();

    let query = "MATCH (s:Source) WHERE s.space_id = $space_id RETURN s.id AS id";
    let parameters = BTreeMap::from([("space_id".to_string(), Value::String("alpha".to_string()))]);
    let explain = db.explain_query_with_params(query, &parameters).unwrap();
    assert!(explain
        .physical_plan
        .explain(0)
        .contains("SourceSegmentScan"));
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose SourceSegmentScan")));
    let admission = db.runtime_admission_plan(query, &parameters).unwrap();
    assert_eq!(admission.required_io_slots, 2);

    let output = db.query_with_params(query, &parameters).unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("source-alpha".to_string()))
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn source_segment_scan_falls_back_after_uncheckpointed_mutation() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("skein-source-query-fallback-{nonce}"));
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Source {id: 'source-checkpointed', space_id: 'alpha'})")
        .unwrap();
    db.checkpoint().unwrap();
    db.query("CREATE (:Source {id: 'source-uncheckpointed', space_id: 'alpha'})")
        .unwrap();

    let query = "MATCH (s:Source) WHERE s.space_id = 'alpha' RETURN s.id AS id ORDER BY id ASC";
    let explain = db.explain_query(query).unwrap();
    assert!(explain
        .physical_plan
        .explain(0)
        .contains("SourceSegmentScan"));

    let output = db.query(query).unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("source-checkpointed".to_string()))
    );
    assert_eq!(
        output.rows[1].get("id"),
        Some(&Value::String("source-uncheckpointed".to_string()))
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn database_config_caps_optimizer_groups_for_explain() {
    let db = Database::new_with_config(DatabaseConfig {
        max_optimizer_groups: Some(2),
        ..DatabaseConfig::default()
    });

    let explain = db
        .explain_query(
            "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title ORDER BY title ASC LIMIT 1",
        )
        .unwrap();

    assert!(explain.trace.warnings.is_empty());
    assert!(explain.trace.groups > 2);
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.contains("max_groups=2") && decision.contains("same physical alternatives")
    }));
    assert!(explain.trace.selected_plan.contains("TopNExec"));
}

#[test]
fn orders_and_limits_by_projected_alias() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Beta'})").unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Gamma'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Alpha'})")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (m:Memory) RETURN m.title AS title ORDER BY title ASC LIMIT $limit",
            &BTreeMap::from([("limit".to_string(), Value::Int(2))]),
        )
        .unwrap();

    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Alpha".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Beta".to_string()))
    );
}

#[test]
fn orders_by_unprojected_property_with_offset() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'First', rank: 3})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Second', rank: 1})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Third', rank: 2})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN m.title AS title ORDER BY m.rank ASC SKIP 1 LIMIT 1")
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Third".to_string()))
    );
}

#[test]
fn distinct_return_deduplicates_before_order_and_limit() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'task'})").unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 4, kind: 'thread'})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN DISTINCT m.kind AS kind ORDER BY kind ASC LIMIT 2")
        .unwrap();

    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("kind"),
        Some(&Value::String("note".to_string()))
    );
    assert_eq!(
        output.rows[1].get("kind"),
        Some(&Value::String("task".to_string()))
    );

    let explain = db
        .explain_query("MATCH (m:Memory) RETURN DISTINCT m.kind AS kind ORDER BY kind ASC")
        .unwrap();
    assert!(explain.physical_plan.explain(0).contains("DistinctExec"));
    assert!(explain.trace.selected_plan.contains("DistinctExec"));
}

#[test]
fn database_config_caps_read_query_result_rows() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(2),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Three'})")
        .unwrap();

    let error = db
        .query("MATCH (m:Memory) RETURN m.title AS title ORDER BY title ASC")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("exceeding max_read_result_rows 2"));

    let output = db
        .query("MATCH (m:Memory) RETURN m.title AS title ORDER BY title ASC LIMIT 2")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
}

#[test]
fn database_config_defaults_to_bounded_read_results() {
    let config = DatabaseConfig::default();

    assert_eq!(
        config.max_read_result_rows,
        Some(crate::DEFAULT_MAX_READ_RESULT_ROWS)
    );
    assert_eq!(
        config.max_read_result_payload_bytes,
        Some(crate::DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES)
    );
}

#[test]
fn relational_query_index_limits_separate_logical_work_from_file_io() {
    let config = DatabaseConfig::default();

    let limits = super::super::relational_query_limits_with_payload(&config, None, None);
    let index = limits.index_read;
    let expected_pages = crate::DEFAULT_MAX_READ_RESULT_ROWS
        .saturating_mul(skein_storage::DEFAULT_RELATIONAL_INDEX_READ_TREE_HEIGHT as usize);

    assert_eq!(index.max_rows.get(), crate::DEFAULT_MAX_READ_RESULT_ROWS);
    assert_eq!(index.max_pages.get(), expected_pages);
    assert_eq!(
        index.max_bytes.get(),
        expected_pages.saturating_mul(skein_storage::DEFAULT_IMMUTABLE_INDEX_PAGE_BYTES)
    );
    assert_eq!(
        index.max_file_bytes,
        config.max_relational_index_read_bytes.get()
    );
}

#[test]
fn database_config_caps_collected_read_query_payload() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_read_result_payload_bytes: Some(16),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 1, title: 'payload exceeds the configured budget'})")
        .unwrap();

    let error = db
        .query("MATCH (m:Memory) RETURN m.title AS title")
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("max_read_result_payload_bytes 16"),
        "unexpected payload budget error: {error}"
    );
}

#[test]
fn read_transaction_streaming_options_cannot_relax_database_payload_cap() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_read_result_payload_bytes: Some(16),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 1, title: 'payload exceeds the configured budget'})")
        .unwrap();

    let mut read = db.begin_read_transaction();
    let mut delivered_rows = 0usize;
    let error = read
        .query_streaming(
            "MATCH (m:Memory) RETURN m.title AS title",
            QueryStreamOptions {
                max_rows: Some(usize::MAX),
                max_payload_bytes: Some(usize::MAX),
            },
            |_| {
                delivered_rows += 1;
                Ok(())
            },
        )
        .unwrap_err();
    assert_eq!(delivered_rows, 0);
    assert!(
        error.to_string().contains("max_payload_bytes 16"),
        "unexpected streaming payload budget error: {error}"
    );
}

#[test]
fn database_config_can_explicitly_allow_unbounded_read_results() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: None,
        max_read_result_payload_bytes: None,
        ..DatabaseConfig::default()
    });
    let title = "x".repeat(128 * 1024);
    db.query_with_params(
        "CREATE (:Memory {id: 1, title: $title})",
        &BTreeMap::from([("title".to_string(), Value::String(title.clone()))]),
    )
    .unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN m.title AS title")
        .unwrap();
    assert_eq!(output.rows[0].get("title"), Some(&Value::String(title)));
}

#[test]
fn read_transaction_inherits_database_result_row_cap() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(1),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();

    let mut read = db.begin_read_transaction();
    let error = read
        .query("MATCH (m:Memory) RETURN m.title AS title ORDER BY title ASC")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("exceeding max_read_result_rows 1"));
}

#[test]
fn read_only_database_rejects_cypher_mutations_before_writing() {
    let mut db = Database::new_with_config(DatabaseConfig {
        read_only: true,
        ..DatabaseConfig::default()
    });

    let error = db
        .query("CREATE (:Memory {id: 1, title: 'Blocked'})")
        .unwrap_err();
    assert!(error.to_string().contains("read-only mode"));

    let output = db
        .query("MATCH (m:Memory) RETURN m.title AS title")
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn read_only_rejected_mutations_do_not_populate_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        read_only: true,
        max_plan_cache_entries: Some(8),
        ..DatabaseConfig::default()
    });

    let error = db
        .query("CREATE (:Memory {id: 1, title: 'Blocked'})")
        .unwrap_err();
    assert!(error.to_string().contains("read-only mode"));

    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 0);
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, 0);
    assert_eq!(stats.evictions, 0);
}

#[test]
fn unlabeled_match_reads_and_updates_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Memory'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 2, name: 'Entity'})")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (n) WHERE n.id IN $ids RETURN n.id AS id ORDER BY id ASC",
            &BTreeMap::from([(
                "ids".to_string(),
                Value::List(vec![Value::Int(1), Value::Int(2)]),
            )]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(2)));

    let output = db
        .query("MATCH (n) WHERE n.id = 2 SET n.community_id = 7")
        .unwrap();
    assert_eq!(output.rows.len(), 1);

    let output = db
        .query("MATCH (n) WHERE n.community_id = 7 RETURN n.id AS id")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(2)));
}

#[test]
fn multi_label_match_reads_any_listed_label() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 1, title: 'Memory'})-[:MENTIONS]->(:Entity {id: 2, name: 'Entity'})",
    )
    .unwrap();
    db.query("CREATE (:Source {id: 3, original_name: 'Source'})")
        .unwrap();

    let output = db
        .query("MATCH (n:Entity:Memory) RETURN n.id AS id ORDER BY id ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(2)));

    let output = db
        .query(
            "MATCH (m:Memory {id: 1})-[r]-(neighbor:Entity:Memory) RETURN DISTINCT neighbor.id AS id",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(2)));

    let output = db
        .query("MATCH (n:MissingLabel) RETURN n.id AS id")
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn variable_return_items_project_graph_records() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', title: 'Memory'})-[:MENTIONS {weight: 3}]->(:Entity {id: 'e1', name: 'Entity'})")
            .unwrap();

    let output = db.query("MATCH (m:Memory {id: 'm1'}) RETURN m").unwrap();
    assert_eq!(output.rows.len(), 1);
    let Some(Value::Map(memory)) = output.rows[0].get("m") else {
        panic!("expected projected memory map");
    };
    assert_eq!(memory.get("id"), Some(&Value::String("m1".to_string())));
    assert_eq!(
        memory.get("title"),
        Some(&Value::String("Memory".to_string()))
    );
    assert_eq!(
        memory.get("labels"),
        Some(&Value::List(vec![Value::String("Memory".to_string())]))
    );

    let output = db
        .query("MATCH (m:Memory {id: 'm1'})-[r:MENTIONS]->(e:Entity) RETURN r")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    let Some(Value::Map(relationship)) = output.rows[0].get("r") else {
        panic!("expected projected relationship map");
    };
    assert_eq!(
        relationship.get("type"),
        Some(&Value::String("MENTIONS".to_string()))
    );
    assert_eq!(relationship.get("weight"), Some(&Value::Int(3)));
    assert_eq!(relationship.get("source_id"), Some(&Value::Int(0)));
    assert_eq!(relationship.get("target_id"), Some(&Value::Int(1)));
}

#[test]
fn read_transaction_streams_rows_with_row_and_payload_budgets() {
    let execution_memory = crate::executor::ExecutionMemoryConfig {
        batch_rows: std::num::NonZeroUsize::new(1).unwrap(),
        ..crate::executor::ExecutionMemoryConfig::default()
    };
    let mut db = Database::new_with_config(DatabaseConfig {
        execution_memory,
        ..DatabaseConfig::default()
    });
    for (id, title) in [(1, "alpha"), (2, "beta"), (3, "gamma")] {
        db.query(&format!("CREATE (:Memory {{id: {id}, title: '{title}'}})"))
            .unwrap();
    }

    let mut tx = db.begin_read_transaction();
    let mut titles = Vec::new();
    let report = tx
        .query_streaming(
            "MATCH (m:Memory) RETURN m.title AS title ORDER BY title ASC LIMIT 2",
            QueryStreamOptions {
                max_rows: Some(2),
                max_payload_bytes: Some(1024),
            },
            |row| {
                titles.push(row["title"].clone());
                Ok(())
            },
        )
        .unwrap();

    assert!(report.fully_streamed);
    assert_eq!(report.output_rows, 2);
    assert!(report.output_payload_bytes > 0);
    assert_eq!(
        titles,
        vec![
            Value::String("alpha".to_string()),
            Value::String("beta".to_string())
        ]
    );

    let mut tx = db.begin_read_transaction();
    let mut delivered_rows = 0usize;
    let error = tx
        .query_streaming(
            "MATCH (m:Memory) RETURN m.title AS title ORDER BY title ASC",
            QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: Some(1024),
            },
            |_| {
                delivered_rows += 1;
                Ok(())
            },
        )
        .unwrap_err();
    assert_eq!(delivered_rows, 0);
    assert!(error
        .to_string()
        .contains("exceeding max_read_result_rows 1"));

    let mut tx = db.begin_read_transaction();
    let first_row_payload_bytes = crate::executor::map_payload_bytes(&BTreeMap::from([(
        "title".to_string(),
        Value::String("alpha".to_string()),
    )]));
    let mut delivered_rows = 0usize;
    let error = tx
        .query_streaming(
            "MATCH (m:Memory) RETURN m.title AS title ORDER BY title ASC LIMIT 2",
            QueryStreamOptions {
                max_rows: Some(2),
                max_payload_bytes: Some(first_row_payload_bytes),
            },
            |_| {
                delivered_rows += 1;
                Ok(())
            },
        )
        .unwrap_err();
    assert_eq!(delivered_rows, 0);
    assert!(error
        .to_string()
        .contains(&format!("max_payload_bytes {first_row_payload_bytes}")));
}

#[test]
fn read_transaction_exposes_borrowed_rows_to_immediate_consumers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 7, title: 'borrowed payload'})")
        .unwrap();

    let mut tx = db.begin_read_transaction();
    let mut observed = Vec::new();
    let report = tx
        .query_with_params_streaming_ref(
            "MATCH (m:Memory) RETURN m.id AS id, m.title AS title",
            &BTreeMap::new(),
            QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: Some(1024),
            },
            |row| {
                observed.push((
                    row.get("id").and_then(crate::ValueRef::as_i64),
                    row.get("title")
                        .and_then(crate::ValueRef::as_str)
                        .map(str::to_owned),
                ));
                Ok(())
            },
        )
        .unwrap();

    assert_eq!(report.output_rows, 1);
    assert_eq!(observed, vec![(Some(7), Some("borrowed payload".into()))]);
}
