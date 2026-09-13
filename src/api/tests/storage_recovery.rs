use super::*;
use crate::store::set_wal_apply_failpoint;
use crate::StorageResidencyMode;

#[path = "storage_recovery/wal_tail.rs"]
mod wal_tail;

#[test]
fn strict_append_sql_replays_from_wal_after_reopen() {
    let path = unique_test_dir("strict_append_sql_reopen");
    {
        let mut db = Database::open(&path).unwrap();
        db.query_sql(
            "CREATE TABLE events (\
               stream_id TEXT NOT NULL, \
               sequence BIGINT NOT NULL, \
               payload TEXT NOT NULL\
             ) WITH (\
               storage_mode = 'strict_append', \
               partition_key = 'stream_id', \
               order_key = 'sequence'\
             )",
        )
        .unwrap();
        db.query_sql_with_params(
            "INSERT INTO events (stream_id, sequence, payload) VALUES ($1, $2, $3)",
            &[
                Value::String("thread-1".to_string()),
                Value::Int(1),
                Value::String("durable".to_string()),
            ],
        )
        .unwrap();
    }

    let db = Database::open(&path).unwrap();
    let output = db
        .begin_read_transaction()
        .query_sql_with_params(
            "SELECT payload FROM events \
             WHERE stream_id = $1 ORDER BY sequence ASC LIMIT 10",
            &[Value::String("thread-1".to_string())],
        )
        .unwrap();
    assert_eq!(
        output.rows[0].get("payload"),
        Some(&Value::String("durable".to_string()))
    );

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn generated_append_watermark_survives_checkpoint_and_wal_replay() {
    let path = unique_test_dir("generated_append_checkpoint_reopen");
    {
        let mut db = Database::open(&path).unwrap();
        db.query_sql(
            "CREATE TABLE events (\
               stream_id TEXT NOT NULL, \
               sequence BIGINT NOT NULL, \
               payload TEXT NOT NULL\
             ) WITH (\
               storage_mode = 'strict_append', \
               partition_key = 'stream_id', \
               order_key = 'sequence', \
               generated_order = 'commit_sequence'\
             )",
        )
        .unwrap();
        db.query_sql(
            "INSERT INTO events (stream_id, payload) VALUES \
             ('thread-1', 'one'), ('thread-2', 'two')",
        )
        .unwrap();
        db.checkpoint().unwrap();
        db.query_sql("INSERT INTO events (stream_id, payload) VALUES ('thread-1', 'three')")
            .unwrap();
    }

    {
        let mut db = Database::open(&path).unwrap();
        let result = db
            .append_transaction_with_result(skein_storage::AppendTransaction {
                writes: vec![skein_storage::AppendWrite::AppendGenerated {
                    table: "events".to_string(),
                    rows: vec![skein_storage::AppendGeneratedRow::new(vec![
                        skein_storage::RelationalValue::Text("thread-1".to_string()),
                        skein_storage::RelationalValue::Text("four".to_string()),
                    ])],
                }],
            })
            .unwrap();
        assert_eq!(
            result.mutations[0].generated_order_keys,
            vec![skein_storage::RelationalKey(vec![
                skein_storage::RelationalValue::BigInt(4),
            ])]
        );
        let output = db
            .query_sql(
                "SELECT sequence FROM events WHERE stream_id = 'thread-1' \
                 ORDER BY sequence LIMIT 10",
            )
            .unwrap();
        assert_eq!(
            output
                .rows
                .iter()
                .map(|row| row["sequence"].clone())
                .collect::<Vec<_>>(),
            vec![Value::Int(1), Value::Int(3), Value::Int(4)]
        );
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn wal_pressure_schedules_and_completes_a_bounded_background_checkpoint() {
    let path = unique_test_dir("wal_pressure_background_checkpoint");
    let config = DatabaseConfig {
        max_wal_replay_bytes: Some(32 * 1024),
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&path, config).unwrap();
    let payload = "x".repeat(128);

    let pressure = (0..256)
        .find_map(|id| {
            db.query_with_params(
                "CREATE (:Memory {id: $id, payload: $payload})",
                &BTreeMap::from([
                    ("id".to_string(), Value::Int(id)),
                    ("payload".to_string(), Value::String(payload.clone())),
                ]),
            )
            .unwrap();
            let pressure = db.storage_pressure_snapshot();
            (pressure.state == crate::StoragePressureState::SpeedUpMaintenance).then_some(pressure)
        })
        .expect("WAL should reach its soft pressure threshold before mutation backpressure");
    assert!(pressure.recommends_checkpoint());
    assert!(pressure.wal_pressure_ratio_per_million.unwrap() >= 700_000);

    let candidates = db.background_maintenance_candidates(
        None,
        BackgroundMaintenanceOptions {
            include_schema_maintenance: false,
            include_property_index_projection: false,
            include_search_projection_graph_delta_freshness: false,
            include_search_projection_rebuild: false,
            include_search_projection_metadata_repair: false,
            include_skein_lightning_bootstrap_export: false,
            include_external_content_artifact_jobs: false,
            ..BackgroundMaintenanceOptions::default()
        },
    );
    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].kind,
        BackgroundMaintenanceKind::StorageCheckpoint
    );
    assert_eq!(candidates[0].plan.request.class, WorkClass::Mutation);

    db.checkpoint_background(
        &LocalQosPolicy::default(),
        &LocalQosState::default(),
        BackgroundWorkHint::default(),
    )
    .unwrap();
    let after = db.storage_pressure_snapshot();
    assert!(!after.recommends_checkpoint());
    assert_eq!(after.current_commit_epoch, after.checkpoint_commit_epoch);
    drop(db);

    let mut reopened = Database::open(&path).unwrap();
    let rows = reopened
        .query("MATCH (m:Memory) RETURN count(m) AS count")
        .unwrap();
    assert!(matches!(rows.rows[0].get("count"), Some(Value::Int(count)) if *count > 0));
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn wal_pressure_rejects_before_append_and_leaves_no_partial_mutation() {
    let path = unique_test_dir("wal_pressure_rejects_before_append");
    let config = DatabaseConfig {
        max_wal_replay_bytes: Some(1024),
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&path, config).unwrap();
    let error = db
        .query_with_params(
            "CREATE (:Memory {id: $id, payload: $payload})",
            &BTreeMap::from([
                ("id".to_string(), Value::Int(1)),
                ("payload".to_string(), Value::String("x".repeat(2048))),
            ]),
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("WAL append rejected by storage pressure"));
    assert_eq!(db.store.commit_epoch(), 1);
    drop(db);

    let mut reopened = Database::open(&path).unwrap();
    let rows = reopened
        .query("MATCH (m:Memory) RETURN m.id AS id")
        .unwrap();
    assert!(rows.rows.is_empty());
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn post_wal_apply_failure_poisons_handle_until_reopen() {
    let path = unique_test_dir("post_wal_apply_poison");
    let mut db = Database::open(&path).unwrap();
    let mut stable_read = db.begin_read_transaction();
    let mut transaction = db.begin_transaction();
    transaction.query("CREATE (:Memory {id: 'first'})").unwrap();
    transaction
        .query("CREATE (:Memory {id: 'second'})")
        .unwrap();

    set_wal_apply_failpoint(Some(1));
    let commit_error = transaction.commit().unwrap_err();
    set_wal_apply_failpoint(None);

    assert!(commit_error
        .to_string()
        .contains("injected failure while applying a durable WAL batch"));
    assert!(db.storage_handle_poisoned());
    let stable_output = stable_read
        .query("MATCH (m:Memory) RETURN m.id AS id")
        .unwrap();
    assert!(stable_output.rows.is_empty());
    let read_error = db
        .query("MATCH (m:Memory) RETURN m.id AS id ORDER BY id")
        .unwrap_err();
    assert!(read_error.to_string().contains("close and reopen"));
    let checkpoint_error = db.checkpoint().unwrap_err();
    assert!(checkpoint_error.to_string().contains("close and reopen"));

    drop(db);
    let mut reopened = Database::open(&path).unwrap();
    assert!(!reopened.storage_handle_poisoned());
    let output = reopened
        .query("MATCH (m:Memory) RETURN m.id AS id ORDER BY id")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("first".to_string()))
    );
    assert_eq!(
        output.rows[1].get("id"),
        Some(&Value::String("second".to_string()))
    );

    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn forced_out_of_core_checkpoint_reopen_and_mutation_are_equivalent() {
    let path = unique_test_dir("forced_out_of_core");
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: 1024 * 1024,
        ..DatabaseConfig::default()
    };
    {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        db.query(
            "CREATE (:Memory {id: 1, title: 'One'})-[:MENTIONS {weight: 1}]->(:Entity {id: 10, name: 'Rust'})",
        )
        .unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
        db.query("CALL project_graph('MemoryGraph', ['Memory', 'Entity'], ['MENTIONS'])")
            .unwrap();
        db.checkpoint().unwrap();

        let residency = db.storage_residency_report();
        assert!(residency.out_of_core);
        assert_eq!(residency.delta_node_count, 0);
        assert_eq!(residency.delta_relationship_count, 0);
        assert!(!residency.checkpoint_statistics_complete);
        assert!(residency.checkpoint_statistics_stale);

        let rows = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN m.id AS memory_id, e.name AS entity_name",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].get("memory_id"), Some(&Value::Int(1)));
        let orphans = db
            .query("MATCH (m:Memory) WHERE NOT (m)-[:MENTIONS]->(:Entity) RETURN m.id AS memory_id")
            .unwrap();
        assert_eq!(orphans.rows.len(), 1);
        assert_eq!(orphans.rows[0].get("memory_id"), Some(&Value::Int(2)));
        let existing_pair = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE NOT EXISTS { MATCH (m)-[:MENTIONS]->(e) } RETURN m.id AS memory_id",
            )
            .unwrap();
        assert!(existing_pair.rows.is_empty());
        let page_rank = db
            .query("CALL page_rank('MemoryGraph') RETURN node, pagerank_score")
            .unwrap();
        assert_eq!(page_rank.rows.len(), 3);
        let snapshot = db.try_export_canonical_graph_snapshot().unwrap();
        assert_eq!(snapshot.nodes.len(), 3);
        assert_eq!(snapshot.relationships.len(), 1);
        let statistics = db.statistics();
        assert!(!statistics.advanced_statistics_complete);
        assert_eq!(statistics.node_count, 3);
        assert_eq!(statistics.relationship_count, 1);
        assert!(statistics.property_distinct_counts.is_empty());
        assert!(statistics.rel_property_distinct_counts.is_empty());
        assert!(statistics.path_counts.is_empty());

        let updated = db
            .query(
                "MATCH (m:Memory) WHERE m.id = 1 SET m.title = 'Updated' RETURN m.id AS memory_id",
            )
            .unwrap();
        assert_eq!(updated.rows.len(), 1);
        assert_eq!(updated.rows[0].get("memory_id"), Some(&Value::Int(1)));
        db.query(
            "MATCH (m:Memory {id: 2}), (e:Entity {id: 10}) CREATE (m)-[:MENTIONS {weight: 2}]->(e)",
        )
        .unwrap();
        db.checkpoint().unwrap();
    }

    {
        let mut db = Database::open_with_config(&path, config).unwrap();
        let residency = db.storage_residency_report();
        assert!(residency.out_of_core);
        assert_eq!(residency.delta_node_count, 0);
        assert_eq!(residency.delta_relationship_count, 0);

        let rows = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN m.id AS memory_id, m.title AS title ORDER BY memory_id ASC",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0].get("title"),
            Some(&Value::String("Updated".to_string()))
        );
        assert_eq!(rows.rows[1].get("memory_id"), Some(&Value::Int(2)));

        let statistics = db.statistics();
        assert_eq!(statistics.node_count, 3);
        assert_eq!(statistics.relationship_count, 2);
        assert!(!statistics.advanced_statistics_complete);
        assert!(statistics.property_distinct_counts.is_empty());
        assert!(statistics.rel_property_distinct_counts.is_empty());
        assert!(statistics.path_counts.is_empty());
        assert!(
            statistics.computed_at_commit_epoch < db.basic_statistics().computed_at_commit_epoch
        );

        let residency = db.storage_residency_report();
        assert!(!residency.checkpoint_statistics_complete);
        assert!(residency.checkpoint_statistics_stale);
        assert!(residency.segment_cache_miss_count > 0);
        assert_eq!(residency.segment_cache_digest_mismatch_count, 0);
        let cache = db.segment_cache_snapshot().unwrap();
        assert!(cache.resident_bytes <= cache.capacity_bytes);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_reads_and_mutations_include_checkpointed_canonical_rows() {
    let path = unique_test_dir("out_of_core_typed_canonical_rows");
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: 1024 * 1024,
        ..DatabaseConfig::default()
    };
    {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        db.query(
            "CREATE (:Memory {id: 'memory:one', title: 'One'})-[:LINKS]->(:Entity {id: 'entity:rust', name: 'Rust'})",
        )
        .unwrap();
        db.checkpoint().unwrap();

        let residency = db.storage_residency_report();
        assert_eq!(residency.delta_node_count, 0);
        assert_eq!(residency.delta_relationship_count, 0);

        let entity = db
            .query_entity_via_cypher(&KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory:one".to_string(),
            })
            .unwrap();
        assert_eq!(
            entity.entity.unwrap().properties.get("title"),
            Some(&Value::String("One".to_string()))
        );

        let properties = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity:rust".to_string(),
                }],
                property_names: vec!["name".to_string()],
            })
            .unwrap();
        assert_eq!(
            properties.rows[0].properties.get("name"),
            Some(&Some(Value::String("Rust".to_string())))
        );

        let neighbors = db
            .query_neighbors_via_cypher(&KnowledgeNeighborsRequest {
                label: "Memory".to_string(),
                external_id: "memory:one".to_string(),
                relationship_type: Some("LINKS".to_string()),
                direction: KnowledgeNeighborDirection::Outgoing,
                limit: 4,
                max_hops: 1,
            })
            .unwrap();
        assert_eq!(neighbors.paths.len(), 1);
        assert_eq!(
            neighbors.paths[0].target_external_id.as_deref(),
            Some("entity:rust")
        );

        let created = db
            .create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory:one".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity:rust".to_string(),
                },
                relationship_type: "MENTIONS".to_string(),
                properties: BTreeMap::from([("weight".to_string(), Value::Int(2))]),
            })
            .unwrap();
        assert!(created.matched);
        assert_eq!(created.created_relationship_count, 1);

        let relationships = db
            .query_relationships_via_cypher(&KnowledgeRelationshipsRequest {
                seeds: vec![KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory:one".to_string(),
                }],
                relationship_type: None,
                direction: KnowledgeNeighborDirection::Outgoing,
                limit_per_seed: 4,
            })
            .unwrap();
        assert_eq!(relationships.relationship_count, 2);
        db.checkpoint().unwrap();
    }

    {
        let db = Database::open_with_config(&path, config).unwrap();
        let neighbors = db
            .query_neighbors_via_cypher(&KnowledgeNeighborsRequest {
                label: "Memory".to_string(),
                external_id: "memory:one".to_string(),
                relationship_type: Some("MENTIONS".to_string()),
                direction: KnowledgeNeighborDirection::Outgoing,
                limit: 4,
                max_hops: 1,
            })
            .unwrap();
        assert_eq!(neighbors.paths.len(), 1);
        assert_eq!(
            neighbors.paths[0].relationship_properties.get("weight"),
            Some(&Value::Int(2))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_read_fails_closed_when_an_out_of_core_segment_is_corrupted() {
    let path = unique_test_dir("out_of_core_typed_read_corruption");
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            segment_cache_capacity_bytes: 1024 * 1024,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'memory:one', title: 'Corrupt me'})")
        .unwrap();
    db.checkpoint().unwrap();

    let canonical_path = path.join("canonical.1.skein");
    let mut bytes = std::fs::read(&canonical_path).unwrap();
    bytes[24] ^= 0xff;
    std::fs::write(&canonical_path, bytes).unwrap();

    let error = db
        .query_entity_via_cypher(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory:one".to_string(),
        })
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("failed content digest verification"),
        "unexpected typed read error: {error}"
    );
    assert_eq!(
        db.storage_residency_report()
            .segment_cache_digest_mismatch_count,
        1
    );
    assert!(db.storage_handle_poisoned());
    let poisoned = db
        .query("MATCH (m:Memory) RETURN m.id AS memory_id")
        .unwrap_err();
    assert!(poisoned.to_string().contains("close and reopen"));

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn public_query_fails_closed_when_an_out_of_core_segment_is_corrupted() {
    let path = unique_test_dir("out_of_core_public_query_corruption");
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            segment_cache_capacity_bytes: 1024 * 1024,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Corrupt me'})")
        .unwrap();
    db.checkpoint().unwrap();

    let canonical_path = path.join("canonical.1.skein");
    let mut bytes = std::fs::read(&canonical_path).unwrap();
    bytes[24] ^= 0xff;
    std::fs::write(&canonical_path, bytes).unwrap();

    let error = db
        .query("MATCH (m:Memory) RETURN m.id AS memory_id")
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("failed content digest verification"),
        "unexpected query error: {error}"
    );
    assert_eq!(
        db.storage_residency_report()
            .segment_cache_digest_mismatch_count,
        1
    );
    assert!(db.storage_handle_poisoned());
    let poisoned = db
        .query("MATCH (m:Memory) RETURN m.id AS memory_id")
        .unwrap_err();
    assert!(poisoned.to_string().contains("close and reopen"));

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[ignore = "production profile; allocates a 256 MiB canonical dataset"]
fn production_sized_resource_profile_stays_within_admission_budgets() {
    let path = unique_test_dir("production_resource_profile");
    let node_count = 8_192usize;
    let body_bytes = 32 * 1024;
    let cache_budget = 32 * 1024 * 1024;
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: cache_budget,
        max_read_result_rows: Some(node_count),
        ..DatabaseConfig::default()
    };
    {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        // Seed the complete dataset without exceeding the default WAL record limit.
        let transaction_rows = 128;
        for start in (0..node_count).step_by(transaction_rows) {
            let mut tx = db.begin_transaction();
            for id in start..(start + transaction_rows).min(node_count) {
                tx.query_with_params(
                    "CREATE (:Memory {id: $id, body: $body})",
                    &BTreeMap::from([
                        ("id".to_string(), Value::Int(id as i64)),
                        ("body".to_string(), Value::String("x".repeat(body_bytes))),
                    ]),
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        db.checkpoint().unwrap();
        assert!(db.storage_residency_report().out_of_core);
    }

    let db = Database::open_with_config(&path, config).unwrap();
    let report = db
        .storage_resource_profile(
            "MATCH (m:Memory) RETURN m.id AS memory_id",
            &BTreeMap::new(),
            crate::StorageResourceProfileLimits {
                min_canonical_artifact_bytes: 256 * 1024 * 1024,
                max_steady_resident_bytes: 2 * 1024 * 1024 * 1024,
                max_peak_resident_bytes: 4 * 1024 * 1024 * 1024,
                max_total_page_faults: Some(10_100_000),
                max_minor_page_faults: cfg!(unix).then_some(10_000_000),
                max_major_page_faults: cfg!(unix).then_some(100_000),
                max_intermediate_rows: node_count * 4,
                max_intermediate_payload_bytes: 1536 * 1024 * 1024,
                max_output_rows: node_count,
                max_output_payload_bytes: 16 * 1024 * 1024,
                require_fully_streamed: true,
            },
        )
        .unwrap();

    assert!(
        report.resource_ready,
        "unexpected blockers: {:?}; profile: {}",
        report.blocker_codes,
        report.json()
    );
    assert!(report.after.canonical_artifact_bytes > cache_budget);
    assert!(report.after.segment_cache_resident_bytes <= cache_budget);
    assert!(report.after.segment_cache_miss_count > report.before.segment_cache_miss_count);
    assert_eq!(report.query.output_rows, node_count);
    let pipeline = &report.query.execution_profile.pipeline_memory_report;
    assert!(pipeline.intermediate_rows >= node_count);
    assert!(pipeline.output_payload_bytes > 0);
    assert!(pipeline.start_resident_bytes.is_some());
    assert!(pipeline.start_peak_resident_bytes.is_some());
    assert!(pipeline.steady_resident_bytes.is_some());
    assert!(pipeline.peak_resident_bytes.is_some());
    assert!(pipeline
        .steady_resident_growth_bytes
        .is_some_and(|bytes| bytes <= 128 * 1024 * 1024));
    assert!(pipeline
        .lifetime_peak_resident_growth_bytes
        .is_some_and(|bytes| bytes <= 128 * 1024 * 1024));
    assert!(pipeline.total_page_faults.is_some());
    assert_eq!(pipeline.minor_page_faults.is_some(), cfg!(unix));
    assert_eq!(pipeline.major_page_faults.is_some(), cfg!(unix));

    assert!(
        report.after.segment_cache_eviction_count > report.before.segment_cache_eviction_count
            || report.after.segment_cache_admission_rejection_count
                > report.before.segment_cache_admission_rejection_count
    );
    assert!(report.after.canonical_artifact_bytes >= report.limits.min_canonical_artifact_bytes);

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_storage_resource_profile_gates_larger_than_cache_reads() {
    let path = unique_test_dir("typed_storage_resource_profile");
    let cache_budget = 1024;
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: cache_budget,
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&path, config).unwrap();
    let mut transaction = db.begin_transaction();
    for id in 0..32 {
        transaction
            .query_with_params(
                "CREATE (:Memory {id: $id, body: $body})",
                &BTreeMap::from([
                    ("id".to_string(), Value::Int(id)),
                    ("body".to_string(), Value::String("x".repeat(1024))),
                ]),
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    db.checkpoint().unwrap();

    let report = db
        .storage_resource_profile(
            "MATCH (m:Memory) RETURN m.id AS memory_id",
            &BTreeMap::new(),
            crate::StorageResourceProfileLimits {
                min_canonical_artifact_bytes: 4096,
                max_steady_resident_bytes: u64::MAX,
                max_peak_resident_bytes: u64::MAX,
                max_total_page_faults: None,
                max_minor_page_faults: None,
                max_major_page_faults: None,
                max_intermediate_rows: 1024,
                max_intermediate_payload_bytes: 1024 * 1024,
                max_output_rows: 64,
                max_output_payload_bytes: 1024 * 1024,
                require_fully_streamed: true,
            },
        )
        .unwrap();

    assert!(
        report.resource_ready,
        "unexpected blockers: {:?}",
        report.blocker_codes
    );
    assert!(report.after.canonical_artifact_bytes > cache_budget);
    assert!(report.after.segment_cache_resident_bytes <= cache_budget);
    assert!(report.after.segment_cache_miss_count > report.before.segment_cache_miss_count);
    assert_eq!(report.query.output_rows, 32);
    assert_eq!(
        report.json()["protocol"],
        crate::STORAGE_RESOURCE_PROFILE_PROTOCOL
    );
    assert!(report.json()["execution"]["total_page_faults"].is_u64());
    assert_eq!(
        report.json()["execution"]["metric_capabilities"]["split_page_faults"],
        cfg!(unix)
    );
    assert_eq!(report.json()["resource_ready"], true);
    assert_eq!(report.json()["ready"], false);

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn platform_storage_resource_profile_emits_bound_evidence() {
    const EVIDENCE_PATH_ENV: &str = "SKEIN_TEST_STORAGE_RESOURCE_EVIDENCE_PATH";

    let path = unique_test_dir("platform_storage_resource_profile");
    let cache_budget = 1024;
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: cache_budget,
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&path, config).unwrap();
    let mut transaction = db.begin_transaction();
    for id in 0..32 {
        transaction
            .query_with_params(
                "CREATE (:Memory {id: $id, body: $body})",
                &BTreeMap::from([
                    ("id".to_string(), Value::Int(id)),
                    ("body".to_string(), Value::String("x".repeat(1024))),
                ]),
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    db.checkpoint().unwrap();

    let identity = crate::ProductionQualificationIdentity {
        source_revision: std::env::var("GITHUB_SHA")
            .unwrap_or_else(|_| "local-test-revision".to_string()),
        rust_toolchain: std::env::var("SKEIN_TEST_RUST_TOOLCHAIN")
            .unwrap_or_else(|_| "local-test-toolchain".to_string()),
        target_os: std::env::consts::OS.to_string(),
        target_arch: std::env::consts::ARCH.to_string(),
        enabled_features: vec![
            "acl".to_string(),
            "background-maintenance".to_string(),
            "full-text-search".to_string(),
            "graph-analytics".to_string(),
            "vector-search".to_string(),
        ],
        durable_format_version: 2,
        schema_version: 1,
        configuration_digest: "storage-resource-out-of-core-1k-cache-v1".to_string(),
        deployment_profile: "storage-resource-platform-ci".to_string(),
        dataset_fingerprint: "storage-resource-platform-fixture-v1".to_string(),
        canonical_graph_commit_epoch: db.commit_epoch(),
        policy_version: crate::PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
    let report = db
        .storage_resource_profile_for_production(
            "MATCH (m:Memory) RETURN m.id AS memory_id",
            &BTreeMap::new(),
            crate::StorageResourceProfileLimits {
                min_canonical_artifact_bytes: 4096,
                max_steady_resident_bytes: u64::MAX,
                max_peak_resident_bytes: u64::MAX,
                max_total_page_faults: Some(u64::MAX),
                max_minor_page_faults: cfg!(unix).then_some(u64::MAX),
                max_major_page_faults: cfg!(unix).then_some(u64::MAX),
                max_intermediate_rows: 1024,
                max_intermediate_payload_bytes: 1024 * 1024,
                max_output_rows: 64,
                max_output_payload_bytes: 1024 * 1024,
                require_fully_streamed: true,
            },
            crate::ProductionEvidenceBinding {
                identity: identity.clone(),
                generated_at_unix_seconds: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            },
            identity,
        )
        .unwrap();

    assert!(
        report.production_ready(),
        "unexpected blockers: {:?}",
        report.production_blocker_codes()
    );
    let report_json = report.json();
    assert_eq!(report_json["ready"], true);
    assert_eq!(report_json["resource_ready"], true);
    assert!(report_json["execution"]["steady_resident_bytes"].is_u64());
    assert!(report_json["execution"]["peak_resident_bytes"].is_u64());
    assert!(report_json["execution"]["total_page_faults"].is_u64());
    assert_eq!(
        report_json["execution"]["metric_capabilities"]["split_page_faults"],
        cfg!(unix)
    );
    if let Some(evidence_path) = std::env::var_os(EVIDENCE_PATH_ENV) {
        std::fs::write(evidence_path, report_json.to_string()).unwrap();
    }

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn external_optimizer_statistics_refresh_spills_and_persists_exact_stats() {
    let path = unique_test_dir("external_optimizer_statistics_refresh");
    let spill_root = path.join("statistics-spill");
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: 1024 * 1024,
        ..DatabaseConfig::default()
    };
    let refreshed_statistics = {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(body) TYPE TEXT")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory:one', kind: 'note'})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'entity:rust', kind: 'language'})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'entity:skein', kind: 'library'})")
            .unwrap();
        db.create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory:one".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity:rust".to_string(),
            },
            relationship_type: "LINKS".to_string(),
            properties: BTreeMap::from([("weight".to_string(), Value::Int(1))]),
        })
        .unwrap();
        db.create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
            source: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity:rust".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity:skein".to_string(),
            },
            relationship_type: "LINKS".to_string(),
            properties: BTreeMap::from([("weight".to_string(), Value::Int(2))]),
        })
        .unwrap();
        let mut transaction = db.begin_transaction();
        for id in 0..128 {
            let mixed_value = if id == 0 {
                Value::Int(1)
            } else {
                Value::Map(BTreeMap::from([("id".to_string(), Value::Int(id))]))
            };
            transaction
                .query_with_params(
                    "CREATE (:Memory {id: $id, body: $body, score: $score, mixed_value: $mixed_value})",
                    &BTreeMap::from([
                        ("id".to_string(), Value::String("filler".to_string())),
                        ("body".to_string(), Value::String("x".repeat(16 * 1024))),
                        ("score".to_string(), Value::Int(id % 8)),
                        ("mixed_value".to_string(), mixed_value),
                    ]),
                )
                .unwrap();
        }
        transaction.commit().unwrap();
        db.checkpoint().unwrap();
        assert!(!db.statistics().advanced_statistics_complete);

        db.query("CREATE (:Memory {id: 'memory:two', kind: 'task'})")
            .unwrap();
        db.create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory:two".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity:skein".to_string(),
            },
            relationship_type: "LINKS".to_string(),
            properties: BTreeMap::from([("weight".to_string(), Value::Int(3))]),
        })
        .unwrap();
        let source_epoch = db.basic_statistics().computed_at_commit_epoch;
        let report = db
            .refresh_optimizer_statistics_external(&crate::OptimizerStatisticsRefreshOptions {
                memory_budget_bytes: 8 * 1024,
                max_spill_bytes: 4 * 1024 * 1024,
                max_spill_runs: 128,
                max_input_records: 100_000,
                max_generated_facts: 100_000,
                max_path_expansions: 10_000,
                spill_directory: spill_root.clone(),
            })
            .unwrap();

        assert!(report.checkpoint_persisted);
        assert_eq!(report.source_commit_epoch, source_epoch);
        assert!(report.spill_run_count > 1);
        assert!(report.spilled_bytes > 0);
        assert!(report.peak_buffer_bytes <= 8 * 1024);
        assert!(report.excluded_property_group_count > 0);
        let statistics = db.statistics();
        assert!(statistics.advanced_statistics_complete);
        assert_eq!(statistics.computed_at_commit_epoch, source_epoch);
        assert!(!statistics
            .property_distinct_counts
            .keys()
            .any(|(_, property)| property == "body"));
        assert!(!statistics
            .property_histograms
            .keys()
            .any(|(_, property)| property == "body"));
        assert!(!statistics
            .property_distinct_counts
            .keys()
            .any(|(_, property)| property == "mixed_value"));
        assert_eq!(
            statistics
                .property_distinct_counts
                .iter()
                .find_map(|((_, property), count)| (property == "score").then_some(*count)),
            Some(8)
        );
        assert_eq!(
            statistics
                .rel_property_distinct_counts
                .values()
                .copied()
                .max(),
            Some(3)
        );
        assert_eq!(
            statistics.rel_type_source_counts.values().copied().max(),
            Some(3)
        );
        assert_eq!(
            statistics.rel_type_target_counts.values().copied().max(),
            Some(2)
        );
        assert!(statistics
            .bounded_path_counts
            .iter()
            .any(|((_, _, _, hop), count)| *hop == 2 && *count == 1));
        assert_eq!(
            std::fs::read_dir(&spill_root).unwrap().count(),
            0,
            "spill runs must be reclaimed after publication"
        );
        statistics
    };

    {
        let db = Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: StorageResidencyMode::Materialized,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        assert_eq!(db.statistics(), refreshed_statistics);
    }

    {
        let db = Database::open_with_config(&path, config).unwrap();
        let statistics = db.statistics();
        assert!(statistics.advanced_statistics_complete);
        assert_eq!(
            statistics
                .rel_property_distinct_counts
                .values()
                .copied()
                .max(),
            Some(3)
        );
        let residency = db.storage_residency_report();
        assert!(residency.checkpoint_statistics_complete);
        assert!(!residency.checkpoint_statistics_stale);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn external_optimizer_statistics_refresh_resamples_live_out_of_core_indexes() {
    let path = unique_test_dir("external_optimizer_index_statistics");
    let spill_root = path.join("statistics-spill");
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: 1024 * 1024,
        ..DatabaseConfig::default()
    };
    let (body_index_id, composite_index_id, refreshed_statistics) = {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(body) TYPE TEXT")
            .unwrap();
        let mut transaction = db.begin_transaction();
        for id in 0..20 {
            transaction
                .query_with_params(
                    "CREATE (:Memory {id: $id, body: $body})",
                    &BTreeMap::from([
                        ("id".to_string(), Value::Int(id)),
                        (
                            "body".to_string(),
                            Value::String(if id % 2 == 0 { "even" } else { "odd" }.to_string()),
                        ),
                    ]),
                )
                .unwrap();
        }
        transaction.commit().unwrap();
        db.checkpoint().unwrap();

        db.query("CREATE INDEX ON :Memory(body)").unwrap();
        db.query("CREATE INDEX ON :Memory(body, id)").unwrap();
        let body_index_id = db
            .property_indexes()
            .into_iter()
            .find(|index| index.property == "body")
            .unwrap()
            .id;
        let composite_index_id = db
            .composite_property_indexes()
            .into_iter()
            .find(|index| index.properties == ["body", "id"])
            .unwrap()
            .id;
        assert!(!db.statistics().index_samples.contains_key(&body_index_id));
        assert!(!db
            .statistics()
            .index_samples
            .contains_key(&composite_index_id));

        let options = crate::OptimizerStatisticsRefreshOptions {
            memory_budget_bytes: 4 * 1024,
            max_spill_bytes: 1024 * 1024,
            max_spill_runs: 64,
            max_input_records: 1_000,
            max_generated_facts: 10_000,
            max_path_expansions: 1_000,
            spill_directory: spill_root.clone(),
        };
        let report = db.refresh_optimizer_statistics_external(&options).unwrap();
        assert!(report.checkpoint_persisted);
        assert_eq!(report.index_sample_count, 2);
        assert!(report.spill_run_count > 1);
        assert_eq!(
            db.statistics().index_samples.get(&body_index_id),
            Some(&crate::schema::IndexStatisticsSample::exact(20, 2))
        );
        assert_eq!(
            db.statistics().index_samples.get(&composite_index_id),
            Some(&crate::schema::IndexStatisticsSample::exact(20, 20))
        );
        assert!(db
            .statistics()
            .property_distinct_counts
            .keys()
            .all(|(_, property)| property != "body"));
        let explain = db
            .explain_query("MATCH (m:Memory) WHERE m.body = 'even' RETURN m.id AS id")
            .unwrap();
        assert!(explain.trace.decisions.iter().any(|decision| {
            decision.contains("Memory.body") && decision.contains("distinct_count=2")
        }));

        db.query("MATCH (m:Memory) WHERE m.id = 0 SET m.body = 'third'")
            .unwrap();
        db.query("MATCH (m:Memory) WHERE m.id = 1 SET m.body = 'fourth'")
            .unwrap();
        let stale_statistics = db.statistics();
        assert!(stale_statistics
            .index_samples
            .get(&body_index_id)
            .unwrap()
            .is_stale());
        assert!(stale_statistics
            .index_samples
            .get(&composite_index_id)
            .unwrap()
            .is_stale());

        let report = db.refresh_optimizer_statistics_external(&options).unwrap();
        assert_eq!(report.index_sample_count, 2);
        assert_eq!(
            db.statistics().index_samples.get(&body_index_id),
            Some(&crate::schema::IndexStatisticsSample::exact(20, 4))
        );
        assert_eq!(
            db.statistics().index_samples.get(&composite_index_id),
            Some(&crate::schema::IndexStatisticsSample::exact(20, 20))
        );
        assert_eq!(std::fs::read_dir(&spill_root).unwrap().count(), 0);
        (body_index_id, composite_index_id, db.statistics())
    };

    let db = Database::open_with_config(&path, config).unwrap();
    assert_eq!(db.statistics(), refreshed_statistics);
    assert_eq!(
        db.statistics().index_samples.get(&body_index_id),
        Some(&crate::schema::IndexStatisticsSample::exact(20, 4))
    );
    assert_eq!(
        db.statistics().index_samples.get(&composite_index_id),
        Some(&crate::schema::IndexStatisticsSample::exact(20, 20))
    );
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn external_optimizer_statistics_refresh_rejects_oversized_index_key_before_publication() {
    let path = unique_test_dir("external_optimizer_oversized_index_key");
    let spill_root = path.join("statistics-spill");
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    db.query("CREATE NODE TABLE Memory").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Memory(body) TYPE TEXT")
        .unwrap();
    db.query_with_params(
        "CREATE (:Memory {id: 1, body: $body})",
        &BTreeMap::from([("body".to_string(), Value::String("x".repeat(8 * 1024)))]),
    )
    .unwrap();
    db.checkpoint().unwrap();
    db.query("CREATE INDEX ON :Memory(body)").unwrap();
    let index_id = db.property_indexes()[0].id;
    assert!(!db.statistics().index_samples.contains_key(&index_id));
    let generation = db.storage_residency_report().canonical_generation;

    let error = db
        .refresh_optimizer_statistics_external(&crate::OptimizerStatisticsRefreshOptions {
            memory_budget_bytes: 4096,
            max_spill_bytes: 1024 * 1024,
            max_spill_runs: 8,
            max_input_records: 100,
            max_generated_facts: 100,
            max_path_expansions: 100,
            spill_directory: spill_root.clone(),
        })
        .unwrap_err();
    assert!(
        error.to_string().contains("optimizer statistics fact uses"),
        "unexpected refresh error: {error}"
    );
    assert!(!db.statistics().index_samples.contains_key(&index_id));
    assert_eq!(
        db.storage_residency_report().canonical_generation,
        generation
    );
    assert_eq!(std::fs::read_dir(&spill_root).unwrap().count(), 0);

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn external_optimizer_statistics_refresh_fails_before_publication_on_work_budget() {
    let path = unique_test_dir("external_optimizer_statistics_budget");
    let spill_root = path.join("statistics-spill");
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'memory:one', kind: 'note'})")
        .unwrap();
    db.query("CREATE INDEX ON :Memory(kind)").unwrap();
    db.checkpoint().unwrap();
    let generation = db.storage_residency_report().canonical_generation;
    let statistics = db.statistics();
    assert_eq!(statistics.index_samples.len(), 1);

    let error = db
        .refresh_optimizer_statistics_external(&crate::OptimizerStatisticsRefreshOptions {
            memory_budget_bytes: 4096,
            max_spill_bytes: 1024 * 1024,
            max_spill_runs: 8,
            max_input_records: 100,
            max_generated_facts: 1,
            max_path_expansions: 100,
            spill_directory: spill_root.clone(),
        })
        .unwrap_err();
    assert!(
        error.to_string().contains("max_generated_facts 1"),
        "unexpected refresh error: {error}"
    );
    assert!(!db.statistics().advanced_statistics_complete);
    assert_eq!(db.statistics(), statistics);
    assert_eq!(
        db.storage_residency_report().canonical_generation,
        generation
    );
    assert_eq!(std::fs::read_dir(&spill_root).unwrap().count(), 0);

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn external_optimizer_statistics_refresh_preserves_spill_limits_and_snapshot() {
    let path = unique_test_dir("external_optimizer_statistics_spill_limits");
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&path, config.clone()).unwrap();
    for id in 0..96 {
        db.query_with_params(
            "CREATE (:Item {score: $score, other: $other})",
            &BTreeMap::from([
                ("score".into(), Value::Int(id % 8)),
                ("other".into(), Value::Int(id % 4)),
            ]),
        )
        .unwrap();
    }
    db.checkpoint().unwrap();
    let before = db.statistics();
    let generation = db.storage_residency_report().canonical_generation;
    let manifest = std::fs::read(path.join("manifest.skein")).unwrap();
    let wal = read_test_wal(&path).unwrap();
    let spill_root = path.join("statistics-spill");
    for (max_spill_bytes, max_spill_runs, expected) in [
        (1, 128, "max_spill_bytes 1"),
        (1024 * 1024, 1, "max_spill_runs 1"),
    ] {
        let error = db
            .refresh_optimizer_statistics_external(&crate::OptimizerStatisticsRefreshOptions {
                memory_budget_bytes: 4096,
                max_spill_bytes,
                max_spill_runs,
                max_input_records: 100_000,
                max_generated_facts: 100_000,
                max_path_expansions: 100_000,
                spill_directory: spill_root.clone(),
            })
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(db.statistics(), before);
        assert_eq!(
            db.storage_residency_report().canonical_generation,
            generation
        );
        assert_eq!(
            std::fs::read(path.join("manifest.skein")).unwrap(),
            manifest
        );
        assert_eq!(read_test_wal(&path).unwrap(), wal);
        assert_eq!(std::fs::read_dir(&spill_root).unwrap().count(), 0);
    }
    drop(db);
    let db = Database::open_with_config(&path, config).unwrap();
    assert_eq!(db.statistics(), before);
    assert_eq!(db.basic_statistics().node_count, 96);
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn out_of_core_delta_budget_rejects_before_wal_append() {
    let path = unique_test_dir("out_of_core_delta_budget");
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        max_out_of_core_delta_bytes: Some(1),
        ..DatabaseConfig::default()
    };
    {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Original'})")
            .unwrap();
        db.checkpoint().unwrap();
        assert_eq!(read_test_wal(&path).unwrap(), "");

        let error = db
            .query("MATCH (m:Memory {id: 1}) SET m.title = 'Rejected'")
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("out-of-core mutation delta admission rejected"));
        assert_eq!(read_test_wal(&path).unwrap(), "");
        let residency = db.storage_residency_report();
        assert_eq!(residency.estimated_delta_resident_bytes, 0);
        assert!(residency.delta_within_budget);
    }

    {
        let mut db = Database::open_with_config(&path, config).unwrap();
        let output = db
            .query("MATCH (m:Memory {id: 1}) RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Original".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn out_of_core_deferred_mutation_is_not_queued_and_can_retry_after_checkpoint() {
    let path = unique_test_dir("out_of_core_deferred_mutation");
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        // One live record uses 36 bytes; admitting another estimates 64 more.
        max_out_of_core_delta_bytes: Some(110),
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&path, config.clone()).unwrap();
    db.query("CREATE (:Memory)").unwrap();
    db.checkpoint().unwrap();
    db.query("CREATE (:Memory)").unwrap();
    assert_eq!(
        db.storage_residency_report().estimated_delta_resident_bytes,
        36
    );
    let before_wal = read_test_wal(&path).unwrap();
    let before_epoch = db.commit_epoch();

    let error = db.query("CREATE (:Memory)").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("out-of-core mutation deferred by storage pressure"),
        "{error}"
    );
    assert!(error
        .to_string()
        .contains("checkpoint the database before retrying"));
    assert_eq!(read_test_wal(&path).unwrap(), before_wal);
    assert_eq!(db.commit_epoch(), before_epoch);
    assert_eq!(
        db.storage_residency_report().estimated_delta_resident_bytes,
        36
    );

    db.checkpoint().unwrap();
    assert_eq!(db.query("MATCH (m:Memory) RETURN m").unwrap().rows.len(), 2);
    db.query("CREATE (:Memory)").unwrap();
    drop(db);
    let mut reopened = Database::open_with_config(&path, config).unwrap();
    assert_eq!(
        reopened
            .query("MATCH (m:Memory) RETURN m")
            .unwrap()
            .rows
            .len(),
        3
    );
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn out_of_core_replay_does_not_defer_already_committed_mutations() {
    let path = unique_test_dir("out_of_core_replay_defer_threshold");
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        max_out_of_core_delta_bytes: None,
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&path, config.clone()).unwrap();
    db.query("CREATE (:Memory)").unwrap();
    db.checkpoint().unwrap();
    for _ in 0..10 {
        db.query("CREATE (:Memory)").unwrap();
    }
    assert_eq!(
        db.storage_residency_report().estimated_delta_resident_bytes,
        360
    );
    drop(db);

    let mut reopened = Database::open_with_config(
        &path,
        DatabaseConfig {
            // The final replay admission estimates 388 bytes, below the hard
            // limit but above its 90% live-admission threshold.
            max_out_of_core_delta_bytes: Some(400),
            ..config
        },
    )
    .unwrap();
    assert_eq!(
        reopened.storage_pressure_snapshot().state,
        crate::StoragePressureState::DeferMutation
    );
    assert_eq!(
        reopened
            .query("MATCH (m:Memory) RETURN m")
            .unwrap()
            .rows
            .len(),
        11
    );
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn out_of_core_delta_budget_also_bounds_wal_replay() {
    let path = unique_test_dir("out_of_core_replay_delta_budget");
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: StorageResidencyMode::OutOfCore,
                max_out_of_core_delta_bytes: None,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Original'})")
            .unwrap();
        db.checkpoint().unwrap();
        db.query("MATCH (m:Memory {id: 1}) SET m.title = 'Pending WAL delta'")
            .unwrap();
    }

    let error = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            max_out_of_core_delta_bytes: Some(1),
            ..DatabaseConfig::default()
        },
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("out-of-core mutation delta admission rejected"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn legacy_text_wal_is_rejected_by_the_single_v1_reader() {
    let path = unique_test_dir("legacy_text_wal_rejected");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
    }
    std::fs::write(
        active_wal_path(&path),
        b"SKEIN_WAL_V1\t1\t1\t00000000000000000000\n",
    )
    .unwrap();

    let error = Database::open(&path).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("WAL is missing a supported generation header"),
        "unexpected legacy WAL error: {error}"
    );

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn stale_generation_wal_fragment_reads_as_clean_end_of_log() {
    let path = unique_test_dir("stale_generation_wal_tail");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
    }
    // A well-formed fragment carrying a stale WAL generation past the
    // logical tail must read as clean end of log (recyclable-log
    // discipline), not as a torn tail and not as corruption.
    crate::store::append_stale_generation_wal_fragment(&active_wal_path(&path)).unwrap();
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Graph foundations".to_string()))
        );
        let recovery = db.storage_recovery_report();
        assert!(recovery.replayed_wal_entries >= 1);
        assert!(!recovery.torn_tail_ignored);
        assert!(!recovery.torn_tail_repaired);
        assert_eq!(recovery.discarded_wal_tail_bytes, 0);
        assert!(recovery.torn_tail_reason.is_none());
    }
    // The stale fragment is not doctor-repairable damage either.
    let error =
        DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap_err();
    assert!(error
        .to_string()
        .contains("no repairable incomplete final record"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn persists_nodes_across_reopen_with_wal_replay() {
    let path = unique_test_dir("wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Graph foundations".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn default_recovery_rejects_torn_wal_tail_until_explicit_doctor_repair() {
    let path = unique_test_dir("strict_torn_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
    }
    std::fs::OpenOptions::new()
        .append(true)
        .open(active_wal_path(&path))
        .unwrap()
        .write_all(b"torn-entry-without-checksum")
        .unwrap();

    let error = Database::open(&path).unwrap_err();
    assert!(error
        .to_string()
        .contains("strict WAL recovery rejected torn tail"));

    let legacy_open_repair_error = Database::open_with_config(
        &path,
        DatabaseConfig {
            recovery_mode: RecoveryMode::DoctorRepairTornTail,
            ..DatabaseConfig::default()
        },
    )
    .unwrap_err();
    assert!(legacy_open_repair_error
        .to_string()
        .contains("repair is not available through database open"));

    let wal_before_repair = read_test_wal(&path).unwrap();
    let plan = DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap();
    assert!(plan.data_loss_possible);
    assert_eq!(
        plan.discarded_wal_tail_bytes,
        b"torn-entry-without-checksum".len() as u64
    );
    assert_eq!(read_test_wal(&path).unwrap(), wal_before_repair);

    let repair = DatabaseDoctor::apply_wal_tail_repair(
        &path,
        &plan,
        plan.acknowledge_potential_data_loss(),
        WalDoctorOptions::default(),
    )
    .unwrap();
    assert_eq!(
        repair.discarded_wal_tail_bytes,
        plan.discarded_wal_tail_bytes
    );
    assert!(!repair.resumed_interrupted_repair);
    assert!(path
        .join("doctor/quarantine")
        .join(&repair.quarantine_file)
        .exists());
    let repair_record = path.join("doctor").join(&repair.repair_record_file);
    assert!(repair_record.exists());
    assert!(std::fs::read_to_string(repair_record)
        .unwrap()
        .contains("\"state\": \"applied\""));

    let mut repaired = Database::open(&path).unwrap();
    let output = repaired
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Graph foundations".to_string()))
    );
    let recovery = repaired.storage_recovery_report();
    assert!(!recovery.torn_tail_ignored);
    assert!(!recovery.torn_tail_repaired);
    assert_eq!(recovery.discarded_wal_tail_bytes, 0);
    assert!(!read_test_wal(&path)
        .unwrap()
        .contains("torn-entry-without-checksum"));
    let quarantined =
        std::fs::read(path.join("doctor/quarantine").join(repair.quarantine_file)).unwrap();
    assert!(quarantined
        .windows(b"torn-entry-without-checksum".len())
        .any(|window| window == b"torn-entry-without-checksum"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn storage_scrub_streams_strong_artifact_verification_and_poisons_on_corruption() {
    let path = unique_test_dir("storage_scrub_corruption");
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            segment_cache_capacity_bytes: 1024 * 1024,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Scrub me'})")
        .unwrap();
    db.checkpoint().unwrap();

    let clean = db.scrub_storage().unwrap();
    assert_eq!(clean.generation, 1);
    assert!(clean.checked_file_count >= 4);
    assert!(clean.sha256_verified_file_count >= 2);
    assert!(clean.checked_bytes > 0);

    let canonical_path = path.join("canonical.1.skein");
    let mut bytes = std::fs::read(&canonical_path).unwrap();
    bytes[24] ^= 0xff;
    std::fs::write(&canonical_path, bytes).unwrap();

    let error = db.scrub_storage().unwrap_err();
    assert!(
        error.to_string().contains("CRC32C mismatch during scrub")
            || error.to_string().contains("SHA-256 mismatch during scrub"),
        "unexpected scrub error: {error}"
    );
    assert!(db.storage_handle_poisoned());
    let poisoned = db
        .query("MATCH (m:Memory) RETURN m.id AS memory_id")
        .unwrap_err();
    assert!(poisoned.to_string().contains("close and reopen"));

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn wal_replay_entry_limit_rejects_long_recovery() {
    let path = unique_test_dir("wal_replay_limit");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
    }

    let error = Database::open_with_config(
        &path,
        DatabaseConfig {
            max_wal_replay_entries: Some(1),
            ..DatabaseConfig::default()
        },
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("WAL replay entry limit exceeded"));

    let mut db = Database::open(&path).unwrap();
    let output = db
        .query("MATCH (m:Memory) RETURN m.title AS title ORDER BY title ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn storage_recovery_report_tracks_wal_replay_boundary() {
    let path = unique_test_dir("storage_recovery_report");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Checkpointed'})")
            .unwrap();
        db.checkpoint().unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'Replayed'})")
            .unwrap();
    }

    let db = Database::open_with_config(
        &path,
        DatabaseConfig {
            max_wal_replay_entries: Some(8),
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let report = db.storage_recovery_report();
    assert!(report.durable);
    assert_eq!(report.recovery_mode, RecoveryMode::Strict);
    assert_eq!(report.max_wal_replay_entries, Some(8));
    assert_eq!(report.checkpoint_epoch, Some(1));
    assert_eq!(report.checkpoint_commit_epoch, Some(2));
    assert!(report.wal_present);
    assert_eq!(report.wal_replay_start_lsn, Some(3));
    assert_eq!(report.next_lsn_after_replay, Some(4));
    assert_eq!(report.replayed_wal_entries, 1);
    assert!(!report.torn_tail_ignored);
    assert_eq!(report.torn_tail_reason, None);
    assert_eq!(report.recovered_commit_epoch, 3);
    assert!(report.open_timings.is_consistent());
    assert!(report.open_timings.wal_replay_micros <= report.open_timings.total_open_micros);
    assert_eq!(
        report.open_timings.accounted_micros() + report.open_timings.unaccounted_micros(),
        report.open_timings.total_open_micros
    );

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn canonical_row_overflow_backup_reopen_and_reclaim_follow_physical_closure() {
    let path = unique_test_dir("canonical_row_overflow_lifecycle");
    let backup = unique_test_dir("canonical_row_overflow_backup");
    let restored = unique_test_dir("canonical_row_overflow_restored");
    let body = "x".repeat(8 * 1024);
    let mut db = Database::open(&path).unwrap();
    db.query_sql("CREATE TABLE public.documents (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    db.query_sql_with_params(
        "INSERT INTO public.documents (id, body) VALUES ($1, $2)",
        &[Value::Int(1), Value::String(body.clone())],
    )
    .unwrap();
    db.checkpoint().unwrap();

    let first_generation = db
        .storage_reclamation_watermark()
        .checkpoint_epoch
        .expect("durable checkpoint generation");
    let first_row_manifest = path.join(
        skein_storage::relational_row_page_manifest_generation_file(first_generation),
    );
    let first_overflow_manifest = path.join(
        skein_storage::relational_overflow_manifest_generation_file(first_generation),
    );
    let first_overflow_extent = path.join(skein_storage::relational_overflow_extent_file(
        first_generation,
    ));
    assert!(first_row_manifest.exists());
    assert!(first_overflow_manifest.exists());
    assert!(first_overflow_extent.exists());

    let pinned = db.begin_read_transaction();
    assert_eq!(
        pinned
            .query_sql("SELECT body FROM public.documents WHERE id = 1")
            .unwrap()
            .rows[0]["body"],
        Value::String(body.clone())
    );
    for id in 2..=4 {
        db.query_sql_with_params(
            "INSERT INTO public.documents (id, body) VALUES ($1, $2)",
            &[Value::Int(id), Value::String(format!("marker-{id}"))],
        )
        .unwrap();
        db.checkpoint().unwrap();
    }

    assert!(first_row_manifest.exists());
    assert!(first_overflow_manifest.exists());
    assert!(first_overflow_extent.exists());
    db.backup_to(&backup).unwrap();
    assert!(backup
        .join(skein_storage::relational_overflow_extent_file(
            first_generation
        ))
        .exists());
    Database::restore_backup(&backup, &restored).unwrap();
    let mut restored_db = Database::open(&restored).unwrap();
    assert_eq!(
        restored_db
            .query_sql("SELECT body FROM public.documents WHERE id = 1")
            .unwrap()
            .rows[0]["body"],
        Value::String(body.clone())
    );
    drop(restored_db);

    drop(pinned);
    db.query_sql("INSERT INTO public.documents (id, body) VALUES (5, 'reclaim')")
        .unwrap();
    db.checkpoint().unwrap();
    assert!(!first_row_manifest.exists());
    assert!(!first_overflow_manifest.exists());
    assert!(first_overflow_extent.exists());
    assert_eq!(
        db.query_sql("SELECT body FROM public.documents WHERE id = 1")
            .unwrap()
            .rows[0]["body"],
        Value::String(body.clone())
    );
    drop(db);

    let mut reopened = Database::open(&path).unwrap();
    assert_eq!(
        reopened
            .query_sql("SELECT body FROM public.documents WHERE id = 1")
            .unwrap()
            .rows[0]["body"],
        Value::String(body)
    );
    drop(reopened);

    std::fs::remove_dir_all(path).unwrap();
    std::fs::remove_dir_all(backup).unwrap();
    std::fs::remove_dir_all(restored).unwrap();
}

#[test]
fn relational_storage_residency_tracks_checkpoint_live_and_recovery_views() {
    let path = unique_test_dir("relational_storage_residency");
    let bootstrap_config = DatabaseConfig {
        relational_index_mode: skein_storage::RelationalIndexMode::Shadow,
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: 64 * 1024,
        ..DatabaseConfig::default()
    };
    let body = "x".repeat(8 * 1024);
    let mut db = Database::open_with_config(&path, bootstrap_config.clone()).unwrap();
    db.query_sql("CREATE TABLE public.documents (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    db.query_sql_with_params(
        "INSERT INTO public.documents (id, body) VALUES ($1, $2)",
        &[Value::Int(1), Value::String(body)],
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'residency-probe'})")
        .unwrap();
    db.checkpoint().unwrap();
    drop(db);

    let config = DatabaseConfig {
        relational_index_mode: skein_storage::RelationalIndexMode::Authoritative,
        ..bootstrap_config
    };
    let mut db = Database::open_with_config(&path, config.clone()).unwrap();

    let checkpoint = db.storage_residency_report();
    assert!(checkpoint.relational_rows.serving);
    assert!(checkpoint.relational_rows.base_generation.is_some());
    assert_eq!(
        checkpoint.relational_rows.base_generation,
        checkpoint.relational_indexes.base_generation
    );
    assert_eq!(
        checkpoint.relational_rows.visible_commit_epoch,
        checkpoint.relational_indexes.visible_commit_epoch
    );
    assert!(checkpoint.relational_rows.root_page_count > 0);
    assert!(checkpoint.relational_rows.canonical_artifact_bytes() > 0);
    assert!(checkpoint.relational_rows.overflow_extent_count > 0);
    assert!(checkpoint.relational_rows.overflow_extent_artifact_bytes > 0);
    assert!(checkpoint.relational_indexes.serving);
    assert!(checkpoint.relational_indexes.root_count > 0);
    assert!(checkpoint.relational_indexes.base_artifact_bytes > 0);
    assert_eq!(checkpoint.relational_rows.live_entries, 0);
    assert_eq!(checkpoint.relational_indexes.live_entries, 0);
    let profile = db
        .storage_resource_profile(
            "MATCH (m:Memory) RETURN m.id AS memory_id LIMIT 1",
            &BTreeMap::new(),
            crate::StorageResourceProfileLimits {
                min_canonical_artifact_bytes: 1,
                max_steady_resident_bytes: u64::MAX,
                max_peak_resident_bytes: u64::MAX,
                max_total_page_faults: None,
                max_minor_page_faults: None,
                max_major_page_faults: None,
                max_intermediate_rows: 16,
                max_intermediate_payload_bytes: 1024,
                max_output_rows: 1,
                max_output_payload_bytes: 1024,
                require_fully_streamed: true,
            },
        )
        .unwrap();
    let profile = profile.json();
    assert_eq!(profile["storage"]["relational_rows"]["serving"], true);
    assert_eq!(
        profile["storage"]["relational_rows"]["recovery_delta_checkpoint_runs"],
        skein_storage::DEFAULT_RELATIONAL_ROW_DELTA_CHECKPOINT_RUNS
    );
    assert_eq!(
        profile["storage"]["relational_rows"]["recovery_delta_checkpoint_recommended"],
        false
    );
    assert_eq!(
        profile["storage"]["relational_rows"]["canonical_artifact_bytes"],
        checkpoint.relational_rows.canonical_artifact_bytes()
    );
    assert_eq!(profile["storage"]["relational_indexes"]["serving"], true);
    assert_eq!(
        profile["storage"]["relational_indexes"]["canonical_artifact_bytes"],
        checkpoint.relational_indexes.canonical_artifact_bytes()
    );

    db.query_sql("INSERT INTO public.documents (id, body) VALUES (2, 'live')")
        .unwrap();
    let live = db.storage_residency_report();
    assert_eq!(
        live.relational_rows.visible_commit_epoch,
        Some(db.commit_epoch())
    );
    assert_eq!(
        live.relational_indexes.visible_commit_epoch,
        Some(db.commit_epoch())
    );
    assert!(live.relational_rows.live_batches > 0);
    assert!(live.relational_rows.live_entries > 0);
    assert!(live.relational_rows.live_encoded_bytes > 0);
    assert!(live.relational_rows.live_resident_bytes > 0);
    assert!(live.relational_indexes.live_batches > 0);
    assert!(live.relational_indexes.live_entries > 0);
    assert!(live.relational_indexes.live_encoded_bytes > 0);
    drop(db);

    let reopened = Database::open_with_config(&path, config).unwrap();
    let recovered = reopened.storage_residency_report();
    assert!(recovered.relational_rows.serving);
    assert!(recovered
        .relational_rows
        .recovery_delta_generation
        .is_some());
    assert!(recovered.relational_rows.recovery_delta_runs > 0);
    assert!(recovered.relational_rows.recovery_delta_entries > 0);
    assert!(recovered.relational_rows.recovery_delta_artifact_bytes > 0);
    assert_eq!(recovered.relational_rows.live_entries, 0);
    assert!(recovered.relational_indexes.serving);
    assert!(recovered
        .relational_indexes
        .recovery_delta_generation
        .is_some());
    assert!(recovered.relational_indexes.recovery_delta_pages > 0);
    assert!(recovered.relational_indexes.recovery_delta_entries > 0);
    assert!(recovered.relational_indexes.recovery_delta_artifact_bytes > 0);
    assert_eq!(
        recovered.relational_indexes.canonical_artifact_bytes(),
        recovered.relational_indexes.base_artifact_bytes
    );
    assert_eq!(recovered.relational_indexes.live_entries, 0);
    assert_eq!(
        recovered.relational_rows.visible_commit_epoch,
        recovered.relational_indexes.visible_commit_epoch
    );
    drop(reopened);

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn mem_shaped_graph_mutations_recover_across_checkpoint_and_wal() {
    let path = unique_test_dir("mem_shaped_recovery");
    let live_snapshot = {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Source {id: 'source:one', space_id: 'default', kind: 'thread', metadata: '{}', memory_count: 1})",
        )
        .unwrap();
        db.query(
            "CREATE (:Thread {id: 'thread:one', thread_id: 'thread:one', source_id: 'source:one', space_id: 'default', message_count: 2})",
        )
        .unwrap();
        db.query(
            "CREATE (:Memory {id: 'mem:checkpointed', title: 'Checkpointed memory', source_id: 'source:one', thread_id: 'thread:one', space_id: 'default', importance: 0.4, confidence: 0.8, is_latest: true})",
        )
        .unwrap();
        db.checkpoint().unwrap();

        {
            let mut tx = db.begin_transaction();
            tx.query(
                "CREATE (:Memory {id: 'mem:replayed', title: 'Replayed memory', source_id: 'source:one', thread_id: 'thread:one', space_id: 'default', importance: 0.9, confidence: 0.7, lifecycle_state: 'active', is_latest: true})-[:MENTIONS {thread_id: 'thread:one', message_index: 1, confidence: 0.7}]->(:Entity {id: 'entity:rust', name: 'Rust', space_id: 'default', unit_type: 'entity'})",
            )
            .unwrap();
            tx.commit().unwrap();
        }

        let snapshot = db.export_canonical_graph_snapshot();
        assert!(snapshot.validate().is_valid);
        snapshot
    };

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 1);
    assert!(wal.contains("\tbatch\t"));
    assert!(wal.contains("create_node"));
    assert!(wal.contains("create_rel"));

    {
        let db = Database::open_with_config(
            &path,
            DatabaseConfig {
                max_wal_replay_entries: Some(8),
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let recovered = db.export_canonical_graph_snapshot();
        assert_eq!(recovered, live_snapshot);
        assert!(recovered.validate().is_valid);

        let recovery = db.storage_recovery_report();
        assert_eq!(recovery.checkpoint_epoch, Some(1));
        assert_eq!(recovery.checkpoint_commit_epoch, Some(4));
        assert_eq!(recovery.replayed_wal_entries, 1);
        assert_eq!(recovery.max_wal_replay_entries, Some(8));
        assert_eq!(recovery.recovered_commit_epoch, 5);

        let mem_recovery = NowledgeMemStorageRecoveryReport::from_storage_report(&recovery);
        assert!(mem_recovery.ready);
        assert!(mem_recovery.durable_recovery_observed);
        assert!(mem_recovery.checkpoint_boundary_present);
        assert!(mem_recovery.wal_replay_bounded);
        assert!(mem_recovery.torn_tail_clean);
        assert!(mem_recovery.blocker_codes.is_empty());
        assert_eq!(mem_recovery.json()["readiness"]["wal_replay_bounded"], true);
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn mem_shaped_post_checkpoint_batch_replays_before_torn_tail() {
    let path = unique_test_dir("mem_shaped_recovery_torn_tail");
    let live_snapshot = {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Source {id: 'source:one', space_id: 'default', kind: 'thread', metadata: '{}', memory_count: 1})",
        )
        .unwrap();
        db.query(
            "CREATE (:Thread {id: 'thread:one', thread_id: 'thread:one', source_id: 'source:one', space_id: 'default', message_count: 2})",
        )
        .unwrap();
        db.query(
            "CREATE (:Memory {id: 'mem:checkpointed', title: 'Checkpointed memory', source_id: 'source:one', thread_id: 'thread:one', space_id: 'default'})",
        )
        .unwrap();
        db.checkpoint().unwrap();

        let mut tx = db.begin_transaction();
        tx.query(
            "CREATE (:Memory {id: 'mem:replayed', title: 'Replayed memory', source_id: 'source:one', thread_id: 'thread:one', space_id: 'default'})-[:MENTIONS {thread_id: 'thread:one', message_index: 1, confidence: 0.7}]->(:Entity {id: 'entity:rust', name: 'Rust', space_id: 'default', unit_type: 'entity'})",
        )
        .unwrap();
        tx.commit().unwrap();

        let snapshot = db.export_canonical_graph_snapshot();
        assert!(snapshot.validate().is_valid);
        snapshot
    };

    std::fs::OpenOptions::new()
        .append(true)
        .open(active_wal_path(&path))
        .unwrap()
        .write_all(b"torn-entry-without-checksum")
        .unwrap();

    let repair_plan =
        DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap();
    let repair = DatabaseDoctor::apply_wal_tail_repair(
        &path,
        &repair_plan,
        repair_plan.acknowledge_potential_data_loss(),
        WalDoctorOptions::default(),
    )
    .unwrap();
    assert!(repair.discarded_wal_tail_bytes > 0);

    {
        let db = Database::open_with_config(
            &path,
            DatabaseConfig {
                max_wal_replay_entries: Some(8),
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let recovered = db.export_canonical_graph_snapshot();
        assert_eq!(recovered, live_snapshot);
        assert!(recovered.validate().is_valid);

        let recovery = db.storage_recovery_report();
        assert_eq!(recovery.checkpoint_epoch, Some(1));
        assert_eq!(recovery.checkpoint_commit_epoch, Some(4));
        assert_eq!(recovery.replayed_wal_entries, 1);
        assert_eq!(recovery.max_wal_replay_entries, Some(8));
        assert_eq!(recovery.recovered_commit_epoch, 5);
        assert!(!recovery.torn_tail_ignored);
        assert!(!recovery.torn_tail_repaired);
        assert_eq!(recovery.discarded_wal_tail_bytes, 0);
        assert!(recovery.torn_tail_reason.is_none());

        let mem_recovery = NowledgeMemStorageRecoveryReport::from_storage_report(&recovery);
        assert!(mem_recovery.ready);
        assert!(mem_recovery.durable_recovery_observed);
        assert!(mem_recovery.checkpoint_boundary_present);
        assert!(mem_recovery.wal_replay_bounded);
        assert!(mem_recovery.torn_tail_clean);
        assert!(mem_recovery.blocker_codes.is_empty());
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn in_memory_storage_recovery_report_is_non_durable() {
    let db = Database::new();
    let report = db.storage_recovery_report();
    assert!(!report.durable);
    assert_eq!(report.recovered_commit_epoch, 0);
    assert_eq!(report.replayed_wal_entries, 0);
    assert_eq!(report.max_wal_replay_entries, None);
    assert_eq!(report.checkpoint_epoch, None);
    assert_eq!(report.next_lsn_after_replay, None);
}

#[test]
fn read_only_open_does_not_create_missing_database_path() {
    let path = unique_test_dir("read_only_missing");
    let error = Database::open_with_config(
        &path,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("read-only database path does not exist"));
    assert!(!path.exists());
}

#[test]
fn durable_database_open_is_exclusive_until_owner_drops() {
    let path = unique_test_dir("exclusive_database_owner");
    let owner = Database::open(&path).unwrap();

    let write_error = Database::open(&path).unwrap_err();
    assert_eq!(
        write_error.to_string(),
        "storage error: database directory is already open by this or another application"
    );
    let read_error = Database::open_with_config(
        &path,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    )
    .unwrap_err();
    assert_eq!(read_error.to_string(), write_error.to_string());
    assert!(!write_error
        .to_string()
        .contains(&path.display().to_string()));

    drop(owner);
    let reopened = Database::open_with_config(
        &path,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn durable_database_rejects_path_alias_until_owner_drops() {
    let path = unique_test_dir("exclusive_database_alias");
    let alias = path.join(".");
    let owner = Database::open(&path).unwrap();

    let error = Database::open(&alias).unwrap_err();
    assert_eq!(
        error.to_string(),
        "storage error: database directory is already open by this or another application"
    );

    drop(owner);
    let reopened = Database::open(&alias).unwrap();
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_only_open_loads_existing_database_without_allowing_writes() {
    let path = unique_test_dir("read_only_existing");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
    }
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Graph foundations".to_string()))
        );

        let error = db.query("CREATE (:Memory {id: 2})").unwrap_err();
        assert!(error.to_string().contains("read-only mode"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoints_nodes_and_truncates_wal() {
    let path = unique_test_dir("checkpoint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        db.checkpoint().unwrap();
    }
    assert_eq!(read_test_wal(&path).unwrap(), "");
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Graph foundations".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_query_invokes_storage_checkpoint() {
    let path = unique_test_dir("checkpoint_query");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        let output = db.query("CHECKPOINT;").unwrap();
        assert!(output.rows.is_empty());
    }

    let checkpoint = read_test_durable_text(&active_checkpoint_path(&path)).unwrap();
    assert!(checkpoint.contains("canonical_records\ttrue\n"));
    assert!(path.join("canonical.1.skein").exists());
    assert_eq!(read_test_wal(&path).unwrap(), "");

    std::fs::remove_dir_all(path).unwrap();
}

const STORAGE_CRASH_CHILD_ENV: &str = "SKEIN_TEST_STORAGE_CRASH_CHILD";
const STORAGE_CRASH_PATH_ENV: &str = "SKEIN_TEST_STORAGE_CRASH_PATH";
const STORAGE_CRASH_EVIDENCE_PATH_ENV: &str = "SKEIN_TEST_STORAGE_CRASH_EVIDENCE_PATH";

#[test]
fn storage_crash_recovery_child() {
    if std::env::var_os(STORAGE_CRASH_CHILD_ENV).is_none() {
        return;
    }
    let path = std::path::PathBuf::from(
        std::env::var_os(STORAGE_CRASH_PATH_ENV).expect("crash test database path"),
    );
    let point = std::env::var("SKEIN_TEST_PROCESS_CRASH_POINT").expect("crash point");
    let mut db = Database::open_with_config(&path, storage_crash_test_config()).unwrap();
    let mut transaction = db.begin_transaction();
    transaction
        .query(
            "CREATE (:Memory {id: 'crash-a'})-[:RELATED_TO {id: 'crash-rel'}]->(:Memory {id: 'crash-b'})",
        )
        .unwrap();
    transaction.commit().unwrap();
    if matches!(
        point.as_str(),
        "during_checkpoint_publication" | "after_manifest_publication"
    ) {
        db.checkpoint().unwrap();
    }
    panic!("crash failpoint {point} did not terminate the child process");
}

#[test]
fn subprocess_crash_matrix_recovers_whole_batches_and_artifact_generations() {
    let stages = [
        ("before_wal_append", false, false),
        ("after_wal_append", false, true),
        ("after_wal_sync", true, true),
        ("during_checkpoint_publication", true, true),
        ("after_manifest_publication", true, true),
    ];

    let mut cases = Vec::new();
    for repetition in 0..2 {
        for (stage, must_be_present, may_be_present) in stages {
            let path = unique_test_dir(&format!("subprocess_crash_{stage}_{repetition}"));
            let baseline_epoch = {
                let mut db =
                    Database::open_with_config(&path, storage_crash_test_config()).unwrap();
                db.query(
                    "CREATE (:Memory {id: 'baseline-a'})-[:RELATED_TO]->(:Memory {id: 'baseline-b'})",
                )
                .unwrap();
                db.checkpoint().unwrap();
                db.commit_epoch()
            };

            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("api::tests::storage_recovery::storage_crash_recovery_child")
                .arg("--nocapture")
                .env(STORAGE_CRASH_CHILD_ENV, "1")
                .env(STORAGE_CRASH_PATH_ENV, &path)
                .env("SKEIN_TEST_PROCESS_CRASH_POINT", stage)
                .status()
                .unwrap();
            assert_eq!(
                status.code(),
                Some(86),
                "child did not terminate at {stage}"
            );

            let mut reopened =
                Database::open_with_config(&path, storage_crash_test_config()).unwrap();
            let node_a = count_query(
                &mut reopened,
                "MATCH (m:Memory {id: 'crash-a'}) RETURN count(m) AS count",
            );
            let node_b = count_query(
                &mut reopened,
                "MATCH (m:Memory {id: 'crash-b'}) RETURN count(m) AS count",
            );
            let relationship = count_query(
                &mut reopened,
                "MATCH (:Memory {id: 'crash-a'})-[r:RELATED_TO]->(:Memory {id: 'crash-b'}) RETURN count(r) AS count",
            );
            assert_eq!(node_a, node_b, "partial node batch after {stage}");
            assert_eq!(
                node_a, relationship,
                "partial relationship batch after {stage}"
            );
            assert!(node_a <= 1, "duplicate recovered batch after {stage}");
            if must_be_present {
                assert_eq!(node_a, 1, "durable batch missing after {stage}");
            }
            if !may_be_present {
                assert_eq!(node_a, 0, "unappended batch visible after {stage}");
            }

            let expected_epoch = baseline_epoch + node_a;
            assert_eq!(reopened.commit_epoch(), expected_epoch);
            let recovery = reopened.storage_recovery_report();
            assert_eq!(recovery.recovered_commit_epoch, expected_epoch);
            assert!(recovery.next_lsn_after_replay.is_some());
            assert!(recovery.wal_replay_start_lsn.is_some());
            assert!(recovery
                .checkpoint_commit_epoch
                .is_some_and(|epoch| epoch <= expected_epoch));

            let residency = reopened.storage_residency_report();
            let artifact_generation_valid =
                recovery.checkpoint_epoch == residency.canonical_generation;
            assert!(artifact_generation_valid);
            assert_eq!(residency.segment_cache_digest_mismatch_count, 0);
            let changefeed = reopened.search_projection_changefeed_status();
            let projection_watermark_valid = changefeed.graph_commit_epoch == expected_epoch;
            assert!(projection_watermark_valid);
            assert!(changefeed.restart_recoverable);
            if node_a == 1 {
                assert_eq!(
                    changefeed
                        .newest_retained_mutation_id
                        .map(|mutation| mutation.commit_epoch()),
                    Some(expected_epoch)
                );
            }
            cases.push(crate::StorageCrashCaseEvidence {
                point: storage_crash_point(stage),
                repetition,
                process_terminated: status.code() == Some(86),
                recovered_batch_present: node_a == 1,
                whole_batch_recovered: node_a == node_b && node_a == relationship,
                commit_epoch: reopened.commit_epoch(),
                recovered_commit_epoch: recovery.recovered_commit_epoch,
                replay_lsn_present: recovery.next_lsn_after_replay.is_some()
                    && recovery.wal_replay_start_lsn.is_some(),
                relationship_endpoints_valid: node_a == relationship,
                projection_watermark_valid,
                artifact_generation_valid,
            });

            drop(reopened);
            std::fs::remove_dir_all(path).unwrap();
        }
    }

    let source_revision =
        std::env::var("GITHUB_SHA").unwrap_or_else(|_| "local-test-revision".to_string());
    let identity = crate::ProductionQualificationIdentity {
        source_revision,
        rust_toolchain: std::env::var("SKEIN_TEST_RUST_TOOLCHAIN")
            .unwrap_or_else(|_| "local-test-toolchain".to_string()),
        target_os: std::env::consts::OS.to_string(),
        target_arch: std::env::consts::ARCH.to_string(),
        enabled_features: vec![
            "acl".to_string(),
            "background-maintenance".to_string(),
            "full-text-search".to_string(),
            "graph-analytics".to_string(),
            "vector-search".to_string(),
        ],
        durable_format_version: 2,
        schema_version: 1,
        configuration_digest: "storage-crash-out-of-core-1m-cache-v1".to_string(),
        deployment_profile: "storage-crash-recovery-ci".to_string(),
        dataset_fingerprint: "storage-crash-matrix-v1".to_string(),
        canonical_graph_commit_epoch: 1,
        policy_version: crate::PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
    let evidence = crate::StorageCrashRecoveryEvidence::evaluate(
        crate::ProductionEvidenceBinding {
            identity: identity.clone(),
            generated_at_unix_seconds: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        },
        identity,
        2,
        cases,
    );
    assert!(
        evidence.ready,
        "unexpected crash evidence blockers: {:?}",
        evidence.blocker_codes
    );
    let evidence_json = evidence.json().to_string();
    println!("storage_crash_recovery_evidence_json {evidence_json}");
    if let Some(path) = std::env::var_os(STORAGE_CRASH_EVIDENCE_PATH_ENV) {
        std::fs::write(path, evidence_json).unwrap();
    }
}

fn storage_crash_point(point: &str) -> crate::StorageCrashPoint {
    match point {
        "before_wal_append" => crate::StorageCrashPoint::BeforeWalAppend,
        "after_wal_append" => crate::StorageCrashPoint::AfterWalAppend,
        "after_wal_sync" => crate::StorageCrashPoint::AfterWalSync,
        "during_checkpoint_publication" => crate::StorageCrashPoint::DuringCheckpointPublication,
        "after_manifest_publication" => crate::StorageCrashPoint::AfterManifestPublication,
        point => panic!("unknown storage crash point {point}"),
    }
}

fn count_query(db: &mut Database, statement: &str) -> u64 {
    let output = db.query(statement).unwrap();
    match output.rows[0].get("count") {
        Some(Value::Int(value)) if *value >= 0 => *value as u64,
        value => panic!("expected non-negative count, got {value:?}"),
    }
}

fn storage_crash_test_config() -> DatabaseConfig {
    DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: 1024 * 1024,
        ..DatabaseConfig::default()
    }
}
