use super::*;

#[test]
fn exposes_property_index_descriptors_and_statistics() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    db.query("CREATE INDEX ON :Memory(kind)").unwrap();
    db.query("CREATE INDEX ON :Entity(id)").unwrap();
    db.query("CREATE (:Memory {id: 1, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'note'})").unwrap();
    db.query(
        "CREATE (:Memory {id: 3, kind: 'decision'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
    )
    .unwrap();

    let indexes = db.property_indexes();
    assert!(indexes
        .iter()
        .any(|index| index.property == "id" && index.label_id.0 == 0));
    assert!(indexes
        .iter()
        .any(|index| index.property == "kind" && index.label_id.0 == 0));

    let statistics = db.statistics();
    let basic_statistics = db.basic_statistics();
    assert_eq!(statistics.node_count, 4);
    assert_eq!(statistics.relationship_count, 1);
    assert_eq!(statistics.label_counts.values().sum::<u64>(), 4);
    assert_eq!(statistics.rel_type_counts.values().sum::<u64>(), 1);
    // Three CREATE INDEX statements commit ahead of the three writes.
    assert_eq!(basic_statistics.computed_at_commit_epoch, 6);
    assert_eq!(basic_statistics.node_count, statistics.node_count);
    assert_eq!(
        basic_statistics.relationship_count,
        statistics.relationship_count
    );
    assert_eq!(basic_statistics.label_counts, statistics.label_counts);
    assert_eq!(basic_statistics.rel_type_counts, statistics.rel_type_counts);
    assert_eq!(statistics.rel_type_source_counts.values().sum::<u64>(), 1);
    assert_eq!(statistics.rel_type_target_counts.values().sum::<u64>(), 1);
    assert_eq!(statistics.path_counts.values().sum::<u64>(), 1);
    assert_eq!(
        statistics.property_distinct_counts.values().copied().max(),
        Some(3)
    );
    assert!(statistics
        .property_histograms
        .values()
        .any(|values| { values == &vec![Value::Int(1), Value::Int(2), Value::Int(3)] }));
    let distinct_report = db.distinct_value_statistics_consistency_report();
    assert!(distinct_report.ready);
    assert_eq!(
        distinct_report.maintained_property_distinct_counts,
        distinct_report.recomputed_property_distinct_counts
    );
    let property_index_report = db.property_index_consistency_report();
    assert!(property_index_report.ready);
    assert_eq!(
        property_index_report.node_index_entry_count,
        property_index_report.recomputed_node_index_entry_count
    );
    assert_eq!(
        property_index_report.relationship_index_reference_count,
        property_index_report.recomputed_relationship_index_reference_count
    );

    let read_tx = db.begin_read_transaction();
    db.query("CREATE (:Memory {id: 4, kind: 'note'})").unwrap();
    assert_eq!(read_tx.statistics().node_count, 4);
    assert_eq!(read_tx.basic_statistics().node_count, 4);
    assert_eq!(db.statistics().node_count, 5);
    assert_eq!(db.basic_statistics().node_count, 5);
    assert_eq!(db.basic_statistics().computed_at_commit_epoch, 7);
}

#[test]
fn explicit_text_index_publishes_payload_free_selectivity() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Memory").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Memory(body) TYPE TEXT")
        .unwrap();
    for id in 0..20 {
        let body = if id % 2 == 0 { "even" } else { "odd" };
        db.query(&format!("CREATE (:Memory {{id: {id}, body: '{body}'}})"))
            .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(body)").unwrap();

    let statistics = db.statistics();
    assert!(statistics
        .property_distinct_counts
        .keys()
        .all(|(_, property)| property != "body"));
    let index = db
        .property_indexes()
        .into_iter()
        .find(|index| index.property == "body" && index.kind == IndexKind::Equality)
        .unwrap();
    assert_eq!(
        statistics.index_samples.get(&index.id),
        Some(&crate::schema::IndexStatisticsSample::exact(20, 2))
    );

    let explain = db
        .explain_query("MATCH (m:Memory) WHERE m.body = 'even' RETURN m.id AS id")
        .unwrap();
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.contains("Memory.body")
            && decision.contains("seek_cost=32")
            && decision.contains("distinct_count=2")
    }));
}

#[test]
fn out_of_core_index_sample_tracks_wal_churn_and_becomes_stale() {
    let path = unique_test_dir("out_of_core_index_sample_churn");
    let config = DatabaseConfig {
        storage_residency_mode: skein_storage::StorageResidencyMode::OutOfCore,
        ..DatabaseConfig::default()
    };
    let (index_id, composite_index_id) = {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        db.query("CREATE INDEX ON :Memory(kind)").unwrap();
        db.query("CREATE INDEX ON :Memory(kind, id)").unwrap();
        for id in 0..100 {
            let kind = if id % 2 == 0 { "note" } else { "decision" };
            db.query(&format!("CREATE (:Memory {{id: {id}, kind: '{kind}'}})"))
                .unwrap();
        }
        let index_id = db
            .property_indexes()
            .into_iter()
            .find(|index| index.property == "kind")
            .unwrap()
            .id;
        let composite_index_id = db
            .composite_property_indexes()
            .into_iter()
            .find(|index| index.properties == ["kind", "id"])
            .unwrap()
            .id;
        db.checkpoint().unwrap();
        (index_id, composite_index_id)
    };

    {
        let mut db = Database::open_with_config(&path, config).unwrap();
        let statistics = db.statistics();
        assert_eq!(
            statistics.index_samples.get(&index_id),
            Some(&crate::schema::IndexStatisticsSample::exact(100, 2))
        );
        assert_eq!(
            statistics.index_samples.get(&composite_index_id),
            Some(&crate::schema::IndexStatisticsSample::exact(100, 100))
        );

        db.query("MATCH (m:Memory) WHERE m.id = 0 SET m.kind = 'note'")
            .unwrap();
        db.query("MATCH (m:Memory) WHERE m.id = 0 SET m.payload = 'not indexed'")
            .unwrap();
        assert_eq!(
            db.statistics()
                .index_samples
                .get(&index_id)
                .unwrap()
                .updates_since_sample,
            0
        );

        db.query("MATCH (m:Memory) WHERE m.id = 0 SET m.kind = 'changed'")
            .unwrap();
        db.query("MATCH (m:Memory) WHERE m.id = 1 DETACH DELETE m")
            .unwrap();
        for id in 100..104 {
            db.query(&format!("CREATE (:Memory {{id: {id}, kind: 'new-{id}'}})"))
                .unwrap();
        }
        let statistics = db.statistics();
        let sample = *statistics.index_samples.get(&index_id).unwrap();
        assert_eq!(sample.updates_since_sample, 6);
        assert!(sample.is_stale());
        assert_eq!(sample.estimated_unique_values(), None);
        let composite_sample = *statistics.index_samples.get(&composite_index_id).unwrap();
        assert_eq!(composite_sample.updates_since_sample, 6);
        assert!(composite_sample.is_stale());
        let explain = db
            .explain_query("MATCH (m:Memory) WHERE m.kind = 'note' RETURN m.id AS id")
            .unwrap();
        assert!(explain.trace.decisions.iter().any(|decision| {
            decision.contains("Memory.kind") && decision.contains("distinct_count=103")
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn range_and_composite_samples_use_complete_index_keys() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {kind: 'note', source_id: 1, rank: 1})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'note', source_id: 1, rank: 2})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'decision', source_id: 2, rank: 3})")
        .unwrap();
    db.query("CREATE RANGE INDEX ON :Memory(rank)").unwrap();
    db.query("CREATE INDEX ON :Memory(kind, source_id)")
        .unwrap();

    let statistics = db.statistics();
    let indexes = db.property_indexes();
    let range = indexes
        .iter()
        .find(|index| index.property == "rank" && index.kind == IndexKind::Range)
        .unwrap();
    assert_eq!(
        statistics.index_samples.get(&range.id),
        Some(&crate::schema::IndexStatisticsSample::exact(3, 3))
    );
    let composite = db
        .composite_property_indexes()
        .into_iter()
        .find(|index| index.properties == ["kind", "source_id"])
        .unwrap();
    assert_eq!(
        statistics.index_samples.get(&composite.id),
        Some(&crate::schema::IndexStatisticsSample::exact(3, 2))
    );
}

#[test]
fn basic_statistics_are_incremental_across_deletes_and_replay() {
    let path = unique_test_dir("basic_statistics_incremental");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1})-[:MENTIONS]->(:Entity {id: 10})")
            .unwrap();
        db.query("CREATE (:Memory {id: 2})").unwrap();
        let read_tx = db.begin_read_transaction();

        db.query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 DELETE r")
            .unwrap();
        db.query("MATCH (m:Memory) WHERE m.id = 2 DETACH DELETE m")
            .unwrap();

        let basic_statistics = db.basic_statistics();
        assert_eq!(basic_statistics.computed_at_commit_epoch, 5);
        assert_eq!(basic_statistics.node_count, 2);
        assert_eq!(basic_statistics.relationship_count, 0);
        assert_eq!(basic_statistics.label_counts.values().sum::<u64>(), 2);
        assert_eq!(basic_statistics.rel_type_counts.values().sum::<u64>(), 0);

        assert_eq!(read_tx.basic_statistics().computed_at_commit_epoch, 3);
        assert_eq!(read_tx.basic_statistics().node_count, 3);
        assert_eq!(read_tx.basic_statistics().relationship_count, 1);
    }

    {
        let db = Database::open(&path).unwrap();
        let basic_statistics = db.basic_statistics();
        assert_eq!(basic_statistics.computed_at_commit_epoch, 5);
        assert_eq!(basic_statistics.node_count, 2);
        assert_eq!(basic_statistics.relationship_count, 0);
        assert_eq!(db.statistics().node_count, basic_statistics.node_count);
        assert_eq!(
            db.statistics().relationship_count,
            basic_statistics.relationship_count
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_multi_hop_statistics_drive_expand_estimates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 1000, name: 'Mid'})")
        .unwrap();
    for id in 0..100 {
        db.query(&format!(
                "MERGE (:Entity {{id: 1000, name: 'Mid'}})-[:LINKS]->(:Entity {{id: {id}, name: 'Leaf {id}'}})"
            ))
            .unwrap();
    }

    let statistics = db.statistics();
    let exact_two_hop_count = statistics
        .bounded_path_counts
        .iter()
        .find_map(|((source_label, _, target_label, hops), count)| {
            (source_label.0 == 0 && target_label.0 == 1 && *hops == 2).then_some(count)
        })
        .copied();
    assert_eq!(exact_two_hop_count, Some(100));

    let explain = db
        .explain_query("MATCH (m:Memory)-[:LINKS*2..2]->(e:Entity) RETURN e.name AS name")
        .unwrap();
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.contains("estimate AdjacencyExpand")
            && decision.contains("*2..2")
            && decision.contains("hop_rows=[1:exact:1,2:exact:100]")
            && decision.contains("estimated_rows=100")
    }));
}

#[test]
fn optimizer_retains_complete_advanced_statistics_after_graph_epoch_advances() {
    let path = unique_test_dir("optimizer_advanced_statistics_freshness");
    let spill_root = path.join("statistics-spill");
    let config = DatabaseConfig {
        storage_residency_mode: crate::StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: 1024 * 1024,
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&path, config.clone()).unwrap();
    for id in 0..16 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}}})-[:MENTIONS {{weight: 1}}]->(:Entity {{id: {}}})",
            id + 100
        ))
        .unwrap();
    }
    db.checkpoint().unwrap();
    db.refresh_optimizer_statistics_external(&crate::OptimizerStatisticsRefreshOptions {
        memory_budget_bytes: 4096,
        max_spill_bytes: 1024 * 1024,
        max_spill_runs: 64,
        max_input_records: 1_000,
        max_generated_facts: 10_000,
        max_path_expansions: 1_000,
        spill_directory: spill_root,
    })
    .unwrap();

    let fresh = db
        .explain_query("MATCH (m:Memory)-[:MENTIONS*1..1]->(e:Entity) RETURN e.id AS entity_id")
        .unwrap();
    assert!(fresh.trace.decisions.iter().any(|decision| {
        decision.starts_with("optimizer advanced statistics freshness:")
            && decision.contains("status=fresh")
    }));
    assert!(fresh.trace.decisions.iter().any(|decision| {
        decision.contains("estimate AdjacencyExpand")
            && decision.contains("path_count=16")
            && decision.contains("hop_rows=[1:exact:16]")
    }));

    db.query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 0 SET r.weight = 2")
        .unwrap();
    let stale = db
        .explain_query("MATCH (m:Memory)-[:MENTIONS*1..1]->(e:Entity) RETURN e.id AS id")
        .unwrap();
    assert!(stale.trace.decisions.iter().any(|decision| {
        decision.starts_with("optimizer advanced statistics freshness:")
            && decision.contains("status=stale")
            && decision.contains("usable=true")
    }));
    assert!(stale.trace.decisions.iter().any(|decision| {
        decision.contains("estimate AdjacencyExpand")
            && decision.contains("path_count=16")
            && decision.contains("hop_rows=[1:exact:16]")
    }));

    db.checkpoint().unwrap();
    drop(db);
    let reopened = Database::open_with_config(&path, config).unwrap();
    let recovered = reopened
        .explain_query("MATCH (m:Memory)-[:MENTIONS*1..1]->(e:Entity) RETURN e.id AS recovered_id")
        .unwrap();
    assert!(recovered.trace.decisions.iter().any(|decision| {
        decision.starts_with("optimizer advanced statistics freshness:")
            && decision.contains("status=stale")
            && decision.contains("usable=true")
    }));
    assert!(recovered.trace.decisions.iter().any(|decision| {
        decision.contains("estimate AdjacencyExpand")
            && decision.contains("path_count=16")
            && decision.contains("hop_rows=[1:exact:16]")
    }));

    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn stale_advanced_statistics_keep_selective_index_plans_after_unrelated_writes() {
    let path = unique_test_dir("optimizer_stale_statistics_index_plan");
    let spill_root = path.join("statistics-spill");
    let config = DatabaseConfig {
        storage_residency_mode: crate::StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: 1024 * 1024,
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&path, config).unwrap();
    for id in 0..100 {
        db.query(&format!("CREATE (:Memory {{id: {id}, created_at: {id}}})"))
            .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(created_at)").unwrap();
    db.checkpoint().unwrap();
    db.refresh_optimizer_statistics_external(&crate::OptimizerStatisticsRefreshOptions {
        memory_budget_bytes: 64 * 1024,
        max_spill_bytes: 1024 * 1024,
        max_spill_runs: 64,
        max_input_records: 1_000,
        max_generated_facts: 10_000,
        max_path_expansions: 1_000,
        spill_directory: spill_root,
    })
    .unwrap();

    let fresh = db
        .explain_query("MATCH (m:Memory) WHERE m.created_at = 99 RETURN m.id AS fresh_memory_id")
        .unwrap();
    assert!(fresh.physical_plan.explain(0).contains("IndexNodeSeek"));
    assert!(fresh.trace.decisions.iter().any(|decision| {
        decision.contains("choose IndexNodeSeek") && decision.contains("distinct_count=100")
    }));

    db.query("MATCH (m:Memory) WHERE m.id = 0 SET m.note = 'updated'")
        .unwrap();
    let stale = db
        .explain_query("MATCH (m:Memory) WHERE m.created_at = 99 RETURN m.id AS stale_memory_id")
        .unwrap();
    assert!(stale.trace.decisions.iter().any(|decision| {
        decision.starts_with("optimizer advanced statistics freshness:")
            && decision.contains("status=stale")
            && decision.contains("usable=true")
    }));
    assert!(stale.physical_plan.explain(0).contains("IndexNodeSeek"));
    assert!(stale.trace.decisions.iter().any(|decision| {
        decision.contains("choose IndexNodeSeek") && decision.contains("distinct_count=100")
    }));

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_persists_index_descriptors_and_statistics() {
    let path = unique_test_dir("catalog_stats_checkpoint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE INDEX ON :Memory(id)").unwrap();
        db.query("CREATE INDEX ON :Memory(kind)").unwrap();
        db.query("CREATE (:Memory {id: 1, kind: 'note'})").unwrap();
        db.query("CREATE (:Memory {id: 2, kind: 'decision'})-[:MENTIONS {weight: 4}]->(:Entity {id: 10, name: 'Rust'})")
                .unwrap();
        db.checkpoint().unwrap();
    }

    let checkpoint = read_test_durable_text(&active_checkpoint_path(&path)).unwrap();
    assert!(checkpoint.contains("property_index"));
    assert!(checkpoint.contains("stat_commit_epoch\t5\n"));
    assert!(checkpoint.contains("stat_advanced_complete\ttrue\n"));
    assert!(checkpoint.contains("stat_histogram_sample_limit\t512\n"));
    assert!(checkpoint.contains("stat_node_count\t3\n"));
    assert!(checkpoint.contains("stat_relationship_count\t1\n"));
    assert!(checkpoint.contains("stat_rel_type_source_count"));
    assert!(checkpoint.contains("stat_path_count"));
    assert!(checkpoint.contains("stat_bounded_path_count"));
    assert!(checkpoint.contains("stat_bounded_path_source_distinct_count"));
    assert!(checkpoint.contains("stat_bounded_path_target_distinct_count"));
    assert!(checkpoint.contains("stat_index_sample"));
    assert!(checkpoint.contains("stat_property_distinct_count"));
    assert!(checkpoint.contains("stat_rel_property_distinct_count"));
    assert!(checkpoint.contains("stat_rel_property_histogram"));
    assert!(checkpoint.contains("stat_property_histogram"));
    assert!(checkpoint.contains("stat_rel_property_histogram_sampled"));
    assert!(checkpoint.contains("stat_property_histogram_sampled"));
    assert!(checkpoint.contains("stat_rel_type_target_count"));

    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .property_indexes()
            .iter()
            .any(|index| index.property == "kind"));
        assert_eq!(db.statistics().computed_at_commit_epoch, 5);
        assert!(db.statistics().advanced_statistics_complete);
        assert_eq!(db.statistics().histogram_sample_limit, 512);
        assert_eq!(db.statistics().node_count, 3);
        assert_eq!(db.statistics().relationship_count, 1);
    }
    std::fs::remove_dir_all(path).unwrap();
}
