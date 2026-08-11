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
