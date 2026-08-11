use super::*;
use crate::store::{ManifestGeneration, StorageResidencyMode};
use crate::DatabaseReadTransaction;
use std::sync::{Arc, Barrier};

#[test]
fn read_transaction_keeps_snapshot_before_later_commit() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Before snapshot'})")
        .unwrap();

    let mut read_tx = db.begin_read_transaction();
    db.query("CREATE (:Memory {id: 2, title: 'After snapshot'})")
        .unwrap();

    let before = read_tx
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        before.rows[0].get("title"),
        Some(&Value::String("Before snapshot".to_string()))
    );
    let after = read_tx
        .query("MATCH (m:Memory) WHERE m.id = 2 RETURN m.title AS title")
        .unwrap();
    assert!(after.rows.is_empty());

    let latest = db
        .query("MATCH (m:Memory) WHERE m.id = 2 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        latest.rows[0].get("title"),
        Some(&Value::String("After snapshot".to_string()))
    );
}

#[test]
fn published_read_view_separates_logical_visibility_from_physical_generation() {
    let path = unique_test_dir("published_read_view_identity");
    let mut db = Database::open(&path).unwrap();

    let empty = db.published_read_view();
    assert_eq!(empty.visible_commit_epoch(), 1);
    assert_eq!(empty.checkpoint_commit_epoch(), None);
    assert_eq!(empty.physical_generation(), None);

    db.query("CREATE (:Memory {id: 1, title: 'Checkpoint base'})")
        .unwrap();
    db.checkpoint().unwrap();
    let checkpointed = db.published_read_view();
    assert_eq!(checkpointed.visible_commit_epoch(), 2);
    assert_eq!(checkpointed.checkpoint_commit_epoch(), Some(2));
    assert_eq!(
        checkpointed.physical_generation(),
        Some(ManifestGeneration(1))
    );
    assert!(checkpointed.physical_base_is_current());
    assert!(!checkpointed.has_delta_after_physical_generation());

    db.query("CREATE (:Memory {id: 2, title: 'Logical delta'})")
        .unwrap();
    let reader = db.begin_read_transaction();
    let pinned = reader.published_read_view();
    assert_eq!(pinned.visible_commit_epoch(), 3);
    assert_eq!(pinned.checkpoint_commit_epoch(), Some(2));
    assert_eq!(pinned.physical_generation(), Some(ManifestGeneration(1)));
    assert!(!pinned.physical_base_is_current());
    assert!(pinned.has_delta_after_physical_generation());

    db.checkpoint().unwrap();
    let current = db.published_read_view();
    assert_eq!(current.visible_commit_epoch(), 3);
    assert_eq!(current.checkpoint_commit_epoch(), Some(3));
    assert_eq!(current.physical_generation(), Some(ManifestGeneration(2)));
    assert!(current.physical_base_is_current());
    assert_eq!(reader.published_read_view(), pinned);

    drop(reader);
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_transaction_keeps_parameterized_query_snapshot() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Before snapshot'})-[:LINKS]->(:Entity {id: 'mid', name: 'Mid'})")
            .unwrap();

    let mut read_tx = db.begin_read_transaction();
    let leaf = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("leaf".to_string())),
                ("name".to_string(), Value::String("Leaf".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(&mut db.catalog, NodeId(0), leaf, "LINKS", BTreeMap::new())
        .unwrap();

    let parameters = BTreeMap::from([("id".to_string(), Value::String("root".to_string()))]);
    let entity = read_tx
        .query_with_params(
            "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title",
            &parameters,
        )
        .unwrap();
    assert_eq!(
        entity.rows[0].get("title"),
        Some(&Value::String("Before snapshot".to_string()))
    );
    let scoped_entity = read_tx
        .query_with_params(
            "MATCH (m:Memory) WHERE m.id = $id AND m.title = $title RETURN m.id AS id",
            &BTreeMap::from([
                ("id".to_string(), Value::String("root".to_string())),
                (
                    "title".to_string(),
                    Value::String("Before snapshot".to_string()),
                ),
            ]),
        )
        .unwrap();
    assert_eq!(scoped_entity.rows.len(), 1);
    let entity_batch = read_tx
        .query_with_params(
            "MATCH (n) WHERE n.id IN $ids RETURN n.id AS id ORDER BY id",
            &BTreeMap::from([(
                "ids".to_string(),
                Value::List(vec![
                    Value::String("root".to_string()),
                    Value::String("leaf".to_string()),
                ]),
            )]),
        )
        .unwrap();
    assert_eq!(entity_batch.rows.len(), 1);
    assert_eq!(
        entity_batch.rows[0].get("id"),
        Some(&Value::String("root".to_string()))
    );
    let property_batch = read_tx
        .query_with_params(
            "MATCH (n) WHERE n.id IN $ids RETURN n.id AS id, n.title AS title, n.name AS name",
            &BTreeMap::from([(
                "ids".to_string(),
                Value::List(vec![
                    Value::String("root".to_string()),
                    Value::String("leaf".to_string()),
                ]),
            )]),
        )
        .unwrap();
    assert_eq!(property_batch.rows.len(), 1);
    assert_eq!(
        property_batch.rows[0].get("title"),
        Some(&Value::String("Before snapshot".to_string()))
    );

    let snapshot_neighbors = read_tx
        .query_with_params(
            "MATCH (m:Memory)-[:LINKS]->(e:Entity) WHERE m.id = $id \
             RETURN e.id AS id ORDER BY id LIMIT 8",
            &parameters,
        )
        .unwrap();
    assert_eq!(snapshot_neighbors.rows.len(), 1);
    assert_eq!(
        snapshot_neighbors.rows[0].get("id"),
        Some(&Value::String("mid".to_string()))
    );

    let latest_neighbors = db
        .query_with_params(
            "MATCH (m:Memory)-[:LINKS]->(e:Entity) WHERE m.id = $id \
             RETURN e.id AS id ORDER BY id LIMIT 8",
            &parameters,
        )
        .unwrap();
    assert_eq!(latest_neighbors.rows.len(), 2);
    assert!(latest_neighbors
        .rows
        .iter()
        .any(|row| row.get("id") == Some(&Value::String("leaf".to_string()))));

    let snapshot_paths = read_tx
        .query_with_params(
            "MATCH (m:Memory)-[:LINKS]->(e:Entity) \
             WHERE m.id = $source_id AND e.id = $target_id RETURN e.id AS id LIMIT 4",
            &BTreeMap::from([
                ("source_id".to_string(), Value::String("root".to_string())),
                ("target_id".to_string(), Value::String("leaf".to_string())),
            ]),
        )
        .unwrap();
    assert!(snapshot_paths.rows.is_empty());

    let snapshot_subgraph = read_tx
        .query_with_params(
            "MATCH (m:Memory)-[:LINKS]->(e:Entity) WHERE m.id = $id \
             RETURN m.id AS source_id, e.id AS target_id LIMIT 8",
            &parameters,
        )
        .unwrap();
    assert_eq!(snapshot_subgraph.rows.len(), 1);
    assert_eq!(
        snapshot_subgraph.rows[0].get("target_id"),
        Some(&Value::String("mid".to_string()))
    );
}

#[test]
fn read_transaction_retrieves_knowledge_from_pinned_snapshot() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Snapshot retrieval', content: 'snapshot retrieval root'})-[:MENTIONS]->(:Entity {id: 'before', name: 'Before'})")
            .unwrap();
    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let read_tx = db.begin_read_transaction();
    let after = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("after".to_string())),
                ("name".to_string(), Value::String("After".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(0),
            after,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();
    db.query("CREATE (:Memory {id: 'later', title: 'Snapshot retrieval', content: 'snapshot retrieval later'})")
        .unwrap();

    let snapshot_output = read_tx.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "snapshot retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 4,
            offset: 0,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 8,
            graph_context_limit: 8,
            graph_context_max_hops: 1,
        },
    );
    assert_eq!(snapshot_output.graph_commit_epoch, 1);
    assert_eq!(snapshot_output.graph_context_paths.len(), 2);
    assert!(snapshot_output
        .graph_context_paths
        .iter()
        .all(|path| path.target_external_id.as_deref() == Some("before")));
    assert!(snapshot_output
        .graph_context_paths
        .iter()
        .all(|path| path.target_external_id.as_deref() != Some("after")));
    assert!(snapshot_output
        .graph_seeds
        .iter()
        .all(|seed| seed.entity.external_id.as_deref() != Some("later")));

    let latest_output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "snapshot retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 4,
            offset: 0,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 8,
            graph_context_limit: 8,
            graph_context_max_hops: 1,
        },
    );
    assert_eq!(latest_output.graph_commit_epoch, 4);
    assert!(latest_output
        .graph_context_paths
        .iter()
        .any(|path| path.target_external_id.as_deref() == Some("after")));
    assert!(latest_output
        .graph_seeds
        .iter()
        .any(|seed| seed.entity.external_id.as_deref() == Some("later")));
}

#[test]
fn read_transaction_rebuilds_search_projection_from_pinned_snapshot() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'snapshot', title: 'Pinned projection', content: 'snapshot only'})",
    )
    .unwrap();
    let read_tx = db.begin_read_transaction();
    db.query("CREATE (:Memory {id: 'live', title: 'Pinned projection', content: 'live only'})")
        .unwrap();

    let mut snapshot_index = SearchIndex::in_memory();
    let summary = read_tx
        .rebuild_search_projection(&mut snapshot_index, SearchRebuildOptions::default())
        .unwrap();

    assert_eq!(summary.scanned_nodes, 1);
    assert_eq!(summary.indexed_documents, 1);
    assert!(snapshot_index.document("memory:snapshot").is_some());
    assert!(snapshot_index.document("memory:live").is_none());
    assert_eq!(
        snapshot_index
            .projection_freshness()
            .source_graph_commit_epoch,
        Some(1)
    );

    let mut live_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut live_index, SearchRebuildOptions::default())
        .unwrap();
    assert!(live_index.document("memory:snapshot").is_some());
    assert!(live_index.document("memory:live").is_some());
    assert_eq!(
        live_index.projection_freshness().source_graph_commit_epoch,
        Some(2)
    );
}

#[test]
fn read_transaction_repairs_search_projection_metadata_from_pinned_snapshot() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'snapshot', title: 'Pinned projection', content: 'snapshot only', source_id: 'before', space_id: 'snapshot-space'})",
    )
    .unwrap();
    let read_tx = db.begin_read_transaction();
    db.query(
        "MATCH (m:Memory {id: 'snapshot'}) SET m.source_id = 'after', m.space_id = 'live-space'",
    )
    .unwrap();

    let mut snapshot_index = SearchIndex::in_memory();
    snapshot_index
        .upsert(SearchDocument {
            id: "memory:snapshot".to_string(),
            title: "Existing title".to_string(),
            content: "Existing body should stay".to_string(),
            embedding: Some(vec![1.0, 0.0]),
            metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
        })
        .unwrap();

    let summary = read_tx
        .repair_search_projection_metadata(&mut snapshot_index, MetadataRepairOptions::default())
        .unwrap();

    assert_eq!(summary.scanned_nodes, 1);
    assert_eq!(summary.repaired_documents, 1);
    let document = snapshot_index.document("memory:snapshot").unwrap();
    assert_eq!(document.title, "Existing title");
    assert_eq!(document.content, "Existing body should stay");
    assert_eq!(document.embedding, Some(vec![1.0, 0.0]));
    assert_eq!(
        document.metadata.get("source_id").map(String::as_str),
        Some("before")
    );
    assert_eq!(
        document.metadata.get("space_id").map(String::as_str),
        Some("snapshot-space")
    );

    db.repair_search_projection_metadata(&mut snapshot_index, MetadataRepairOptions::default())
        .unwrap();
    let live_document = snapshot_index.document("memory:snapshot").unwrap();
    assert_eq!(
        live_document.metadata.get("source_id").map(String::as_str),
        Some("after")
    );
    assert_eq!(
        live_document.metadata.get("space_id").map(String::as_str),
        Some("live-space")
    );
}

#[test]
fn read_transaction_survives_later_checkpoint() {
    let path = unique_test_dir("read_tx_checkpoint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Pinned snapshot'})")
            .unwrap();

        let mut read_tx = db.begin_read_transaction();
        db.query("CREATE (:Memory {id: 2, title: 'Checkpoint commit'})")
            .unwrap();
        db.checkpoint().unwrap();

        let pinned = read_tx
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            pinned.rows[0].get("title"),
            Some(&Value::String("Pinned snapshot".to_string()))
        );
        let later = read_tx
            .query("MATCH (m:Memory) WHERE m.id = 2 RETURN m.title AS title")
            .unwrap();
        assert!(later.rows.is_empty());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_transaction_pins_checkpoint_manifest_until_drop() {
    let path = unique_test_dir("read_tx_manifest_pin");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Pinned snapshot'})")
            .unwrap();

        {
            let _read_tx = db.begin_read_transaction();
            db.query("CREATE (:Memory {id: 2, title: 'Newer commit'})")
                .unwrap();
            db.checkpoint().unwrap();
            let manifest = std::fs::read_to_string(path.join("manifest.skein")).unwrap();
            assert!(manifest.contains("checkpoint_commit_epoch\t3\n"));
            assert!(manifest.contains("oldest_reader_commit_epoch\t2\n"));
            assert!(manifest.contains("safe_reclaim_commit_epoch\t1\n"));
            let watermark = db.storage_reclamation_watermark();
            assert_eq!(watermark.current_commit_epoch, 3);
            assert_eq!(watermark.checkpoint_epoch, Some(1));
            assert_eq!(watermark.checkpoint_commit_epoch, Some(3));
            assert_eq!(watermark.oldest_reader_commit_epoch, Some(2));
            assert_eq!(watermark.safe_reclaim_commit_epoch, 1);
            assert!(watermark.durable);
        }

        db.checkpoint().unwrap();
        let manifest = std::fs::read_to_string(path.join("manifest.skein")).unwrap();
        assert!(manifest.contains("checkpoint_commit_epoch\t3\n"));
        assert!(manifest.contains("oldest_reader_commit_epoch\tnone\n"));
        assert!(manifest.contains("safe_reclaim_commit_epoch\t3\n"));
        let watermark = db.storage_reclamation_watermark();
        assert_eq!(watermark.current_commit_epoch, 3);
        assert_eq!(watermark.checkpoint_epoch, Some(2));
        assert_eq!(watermark.checkpoint_commit_epoch, Some(3));
        assert_eq!(watermark.oldest_reader_commit_epoch, None);
        assert_eq!(watermark.safe_reclaim_commit_epoch, 3);
        assert!(watermark.durable);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn out_of_core_reader_pin_retains_its_canonical_generation_until_drop() {
    let path = unique_test_dir("out_of_core_reader_generation_pin");
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&path, config).unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Pinned snapshot'})")
        .unwrap();
    db.checkpoint().unwrap();

    let mut reader = db.begin_read_transaction();
    let pinned_view = reader.published_read_view();
    assert_eq!(pinned_view.visible_commit_epoch(), 2);
    assert_eq!(pinned_view.checkpoint_commit_epoch(), Some(2));
    assert_eq!(
        pinned_view.physical_generation(),
        Some(ManifestGeneration(1))
    );
    db.query("CREATE (:Memory {id: 2, title: 'Second'})")
        .unwrap();
    db.checkpoint().unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Third'})")
        .unwrap();
    db.checkpoint().unwrap();

    assert!(path.join("canonical.1.skein").exists());
    let pinned = reader
        .query("MATCH (m:Memory) RETURN m.id AS id ORDER BY id")
        .unwrap();
    assert_eq!(pinned.rows.len(), 1);
    assert_eq!(pinned.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(reader.published_read_view(), pinned_view);

    drop(reader);
    db.checkpoint().unwrap();
    assert!(!path.join("canonical.1.skein").exists());
    assert!(!path.join("canonical.2.skein").exists());
    assert!(path.join("canonical.3.skein").exists());
    assert!(path.join("canonical.4.skein").exists());
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn overlapping_pinned_reads_survive_serialized_durable_commit() {
    let path = unique_test_dir("overlapping_pinned_reads");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Pinned snapshot'})")
        .unwrap();

    let first_reader = db.begin_read_transaction();
    let second_reader = db.begin_read_transaction();
    let readers_started = Arc::new(Barrier::new(3));
    let readers_release = Arc::new(Barrier::new(3));
    let spawn_reader = |mut reader: DatabaseReadTransaction| {
        let readers_started = Arc::clone(&readers_started);
        let readers_release = Arc::clone(&readers_release);
        std::thread::spawn(move || {
            readers_started.wait();
            let before = reader
                .query("MATCH (m:Memory {id: 1}) RETURN m.title AS title")
                .unwrap();
            let after = reader
                .query("MATCH (m:Memory {id: 2}) RETURN m.title AS title")
                .unwrap();
            readers_release.wait();
            (before, after)
        })
    };
    let first = spawn_reader(first_reader);
    let second = spawn_reader(second_reader);

    readers_started.wait();
    db.query("CREATE (:Memory {id: 2, title: 'Durable commit'})")
        .unwrap();
    db.checkpoint().unwrap();
    let watermark = db.storage_reclamation_watermark();
    assert_eq!(watermark.current_commit_epoch, 3);
    assert_eq!(watermark.oldest_reader_commit_epoch, Some(2));
    assert_eq!(watermark.safe_reclaim_commit_epoch, 1);
    assert!(watermark.durable);

    readers_release.wait();
    for reader in [first, second] {
        let (before, after) = reader.join().unwrap();
        assert_eq!(
            before.rows[0].get("title"),
            Some(&Value::String("Pinned snapshot".to_string()))
        );
        assert!(after.rows.is_empty());
    }
    assert_eq!(
        db.storage_reclamation_watermark()
            .oldest_reader_commit_epoch,
        None
    );
    drop(db);

    let mut reopened = Database::open(&path).unwrap();
    let durable = reopened
        .query("MATCH (m:Memory {id: 2}) RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        durable.rows[0].get("title"),
        Some(&Value::String("Durable commit".to_string()))
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn in_memory_reclamation_watermark_tracks_reader_pins() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Pinned snapshot'})")
        .unwrap();

    {
        let _read_tx = db.begin_read_transaction();
        db.query("CREATE (:Memory {id: 2, title: 'Newer commit'})")
            .unwrap();
        let watermark = db.storage_reclamation_watermark();
        assert_eq!(watermark.current_commit_epoch, 2);
        assert_eq!(watermark.checkpoint_epoch, None);
        assert_eq!(watermark.checkpoint_commit_epoch, None);
        assert_eq!(watermark.oldest_reader_commit_epoch, Some(1));
        assert_eq!(watermark.safe_reclaim_commit_epoch, 0);
        assert!(!watermark.durable);
    }

    let watermark = db.storage_reclamation_watermark();
    assert_eq!(watermark.current_commit_epoch, 2);
    assert_eq!(watermark.oldest_reader_commit_epoch, None);
    assert_eq!(watermark.safe_reclaim_commit_epoch, 2);
    assert!(!watermark.durable);
}

#[test]
fn read_transaction_rejects_mutations() {
    let db = Database::new();
    let mut read_tx = db.begin_read_transaction();
    let error = read_tx
        .query("CREATE (:Memory {id: 1, title: 'No writes'})")
        .unwrap_err();
    assert!(error.to_string().contains("must not be a mutation"));
}

#[test]
fn read_transaction_rejects_checkpoint_control() {
    let db = Database::new();
    let mut read_tx = db.begin_read_transaction();
    let error = read_tx.query("CHECKPOINT").unwrap_err();

    assert!(error
        .to_string()
        .contains("CHECKPOINT is not allowed inside a read transaction"));
}
