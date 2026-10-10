// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use hawdb_search::SearchMode;

fn oversized_nonmatching_source() -> (Database, SearchIndex, KnowledgeRetrievalRequest) {
    let mut database = Database::new();
    let params = BTreeMap::from([("body".into(), Value::String("x".repeat(64 * 1024)))]);
    database
        .query_with_params(
            "CREATE (:Memory {id: 'unrelated', title: 'unrelated', content: $body})",
            &params,
        )
        .unwrap();
    database.config.execution_memory.query_memory_bytes = NonZeroUsize::new(8 * 1024).unwrap();
    let request = KnowledgeRetrievalRequest {
        query_text: "needle".into(),
        query_embedding: None,
        mode: SearchMode::Text,
        limit: 8,
        offset: 0,
        rank_window: None,
        search_fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        candidate_limit: Some(1),
        candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
        graph_seed_limit: 1,
        graph_context_limit: 0,
        graph_context_max_hops: 0,
    };
    (database, SearchIndex::in_memory(), request)
}

#[test]
fn legacy_graph_seed_live_retrieval_refuses_oversized_nonmatching_owned_source() {
    let (database, index, request) = oversized_nonmatching_source();
    let result = database.retrieve_knowledge(&index, &request);
    assert!(matches!(result, Err(HawDBError::Execution(_))), "every canonical source needs query admission even when no candidate matches; actual {result:?}");
}

#[test]
fn legacy_graph_seed_snapshot_retrieval_refuses_oversized_nonmatching_owned_source() {
    let (database, index, request) = oversized_nonmatching_source();
    let result = database
        .begin_read_transaction()
        .unwrap()
        .retrieve_knowledge(&index, &request);
    assert!(matches!(result, Err(HawDBError::Execution(_))), "snapshot canonical source needs admission even when no candidate matches; actual {result:?}");
}

fn overlapping_filter_sources() -> (Database, SearchIndex, KnowledgeRetrievalRequest) {
    let mut database = Database::new();
    let params = BTreeMap::from([("body".into(), Value::String("x".repeat(12 * 1024)))]);
    database.query_with_params(
        "CREATE (:Memory {id: 'unrelated', title: 'unrelated', content: $body})-[:HAS_LABEL]->(:Label {id: 'label', canonical_name: 'topic', description: $body})",
        &params,
    ).unwrap();
    database.config.execution_memory.query_memory_bytes = NonZeroUsize::new(20 * 1024).unwrap();
    let request = KnowledgeRetrievalRequest {
        query_text: "needle".into(),
        query_embedding: None,
        mode: SearchMode::Text,
        limit: 8,
        offset: 0,
        rank_window: None,
        search_fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::from([
            ("kind".into(), "memory".into()),
            ("labels".into(), "topic".into()),
        ]),
        candidate_limit: Some(1),
        candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
        graph_seed_limit: 1,
        graph_context_limit: 0,
        graph_context_max_hops: 0,
    };
    (database, SearchIndex::in_memory(), request)
}

#[test]
fn legacy_graph_seed_live_filter_refuses_overlapping_source_and_label_reads() {
    let (database, index, request) = overlapping_filter_sources();
    let result = database.retrieve_knowledge(&index, &request);
    assert!(
        matches!(result, Err(HawDBError::Execution(_))),
        "source and business-label ownership must share one root budget; actual {result:?}"
    );
}

#[test]
fn legacy_graph_seed_snapshot_filter_refuses_overlapping_source_and_label_reads() {
    let (database, index, request) = overlapping_filter_sources();
    let result = database
        .begin_read_transaction()
        .unwrap()
        .retrieve_knowledge(&index, &request);
    assert!(matches!(result, Err(HawDBError::Execution(_))), "snapshot source and business-label ownership must share one root budget; actual {result:?}");
}

#[test]
fn legacy_graph_seed_preserves_contextual_unicode_metadata_filtering() {
    let mut database = Database::new();
    database
        .query("CREATE (:Memory {id: 'greek', title: 'ΟΣ'})")
        .unwrap();
    let request = KnowledgeRetrievalRequest {
        query_text: "ΟΣ".into(),
        query_embedding: None,
        mode: SearchMode::Text,
        limit: 8,
        offset: 0,
        rank_window: None,
        search_fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::from([("title".into(), "ος".into())]),
        candidate_limit: Some(1),
        candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
        graph_seed_limit: 1,
        graph_context_limit: 0,
        graph_context_max_hops: 0,
    };
    let index = SearchIndex::in_memory();
    let result = database.retrieve_knowledge(&index, &request).unwrap();
    assert_eq!(
        result.graph_seeds.len(),
        1,
        "string metadata normalization must preserve contextual final sigma"
    );
    assert_eq!(
        result.graph_seeds[0].entity.external_id.as_deref(),
        Some("greek")
    );
}

fn oversized_lookup_without_seed_scan() -> (Database, SearchIndex, KnowledgeRetrievalRequest) {
    let mut database = Database::new();
    database
        .query_with_params(
            "CREATE (:Memory {id: 'point', title: 'needle', opaque: $body})",
            &BTreeMap::from([("body".into(), Value::String("x".repeat(64 * 1024)))]),
        )
        .unwrap();
    let mut index = SearchIndex::in_memory();
    database
        .rebuild_search_projection(&mut index, SearchRebuildOptions::default())
        .unwrap();
    database.config.execution_memory.query_memory_bytes = NonZeroUsize::new(16 * 1024).unwrap();
    let request = KnowledgeRetrievalRequest {
        query_text: "needle".into(),
        query_embedding: None,
        mode: SearchMode::Text,
        limit: 1,
        offset: 0,
        rank_window: None,
        search_fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        candidate_limit: Some(0),
        candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
        graph_seed_limit: 0,
        graph_context_limit: 0,
        graph_context_max_hops: 0,
    };
    (database, index, request)
}

#[test]
fn legacy_graph_seed_disabled_live_lookup_admits_owned_input_before_empty_top_k() {
    let (database, index, request) = oversized_lookup_without_seed_scan();
    let result = database.retrieve_knowledge(&index, &request);
    assert!(matches!(result, Err(HawDBError::Execution(_))), "canonical lookup ownership must fit the root even when the seed scan is disabled and final candidates are empty; actual {result:?}");
}

#[test]
fn legacy_graph_seed_disabled_snapshot_lookup_admits_owned_input_before_empty_top_k() {
    let (database, index, request) = oversized_lookup_without_seed_scan();
    let result = database
        .begin_read_transaction()
        .unwrap()
        .retrieve_knowledge(&index, &request);
    assert!(matches!(result, Err(HawDBError::Execution(_))), "pinned canonical lookup ownership must fit the root before an empty final candidate window; actual {result:?}");
}

#[test]
fn legacy_graph_seed_numeric_features_preserve_canonical_aliases_and_values() {
    let values = [
        Value::Null,
        Value::Bool(true),
        Value::Bool(false),
        Value::Int(i64::MIN),
        Value::Int(i64::MAX),
        Value::Float(-0.0),
        Value::Float(f64::from_bits(1)),
        Value::Float(1.2345678901234567),
        Value::Float(f64::NAN),
        Value::Float(f64::INFINITY),
        Value::Float(f64::NEG_INFINITY),
        Value::String("17".into()),
        Value::String("-0".into()),
        Value::String(" 17".into()),
        Value::String("17 ".into()),
        Value::String("1e309".into()),
        Value::String("".into()),
        Value::String("NaN".into()),
        Value::String("inf".into()),
        Value::String("+1.25".into()),
        Value::Binary(vec![17; 4]),
        Value::Uuid("00000000-0000-0000-0000-000000000017".parse().unwrap()),
        Value::List(vec![]),
        Value::List(vec![Value::Null]),
        Value::List(vec![Value::Null, Value::Null]),
        Value::List(vec![Value::List(vec![Value::String("17".into())])]),
        Value::List(vec![Value::Int(1), Value::Int(2)]),
        Value::Map(BTreeMap::new()),
        Value::Map(BTreeMap::from([("".into(), Value::Null)])),
    ];
    for value in values {
        let node = NodeRecord {
            id: NodeId(9_007_199_254_740_993),
            labels: Default::default(),
            properties: BTreeMap::from([
                ("id".into(), value.clone()),
                ("source_id".into(), value.clone()),
                ("thread_id".into(), Value::String("31".into())),
                ("source".into(), Value::String("47".into())),
                ("space_id".into(), value.clone()),
                ("numeric".into(), value.clone()),
                ("labels".into(), value),
            ]),
        };
        for key in [
            "kind",
            "external_id",
            "source_id",
            "space_id",
            "numeric",
            "labels",
            "missing",
        ] {
            // Independent legacy text/alias helpers remain the behavior oracle.
            let expected = match key {
                "kind" => None,
                "external_id" => parse_metadata_filter_number(&projected_node_external_id(&node)),
                "source_id" => node_projection_source_id(&node)
                    .as_deref()
                    .and_then(parse_metadata_filter_number),
                "space_id" => parse_metadata_filter_number(&normalized_node_space_id(&node)),
                _ => node
                    .properties
                    .get(key)
                    .map(value_to_external_id)
                    .as_deref()
                    .and_then(parse_metadata_filter_number),
            };
            assert_eq!(
                knowledge_graph_seed_filter_numeric_value(&node, key).map(f64::to_bits),
                expected.map(f64::to_bits),
                "legacy numeric semantics for {key}, properties {:?}",
                node.properties
            );
        }
    }
}

fn oversized_discarded_expansion_relationship() -> (Database, SearchIndex, KnowledgeRetrievalRequest)
{
    let mut database = Database::new();
    database.query("CREATE (:Memory {id: 'edge_seed', title: 'needle'})-[:RELATED]->(:Memory {id: 'edge_first', title: 'unrelated'})").unwrap();
    database
        .query("CREATE (:Memory {id: 'edge_overflow', title: 'unrelated'})")
        .unwrap();
    database.query_with_params(
        "MATCH (s:Memory {id: 'edge_seed'}), (t:Memory {id: 'edge_overflow'}) CREATE (s)-[:RELATED {opaque: $body}]->(t)",
        &BTreeMap::from([("body".into(), Value::String("x".repeat(64 * 1024)))]),
    ).unwrap();
    let mut index = SearchIndex::in_memory();
    database
        .rebuild_search_projection(&mut index, SearchRebuildOptions::default())
        .unwrap();
    database.config.execution_memory.query_memory_bytes = NonZeroUsize::new(24 * 1024).unwrap();
    let request = KnowledgeRetrievalRequest {
        query_text: "needle".into(),
        query_embedding: None,
        mode: SearchMode::Text,
        limit: 1,
        offset: 0,
        rank_window: None,
        search_fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        candidate_limit: Some(0),
        candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
        graph_seed_limit: 0,
        graph_context_limit: 1,
        graph_context_max_hops: 1,
    };
    (database, index, request)
}

#[test]
fn legacy_graph_seed_disabled_live_expansion_admits_discarded_relationship() {
    let (database, index, request) = oversized_discarded_expansion_relationship();
    let result = database.retrieve_knowledge(&index, &request);
    assert!(matches!(result, Err(HawDBError::Execution(_))), "owned expansion input must fit the root even when its large properties fall outside the final path window; actual {result:?}");
}

#[test]
fn legacy_graph_seed_disabled_snapshot_expansion_admits_discarded_relationship() {
    let (database, index, request) = oversized_discarded_expansion_relationship();
    let result = database
        .begin_read_transaction()
        .unwrap()
        .retrieve_knowledge(&index, &request);
    assert!(matches!(result, Err(HawDBError::Execution(_))), "pinned expansion input must be admitted before copying properties discarded by final path selection; actual {result:?}");
}
