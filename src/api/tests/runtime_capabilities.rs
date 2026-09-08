use super::*;
use crate::{QueryAccessControlContext, RuntimeCapabilities, RuntimeCapability, SkeinError};

#[test]
fn disabled_query_capabilities_fail_before_planning_or_catalog_mutation() {
    let mut db = Database::new_with_config(DatabaseConfig {
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::FullTextSearch, false)
            .with(RuntimeCapability::VectorSearch, false)
            .with(RuntimeCapability::GraphAnalytics, false),
        ..DatabaseConfig::default()
    });
    let plan_cache_before = db.plan_cache_stats();

    assert_capability_error(
        db.query("CREATE FULLTEXT INDEX ON :Memory(title)")
            .unwrap_err(),
        RuntimeCapability::FullTextSearch,
    );
    assert_capability_error(
        db.query("CALL vector_search($embedding, topK := 1) RETURN id, score")
            .unwrap_err(),
        RuntimeCapability::VectorSearch,
    );
    assert_capability_error(
        db.query("CALL project_graph('EntityGraph', ['Entity'], ['MENTIONS'])")
            .unwrap_err(),
        RuntimeCapability::GraphAnalytics,
    );

    assert_eq!(db.plan_cache_stats(), plan_cache_before);
    assert!(db.property_indexes().is_empty());
}

#[test]
fn disabled_search_capability_does_not_fall_back_to_another_retriever() {
    let mut search = SearchIndex::in_memory();
    search.set_runtime_capabilities(
        RuntimeCapabilities::default().with(RuntimeCapability::FullTextSearch, false),
    );

    let error = search
        .try_search_with_options(
            "skein",
            None,
            SearchMode::Text,
            crate::search::SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        )
        .unwrap_err();

    assert_capability_error(error, RuntimeCapability::FullTextSearch);
}

#[test]
fn disabled_background_capability_precedes_qos_admission() {
    let mut db = Database::new_with_config(DatabaseConfig {
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::BackgroundMaintenance, false),
        ..DatabaseConfig::default()
    });

    let error = db
        .run_background_schema_maintenance(&LocalQosPolicy::default(), &LocalQosState::default(), 0)
        .unwrap_err();

    assert_capability_error(error, RuntimeCapability::BackgroundMaintenance);
}

#[test]
fn access_control_is_disabled_by_default_and_fails_closed_when_requested() {
    let mut search = SearchIndex::in_memory();
    search.set_runtime_capabilities(
        RuntimeCapabilities::default().with(RuntimeCapability::AccessControl, false),
    );
    search
        .upsert(SearchDocument {
            id: "doc-1".to_string(),
            title: "ACL".to_string(),
            content: "visibility scoped".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("space_id".to_string(), "allowed".to_string())]),
        })
        .unwrap();

    let error = search
        .try_search_with_options_access_control(
            "visibility",
            None,
            SearchMode::Text,
            crate::search::SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
            crate::search::SearchAccessControlContext::visibility_scopes(
                7,
                "space_id",
                ["allowed"],
            ),
        )
        .unwrap_err();

    assert_capability_error(error, RuntimeCapability::AccessControl);
}

#[test]
fn cypher_access_control_is_disabled_by_default_and_fails_before_plan_cache() {
    let db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::AccessControl, false),
        ..DatabaseConfig::default()
    });
    let before = db.plan_cache_stats();

    let error = db
        .explain_query_with_params_access_control(
            "MATCH (m:Memory) RETURN m.id AS id",
            &BTreeMap::new(),
            QueryAccessControlContext::visibility_scope(7, "space_id", "allowed"),
        )
        .unwrap_err();

    assert_capability_error(error, RuntimeCapability::AccessControl);
    assert_eq!(db.plan_cache_stats(), before);
}

#[test]
fn cypher_access_control_rejects_zero_policy_epoch_before_plan_cache() {
    let db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::AccessControl, true),
        ..DatabaseConfig::default()
    });
    let before = db.plan_cache_stats();

    let error = db
        .explain_query_with_params_access_control(
            "MATCH (m:Memory) RETURN m.id AS id",
            &BTreeMap::new(),
            QueryAccessControlContext::visibility_scope(0, "space_id", "allowed"),
        )
        .unwrap_err();

    if cfg!(feature = "acl") {
        assert!(error
            .to_string()
            .contains("access control policy epoch must be non-zero"));
    } else {
        assert_capability_error(error, RuntimeCapability::AccessControl);
    }
    assert_eq!(db.plan_cache_stats(), before);
}

#[cfg(feature = "acl")]
#[test]
fn cypher_access_control_policy_epoch_isolates_plan_cache_entries() {
    let db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::AccessControl, true),
        ..DatabaseConfig::default()
    });
    let query = "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title";
    let parameters = BTreeMap::from([("id".to_string(), Value::String("mem-1".to_string()))]);

    let epoch_7_first = db
        .explain_query_with_params_access_control(
            query,
            &parameters,
            QueryAccessControlContext::visibility_scope(7, "space_id", "allowed"),
        )
        .unwrap();
    let epoch_8_first = db
        .explain_query_with_params_access_control(
            query,
            &parameters,
            QueryAccessControlContext::visibility_scope(8, "space_id", "allowed"),
        )
        .unwrap();
    let epoch_7_second = db
        .explain_query_with_params_access_control(
            query,
            &parameters,
            QueryAccessControlContext::visibility_scope(7, "space_id", "allowed"),
        )
        .unwrap();

    assert_eq!(epoch_7_first.plan_cache_lookup, PlanCacheLookup::Miss);
    assert_eq!(epoch_8_first.plan_cache_lookup, PlanCacheLookup::Miss);
    assert_eq!(epoch_7_second.plan_cache_lookup, PlanCacheLookup::Hit);
    assert!(epoch_7_second
        .trace
        .decisions
        .iter()
        .any(|decision| decision == "access control policy epoch 7 bound to plan cache key"));
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 2);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.hits, 1);
}

#[cfg(feature = "acl")]
#[test]
fn cypher_access_control_rebinds_scope_values_on_plan_cache_hit() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::AccessControl, true),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'team-a', space_id: 'team-a'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'team-b', space_id: 'team-b'})")
        .unwrap();
    let query = "MATCH (m:Memory) WHERE m.id = $id RETURN m.id AS id";
    let team_a_parameters =
        BTreeMap::from([("id".to_string(), Value::String("team-a".to_string()))]);
    let team_b_parameters =
        BTreeMap::from([("id".to_string(), Value::String("team-b".to_string()))]);

    let team_a = db
        .explain_analyze_query_with_params_access_control(
            query,
            &team_a_parameters,
            QueryAccessControlContext::visibility_scope(7, "space_id", "team-a"),
        )
        .unwrap();
    let team_b = db
        .explain_analyze_query_with_params_access_control(
            query,
            &team_b_parameters,
            QueryAccessControlContext::visibility_scope(7, "space_id", "team-b"),
        )
        .unwrap();

    assert_eq!(team_a.plan_cache_lookup, PlanCacheLookup::Miss);
    assert_eq!(team_b.plan_cache_lookup, PlanCacheLookup::Hit);
    assert!(team_b
        .trace
        .decisions
        .iter()
        .any(|decision| { decision == "access control scope values bound for this execution" }));
    assert_eq!(team_a.output.rows.len(), 1);
    assert_eq!(
        team_a.output.rows[0].get("id"),
        Some(&Value::String("team-a".to_string()))
    );
    assert_eq!(team_b.output.rows.len(), 1);
    assert_eq!(
        team_b.output.rows[0].get("id"),
        Some(&Value::String("team-b".to_string()))
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}

#[cfg(feature = "acl")]
#[test]
fn cypher_access_control_visibility_shape_isolates_plan_cache_entries() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::AccessControl, true),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'team-a', space_id: 'team-a', visibility_class: 'public'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'team-b', space_id: 'team-b', visibility_class: 'private'})")
        .unwrap();
    let query = "MATCH (m:Memory) RETURN m.id AS id ORDER BY id";

    let by_space = db
        .explain_analyze_query_with_params_access_control(
            query,
            &BTreeMap::new(),
            QueryAccessControlContext::visibility_scope(7, "space_id", "team-a"),
        )
        .unwrap();
    let by_class = db
        .explain_analyze_query_with_params_access_control(
            query,
            &BTreeMap::new(),
            QueryAccessControlContext::visibility_scope(7, "visibility_class", "private"),
        )
        .unwrap();

    assert_eq!(by_space.plan_cache_lookup, PlanCacheLookup::Miss);
    assert_eq!(by_class.plan_cache_lookup, PlanCacheLookup::Miss);
    assert_eq!(by_class.output.rows.len(), 1);
    assert_eq!(
        by_class.output.rows[0].get("id"),
        Some(&Value::String("team-b".to_string()))
    );
    assert_eq!(db.plan_cache_stats().entries, 2);
}

#[cfg(feature = "acl")]
#[test]
fn cypher_access_control_readiness_fails_closed_for_missing_and_stale_policy() {
    let db = Database::new_with_config(DatabaseConfig {
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::AccessControl, true),
        ..DatabaseConfig::default()
    });

    let missing = db.access_control_policy_readiness(10, None);
    assert!(!missing.ready);
    assert!(!missing.stale_policy_state);
    assert_eq!(missing.observed_policy_epoch, None);
    assert!(missing
        .blocker_codes
        .contains(&"access_control_policy_missing".to_string()));

    let stale_policy = QueryAccessControlContext::visibility_scope(9, "space_id", "allowed");
    let stale = db.access_control_policy_readiness(10, Some(&stale_policy));
    assert!(!stale.ready);
    assert!(stale.stale_policy_state);
    assert_eq!(stale.observed_policy_epoch, Some(9));
    assert!(stale
        .blocker_codes
        .contains(&"access_control_policy_stale".to_string()));

    let current_policy = QueryAccessControlContext::visibility_scope(10, "space_id", "allowed");
    let current = db.access_control_policy_readiness(10, Some(&current_policy));
    assert!(current.ready);
    assert!(current.blocker_codes.is_empty());
    assert_eq!(current.observed_policy_epoch, Some(10));
}

#[test]
fn cypher_access_control_readiness_reports_disabled_capability_without_policy_inputs() {
    let db = Database::new_with_config(DatabaseConfig {
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::AccessControl, false),
        ..DatabaseConfig::default()
    });
    let policy = QueryAccessControlContext::visibility_scope(10, "secret_space_id", "secret_space");

    let readiness = db.access_control_policy_readiness(10, Some(&policy));

    assert!(!readiness.ready);
    assert!(!readiness.access_control_capability_enabled);
    assert_eq!(readiness.required_policy_epoch, 10);
    assert_eq!(readiness.observed_policy_epoch, Some(10));
    assert!(readiness
        .blocker_codes
        .contains(&"access_control_capability_disabled".to_string()));
    assert!(!readiness
        .blocker_codes
        .iter()
        .any(|code| code.contains("secret")));
}

#[cfg(feature = "acl")]
#[test]
fn cypher_access_control_filters_node_scans_before_payload_projection() {
    let mut db = Database::new_with_config(DatabaseConfig {
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::AccessControl, true),
        ..DatabaseConfig::default()
    });
    db.query("CREATE INDEX ON :Memory(space_id)").unwrap();
    db.query("CREATE (:Memory {id: 'allowed', title: 'Allowed', space_id: 'allowed'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'denied', title: 'Denied', space_id: 'denied'})")
        .unwrap();

    let output = db
        .explain_analyze_query_with_params_access_control(
            "MATCH (m:Memory) RETURN m.id AS id ORDER BY id",
            &BTreeMap::new(),
            QueryAccessControlContext::visibility_scope(7, "space_id", "allowed"),
        )
        .unwrap();

    assert_eq!(output.output.rows.len(), 1);
    assert_eq!(
        output.output.rows[0].get("id"),
        Some(&Value::String("allowed".to_string()))
    );
    let plan = output.physical_plan.explain(0);
    assert!(plan.contains("NodeProjectionScanExec"), "plan: {plan}");
    assert!(
        plan.contains("properties=[\"id\", \"space_id\"]"),
        "plan: {plan}"
    );
    assert!(plan.contains("predicate=Some(PropertyEq"), "plan: {plan}");
    assert!(!plan.contains("FilterExec"), "plan: {plan}");
    assert!(output
        .trace
        .decisions
        .iter()
        .any(|decision| decision == "access control policy epoch 7 bound to plan cache key"));
    assert!(output
        .execution_profile
        .scan_pruning_reports
        .iter()
        .any(|report| report.strategy
            == crate::store::ScanPruningStrategy::PropertyEq {
                property: "space_id".to_string()
            }));
}

#[cfg(feature = "acl")]
#[test]
fn cypher_access_control_filters_adjacency_targets_before_projection() {
    let mut db = Database::new_with_config(DatabaseConfig {
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::AccessControl, true),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'seed', title: 'Seed', space_id: 'allowed'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'visible', name: 'Visible', space_id: 'allowed'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'hidden', name: 'Hidden', space_id: 'denied'})")
        .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'seed'}), (e:Entity {id: 'visible'}) CREATE (m)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'seed'}), (e:Entity {id: 'hidden'}) CREATE (m)-[:MENTIONS]->(e)",
    )
    .unwrap();

    let output = db
        .explain_analyze_query_with_params_access_control(
            "MATCH (m:Memory {id: 'seed'})-[:MENTIONS]->(e:Entity) RETURN e.id AS id ORDER BY id",
            &BTreeMap::new(),
            QueryAccessControlContext::visibility_scope(7, "space_id", "allowed"),
        )
        .unwrap();

    assert_eq!(output.output.rows.len(), 1);
    assert_eq!(
        output.output.rows[0].get("id"),
        Some(&Value::String("visible".to_string()))
    );
    let physical_plan = output.physical_plan.explain(0);
    assert!(physical_plan.contains("AdjacencyExpandExec"));
    assert!(physical_plan.contains("FilterExec"));
    assert!(output
        .trace
        .decisions
        .iter()
        .any(|decision| decision == "access control policy epoch 7 bound to plan cache key"));
}

#[cfg(feature = "acl")]
#[test]
fn cypher_access_control_filters_shortest_path_nodes_before_path_projection() {
    let mut db = Database::new_with_config(DatabaseConfig {
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::AccessControl, true),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Entity {id: 'source', name: 'Source', space_id: 'allowed'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'hidden', name: 'Hidden', space_id: 'denied'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'target', name: 'Target', space_id: 'allowed'})")
        .unwrap();
    db.query("MATCH (source:Entity {id: 'source'}), (hidden:Entity {id: 'hidden'}) CREATE (source)-[:LINKS]->(hidden)")
        .unwrap();
    db.query("MATCH (hidden:Entity {id: 'hidden'}), (target:Entity {id: 'target'}) CREATE (hidden)-[:LINKS]->(target)")
        .unwrap();

    let parameters = BTreeMap::from([
        ("from_id".to_string(), Value::String("source".to_string())),
        ("to_id".to_string(), Value::String("target".to_string())),
    ]);
    let output = db
        .explain_analyze_query_with_params_access_control(
            "MATCH p = (a:Entity)-[:LINKS* ALL SHORTEST 1..3]-(b:Entity) WHERE a.id = $from_id AND b.id = $to_id RETURN properties(nodes(p), 'id') AS node_ids, length(p) AS hops",
            &parameters,
            QueryAccessControlContext::visibility_scope(7, "space_id", "allowed"),
        )
        .unwrap();

    assert!(output.output.rows.is_empty());
    assert!(output.physical_plan.explain(0).contains("ShortestPathExec"));
    assert!(output
        .physical_plan
        .instance_fingerprint()
        .contains("source_visibility="));
    assert!(output
        .physical_plan
        .instance_fingerprint()
        .contains("target_visibility="));
}

#[cfg(feature = "acl")]
#[test]
fn cypher_access_control_filters_graph_algorithm_nodes_before_result_projection() {
    let mut db = Database::new_with_config(DatabaseConfig {
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::AccessControl, true)
            .with(RuntimeCapability::GraphAnalytics, true),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Entity {id: 'source', name: 'Source', space_id: 'allowed'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'hidden', name: 'Hidden', space_id: 'denied'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'target', name: 'Target', space_id: 'allowed'})")
        .unwrap();
    db.query("MATCH (source:Entity {id: 'source'}), (hidden:Entity {id: 'hidden'}) CREATE (source)-[:LINKS]->(hidden)")
        .unwrap();
    db.query("MATCH (hidden:Entity {id: 'hidden'}), (target:Entity {id: 'target'}) CREATE (hidden)-[:LINKS]->(target)")
        .unwrap();
    let hidden_node = db
        .query("MATCH (hidden:Entity {id: 'hidden'}) RETURN id(hidden) AS node")
        .unwrap()
        .rows[0]
        .get("node")
        .cloned()
        .unwrap();
    db.query("CALL project_graph('EntityGraph', ['Entity'], ['LINKS'])")
        .unwrap();

    let output = db
        .explain_analyze_query_with_params_access_control(
            "CALL page_rank('EntityGraph') RETURN node, pagerank_score",
            &BTreeMap::new(),
            QueryAccessControlContext::visibility_scope(7, "space_id", "allowed"),
        )
        .unwrap();

    assert_eq!(output.output.rows.len(), 2);
    assert!(output
        .output
        .rows
        .iter()
        .all(|row| row.get("node") != Some(&hidden_node)));
    assert!(output.physical_plan.explain(0).contains("GraphAlgorithm"));
    assert!(output
        .physical_plan
        .instance_fingerprint()
        .contains("node_visibility="));
}

#[cfg(feature = "acl")]
#[test]
fn enabled_access_control_filters_before_ranking_without_exposing_policy_inputs() {
    let mut search = SearchIndex::in_memory();
    search.set_runtime_capabilities(
        RuntimeCapabilities::default().with(RuntimeCapability::AccessControl, true),
    );
    search
        .upsert(SearchDocument {
            id: "doc-1".to_string(),
            title: "ACL allowed".to_string(),
            content: "visibility scoped".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("space_id".to_string(), "allowed".to_string())]),
        })
        .unwrap();
    search
        .upsert(SearchDocument {
            id: "doc-2".to_string(),
            title: "ACL denied".to_string(),
            content: "visibility scoped".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("space_id".to_string(), "denied".to_string())]),
        })
        .unwrap();

    let result = search
        .try_search_with_options_access_control(
            "visibility",
            None,
            SearchMode::Text,
            crate::search::SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
            crate::search::SearchAccessControlContext::visibility_scopes(
                7,
                "space_id",
                ["allowed"],
            ),
        )
        .unwrap();

    assert_eq!(result.total_hits, 1);
    assert_eq!(result.hits[0].id, "doc-1");
    assert_eq!(result.candidate_set.policy_epoch, Some(7));
    assert_eq!(result.candidate_set.filtered_out_count, 1);
    assert!(result.candidate_set.metadata_filters.is_empty());
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .pushed_predicate_count,
        1
    );
}

#[test]
fn runtime_capabilities_cannot_exceed_compiled_availability() {
    for mask in 0_u8..32 {
        let requested = RuntimeCapabilities {
            access_control: mask & 1 != 0,
            full_text_search: mask & 2 != 0,
            vector_search: mask & 4 != 0,
            graph_analytics: mask & 8 != 0,
            background_maintenance: mask & 16 != 0,
        };
        let db = Database::new_with_config(DatabaseConfig {
            runtime_capabilities: requested,
            ..DatabaseConfig::default()
        });
        let mut search = SearchIndex::in_memory();
        search.set_runtime_capabilities(requested);
        let expected = requested.intersection(crate::compiled_runtime_capabilities());

        assert_eq!(db.runtime_capabilities(), expected, "database mask {mask}");
        assert_eq!(
            search.runtime_capabilities(),
            expected,
            "search mask {mask}"
        );
    }
}

#[cfg(not(any(
    feature = "background-maintenance",
    feature = "full-text-search",
    feature = "graph-analytics",
    feature = "vector-search"
)))]
#[test]
fn minimal_build_fails_closed_for_every_optional_capability() {
    let mut db = Database::new_with_config(DatabaseConfig {
        runtime_capabilities: RuntimeCapabilities::shared_host(),
        ..DatabaseConfig::default()
    });

    assert_capability_error(
        db.query("CREATE FULLTEXT INDEX ON :Memory(title)")
            .unwrap_err(),
        RuntimeCapability::FullTextSearch,
    );
    assert_capability_error(
        db.query("CALL vector_search($embedding, topK := 1) RETURN id, score")
            .unwrap_err(),
        RuntimeCapability::VectorSearch,
    );
    assert_capability_error(
        db.query("CALL project_graph('EntityGraph', ['Entity'], ['MENTIONS'])")
            .unwrap_err(),
        RuntimeCapability::GraphAnalytics,
    );
    assert_capability_error(
        db.run_background_schema_maintenance(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            0,
        )
        .unwrap_err(),
        RuntimeCapability::BackgroundMaintenance,
    );
}

fn assert_capability_error(error: SkeinError, expected: RuntimeCapability) {
    assert_eq!(
        error,
        SkeinError::CapabilityUnavailable {
            capability: expected
        }
    );
}
