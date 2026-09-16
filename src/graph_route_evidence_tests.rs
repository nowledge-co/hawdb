#[cfg(test)]
mod tests {
    use super::{
        nowledge_graph_route_evidence_json, nowledge_mem_graph_augmentation_state_route_query,
        nowledge_mem_graph_community_members_route_query,
        nowledge_mem_graph_community_recent_memories_route_query,
        nowledge_mem_graph_community_subgraph_route_query,
        nowledge_mem_graph_node_details_route_query, nowledge_mem_graph_orphans_route_query,
        nowledge_mem_graph_overview_route_query, nowledge_mem_graph_pagerank_plan_route_query,
        nowledge_mem_graph_sample_route_query, parse_route_parity_evidence,
        parse_route_query_inventory, query_requirement_blockers, RouteCypherQuery,
    };
    use crate::{
        nowledge_graph_route_readiness_json, Database, NowledgeMemGraph, NowledgeMemGraphMode,
        NowledgeMemQueryReportOptions, Value, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
    };
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn route_evidence_runs_queries_through_nowledge_runtime() {
        let mut db = Database::new();
        db.query("CREATE INDEX ON :Memory(id)").unwrap();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'mem-other', title: 'Other Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": [
                {
                    "route": "/graph/overview",
                    "shadow_compare_ready": true,
                    "primary_ready": true,
                    "queries": [
                        {
                            "name": "overview-memory-lookup",
                            "query_family": "memory_lookup",
                            "cypher": "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
                            "parameters": {
                                "id": "mem-route"
                            },
                            "require_scan_pruning": true,
                            "require_pruned": true
                        }
                    ],
                    "blocker_codes": []
                }
            ]
        }))
        .unwrap();

        let route_parity = ready_route_parity();
        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["protocol"], "nmem-graph-route-evidence-v1");
        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["required_routes_covered"], false);
        assert_eq!(
            evidence["covered_routes"],
            serde_json::json!(["/graph/overview"])
        );
        assert!(evidence["missing_required_routes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|route| route == "/graph/explore"));
        assert_eq!(evidence["routes"][0]["route"], "/graph/overview");
        assert_eq!(evidence["routes"][0]["shadow_compare_ready"], true);
        assert_eq!(
            evidence["routes"][0]["shadow_compare_evidence_source"],
            "route_parity_evidence"
        );
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["query_reports"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["protocol"],
            "skein-nowledge-mem-query-report-v1"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "overview-memory-lookup"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "memory_lookup"
        );
        assert_eq!(evidence["routes"][0]["query_reports"][0]["query_index"], 0);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["require_scan_pruning"],
            true
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["require_pruned"],
            true
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["statement_kind"],
            "match_return"
        );
        assert_eq!(
            evidence["routes"][0]["required_query_families"],
            serde_json::json!(["memory_lookup"])
        );
    }

    #[test]
    fn graph_route_query_json_parse_errors_are_redacted_by_default() {
        let root = unique_test_dir("graph_route_secret_path_do_not_emit");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("secret-route-inventory-path-do-not-emit.json");
        std::fs::write(
            &path,
            "{ \"cypher\": \"MATCH (m {id: 'secret-route-query-do-not-emit'})\", \"unterminated\": ",
        )
        .unwrap();

        let error = super::run_nowledge_graph_route_evidence(
            ["unused-graph.db", path.to_str().unwrap()]
                .into_iter()
                .map(str::to_string),
        )
        .unwrap_err()
        .to_string();

        assert_eq!(
            error,
            "semantic error: failed to parse graph route query JSON: invalid_json"
        );
        assert!(!error.contains("secret-route-inventory-path-do-not-emit"));
        assert!(!error.contains("secret-route-query-do-not-emit"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn overview_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'overview-memory-1', title: 'Overview One', content: 'body one', pagerank_score: 3.0, importance: 0.1})")
            .unwrap();
        db.query(
            "CREATE (:Memory {id: 'overview-memory-2', content: 'Fallback body', importance: 2.0})",
        )
        .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = vec![nowledge_mem_graph_overview_route_query(2).unwrap()];
        let route_parity = ready_route_parity_for(&["/graph/overview"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(evidence["routes"][0]["route"], "/graph/overview");
        assert_eq!(evidence["routes"][0]["primary_read_routing_enabled"], true);
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "overview-memory-ranking"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "memory_lookup"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["physical_plan_captured"],
            true
        );
        assert!(
            evidence["routes"][0]["query_reports"][0]["scan_pruning_report_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
        assert_eq!(
            readiness["routes"][0]["query_reports"][0]["scan_pruning_reports_present"],
            true
        );
    }

    #[test]
    fn sample_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'sample-route-a', title: 'Sample Route A', importance: 1.0})",
        )
        .unwrap();
        db.query(
            "CREATE (:Memory {id: 'sample-route-b', title: 'Sample Route B', importance: 2.0})",
        )
        .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = vec![nowledge_mem_graph_sample_route_query(2).unwrap()];
        let route_parity = ready_route_parity_for(&["/graph/sample"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(evidence["routes"][0]["route"], "/graph/sample");
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "sample-memory-list"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "memory_lookup"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["require_scan_pruning"],
            false
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
    }

    #[test]
    fn node_details_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'detail-memory-route', title: 'Detail Route', content: 'detail body'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let node_id = memory_node_id(&mut graph, "detail-memory-route");
        let route_queries = vec![nowledge_mem_graph_node_details_route_query(node_id).unwrap()];
        let route_parity = ready_route_parity_for(&["/graph/node-details/{node_id}"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(
            evidence["routes"][0]["route"],
            "/graph/node-details/{node_id}"
        );
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "node-details-memory-lookup"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "memory_lookup"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["require_pruned"],
            false
        );
        assert!(
            evidence["routes"][0]["query_reports"][0]["scan_pruning_report_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
        assert_eq!(
            readiness["routes"][0]["query_reports"][0]["scan_pruning_reports_present"],
            true
        );
    }

    #[test]
    fn community_members_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'community-route-high', title: 'Community Route High', pagerank_score: 2.0, community_id: 42})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'community-route-other', title: 'Community Route Other', pagerank_score: 9.0, community_id: 7})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = vec![nowledge_mem_graph_community_members_route_query(42, 5).unwrap()];
        let route_parity = ready_route_parity_for(&["/graph/community-members/{community_id}"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(
            evidence["routes"][0]["route"],
            "/graph/community-members/{community_id}"
        );
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "community-members-memory-lookup"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "memory_lookup"
        );
        assert!(
            evidence["routes"][0]["query_reports"][0]["scan_pruning_report_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
        assert_eq!(
            readiness["routes"][0]["query_reports"][0]["scan_pruning_reports_present"],
            true
        );
    }

    #[test]
    fn community_recent_memories_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'community-recent-route-memory', title: 'Community Recent Route', content: 'recent route body', importance: 0.9, created_at: 1700000001, updated_at: 1700000002, is_crystal: false})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'community-recent-route-entity', community_id: 3676})")
            .unwrap();
        db.query("MATCH (m:Memory {id: 'community-recent-route-memory'}), (e:Entity {id: 'community-recent-route-entity'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries =
            vec![nowledge_mem_graph_community_recent_memories_route_query(3676, 5).unwrap()];
        let route_parity =
            ready_route_parity_for(&["/library/community/{community_id}/recent-memories"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(
            evidence["routes"][0]["route"],
            "/library/community/{community_id}/recent-memories"
        );
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "community-recent-memories-lookup"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "memory_lookup"
        );
        assert!(
            evidence["routes"][0]["query_reports"][0]["scan_pruning_report_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
        assert_eq!(
            readiness["routes"][0]["query_reports"][0]["scan_pruning_reports_present"],
            true
        );
    }

    #[test]
    fn community_subgraph_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Entity {id: 'community-subgraph-route-alpha', name: 'Alpha Route', entity_type: 'concept', community_id: 3505, confidence: 0.9})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'community-subgraph-route-beta', name: 'Beta Route', entity_type: 'concept', community_id: 3505, confidence: 0.7})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'community-subgraph-route-memory'})")
            .unwrap();
        db.query("MATCH (m:Memory {id: 'community-subgraph-route-memory'}), (e:Entity {id: 'community-subgraph-route-alpha'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
        db.query("MATCH (a:Entity {id: 'community-subgraph-route-alpha'}), (b:Entity {id: 'community-subgraph-route-beta'}) CREATE (a)-[:RELATES_TO {confidence: 0.77, relation_type: 'related'}]->(b)")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = vec![nowledge_mem_graph_community_subgraph_route_query(
            3505,
            5,
            [
                "community-subgraph-route-alpha",
                "community-subgraph-route-beta",
            ],
            10,
        )
        .unwrap()];
        let route_parity = ready_route_parity_for(&["/library/community/{community_id}/subgraph"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(
            evidence["routes"][0]["route"],
            "/library/community/{community_id}/subgraph"
        );
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["required_query_families"],
            serde_json::json!(["graph_traversal"])
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "community-subgraph-entity-ranking"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][1]["query_name"],
            "community-subgraph-relation-edges"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "graph_traversal"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][1]["query_family"],
            "graph_traversal"
        );
        assert!(
            evidence["routes"][0]["query_reports"][0]["scan_pruning_report_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
    }

    #[test]
    fn augmentation_state_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:GraphMeta {meta_id: 'main', community_detection_applied: true, pagerank_applied: true, community_algorithm: 'louvain', community_resolution: 1.0, community_count: 12, pagerank_algorithm: 'pagerank', pagerank_damping: 0.85, pagerank_iterations: 20, last_augmentation_at: 1000, schema_version: 2, community_detection_computed_at: 900, pagerank_computed_at: 950})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = vec![nowledge_mem_graph_augmentation_state_route_query()];
        let route_parity = ready_route_parity_for(&["/graph/augmentation/state"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(evidence["routes"][0]["route"], "/graph/augmentation/state");
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["required_query_families"],
            serde_json::json!(["projected_graph"])
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "augmentation-state-graph-meta"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "projected_graph"
        );
        assert!(
            evidence["routes"][0]["query_reports"][0]["scan_pruning_report_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
    }

    #[test]
    fn pagerank_plan_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: true, pagerank_computed_at: 404})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'pagerank-route-m1', created_at: 10, updated_at: 20})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'pagerank-route-m2', created_at: 120, updated_at: 130})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'pagerank-route-e1', created_at: 15, updated_at: 25})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'pagerank-route-e2', created_at: 140, updated_at: 150})")
            .unwrap();
        db.query("MATCH (m:Memory {id: 'pagerank-route-m1'}), (e:Entity {id: 'pagerank-route-e1'}) CREATE (m)-[:MENTIONS {created_at: 30}]->(e)")
            .unwrap();
        db.query("MATCH (a:Entity {id: 'pagerank-route-e1'}), (b:Entity {id: 'pagerank-route-e2'}) CREATE (a)-[:RELATES_TO {created_at: 170}]->(b)")
            .unwrap();
        db.query("MATCH (a:Memory {id: 'pagerank-route-m1'}), (b:Memory {id: 'pagerank-route-m2'}) CREATE (a)-[:MEMORY_RELATES_TO {status: 'active', created_at: 180}]->(b)")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = vec![nowledge_mem_graph_pagerank_plan_route_query(Some(100))];
        let route_parity = ready_route_parity_for(&["/graph/augmentation/pagerank/plan"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(
            evidence["routes"][0]["route"],
            "/graph/augmentation/pagerank/plan"
        );
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["required_query_families"],
            serde_json::json!(["projected_graph"])
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "pagerank-plan-graph-meta"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "projected_graph"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"]
                .as_array()
                .unwrap()
                .len(),
            11
        );
        assert!(
            evidence["routes"][0]["query_reports"][0]["scan_pruning_report_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
    }

    #[test]
    fn orphans_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Entity {id: 'orphan-route-entity', name: 'Orphan Route Entity', entity_type: 'concept'})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'mentioned-route-entity', name: 'Mentioned Route Entity', entity_type: 'concept'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'orphan-route-memory', title: 'Blocking Memory'})")
            .unwrap();
        db.query("MATCH (m:Memory {id: 'orphan-route-memory'}), (e:Entity {id: 'mentioned-route-entity'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = vec![nowledge_mem_graph_orphans_route_query(10).unwrap()];
        let route_parity = ready_route_parity_for(&["/graph/orphans"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(evidence["routes"][0]["route"], "/graph/orphans");
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "orphan-entity-relationship-exclusion"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "graph_traversal"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["require_scan_pruning"],
            false
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
    }

    #[test]
    fn route_evidence_requires_complete_required_route_coverage() {
        let mut db = Database::new();
        db.query("CREATE INDEX ON :Memory(id)").unwrap();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'mem-other', title: 'Other Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
                .iter()
                .map(|route| ready_route_query(route))
                .collect::<Vec<_>>()
        }))
        .unwrap();

        let route_parity = ready_route_parity_for(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES);
        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["ready"], true);
        assert_eq!(
            evidence["required_route_count"],
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len())
        );
        assert_eq!(
            evidence["covered_route_count"],
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len())
        );
        assert_eq!(
            evidence["covered_routes"],
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES)
        );
        assert_eq!(evidence["missing_required_routes"], serde_json::json!([]));
        assert_eq!(evidence["required_routes_covered"], true);
        assert_eq!(evidence["unknown_routes"], serde_json::json!([]));
        assert_eq!(evidence["duplicate_routes"], serde_json::json!([]));
        assert_eq!(evidence["route_coverage_ready"], true);
        assert_eq!(
            evidence["route_coverage_blocker_codes"],
            serde_json::json!([])
        );
    }

    #[test]
    fn route_evidence_fails_closed_on_unknown_routes() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'mem-other', title: 'Other Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| ready_route_query(route))
            .collect::<Vec<_>>();
        routes.push(ready_route_query("/graph/manual-extra-route"));
        let route_queries =
            parse_route_query_inventory(&serde_json::json!({ "routes": routes })).unwrap();
        let mut parity_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.to_vec();
        parity_routes.push("/graph/manual-extra-route");
        let route_parity = ready_route_parity_for(&parity_routes);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["required_routes_covered"], true);
        assert_eq!(evidence["route_coverage_ready"], false);
        assert_eq!(
            evidence["unknown_routes"],
            serde_json::json!(["/graph/manual-extra-route"])
        );
        assert_eq!(
            evidence["route_coverage_blocker_codes"],
            serde_json::json!(["unknown_routes"])
        );
    }

    #[test]
    fn route_evidence_fails_closed_on_duplicate_routes() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'mem-other', title: 'Other Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| ready_route_query(route))
            .collect::<Vec<_>>();
        routes.push(ready_route_query("/graph/overview"));
        let route_queries =
            parse_route_query_inventory(&serde_json::json!({ "routes": routes })).unwrap();
        let route_parity = ready_route_parity_for(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["required_routes_covered"], true);
        assert_eq!(evidence["route_coverage_ready"], false);
        assert_eq!(
            evidence["duplicate_routes"],
            serde_json::json!(["/graph/overview"])
        );
        assert_eq!(
            evidence["route_coverage_blocker_codes"],
            serde_json::json!(["duplicate_routes"])
        );
    }

    #[test]
    fn route_evidence_promotes_primary_routing_after_query_runtime_success() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": [
                {
                    "route": "/graph/overview",
                    "shadow_compare_ready": true,
                    "primary_read_routing_enabled": true,
                    "primary_ready": false,
                    "queries": [
                        {
                            "query_family": "memory_lookup",
                            "cypher": "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
                            "parameters": {
                                "id": "mem-route"
                            }
                        }
                    ],
                    "blocker_codes": ["graph_route_execution_evidence_missing"]
                }
            ]
        }))
        .unwrap();

        let route_parity = ready_route_parity();
        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["routes"][0]["primary_read_routing_enabled"], true);
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert!(!evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "graph_route_execution_evidence_missing"));
    }

    #[test]
    fn route_evidence_fails_closed_when_query_family_is_missing() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": [
                {
                    "route": "/graph/overview",
                    "shadow_compare_ready": true,
                    "primary_ready": true,
                    "queries": [
                        {
                            "name": "overview-unclassified-smoke",
                            "cypher": "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
                            "parameters": {
                                "id": "mem-route"
                            }
                        }
                    ],
                    "blocker_codes": []
                }
            ]
        }))
        .unwrap();

        let route_parity = ready_route_parity();
        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["routes"][0]["primary_ready"], false);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            serde_json::Value::Null
        );
        assert!(evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "query_family_missing"));
        assert!(evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "route_required_query_family_missing"));
    }

    #[test]
    fn route_evidence_fails_closed_when_route_family_does_not_match() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": [
                {
                    "route": "/graph/shortest-path",
                    "shadow_compare_ready": true,
                    "primary_ready": true,
                    "queries": [
                        {
                            "name": "shortest-path-smoke",
                            "query_family": "memory_lookup",
                            "cypher": "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
                            "parameters": {
                                "id": "mem-route"
                            }
                        }
                    ],
                    "blocker_codes": []
                }
            ]
        }))
        .unwrap();

        let route_parity = ready_route_parity_for(&["/graph/shortest-path"]);
        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["routes"][0]["primary_ready"], false);
        assert_eq!(
            evidence["routes"][0]["required_query_families"],
            serde_json::json!(["graph_traversal"])
        );
        assert!(evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "route_required_query_family_missing"));
    }

    #[test]
    fn route_evidence_fails_closed_on_query_execution_error() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": [
                {
                    "route": "/graph/overview",
                    "shadow_compare_ready": true,
                    "primary_ready": true,
                    "queries": [
                        {
                            "name": "broken-overview-query",
                            "query_family": "memory_lookup",
                            "cypher": "MATCH (m:Memory) RETURN unknown.property AS value"
                        }
                    ]
                }
            ]
        }))
        .unwrap();

        let route_parity = ready_route_parity();
        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["routes"][0]["primary_ready"], false);
        assert_eq!(
            evidence["routes"][0]["query_reports"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert!(evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "query_runtime_execution_failed"));
        assert_eq!(
            evidence["routes"][0]["query_errors"][0]["error_class"],
            "semantic"
        );
        assert_eq!(
            evidence["routes"][0]["query_errors"][0]["query_name"],
            "broken-overview-query"
        );
        assert_eq!(evidence["routes"][0]["query_errors"][0]["query_index"], 0);
    }

    #[test]
    fn route_evidence_fails_closed_when_required_pruning_is_not_reduced() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": [
                {
                    "route": "/graph/overview",
                    "shadow_compare_ready": true,
                    "primary_ready": true,
                    "queries": [
                        {
                            "name": "overview-full-scan",
                            "query_family": "memory_lookup",
                            "cypher": "MATCH (m:Memory) RETURN m.title AS title",
                            "require_pruned": true
                        }
                    ],
                    "blocker_codes": []
                }
            ]
        }))
        .unwrap();

        let route_parity = ready_route_parity();
        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["routes"][0]["primary_ready"], false);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "overview-full-scan"
        );
        assert!(evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "query_pruned_scan_required_but_missing"));
    }

    #[test]
    fn route_query_requirements_reject_malformed_scan_pruning_evidence() {
        let query = RouteCypherQuery {
            name: "malformed-pruning".to_string(),
            query_family: Some("memory_lookup".to_string()),
            require_scan_pruning: true,
            require_pruned: false,
            cypher: "MATCH (m:Memory {id: $id}) RETURN m.title AS title".to_string(),
            parameters: BTreeMap::new(),
        };
        let report = serde_json::json!({
            "scan_pruning_report_count": 2,
            "scan_pruning_reports": [
                {
                    "strategy": { "kind": "id_eq" },
                    "pruned": true,
                    "pruned_candidate_count": 1
                },
                {
                    "strategy": {},
                    "pruned": false,
                    "pruned_candidate_count": 0
                },
                {
                    "pruned": false,
                    "pruned_candidate_count": 0
                }
            ]
        });

        assert_eq!(
            query_requirement_blockers(&query, &report),
            vec![
                "query_scan_pruning_report_count_mismatch".to_string(),
                "query_scan_pruning_strategy_missing".to_string(),
            ]
        );
    }

    #[test]
    fn route_query_requirements_reject_unknown_query_family() {
        let query = RouteCypherQuery {
            name: "unknown-family".to_string(),
            query_family: Some("manual_smoke".to_string()),
            require_scan_pruning: false,
            require_pruned: false,
            cypher: "MATCH (m:Memory {id: $id}) RETURN m.title AS title".to_string(),
            parameters: BTreeMap::new(),
        };

        assert_eq!(
            query_requirement_blockers(&query, &serde_json::json!({})),
            vec!["query_unknown_query_family".to_string()]
        );
    }

    #[test]
    fn route_evidence_fails_closed_without_route_parity_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": [
                {
                    "route": "/graph/overview",
                    "shadow_compare_ready": true,
                    "primary_ready": true,
                    "queries": [
                        {
                            "query_family": "memory_lookup",
                            "cypher": "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
                            "parameters": {
                                "id": "mem-route"
                            }
                        }
                    ],
                    "blocker_codes": []
                }
            ]
        }))
        .unwrap();

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            None,
        );

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["routes"][0]["primary_ready"], false);
        assert_eq!(
            evidence["routes"][0]["shadow_compare_evidence_source"],
            "route_query_inventory"
        );
        assert!(evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "shadow_compare_evidence_missing"));
    }

    #[test]
    fn route_evidence_recomputes_route_parity_identity() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": [ready_route_query("/graph/overview")]
        }))
        .unwrap();
        let route_parity = parse_route_parity_evidence(&serde_json::json!({
            "protocol": "nmem-graph-route-parity-evidence-v1",
            "routes": [
                {
                    "route": "/graph/overview",
                    "ready": true,
                    "matched_per_million": 999999,
                    "primary_engine": "skein",
                    "shadow_engine": "kuzu"
                }
            ]
        }))
        .unwrap();

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["routes"][0]["shadow_compare_ready"], false);
        assert_eq!(evidence["routes"][0]["primary_ready"], false);
        assert_eq!(
            evidence["routes"][0]["shadow_compare"]["blocker_codes"],
            serde_json::json!([
                "route_parity_matched_per_million_not_full",
                "route_parity_primary_engine_mismatch",
                "route_parity_shadow_engine_mismatch"
            ])
        );
        assert!(evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "route_parity_primary_engine_mismatch"));
    }

    fn ready_route_parity() -> super::RouteParityEvidence {
        ready_route_parity_for(&["/graph/overview"])
    }

    fn ready_route_query(route: &str) -> serde_json::Value {
        let query_family = crate::nowledge_mem_required_query_families_for_route(route)
            .first()
            .copied()
            .unwrap_or("memory_lookup");
        serde_json::json!({
            "route": route,
            "shadow_compare_ready": true,
            "primary_ready": true,
            "queries": [
                {
                    "name": format!("{}:memory-lookup", route),
                    "query_family": query_family,
                    "cypher": "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
                    "parameters": {
                        "id": "mem-route"
                    },
                    "require_scan_pruning": true,
                    "require_pruned": true
                }
            ],
            "blocker_codes": []
        })
    }

    fn memory_node_id(graph: &mut NowledgeMemGraph, memory_id: &str) -> u64 {
        let mut parameters = BTreeMap::new();
        parameters.insert("id".to_string(), Value::String(memory_id.to_string()));
        let output = graph
            .query_with_params(
                "MATCH (m:Memory {id: $id}) RETURN id(m) AS node_id",
                &parameters,
            )
            .unwrap();
        match output.rows[0].get("node_id") {
            Some(Value::Int(value)) if *value >= 0 => *value as u64,
            other => panic!("expected non-negative node_id, got {other:?}"),
        }
    }

    fn ready_route_parity_for(routes: &[&str]) -> super::RouteParityEvidence {
        parse_route_parity_evidence(&serde_json::json!({
            "protocol": "nmem-graph-route-parity-evidence-v1",
            "routes": routes
                .iter()
                .map(|route| {
                    serde_json::json!({
                        "route": route,
                        "ready": true,
                        "matched_per_million": 1000000,
                        "primary_engine": "kuzu",
                        "shadow_engine": "skein"
                    })
                })
                .collect::<Vec<_>>()
        }))
        .unwrap()
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_{name}_{}_{nanos}", std::process::id()))
    }
}
