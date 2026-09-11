use super::*;

// Freeze the pre-extraction catalog independently of the production tables.
// A deliberate catalog change must update this compatibility fixture as well.
const CATALOG: &[(&str, &str, &str, &str)] = &[
    (
        "/communities",
        "read_batch_runtime",
        "read_batch_runtime",
        "label_stats_read",
    ),
    (
        "/communities/{community_id}",
        "read_batch_runtime",
        "read_batch_runtime",
        "label_stats_read",
    ),
    (
        "/graph/overview",
        "graph_runtime",
        "graph_route_execution",
        "memory_lookup",
    ),
    (
        "/graph/sample",
        "graph_runtime",
        "graph_route_execution",
        "memory_lookup",
    ),
    (
        "/graph/search",
        "search_runtime",
        "search_candidate_shadow",
        "search_projection",
    ),
    (
        "/graph/explore",
        "graph_runtime",
        "graph_route_execution",
        "graph_traversal",
    ),
    (
        "/graph/expand/{node_id}",
        "graph_runtime",
        "graph_route_execution",
        "graph_traversal",
    ),
    (
        "/graph/live-preview",
        "graph_runtime",
        "graph_route_execution",
        "memory_lookup",
    ),
    (
        "/graph/live-preview/{node_id}",
        "graph_runtime",
        "graph_route_execution",
        "memory_lookup",
    ),
    (
        "/graph/community-members/{community_id}",
        "graph_runtime",
        "graph_route_execution",
        "memory_lookup",
    ),
    (
        "/library/community/{community_id}/subgraph",
        "graph_runtime",
        "graph_route_execution",
        "graph_traversal",
    ),
    (
        "/library/community/{community_id}/recent-memories",
        "graph_runtime",
        "graph_route_execution",
        "memory_lookup",
    ),
    (
        "/library/community/{community_id}/related",
        "graph_runtime",
        "graph_route_execution",
        "graph_traversal",
    ),
    (
        "/graph/analysis",
        "graph_runtime",
        "graph_route_execution",
        "projected_graph",
    ),
    (
        "/graph/augmentation/state",
        "graph_runtime",
        "graph_route_execution",
        "projected_graph",
    ),
    (
        "/graph/augmentation/pagerank/plan",
        "graph_runtime",
        "graph_route_execution",
        "projected_graph",
    ),
    (
        "/graph/node-details/{node_id}",
        "graph_runtime",
        "graph_route_execution",
        "memory_lookup",
    ),
    (
        "/graph/orphans",
        "graph_runtime",
        "graph_route_execution",
        "graph_traversal",
    ),
    (
        "/graph/shortest-path",
        "graph_runtime",
        "graph_route_execution",
        "graph_traversal",
    ),
    (
        "/sources/{source_id}",
        "read_batch_runtime",
        "read_batch_runtime",
        "label_stats_read",
    ),
    (
        "/stats/entity-relations",
        "read_batch_runtime",
        "read_batch_runtime",
        "label_stats_read",
    ),
    (
        "/stats/sources",
        "read_batch_runtime",
        "read_batch_runtime",
        "label_stats_read",
    ),
    (
        "/stats/top-communities",
        "read_batch_runtime",
        "read_batch_runtime",
        "label_stats_read",
    ),
    (
        "/entities",
        "read_batch_runtime",
        "read_batch_runtime",
        "label_stats_read",
    ),
    (
        "/entities/{entity_id}/relationships",
        "read_batch_runtime",
        "read_batch_runtime",
        "label_stats_read",
    ),
    (
        "/agent/evolves",
        "read_batch_runtime",
        "read_batch_runtime",
        "label_stats_read",
    ),
];
const DIGEST: &str = "fnv1a64:80816a4f519d9693";

#[test]
fn catalog_preserves_routes_roles_families_and_digest() {
    let routes: Vec<_> = CATALOG.iter().map(|entry| entry.0).collect();
    assert_eq!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES, routes);
    assert_eq!(NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS.len(), CATALOG.len());
    let expected = serde_json::Value::Array(
        CATALOG
            .iter()
            .map(|entry| {
                let spec = nowledge_mem_graph_read_route_spec(entry.0).unwrap();
                assert_eq!(spec.owner.as_str(), entry.1);
                assert_eq!(spec.required_evidence_kind.as_str(), entry.2);
                assert!(spec.stale_on_catalog_change);
                assert_eq!(
                    nowledge_mem_required_query_families_for_route(entry.0),
                    &[entry.3]
                );
                serde_json::json!({
                    "route": entry.0,
                    "owner": entry.1,
                    "required_evidence_kind": entry.2,
                    "stale_on_catalog_change": true,
                })
            })
            .collect(),
    );
    assert_eq!(nowledge_mem_graph_read_route_specs_json(), expected);
    assert_eq!(nowledge_mem_graph_read_route_catalog_digest(), DIGEST);
    assert_eq!(
        NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
        "nowledge-mem-graph-read-route-catalog-v1"
    );
    for route in [
        "",
        "/unknown",
        "/graph/overview/",
        "/GRAPH/overview",
        "/\u{65e5}\u{672c}\u{8a9e}",
        "/graph/overview\0",
    ] {
        assert!(nowledge_mem_graph_read_route_spec(route).is_none());
        assert!(nowledge_mem_required_query_families_for_route(route).is_empty());
    }
}

fn inventory(seed: usize, layout: usize) -> Vec<NowledgeMemRouteOwnership> {
    let mut routes: Vec<_> = CATALOG
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            NowledgeMemRouteOwnership::new(
                entry.0,
                if layout == 0 || (layout == 2 && (index + seed).is_multiple_of(3)) {
                    NowledgeMemRouteReadEngine::Legacy
                } else {
                    NowledgeMemRouteReadEngine::Skein
                },
            )
        })
        .collect();
    let selected = seed % routes.len();
    match layout {
        0..=2 => {}
        3 => {
            routes.remove(selected);
        }
        4 => routes.push(routes[selected].clone()),
        5 => routes.push(NowledgeMemRouteOwnership::new(
            routes[selected].route.clone(),
            NowledgeMemRouteReadEngine::Legacy,
        )),
        6 => routes.push(NowledgeMemRouteOwnership::new(
            "/unknown",
            NowledgeMemRouteReadEngine::Legacy,
        )),
        7 => routes.push(NowledgeMemRouteOwnership::new(
            "/\u{65e5}\u{672c}\u{8a9e}",
            NowledgeMemRouteReadEngine::Skein,
        )),
        8 => routes.clear(),
        9 => {
            routes.remove(selected);
            routes.push(routes[seed % routes.len()].clone());
            routes.push(NowledgeMemRouteOwnership::new(
                "/unknown",
                NowledgeMemRouteReadEngine::Skein,
            ));
            routes.push(NowledgeMemRouteOwnership::new(
                "/unknown",
                NowledgeMemRouteReadEngine::Legacy,
            ));
        }
        _ => unreachable!(),
    }
    if !routes.is_empty() {
        let shift = seed % routes.len();
        routes.rotate_left(shift);
        if seed.is_multiple_of(2) {
            routes.reverse();
        }
    }
    routes
}

fn readiness(seed: usize, state: usize) -> Option<NowledgeMemRouteReadinessSummary> {
    if state == 0 {
        return None;
    }
    let mut summary = NowledgeMemRouteReadinessSummary {
        route_primary_ready: state != 2,
        primary_ready_routes: CATALOG.iter().map(|entry| entry.0.to_string()).collect(),
        route_query_plan_evidence_ready: state != 3,
        route_query_profile_evidence_ready: state != 4,
        route_query_api_behavior_evidence_ready: state != 5,
        relationship_property_pruning_required_count: seed as u64,
        relationship_property_pruning_report_count: seed as u64 + u64::from(state == 7),
        route_relationship_property_pruning_evidence_ready: state != 6,
    };
    if state == 8 {
        summary
            .primary_ready_routes
            .truncate(seed % (CATALOG.len() + 1));
    } else if state == 9 {
        summary.primary_ready_routes.push("/unknown".into());
        summary
            .primary_ready_routes
            .push(CATALOG[seed % CATALOG.len()].0.into());
    } else if state == 10 {
        summary.relationship_property_pruning_required_count = u64::MAX;
        summary.relationship_property_pruning_report_count = u64::MAX;
    }
    Some(summary)
}

fn sorted_unique(mut names: Vec<String>) -> Vec<String> {
    names.sort();
    names.dedup();
    names
}

// Use direct occurrence scans instead of the production count/engine maps.
// The reference never calls a production catalog, readiness, or JSON helper.
fn reference(
    routes: &[NowledgeMemRouteOwnership],
    evidence: Option<&NowledgeMemRouteReadinessSummary>,
    require_all_skein: bool,
) -> serde_json::Value {
    let required: Vec<_> = CATALOG.iter().map(|entry| entry.0).collect();
    let missing: Vec<_> = required
        .iter()
        .filter(|name| !routes.iter().any(|route| route.route == **name))
        .map(|name| (*name).to_string())
        .collect();
    let observed = sorted_unique(routes.iter().map(|route| route.route.clone()).collect());
    let unknown: Vec<_> = observed
        .iter()
        .filter(|name| !required.contains(&name.as_str()))
        .cloned()
        .collect();
    let duplicates: Vec<_> = observed
        .iter()
        .filter(|name| routes.iter().filter(|route| route.route == **name).count() > 1)
        .cloned()
        .collect();
    let conflicts: Vec<_> = observed
        .iter()
        .filter(|name| {
            routes.iter().any(|route| {
                route.route == **name && route.read_engine == NowledgeMemRouteReadEngine::Legacy
            }) && routes.iter().any(|route| {
                route.route == **name && route.read_engine == NowledgeMemRouteReadEngine::Skein
            })
        })
        .cloned()
        .collect();
    let skein = sorted_unique(
        routes
            .iter()
            .filter(|route| route.read_engine == NowledgeMemRouteReadEngine::Skein)
            .map(|route| route.route.clone())
            .collect(),
    );
    let legacy = sorted_unique(
        routes
            .iter()
            .filter(|route| route.read_engine == NowledgeMemRouteReadEngine::Legacy)
            .map(|route| route.route.clone())
            .collect(),
    );
    let evidence_ready = evidence.is_some_and(|summary| {
        [
            summary.route_primary_ready,
            summary.route_query_plan_evidence_ready,
            summary.route_query_profile_evidence_ready,
            summary.route_query_api_behavior_evidence_ready,
            summary.route_relationship_property_pruning_evidence_ready,
            summary.relationship_property_pruning_required_count
                == summary.relationship_property_pruning_report_count,
        ]
        .into_iter()
        .all(|ready| ready)
    });
    let not_ready: Vec<_> = skein
        .iter()
        .filter(|name| !evidence_ready || !evidence.unwrap().primary_ready_routes.contains(name))
        .cloned()
        .collect();
    let mut blockers = Vec::new();
    for (reject, code) in [
        (
            !missing.is_empty(),
            "route_ownership_missing_required_routes",
        ),
        (!unknown.is_empty(), "route_ownership_unknown_routes"),
        (!duplicates.is_empty(), "route_ownership_duplicate_routes"),
        (!conflicts.is_empty(), "route_ownership_conflicting_routes"),
        (
            !skein.is_empty() && evidence.is_none(),
            "route_ownership_route_readiness_missing",
        ),
        (
            !not_ready.is_empty(),
            "route_ownership_skein_routes_not_ready",
        ),
        (
            require_all_skein && !legacy.is_empty(),
            "route_ownership_legacy_routes_remaining",
        ),
    ] {
        if reject {
            blockers.push(code);
        }
    }
    let ready = blockers.is_empty();
    let assignments: Vec<_> = routes
        .iter()
        .map(|route| {
            serde_json::json!({
                "route": route.route,
                "read_engine": match route.read_engine {
                    NowledgeMemRouteReadEngine::Legacy => "legacy",
                    NowledgeMemRouteReadEngine::Skein => "skein",
                },
            })
        })
        .collect();
    serde_json::json!({
        "protocol": "skein-nowledge-mem-route-ownership-v1",
        "ready": ready,
        "production_cutover_ready": ready && legacy.is_empty(),
        "require_all_skein": require_all_skein,
        "required_route_count": required.len(),
        "explicit_route_count": required.len() - missing.len(),
        "skein_route_count": skein.len(),
        "legacy_route_count": legacy.len(),
        "routes": assignments,
        "skein_routes": skein,
        "legacy_routes": legacy,
        "missing_required_routes": missing,
        "unknown_routes": unknown,
        "duplicate_routes": duplicates,
        "conflicting_routes": conflicts,
        "skein_not_ready_routes": not_ready,
        "route_readiness_present": evidence.is_some(),
        "route_readiness_ready": evidence_ready,
        "route_catalog_version": "nowledge-mem-graph-read-route-catalog-v1",
        "route_catalog_digest": DIGEST,
        "blocker_codes": blockers,
    })
}

fn campaign(seeds: usize) {
    let mut cases = 0;
    for seed in 0..seeds {
        for layout in 0..10 {
            let routes = inventory(seed, layout);
            for state in 0..11 {
                let evidence = readiness(seed, state);
                for require_all_skein in [false, true] {
                    let report = nowledge_mem_route_ownership_readiness(
                        &routes,
                        evidence.as_ref(),
                        NowledgeMemRouteOwnershipPolicy { require_all_skein },
                    );
                    assert_eq!(report.json(), reference(&routes, evidence.as_ref(), require_all_skein),
                        "seed {seed}, layout {layout}, readiness {state}, all_skein {require_all_skein}");
                    cases += 1;
                }
            }
        }
    }
    assert_eq!(cases, seeds * 220);
}

#[test]
fn ownership_matches_independent_fail_closed_oracle() {
    campaign(4);
}

#[test]
#[ignore = "deterministic local graph route ownership campaign"]
fn graph_route_ownership_differential_campaign() {
    campaign(128);
    eprintln!("graph route ownership: 128 seeds, 28160 inventory/evidence/policy cases");
}
