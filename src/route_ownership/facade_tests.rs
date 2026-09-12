use super::*;
use skein_route_ownership::graph as owner;

#[test]
fn ownership_facade_preserves_concrete_types_and_function_signatures() {
    let summarize: fn(
        &[owner::NowledgeMemRouteOwnership],
        Option<&crate::NowledgeMemRouteReadinessSummary>,
        owner::NowledgeMemRouteOwnershipPolicy,
    ) -> crate::NowledgeMemRouteOwnershipReadinessReport =
        crate::nowledge_mem_route_ownership_readiness;
    let routes: Vec<owner::NowledgeMemRouteOwnership> = nowledge_mem_route_ownership_all_legacy();
    let report: NowledgeMemRouteOwnershipReadinessReport =
        summarize(&routes, None, NowledgeMemRouteOwnershipPolicy::migration());
    assert!(report.ready);
    assert!(!report.production_cutover_ready);
    assert_eq!(report.route_catalog_digest, "fnv1a64:80816a4f519d9693");
    assert_eq!(
        report,
        owner::nowledge_mem_route_ownership_readiness(
            &routes,
            None,
            NowledgeMemRouteOwnershipPolicy::migration(),
        )
    );
}

#[test]
fn catalog_facade_preserves_public_paths_and_concrete_types() {
    let lookup: fn(&str) -> Option<&'static owner::NowledgeMemGraphReadRouteSpec> =
        crate::nowledge_mem_graph_read_route_spec;
    let summary: Option<&owner::NowledgeMemRouteReadinessSummary> =
        None::<&crate::nowledge_mem::NowledgeMemRouteReadinessSummary>;
    assert!(summary.is_none());
    // Const slices can be promoted separately in each consuming crate; their
    // addresses are not part of the facade contract.
    for expected in owner::NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS {
        assert_eq!(lookup(expected.route).unwrap(), expected);
        let actual: &crate::NowledgeMemGraphReadRouteSpec =
            crate::nowledge_mem::nowledge_mem_graph_read_route_spec(expected.route).unwrap();
        assert_eq!(actual, expected);
        let _: crate::NowledgeMemGraphReadRouteOwner = expected.owner;
        let _: crate::NowledgeMemGraphReadRouteEvidenceKind = expected.required_evidence_kind;
    }
    assert_eq!(
        crate::nowledge_mem::NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS,
        owner::NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS
    );
    assert_eq!(
        crate::REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
        owner::REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
    );
    assert_eq!(
        crate::nowledge_mem_graph_read_route_specs_json(),
        owner::nowledge_mem_graph_read_route_specs_json()
    );
    assert_eq!(
        crate::nowledge_mem_graph_read_route_catalog_digest(),
        "fnv1a64:80816a4f519d9693"
    );
}
