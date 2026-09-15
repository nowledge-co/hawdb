use super::*;
use crate::{RuntimeCapabilities, RuntimeCapability};

fn disabled_search_index() -> SearchIndex {
    let mut index = SearchIndex::in_memory();
    index.set_runtime_capabilities(
        RuntimeCapabilities::default().with(RuntimeCapability::FullTextSearch, false),
    );
    index
}

#[test]
fn disabled_search_metadata_probe_reports_capability_error() {
    let index = disabled_search_index();
    let filters = BTreeMap::from([("space_id".into(), "workspace".into())]);
    let metadata = run_search_metadata_workload_probe(&index, "metadata", filters.clone());
    assert!(!metadata.ready);
    assert_eq!(
        metadata.error_class.as_deref(),
        Some("capability_unavailable")
    );
    assert_eq!(metadata.metadata_filters, filters);
    assert_eq!(metadata.total_hits, 0);
}

#[test]
fn disabled_bounded_expansion_probe_reports_capability_error() {
    let index = disabled_search_index();
    let expansion = run_bounded_expansion_probe(&Database::new(), &index, "expand", "query", 4, 2);
    assert!(!expansion.ready);
    assert_eq!(
        expansion.error_class.as_deref(),
        Some("capability_unavailable")
    );
    assert_eq!(expansion.graph_context_limit, 4);
    assert_eq!(expansion.graph_context_max_hops, 2);
    assert_eq!(expansion.path_count, 0);
}

#[cfg(not(feature = "full-text-search"))]
#[test]
fn unavailable_compiled_search_is_reported_by_the_public_workload_fixture() {
    let report = nowledge_graph_route_workload_fixture_report(Default::default()).unwrap();
    assert!(!report.ready);
    assert_eq!(report.search_metadata_probe_count, 3);
    assert_eq!(report.failed_search_metadata_probe_count, 3);
    assert_eq!(report.bounded_expansion_probe_count, 2);
    assert_eq!(report.failed_bounded_expansion_probe_count, 2);
    assert!(report.search_metadata_reports.iter().all(|probe| {
        !probe.ready && probe.error_class.as_deref() == Some("capability_unavailable")
    }));
    assert!(report.bounded_expansion_reports.iter().all(|probe| {
        !probe.ready && probe.error_class.as_deref() == Some("capability_unavailable")
    }));
}
