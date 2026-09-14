//! Embedded facade paths and developer CLI help for replacement summaries.

pub use skein_readiness::graph_summary::{
    nowledge_graph_route_readiness_summary, nowledge_graph_route_readiness_summary_from_bundle,
    GraphRouteReadinessSummary,
};
pub use skein_readiness::replacement_summary::{
    nowledge_replacement_summary_json, nowledge_replacement_summary_json_with_options,
    NowledgeReplacementSummaryOptions,
};

pub fn nowledge_replacement_summary_usage() -> String {
    "nowledge-replacement-summary requires [--require-production-ready] [--compact] [--max-family-items <n>] [--max-blockers <n>] [--search-projection-evidence-json <path>] [--search-projection-shadow-evidence-json <path>] [--search-candidate-shadow-evidence-json <path>] [--bounded-read-evidence-json <path>] [--query-runtime-preflight-json <path>] [--query-family-evidence-json <path>] <migration-gate-json>"
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::nowledge_replacement_summary_usage;

    #[test]
    fn replacement_summary_facade_preserves_owner_type_and_entrypoints() {
        use skein_readiness::replacement_summary as owner;

        let options: owner::NowledgeReplacementSummaryOptions =
            crate::NowledgeReplacementSummaryOptions::default();
        let summarize: fn(
            &serde_json::Value,
            owner::NowledgeReplacementSummaryOptions,
        ) -> serde_json::Value = crate::nowledge_replacement_summary_json_with_options;
        let bundle = serde_json::json!({});
        let summary = summarize(&bundle, options);
        assert_eq!(summary, owner::nowledge_replacement_summary_json(&bundle));
        assert_eq!(summary, super::nowledge_replacement_summary_json(&bundle));
        assert_eq!(summary["protocol"], "skein-nowledge-replacement-summary");
        assert_eq!(summary["production_cutover_ready"], false);
    }

    #[test]
    fn replacement_summary_facade_preserves_shared_evidence_contracts() {
        use skein_evidence::replacement_contract as contract;

        assert_eq!(
            crate::NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
            contract::NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
        );
        assert_eq!(
            crate::NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL,
            contract::NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL,
        );
        assert_eq!(
            crate::NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
            contract::NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
        );
        assert_eq!(
            crate::NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
            contract::NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
        );
    }

    #[test]
    fn validates_nowledge_replacement_summary_usage_text() {
        assert!(nowledge_replacement_summary_usage().contains("<migration-gate-json>"));
        assert!(nowledge_replacement_summary_usage().contains("--require-production-ready"));
        assert!(nowledge_replacement_summary_usage().contains("--compact"));
        assert!(nowledge_replacement_summary_usage().contains("--max-family-items"));
        assert!(nowledge_replacement_summary_usage().contains("--max-blockers"));
        assert!(nowledge_replacement_summary_usage().contains("--search-projection-evidence-json"));
        assert!(nowledge_replacement_summary_usage()
            .contains("--search-projection-shadow-evidence-json"));
        assert!(nowledge_replacement_summary_usage()
            .contains("--search-candidate-shadow-evidence-json"));
        assert!(nowledge_replacement_summary_usage().contains("--bounded-read-evidence-json"));
        assert!(nowledge_replacement_summary_usage().contains("--query-runtime-preflight-json"));
        assert!(nowledge_replacement_summary_usage().contains("--query-family-evidence-json"));
    }
}
