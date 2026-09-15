//! Compatibility re-exports for search-owned projection evidence CLI adapters.

pub use skein_search::projection_evidence_cli::*;

#[cfg(test)]
mod tests {
    use skein_search::projection_evidence_cli;
    use std::any::TypeId;

    #[test]
    fn facade_preserves_owner_type_and_function_identity() {
        assert_eq!(
            TypeId::of::<crate::NowledgeSearchProjectionEvidenceReport>(),
            TypeId::of::<projection_evidence_cli::NowledgeSearchProjectionEvidenceReport>()
        );
        let evidence: fn(&serde_json::Value) -> serde_json::Value =
            crate::search_projection_evidence::nowledge_search_projection_evidence_json;
        assert!(std::ptr::fn_addr_eq(
            evidence,
            projection_evidence_cli::nowledge_search_projection_evidence_json as fn(_) -> _
        ));
        let runner: fn(std::vec::IntoIter<String>) -> crate::Result<(serde_json::Value, bool)> =
            crate::search_projection_evidence::run_nowledge_search_projection_evidence;
        assert!(std::ptr::fn_addr_eq(
            runner,
            projection_evidence_cli::run_nowledge_search_projection_evidence
                as fn(std::vec::IntoIter<String>) -> crate::Result<(serde_json::Value, bool)>
        ));
    }
}
