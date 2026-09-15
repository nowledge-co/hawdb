//! Compatibility re-exports for search-owned candidate evidence CLI adapters.

pub use skein_search::candidate_evidence_cli::*;

#[cfg(test)]
mod tests {
    use super::parse_search_candidate_shadow_probe;

    #[test]
    fn root_compatibility_module_preserves_candidate_probe_entrypoint() {
        let facade: fn(
            &serde_json::Value,
        ) -> skein_core::Result<
            skein_search::candidate_evidence::NowledgeMemSearchCandidateShadowAccumulator,
        > = parse_search_candidate_shadow_probe;
        assert!(std::ptr::fn_addr_eq(
            facade,
            skein_search::candidate_evidence::parse_search_candidate_shadow_probe as fn(_) -> _,
        ));
    }
}
