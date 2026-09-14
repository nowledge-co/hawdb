//! Compatibility re-exports for evidence-owned query-family preflight.

pub use skein_evidence::query_family_evidence::*;

#[cfg(test)]
mod tests {
    use super::nowledge_query_family_evidence_json;

    #[test]
    fn root_compatibility_module_preserves_evidence_entrypoint() {
        let facade: fn(&serde_json::Value) -> crate::Result<serde_json::Value> =
            nowledge_query_family_evidence_json;
        assert!(std::ptr::fn_addr_eq(
            facade,
            skein_evidence::query_family_evidence::nowledge_query_family_evidence_json
                as fn(_) -> _,
        ));
    }
}
