//! Compatibility re-exports for evidence-owned background-maintenance preflight.

pub use skein_evidence::background_maintenance_evidence::*;

#[cfg(test)]
mod tests {
    use super::nowledge_background_maintenance_evidence_json;

    #[test]
    fn root_compatibility_module_preserves_evidence_entrypoint() {
        let facade: fn(&serde_json::Value, bool) -> serde_json::Value =
            nowledge_background_maintenance_evidence_json;
        assert!(std::ptr::fn_addr_eq(
            facade,
            skein_evidence::background_maintenance_evidence::nowledge_background_maintenance_evidence_json
                as fn(_, _) -> _,
        ));
    }
}
