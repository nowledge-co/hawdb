//! Compatibility re-exports for evidence-owned storage-recovery preflight.

pub use skein_evidence::storage_recovery_evidence::*;

#[cfg(test)]
mod tests {
    use super::nowledge_storage_recovery_evidence_json;

    #[test]
    fn root_compatibility_module_preserves_evidence_entrypoint() {
        let facade: fn(&serde_json::Value, bool) -> serde_json::Value =
            nowledge_storage_recovery_evidence_json;
        assert!(std::ptr::fn_addr_eq(
            facade,
            skein_evidence::storage_recovery_evidence::nowledge_storage_recovery_evidence_json
                as fn(_, _) -> _,
        ));
    }
}
