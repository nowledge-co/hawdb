//! Compatibility re-exports for readiness-owned integration cutover evaluation.

pub use skein_readiness::integration_readiness::*;

#[cfg(test)]
mod tests {
    use super::nowledge_mem_integration_readiness_json;

    #[test]
    fn root_compatibility_module_preserves_integration_entrypoint() {
        let facade: fn(&serde_json::Value) -> serde_json::Value =
            nowledge_mem_integration_readiness_json;
        assert!(std::ptr::fn_addr_eq(
            facade,
            skein_readiness::integration_readiness::nowledge_mem_integration_readiness_json
                as fn(_) -> _,
        ));
    }
}
