//! Compatibility re-exports for readiness-owned integration bundle CLI adapters.

pub use skein_readiness::integration_bundle_cli::*;

#[cfg(test)]
mod tests {
    use skein_readiness::integration_bundle_cli;
    use std::any::TypeId;

    #[test]
    fn facade_preserves_owner_type_and_function_identity() {
        assert_eq!(
            TypeId::of::<crate::IntegrationBundleInputs>(),
            TypeId::of::<integration_bundle_cli::IntegrationBundleInputs>()
        );
        let bundle: fn(
            integration_bundle_cli::IntegrationBundleInputs,
        ) -> crate::Result<serde_json::Value> = crate::nowledge_mem_integration_bundle_json;
        assert!(std::ptr::fn_addr_eq(
            bundle,
            integration_bundle_cli::nowledge_mem_integration_bundle_json as fn(_) -> _
        ));
        let runner: fn(std::vec::IntoIter<String>) -> crate::Result<(serde_json::Value, bool)> =
            crate::mem_integration_bundle::run_nowledge_mem_integration_bundle;
        assert!(std::ptr::fn_addr_eq(
            runner,
            integration_bundle_cli::run_nowledge_mem_integration_bundle
                as fn(std::vec::IntoIter<String>) -> crate::Result<(serde_json::Value, bool)>
        ));
    }
}
