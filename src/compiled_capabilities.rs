use crate::RuntimeCapabilities;

pub use skein_search::compiled_runtime_capabilities;

pub(crate) const fn effective_runtime_capabilities(
    requested: RuntimeCapabilities,
) -> RuntimeCapabilities {
    requested.intersection(compiled_runtime_capabilities())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeCapability;

    #[test]
    fn compiled_matrix_matches_enabled_cargo_features() {
        let capabilities = compiled_runtime_capabilities();
        assert_eq!(capabilities, skein_search::compiled_runtime_capabilities());

        assert_eq!(
            capabilities.is_enabled(RuntimeCapability::AccessControl),
            cfg!(feature = "acl")
        );
        assert_eq!(
            capabilities.is_enabled(RuntimeCapability::FullTextSearch),
            cfg!(feature = "full-text-search")
        );
        assert_eq!(
            capabilities.is_enabled(RuntimeCapability::VectorSearch),
            cfg!(feature = "vector-search")
        );
        assert_eq!(
            capabilities.is_enabled(RuntimeCapability::GraphAnalytics),
            cfg!(feature = "graph-analytics")
        );
        assert_eq!(
            capabilities.is_enabled(RuntimeCapability::BackgroundMaintenance),
            cfg!(feature = "background-maintenance")
        );
    }
}
