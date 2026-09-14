//! Compatibility re-exports for readiness-owned graph route CLI adapters.

pub use skein_readiness::graph_route_cli::*;

#[cfg(test)]
mod tests {
    use super::nowledge_graph_route_readiness_json;

    #[test]
    fn root_compatibility_module_preserves_graph_route_readiness_entrypoint() {
        let facade: fn(&serde_json::Value) -> skein_core::Result<serde_json::Value> =
            nowledge_graph_route_readiness_json;
        assert!(std::ptr::fn_addr_eq(
            facade,
            skein_readiness::graph_route::nowledge_graph_route_readiness_json as fn(_) -> _,
        ));
    }
}
