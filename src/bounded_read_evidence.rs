//! Compatibility re-exports for the bounded-read evidence CLI adapter.
//!
//! The implementation is owned by `skein-readiness`; this module preserves the
//! embedded facade's established API path.

pub use skein_readiness::bounded_read_evidence_cli::*;

#[cfg(test)]
mod tests {
    use super::{parse_graph_route_readiness_json, parse_read_report_json};
    use crate::{NowledgeMemReadReport, NowledgeMemRouteReadinessSummary, Result};

    #[test]
    fn bounded_read_evidence_facade_preserves_contract_types() {
        let _: fn(&serde_json::Value) -> Result<NowledgeMemReadReport> = parse_read_report_json;
        let _: fn(&serde_json::Value) -> Result<NowledgeMemRouteReadinessSummary> =
            parse_graph_route_readiness_json;
    }
}
