//! Storage-neutral query-runtime preflight protocol models.

use skein_core::Value;
use std::collections::BTreeMap;

/// One bounded query probe supplied to the embedded query-runtime preflight.
///
/// The host retains database opening and probe execution. This model only
/// describes the requested query and the evidence requirements.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeQueryRuntimePreflightProbe {
    pub name: String,
    pub route: Option<String>,
    pub query_family: Option<String>,
    pub cypher: String,
    pub parameters: BTreeMap<String, Value>,
    pub require_scan_pruning: bool,
    pub require_pruned: bool,
    pub min_scan_pruning_reports: usize,
    pub max_output_rows: Option<usize>,
}

impl NowledgeQueryRuntimePreflightProbe {
    pub fn new(name: impl Into<String>, cypher: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            route: None,
            query_family: None,
            cypher: cypher.into(),
            parameters: BTreeMap::new(),
            require_scan_pruning: false,
            require_pruned: false,
            min_scan_pruning_reports: 1,
            max_output_rows: None,
        }
    }

    pub fn with_route(mut self, route: impl Into<String>) -> Self {
        self.route = Some(route.into());
        self
    }

    pub fn with_query_family(mut self, query_family: impl Into<String>) -> Self {
        self.query_family = Some(query_family.into());
        self
    }

    pub fn with_parameters(mut self, parameters: BTreeMap<String, Value>) -> Self {
        self.parameters = parameters;
        self
    }

    pub fn require_scan_pruning(mut self, min_scan_pruning_reports: usize) -> Self {
        self.require_scan_pruning = true;
        self.min_scan_pruning_reports = min_scan_pruning_reports;
        self
    }

    pub fn require_pruned(mut self) -> Self {
        self.require_pruned = true;
        self
    }

    pub fn with_max_output_rows(mut self, max_output_rows: usize) -> Self {
        self.max_output_rows = Some(max_output_rows);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_builder_preserves_protocol_defaults_and_optional_evidence() {
        let probe = NowledgeQueryRuntimePreflightProbe::new("bounded-read", "MATCH (m) RETURN m")
            .with_route("/graph/overview")
            .with_query_family("graph_overview")
            .with_parameters(BTreeMap::from([("limit".to_string(), Value::Int(1))]))
            .require_scan_pruning(2)
            .require_pruned()
            .with_max_output_rows(1);

        assert_eq!(probe.name, "bounded-read");
        assert_eq!(probe.route.as_deref(), Some("/graph/overview"));
        assert_eq!(probe.query_family.as_deref(), Some("graph_overview"));
        assert_eq!(probe.parameters["limit"], Value::Int(1));
        assert!(probe.require_scan_pruning);
        assert!(probe.require_pruned);
        assert_eq!(probe.min_scan_pruning_reports, 2);
        assert_eq!(probe.max_output_rows, Some(1));
    }
}
