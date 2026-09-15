//! Storage-neutral query-runtime preflight protocol models.

use skein_core::{Result, SkeinError, Value};
use skein_storage::{ScanPruningReport, ScanPruningStrategy};
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

/// Parses standalone probes, probe bundles, or graph-route inventories.
#[doc(hidden)]
pub fn parse_query_runtime_preflight_probes(
    value: &serde_json::Value,
) -> Result<Vec<NowledgeQueryRuntimePreflightProbe>> {
    if let Some(array) = value.as_array() {
        return array.iter().map(parse_probe).collect();
    }
    if let Some(array) = value.get("probes").and_then(serde_json::Value::as_array) {
        return array.iter().map(parse_probe).collect();
    }
    if let Some(array) = value.get("routes").and_then(serde_json::Value::as_array) {
        return parse_route_query_inventory_probes(array);
    }
    Ok(vec![parse_probe(value)?])
}

fn parse_route_query_inventory_probes(
    routes: &[serde_json::Value],
) -> Result<Vec<NowledgeQueryRuntimePreflightProbe>> {
    routes
        .iter()
        .flat_map(|route| match parse_route_query_probes(route) {
            Ok(probes) => probes.into_iter().map(Ok).collect::<Vec<_>>(),
            Err(error) => vec![Err(error)],
        })
        .collect()
}

fn parse_route_query_probes(
    value: &serde_json::Value,
) -> Result<Vec<NowledgeQueryRuntimePreflightProbe>> {
    let object = value.as_object().ok_or_else(|| {
        SkeinError::Semantic("graph route query inventory route must be a JSON object".to_string())
    })?;
    let route = object
        .get("route")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            SkeinError::Semantic(
                "graph route query inventory field 'route' must be a string".to_string(),
            )
        })?
        .to_string();
    if route.trim().is_empty() {
        return Err(SkeinError::Semantic(
            "graph route query inventory route must be non-empty".to_string(),
        ));
    }
    let queries = object
        .get("queries")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            SkeinError::Semantic(
                "graph route query inventory field 'queries' must be an array".to_string(),
            )
        })?;
    queries
        .iter()
        .enumerate()
        .map(|(query_index, query)| parse_route_query_probe(&route, query, query_index))
        .collect()
}

fn parse_route_query_probe(
    route: &str,
    value: &serde_json::Value,
    query_index: usize,
) -> Result<NowledgeQueryRuntimePreflightProbe> {
    let object = value.as_object().ok_or_else(|| {
        SkeinError::Semantic("graph route query inventory query must be a JSON object".to_string())
    })?;
    let name =
        optional_query_name(value).unwrap_or_else(|| format!("{route}:query-{}", query_index + 1));
    if name.trim().is_empty() {
        return Err(SkeinError::Semantic(
            "graph route query inventory query name must be non-empty when provided".to_string(),
        ));
    }
    let cypher = object
        .get("cypher")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            SkeinError::Semantic(
                "graph route query inventory field 'cypher' must be a string".to_string(),
            )
        })?
        .to_string();
    if cypher.trim().is_empty() {
        return Err(SkeinError::Semantic(
            "graph route query inventory field 'cypher' must be non-empty".to_string(),
        ));
    }
    let parameters = object
        .get("parameters")
        .map(parse_parameters_json)
        .transpose()?
        .unwrap_or_default();
    let min_scan_pruning_reports = optional_usize(object, "min_scan_pruning_reports")?.unwrap_or(1);
    Ok(NowledgeQueryRuntimePreflightProbe {
        name,
        route: Some(route.to_string()),
        query_family: optional_string(object, "query_family")?,
        cypher,
        parameters,
        require_scan_pruning: optional_bool(object, "require_scan_pruning")?.unwrap_or(false),
        require_pruned: optional_bool(object, "require_pruned")?.unwrap_or(false),
        min_scan_pruning_reports,
        max_output_rows: optional_usize(object, "max_output_rows")?,
    })
}

fn parse_probe(value: &serde_json::Value) -> Result<NowledgeQueryRuntimePreflightProbe> {
    let object = value.as_object().ok_or_else(|| {
        SkeinError::Semantic("query runtime probe must be a JSON object".to_string())
    })?;
    let name = object
        .get("name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unnamed")
        .to_string();
    let cypher = object
        .get("cypher")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            SkeinError::Semantic("query runtime probe field 'cypher' must be a string".to_string())
        })?
        .to_string();
    let parameters = object
        .get("parameters")
        .map(parse_parameters_json)
        .transpose()?
        .unwrap_or_default();
    let min_scan_pruning_reports = optional_usize(object, "min_scan_pruning_reports")?.unwrap_or(1);
    Ok(NowledgeQueryRuntimePreflightProbe {
        name,
        route: optional_string(object, "route")?,
        query_family: optional_string(object, "query_family")?,
        cypher,
        parameters,
        require_scan_pruning: optional_bool(object, "require_scan_pruning")?.unwrap_or(false),
        require_pruned: optional_bool(object, "require_pruned")?.unwrap_or(false),
        min_scan_pruning_reports,
        max_output_rows: optional_usize(object, "max_output_rows")?,
    })
}

fn optional_query_name(value: &serde_json::Value) -> Option<String> {
    ["name", "query_id", "id"]
        .iter()
        .find_map(|field| value.get(*field).and_then(serde_json::Value::as_str))
        .map(str::to_string)
}

fn parse_parameters_json(value: &serde_json::Value) -> Result<BTreeMap<String, Value>> {
    let object = value.as_object().ok_or_else(|| {
        SkeinError::Semantic("query runtime probe field 'parameters' must be an object".to_string())
    })?;
    object
        .iter()
        .map(|(key, value)| Ok((key.clone(), value_from_json(value)?)))
        .collect()
}

fn value_from_json(value: &serde_json::Value) -> Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float(value))
            } else {
                Err(SkeinError::Semantic(
                    "unsupported JSON number in query runtime probe parameters".to_string(),
                ))
            }
        }
        serde_json::Value::String(value) => Ok(Value::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(value_from_json)
            .collect::<Result<Vec<_>>>()
            .map(Value::List),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), value_from_json(value)?)))
            .collect::<Result<BTreeMap<_, _>>>()
            .map(Value::Map),
    }
}

fn optional_string(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<String>> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(|value| Some(value.to_string()))
        .ok_or_else(|| {
            SkeinError::Semantic(format!(
                "query runtime probe field '{field}' must be a string"
            ))
        })
}

fn optional_bool(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<bool>> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value.as_bool().map(Some).ok_or_else(|| {
        SkeinError::Semantic(format!(
            "query runtime probe field '{field}' must be a boolean"
        ))
    })
}

fn optional_usize(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<usize>> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let raw = value.as_u64().ok_or_else(|| {
        SkeinError::Semantic(format!(
            "query runtime probe field '{field}' must be an integer"
        ))
    })?;
    usize::try_from(raw).map(Some).map_err(|_| {
        SkeinError::Semantic(format!(
            "query runtime probe field '{field}' exceeds usize range"
        ))
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeQueryRuntimePreflightReport {
    pub protocol: String,
    pub ready: bool,
    pub database_opened: bool,
    pub redaction: NowledgeQueryRuntimePreflightRedactionSummary,
    pub probe_count: usize,
    pub passed_probe_count: usize,
    pub failed_probe_count: usize,
    pub required_route_count: usize,
    pub covered_route_count: usize,
    pub covered_routes: Vec<String>,
    pub missing_required_routes: Vec<String>,
    pub required_routes_covered: bool,
    pub unknown_routes: Vec<String>,
    pub duplicate_routes: Vec<String>,
    pub route_catalog_version: String,
    pub route_catalog_digest: String,
    pub route_coverage_ready: bool,
    pub route_coverage_blocker_codes: Vec<String>,
    pub blocker_codes: Vec<String>,
    pub probes: Vec<NowledgeQueryRuntimePreflightProbeReport>,
}

impl NowledgeQueryRuntimePreflightReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "database_opened": self.database_opened,
            "redaction": self.redaction.json(),
            "probe_count": self.probe_count,
            "passed_probe_count": self.passed_probe_count,
            "failed_probe_count": self.failed_probe_count,
            "required_route_count": self.required_route_count,
            "covered_route_count": self.covered_route_count,
            "covered_routes": self.covered_routes,
            "missing_required_routes": self.missing_required_routes,
            "required_routes_covered": self.required_routes_covered,
            "unknown_routes": self.unknown_routes,
            "duplicate_routes": self.duplicate_routes,
            "route_catalog_version": self.route_catalog_version,
            "route_catalog_digest": self.route_catalog_digest,
            "route_coverage_ready": self.route_coverage_ready,
            "route_coverage_blocker_codes": self.route_coverage_blocker_codes,
            "blocker_codes": self.blocker_codes,
            "probes": self.probes.iter().map(NowledgeQueryRuntimePreflightProbeReport::json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NowledgeQueryRuntimePreflightRedactionSummary {
    pub rows_copied: bool,
    pub parameters_copied: bool,
    pub local_paths_copied: bool,
    pub raw_errors_copied: bool,
}

impl NowledgeQueryRuntimePreflightRedactionSummary {
    pub fn ready(&self) -> bool {
        !self.rows_copied
            && !self.parameters_copied
            && !self.local_paths_copied
            && !self.raw_errors_copied
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "ready": self.ready(),
            "rows_copied": self.rows_copied,
            "parameters_copied": self.parameters_copied,
            "local_paths_copied": self.local_paths_copied,
            "raw_errors_copied": self.raw_errors_copied,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeQueryRuntimePreflightProbeReport {
    pub name: String,
    pub route: Option<String>,
    pub query_family: Option<String>,
    pub ready: bool,
    pub success: bool,
    pub output_row_count: usize,
    pub selected_plan_fingerprint: Option<String>,
    pub search_mode: Option<String>,
    pub selected_plan_operator_counts: BTreeMap<String, usize>,
    pub selected_plan_class_counts: BTreeMap<String, usize>,
    pub optimizer_decision_count: usize,
    pub optimizer_rule_event_count: usize,
    pub plan_cache_lookup: Option<String>,
    pub plan_cache_bypass_reason: Option<String>,
    pub plan_cache_cacheable: bool,
    pub plan_cache_hit: bool,
    pub plan_cache_miss: bool,
    pub plan_cache_bypassed: bool,
    pub work_priority: Option<String>,
    pub work_class: Option<String>,
    pub estimated_operations: Option<usize>,
    pub max_rows: Option<usize>,
    pub detection_row_cap: Option<usize>,
    pub row_limit_enforced_before_output: bool,
    pub operator_row_cap_enabled: bool,
    pub blocking_operator_kinds: Vec<String>,
    pub scan_pruning_reports: Vec<ScanPruningReport>,
    pub pruned_scan_count: usize,
    pub error_class: Option<String>,
    pub blocker_codes: Vec<String>,
}

impl NowledgeQueryRuntimePreflightProbeReport {
    pub fn json(&self) -> serde_json::Value {
        let mut value = serde_json::json!({
            "name": self.name,
            "route": self.route,
            "query_family": self.query_family,
            "ready": self.ready,
            "success": self.success,
            "blocker_codes": self.blocker_codes,
        });
        let object = value
            .as_object_mut()
            .expect("query runtime preflight probe report is an object");
        if self.success {
            object.insert(
                "output_row_count".to_string(),
                serde_json::json!(self.output_row_count),
            );
            object.insert(
                "selected_plan_fingerprint".to_string(),
                serde_json::json!(self.selected_plan_fingerprint),
            );
            object.insert(
                "search_mode".to_string(),
                serde_json::json!(self.search_mode),
            );
            object.insert(
                "selected_plan_operator_counts".to_string(),
                serde_json::json!(self.selected_plan_operator_counts),
            );
            object.insert(
                "selected_plan_class_counts".to_string(),
                serde_json::json!(self.selected_plan_class_counts),
            );
            object.insert(
                "optimizer_decision_count".to_string(),
                serde_json::json!(self.optimizer_decision_count),
            );
            object.insert(
                "optimizer_rule_event_count".to_string(),
                serde_json::json!(self.optimizer_rule_event_count),
            );
            object.insert(
                "plan_cache_lookup".to_string(),
                serde_json::json!(self.plan_cache_lookup),
            );
            object.insert(
                "plan_cache".to_string(),
                serde_json::json!({
                    "lookup": self.plan_cache_lookup,
                    "bypass_reason": self.plan_cache_bypass_reason,
                    "cacheable": self.plan_cache_cacheable,
                    "hit": self.plan_cache_hit,
                    "miss": self.plan_cache_miss,
                    "bypassed": self.plan_cache_bypassed,
                }),
            );
            object.insert(
                "work_request".to_string(),
                serde_json::json!({
                    "priority": self.work_priority,
                    "class": self.work_class,
                    "estimated_operations": self.estimated_operations,
                }),
            );
            object.insert(
                "execution_profile".to_string(),
                serde_json::json!({
                    "max_rows": self.max_rows,
                    "detection_row_cap": self.detection_row_cap,
                    "row_limit_enforced_before_output": self.row_limit_enforced_before_output,
                    "operator_row_cap_enabled": self.operator_row_cap_enabled,
                    "blocking_operator_kinds": self.blocking_operator_kinds,
                    "scan_pruning_report_count": self.scan_pruning_reports.len(),
                    "pruned_scan_count": self.pruned_scan_count,
                    "scan_pruning_reports": self.scan_pruning_reports.iter().map(scan_pruning_report_json).collect::<Vec<_>>(),
                }),
            );
        } else {
            object.insert(
                "error_class".to_string(),
                serde_json::json!(self.error_class),
            );
        }
        value
    }
}

/// Encodes scan pruning evidence without exposing storage implementation details.
#[doc(hidden)]
pub fn scan_pruning_report_json(report: &ScanPruningReport) -> serde_json::Value {
    serde_json::json!({
        "target_kind": report.target_kind.as_str(),
        "label_id": report.label_id.map(|label_id| label_id.0),
        "rel_type_id": report.rel_type_id.map(|rel_type_id| rel_type_id.0),
        "strategy": scan_pruning_strategy_json(&report.strategy),
        "pruned": report.pruned,
        "exact_empty": report.exact_empty,
        "candidate_count_before_pruning": report.candidate_count_before_pruning,
        "pruned_candidate_count": report.pruned_candidate_count,
        "candidate_count_before_filter": report.candidate_count_before_filter,
        "output_count": report.output_count,
        "filtered_out_count": report.filtered_out_count,
    })
}

fn scan_pruning_strategy_json(strategy: &ScanPruningStrategy) -> serde_json::Value {
    match strategy {
        ScanPruningStrategy::FullLabelScan => serde_json::json!({"kind": "full_label_scan"}),
        ScanPruningStrategy::ExactCount => serde_json::json!({"kind": "exact_count"}),
        ScanPruningStrategy::Empty => serde_json::json!({"kind": "empty"}),
        ScanPruningStrategy::IdEq => serde_json::json!({"kind": "id_eq"}),
        ScanPruningStrategy::IdIn => serde_json::json!({"kind": "id_in"}),
        ScanPruningStrategy::IdRange => serde_json::json!({"kind": "id_range"}),
        ScanPruningStrategy::PropertyEq { property } => {
            serde_json::json!({"kind": "property_eq", "property": property})
        }
        ScanPruningStrategy::PropertyNotEq { property } => {
            serde_json::json!({"kind": "property_not_eq", "property": property})
        }
        ScanPruningStrategy::PropertyMissingOrNull { property } => {
            serde_json::json!({"kind": "property_missing_or_null", "property": property})
        }
        ScanPruningStrategy::PropertyExists { property } => {
            serde_json::json!({"kind": "property_exists", "property": property})
        }
        ScanPruningStrategy::PropertyDefaultIfNullEq { property } => {
            serde_json::json!({"kind": "property_default_if_null_eq", "property": property})
        }
        ScanPruningStrategy::PropertyDefaultIfNullNotEq { property } => {
            serde_json::json!({"kind": "property_default_if_null_not_eq", "property": property})
        }
        ScanPruningStrategy::PropertyIn { property } => {
            serde_json::json!({"kind": "property_in", "property": property})
        }
        ScanPruningStrategy::CompositePropertyEq { properties } => {
            serde_json::json!({"kind": "composite_property_eq", "properties": properties})
        }
        ScanPruningStrategy::CompositePropertyRange { properties } => serde_json::json!({
            "kind": "composite_property_range",
            "properties": properties,
        }),
        ScanPruningStrategy::PropertyRange { property } => {
            serde_json::json!({"kind": "property_range", "property": property})
        }
        ScanPruningStrategy::FullText { property } => {
            serde_json::json!({"kind": "full_text", "property": property})
        }
        ScanPruningStrategy::OrUnion => serde_json::json!({"kind": "or_union"}),
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

    #[test]
    fn parser_preserves_route_inventory_probe_metadata() {
        let probes = parse_query_runtime_preflight_probes(&serde_json::json!({
            "routes": [{
                "route": "/graph/overview",
                "queries": [{
                    "query_id": "overview-by-kind",
                    "query_family": "graph_overview",
                    "cypher": "MATCH (m:Memory) WHERE m.kind = $kind RETURN m",
                    "parameters": {
                        "kind": "note",
                        "limits": [1, 2],
                        "flags": {"strict": true}
                    },
                    "require_scan_pruning": true,
                    "require_pruned": true,
                    "min_scan_pruning_reports": 2,
                    "max_output_rows": 3
                }]
            }]
        }))
        .unwrap();

        assert_eq!(probes.len(), 1);
        let probe = &probes[0];
        assert_eq!(probe.name, "overview-by-kind");
        assert_eq!(probe.route.as_deref(), Some("/graph/overview"));
        assert_eq!(probe.query_family.as_deref(), Some("graph_overview"));
        assert_eq!(probe.parameters["kind"], Value::String("note".to_string()));
        assert_eq!(
            probe.parameters["limits"],
            Value::List(vec![Value::Int(1), Value::Int(2)])
        );
        assert_eq!(
            probe.parameters["flags"],
            Value::Map(BTreeMap::from([("strict".to_string(), Value::Bool(true))]))
        );
        assert!(probe.require_scan_pruning);
        assert!(probe.require_pruned);
        assert_eq!(probe.min_scan_pruning_reports, 2);
        assert_eq!(probe.max_output_rows, Some(3));
    }

    #[test]
    fn parser_rejects_invalid_route_inventory_shape() {
        let error = parse_query_runtime_preflight_probes(&serde_json::json!({
            "routes": [{
                "route": " ",
                "queries": []
            }]
        }))
        .unwrap_err();

        assert!(error.to_string().contains("route must be non-empty"));
    }

    #[test]
    fn preflight_redaction_summary_stays_safe_by_default() {
        assert_eq!(
            NowledgeQueryRuntimePreflightRedactionSummary::default().json(),
            serde_json::json!({
                "ready": true,
                "rows_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false,
                "raw_errors_copied": false,
            })
        );
    }

    #[test]
    fn successful_probe_report_preserves_scan_pruning_evidence_shape() {
        let report = NowledgeQueryRuntimePreflightProbeReport {
            name: "memory-by-kind".to_string(),
            route: Some("/graph/overview".to_string()),
            query_family: Some("memory_lookup".to_string()),
            ready: true,
            success: true,
            output_row_count: 1,
            selected_plan_fingerprint: Some("plan-a".to_string()),
            search_mode: None,
            selected_plan_operator_counts: BTreeMap::new(),
            selected_plan_class_counts: BTreeMap::new(),
            optimizer_decision_count: 0,
            optimizer_rule_event_count: 0,
            plan_cache_lookup: Some("hit".to_string()),
            plan_cache_bypass_reason: None,
            plan_cache_cacheable: true,
            plan_cache_hit: true,
            plan_cache_miss: false,
            plan_cache_bypassed: false,
            work_priority: None,
            work_class: None,
            estimated_operations: None,
            max_rows: Some(1),
            detection_row_cap: Some(1),
            row_limit_enforced_before_output: true,
            operator_row_cap_enabled: true,
            blocking_operator_kinds: Vec::new(),
            scan_pruning_reports: vec![ScanPruningReport {
                target_kind: skein_storage::ScanPruningTargetKind::Node,
                label_id: None,
                rel_type_id: None,
                strategy: ScanPruningStrategy::PropertyEq {
                    property: "kind".to_string(),
                },
                pruned: true,
                exact_empty: false,
                candidate_count_before_pruning: 8,
                pruned_candidate_count: 6,
                candidate_count_before_filter: 2,
                output_count: 1,
                filtered_out_count: 1,
            }],
            pruned_scan_count: 1,
            error_class: None,
            blocker_codes: Vec::new(),
        };

        let json = report.json();
        assert_eq!(json["execution_profile"]["scan_pruning_report_count"], 1);
        assert_eq!(json["execution_profile"]["pruned_scan_count"], 1);
        assert_eq!(
            json["execution_profile"]["scan_pruning_reports"][0]["strategy"],
            serde_json::json!({"kind": "property_eq", "property": "kind"})
        );
        assert!(json.get("error_class").is_none());
    }
}
