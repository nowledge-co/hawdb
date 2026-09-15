//! Source-derived Nowledge query-inventory coverage over compatibility fixtures.
//! Database execution and host-specific migration-gate assembly stay in the facade.

use crate::{
    assess_query_inventory_cypher_coverage, compatibility_inventory_coverage_report_to_json,
    nowledge_memory_core_fixture, CompatibilityCheck, CompatibilityQueryInventoryItem,
    CompatibilityRollbackEvidence, ExternalShadowReady,
};
use skein_core::Result;
use skein_evidence::query_inventory::scan_nowledge_query_inventory;
use std::collections::BTreeSet;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NowledgeCypherMigrationGateJsonOptions {
    pub shadow_name: Option<String>,
    pub self_shadow: bool,
    pub shadow_ready: Option<ExternalShadowReady>,
    pub ready_preflight: bool,
    pub shadow_trace_path: Option<String>,
    pub shadow_request_count: Option<u64>,
    pub include_cutover_evidence: bool,
    pub storage_recovery_required: bool,
    pub storage_recovery: Option<serde_json::Value>,
    pub background_maintenance_required: bool,
    pub background_maintenance: Option<serde_json::Value>,
    pub replacement_readiness_by_query_family: Option<serde_json::Value>,
    pub previous_wrapper_contract_evidence: Option<serde_json::Value>,
    pub rollback: CompatibilityRollbackEvidence,
}

pub fn scan_nowledge_query_inventory_cypher_coverage_to_json(
    root: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    let inventory = scan_nowledge_query_inventory(root)?;
    let fixture = nowledge_memory_core_fixture();
    let coverage = assess_query_inventory_cypher_coverage(&fixture, &inventory);
    Ok(compatibility_inventory_coverage_report_to_json(&coverage))
}

pub fn scan_nowledge_query_inventory_cypher_coverage_detail_to_json(
    root: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    let inventory = scan_nowledge_query_inventory(root)?;
    let fixture = nowledge_memory_core_fixture();
    let coverage = assess_query_inventory_cypher_coverage(&fixture, &inventory);
    let fixture_cypher_keys = fixture
        .checks
        .iter()
        .filter_map(fixture_check_cypher)
        .map(cypher_coverage_key)
        .collect::<BTreeSet<_>>();
    let missing_items = inventory
        .required_checks
        .iter()
        .filter(|item| {
            item.cypher
                .as_deref()
                .map(cypher_coverage_key)
                .is_none_or(|key| !fixture_cypher_keys.contains(&key))
        })
        .map(inventory_item_detail_to_json)
        .collect::<Vec<_>>();
    let covered_items = inventory
        .required_checks
        .iter()
        .filter(|item| {
            item.cypher
                .as_deref()
                .map(cypher_coverage_key)
                .is_some_and(|key| fixture_cypher_keys.contains(&key))
        })
        .map(inventory_item_detail_to_json)
        .collect::<Vec<_>>();

    Ok(serde_json::json!({
        "coverage": compatibility_inventory_coverage_report_to_json(&coverage),
        "covered_items": covered_items,
        "missing_items": missing_items,
    }))
}

fn fixture_check_cypher(check: &CompatibilityCheck) -> Option<&str> {
    match check {
        CompatibilityCheck::Cypher(check) => Some(check.statement.cypher.as_str()),
        CompatibilityCheck::ProjectedGraph(_) => None,
    }
}

fn inventory_item_detail_to_json(item: &CompatibilityQueryInventoryItem) -> serde_json::Value {
    serde_json::json!({
        "name": item.name,
        "query_family": item.query_family,
        "source": item.source,
        "cypher": item.cypher,
    })
}

fn cypher_coverage_key(cypher: &str) -> String {
    cypher.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::{
        scan_nowledge_query_inventory_cypher_coverage_detail_to_json,
        scan_nowledge_query_inventory_cypher_coverage_to_json,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn scanned_cypher_coverage_reports_fixture_matches() {
        let root = unique_test_path("coverage");
        let source_dir = root.join("crates/nmem-graph/src");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(
            source_dir.join("repo.rs"),
            r#"
                pub fn query() -> &'static str {
                    "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title"
                }
            "#,
        )
        .unwrap();

        let coverage = scan_nowledge_query_inventory_cypher_coverage_to_json(&root).unwrap();

        assert_eq!(coverage["fixture"], "nowledge-memory-core");
        assert_eq!(coverage["required_checks"], 1);
        assert_eq!(coverage["covered_checks"], 1);
        assert_eq!(coverage["missing_checks"].as_array().unwrap().len(), 0);
        assert_eq!(
            coverage["extra_fixture_checks"].as_array().unwrap().len(),
            643
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn coverage_detail_reports_missing_item_metadata() {
        let root = unique_test_path("detail");
        let source_dir = root.join("crates/nmem-graph/src");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(
            source_dir.join("repo.rs"),
            r#"
                pub fn covered() -> &'static str {
                    "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title"
                }

                pub fn missing() -> &'static str {
                    "MATCH (m:Memory) WHERE m.id = $id RETURN m.uncovered_property"
                }
            "#,
        )
        .unwrap();

        let detail = scan_nowledge_query_inventory_cypher_coverage_detail_to_json(&root).unwrap();
        let missing_items = detail["missing_items"].as_array().unwrap();
        let covered_items = detail["covered_items"].as_array().unwrap();

        assert_eq!(detail["coverage"]["required_checks"], 2);
        assert_eq!(detail["coverage"]["covered_checks"], 1);
        assert_eq!(covered_items.len(), 1);
        assert_eq!(missing_items.len(), 1);
        assert_eq!(
            missing_items[0]["cypher"],
            "MATCH (m:Memory) WHERE m.id = $id RETURN m.uncovered_property"
        );
        assert_eq!(missing_items[0]["query_family"], "read");
        assert_eq!(
            missing_items[0]["source"],
            "crates/nmem-graph/src/repo.rs:7"
        );

        fs::remove_dir_all(root).unwrap();
    }

    fn unique_test_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein-compat-nowledge-inventory-{name}-{nanos}"))
    }
}
