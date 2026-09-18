// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

#[test]
fn query_inventory_facade_preserves_owner_types_and_artifacts() {
    use hawdb_evidence::query_inventory as owner;

    let site: owner::CompatibilityQueryCallSite =
        crate::CompatibilityQueryCallSite::new("lookup", "read", "client.rs:7")
            .with_cypher("MATCH (m:Memory) RETURN m.id");
    let inventory: owner::CompatibilityQueryInventory =
        crate::build_compatibility_query_inventory("inventory", [site]).unwrap();
    let item: crate::CompatibilityQueryInventoryItem = inventory.required_checks[0].clone();
    let owner_item: owner::CompatibilityQueryInventoryItem = item;
    assert_eq!(owner_item, inventory.required_checks[0]);
    let json = crate::compatibility_query_inventory_to_json(&inventory);
    assert_eq!(
        json,
        owner::compatibility_query_inventory_to_json(&inventory)
    );
    let facade: crate::CompatibilityQueryInventory =
        owner::build_compatibility_query_inventory_from_json(&json).unwrap();
    assert_eq!(facade, inventory);
    assert_eq!(
        crate::build_compatibility_query_inventory_from_json(&json).unwrap(),
        inventory
    );
    assert_eq!(
        crate::build_compatibility_query_inventory_from_json_str(&json.to_string()).unwrap(),
        inventory
    );
    let options: owner::NowledgeInventoryScanOptions =
        crate::NowledgeInventoryScanOptions::default();
    let _: crate::NowledgeInventoryScanOptions = options;
    let _: fn(std::path::PathBuf) -> Result<owner::CompatibilityQueryInventory> =
        crate::scan_nowledge_query_inventory;
    let _: fn(
        std::path::PathBuf,
        owner::NowledgeInventoryScanOptions,
    ) -> Result<owner::CompatibilityQueryInventory> =
        crate::scan_nowledge_query_inventory_with_options;
    let _: fn(std::path::PathBuf) -> Result<serde_json::Value> =
        crate::scan_nowledge_query_inventory_to_json;
}

#[test]
fn cypher_inventory_coverage_facade_preserves_compat_owner_contracts() {
    use hawdb_compat::nowledge_inventory as owner;

    let options: owner::NowledgeCypherMigrationGateJsonOptions = Default::default();
    let _: crate::NowledgeCypherMigrationGateJsonOptions = options;
    let _: fn(std::path::PathBuf) -> Result<serde_json::Value> =
        crate::scan_nowledge_query_inventory_cypher_coverage_to_json;
    let _: fn(std::path::PathBuf) -> Result<serde_json::Value> =
        crate::scan_nowledge_query_inventory_cypher_coverage_detail_to_json;
}

#[test]
fn inventory_health_facade_preserves_owner_types_and_entrypoints() {
    let report = serde_json::json!({"protocol": "wrong", "readiness": {}});
    let owner: hawdb_evidence::inventory::StorageRecoveryEvidenceHealth =
        storage_recovery_evidence_health(Some(&report), true);
    let facade: crate::StorageRecoveryEvidenceHealth = owner.clone();
    assert_eq!(
        facade,
        crate::storage_recovery_evidence_health(Some(&report), true)
    );
    assert_eq!(
        owner,
        hawdb_evidence::inventory::storage_recovery_evidence_health(Some(&report), true)
    );

    let owner: hawdb_evidence::inventory::BackgroundMaintenanceEvidenceHealth =
        background_maintenance_evidence_health(Some(&report), true);
    let facade: crate::BackgroundMaintenanceEvidenceHealth = owner.clone();
    assert_eq!(
        facade,
        crate::background_maintenance_evidence_health(Some(&report), true)
    );
    assert_eq!(
        owner,
        hawdb_evidence::inventory::background_maintenance_evidence_health(Some(&report), true)
    );

    let families = serde_json::json!([]);
    let owner: hawdb_evidence::inventory::ReplacementReadinessFamilyEvidenceHealth =
        replacement_readiness_family_evidence_health(Some(&families));
    let facade: crate::ReplacementReadinessFamilyEvidenceHealth = owner.clone();
    assert_eq!(
        facade,
        crate::replacement_readiness_family_evidence_health(Some(&families))
    );
    assert_eq!(
        owner,
        hawdb_evidence::inventory::replacement_readiness_family_evidence_health(Some(&families))
    );
    assert_eq!(
        REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
        hawdb_evidence::inventory::REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES
    );
}

#[test]
fn inventory_health_bundle_facade_preserves_optional_and_required_evidence() {
    for bundle in [
        serde_json::json!({}),
        serde_json::json!({
            "storage_recovery": null,
            "background_maintenance": null,
            "replacement_readiness_by_query_family": []
        }),
    ] {
        for required in [false, true] {
            assert_eq!(
                crate::storage_recovery_evidence_health_from_bundle(&bundle, required),
                hawdb_evidence::inventory::storage_recovery_evidence_health_from_bundle(
                    &bundle, required
                )
            );
            assert_eq!(
                crate::background_maintenance_evidence_health_from_bundle(&bundle, required),
                hawdb_evidence::inventory::background_maintenance_evidence_health_from_bundle(
                    &bundle, required
                )
            );
        }
        assert_eq!(
            crate::replacement_readiness_family_evidence_health_from_bundle(&bundle),
            hawdb_evidence::inventory::replacement_readiness_family_evidence_health_from_bundle(
                &bundle
            )
        );
    }
}
