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
fn artifact_formats_preserve_precedence_and_whitespace_contracts() {
    let artifact = serde_json::json!({
        "name": " inventory ",
        "call_sites": [{
            "name": " lookup ", "query_family": " read ",
            "source": " client.rs:1 ", "cypher": "  "
        }],
        "required_checks": "ignored because call_sites takes precedence"
    });
    let inventory = build_compatibility_query_inventory_from_json(&artifact).unwrap();
    assert_eq!(inventory.name, " inventory ");
    assert_eq!(
        inventory.required_checks,
        vec![CompatibilityQueryInventoryItem::new("lookup", "read").with_source("client.rs:1")]
    );
    let exported = compatibility_query_inventory_to_json(&inventory);
    assert!(exported["required_checks"][0].get("cypher").is_none());
    let imported = build_compatibility_query_inventory_from_json(&exported).unwrap();
    assert_eq!(imported.name, "inventory");
    assert_eq!(imported.required_checks, inventory.required_checks);
    let optional = serde_json::json!({
        "name": " inventory ",
        "required_checks": [{"name": " lookup ", "query_family": " read ", "source": null}]
    });
    let inventory = build_compatibility_query_inventory_from_json(&optional).unwrap();
    assert_eq!(inventory.name, "inventory");
    assert_eq!(
        inventory.required_checks,
        vec![CompatibilityQueryInventoryItem::new("lookup", "read")]
    );
}

#[test]
fn artifact_rejects_duplicates_after_trimming_and_invalid_optional_fields() {
    for key in ["call_sites", "required_checks"] {
        let mut artifact = serde_json::json!({"name": "inventory"});
        artifact[key] = serde_json::json!([
            {"name": "lookup", "query_family": "read", "source": "a.rs:1"},
            {"name": " lookup ", "query_family": "read", "source": "b.rs:2"}
        ]);
        let error = build_compatibility_query_inventory_from_json(&artifact).unwrap_err();
        assert!(error.to_string().contains("duplicate compatibility query"));
        assert!(error.to_string().contains("'a.rs:1' and 'b.rs:2'"));
        artifact[key] = serde_json::json!([
            {"name": "lookup", "query_family": "read", "source": "a.rs:1", "cypher": 7}
        ]);
        assert_eq!(
            build_compatibility_query_inventory_from_json(&artifact).unwrap_err().to_string(),
            "semantic error: compatibility query inventory artifact field 'cypher' must be a string"
        );
    }
}

#[test]
fn query_inventory_builder_preserves_call_site_metadata() {
    let inventory = build_compatibility_query_inventory(
        "production-nowledge-inventory",
        [
            CompatibilityQueryCallSite::new(
                "memory lookup",
                "parameterized_read",
                "memory_store.rs:42",
            )
            .with_cypher("MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title"),
            CompatibilityQueryCallSite::new(
                "graph context",
                "bounded_path_read",
                "retrieval.rs:88",
            ),
        ],
    )
    .unwrap();

    assert_eq!(inventory.name, "production-nowledge-inventory");
    assert_eq!(inventory.required_checks.len(), 2);
    assert_eq!(inventory.required_checks[0].name, "memory lookup");
    assert_eq!(
        inventory.required_checks[0].source.as_deref(),
        Some("memory_store.rs:42")
    );
    assert_eq!(
        inventory.required_checks[0].cypher.as_deref(),
        Some("MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title")
    );
    assert_eq!(inventory.required_checks[1].cypher, None);
}

#[test]
fn query_inventory_builder_rejects_duplicate_check_names() {
    let error = build_compatibility_query_inventory(
        "production-nowledge-inventory",
        [
            CompatibilityQueryCallSite::new("memory lookup", "read", "first.rs:1"),
            CompatibilityQueryCallSite::new("memory lookup", "read", "second.rs:2"),
        ],
    )
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("duplicate compatibility query call site"));
}

#[test]
fn query_inventory_json_imports_scanner_call_site_artifacts() {
    let artifact = serde_json::json!({
        "name": "production-nowledge-inventory",
        "call_sites": [
            {
                "name": "memory lookup",
                "query_family": "parameterized_read",
                "source": "memory_store.rs:42",
                "cypher": "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title"
            },
            {
                "name": "graph context",
                "query_family": "bounded_path_read",
                "source": "retrieval.rs:88"
            }
        ]
    });

    let inventory = super::build_compatibility_query_inventory_from_json(&artifact).unwrap();
    let exported = super::compatibility_query_inventory_to_json(&inventory);
    let reimported = super::build_compatibility_query_inventory_from_json(&exported).unwrap();

    assert_eq!(inventory.name, "production-nowledge-inventory");
    assert_eq!(inventory.required_checks.len(), 2);
    assert_eq!(
        inventory.required_checks[0].source.as_deref(),
        Some("memory_store.rs:42")
    );
    assert_eq!(
        inventory.required_checks[0].cypher.as_deref(),
        Some("MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title")
    );
    assert_eq!(exported["required_checks"].as_array().unwrap().len(), 2);
    assert_eq!(reimported, inventory);
}

#[test]
fn query_inventory_json_rejects_duplicate_required_checks() {
    let artifact = r#"{
            "name": "production-nowledge-inventory",
            "required_checks": [
                {
                    "name": "memory lookup",
                    "query_family": "parameterized_read",
                    "source": "first.rs:1"
                },
                {
                    "name": "memory lookup",
                    "query_family": "parameterized_read",
                    "source": "second.rs:2"
                }
            ]
        }"#;

    let error = super::build_compatibility_query_inventory_from_json_str(artifact).unwrap_err();

    assert!(error
        .to_string()
        .contains("duplicate compatibility query inventory item"));
}

#[test]
fn query_inventory_json_rejects_malformed_artifacts() {
    let artifact = serde_json::json!({
        "name": "production-nowledge-inventory",
        "call_sites": [
            {
                "name": "memory lookup",
                "query_family": "parameterized_read",
                "source": 42
            }
        ]
    });

    let error = super::build_compatibility_query_inventory_from_json(&artifact).unwrap_err();

    assert!(error
        .to_string()
        .contains("field 'source' must be a string"));
}
