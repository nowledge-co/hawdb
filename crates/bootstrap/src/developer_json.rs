//! Developer-facing JSON derived from the portable bootstrap contracts.
//!
//! The embedded facade supplies live snapshots and any storage-specific
//! evidence. This module serializes only the storage-neutral bootstrap models.

use crate::{
    CanonicalSnapshotEndpointViolation, CanonicalSnapshotIdentityAudit,
    SkeinLightningBootstrapExport, SkeinLightningBootstrapManifest,
    SkeinLightningGraphStreamValidation, SkeinLightningRelationalStreamValidation,
};
use skein_core::Value;

pub fn skein_lightning_bootstrap_manifest_json(
    manifest: &SkeinLightningBootstrapManifest,
) -> serde_json::Value {
    serde_json::json!({
        "protocol": "skein-lightning-bootstrap",
        "protocol_version": manifest.protocol_version,
        "database_commit_epoch": manifest.database_commit_epoch,
        "graph_commit_epoch": manifest.graph_commit_epoch,
        "logical_checksum": manifest.logical_checksum,
        "graph_stream_checksum": manifest.graph_stream_checksum,
        "graph_stream_byte_len": manifest.graph_stream_byte_len,
        "relational_stream_format_version": manifest.relational_stream_format_version,
        "relational_stream_checksum": manifest.relational_stream_checksum,
        "relational_stream_byte_len": manifest.relational_stream_byte_len,
        "relational_table_count": manifest.relational_table_count,
        "relational_row_count": manifest.relational_row_count,
        "relational_overflow_segment_count": manifest.relational_overflow_segment_count,
        "schema_checksum": manifest.schema_checksum,
        "node_count": manifest.node_count,
        "relationship_count": manifest.relationship_count,
        "label_count": manifest.label_count,
        "relationship_type_count": manifest.relationship_type_count,
        "node_property_count": manifest.node_property_count,
        "relationship_property_count": manifest.relationship_property_count,
        "validation": {
            "is_valid": manifest.validation.is_valid,
            "is_import_ready": manifest.validation.is_import_ready,
            "checksum_matches": manifest.validation.checksum_matches,
            "expected_logical_checksum": manifest.validation.expected_logical_checksum,
            "stable_identity_matches": manifest.validation.stable_identity_matches,
            "stable_identity_ready": manifest.validation.stable_identity_ready,
            "expected_stable_identity": stable_identity_audit_json(&manifest.validation.expected_stable_identity),
            "duplicate_node_ids": manifest.validation.duplicate_node_ids,
            "duplicate_relationship_ids": manifest.validation.duplicate_relationship_ids,
            "missing_sources": endpoint_violations_json(&manifest.validation.missing_sources),
            "missing_targets": endpoint_violations_json(&manifest.validation.missing_targets),
        },
        "relational_validation": skein_lightning_relational_stream_validation_json(
            &manifest.relational_validation,
        ),
    })
}

pub fn skein_lightning_bootstrap_bundle_json(
    export: &SkeinLightningBootstrapExport,
) -> serde_json::Value {
    skein_lightning_bootstrap_bundle_json_with_optional_storage_recovery(export, None)
}

pub fn skein_lightning_bootstrap_bundle_json_with_optional_storage_recovery(
    export: &SkeinLightningBootstrapExport,
    storage_recovery: Option<serde_json::Value>,
) -> serde_json::Value {
    let graph_stream_validation = export
        .graph_stream
        .validate_against_manifest(&export.manifest);
    let relational_stream_validation = export
        .relational_stream
        .validate_against_manifest(&export.manifest);
    let mut blockers = Vec::new();
    let mut manifest_blocker_messages = Vec::new();
    if !export.manifest.validation.is_import_ready {
        manifest_blocker_messages.push("manifest validation is not import ready");
    }
    if !export.manifest.relational_validation.is_valid {
        manifest_blocker_messages.push("manifest relational validation is not import ready");
    }
    blockers.extend(manifest_blocker_messages.iter().copied());
    let mut graph_stream_blocker_messages = Vec::new();
    if !graph_stream_validation.is_valid {
        graph_stream_blocker_messages.push("graph stream validation failed");
    }
    blockers.extend(graph_stream_blocker_messages.iter().copied());
    let mut relational_stream_blocker_messages = Vec::new();
    if !relational_stream_validation.is_valid {
        relational_stream_blocker_messages.push("relational stream validation failed");
    }
    blockers.extend(relational_stream_blocker_messages.iter().copied());
    let decision = if blockers.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    let mut bundle = serde_json::json!({
        "protocol": "skein-lightning-bootstrap-bundle",
        "manifest": skein_lightning_bootstrap_manifest_json(&export.manifest),
        "graph_stream_validation": skein_lightning_graph_stream_validation_json(&graph_stream_validation),
        "relational_stream_validation": skein_lightning_relational_stream_validation_json(&relational_stream_validation),
        "export_gate": {
            "decision": decision,
            "manifest_blockers": manifest_blocker_messages.len(),
            "graph_stream_blockers": graph_stream_blocker_messages.len(),
            "relational_stream_blockers": relational_stream_blocker_messages.len(),
            "manifest_blocker_messages": manifest_blocker_messages,
            "graph_stream_blocker_messages": graph_stream_blocker_messages,
            "relational_stream_blocker_messages": relational_stream_blocker_messages,
            "blockers": blockers,
        },
    });
    if let Some(storage_recovery) = storage_recovery {
        bundle
            .as_object_mut()
            .expect("bootstrap bundle JSON must be an object")
            .insert("storage_recovery".to_string(), storage_recovery);
    }
    bundle
}

pub fn skein_lightning_graph_stream_validation_json(
    validation: &SkeinLightningGraphStreamValidation,
) -> serde_json::Value {
    serde_json::json!({
        "is_valid": validation.is_valid,
        "checksum_matches": validation.checksum_matches,
        "format_version_matches": validation.format_version_matches,
        "count_matches": validation.count_matches,
        "endpoint_integrity": validation.endpoint_integrity,
        "manifest_matches": validation.manifest_matches,
        "expected_stream_checksum": validation.expected_stream_checksum,
        "actual_stream_checksum": validation.actual_stream_checksum,
        "format_version": validation.format_version,
        "graph_commit_epoch": validation.graph_commit_epoch,
        "logical_checksum": validation.logical_checksum,
        "node_count": validation.node_count,
        "relationship_count": validation.relationship_count,
        "duplicate_node_ids": validation.duplicate_node_ids,
        "duplicate_relationship_ids": validation.duplicate_relationship_ids,
        "missing_sources": endpoint_violations_json(&validation.missing_sources),
        "missing_targets": endpoint_violations_json(&validation.missing_targets),
        "errors": validation.errors,
    })
}

pub fn skein_lightning_relational_stream_validation_json(
    validation: &SkeinLightningRelationalStreamValidation,
) -> serde_json::Value {
    serde_json::json!({
        "is_valid": validation.is_valid,
        "checksum_matches": validation.checksum_matches,
        "format_version_matches": validation.format_version_matches,
        "epoch_matches": validation.epoch_matches,
        "count_matches": validation.count_matches,
        "manifest_matches": validation.manifest_matches,
        "expected_stream_checksum": validation.expected_stream_checksum,
        "actual_stream_checksum": validation.actual_stream_checksum,
        "database_commit_epoch": validation.database_commit_epoch,
        "table_count": validation.table_count,
        "row_count": validation.row_count,
        "overflow_segment_count": validation.overflow_segment_count,
        "errors": validation.errors,
    })
}

pub fn stable_identity_audit_json(audit: &CanonicalSnapshotIdentityAudit) -> serde_json::Value {
    serde_json::json!({
        "requires_stable_id_mapping": audit.requires_stable_id_mapping,
        "nodes_without_stable_id": audit.nodes_without_stable_id,
        "relationships_without_stable_id": audit.relationships_without_stable_id,
        "duplicate_node_stable_ids": audit.duplicate_node_stable_ids.iter().map(bootstrap_value_json).collect::<Vec<_>>(),
        "duplicate_relationship_stable_ids": audit.duplicate_relationship_stable_ids.iter().map(bootstrap_value_json).collect::<Vec<_>>(),
    })
}

pub fn endpoint_violations_json(
    violations: &[CanonicalSnapshotEndpointViolation],
) -> serde_json::Value {
    serde_json::Value::Array(
        violations
            .iter()
            .map(|violation| {
                serde_json::json!({
                    "relationship_id": violation.relationship_id,
                    "missing_node_id": violation.missing_node_id,
                })
            })
            .collect(),
    )
}

fn bootstrap_value_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(*value),
        Value::Int(value) => serde_json::json!(value),
        Value::Float(value) => serde_json::json!(value),
        Value::String(value) => serde_json::Value::String(value.clone()),
        Value::Binary(value) => serde_json::json!({
            "$binary": value.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        }),
        Value::Uuid(value) => serde_json::json!({ "$uuid": value.to_string() }),
        Value::List(values) => {
            serde_json::Value::Array(values.iter().map(bootstrap_value_json).collect())
        }
        Value::Map(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), bootstrap_value_json(value)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_identity_json_preserves_canonical_value_encoding() {
        let audit = CanonicalSnapshotIdentityAudit {
            requires_stable_id_mapping: true,
            nodes_without_stable_id: vec![1],
            relationships_without_stable_id: vec![2],
            duplicate_node_stable_ids: vec![Value::Binary(vec![0xab])],
            duplicate_relationship_stable_ids: vec![Value::Uuid(
                skein_core::Uuid::parse_str("018f4e6a-7c1b-7cc8-8f4d-1234567890ab")
                    .expect("parse UUID"),
            )],
        };

        let json = stable_identity_audit_json(&audit);

        assert_eq!(
            json["duplicate_node_stable_ids"],
            serde_json::json!([{ "$binary": "ab" }])
        );
        assert_eq!(
            json["duplicate_relationship_stable_ids"][0]["$uuid"],
            "018f4e6a-7c1b-7cc8-8f4d-1234567890ab"
        );
    }
}
