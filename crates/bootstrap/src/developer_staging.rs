//! Developer-only staging artifact writer for portable bootstrap exports.
//!
//! Hosts supply a validated export and may attach already-redacted storage
//! evidence. This module never opens an embedded database or reads host state.

use crate::{
    skein_lightning_bootstrap_bundle_json_with_optional_storage_recovery,
    skein_lightning_bootstrap_manifest_json, skein_lightning_graph_stream_validation_json,
    skein_lightning_relational_stream_validation_json, SkeinLightningBootstrapExport,
};
use skein_core::Result;
use skein_integrity::checksum_u64;
use skein_storage::{durable_replace_file, sync_directory};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

pub const SKEIN_LIGHTNING_STAGING_CATALOG_PROTOCOL_VERSION: u64 = 1;

pub fn stage_skein_lightning_bootstrap_export(
    export: &SkeinLightningBootstrapExport,
    staging_dir: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    stage_skein_lightning_bootstrap_export_with_optional_storage_recovery(export, staging_dir, None)
}

pub fn stage_skein_lightning_bootstrap_export_with_optional_storage_recovery(
    export: &SkeinLightningBootstrapExport,
    staging_dir: impl AsRef<Path>,
    storage_recovery: Option<serde_json::Value>,
) -> Result<serde_json::Value> {
    let staging_dir = staging_dir.as_ref();
    fs::create_dir_all(staging_dir)?;
    let bundle = skein_lightning_bootstrap_bundle_json_with_optional_storage_recovery(
        export,
        storage_recovery,
    );
    let manifest = skein_lightning_bootstrap_manifest_json(&export.manifest);
    let graph_stream_validation = export
        .graph_stream
        .validate_against_manifest(&export.manifest);
    let relational_stream_validation = export
        .relational_stream
        .validate_against_manifest(&export.manifest);
    let stage_state = if bundle
        .get("export_gate")
        .and_then(|gate| gate.get("decision"))
        .and_then(serde_json::Value::as_str)
        == Some("ready")
    {
        "READY"
    } else {
        "QUARANTINED"
    };
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).unwrap();
    let graph_stream_bytes = export.graph_stream.encoded.as_bytes();
    let relational_stream_bytes = export.relational_stream.encoded.as_slice();
    let bundle_bytes = serde_json::to_vec_pretty(&bundle).unwrap();
    let manifest_artifact = write_staging_artifact(
        staging_dir,
        "skein_lightning_bootstrap_manifest.json",
        &manifest_bytes,
    )?;
    let graph_stream_artifact = write_staging_artifact(
        staging_dir,
        "skein_lightning_graph_stream.txt",
        graph_stream_bytes,
    )?;
    let relational_stream_artifact = write_staging_artifact(
        staging_dir,
        "skein_lightning_relational_stream.bin",
        relational_stream_bytes,
    )?;
    let bundle_artifact = write_staging_artifact(
        staging_dir,
        "skein_lightning_bootstrap_bundle.json",
        &bundle_bytes,
    )?;
    let artifacts = vec![
        manifest_artifact,
        graph_stream_artifact,
        relational_stream_artifact,
        bundle_artifact,
    ];
    let artifact_summary = skein_lightning_artifact_summary(&artifacts, "byte_len");
    let catalog = serde_json::json!({
        "protocol": "skein-lightning-staging-catalog",
        "protocol_version": SKEIN_LIGHTNING_STAGING_CATALOG_PROTOCOL_VERSION,
        "stage_state": stage_state,
        "database_commit_epoch": export.manifest.database_commit_epoch,
        "graph_commit_epoch": export.manifest.graph_commit_epoch,
        "logical_checksum": export.manifest.logical_checksum,
        "schema_checksum": export.manifest.schema_checksum,
        "export_gate": bundle["export_gate"].clone(),
        "artifact_summary": artifact_summary,
        "artifacts": artifacts,
        "graph_stream_validation": skein_lightning_graph_stream_validation_json(&graph_stream_validation),
        "relational_stream_validation": skein_lightning_relational_stream_validation_json(&relational_stream_validation),
    });
    let catalog_bytes = serde_json::to_vec_pretty(&catalog).unwrap();
    write_staging_artifact(
        staging_dir,
        "skein_lightning_staging_catalog.json",
        &catalog_bytes,
    )?;
    sync_bootstrap_directory(staging_dir)?;
    Ok(catalog)
}

pub fn write_bootstrap_atomic_file(
    directory: impl AsRef<Path>,
    file_name: &str,
    bytes: &[u8],
) -> Result<()> {
    let directory = directory.as_ref();
    let path = directory.join(file_name);
    let temporary_path = directory.join(format!("{file_name}.tmp"));
    {
        let mut file = File::create(&temporary_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    durable_replace_file(&temporary_path, &path)?;
    Ok(())
}

pub fn sync_bootstrap_directory(path: impl AsRef<Path>) -> Result<()> {
    sync_directory(path.as_ref())?;
    Ok(())
}

pub fn skein_lightning_artifact_summary(
    artifacts: &[serde_json::Value],
    byte_len_field: &str,
) -> serde_json::Value {
    let mut kind_counts = BTreeMap::new();
    let mut total_byte_len = 0u64;
    let mut measured_object_count = 0usize;
    for artifact in artifacts {
        if let Some(kind) = artifact.get("kind").and_then(serde_json::Value::as_str) {
            *kind_counts.entry(kind.to_string()).or_insert(0usize) += 1;
        }
        if let Some(byte_len) = artifact
            .get(byte_len_field)
            .and_then(serde_json::Value::as_u64)
        {
            total_byte_len = total_byte_len.saturating_add(byte_len);
            measured_object_count += 1;
        }
    }
    let average_byte_len = if measured_object_count == 0 {
        serde_json::Value::Null
    } else {
        serde_json::json!(total_byte_len as f64 / measured_object_count as f64)
    };
    serde_json::json!({
        "object_count": artifacts.len(),
        "measured_object_count": measured_object_count,
        "missing_byte_len_count": artifacts.len().saturating_sub(measured_object_count),
        "total_byte_len": total_byte_len,
        "average_byte_len": average_byte_len,
        "kind_counts": kind_counts,
    })
}

fn write_staging_artifact(
    staging_dir: &Path,
    file_name: &str,
    bytes: &[u8],
) -> Result<serde_json::Value> {
    write_bootstrap_atomic_file(staging_dir, file_name, bytes)?;
    Ok(serde_json::json!({
        "kind": skein_lightning_artifact_kind(file_name),
        "path": file_name,
        "byte_len": bytes.len(),
        "checksum": checksum_u64(bytes),
    }))
}

fn skein_lightning_artifact_kind(file_name: &str) -> &'static str {
    match file_name {
        "skein_lightning_bootstrap_manifest.json" => "manifest",
        "skein_lightning_graph_stream.txt" => "graph_stream",
        "skein_lightning_relational_stream.bin" => "relational_stream",
        "skein_lightning_bootstrap_bundle.json" => "bundle",
        "skein_lightning_staging_catalog.json" => "staging_catalog",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::skein_lightning_artifact_summary;

    #[test]
    fn artifact_summary_preserves_missing_and_measured_lengths() {
        let artifacts = vec![
            serde_json::json!({"kind": "manifest", "byte_len": 4}),
            serde_json::json!({"kind": "bundle"}),
            serde_json::json!({"kind": "manifest", "byte_len": 8}),
        ];

        let summary = skein_lightning_artifact_summary(&artifacts, "byte_len");

        assert_eq!(summary["object_count"], 3);
        assert_eq!(summary["measured_object_count"], 2);
        assert_eq!(summary["missing_byte_len_count"], 1);
        assert_eq!(summary["total_byte_len"], 12);
        assert_eq!(summary["average_byte_len"], 6.0);
        assert_eq!(
            summary["kind_counts"],
            serde_json::json!({"bundle": 1, "manifest": 2})
        );
    }
}
