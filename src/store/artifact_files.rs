//! Naming and parsing helpers for generation-suffixed storage artifact files.

use super::{checksum_bytes, sync_parent_dir, MANIFEST_FILE, RELATIONAL_CHECKPOINT_FILE_PREFIX};
use crate::error::Result;
use skein_storage::StoreId;
use std::fs;
use std::path::Path;

pub(super) fn checkpoint_generation_file(generation: u64) -> String {
    format!("checkpoint.{generation}.skein")
}

pub(super) fn relational_checkpoint_generation_file(generation: u64) -> String {
    format!("{RELATIONAL_CHECKPOINT_FILE_PREFIX}.{generation}.skein")
}

pub(super) fn wal_generation_file(generation: u64) -> String {
    format!("wal.{generation}.skein")
}

pub(super) fn canonical_artifact_generation_file(generation: u64) -> String {
    format!("canonical.{generation}.skein")
}

pub(super) fn canonical_manifest_generation_file(generation: u64) -> String {
    format!("canonical.{generation}.manifest.skein")
}

pub(super) fn canonical_adjacency_artifact_generation_file(generation: u64) -> String {
    format!("adjacency.{generation}.skein")
}

pub(super) fn canonical_adjacency_manifest_generation_file(generation: u64) -> String {
    format!("adjacency.{generation}.manifest.skein")
}

pub(super) fn property_spill_artifact_generation_file(generation: u64) -> String {
    format!("properties.{generation}.skein")
}

pub(super) fn property_spill_manifest_generation_file(generation: u64) -> String {
    format!("properties.{generation}.manifest.skein")
}

pub(super) fn property_projection_artifact_generation_file(generation: u64) -> String {
    format!("property-index.{generation}.skein")
}

pub(super) fn property_projection_manifest_generation_file(generation: u64) -> String {
    format!("property-index.{generation}.manifest.skein")
}

pub(super) fn parse_generation_file(name: &str, prefix: &str) -> Option<u64> {
    name.strip_prefix(prefix)?
        .strip_suffix(".skein")?
        .parse()
        .ok()
}

pub(super) fn parse_canonical_manifest_generation_file(name: &str) -> Option<u64> {
    name.strip_prefix("canonical.")?
        .strip_suffix(".manifest.skein")?
        .parse()
        .ok()
}

pub(super) fn parse_canonical_adjacency_manifest_generation_file(name: &str) -> Option<u64> {
    name.strip_prefix("adjacency.")?
        .strip_suffix(".manifest.skein")?
        .parse()
        .ok()
}

pub(super) fn parse_property_spill_manifest_generation_file(name: &str) -> Option<u64> {
    name.strip_prefix("properties.")?
        .strip_suffix(".manifest.skein")?
        .parse()
        .ok()
}

pub(super) fn parse_property_projection_manifest_generation_file(name: &str) -> Option<u64> {
    name.strip_prefix("property-index.")?
        .strip_suffix(".manifest.skein")?
        .parse()
        .ok()
}

pub(super) fn storage_generation_for_file(name: &str) -> Option<u64> {
    parse_generation_file(name, "checkpoint.")
        .or_else(|| parse_generation_file(name, "wal."))
        .or_else(|| parse_generation_file(name, "relational."))
        .or_else(|| parse_generation_file(name, "canonical."))
        .or_else(|| parse_canonical_manifest_generation_file(name))
        .or_else(|| parse_generation_file(name, "adjacency."))
        .or_else(|| parse_canonical_adjacency_manifest_generation_file(name))
        .or_else(|| parse_generation_file(name, "properties."))
        .or_else(|| parse_property_spill_manifest_generation_file(name))
        .or_else(|| parse_generation_file(name, "property-index."))
        .or_else(|| parse_property_projection_manifest_generation_file(name))
}

fn parse_checkpoint_staging_generation(name: &str) -> Option<u64> {
    name.strip_prefix(".checkpoint.")?
        .strip_suffix(".prepare")?
        .parse()
        .ok()
}

pub(super) fn cleanup_abandoned_checkpoint_preparations(
    root: &Path,
    published_generation: u64,
) -> Result<()> {
    let mut changed = false;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if entry.file_type()?.is_dir()
            && parse_checkpoint_staging_generation(name)
                .is_some_and(|generation| generation > published_generation)
        {
            fs::remove_dir_all(entry.path())?;
            changed = true;
            continue;
        }
        if storage_generation_for_file(name)
            .is_some_and(|generation| generation > published_generation)
        {
            fs::remove_file(entry.path())?;
            changed = true;
        }
    }
    if changed {
        sync_parent_dir(&root.join(MANIFEST_FILE))?;
    }
    Ok(())
}

pub(super) fn has_storage_artifacts(root: &Path) -> Result<bool> {
    for entry in fs::read_dir(root)? {
        let name = entry?.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.ends_with(".skein") || name.ends_with(".skein.tmp") {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn store_id_for_path(root: &Path) -> Result<StoreId> {
    let canonical = fs::canonicalize(root)?;
    let path = canonical.to_string_lossy();
    let lower = checksum_bytes(path.as_bytes());
    let mut salted = Vec::with_capacity(path.len().saturating_add(16));
    salted.extend_from_slice(b"skein-store-id\0");
    salted.extend_from_slice(path.as_bytes());
    let upper = checksum_bytes(&salted);
    Ok(StoreId((u128::from(upper) << 64) | u128::from(lower)))
}
