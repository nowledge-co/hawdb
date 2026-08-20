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

pub(super) fn parse_canonical_segment_descriptor_generation_file(name: &str) -> Option<u64> {
    parse_hyphenated_generation_file(name, "canonical-segment-descriptors-", ".pages.skein")
        .or_else(|| {
            parse_hyphenated_generation_file(name, "canonical-segment-descriptors-", ".root.skein")
        })
}

pub(super) fn parse_canonical_adjacency_descriptor_generation_file(name: &str) -> Option<u64> {
    parse_hyphenated_generation_file(name, "adjacency-descriptors-", ".pages.skein")
        .or_else(|| parse_hyphenated_generation_file(name, "adjacency-descriptors-", ".root.skein"))
}

pub(super) fn parse_property_spill_manifest_generation_file(name: &str) -> Option<u64> {
    name.strip_prefix("properties.")?
        .strip_suffix(".manifest.skein")?
        .parse()
        .ok()
}

pub(super) fn parse_property_spill_descriptor_generation_file(name: &str) -> Option<u64> {
    parse_hyphenated_generation_file(name, "property-spill-descriptors-", ".pages.skein").or_else(
        || parse_hyphenated_generation_file(name, "property-spill-descriptors-", ".root.skein"),
    )
}

pub(super) fn parse_property_projection_manifest_generation_file(name: &str) -> Option<u64> {
    name.strip_prefix("property-index.")?
        .strip_suffix(".manifest.skein")?
        .parse()
        .ok()
}

pub(super) fn parse_property_projection_descriptor_generation_file(name: &str) -> Option<u64> {
    parse_hyphenated_generation_file(name, "property-index-descriptors-", ".pages.skein").or_else(
        || parse_hyphenated_generation_file(name, "property-index-descriptors-", ".root.skein"),
    )
}

pub(super) fn parse_relational_index_artifact_generation_file(name: &str) -> Option<u64> {
    name.strip_prefix("relational-index-shadow-")?
        .strip_suffix(".pages.skein")?
        .parse()
        .ok()
}

pub(super) fn parse_relational_index_manifest_generation_file(name: &str) -> Option<u64> {
    name.strip_prefix("relational-index-shadow-")?
        .strip_suffix(".manifest.skein")?
        .parse()
        .ok()
}

fn parse_hyphenated_generation_file(name: &str, prefix: &str, suffix: &str) -> Option<u64> {
    name.strip_prefix(prefix)?
        .strip_suffix(suffix)?
        .parse()
        .ok()
}

pub(super) fn parse_relational_row_generation_file(name: &str) -> Option<u64> {
    parse_hyphenated_generation_file(name, "relational-row-pages-", ".pages.skein")
        .or_else(|| {
            parse_hyphenated_generation_file(name, "relational-row-root-", ".descriptors.skein")
        })
        .or_else(|| parse_hyphenated_generation_file(name, "relational-row-root-", ".keys.skein"))
        .or_else(|| {
            parse_hyphenated_generation_file(name, "relational-row-pages-", ".manifest.skein")
        })
}

pub(super) fn parse_relational_row_page_artifact_generation_file(name: &str) -> Option<u64> {
    parse_hyphenated_generation_file(name, "relational-row-pages-", ".pages.skein")
}

pub(super) fn parse_relational_overflow_generation_file(name: &str) -> Option<u64> {
    parse_hyphenated_generation_file(name, "relational-overflow-", ".extents.skein")
        .or_else(|| {
            parse_hyphenated_generation_file(
                name,
                "relational-overflow-root-",
                ".descriptors.skein",
            )
        })
        .or_else(|| {
            parse_hyphenated_generation_file(name, "relational-overflow-", ".manifest.skein")
        })
}

pub(super) fn parse_relational_overflow_extent_generation_file(name: &str) -> Option<u64> {
    parse_hyphenated_generation_file(name, "relational-overflow-", ".extents.skein")
}

pub(super) fn parse_append_segment_generation_file(name: &str) -> Option<u64> {
    parse_hyphenated_generation_file(name, "append-", ".segment.skein")
}

pub(super) fn parse_append_manifest_generation_file(name: &str) -> Option<u64> {
    parse_hyphenated_generation_file(name, "append-", ".manifest.skein")
}

pub(super) fn storage_generation_for_file(name: &str) -> Option<u64> {
    parse_generation_file(name, "checkpoint.")
        .or_else(|| parse_generation_file(name, "wal."))
        .or_else(|| parse_generation_file(name, "relational."))
        .or_else(|| parse_generation_file(name, "canonical."))
        .or_else(|| parse_canonical_manifest_generation_file(name))
        .or_else(|| parse_canonical_segment_descriptor_generation_file(name))
        .or_else(|| parse_generation_file(name, "adjacency."))
        .or_else(|| parse_canonical_adjacency_descriptor_generation_file(name))
        .or_else(|| parse_generation_file(name, "properties."))
        .or_else(|| parse_property_spill_manifest_generation_file(name))
        .or_else(|| parse_property_spill_descriptor_generation_file(name))
        .or_else(|| parse_generation_file(name, "property-index."))
        .or_else(|| parse_property_projection_manifest_generation_file(name))
        .or_else(|| parse_property_projection_descriptor_generation_file(name))
        .or_else(|| parse_relational_index_artifact_generation_file(name))
        .or_else(|| parse_relational_index_manifest_generation_file(name))
        .or_else(|| parse_relational_row_generation_file(name))
        .or_else(|| parse_relational_overflow_generation_file(name))
        .or_else(|| parse_append_segment_generation_file(name))
        .or_else(|| parse_append_manifest_generation_file(name))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_descriptor_shadow_files_follow_generation_cleanup() {
        assert_eq!(
            storage_generation_for_file("canonical-segment-descriptors-17.pages.skein"),
            Some(17)
        );
        assert_eq!(
            storage_generation_for_file("canonical-segment-descriptors-17.root.skein"),
            Some(17)
        );
        assert_eq!(
            storage_generation_for_file("adjacency-descriptors-17.pages.skein"),
            Some(17)
        );
        assert_eq!(
            storage_generation_for_file("adjacency-descriptors-17.root.skein"),
            Some(17)
        );
        assert_eq!(
            storage_generation_for_file("adjacency-descriptors-x.root.skein"),
            None
        );
        assert_eq!(
            storage_generation_for_file("property-spill-descriptors-17.pages.skein"),
            Some(17)
        );
        assert_eq!(
            storage_generation_for_file("property-spill-descriptors-17.root.skein"),
            Some(17)
        );
        assert_eq!(
            storage_generation_for_file("append-17.segment.skein"),
            Some(17)
        );
        assert_eq!(
            storage_generation_for_file("append-17.manifest.skein"),
            Some(17)
        );
    }
}
