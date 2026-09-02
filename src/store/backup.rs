//! Backup manifest handling and storage backup validation and restore.

use super::{
    canonical_adjacency_artifact_generation_file, canonical_artifact_generation_file,
    canonical_manifest_generation_file, checkpoint_generation_file, checksum_bytes,
    parse_append_manifest_generation_file, parse_append_segment_generation_file,
    parse_generation_file, parse_relational_index_artifact_generation_file,
    parse_relational_index_manifest_generation_file, parse_relational_overflow_generation_file,
    parse_relational_row_generation_file, property_projection_artifact_generation_file,
    property_projection_manifest_generation_file, property_spill_artifact_generation_file,
    property_spill_manifest_generation_file, read_durable_text_bytes_with_limit,
    relational_checkpoint_generation_file, relational_checkpoint_metadata, source_scan,
    split_checkpoint_checksum, store_id_for_path, sync_parent_dir, wal_generation_file,
    BackupFileEntry, BackupManifest, DurableManifest, BACKUP_MANIFEST_FILE, MANIFEST_FILE,
    PROPERTY_PROJECTION_MANIFEST_MAX_BYTES, PROPERTY_SPILL_MANIFEST_MAX_BYTES,
    STABLE_ID_MAPPING_FILE,
};
use crate::error::{Result, SkeinError};
use skein_integrity::{IntegrityHasher, Sha256Digest};
use skein_storage::{
    append_generation_manifest_file, append_segment_file, decode_relational_checkpoint_file,
    validate_backup_file_name, AppendGenerationReader, AppendPublicationConfig,
    CanonicalAdjacencyConfig, CanonicalAdjacencyReader, CanonicalSegmentConfig,
    CanonicalSegmentManifest, CanonicalSegmentReader, GraphDescriptorKind,
    GraphDescriptorTreeBuildConfig, GraphDescriptorTreeGenerationArtifacts,
    GraphDescriptorTreePaths, GraphDescriptorTreeRootReader, ManifestGeneration,
    PersistentPropertyProjectionConfig, PersistentPropertyProjectionDescriptorTree,
    PersistentPropertyProjectionManifest, PersistentPropertyProjectionReader,
    PersistentPropertySpillDescriptorTree, PropertySpillConfig, PropertySpillManifest,
    PropertySpillReader, RelationalDecodeLimits, RelationalIndexArtifactMetadata,
    RelationalIndexGenerationIdentity, RelationalIndexShadowConfig, RelationalIndexShadowReader,
    SegmentCache, StableIdentityMappingConfig, StableIdentityMappingReader, StorageRestoreReport,
};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;

pub(super) fn validate_new_backup_destination(root: &Path, destination: &Path) -> Result<()> {
    if destination.exists() {
        return Err(SkeinError::Storage(format!(
            "backup destination already exists: {}",
            destination.display()
        )));
    }
    let file_name = destination.file_name().ok_or_else(|| {
        SkeinError::Storage("backup destination must have a file name".to_string())
    })?;
    let parent = destination.parent().ok_or_else(|| {
        SkeinError::Storage("backup destination must have a parent directory".to_string())
    })?;
    let canonical_parent = parent.canonicalize()?;
    let destination = canonical_parent.join(file_name);
    let canonical_root = root.canonicalize()?;
    if destination.starts_with(&canonical_root) {
        return Err(SkeinError::Storage(
            "backup destination cannot be inside the database directory".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn copy_backup_file(
    source: &Path,
    destination: &Path,
    name: &str,
) -> Result<BackupFileEntry> {
    let (encoded_len, encoded_checksum, sha256) = copy_file_with_checksum(source, destination)?;
    Ok(BackupFileEntry {
        name: name.to_string(),
        encoded_len,
        encoded_checksum,
        sha256,
    })
}

pub(super) fn copy_file_with_checksum(
    source: &Path,
    destination: &Path,
) -> Result<(u64, u64, Sha256Digest)> {
    let mut source = File::open(source)?;
    let mut destination = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    let mut integrity = IntegrityHasher::new();
    let mut total = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        destination.write_all(&buffer[..read])?;
        integrity.update(&buffer[..read]);
        total = total
            .checked_add(read as u64)
            .ok_or_else(|| SkeinError::Storage("file byte count overflow".to_string()))?;
    }
    destination.sync_all()?;
    let digest = integrity.finish();
    Ok((total, digest.crc32c.as_u64(), digest.sha256))
}

pub(super) fn file_checksum(path: &Path) -> Result<(u64, u64, Sha256Digest)> {
    let mut file = File::open(path)?;
    let mut integrity = IntegrityHasher::new();
    let mut total = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        integrity.update(&buffer[..read]);
        total = total
            .checked_add(read as u64)
            .ok_or_else(|| SkeinError::Storage("file byte count overflow".to_string()))?;
    }
    let digest = integrity.finish();
    Ok((total, digest.crc32c.as_u64(), digest.sha256))
}

pub(super) fn validate_backup_files(
    root: &Path,
    files: &[BackupFileEntry],
    generation: u64,
) -> Result<()> {
    let names = files
        .iter()
        .map(|file| file.name.as_str())
        .collect::<BTreeSet<_>>();
    let manifest_path = root.join(MANIFEST_FILE);
    let manifest = DurableManifest::load(&manifest_path)?;
    if manifest.checkpoint_generation != Some(generation)
        || manifest.checkpoint_epoch != generation
        || manifest.wal_generation != generation
    {
        return Err(SkeinError::Storage(
            "backup files do not describe one published generation".to_string(),
        ));
    }
    let checkpoint_name = checkpoint_generation_file(generation);
    let wal_name = wal_generation_file(generation);
    for required in [MANIFEST_FILE, checkpoint_name.as_str(), wal_name.as_str()] {
        if !names.contains(required) {
            return Err(SkeinError::Storage(format!(
                "backup is missing required file: {required}"
            )));
        }
    }
    for file in files {
        validate_backup_file_name(&file.name)?;
        let (actual_len, actual_checksum, actual_sha256) = file_checksum(&root.join(&file.name))?;
        if actual_len != file.encoded_len
            || actual_checksum != file.encoded_checksum
            || actual_sha256 != file.sha256
        {
            return Err(SkeinError::Storage(format!(
                "backup file verification failed: {}",
                file.name
            )));
        }
    }
    let checkpoint = files
        .iter()
        .find(|file| file.name == checkpoint_name)
        .expect("required checkpoint must exist");
    if manifest.checkpoint_encoded_len != Some(checkpoint.encoded_len)
        || manifest.checkpoint_encoded_checksum != Some(checkpoint.encoded_checksum)
        || manifest.checkpoint_encoded_sha256 != Some(checkpoint.sha256)
    {
        return Err(SkeinError::Storage(
            "backup checkpoint metadata does not match the durable manifest".to_string(),
        ));
    }
    validate_backup_relational_roots(root, files, manifest)?;
    validate_backup_relational_index_generation(root, files, manifest)?;
    validate_backup_stable_identity(root, &names)?;
    validate_backup_append_generation(root, files, &names, manifest)?;
    let checkpoint_text = read_durable_text_bytes_with_limit(
        &fs::read(root.join(&checkpoint_name))?,
        "checkpoint",
        Some(skein_storage::DEFAULT_MAX_CHECKPOINT_DECODED_BYTES),
    )?;
    let (checkpoint_body, checkpoint_checksum) = split_checkpoint_checksum(&checkpoint_text)?;
    if checkpoint_checksum != checksum_bytes(checkpoint_body.as_bytes()) {
        return Err(SkeinError::Storage(
            "backup checkpoint logical checksum mismatch".to_string(),
        ));
    }
    let relational_name = relational_checkpoint_generation_file(generation);
    match relational_checkpoint_metadata(checkpoint_body)? {
        Some(metadata) => {
            let relational = files
                .iter()
                .find(|file| file.name == relational_name)
                .ok_or_else(|| {
                    SkeinError::Storage(format!(
                        "backup is missing required relational checkpoint: {relational_name}"
                    ))
                })?;
            if relational.encoded_len != metadata.encoded_len
                || relational.encoded_checksum != metadata.encoded_checksum
                || relational.sha256 != metadata.encoded_sha256
            {
                return Err(SkeinError::Storage(
                    "backup relational checkpoint metadata does not match the generation checkpoint"
                        .to_string(),
                ));
            }
            let max_bytes = RelationalDecodeLimits::checkpoint().max_record_bytes;
            if relational.encoded_len > max_bytes as u64 {
                return Err(SkeinError::Storage(format!(
                    "backup relational checkpoint contains {} bytes, exceeding max_record_bytes {max_bytes}",
                    relational.encoded_len
                )));
            }
            let relational_checkpoint = decode_relational_checkpoint_file(
                &root.join(&relational_name),
                RelationalDecodeLimits::checkpoint(),
            )
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
            if relational_checkpoint.epoch != manifest.checkpoint_commit_epoch {
                return Err(SkeinError::Storage(format!(
                    "backup relational checkpoint epoch {} does not match checkpoint commit epoch {}",
                    relational_checkpoint.epoch, manifest.checkpoint_commit_epoch
                )));
            }
        }
        None if names.contains(relational_name.as_str()) => {
            return Err(SkeinError::Storage(
                "backup contains an unreferenced relational checkpoint".to_string(),
            ));
        }
        None => {}
    }
    let property_spills =
        validate_backup_property_spills(root, files, &names, manifest, generation)?;
    if let (Some(expected_len), Some(expected_checksum)) = (
        manifest.canonical_manifest_encoded_len,
        manifest.canonical_manifest_encoded_checksum,
    ) {
        let canonical_manifest_name = canonical_manifest_generation_file(generation);
        let canonical_artifact_name = canonical_artifact_generation_file(generation);
        let descriptor_page_name =
            skein_storage::canonical_segment_descriptor_page_file(generation);
        let descriptor_root_name =
            skein_storage::canonical_segment_descriptor_root_file(generation);
        for required in [
            canonical_manifest_name.as_str(),
            canonical_artifact_name.as_str(),
            descriptor_page_name.as_str(),
            descriptor_root_name.as_str(),
        ] {
            if !names.contains(required) {
                return Err(SkeinError::Storage(format!(
                    "backup is missing required canonical file: {required}"
                )));
            }
        }
        let encoded_manifest = files
            .iter()
            .find(|file| file.name == canonical_manifest_name)
            .expect("required canonical manifest must exist");
        if encoded_manifest.encoded_len != expected_len
            || encoded_manifest.encoded_checksum != expected_checksum
            || manifest.canonical_manifest_encoded_sha256 != Some(encoded_manifest.sha256)
        {
            return Err(SkeinError::Storage(
                "backup canonical manifest metadata does not match the durable manifest"
                    .to_string(),
            ));
        }
        let canonical_manifest_text = fs::read_to_string(root.join(&canonical_manifest_name))?;
        let canonical_manifest = CanonicalSegmentManifest::decode(&canonical_manifest_text)
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        if canonical_manifest.generation != ManifestGeneration(generation)
            || canonical_manifest.source_commit_epoch != manifest.checkpoint_commit_epoch
        {
            return Err(SkeinError::Storage(
                "backup canonical descriptor identity does not match its checkpoint".to_string(),
            ));
        }
        let canonical_artifact = files
            .iter()
            .find(|file| file.name == canonical_artifact_name)
            .expect("required canonical artifact must exist");
        if canonical_manifest.artifact_len != canonical_artifact.encoded_len
            || canonical_manifest.artifact_digest.0 != canonical_artifact.encoded_checksum
            || canonical_manifest.artifact_sha256 != canonical_artifact.sha256
        {
            return Err(SkeinError::Storage(
                "backup canonical artifact metadata does not match its manifest".to_string(),
            ));
        }
        let descriptor_root = files
            .iter()
            .find(|file| file.name == descriptor_root_name)
            .expect("required canonical descriptor root must exist");
        if descriptor_root.encoded_len != canonical_manifest.descriptor_root_artifact.encoded_len
            || descriptor_root.encoded_checksum
                != u64::from(canonical_manifest.descriptor_root_artifact.encoded_crc32c)
            || descriptor_root.sha256 != canonical_manifest.descriptor_root_artifact.encoded_sha256
        {
            return Err(SkeinError::Storage(
                "backup canonical descriptor root does not match its manifest".to_string(),
            ));
        }
        let descriptor_config = GraphDescriptorTreeBuildConfig::default();
        let descriptor_paths = GraphDescriptorTreePaths::new(
            root.join(&descriptor_page_name),
            root.join(&descriptor_root_name),
        );
        let root_reader = GraphDescriptorTreeRootReader::open_bound(
            descriptor_paths,
            canonical_manifest.descriptor_generation_artifacts(),
            descriptor_config,
        )
        .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
        if root_reader.root().descriptor_count != canonical_manifest.segment_count {
            return Err(SkeinError::Storage(
                "backup canonical descriptor count does not match its manifest".to_string(),
            ));
        }
        let descriptor_page = files
            .iter()
            .find(|file| file.name == descriptor_page_name)
            .expect("required canonical descriptor pages must exist");
        if descriptor_page.encoded_len != root_reader.root().page_artifact_len
            || descriptor_page.encoded_checksum != root_reader.root().page_artifact_crc32c.as_u64()
            || descriptor_page.sha256 != root_reader.root().page_artifact_sha256
        {
            return Err(SkeinError::Storage(
                "backup canonical descriptor pages do not match their root".to_string(),
            ));
        }
        let config = CanonicalSegmentConfig::default();
        let max_segment_bytes = NonZeroU64::new(
            config
                .target_segment_bytes
                .get()
                .max(config.max_record_bytes.get().saturating_add(64)),
        )
        .expect("canonical segment maximum is non-zero");
        let cache = Arc::new(SegmentCache::new(0));
        let store_id = store_id_for_path(root)?;
        let reader = match property_spills.clone() {
            Some(property_spills) => CanonicalSegmentReader::open_with_property_spills(
                root.join(&canonical_artifact_name),
                canonical_manifest,
                cache,
                store_id,
                max_segment_bytes,
                property_spills,
            ),
            None => CanonicalSegmentReader::open(
                root.join(&canonical_artifact_name),
                canonical_manifest,
                cache,
                store_id,
                max_segment_bytes,
            ),
        };
        reader
            .and_then(|reader| reader.deep_scrub())
            .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
    }
    if let Some(binding) = manifest.canonical_adjacency_generation_artifacts {
        let adjacency_artifact_name = canonical_adjacency_artifact_generation_file(generation);
        let descriptor_page_name =
            skein_storage::canonical_adjacency_descriptor_page_file(generation);
        let descriptor_root_name =
            skein_storage::canonical_adjacency_descriptor_root_file(generation);
        for required in [
            adjacency_artifact_name.as_str(),
            descriptor_page_name.as_str(),
            descriptor_root_name.as_str(),
        ] {
            if !names.contains(required) {
                return Err(SkeinError::Storage(format!(
                    "backup is missing required canonical adjacency file: {required}"
                )));
            }
        }
        let descriptor_root = files
            .iter()
            .find(|file| file.name == descriptor_root_name)
            .expect("required canonical adjacency descriptor root must exist");
        if descriptor_root.encoded_len != binding.descriptor_root_artifact.encoded_len
            || descriptor_root.encoded_checksum
                != u64::from(binding.descriptor_root_artifact.encoded_crc32c)
            || descriptor_root.sha256 != binding.descriptor_root_artifact.encoded_sha256
        {
            return Err(SkeinError::Storage(
                "backup canonical adjacency descriptor root does not match the durable manifest"
                    .to_string(),
            ));
        }
        let adjacency_artifact = files
            .iter()
            .find(|file| file.name == adjacency_artifact_name)
            .expect("required canonical adjacency artifact must exist");
        if binding.adjacency_artifact.encoded_len != adjacency_artifact.encoded_len
            || binding.adjacency_artifact.encoded_crc32c != adjacency_artifact.encoded_checksum
            || binding.adjacency_artifact.encoded_sha256 != adjacency_artifact.sha256
        {
            return Err(SkeinError::Storage(
                "backup canonical adjacency artifact metadata does not match its durable binding"
                    .to_string(),
            ));
        }
        let descriptor_config = GraphDescriptorTreeBuildConfig::default();
        let descriptor_paths = GraphDescriptorTreePaths::new(
            root.join(&descriptor_page_name),
            root.join(&descriptor_root_name),
        );
        let root_reader = GraphDescriptorTreeRootReader::open_bound(
            descriptor_paths,
            GraphDescriptorTreeGenerationArtifacts {
                kind: GraphDescriptorKind::CanonicalAdjacency,
                generation: binding.generation,
                source_commit_epoch: binding.source_commit_epoch,
                root_artifact: binding.descriptor_root_artifact,
            },
            descriptor_config,
        )
        .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
        let descriptor_page = files
            .iter()
            .find(|file| file.name == descriptor_page_name)
            .expect("required canonical adjacency descriptor pages must exist");
        if descriptor_page.encoded_len != root_reader.root().page_artifact_len
            || descriptor_page.encoded_checksum != root_reader.root().page_artifact_crc32c.as_u64()
            || descriptor_page.sha256 != root_reader.root().page_artifact_sha256
        {
            return Err(SkeinError::Storage(
                "backup canonical adjacency descriptor pages do not match their root".to_string(),
            ));
        }
        let config = CanonicalAdjacencyConfig::default();
        let max_block_bytes = NonZeroU64::new(
            config
                .target_block_bytes
                .get()
                .max(config.max_record_bytes.get().saturating_add(1024)),
        )
        .expect("canonical adjacency maximum block size is non-zero");
        CanonicalAdjacencyReader::open_demand_paged(
            root.join(&adjacency_artifact_name),
            binding,
            root_reader,
            descriptor_config,
            Arc::new(SegmentCache::new(0)),
            store_id_for_path(root)?,
            max_block_bytes,
        )
        .and_then(|reader| reader.deep_scrub())
        .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
    }
    if let (Some(expected_len), Some(expected_checksum)) = (
        manifest.property_projection_manifest_encoded_len,
        manifest.property_projection_manifest_encoded_checksum,
    ) {
        let projection_manifest_name = property_projection_manifest_generation_file(generation);
        let projection_artifact_name = property_projection_artifact_generation_file(generation);
        let descriptor_page_name =
            skein_storage::property_projection_descriptor_page_file(generation);
        let descriptor_root_name =
            skein_storage::property_projection_descriptor_root_file(generation);
        for required in [
            projection_manifest_name.as_str(),
            projection_artifact_name.as_str(),
            descriptor_page_name.as_str(),
            descriptor_root_name.as_str(),
        ] {
            if !names.contains(required) {
                return Err(SkeinError::Storage(format!(
                    "backup is missing required property projection file: {required}"
                )));
            }
        }
        let encoded_manifest = files
            .iter()
            .find(|file| file.name == projection_manifest_name)
            .expect("required property projection manifest must exist");
        if encoded_manifest.encoded_len != expected_len
            || encoded_manifest.encoded_checksum != expected_checksum
            || manifest.property_projection_manifest_encoded_sha256 != Some(encoded_manifest.sha256)
        {
            return Err(SkeinError::Storage(
                "backup property projection manifest metadata does not match the durable manifest"
                    .to_string(),
            ));
        }
        if encoded_manifest.encoded_len > PROPERTY_PROJECTION_MANIFEST_MAX_BYTES {
            return Err(SkeinError::Storage(format!(
                "backup property projection manifest contains {} bytes, exceeding the {} byte format limit",
                encoded_manifest.encoded_len, PROPERTY_PROJECTION_MANIFEST_MAX_BYTES
            )));
        }
        let projection_manifest_text = fs::read_to_string(root.join(&projection_manifest_name))?;
        let projection_manifest =
            PersistentPropertyProjectionManifest::decode(&projection_manifest_text)
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
        let projection_artifact = files
            .iter()
            .find(|file| file.name == projection_artifact_name)
            .expect("required property projection artifact must exist");
        if projection_manifest.generation != ManifestGeneration(generation)
            || projection_manifest.source_commit_epoch != manifest.checkpoint_commit_epoch
            || projection_manifest.artifact_len != projection_artifact.encoded_len
            || projection_manifest.artifact_digest.0 != projection_artifact.encoded_checksum
            || projection_manifest.artifact_sha256 != projection_artifact.sha256
        {
            return Err(SkeinError::Storage(
                "backup property projection artifact metadata does not match its manifest"
                    .to_string(),
            ));
        }
        let descriptor_paths = GraphDescriptorTreePaths::new(
            root.join(&descriptor_page_name),
            root.join(&descriptor_root_name),
        );
        let descriptor_root = GraphDescriptorTreeRootReader::open_bound(
            descriptor_paths.clone(),
            projection_manifest.descriptor_generation_artifacts(),
            GraphDescriptorTreeBuildConfig::default(),
        )
        .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
        if descriptor_root.root().kind != GraphDescriptorKind::PropertyProjection
            || descriptor_root.root().generation != generation
            || descriptor_root.root().source_commit_epoch != manifest.checkpoint_commit_epoch
            || descriptor_root.root().descriptor_count != projection_manifest.block_count
        {
            return Err(SkeinError::Storage(
                "backup property projection descriptor root identity is inconsistent".to_string(),
            ));
        }
        let descriptor_page = files
            .iter()
            .find(|file| file.name == descriptor_page_name)
            .expect("required property projection descriptor pages must exist");
        if descriptor_page.encoded_len != descriptor_root.root().page_artifact_len
            || descriptor_page.encoded_checksum
                != descriptor_root.root().page_artifact_crc32c.as_u64()
            || descriptor_page.sha256 != descriptor_root.root().page_artifact_sha256
        {
            return Err(SkeinError::Storage(
                "backup property projection descriptor pages do not match their root".to_string(),
            ));
        }
        let config = PersistentPropertyProjectionConfig::default();
        let max_block_bytes = NonZeroU64::new(
            config
                .target_block_bytes
                .get()
                .max(config.max_index_key_bytes.get().saturating_add(1024)),
        )
        .expect("property projection maximum block size is non-zero");
        PersistentPropertyProjectionReader::open(
            root.join(&projection_artifact_name),
            projection_manifest,
            PersistentPropertyProjectionDescriptorTree::new(
                descriptor_paths,
                GraphDescriptorTreeBuildConfig::default(),
            ),
            Arc::new(SegmentCache::new(0)),
            store_id_for_path(root)?,
            max_block_bytes,
        )
        .and_then(|reader| reader.deep_scrub())
        .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
    }
    Ok(())
}

fn validate_backup_stable_identity(root: &Path, names: &BTreeSet<&str>) -> Result<()> {
    let selector_present = names.contains(STABLE_ID_MAPPING_FILE);
    let generation_files = names
        .iter()
        .copied()
        .filter(|name| parse_generation_file(name, "stable_ids.").is_some())
        .collect::<Vec<_>>();
    if !selector_present {
        if generation_files.is_empty() {
            return Ok(());
        }
        return Err(SkeinError::Storage(
            "backup contains a stable identity generation without its selector".to_string(),
        ));
    }
    let reader = StableIdentityMappingReader::open(
        &root.join(STABLE_ID_MAPPING_FILE),
        StableIdentityMappingConfig::default(),
    )
    .map_err(|error| SkeinError::Storage(error.to_string()))?;
    let selected_name = reader
        .artifact_path()
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            SkeinError::Storage(
                "backup stable identity generation artifact name is not UTF-8".to_string(),
            )
        })?;
    if generation_files.as_slice() != [selected_name] {
        return Err(SkeinError::Storage(format!(
            "backup stable identity selector must bind exactly one generation: selected {selected_name}, found {generation_files:?}"
        )));
    }
    Ok(())
}

fn validate_backup_append_generation(
    root: &Path,
    files: &[BackupFileEntry],
    names: &BTreeSet<&str>,
    manifest: DurableManifest,
) -> Result<()> {
    let Some(binding) = manifest.append_generation_artifacts else {
        if files.iter().any(|file| {
            parse_append_segment_generation_file(&file.name).is_some()
                || parse_append_manifest_generation_file(&file.name).is_some()
        }) {
            return Err(SkeinError::Storage(
                "backup contains unreferenced append artifacts".to_string(),
            ));
        }
        return Ok(());
    };

    let manifest_name = append_generation_manifest_file(binding.generation);
    if !names.contains(manifest_name.as_str()) {
        return Err(SkeinError::Storage(format!(
            "backup is missing required append generation manifest: {manifest_name}"
        )));
    }
    let manifest_file = files
        .iter()
        .find(|file| file.name == manifest_name)
        .expect("required append manifest must exist");
    if manifest_file.encoded_len != binding.manifest_artifact.encoded_len
        || manifest_file.encoded_checksum != u64::from(binding.manifest_artifact.encoded_crc32c)
        || manifest_file.sha256 != binding.manifest_artifact.encoded_sha256
    {
        return Err(SkeinError::Storage(
            "backup append manifest metadata does not match the durable manifest".to_string(),
        ));
    }

    let reader =
        AppendGenerationReader::open_bound(root, binding, AppendPublicationConfig::default())
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
    for segment in reader.segment_bindings() {
        let name = append_segment_file(segment.generation);
        if !names.contains(name.as_str()) {
            return Err(SkeinError::Storage(format!(
                "backup is missing required append segment: {name}"
            )));
        }
        let file = files
            .iter()
            .find(|file| file.name == name)
            .expect("required append segment must exist");
        if file.encoded_len != segment.artifact.encoded_len
            || file.encoded_checksum != u64::from(segment.artifact.encoded_crc32c)
            || file.sha256 != segment.artifact.encoded_sha256
        {
            return Err(SkeinError::Storage(format!(
                "backup append segment metadata does not match its manifest: {name}"
            )));
        }
    }
    reader
        .deep_scrub()
        .map_err(|error| SkeinError::Storage(error.to_string()))?;
    Ok(())
}

fn validate_backup_property_spills(
    root: &Path,
    files: &[BackupFileEntry],
    names: &BTreeSet<&str>,
    manifest: DurableManifest,
    generation: u64,
) -> Result<Option<PropertySpillReader>> {
    let (Some(expected_len), Some(expected_checksum), Some(expected_sha256)) = (
        manifest.property_spill_manifest_encoded_len,
        manifest.property_spill_manifest_encoded_checksum,
        manifest.property_spill_manifest_encoded_sha256,
    ) else {
        return Ok(None);
    };
    let property_manifest_name = property_spill_manifest_generation_file(generation);
    let property_artifact_name = property_spill_artifact_generation_file(generation);
    let descriptor_page_name = skein_storage::property_spill_descriptor_page_file(generation);
    let descriptor_root_name = skein_storage::property_spill_descriptor_root_file(generation);
    for required in [
        property_manifest_name.as_str(),
        property_artifact_name.as_str(),
        descriptor_page_name.as_str(),
        descriptor_root_name.as_str(),
    ] {
        if !names.contains(required) {
            return Err(SkeinError::Storage(format!(
                "backup is missing required property spill file: {required}"
            )));
        }
    }
    let encoded_manifest = files
        .iter()
        .find(|file| file.name == property_manifest_name)
        .expect("required property spill manifest must exist");
    if encoded_manifest.encoded_len != expected_len
        || encoded_manifest.encoded_checksum != expected_checksum
        || encoded_manifest.sha256 != expected_sha256
    {
        return Err(SkeinError::Storage(
            "backup property spill manifest metadata does not match the durable manifest"
                .to_string(),
        ));
    }
    if encoded_manifest.encoded_len > PROPERTY_SPILL_MANIFEST_MAX_BYTES {
        return Err(SkeinError::Storage(format!(
            "backup property spill manifest contains {} bytes, exceeding the {} byte format limit",
            encoded_manifest.encoded_len, PROPERTY_SPILL_MANIFEST_MAX_BYTES
        )));
    }
    let property_manifest_text = fs::read_to_string(root.join(&property_manifest_name))?;
    let property_manifest = PropertySpillManifest::decode(&property_manifest_text)
        .map_err(|error| SkeinError::Storage(error.to_string()))?;
    if property_manifest.generation != ManifestGeneration(generation)
        || property_manifest.source_commit_epoch != manifest.checkpoint_commit_epoch
    {
        return Err(SkeinError::Storage(
            "backup property spill identity does not match its checkpoint".to_string(),
        ));
    }
    let property_artifact = files
        .iter()
        .find(|file| file.name == property_artifact_name)
        .expect("required property spill artifact must exist");
    if property_manifest.artifact_len != property_artifact.encoded_len
        || property_manifest.artifact_digest.0 != property_artifact.encoded_checksum
        || property_manifest.artifact_sha256 != property_artifact.sha256
    {
        return Err(SkeinError::Storage(
            "backup property spill artifact metadata does not match its manifest".to_string(),
        ));
    }
    let descriptor_root = files
        .iter()
        .find(|file| file.name == descriptor_root_name)
        .expect("required property spill descriptor root must exist");
    if descriptor_root.encoded_len != property_manifest.descriptor_root_artifact.encoded_len
        || descriptor_root.encoded_checksum
            != u64::from(property_manifest.descriptor_root_artifact.encoded_crc32c)
        || descriptor_root.sha256 != property_manifest.descriptor_root_artifact.encoded_sha256
    {
        return Err(SkeinError::Storage(
            "backup property spill descriptor root does not match its manifest".to_string(),
        ));
    }
    let descriptor_paths = GraphDescriptorTreePaths::new(
        root.join(&descriptor_page_name),
        root.join(&descriptor_root_name),
    );
    let root_reader = GraphDescriptorTreeRootReader::open_bound(
        descriptor_paths.clone(),
        property_manifest.descriptor_generation_artifacts(),
        GraphDescriptorTreeBuildConfig::default(),
    )
    .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
    if root_reader.root().descriptor_count != property_manifest.block_count {
        return Err(SkeinError::Storage(
            "backup property spill descriptor count does not match its manifest".to_string(),
        ));
    }
    let descriptor_page = files
        .iter()
        .find(|file| file.name == descriptor_page_name)
        .expect("required property spill descriptor pages must exist");
    if descriptor_page.encoded_len != root_reader.root().page_artifact_len
        || descriptor_page.encoded_checksum != root_reader.root().page_artifact_crc32c.as_u64()
        || descriptor_page.sha256 != root_reader.root().page_artifact_sha256
    {
        return Err(SkeinError::Storage(
            "backup property spill descriptor pages do not match their root".to_string(),
        ));
    }
    let config = PropertySpillConfig::default();
    let max_block_bytes = NonZeroU64::new(
        config
            .target_block_bytes
            .get()
            .max(config.max_value_bytes.get().saturating_add(1024)),
    )
    .expect("property spill maximum block size is non-zero");
    let reader = PropertySpillReader::open(
        root.join(&property_artifact_name),
        property_manifest,
        PersistentPropertySpillDescriptorTree::new(
            descriptor_paths,
            GraphDescriptorTreeBuildConfig::default(),
        ),
        Arc::new(SegmentCache::new(0)),
        store_id_for_path(root)?,
        max_block_bytes,
    )
    .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
    Ok(Some(reader))
}

fn validate_backup_relational_roots(
    root: &Path,
    files: &[BackupFileEntry],
    manifest: DurableManifest,
) -> Result<()> {
    let row_binding = manifest
        .relational_row_generation_artifacts
        .ok_or_else(|| {
            SkeinError::Storage("backup manifest has no relational row-page binding".to_string())
        })?;
    let overflow_binding = manifest
        .relational_overflow_generation_artifacts
        .ok_or_else(|| {
            SkeinError::Storage("backup manifest has no relational overflow binding".to_string())
        })?;
    let row_files = files
        .iter()
        .filter(|file| parse_relational_row_generation_file(&file.name).is_some())
        .map(|file| file.name.clone())
        .collect::<BTreeSet<_>>();
    let overflow_files = files
        .iter()
        .filter(|file| parse_relational_overflow_generation_file(&file.name).is_some())
        .map(|file| file.name.clone())
        .collect::<BTreeSet<_>>();

    let require = |name: &str| {
        files
            .iter()
            .find(|file| file.name == name)
            .ok_or_else(|| SkeinError::Storage(format!("backup is missing bound file: {name}")))
    };
    let overflow_manifest_name =
        skein_storage::relational_overflow_manifest_generation_file(overflow_binding.generation);
    let overflow_manifest_file = require(&overflow_manifest_name)?;
    if overflow_manifest_file.encoded_len != overflow_binding.manifest_artifact.encoded_len
        || overflow_manifest_file.encoded_checksum
            != u64::from(overflow_binding.manifest_artifact.encoded_crc32c)
        || overflow_manifest_file.sha256 != overflow_binding.manifest_artifact.encoded_sha256
    {
        return Err(SkeinError::Storage(
            "backup relational overflow manifest does not match its canonical binding".to_string(),
        ));
    }
    let overflow = skein_storage::RelationalOverflowRootReader::open_generation(
        root,
        overflow_binding.generation,
        skein_storage::RelationalOverflowPublicationConfig::default(),
    )
    .map_err(|error| SkeinError::Storage(error.to_string()))?;
    if overflow.manifest().source_commit_epoch != overflow_binding.source_commit_epoch
        || overflow.manifest().root_set_digest != overflow_binding.root_set_digest
    {
        return Err(SkeinError::Storage(
            "backup relational overflow identity differs from its canonical binding".to_string(),
        ));
    }
    for (name, metadata) in [
        (
            skein_storage::relational_overflow_extent_file(overflow_binding.generation),
            overflow.manifest().extent_artifact,
        ),
        (
            skein_storage::relational_overflow_descriptor_file(overflow_binding.generation),
            overflow.manifest().descriptor_artifact,
        ),
    ] {
        let file = require(&name)?;
        if file.encoded_len != metadata.encoded_len
            || file.encoded_checksum != u64::from(metadata.encoded_crc32c)
            || file.sha256 != metadata.encoded_sha256
        {
            return Err(SkeinError::Storage(format!(
                "backup relational overflow artifact does not match its generation manifest: {name}"
            )));
        }
    }
    let mut expected_overflow_files = BTreeSet::from([
        overflow_manifest_name,
        skein_storage::relational_overflow_descriptor_file(overflow_binding.generation),
        skein_storage::relational_overflow_extent_file(overflow_binding.generation),
    ]);
    overflow
        .visit_descriptors(|descriptor| {
            expected_overflow_files.insert(skein_storage::relational_overflow_extent_file(
                descriptor.physical_generation,
            ));
            overflow.hydrate(
                &descriptor.reference,
                &mut skein_storage::RelationalHydrationBudget::default(),
                None,
            )?;
            Ok(())
        })
        .map_err(|error| SkeinError::Storage(error.to_string()))?;
    if overflow_files != expected_overflow_files {
        return Err(SkeinError::Storage(
            "backup relational overflow files do not match the bound physical closure".to_string(),
        ));
    }

    let row_manifest_name =
        skein_storage::relational_row_page_manifest_generation_file(row_binding.generation);
    let row_manifest_file = require(&row_manifest_name)?;
    if row_manifest_file.encoded_len != row_binding.manifest_artifact.encoded_len
        || row_manifest_file.encoded_checksum
            != u64::from(row_binding.manifest_artifact.encoded_crc32c)
        || row_manifest_file.sha256 != row_binding.manifest_artifact.encoded_sha256
    {
        return Err(SkeinError::Storage(
            "backup relational row-page manifest does not match its canonical binding".to_string(),
        ));
    }
    let row = skein_storage::RelationalRowPageRootReader::open_generation(
        root,
        row_binding.generation,
        skein_storage::RelationalRowPagePublicationConfig::default(),
    )
    .map_err(|error| SkeinError::Storage(error.to_string()))?;
    if row.manifest().source_commit_epoch != row_binding.source_commit_epoch
        || row.manifest().root_set_digest != row_binding.root_set_digest
    {
        return Err(SkeinError::Storage(
            "backup relational row-page identity differs from its canonical binding".to_string(),
        ));
    }
    row.validate_overflow_root(&overflow)
        .map_err(|error| SkeinError::Storage(error.to_string()))?;
    for (name, metadata) in [
        (
            skein_storage::relational_row_page_artifact_file(row_binding.generation),
            row.manifest().page_artifact,
        ),
        (
            skein_storage::relational_row_page_root_descriptor_file(row_binding.generation),
            row.manifest().root_descriptor_artifact,
        ),
        (
            skein_storage::relational_row_page_root_key_file(row_binding.generation),
            row.manifest().root_key_artifact,
        ),
    ] {
        let file = require(&name)?;
        if file.encoded_len != metadata.encoded_len
            || file.encoded_checksum != u64::from(metadata.encoded_crc32c)
            || file.sha256 != metadata.encoded_sha256
        {
            return Err(SkeinError::Storage(format!(
                "backup relational row-page artifact does not match its generation manifest: {name}"
            )));
        }
    }
    let mut expected_row_files = BTreeSet::from([
        row_manifest_name,
        skein_storage::relational_row_page_root_descriptor_file(row_binding.generation),
        skein_storage::relational_row_page_root_key_file(row_binding.generation),
        skein_storage::relational_row_page_artifact_file(row_binding.generation),
    ]);
    let tables = row
        .manifest()
        .tables
        .iter()
        .map(|table| table.table.clone())
        .collect::<Vec<_>>();
    for table in tables {
        row.visit_table_pages(&table, |descriptor| {
            expected_row_files.insert(skein_storage::relational_row_page_artifact_file(
                descriptor.physical_generation,
            ));
            row.read_page(descriptor)?;
            Ok(())
        })
        .map_err(|error| SkeinError::Storage(error.to_string()))?;
    }
    if row_files != expected_row_files {
        return Err(SkeinError::Storage(
            "backup relational row-page files do not match the bound physical closure".to_string(),
        ));
    }
    Ok(())
}

fn validate_backup_relational_index_generation(
    root: &Path,
    files: &[BackupFileEntry],
    manifest: DurableManifest,
) -> Result<()> {
    let relational_index_files = files
        .iter()
        .filter(|file| {
            parse_relational_index_artifact_generation_file(&file.name).is_some()
                || parse_relational_index_manifest_generation_file(&file.name).is_some()
        })
        .collect::<Vec<_>>();
    let Some(binding) = manifest.relational_index_generation_artifacts else {
        if relational_index_files.is_empty() {
            return Ok(());
        }
        return Err(SkeinError::Storage(
            "backup contains relational index files without a canonical manifest binding"
                .to_string(),
        ));
    };
    let page_name = skein_storage::relational_index_shadow_artifact_file(binding.generation);
    let generation_manifest_name =
        skein_storage::relational_index_shadow_manifest_generation_file(binding.generation);
    let page = files
        .iter()
        .find(|file| file.name == page_name)
        .ok_or_else(|| {
            SkeinError::Storage(format!(
                "backup is missing bound relational index page artifact: {page_name}"
            ))
        })?;
    let generation_manifest = files
        .iter()
        .find(|file| file.name == generation_manifest_name)
        .ok_or_else(|| {
            SkeinError::Storage(format!(
                "backup is missing bound relational index generation manifest: {generation_manifest_name}"
            ))
        })?;
    if relational_index_files.len() != 2 {
        return Err(SkeinError::Storage(
            "backup relational index files do not match the single canonical generation binding"
                .to_string(),
        ));
    }
    validate_backup_relational_index_artifact(page, binding.page_artifact, "page artifact")?;
    validate_backup_relational_index_artifact(
        generation_manifest,
        binding.manifest_artifact,
        "generation manifest",
    )?;
    let reader = RelationalIndexShadowReader::open_generation(
        root,
        RelationalIndexGenerationIdentity {
            generation: binding.generation,
            source_commit_epoch: binding.source_commit_epoch,
        },
        RelationalIndexShadowConfig::default(),
    )
    .map_err(|error| SkeinError::Storage(error.to_string()))?;
    if reader.manifest().catalog_schema_digest != binding.catalog_schema_digest
        || reader.manifest().root_set_digest != binding.root_set_digest
    {
        return Err(SkeinError::Storage(
            "backup relational index manifest digests do not match the canonical binding"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_backup_relational_index_artifact(
    file: &BackupFileEntry,
    expected: RelationalIndexArtifactMetadata,
    artifact: &str,
) -> Result<()> {
    if file.encoded_len != expected.encoded_len
        || file.encoded_checksum != expected.encoded_crc32c
        || file.sha256 != expected.encoded_sha256
    {
        return Err(SkeinError::Storage(format!(
            "backup relational index {artifact} does not match the canonical binding"
        )));
    }
    Ok(())
}

pub fn restore_storage_backup(
    backup: impl AsRef<Path>,
    destination: impl AsRef<Path>,
) -> Result<StorageRestoreReport> {
    let backup = backup.as_ref();
    let destination = destination.as_ref();
    let backup_manifest = BackupManifest::load(&backup.join(BACKUP_MANIFEST_FILE))?;
    validate_backup_files(backup, &backup_manifest.files, backup_manifest.generation)?;
    let durable_manifest = DurableManifest::load(&backup.join(MANIFEST_FILE))?;
    if durable_manifest.checkpoint_commit_epoch != backup_manifest.checkpoint_commit_epoch {
        return Err(SkeinError::Storage(
            "backup checkpoint commit epoch does not match the durable manifest".to_string(),
        ));
    }
    validate_new_backup_destination(backup, destination)?;
    fs::create_dir(destination)?;

    let result = (|| {
        for entry in backup_manifest
            .files
            .iter()
            .filter(|entry| entry.name != MANIFEST_FILE)
        {
            let (encoded_len, encoded_checksum, sha256) =
                copy_file_with_checksum(&backup.join(&entry.name), &destination.join(&entry.name))?;
            if encoded_len != entry.encoded_len
                || encoded_checksum != entry.encoded_checksum
                || sha256 != entry.sha256
            {
                return Err(SkeinError::Storage(format!(
                    "backup file changed while restoring: {}",
                    entry.name
                )));
            }
        }
        sync_parent_dir(&destination.join(MANIFEST_FILE))?;
        let manifest_entry = backup_manifest
            .files
            .iter()
            .find(|entry| entry.name == MANIFEST_FILE)
            .expect("validated backup must contain durable manifest");
        let (encoded_len, encoded_checksum, sha256) = copy_file_with_checksum(
            &backup.join(MANIFEST_FILE),
            &destination.join(MANIFEST_FILE),
        )?;
        if encoded_len != manifest_entry.encoded_len
            || encoded_checksum != manifest_entry.encoded_checksum
            || sha256 != manifest_entry.sha256
        {
            return Err(SkeinError::Storage(
                "backup manifest file changed while restoring".to_string(),
            ));
        }
        sync_parent_dir(&destination.join(MANIFEST_FILE))?;
        let total_bytes = backup_manifest
            .files
            .iter()
            .try_fold(0u64, |total, file| total.checked_add(file.encoded_len))
            .ok_or_else(|| SkeinError::Storage("restore byte count overflow".to_string()))?;
        Ok(StorageRestoreReport {
            generation: backup_manifest.generation,
            checkpoint_commit_epoch: backup_manifest.checkpoint_commit_epoch,
            file_count: backup_manifest.files.len(),
            total_bytes,
            manifest_checksum: backup_manifest.checksum,
        })
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(destination);
    }
    result
}

pub(super) fn remove_source_scan_artifacts(path: &Path) -> Result<()> {
    for file in [
        source_scan::SOURCE_SCAN_DESCRIPTOR_FILE,
        source_scan::SOURCE_SCAN_PAYLOAD_FILE,
    ] {
        let artifact = path.join(file);
        match fs::remove_file(&artifact) {
            Ok(()) => sync_parent_dir(&artifact)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
