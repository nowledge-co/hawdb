//! Backup manifest handling and storage backup validation and restore.

use super::{
    canonical_adjacency_artifact_generation_file, canonical_adjacency_manifest_generation_file,
    canonical_artifact_generation_file, canonical_manifest_generation_file,
    checkpoint_generation_file, checksum_bytes, decode_string, encode_string,
    parse_canonical_adjacency_manifest_generation_file, parse_canonical_manifest_generation_file,
    parse_generation_file, parse_property_projection_manifest_generation_file,
    parse_property_spill_manifest_generation_file, parse_u64,
    property_projection_artifact_generation_file, property_projection_manifest_generation_file,
    property_spill_artifact_generation_file, property_spill_manifest_generation_file,
    read_durable_text_bytes_with_limit, relational_checkpoint_generation_file,
    relational_checkpoint_metadata, source_scan, split_checkpoint_checksum, sync_parent_dir,
    wal_generation_file, BackupFileEntry, BackupManifest, DurableManifest, BACKUP_HEADER_V1,
    BACKUP_MANIFEST_FILE, MANIFEST_FILE, STABLE_ID_MAPPING_FILE,
};
use crate::error::{Result, SkeinError};
use skein_integrity::{IntegrityHasher, Sha256Digest};
use skein_storage::{
    decode_relational_checkpoint_file, durable_replace_file, CanonicalAdjacencyManifest,
    CanonicalSegmentManifest, ManifestGeneration, PersistentPropertyProjectionManifest,
    PropertySpillManifest, RelationalDecodeLimits, StorageRestoreReport,
};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

impl BackupManifest {
    fn load(path: &Path) -> Result<Self> {
        const MAX_BACKUP_MANIFEST_BYTES: u64 = 64 * 1024;
        let metadata = fs::metadata(path)?;
        if metadata.len() > MAX_BACKUP_MANIFEST_BYTES {
            return Err(SkeinError::Storage(format!(
                "backup manifest exceeds {MAX_BACKUP_MANIFEST_BYTES} bytes"
            )));
        }
        let text = fs::read_to_string(path)?;
        let (body, checksum) = split_backup_manifest_checksum(&text)?;
        let actual = checksum_bytes(body.as_bytes());
        if checksum != actual {
            return Err(SkeinError::Storage(format!(
                "backup manifest checksum mismatch: expected {checksum}, got {actual}"
            )));
        }

        let mut generation = None;
        let mut checkpoint_commit_epoch = None;
        let mut files = Vec::new();
        let mut names = BTreeSet::new();
        let mut saw_header = false;
        for line in body.lines() {
            if line == BACKUP_HEADER_V1 {
                if saw_header {
                    return Err(SkeinError::Storage(
                        "backup manifest has duplicate headers".to_string(),
                    ));
                }
                saw_header = true;
                continue;
            }
            let fields = line.split('\t').collect::<Vec<_>>();
            match fields.as_slice() {
                ["version", "1"] => {}
                ["generation", raw] => {
                    if generation
                        .replace(parse_u64(raw, "backup generation")?)
                        .is_some()
                    {
                        return Err(SkeinError::Storage(
                            "backup manifest has duplicate generation".to_string(),
                        ));
                    }
                }
                ["checkpoint_commit_epoch", raw] => {
                    if checkpoint_commit_epoch
                        .replace(parse_u64(raw, "backup checkpoint commit epoch")?)
                        .is_some()
                    {
                        return Err(SkeinError::Storage(
                            "backup manifest has duplicate checkpoint commit epoch".to_string(),
                        ));
                    }
                }
                ["file", encoded_name, encoded_len, encoded_checksum, sha256] => {
                    let name = decode_string(encoded_name)?;
                    validate_backup_file_name(&name)?;
                    if !names.insert(name.clone()) {
                        return Err(SkeinError::Storage(format!(
                            "backup manifest has duplicate file: {name}"
                        )));
                    }
                    files.push(BackupFileEntry {
                        name,
                        encoded_len: parse_u64(encoded_len, "backup file length")?,
                        encoded_checksum: parse_u64(encoded_checksum, "backup file checksum")?,
                        sha256: sha256.parse().map_err(|error| {
                            SkeinError::Storage(format!(
                                "invalid backup file SHA-256 digest: {error}"
                            ))
                        })?,
                    });
                }
                [""] => {}
                _ => {
                    return Err(SkeinError::Storage(format!(
                        "invalid backup manifest line: {line}"
                    )));
                }
            }
        }
        if !saw_header {
            return Err(SkeinError::Storage(
                "backup manifest is missing its format header".to_string(),
            ));
        }
        let generation = generation.ok_or_else(|| {
            SkeinError::Storage("backup manifest is missing generation".to_string())
        })?;
        let checkpoint_commit_epoch = checkpoint_commit_epoch.ok_or_else(|| {
            SkeinError::Storage("backup manifest is missing checkpoint commit epoch".to_string())
        })?;
        Ok(Self {
            generation,
            checkpoint_commit_epoch,
            files,
            checksum,
        })
    }

    pub(super) fn write(
        path: &Path,
        generation: u64,
        checkpoint_commit_epoch: u64,
        files: Vec<BackupFileEntry>,
    ) -> Result<Self> {
        let mut body = format!(
            "{BACKUP_HEADER_V1}\nversion\t1\ngeneration\t{generation}\ncheckpoint_commit_epoch\t{checkpoint_commit_epoch}\n"
        );
        for file in &files {
            body.push_str(&format!(
                "file\t{}\t{}\t{}\t{}\n",
                encode_string(&file.name),
                file.encoded_len,
                file.encoded_checksum,
                file.sha256
            ));
        }
        let checksum = checksum_bytes(body.as_bytes());
        let tmp_path = path.with_extension("skein.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(format!("{body}checksum\t{checksum}\n").as_bytes())?;
            file.sync_all()?;
        }
        durable_replace_file(&tmp_path, path)?;
        Ok(Self {
            generation,
            checkpoint_commit_epoch,
            files,
            checksum,
        })
    }
}

fn split_backup_manifest_checksum(text: &str) -> Result<(&str, u64)> {
    let marker = "checksum\t";
    let checksum_offset = text
        .rfind(marker)
        .ok_or_else(|| SkeinError::Storage("backup manifest is missing checksum".to_string()))?;
    let body = &text[..checksum_offset];
    let checksum_line = text[checksum_offset..].trim_end();
    if checksum_line.contains('\n') {
        return Err(SkeinError::Storage(
            "backup manifest has data after checksum".to_string(),
        ));
    }
    let checksum = parse_u64(
        checksum_line.strip_prefix(marker).unwrap_or_default(),
        "backup manifest checksum",
    )?;
    Ok((body, checksum))
}

fn validate_backup_file_name(name: &str) -> Result<()> {
    let allowed = name == MANIFEST_FILE
        || name == STABLE_ID_MAPPING_FILE
        || parse_generation_file(name, "checkpoint.").is_some()
        || parse_generation_file(name, "wal.").is_some()
        || parse_generation_file(name, "relational.").is_some()
        || parse_generation_file(name, "canonical.").is_some()
        || parse_canonical_manifest_generation_file(name).is_some()
        || parse_generation_file(name, "adjacency.").is_some()
        || parse_canonical_adjacency_manifest_generation_file(name).is_some()
        || parse_generation_file(name, "properties.").is_some()
        || parse_property_spill_manifest_generation_file(name).is_some()
        || parse_generation_file(name, "property-index.").is_some()
        || parse_property_projection_manifest_generation_file(name).is_some();
    if !allowed || Path::new(name).file_name().and_then(|value| value.to_str()) != Some(name) {
        return Err(SkeinError::Storage(format!(
            "backup contains unsupported file name: {name}"
        )));
    }
    Ok(())
}

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
    if let (Some(expected_len), Some(expected_checksum)) = (
        manifest.canonical_manifest_encoded_len,
        manifest.canonical_manifest_encoded_checksum,
    ) {
        let canonical_manifest_name = canonical_manifest_generation_file(generation);
        let canonical_artifact_name = canonical_artifact_generation_file(generation);
        for required in [
            canonical_manifest_name.as_str(),
            canonical_artifact_name.as_str(),
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
    }
    if let (Some(expected_len), Some(expected_checksum)) = (
        manifest.canonical_adjacency_manifest_encoded_len,
        manifest.canonical_adjacency_manifest_encoded_checksum,
    ) {
        let adjacency_manifest_name = canonical_adjacency_manifest_generation_file(generation);
        let adjacency_artifact_name = canonical_adjacency_artifact_generation_file(generation);
        for required in [
            adjacency_manifest_name.as_str(),
            adjacency_artifact_name.as_str(),
        ] {
            if !names.contains(required) {
                return Err(SkeinError::Storage(format!(
                    "backup is missing required canonical adjacency file: {required}"
                )));
            }
        }
        let encoded_manifest = files
            .iter()
            .find(|file| file.name == adjacency_manifest_name)
            .expect("required canonical adjacency manifest must exist");
        if encoded_manifest.encoded_len != expected_len
            || encoded_manifest.encoded_checksum != expected_checksum
            || manifest.canonical_adjacency_manifest_encoded_sha256 != Some(encoded_manifest.sha256)
        {
            return Err(SkeinError::Storage(
                "backup canonical adjacency manifest metadata does not match the durable manifest"
                    .to_string(),
            ));
        }
        let adjacency_manifest_text = fs::read_to_string(root.join(&adjacency_manifest_name))?;
        let adjacency_manifest = CanonicalAdjacencyManifest::decode(&adjacency_manifest_text)
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        let adjacency_artifact = files
            .iter()
            .find(|file| file.name == adjacency_artifact_name)
            .expect("required canonical adjacency artifact must exist");
        if adjacency_manifest.artifact_len != adjacency_artifact.encoded_len
            || adjacency_manifest.artifact_digest.0 != adjacency_artifact.encoded_checksum
            || adjacency_manifest.artifact_sha256 != adjacency_artifact.sha256
        {
            return Err(SkeinError::Storage(
                "backup canonical adjacency artifact metadata does not match its manifest"
                    .to_string(),
            ));
        }
    }
    if let (Some(expected_len), Some(expected_checksum)) = (
        manifest.property_spill_manifest_encoded_len,
        manifest.property_spill_manifest_encoded_checksum,
    ) {
        let property_manifest_name = property_spill_manifest_generation_file(generation);
        let property_artifact_name = property_spill_artifact_generation_file(generation);
        for required in [
            property_manifest_name.as_str(),
            property_artifact_name.as_str(),
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
            || manifest.property_spill_manifest_encoded_sha256 != Some(encoded_manifest.sha256)
        {
            return Err(SkeinError::Storage(
                "backup property spill manifest metadata does not match the durable manifest"
                    .to_string(),
            ));
        }
        let property_manifest_text = fs::read_to_string(root.join(&property_manifest_name))?;
        let property_manifest = PropertySpillManifest::decode(&property_manifest_text)
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
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
    }
    if let (Some(expected_len), Some(expected_checksum)) = (
        manifest.property_projection_manifest_encoded_len,
        manifest.property_projection_manifest_encoded_checksum,
    ) {
        let projection_manifest_name = property_projection_manifest_generation_file(generation);
        let projection_artifact_name = property_projection_artifact_generation_file(generation);
        for required in [
            projection_manifest_name.as_str(),
            projection_artifact_name.as_str(),
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
