use crate::artifact_files::{
    parse_append_manifest_generation_file, parse_append_segment_generation_file,
    parse_canonical_adjacency_descriptor_generation_file, parse_canonical_manifest_generation_file,
    parse_canonical_segment_descriptor_generation_file, parse_generation_file,
    parse_property_projection_descriptor_generation_file,
    parse_property_projection_manifest_generation_file,
    parse_property_spill_descriptor_generation_file, parse_property_spill_manifest_generation_file,
    parse_relational_index_artifact_generation_file,
    parse_relational_index_manifest_generation_file, parse_relational_overflow_generation_file,
    parse_relational_row_generation_file,
};
use crate::durable_replace_file;
use skein_core::{Result, SkeinError};
use skein_integrity::{checksum_u64, IntegrityHasher, Sha256Digest};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

pub const BACKUP_MANIFEST_FILE: &str = "backup.skein";
pub const BACKUP_HEADER_V1: &str = "SKEIN_BACKUP_V1";
pub const STORAGE_MANIFEST_FILE: &str = "manifest.skein";
pub const STABLE_ID_MAPPING_FILE: &str = "stable_ids.skein";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupFileEntry {
    pub name: String,
    pub encoded_len: u64,
    pub encoded_checksum: u64,
    pub sha256: Sha256Digest,
}

#[doc(hidden)]
pub fn validate_new_backup_destination(root: &Path, destination: &Path) -> Result<()> {
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

#[doc(hidden)]
pub fn copy_backup_file(source: &Path, destination: &Path, name: &str) -> Result<BackupFileEntry> {
    let (encoded_len, encoded_checksum, sha256) = copy_file_with_checksum(source, destination)?;
    Ok(BackupFileEntry {
        name: name.to_string(),
        encoded_len,
        encoded_checksum,
        sha256,
    })
}

#[doc(hidden)]
pub fn copy_file_with_checksum(
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

#[doc(hidden)]
pub fn file_checksum(path: &Path) -> Result<(u64, u64, Sha256Digest)> {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupManifest {
    pub generation: u64,
    pub checkpoint_commit_epoch: u64,
    pub files: Vec<BackupFileEntry>,
    pub checksum: u64,
}

impl BackupManifest {
    pub fn load(path: &Path) -> Result<Self> {
        const MAX_BACKUP_MANIFEST_BYTES: u64 = 64 * 1024;

        let metadata = fs::metadata(path)?;
        if metadata.len() > MAX_BACKUP_MANIFEST_BYTES {
            return Err(SkeinError::Storage(format!(
                "backup manifest exceeds {MAX_BACKUP_MANIFEST_BYTES} bytes"
            )));
        }
        let text = fs::read_to_string(path)?;
        let (body, checksum) = split_backup_manifest_checksum(&text)?;
        let actual = checksum_u64(body.as_bytes());
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

    pub fn write(
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
        let checksum = checksum_u64(body.as_bytes());
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

pub fn validate_backup_file_name(name: &str) -> Result<()> {
    let allowed = name == STORAGE_MANIFEST_FILE
        || name == STABLE_ID_MAPPING_FILE
        || parse_generation_file(name, "stable_ids.").is_some()
        || parse_generation_file(name, "checkpoint.").is_some()
        || parse_generation_file(name, "wal.").is_some()
        || parse_generation_file(name, "relational.").is_some()
        || parse_generation_file(name, "canonical.").is_some()
        || parse_canonical_manifest_generation_file(name).is_some()
        || parse_canonical_segment_descriptor_generation_file(name).is_some()
        || parse_generation_file(name, "adjacency.").is_some()
        || parse_canonical_adjacency_descriptor_generation_file(name).is_some()
        || parse_generation_file(name, "properties.").is_some()
        || parse_property_spill_manifest_generation_file(name).is_some()
        || parse_property_spill_descriptor_generation_file(name).is_some()
        || parse_generation_file(name, "property-index.").is_some()
        || parse_property_projection_manifest_generation_file(name).is_some()
        || parse_property_projection_descriptor_generation_file(name).is_some()
        || parse_relational_index_artifact_generation_file(name).is_some()
        || parse_relational_index_manifest_generation_file(name).is_some()
        || parse_relational_row_generation_file(name).is_some()
        || parse_relational_overflow_generation_file(name).is_some()
        || parse_append_segment_generation_file(name).is_some()
        || parse_append_manifest_generation_file(name).is_some();
    if !allowed || Path::new(name).file_name().and_then(|value| value.to_str()) != Some(name) {
        return Err(SkeinError::Storage(format!(
            "backup contains unsupported file name: {name}"
        )));
    }
    Ok(())
}

fn parse_u64(raw: &str, field: &str) -> Result<u64> {
    raw.parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {field}: {raw}")))
}

fn encode_string(input: &str) -> String {
    input
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn decode_string(input: &str) -> Result<String> {
    if !input.len().is_multiple_of(2) {
        return Err(SkeinError::Storage(format!(
            "invalid hex string length: {}",
            input.len()
        )));
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    for offset in (0..input.len()).step_by(2) {
        let byte = input
            .get(offset..offset + 2)
            .and_then(|pair| u8::from_str_radix(pair, 16).ok())
            .ok_or_else(|| {
                SkeinError::Storage(format!("invalid hex string at byte offset {offset}"))
            })?;
        bytes.push(byte);
    }
    String::from_utf8(bytes).map_err(|error| SkeinError::Storage(error.to_string()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageBackupReport {
    pub generation: u64,
    pub checkpoint_commit_epoch: u64,
    pub file_count: usize,
    pub total_bytes: u64,
    pub manifest_checksum: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageRestoreReport {
    pub generation: u64,
    pub checkpoint_commit_epoch: u64,
    pub file_count: usize,
    pub total_bytes: u64,
    pub manifest_checksum: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageScrubReport {
    pub generation: u64,
    pub checked_file_count: usize,
    pub checked_bytes: u64,
    pub sha256_verified_file_count: usize,
    pub wal_record_count: usize,
    pub wal_bytes: u64,
}

#[cfg(test)]
mod hex_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(1);

    fn test_directory() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "skein-storage-backup-manifest-{}-{}",
            std::process::id(),
            NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn backup_manifest_round_trips_protocol_fields() {
        let directory = test_directory();
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join(BACKUP_MANIFEST_FILE);
        let files = vec![BackupFileEntry {
            name: "checkpoint.7.skein".to_string(),
            encoded_len: 42,
            encoded_checksum: 9,
            sha256: Sha256Digest::from_bytes([3; 32]),
        }];

        let written = BackupManifest::write(&path, 7, 11, files.clone()).unwrap();
        let loaded = BackupManifest::load(&path).unwrap();

        assert_eq!(loaded, written);
        assert_eq!(loaded.files, files);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn backup_manifest_rejects_path_traversal() {
        let error = validate_backup_file_name("../checkpoint.7.skein").unwrap_err();

        assert!(error.to_string().contains("unsupported file name"));
    }

    #[test]
    fn backup_manifest_rejects_trailing_data_after_checksum() {
        let body = "SKEIN_BACKUP_V1\nversion\t1\ngeneration\t7\ncheckpoint_commit_epoch\t11\n";
        let text = format!(
            "{body}checksum\t{}\nunexpected\n",
            checksum_u64(body.as_bytes())
        );

        let error = split_backup_manifest_checksum(&text).unwrap_err();

        assert!(error.to_string().contains("data after checksum"));
    }

    #[test]
    fn backup_file_copy_preserves_integrity_and_manifest_entry() {
        let directory = test_directory();
        fs::create_dir_all(&directory).unwrap();
        let source = directory.join("source.skein");
        let destination = directory.join("destination.skein");
        let payload = vec![0x5a; 1024 * 1024 + 17];
        fs::write(&source, &payload).unwrap();

        let entry = copy_backup_file(&source, &destination, "checkpoint.7.skein").unwrap();
        let source_identity = file_checksum(&source).unwrap();
        let destination_identity = file_checksum(&destination).unwrap();

        assert_eq!(source_identity, destination_identity);
        assert_eq!(
            entry,
            BackupFileEntry {
                name: "checkpoint.7.skein".to_string(),
                encoded_len: source_identity.0,
                encoded_checksum: source_identity.1,
                sha256: source_identity.2,
            }
        );
        assert_eq!(fs::read(destination).unwrap(), payload);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn backup_destination_must_be_new_and_outside_database_root() {
        let directory = test_directory();
        let root = directory.join("database");
        let backup_parent = directory.join("backups");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&backup_parent).unwrap();

        validate_new_backup_destination(&root, &backup_parent.join("backup")).unwrap();
        let error = validate_new_backup_destination(&root, &root.join("backup")).unwrap_err();
        assert!(error
            .to_string()
            .contains("cannot be inside the database directory"));

        let existing = backup_parent.join("existing");
        fs::create_dir(&existing).unwrap();
        let error = validate_new_backup_destination(&root, &existing).unwrap_err();
        assert!(error.to_string().contains("already exists"));
        fs::remove_dir_all(directory).unwrap();
    }
}
