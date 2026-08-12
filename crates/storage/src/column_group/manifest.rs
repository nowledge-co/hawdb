//! Layered, generation-bound publication metadata for column groups.
//!
//! The active manifest is the only mutable publication point. It references
//! immutable per-table directories, which in turn reference immutable group
//! and deletion-vector artifacts. Updating one table therefore rewrites one
//! small directory plus the active manifest, never every table's metadata or
//! any untouched group bytes.

use super::deletion::{encoded_file_len as deletion_vector_file_len, DeletionVector};
use super::encoding::Cursor;
use super::group::ColumnGroupReader;
use super::{corrupt, unsupported, ColumnGroupError};
use crate::durability::durable_replace_file;
use crate::ManifestGeneration;
use skein_integrity::{crc32c, IntegrityHasher, Sha256Digest, SHA256_BYTES};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub const COLUMN_GROUP_MANIFEST_FILE: &str = "column-groups.manifest.skein";

const MANIFEST_MAGIC: &[u8; 9] = b"SKNCOLM01";
const TABLE_DIRECTORY_MAGIC: &[u8; 10] = b"SKNCOLDIR1";
const FORMAT_VERSION: u32 = 1;
const MANIFEST_LOCK_FILE: &str = "column-groups.publish.lock";
const FOOTER_FIXED_BYTES: usize = 8 + 4;
const MAX_METADATA_BODY_BYTES: u64 = 16 * 1024 * 1024;
const MAX_METADATA_FILE_BYTES: u64 =
    MAX_METADATA_BODY_BYTES + (TABLE_DIRECTORY_MAGIC.len() * 2 + FOOTER_FIXED_BYTES) as u64;
const MAX_TABLES: usize = 1_000_000;
const MAX_GROUPS_PER_TABLE: usize = 4_000_000;
const MAX_FILE_NAME_BYTES: usize = 255;
const MIN_GROUP_DESCRIPTOR_BYTES: usize = 93;
const MIN_TABLE_REFERENCE_BYTES: usize = 69;

static CANDIDATE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum ColumnGroupTableKind {
    Node = 1,
    Relationship = 2,
    Relational = 3,
}

impl ColumnGroupTableKind {
    fn from_tag(tag: u8) -> Result<Self, ColumnGroupError> {
        match tag {
            1 => Ok(Self::Node),
            2 => Ok(Self::Relationship),
            3 => Ok(Self::Relational),
            _ => Err(corrupt(format!("unknown column-group table kind {tag}"))),
        }
    }

    const fn file_tag(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Relationship => "relationship",
            Self::Relational => "relational",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ColumnGroupTableKey {
    pub kind: ColumnGroupTableKind,
    pub table_id: u64,
}

impl ColumnGroupTableKey {
    pub const fn new(kind: ColumnGroupTableKind, table_id: u64) -> Self {
        Self { kind, table_id }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnGroupArtifactDescriptor {
    group_id: u64,
    group_generation: ManifestGeneration,
    row_count: u32,
    deleted_count: u32,
    min_id: u64,
    max_id: u64,
    group_file: String,
    group_len: u64,
    group_sha256: Sha256Digest,
    deletion_vector_file: Option<String>,
    deletion_vector_len: u64,
    deletion_vector_sha256: Option<Sha256Digest>,
    deletion_vector_publication_generation: Option<ManifestGeneration>,
}

impl ColumnGroupArtifactDescriptor {
    /// Inspects newly written immutable artifacts once, including their full
    /// SHA-256 identity. Reusing this descriptor in a later table directory
    /// avoids rehashing untouched group bytes.
    pub fn inspect(
        root: &Path,
        group_file: impl Into<String>,
        deletion_vector_file: Option<String>,
    ) -> Result<Self, ColumnGroupError> {
        let group_file = group_file.into();
        validate_file_name(&group_file, "column group")?;
        let group_path = root.join(&group_file);
        let group_len = bounded_file_len(&group_path, u64::MAX, "column group")?;
        let group_sha256 = hash_file(&group_path)?;
        let reader = ColumnGroupReader::open_path(&group_path)?;
        let directory = reader.directory();

        let mut descriptor = Self {
            group_id: directory.group_id,
            group_generation: directory.generation,
            row_count: directory.row_count,
            deleted_count: 0,
            min_id: directory.min_id,
            max_id: directory.max_id,
            group_file,
            group_len,
            group_sha256,
            deletion_vector_file: None,
            deletion_vector_len: 0,
            deletion_vector_sha256: None,
            deletion_vector_publication_generation: None,
        };
        if let Some(file_name) = deletion_vector_file {
            descriptor = descriptor.with_deletion_vector(root, file_name)?;
        }
        Ok(descriptor)
    }

    /// Rebinds an existing immutable group descriptor to a newly published
    /// cumulative deletion vector without rereading or rehashing group bytes.
    pub fn with_deletion_vector(
        mut self,
        root: &Path,
        file_name: impl Into<String>,
    ) -> Result<Self, ColumnGroupError> {
        let file_name = file_name.into();
        validate_file_name(&file_name, "deletion vector")?;
        let path = root.join(&file_name);
        let vector = DeletionVector::open(&path)?;
        if vector.group_id() != self.group_id
            || vector.group_generation() != self.group_generation
            || vector.row_count() != self.row_count
        {
            return Err(ColumnGroupError::DeletionVectorGroupMismatch {
                expected_group: self.group_id,
                actual_group: vector.group_id(),
                expected_group_generation: self.group_generation.0,
                actual_group_generation: vector.group_generation().0,
            });
        }
        self.deleted_count = vector.deleted_count();
        self.deletion_vector_len = bounded_file_len(&path, u64::MAX, "deletion vector")?;
        self.deletion_vector_sha256 = Some(hash_file(&path)?);
        self.deletion_vector_publication_generation = Some(vector.publication_generation());
        self.deletion_vector_file = Some(file_name);
        Ok(self)
    }

    pub const fn group_id(&self) -> u64 {
        self.group_id
    }

    pub const fn group_generation(&self) -> ManifestGeneration {
        self.group_generation
    }

    pub const fn row_count(&self) -> u32 {
        self.row_count
    }

    pub const fn deleted_count(&self) -> u32 {
        self.deleted_count
    }

    pub const fn visible_count(&self) -> u32 {
        self.row_count.saturating_sub(self.deleted_count)
    }

    pub const fn min_id(&self) -> u64 {
        self.min_id
    }

    pub const fn max_id(&self) -> u64 {
        self.max_id
    }

    pub fn group_file(&self) -> &str {
        &self.group_file
    }

    pub fn deletion_vector_file(&self) -> Option<&str> {
        self.deletion_vector_file.as_deref()
    }

    pub const fn group_sha256(&self) -> Sha256Digest {
        self.group_sha256
    }

    pub const fn deletion_vector_sha256(&self) -> Option<Sha256Digest> {
        self.deletion_vector_sha256
    }

    pub const fn deletion_vector_publication_generation(&self) -> Option<ManifestGeneration> {
        self.deletion_vector_publication_generation
    }

    fn validate(&self, publication_generation: ManifestGeneration) -> Result<(), ColumnGroupError> {
        validate_file_name(&self.group_file, "column group")?;
        if self.row_count == 0 {
            if self.min_id != 0 || self.max_id != 0 {
                return Err(corrupt(format!(
                    "empty column group {} has nonzero id bounds",
                    self.group_id
                )));
            }
        } else if self.min_id > self.max_id {
            return Err(corrupt(format!(
                "column group {} has inverted id bounds",
                self.group_id
            )));
        }
        if self.group_generation.0 > publication_generation.0 {
            return Err(corrupt(format!(
                "column group {} was created in future generation {}",
                self.group_id, self.group_generation.0
            )));
        }
        match (
            &self.deletion_vector_file,
            self.deletion_vector_sha256,
            self.deletion_vector_publication_generation,
        ) {
            (None, None, None) if self.deletion_vector_len == 0 && self.deleted_count == 0 => {}
            (Some(file), Some(_), Some(generation)) => {
                validate_file_name(file, "deletion vector")?;
                if self.deletion_vector_len == 0
                    || self.deletion_vector_len != deletion_vector_file_len(self.row_count)
                    || generation.0 < self.group_generation.0
                    || generation.0 > publication_generation.0
                    || self.deleted_count > self.row_count
                {
                    return Err(corrupt(format!(
                        "column group {} has an invalid deletion-vector binding",
                        self.group_id
                    )));
                }
            }
            _ => {
                return Err(corrupt(format!(
                    "column group {} has incomplete deletion-vector metadata",
                    self.group_id
                )));
            }
        }
        Ok(())
    }

    fn validate_artifacts(&self, root: &Path) -> Result<(), ColumnGroupError> {
        let group_path = root.join(&self.group_file);
        let actual_len = bounded_file_len(&group_path, u64::MAX, "column group")?;
        if actual_len != self.group_len {
            return Err(corrupt(format!(
                "column group {} length changed from {} to {actual_len}",
                self.group_id, self.group_len
            )));
        }
        let reader = ColumnGroupReader::open_path(&group_path)?;
        let directory = reader.directory();
        if directory.group_id != self.group_id
            || directory.generation != self.group_generation
            || directory.row_count != self.row_count
            || directory.min_id != self.min_id
            || directory.max_id != self.max_id
        {
            return Err(corrupt(format!(
                "column group {} no longer matches its table directory",
                self.group_id
            )));
        }
        if let Some(file_name) = &self.deletion_vector_file {
            let path = root.join(file_name);
            let actual_len = bounded_file_len(&path, u64::MAX, "deletion vector")?;
            if actual_len != self.deletion_vector_len {
                return Err(corrupt(format!(
                    "deletion vector for group {} length changed from {} to {actual_len}",
                    self.group_id, self.deletion_vector_len
                )));
            }
            let vector = DeletionVector::open(&path)?;
            if vector.group_id() != self.group_id
                || vector.group_generation() != self.group_generation
                || vector.publication_generation()
                    != self
                        .deletion_vector_publication_generation
                        .expect("validated")
                || vector.row_count() != self.row_count
                || vector.deleted_count() != self.deleted_count
            {
                return Err(corrupt(format!(
                    "deletion vector for group {} no longer matches its table directory",
                    self.group_id
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnGroupTableDirectory {
    table: ColumnGroupTableKey,
    publication_generation: ManifestGeneration,
    groups: Vec<ColumnGroupArtifactDescriptor>,
}

impl ColumnGroupTableDirectory {
    pub fn new(
        table: ColumnGroupTableKey,
        publication_generation: ManifestGeneration,
        mut groups: Vec<ColumnGroupArtifactDescriptor>,
    ) -> Result<Self, ColumnGroupError> {
        groups.sort_by_key(|group| (group.min_id, group.group_id));
        let directory = Self {
            table,
            publication_generation,
            groups,
        };
        directory.validate()?;
        Ok(directory)
    }

    pub const fn table(&self) -> ColumnGroupTableKey {
        self.table
    }

    pub const fn publication_generation(&self) -> ManifestGeneration {
        self.publication_generation
    }

    pub fn groups(&self) -> &[ColumnGroupArtifactDescriptor] {
        &self.groups
    }

    pub fn file_name(&self) -> String {
        format!(
            "table-{}-{}-groups-{}.skein",
            self.table.kind.file_tag(),
            self.table.table_id,
            self.publication_generation.0
        )
    }

    pub fn write_immutable(
        &self,
        root: &Path,
    ) -> Result<ColumnGroupTableDirectoryRef, ColumnGroupError> {
        self.validate()?;
        fs::create_dir_all(root)?;
        for group in &self.groups {
            group.validate_artifacts(root)?;
        }
        let _lease = PublicationLease::acquire(root)?;
        remove_orphaned_candidates(root);
        let file_name = self.file_name();
        let path = root.join(&file_name);
        let bytes = encode_envelope(TABLE_DIRECTORY_MAGIC, &self.encode_body()?)?;
        if path.exists() {
            let existing = read_bounded(&path, MAX_METADATA_FILE_BYTES, "table directory")?;
            if existing != bytes {
                return Err(corrupt(format!(
                    "immutable table directory {file_name} already exists with different bytes"
                )));
            }
        } else {
            publish_bytes(&path, &bytes)?;
        }
        Ok(ColumnGroupTableDirectoryRef {
            table: self.table,
            directory_generation: self.publication_generation,
            file_name,
            byte_len: bytes.len() as u64,
            sha256: skein_integrity::sha256(&bytes),
        })
    }

    fn validate(&self) -> Result<(), ColumnGroupError> {
        if self.groups.len() > MAX_GROUPS_PER_TABLE {
            return Err(unsupported(format!(
                "table directory holds {} groups, exceeding {MAX_GROUPS_PER_TABLE}",
                self.groups.len()
            )));
        }
        let mut ids = BTreeSet::new();
        let mut previous_max = None;
        let mut files = BTreeSet::new();
        for group in &self.groups {
            group.validate(self.publication_generation)?;
            if !ids.insert(group.group_id) {
                return Err(corrupt(format!(
                    "table directory declares group {} twice",
                    group.group_id
                )));
            }
            if group.row_count > 0 && previous_max.is_some_and(|max| group.min_id <= max) {
                return Err(corrupt(format!(
                    "table directory group {} overlaps a previous id range",
                    group.group_id
                )));
            }
            if group.row_count > 0 {
                previous_max = Some(group.max_id);
            }
            if !files.insert(group.group_file.as_str())
                || group
                    .deletion_vector_file
                    .as_deref()
                    .is_some_and(|file| !files.insert(file))
            {
                return Err(corrupt(
                    "table directory reuses an artifact file".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn encode_body(&self) -> Result<Vec<u8>, ColumnGroupError> {
        let group_count = u32::try_from(self.groups.len())
            .map_err(|_| unsupported("table directory group count exceeds u32".to_string()))?;
        let mut bytes = Vec::new();
        bytes.extend(FORMAT_VERSION.to_le_bytes());
        bytes.push(self.table.kind as u8);
        bytes.extend([0; 3]);
        bytes.extend(self.table.table_id.to_le_bytes());
        bytes.extend(self.publication_generation.0.to_le_bytes());
        bytes.extend(group_count.to_le_bytes());
        bytes.extend(0u32.to_le_bytes());
        for group in &self.groups {
            bytes.extend(group.group_id.to_le_bytes());
            bytes.extend(group.group_generation.0.to_le_bytes());
            bytes.extend(group.row_count.to_le_bytes());
            bytes.extend(group.deleted_count.to_le_bytes());
            bytes.extend(group.min_id.to_le_bytes());
            bytes.extend(group.max_id.to_le_bytes());
            bytes.extend(group.group_len.to_le_bytes());
            bytes.extend(group.group_sha256.as_bytes());
            encode_file_name(&mut bytes, &group.group_file)?;
            match (
                &group.deletion_vector_file,
                group.deletion_vector_sha256,
                group.deletion_vector_publication_generation,
            ) {
                (Some(file), Some(sha256), Some(generation)) => {
                    bytes.push(1);
                    bytes.extend([0; 7]);
                    bytes.extend(group.deletion_vector_len.to_le_bytes());
                    bytes.extend(generation.0.to_le_bytes());
                    bytes.extend(sha256.as_bytes());
                    encode_file_name(&mut bytes, file)?;
                }
                (None, None, None) => {
                    bytes.push(0);
                    bytes.extend([0; 7]);
                }
                _ => unreachable!("validated deletion-vector metadata"),
            }
        }
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, ColumnGroupError> {
        let body = decode_envelope(TABLE_DIRECTORY_MAGIC, bytes, "table directory")?;
        let mut cursor = Cursor::new(body);
        let version = cursor.read_u32("table directory version")?;
        if version != FORMAT_VERSION {
            return Err(corrupt(format!(
                "unsupported table directory version {version}"
            )));
        }
        let kind = ColumnGroupTableKind::from_tag(cursor.read_u8("table kind")?)?;
        require_zero(cursor.read_bytes(3, "table directory reserved bytes")?)?;
        let table_id = cursor.read_u64("table id")?;
        let publication_generation =
            ManifestGeneration(cursor.read_u64("table directory generation")?);
        let group_count = cursor.read_u32("table directory group count")? as usize;
        if group_count > MAX_GROUPS_PER_TABLE {
            return Err(corrupt(format!(
                "table directory group count {group_count} exceeds {MAX_GROUPS_PER_TABLE}"
            )));
        }
        require_zero(cursor.read_bytes(4, "table directory reserved bytes")?)?;
        if group_count > cursor.remaining_len() / MIN_GROUP_DESCRIPTOR_BYTES {
            return Err(corrupt(
                "table directory group count exceeds the remaining bytes".to_string(),
            ));
        }
        let mut groups = Vec::with_capacity(group_count);
        for _ in 0..group_count {
            let group_id = cursor.read_u64("group id")?;
            let group_generation = ManifestGeneration(cursor.read_u64("group generation")?);
            let row_count = cursor.read_u32("group row count")?;
            let deleted_count = cursor.read_u32("group deleted count")?;
            let min_id = cursor.read_u64("group minimum id")?;
            let max_id = cursor.read_u64("group maximum id")?;
            let group_len = cursor.read_u64("group byte length")?;
            let group_sha256 = read_sha256(&mut cursor, "group SHA-256")?;
            let group_file = decode_file_name(&mut cursor, "column group")?;
            let has_deletion_vector = cursor.read_u8("deletion-vector flag")?;
            require_zero(cursor.read_bytes(7, "deletion-vector reserved bytes")?)?;
            let (
                deletion_vector_file,
                deletion_vector_len,
                deletion_vector_sha256,
                deletion_vector_publication_generation,
            ) = match has_deletion_vector {
                0 => (None, 0, None, None),
                1 => {
                    let byte_len = cursor.read_u64("deletion-vector byte length")?;
                    let generation = ManifestGeneration(
                        cursor.read_u64("deletion-vector publication generation")?,
                    );
                    let sha256 = read_sha256(&mut cursor, "deletion-vector SHA-256")?;
                    let file = decode_file_name(&mut cursor, "deletion vector")?;
                    (Some(file), byte_len, Some(sha256), Some(generation))
                }
                flag => {
                    return Err(corrupt(format!(
                        "invalid deletion-vector presence flag {flag}"
                    )));
                }
            };
            groups.push(ColumnGroupArtifactDescriptor {
                group_id,
                group_generation,
                row_count,
                deleted_count,
                min_id,
                max_id,
                group_file,
                group_len,
                group_sha256,
                deletion_vector_file,
                deletion_vector_len,
                deletion_vector_sha256,
                deletion_vector_publication_generation,
            });
        }
        cursor.expect_exhausted("table directory")?;
        let directory = Self {
            table: ColumnGroupTableKey { kind, table_id },
            publication_generation,
            groups,
        };
        directory.validate()?;
        Ok(directory)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnGroupTableDirectoryRef {
    table: ColumnGroupTableKey,
    directory_generation: ManifestGeneration,
    file_name: String,
    byte_len: u64,
    sha256: Sha256Digest,
}

impl ColumnGroupTableDirectoryRef {
    pub const fn table(&self) -> ColumnGroupTableKey {
        self.table
    }

    pub const fn directory_generation(&self) -> ManifestGeneration {
        self.directory_generation
    }

    pub fn file_name(&self) -> &str {
        &self.file_name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnGroupManifest {
    generation: ManifestGeneration,
    parent_generation: Option<ManifestGeneration>,
    source_commit_epoch: u64,
    tables: Vec<ColumnGroupTableDirectoryRef>,
}

impl ColumnGroupManifest {
    pub fn new(
        generation: ManifestGeneration,
        parent_generation: Option<ManifestGeneration>,
        source_commit_epoch: u64,
        mut tables: Vec<ColumnGroupTableDirectoryRef>,
    ) -> Result<Self, ColumnGroupError> {
        tables.sort_by_key(|reference| reference.table);
        let manifest = Self {
            generation,
            parent_generation,
            source_commit_epoch,
            tables,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub const fn generation(&self) -> ManifestGeneration {
        self.generation
    }

    pub const fn parent_generation(&self) -> Option<ManifestGeneration> {
        self.parent_generation
    }

    pub const fn source_commit_epoch(&self) -> u64 {
        self.source_commit_epoch
    }

    pub fn tables(&self) -> &[ColumnGroupTableDirectoryRef] {
        &self.tables
    }

    pub fn table(&self, key: ColumnGroupTableKey) -> Option<&ColumnGroupTableDirectoryRef> {
        self.tables
            .binary_search_by_key(&key, |reference| reference.table)
            .ok()
            .map(|index| &self.tables[index])
    }

    /// Publishes this candidate while holding the cooperative filesystem
    /// publish lease. Its embedded parent generation is the compare-and-swap
    /// token; a stale candidate fails before replacing the active manifest.
    ///
    /// Artifact validation here is proportional to the change volume: only
    /// tables whose directory generation equals the candidate generation open
    /// their group and deletion-vector artifacts. `validate_transition`
    /// proves untouched tables are byte-identical references to the parent's
    /// directories, which were validated at their own publication and remain
    /// SHA-256-bound. Reopen (`open`) stays the full fail-closed boundary.
    pub fn publish(&self, root: &Path) -> Result<PublishedColumnGroupCatalog, ColumnGroupError> {
        self.validate()?;
        fs::create_dir_all(root)?;
        let _lease = PublicationLease::acquire(root)?;
        remove_orphaned_candidates(root);
        let current = Self::load_active_manifest(root)?;
        if current.as_ref() == Some(self) {
            let directories = self.load_directories(root, ArtifactValidation::ChangedTables)?;
            return Ok(PublishedColumnGroupCatalog {
                manifest: self.clone(),
                directories,
            });
        }
        let actual_parent = current.as_ref().map(|manifest| manifest.generation);
        if self.parent_generation != actual_parent {
            return Err(ColumnGroupError::StaleManifestGeneration {
                expected_parent: self.parent_generation.map(|generation| generation.0),
                actual_parent: actual_parent.map(|generation| generation.0),
            });
        }
        self.validate_transition(current.as_ref())?;
        let directories = self.load_directories(root, ArtifactValidation::ChangedTables)?;
        let bytes = encode_envelope(MANIFEST_MAGIC, &self.encode_body()?)?;
        publish_bytes(&root.join(COLUMN_GROUP_MANIFEST_FILE), &bytes)?;
        Ok(PublishedColumnGroupCatalog {
            manifest: self.clone(),
            directories,
        })
    }

    pub fn open(root: &Path) -> Result<Option<PublishedColumnGroupCatalog>, ColumnGroupError> {
        let Some(manifest) = Self::load_active_manifest(root)? else {
            return Ok(None);
        };
        let directories = manifest.load_directories(root, ArtifactValidation::AllTables)?;
        Ok(Some(PublishedColumnGroupCatalog {
            manifest,
            directories,
        }))
    }

    fn validate(&self) -> Result<(), ColumnGroupError> {
        let expected = match self.parent_generation {
            Some(parent) => parent
                .0
                .checked_add(1)
                .ok_or_else(|| unsupported("manifest generation overflows u64".to_string()))?,
            None => 1,
        };
        if self.generation.0 != expected {
            return Err(unsupported(format!(
                "manifest generation {} must immediately follow parent {:?}",
                self.generation.0,
                self.parent_generation.map(|generation| generation.0)
            )));
        }
        if self.tables.len() > MAX_TABLES {
            return Err(unsupported(format!(
                "manifest holds {} tables, exceeding {MAX_TABLES}",
                self.tables.len()
            )));
        }
        let mut previous = None;
        let mut files = BTreeSet::new();
        for reference in &self.tables {
            if previous.is_some_and(|table| reference.table <= table) {
                return Err(corrupt(
                    "manifest table references are not strictly ordered".to_string(),
                ));
            }
            previous = Some(reference.table);
            validate_file_name(&reference.file_name, "table directory")?;
            if reference.byte_len == 0 || reference.directory_generation.0 > self.generation.0 {
                return Err(corrupt(format!(
                    "manifest has an invalid directory reference for table {:?}",
                    reference.table
                )));
            }
            if !files.insert(reference.file_name.as_str()) {
                return Err(corrupt(
                    "manifest reuses one table directory file".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn validate_transition(&self, current: Option<&Self>) -> Result<(), ColumnGroupError> {
        if let Some(current) = current
            && self.source_commit_epoch < current.source_commit_epoch
        {
            return Err(unsupported(format!(
                "manifest source commit epoch {} precedes current epoch {}",
                self.source_commit_epoch, current.source_commit_epoch
            )));
        }
        for reference in &self.tables {
            let unchanged = current
                .and_then(|manifest| manifest.table(reference.table))
                .is_some_and(|previous| previous == reference);
            if !unchanged && reference.directory_generation != self.generation {
                return Err(unsupported(format!(
                    "changed table {:?} must publish directory generation {}",
                    reference.table, self.generation.0
                )));
            }
        }
        Ok(())
    }

    /// Loads and identity-checks every referenced table directory. Directory
    /// file identity (byte length plus SHA-256 of the directory bytes) is
    /// always verified for all tables; `validation` selects which tables'
    /// referenced group and deletion-vector artifacts are also opened.
    fn load_directories(
        &self,
        root: &Path,
        validation: ArtifactValidation,
    ) -> Result<Vec<ColumnGroupTableDirectory>, ColumnGroupError> {
        let mut directories = Vec::with_capacity(self.tables.len());
        let mut artifact_files = BTreeSet::new();
        for reference in &self.tables {
            let path = root.join(&reference.file_name);
            let bytes = read_bounded(&path, MAX_METADATA_FILE_BYTES, "table directory")?;
            if bytes.len() as u64 != reference.byte_len
                || skein_integrity::sha256(&bytes) != reference.sha256
            {
                return Err(corrupt(format!(
                    "table directory {} does not match its manifest identity",
                    reference.file_name
                )));
            }
            let directory = ColumnGroupTableDirectory::decode(&bytes)?;
            if directory.table != reference.table
                || directory.publication_generation != reference.directory_generation
            {
                return Err(corrupt(format!(
                    "table directory {} has the wrong identity",
                    reference.file_name
                )));
            }
            let validate_artifacts = match validation {
                ArtifactValidation::AllTables => true,
                ArtifactValidation::ChangedTables => {
                    reference.directory_generation == self.generation
                }
            };
            for group in &directory.groups {
                if !artifact_files.insert(group.group_file.clone())
                    || group
                        .deletion_vector_file
                        .as_deref()
                        .is_some_and(|file| !artifact_files.insert(file.to_string()))
                {
                    return Err(corrupt(format!(
                        "table directory {} reuses an artifact selected by another table",
                        reference.file_name
                    )));
                }
                if validate_artifacts {
                    group.validate_artifacts(root)?;
                }
            }
            directories.push(directory);
        }
        Ok(directories)
    }

    fn load_active_manifest(root: &Path) -> Result<Option<Self>, ColumnGroupError> {
        let path = root.join(COLUMN_GROUP_MANIFEST_FILE);
        let bytes = match read_bounded(&path, MAX_METADATA_FILE_BYTES, "column-group manifest") {
            Ok(bytes) => bytes,
            Err(ColumnGroupError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        Self::decode(&bytes).map(Some)
    }

    fn encode_body(&self) -> Result<Vec<u8>, ColumnGroupError> {
        let table_count = u32::try_from(self.tables.len())
            .map_err(|_| unsupported("manifest table count exceeds u32".to_string()))?;
        let mut bytes = Vec::new();
        bytes.extend(FORMAT_VERSION.to_le_bytes());
        bytes.extend(self.generation.0.to_le_bytes());
        match self.parent_generation {
            Some(parent) => {
                bytes.push(1);
                bytes.extend([0; 7]);
                bytes.extend(parent.0.to_le_bytes());
            }
            None => {
                bytes.push(0);
                bytes.extend([0; 7]);
                bytes.extend(0u64.to_le_bytes());
            }
        }
        bytes.extend(self.source_commit_epoch.to_le_bytes());
        bytes.extend(table_count.to_le_bytes());
        bytes.extend(0u32.to_le_bytes());
        for reference in &self.tables {
            bytes.push(reference.table.kind as u8);
            bytes.extend([0; 7]);
            bytes.extend(reference.table.table_id.to_le_bytes());
            bytes.extend(reference.directory_generation.0.to_le_bytes());
            bytes.extend(reference.byte_len.to_le_bytes());
            bytes.extend(reference.sha256.as_bytes());
            encode_file_name(&mut bytes, &reference.file_name)?;
        }
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, ColumnGroupError> {
        let body = decode_envelope(MANIFEST_MAGIC, bytes, "column-group manifest")?;
        let mut cursor = Cursor::new(body);
        let version = cursor.read_u32("manifest version")?;
        if version != FORMAT_VERSION {
            return Err(corrupt(format!("unsupported manifest version {version}")));
        }
        let generation = ManifestGeneration(cursor.read_u64("manifest generation")?);
        let has_parent = cursor.read_u8("manifest parent flag")?;
        require_zero(cursor.read_bytes(7, "manifest reserved bytes")?)?;
        let encoded_parent = cursor.read_u64("manifest parent generation")?;
        let parent_generation = match has_parent {
            0 if encoded_parent == 0 => None,
            1 => Some(ManifestGeneration(encoded_parent)),
            _ => return Err(corrupt("manifest parent encoding is invalid".to_string())),
        };
        let source_commit_epoch = cursor.read_u64("manifest source commit epoch")?;
        let table_count = cursor.read_u32("manifest table count")? as usize;
        if table_count > MAX_TABLES {
            return Err(corrupt(format!(
                "manifest table count {table_count} exceeds {MAX_TABLES}"
            )));
        }
        require_zero(cursor.read_bytes(4, "manifest reserved bytes")?)?;
        if table_count > cursor.remaining_len() / MIN_TABLE_REFERENCE_BYTES {
            return Err(corrupt(
                "manifest table count exceeds the remaining bytes".to_string(),
            ));
        }
        let mut tables = Vec::with_capacity(table_count);
        for _ in 0..table_count {
            let kind = ColumnGroupTableKind::from_tag(cursor.read_u8("table kind")?)?;
            require_zero(cursor.read_bytes(7, "table reference reserved bytes")?)?;
            let table_id = cursor.read_u64("table id")?;
            let directory_generation =
                ManifestGeneration(cursor.read_u64("table directory generation")?);
            let byte_len = cursor.read_u64("table directory byte length")?;
            let sha256 = read_sha256(&mut cursor, "table directory SHA-256")?;
            let file_name = decode_file_name(&mut cursor, "table directory")?;
            tables.push(ColumnGroupTableDirectoryRef {
                table: ColumnGroupTableKey { kind, table_id },
                directory_generation,
                file_name,
                byte_len,
                sha256,
            });
        }
        cursor.expect_exhausted("column-group manifest")?;
        let manifest = Self {
            generation,
            parent_generation,
            source_commit_epoch,
            tables,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

/// Selects which tables' referenced artifacts (group files and deletion
/// vectors) are opened and cross-checked while loading table directories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactValidation {
    /// Validate every table's artifacts. Reopen is the fail-closed boundary
    /// and pays footer-proportional cost for the whole catalog.
    AllTables,
    /// Validate only tables published at the candidate manifest generation.
    /// Untouched tables are byte-identical references to already-validated,
    /// SHA-256-bound parent directories, so publish cost stays proportional
    /// to the change volume.
    ChangedTables,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedColumnGroupCatalog {
    manifest: ColumnGroupManifest,
    directories: Vec<ColumnGroupTableDirectory>,
}

impl PublishedColumnGroupCatalog {
    pub fn manifest(&self) -> &ColumnGroupManifest {
        &self.manifest
    }

    pub fn directories(&self) -> &[ColumnGroupTableDirectory] {
        &self.directories
    }

    pub fn directory(&self, key: ColumnGroupTableKey) -> Option<&ColumnGroupTableDirectory> {
        self.directories
            .binary_search_by_key(&key, |directory| directory.table)
            .ok()
            .map(|index| &self.directories[index])
    }

    /// Performs the expensive whole-artifact SHA-256 verification used by
    /// scrub/doctor flows. Normal reopen remains proportional to metadata and
    /// footer size; individual chunk CRC32C checks remain on the read path.
    pub fn scrub_artifacts(&self, root: &Path) -> Result<(), ColumnGroupError> {
        for directory in &self.directories {
            for group in &directory.groups {
                let actual_group_sha256 = hash_file(&root.join(&group.group_file))?;
                if actual_group_sha256 != group.group_sha256 {
                    return Err(corrupt(format!(
                        "column group {} SHA-256 does not match its table directory",
                        group.group_id
                    )));
                }
                if let (Some(file_name), Some(expected)) =
                    (&group.deletion_vector_file, group.deletion_vector_sha256)
                {
                    let actual = hash_file(&root.join(file_name))?;
                    if actual != expected {
                        return Err(corrupt(format!(
                            "deletion vector for group {} SHA-256 does not match its table directory",
                            group.group_id
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

struct PublicationLease(File);

impl PublicationLease {
    fn acquire(root: &Path) -> Result<Self, ColumnGroupError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join(MANIFEST_LOCK_FILE))?;
        file.lock()?;
        Ok(Self(file))
    }
}

impl Drop for PublicationLease {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

/// Removes candidate files orphaned by a crash between candidate creation and
/// the atomic rename. Callers MUST hold the publication lease: candidates are
/// only ever written under it, so any candidate visible here is garbage from
/// a dead publisher, never another publisher's in-flight file. Per-file
/// removal errors are ignored so a transient sharing violation (for example
/// on Windows) cannot fail an otherwise valid publication.
fn remove_orphaned_candidates(root: &Path) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if name.starts_with('.') && name.ends_with(".candidate") {
            let _ = fs::remove_file(entry.path());
        }
    }
}

const REPLACE_RETRY_LIMIT: u32 = 32;
const REPLACE_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(10);

fn publish_bytes(path: &Path, bytes: &[u8]) -> Result<(), ColumnGroupError> {
    let candidate = candidate_path(path);
    let result = (|| {
        let mut file = File::create(&candidate)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        replace_published_file(&candidate, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&candidate);
    }
    result
}

/// Windows denies replacing a destination while a concurrent reader holds it
/// open; readers hold metadata files only for one bounded read, so a bounded
/// retry converts that transient sharing violation into the same
/// atomic-replace outcome POSIX rename provides. Elsewhere the first attempt
/// is the only attempt.
fn replace_published_file(candidate: &Path, path: &Path) -> Result<(), ColumnGroupError> {
    let mut attempt = 0;
    loop {
        match durable_replace_file(candidate, path) {
            Err(error)
                if attempt < REPLACE_RETRY_LIMIT
                    && is_transient_windows_sharing_violation(&error) =>
            {
                attempt += 1;
                std::thread::sleep(REPLACE_RETRY_DELAY);
            }
            result => return result.map_err(ColumnGroupError::from),
        }
    }
}

fn is_transient_windows_sharing_violation(error: &std::io::Error) -> bool {
    // ERROR_ACCESS_DENIED (5) and ERROR_SHARING_VIOLATION (32).
    cfg!(windows) && matches!(error.raw_os_error(), Some(5) | Some(32))
}

fn candidate_path(path: &Path) -> PathBuf {
    let file = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("column-groups");
    path.with_file_name(format!(
        ".{file}.{}-{}.candidate",
        std::process::id(),
        CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

fn encode_envelope(magic: &[u8], body: &[u8]) -> Result<Vec<u8>, ColumnGroupError> {
    if body.len() as u64 > MAX_METADATA_BODY_BYTES {
        return Err(unsupported(format!(
            "column-group metadata body exceeds {MAX_METADATA_BODY_BYTES} bytes"
        )));
    }
    let mut bytes = Vec::with_capacity(envelope_len(magic.len(), body.len()));
    bytes.extend(magic);
    bytes.extend(body);
    bytes.extend((body.len() as u64).to_le_bytes());
    bytes.extend(crc32c(body).get().to_le_bytes());
    bytes.extend(magic);
    Ok(bytes)
}

const fn envelope_len(magic_len: usize, body_len: usize) -> usize {
    magic_len * 2 + FOOTER_FIXED_BYTES + body_len
}

fn decode_envelope<'a>(
    magic: &[u8],
    bytes: &'a [u8],
    what: &str,
) -> Result<&'a [u8], ColumnGroupError> {
    let footer_len = FOOTER_FIXED_BYTES + magic.len();
    let minimum = magic.len() + footer_len;
    if bytes.len() < minimum || &bytes[..magic.len()] != magic {
        return Err(corrupt(format!("{what} header is invalid")));
    }
    let footer_start = bytes.len() - footer_len;
    let footer = &bytes[footer_start..];
    if &footer[FOOTER_FIXED_BYTES..] != magic {
        return Err(corrupt(format!("{what} footer is invalid")));
    }
    let body_len = u64::from_le_bytes(footer[..8].try_into().expect("8 bytes"));
    let stored_crc = u32::from_le_bytes(footer[8..12].try_into().expect("4 bytes"));
    let body = &bytes[magic.len()..footer_start];
    if body_len != body.len() as u64 || crc32c(body).get() != stored_crc {
        return Err(corrupt(format!("{what} checksum or length is invalid")));
    }
    Ok(body)
}

fn encode_file_name(bytes: &mut Vec<u8>, file_name: &str) -> Result<(), ColumnGroupError> {
    validate_file_name(file_name, "artifact")?;
    let length = u32::try_from(file_name.len())
        .map_err(|_| unsupported("artifact file name exceeds u32".to_string()))?;
    bytes.extend(length.to_le_bytes());
    bytes.extend(file_name.as_bytes());
    Ok(())
}

fn decode_file_name(cursor: &mut Cursor<'_>, what: &str) -> Result<String, ColumnGroupError> {
    let length = cursor.read_u32(&format!("{what} file-name length"))? as usize;
    if length > MAX_FILE_NAME_BYTES {
        return Err(corrupt(format!(
            "{what} file name exceeds {MAX_FILE_NAME_BYTES} bytes"
        )));
    }
    let raw = cursor.read_bytes(length, &format!("{what} file name"))?;
    let file_name = std::str::from_utf8(raw)
        .map_err(|_| corrupt(format!("{what} file name is not UTF-8")))?
        .to_string();
    validate_file_name(&file_name, what)?;
    Ok(file_name)
}

fn validate_file_name(file_name: &str, what: &str) -> Result<(), ColumnGroupError> {
    if file_name.is_empty()
        || file_name.len() > MAX_FILE_NAME_BYTES
        || file_name == "."
        || file_name == ".."
        || !file_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(unsupported(format!(
            "{what} file name is not a safe basename"
        )));
    }
    Ok(())
}

fn read_sha256(cursor: &mut Cursor<'_>, what: &str) -> Result<Sha256Digest, ColumnGroupError> {
    let bytes: [u8; SHA256_BYTES] = cursor
        .read_bytes(SHA256_BYTES, what)?
        .try_into()
        .expect("SHA-256 width");
    Ok(Sha256Digest::from_bytes(bytes))
}

fn require_zero(bytes: &[u8]) -> Result<(), ColumnGroupError> {
    if bytes.iter().any(|byte| *byte != 0) {
        return Err(corrupt("reserved metadata bytes are nonzero".to_string()));
    }
    Ok(())
}

fn bounded_file_len(path: &Path, maximum: u64, what: &str) -> Result<u64, ColumnGroupError> {
    let length = fs::metadata(path)?.len();
    if length > maximum {
        return Err(corrupt(format!(
            "{what} holds {length} bytes, exceeding {maximum}"
        )));
    }
    Ok(length)
}

fn read_bounded(path: &Path, maximum: u64, what: &str) -> Result<Vec<u8>, ColumnGroupError> {
    // Open first and stat the descriptor: length and bytes then come from the
    // same inode, so a concurrent atomic rename over `path` (a publish racing
    // this reader) can never make the length check fail spuriously.
    let file = File::open(path)?;
    let length = file.metadata()?.len();
    if length > maximum {
        return Err(corrupt(format!(
            "{what} holds {length} bytes, exceeding {maximum}"
        )));
    }
    let capacity = usize::try_from(length)
        .map_err(|_| corrupt(format!("{what} length exceeds addressable memory")))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length {
        return Err(corrupt(format!("{what} changed while it was being read")));
    }
    Ok(bytes)
}

fn hash_file(path: &Path) -> Result<Sha256Digest, ColumnGroupError> {
    let mut file = File::open(path)?;
    let mut buffer = [0u8; 64 * 1024];
    let mut hasher = IntegrityHasher::new();
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finish().sha256)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_group::group::ColumnGroupWriter;
    use crate::column_group::DeletionVectorBinding;
    use skein_core::{PropertyId, Value};
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn publishes_reopens_and_reuses_untouched_table_directory() {
        let root = unique_dir("round-trip");
        fs::create_dir_all(&root).unwrap();
        let node = table_with_group(&root, ColumnGroupTableKind::Node, 1, 1, 10, None);
        let relational = table_with_group(&root, ColumnGroupTableKind::Relational, 2, 1, 20, None);
        let node_ref = node.write_immutable(&root).unwrap();
        let relational_ref = relational.write_immutable(&root).unwrap();
        let first = ColumnGroupManifest::new(
            ManifestGeneration(1),
            None,
            7,
            vec![node_ref.clone(), relational_ref.clone()],
        )
        .unwrap();
        first.publish(&root).unwrap();

        let node_v2 = table_with_group(&root, ColumnGroupTableKind::Node, 1, 2, 11, Some((1, 1)));
        let node_v2_ref = node_v2.write_immutable(&root).unwrap();
        let second = ColumnGroupManifest::new(
            ManifestGeneration(2),
            Some(ManifestGeneration(1)),
            9,
            vec![node_v2_ref, relational_ref.clone()],
        )
        .unwrap();
        second.publish(&root).unwrap();

        let reopened = ColumnGroupManifest::open(&root).unwrap().unwrap();
        assert_eq!(reopened.manifest().generation(), ManifestGeneration(2));
        assert_eq!(reopened.manifest().source_commit_epoch(), 9);
        assert_eq!(reopened.directories().len(), 2);
        assert_eq!(
            reopened
                .manifest()
                .table(ColumnGroupTableKey::new(
                    ColumnGroupTableKind::Relational,
                    2
                ))
                .unwrap()
                .directory_generation(),
            ManifestGeneration(1)
        );
        let node = reopened
            .directory(ColumnGroupTableKey::new(ColumnGroupTableKind::Node, 1))
            .unwrap();
        assert_eq!(node.publication_generation(), ManifestGeneration(2));
        assert_eq!(node.groups()[0].deleted_count(), 1);
        assert_eq!(node.groups()[0].visible_count(), 2);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn stale_publishers_are_serialized_and_one_fails_closed() {
        let root = Arc::new(unique_dir("stale-publish"));
        fs::create_dir_all(root.as_ref()).unwrap();
        let directory = table_with_group(root.as_ref(), ColumnGroupTableKind::Node, 1, 1, 1, None);
        let reference = directory.write_immutable(root.as_ref()).unwrap();
        ColumnGroupManifest::new(ManifestGeneration(1), None, 1, vec![reference.clone()])
            .unwrap()
            .publish(root.as_ref())
            .unwrap();

        let directory = table_with_group(root.as_ref(), ColumnGroupTableKind::Node, 1, 2, 2, None);
        let reference = directory.write_immutable(root.as_ref()).unwrap();
        let first_candidate = Arc::new(
            ColumnGroupManifest::new(
                ManifestGeneration(2),
                Some(ManifestGeneration(1)),
                2,
                vec![reference.clone()],
            )
            .unwrap(),
        );
        let second_candidate = Arc::new(
            ColumnGroupManifest::new(
                ManifestGeneration(2),
                Some(ManifestGeneration(1)),
                3,
                vec![reference],
            )
            .unwrap(),
        );
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for candidate in [first_candidate, second_candidate] {
            let root = Arc::clone(&root);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                candidate.publish(root.as_ref())
            }));
        }
        let results = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(
                    result,
                    Err(ColumnGroupError::StaleManifestGeneration { .. })
                ))
                .count(),
            1
        );
        assert_eq!(
            ColumnGroupManifest::open(root.as_ref())
                .unwrap()
                .unwrap()
                .manifest()
                .generation(),
            ManifestGeneration(2)
        );
        fs::remove_dir_all(root.as_ref()).unwrap();
    }

    #[test]
    fn identical_publish_retry_is_idempotent() {
        let root = unique_dir("idempotent-publish");
        fs::create_dir_all(&root).unwrap();
        let directory = table_with_group(&root, ColumnGroupTableKind::Node, 1, 1, 1, None);
        let reference = directory.write_immutable(&root).unwrap();
        let manifest =
            ColumnGroupManifest::new(ManifestGeneration(1), None, 1, vec![reference]).unwrap();
        manifest.publish(&root).unwrap();
        manifest.publish(&root).unwrap();
        assert_eq!(
            ColumnGroupManifest::open(&root)
                .unwrap()
                .unwrap()
                .manifest(),
            &manifest
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn deep_scrub_detects_payload_corruption_not_read_by_reopen() {
        let root = unique_dir("deep-scrub");
        fs::create_dir_all(&root).unwrap();
        let directory = table_with_group(&root, ColumnGroupTableKind::Node, 1, 1, 1, None);
        let group_file = directory.groups()[0].group_file().to_string();
        let reference = directory.write_immutable(&root).unwrap();
        ColumnGroupManifest::new(ManifestGeneration(1), None, 1, vec![reference])
            .unwrap()
            .publish(&root)
            .unwrap();

        let path = root.join(group_file);
        let mut bytes = fs::read(&path).unwrap();
        bytes[super::super::COLUMN_GROUP_MAGIC.len()] ^= 0x40;
        fs::write(path, bytes).unwrap();
        let reopened = ColumnGroupManifest::open(&root).unwrap().unwrap();
        assert!(matches!(
            reopened.scrub_artifacts(&root),
            Err(ColumnGroupError::Corrupt(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn metadata_file_budget_includes_the_checksumming_envelope() {
        assert_eq!(
            envelope_len(
                TABLE_DIRECTORY_MAGIC.len(),
                MAX_METADATA_BODY_BYTES as usize
            ) as u64,
            MAX_METADATA_FILE_BYTES
        );
        assert!(
            envelope_len(MANIFEST_MAGIC.len(), MAX_METADATA_BODY_BYTES as usize) as u64
                <= MAX_METADATA_FILE_BYTES
        );
    }

    #[test]
    fn orphan_candidate_is_ignored_and_corrupt_published_metadata_fails_closed() {
        let root = unique_dir("crash-boundary");
        fs::create_dir_all(&root).unwrap();
        let directory = table_with_group(&root, ColumnGroupTableKind::Relationship, 7, 1, 1, None);
        let reference = directory.write_immutable(&root).unwrap();
        let first = ColumnGroupManifest::new(ManifestGeneration(1), None, 1, vec![reference]);
        first.unwrap().publish(&root).unwrap();

        fs::write(
            candidate_path(&root.join(COLUMN_GROUP_MANIFEST_FILE)),
            b"torn candidate",
        )
        .unwrap();
        assert_eq!(
            ColumnGroupManifest::open(&root)
                .unwrap()
                .unwrap()
                .manifest()
                .generation(),
            ManifestGeneration(1)
        );

        let path = root.join(COLUMN_GROUP_MANIFEST_FILE);
        let mut bytes = fs::read(&path).unwrap();
        bytes[MANIFEST_MAGIC.len() + 3] ^= 0x80;
        fs::write(path, bytes).unwrap();
        assert!(matches!(
            ColumnGroupManifest::open(&root),
            Err(ColumnGroupError::Corrupt(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn corrupt_referenced_table_directory_fails_closed() {
        let root = unique_dir("corrupt-directory");
        fs::create_dir_all(&root).unwrap();
        let directory = table_with_group(&root, ColumnGroupTableKind::Node, 1, 1, 1, None);
        let directory_file = directory.file_name();
        let reference = directory.write_immutable(&root).unwrap();
        ColumnGroupManifest::new(ManifestGeneration(1), None, 1, vec![reference])
            .unwrap()
            .publish(&root)
            .unwrap();

        let path = root.join(directory_file);
        let mut bytes = fs::read(&path).unwrap();
        bytes[TABLE_DIRECTORY_MAGIC.len() + 1] ^= 0x20;
        fs::write(path, bytes).unwrap();
        assert!(matches!(
            ColumnGroupManifest::open(&root),
            Err(ColumnGroupError::Corrupt(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn changed_table_must_use_the_candidate_generation() {
        let root = unique_dir("changed-generation");
        fs::create_dir_all(&root).unwrap();
        let directory = table_with_group(&root, ColumnGroupTableKind::Node, 1, 1, 1, None);
        let first_ref = directory.write_immutable(&root).unwrap();
        ColumnGroupManifest::new(ManifestGeneration(1), None, 1, vec![first_ref.clone()])
            .unwrap()
            .publish(&root)
            .unwrap();

        let mut stale_ref = first_ref;
        stale_ref.file_name = "different-old-directory.skein".to_string();
        fs::copy(
            root.join(directory.file_name()),
            root.join(&stale_ref.file_name),
        )
        .unwrap();
        let candidate = ColumnGroupManifest::new(
            ManifestGeneration(2),
            Some(ManifestGeneration(1)),
            2,
            vec![stale_ref],
        )
        .unwrap();
        assert!(matches!(
            candidate.publish(&root),
            Err(ColumnGroupError::Unsupported(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn decode_rejects_file_names_with_path_separators_despite_valid_checksum() {
        for file_name in ["../escape", "a/b", "a\\b"] {
            let bytes = encode_envelope(
                TABLE_DIRECTORY_MAGIC,
                &directory_body_with_group_file_name(file_name),
            )
            .unwrap();
            assert!(
                matches!(
                    ColumnGroupTableDirectory::decode(&bytes),
                    Err(ColumnGroupError::Unsupported(_))
                ),
                "file name {file_name:?} must be rejected by the basename whitelist"
            );
        }
    }

    /// Encodes a syntactically well-formed table-directory body whose single
    /// group references `file_name`, bypassing the encoder's own validation.
    fn directory_body_with_group_file_name(file_name: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend(FORMAT_VERSION.to_le_bytes());
        body.push(ColumnGroupTableKind::Node as u8);
        body.extend([0; 3]);
        body.extend(1u64.to_le_bytes()); // table id
        body.extend(1u64.to_le_bytes()); // publication generation
        body.extend(1u32.to_le_bytes()); // group count
        body.extend(0u32.to_le_bytes()); // reserved
        body.extend(1u64.to_le_bytes()); // group id
        body.extend(1u64.to_le_bytes()); // group generation
        body.extend(0u32.to_le_bytes()); // row count
        body.extend(0u32.to_le_bytes()); // deleted count
        body.extend(0u64.to_le_bytes()); // min id
        body.extend(0u64.to_le_bytes()); // max id
        body.extend(64u64.to_le_bytes()); // group byte length
        body.extend([0u8; SHA256_BYTES]); // group SHA-256
        body.extend((file_name.len() as u32).to_le_bytes());
        body.extend(file_name.as_bytes());
        body.push(0); // no deletion vector
        body.extend([0; 7]);
        body
    }

    #[test]
    fn publication_sweeps_orphaned_candidate_files_under_the_lease() {
        let root = unique_dir("orphan-sweep");
        fs::create_dir_all(&root).unwrap();
        let write_orphan = |name: &str| {
            let path = root.join(name);
            fs::write(&path, b"torn candidate").unwrap();
            path
        };
        let before_directory = write_orphan(".table-stale.skein.999-0.candidate");
        let directory = table_with_group(&root, ColumnGroupTableKind::Node, 1, 1, 1, None);
        let reference = directory.write_immutable(&root).unwrap();
        assert!(
            !before_directory.exists(),
            "write_immutable must sweep orphaned candidates"
        );

        let before_publish = write_orphan(&format!(
            ".{COLUMN_GROUP_MANIFEST_FILE}.{}-99999.candidate",
            std::process::id()
        ));
        let kept = root.join("candidate.notes"); // no leading dot, wrong suffix
        fs::write(&kept, b"unrelated").unwrap();
        ColumnGroupManifest::new(ManifestGeneration(1), None, 1, vec![reference])
            .unwrap()
            .publish(&root)
            .unwrap();
        assert!(
            !before_publish.exists(),
            "publish must sweep orphaned candidates"
        );
        assert!(kept.exists(), "non-candidate files must be left alone");
        assert!(ColumnGroupManifest::open(&root).unwrap().is_some());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn publish_skips_unchanged_tables_artifacts_while_reopen_fails_closed() {
        let root = unique_dir("unchanged-artifact-skip");
        fs::create_dir_all(&root).unwrap();
        let node = table_with_group(&root, ColumnGroupTableKind::Node, 1, 1, 1, None);
        let relational = table_with_group(&root, ColumnGroupTableKind::Relational, 2, 1, 20, None);
        let relational_group_file = relational.groups()[0].group_file().to_string();
        let node_ref = node.write_immutable(&root).unwrap();
        let relational_ref = relational.write_immutable(&root).unwrap();
        ColumnGroupManifest::new(
            ManifestGeneration(1),
            None,
            1,
            vec![node_ref, relational_ref.clone()],
        )
        .unwrap()
        .publish(&root)
        .unwrap();

        // Corrupt the unchanged relational table's group artifact footer in
        // place (same length, broken footer magic). If publish opened the
        // unchanged table's artifacts, it would fail closed here.
        let path = root.join(relational_group_file);
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x40;
        fs::write(path, bytes).unwrap();

        let node_v2 = table_with_group(&root, ColumnGroupTableKind::Node, 1, 2, 2, None);
        let node_v2_ref = node_v2.write_immutable(&root).unwrap();
        ColumnGroupManifest::new(
            ManifestGeneration(2),
            Some(ManifestGeneration(1)),
            2,
            vec![node_v2_ref, relational_ref],
        )
        .unwrap()
        .publish(&root)
        .expect("publish changing only the node table must not open unchanged artifacts");

        assert!(
            matches!(
                ColumnGroupManifest::open(&root),
                Err(ColumnGroupError::Corrupt(_))
            ),
            "reopen must keep validating every table's artifacts"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn concurrent_reopen_always_observes_a_complete_published_catalog() {
        const LAST_GENERATION: u64 = 50;
        let root = Arc::new(unique_dir("publish-reopen-race"));
        fs::create_dir_all(root.as_ref()).unwrap();
        let directory = table_with_group(root.as_ref(), ColumnGroupTableKind::Node, 1, 1, 1, None);
        let reference = directory.write_immutable(root.as_ref()).unwrap();
        ColumnGroupManifest::new(ManifestGeneration(1), None, 1, vec![reference])
            .unwrap()
            .publish(root.as_ref())
            .unwrap();

        let done = Arc::new(AtomicBool::new(false));
        let reader = {
            let root = Arc::clone(&root);
            let done = Arc::clone(&done);
            thread::spawn(move || {
                let mut observed = 0u64;
                while !done.load(Ordering::Acquire) {
                    let catalog = ColumnGroupManifest::open(root.as_ref())
                        .expect("concurrent reopen must never fail")
                        .expect("a manifest is always published");
                    let generation = catalog.manifest().generation().0;
                    assert!(
                        (1..=LAST_GENERATION).contains(&generation),
                        "observed unpublished generation {generation}"
                    );
                    assert_eq!(
                        catalog.directories().len(),
                        catalog.manifest().tables().len(),
                        "catalog must be complete"
                    );
                    for (directory, reference) in catalog
                        .directories()
                        .iter()
                        .zip(catalog.manifest().tables())
                    {
                        assert_eq!(directory.table(), reference.table());
                        assert_eq!(
                            directory.publication_generation(),
                            reference.directory_generation()
                        );
                    }
                    observed += 1;
                }
                observed
            })
        };
        for generation in 2..=LAST_GENERATION {
            let directory = table_with_group(
                root.as_ref(),
                ColumnGroupTableKind::Node,
                1,
                generation,
                generation,
                None,
            );
            let reference = directory.write_immutable(root.as_ref()).unwrap();
            ColumnGroupManifest::new(
                ManifestGeneration(generation),
                Some(ManifestGeneration(generation - 1)),
                generation,
                vec![reference],
            )
            .unwrap()
            .publish(root.as_ref())
            .unwrap();
        }
        done.store(true, Ordering::Release);
        let observed = reader.join().unwrap();
        assert!(observed > 0, "the reader must have reopened at least once");
        fs::remove_dir_all(root.as_ref()).unwrap();
    }

    fn table_with_group(
        root: &Path,
        kind: ColumnGroupTableKind,
        table_id: u64,
        publication_generation: u64,
        group_id: u64,
        deletion: Option<(u64, u32)>,
    ) -> ColumnGroupTableDirectory {
        let group_generation =
            deletion.map_or(publication_generation, |(generation, _)| generation);
        let group_file = format!("group-{group_id}-{group_generation}.skein");
        if !root.join(&group_file).exists() {
            ColumnGroupWriter::default()
                .write(
                    &root.join(&group_file),
                    group_id,
                    ManifestGeneration(group_generation),
                    &[10, 20, 30],
                    &[(
                        PropertyId(1),
                        vec![Value::Int(1), Value::Int(2), Value::Int(3)],
                    )],
                )
                .unwrap();
        }
        let deletion_file = deletion.map(|(group_generation, row)| {
            let file = format!("group-{group_id}-dv-{publication_generation}.skein");
            let binding = DeletionVectorBinding::new(
                group_id,
                ManifestGeneration(group_generation),
                ManifestGeneration(publication_generation),
                3,
            )
            .unwrap();
            let mut vector = DeletionVector::new(binding);
            vector.mark_deleted(row).unwrap();
            vector.write(&root.join(&file)).unwrap();
            file
        });
        let descriptor =
            ColumnGroupArtifactDescriptor::inspect(root, group_file, deletion_file).unwrap();
        ColumnGroupTableDirectory::new(
            ColumnGroupTableKey::new(kind, table_id),
            ManifestGeneration(publication_generation),
            vec![descriptor],
        )
        .unwrap()
    }

    fn unique_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein-column-manifest-{name}-{}-{nonce}",
            std::process::id()
        ))
    }
}
