//! Bounded owner-scoped publication for deterministic derived projections.
//!
//! Producers append one canonically ordered candidate in bounded batches,
//! verify its complete streaming digest, and publish one immutable head. Normal
//! readers can open only the selected head, so candidate batches never become
//! partial logical changes.

use crate::durability::{durable_replace_file, sync_directory, sync_parent_directory};
use crate::relational::{
    decode_relational_row_payload, encode_relational_row_payload, validate_row, RelationalKey,
    RelationalRow, RelationalTableSchema,
};
use crate::{decode_relational_primary_key, encode_relational_primary_key};
use skein_integrity::{crc32c, Crc32c, IntegrityDigest, IntegrityHasher, Sha256Digest};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

const DATA_MAGIC: &[u8; 8] = b"SKPRGD01";
const MANIFEST_MAGIC: &[u8; 8] = b"SKPRGM01";
const HEAD_MAGIC: &[u8; 8] = b"SKPRGH01";
const PROTOCOL_VERSION: u32 = 1;
const DATA_SUFFIX: &str = "data";
const MANIFEST_SUFFIX: &str = "manifest";
const HEAD_SUFFIX: &str = "head";
const MAX_COLLECTION_BYTES: usize = 1_024;
const MAX_KEY_BYTES: usize = 64 * 1024;
const MAX_MEMBER_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const MAX_ENVELOPE_BODY_BYTES: usize = 16 * 1024 * 1024;
const ENVELOPE_OVERHEAD_BYTES: usize = 8 + 4 + 4 + 4;
const MAX_ENVELOPE_BYTES: usize = MAX_ENVELOPE_BODY_BYTES + ENVELOPE_OVERHEAD_BYTES;
const MAX_ENCODED_MEMBER_BYTES: usize =
    MAX_COLLECTION_BYTES + MAX_KEY_BYTES + MAX_MEMBER_PAYLOAD_BYTES + 32;

#[derive(Debug)]
pub enum ProjectionGenerationError {
    Io(io::Error),
    Admission(String),
    Conflict(String),
    Corruption(String),
    NotFound(String),
}

impl Display for ProjectionGenerationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "projection generation I/O failed: {error}"),
            Self::Admission(message) => {
                write!(
                    formatter,
                    "projection generation admission failed: {message}"
                )
            }
            Self::Conflict(message) => {
                write!(formatter, "projection generation conflict: {message}")
            }
            Self::Corruption(message) => {
                write!(formatter, "projection generation corruption: {message}")
            }
            Self::NotFound(message) => {
                write!(formatter, "projection generation not found: {message}")
            }
        }
    }
}

impl std::error::Error for ProjectionGenerationError {}

impl From<io::Error> for ProjectionGenerationError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectionGenerationIdentity {
    pub projection: String,
    pub owner_key: Vec<u8>,
    pub generation: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionGenerationBegin {
    pub identity: ProjectionGenerationIdentity,
    pub source_watermark: u64,
    pub projection_version: u64,
    pub expected_head: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionGenerationMember {
    pub collection: String,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}

/// Encodes one relational row as a storage-neutral projection member.
///
/// The collection is the relational table name, the member key is the
/// canonical ordered primary-key encoding, and the payload uses Skein's
/// versioned relational row codec. Query bindings decode the same member
/// against the durable table schema before exposing it to PostgreSQL SQL.
pub fn encode_projection_relational_member(
    schema: &RelationalTableSchema,
    row: RelationalRow,
) -> Result<ProjectionGenerationMember, ProjectionGenerationError> {
    validate_row(schema, &row).map_err(|error| {
        ProjectionGenerationError::Admission(format!(
            "projection row for {} is invalid: {error}",
            schema.name
        ))
    })?;
    let key = schema
        .primary_key
        .iter()
        .map(|column| {
            let position = schema.column_position(column).ok_or_else(|| {
                ProjectionGenerationError::Corruption(format!(
                    "projection table {} has an unknown primary-key column {column}",
                    schema.name
                ))
            })?;
            Ok(row.values()[position].clone())
        })
        .collect::<Result<Vec<_>, ProjectionGenerationError>>()?;
    let key = encode_relational_primary_key(&RelationalKey(key)).map_err(|error| {
        ProjectionGenerationError::Admission(format!(
            "projection primary key for {} cannot be encoded: {error}",
            schema.name
        ))
    })?;
    let payload = encode_relational_row_payload(&row).map_err(|error| {
        ProjectionGenerationError::Admission(format!(
            "projection row for {} cannot be encoded: {error}",
            schema.name
        ))
    })?;
    Ok(ProjectionGenerationMember {
        collection: schema.name.clone(),
        key,
        payload,
    })
}

/// Decodes and verifies one relational projection member against a table
/// schema. Both the row shape and the independently encoded primary key must
/// agree, so a corrupt payload cannot be attached to another logical row.
pub fn decode_projection_relational_member(
    schema: &RelationalTableSchema,
    member: &ProjectionGenerationMember,
    max_value_bytes: usize,
) -> Result<(RelationalKey, RelationalRow), ProjectionGenerationError> {
    if member.collection != schema.name {
        return Err(ProjectionGenerationError::Corruption(format!(
            "projection collection {} does not match relational table {}",
            member.collection, schema.name
        )));
    }
    let row = decode_relational_row_payload(&member.payload, schema.columns.len(), max_value_bytes)
        .map_err(|error| {
            ProjectionGenerationError::Corruption(format!(
                "projection row for {} cannot be decoded: {error}",
                schema.name
            ))
        })?;
    validate_row(schema, &row).map_err(|error| {
        ProjectionGenerationError::Corruption(format!(
            "projection row for {} is invalid: {error}",
            schema.name
        ))
    })?;
    let key = decode_relational_primary_key(&member.key).map_err(|error| {
        ProjectionGenerationError::Corruption(format!(
            "projection primary key for {} cannot be decoded: {error}",
            schema.name
        ))
    })?;
    let expected = schema
        .primary_key
        .iter()
        .map(|column| {
            let position = schema.column_position(column).ok_or_else(|| {
                ProjectionGenerationError::Corruption(format!(
                    "projection table {} has an unknown primary-key column {column}",
                    schema.name
                ))
            })?;
            Ok(row.values()[position].clone())
        })
        .collect::<Result<Vec<_>, ProjectionGenerationError>>()?;
    if key.0 != expected {
        return Err(ProjectionGenerationError::Corruption(format!(
            "projection member key does not match the decoded row for table {}",
            schema.name
        )));
    }
    Ok((key, row))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionGenerationBatchLimits {
    pub max_rows: NonZeroUsize,
    pub max_payload_bytes: NonZeroUsize,
    pub max_collection_bytes: NonZeroUsize,
    pub max_key_bytes: NonZeroUsize,
}

impl Default for ProjectionGenerationBatchLimits {
    fn default() -> Self {
        Self {
            max_rows: NonZeroUsize::new(1_024).expect("default batch rows are non-zero"),
            max_payload_bytes: NonZeroUsize::new(8 * 1024 * 1024)
                .expect("default batch bytes are non-zero"),
            max_collection_bytes: NonZeroUsize::new(1_024)
                .expect("default collection bytes are non-zero"),
            max_key_bytes: NonZeroUsize::new(64 * 1024).expect("default key bytes are non-zero"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionGenerationSeal {
    pub expected_member_count: u64,
    pub expected_payload_bytes: u64,
    pub expected_digest: IntegrityDigest,
    pub rollup_metadata: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionGenerationManifest {
    pub begin: ProjectionGenerationBegin,
    pub member_count: u64,
    pub payload_bytes: u64,
    pub content_digest: IntegrityDigest,
    pub data_bytes: u64,
    pub rollup_metadata: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedProjectionGeneration {
    manifest: ProjectionGenerationManifest,
    idempotent: bool,
}

impl SealedProjectionGeneration {
    pub fn manifest(&self) -> &ProjectionGenerationManifest {
        &self.manifest
    }

    pub const fn idempotent(&self) -> bool {
        self.idempotent
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionGenerationPublishReport {
    pub identity: ProjectionGenerationIdentity,
    pub previous_generation: Option<String>,
    pub publication_commit_epoch: u64,
    pub idempotent: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionGenerationState {
    Staging,
    Abandoned,
    Sealed,
    Published,
    Failed,
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionGenerationStatus {
    pub identity: ProjectionGenerationIdentity,
    pub state: ProjectionGenerationState,
    pub active_generation: Option<String>,
    pub source_watermark: Option<u64>,
    pub member_count: Option<u64>,
    pub payload_bytes: Option<u64>,
    pub publication_commit_epoch: Option<u64>,
    pub pinned: bool,
    pub blocker: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionGenerationReadLimits {
    pub max_rows: NonZeroUsize,
    pub max_payload_bytes: NonZeroUsize,
    pub max_record_bytes: NonZeroUsize,
}

impl Default for ProjectionGenerationReadLimits {
    fn default() -> Self {
        Self {
            max_rows: NonZeroUsize::new(1_024).expect("default read rows are non-zero"),
            max_payload_bytes: NonZeroUsize::new(8 * 1024 * 1024)
                .expect("default read bytes are non-zero"),
            max_record_bytes: NonZeroUsize::new(8 * 1024 * 1024)
                .expect("default record bytes are non-zero"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionGenerationCursor {
    generation: String,
    offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionGenerationPage {
    pub members: Vec<ProjectionGenerationMember>,
    pub next: Option<ProjectionGenerationCursor>,
    pub report: ProjectionGenerationReadReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionGenerationReadReport {
    pub generation: String,
    pub source_watermark: u64,
    pub projection_version: u64,
    pub publication_commit_epoch: u64,
    pub rows_returned: usize,
    pub payload_bytes: usize,
    pub complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionGenerationGcLimits {
    pub max_files_to_scan: NonZeroUsize,
    pub max_generations_to_reclaim: NonZeroUsize,
    pub max_bytes_to_reclaim: NonZeroUsize,
}

impl Default for ProjectionGenerationGcLimits {
    fn default() -> Self {
        Self {
            max_files_to_scan: NonZeroUsize::new(1_024).expect("default GC scan is non-zero"),
            max_generations_to_reclaim: NonZeroUsize::new(64)
                .expect("default GC generations are non-zero"),
            max_bytes_to_reclaim: NonZeroUsize::new(64 * 1024 * 1024)
                .expect("default GC bytes are non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProjectionGenerationGcReport {
    pub files_scanned: usize,
    pub generations_reclaimed: usize,
    pub bytes_reclaimed: u64,
    pub pinned_generations_skipped: usize,
    pub active_generations_skipped: usize,
    pub backlog_remaining: bool,
}

#[derive(Clone, Debug)]
pub struct ProjectionGenerationStore {
    root: PathBuf,
    read_only: bool,
    shared: Arc<Mutex<ProjectionGenerationShared>>,
}

#[derive(Debug, Default)]
struct ProjectionGenerationShared {
    open_writers: BTreeSet<String>,
    pinned: BTreeMap<String, usize>,
    temporary_sequence: u64,
}

impl ProjectionGenerationStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, ProjectionGenerationError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("generations"))?;
        fs::create_dir_all(root.join("heads"))?;
        sync_directory(&root)?;
        Ok(Self {
            root,
            read_only: false,
            shared: Arc::new(Mutex::new(ProjectionGenerationShared::default())),
        })
    }

    pub fn open_existing(root: impl AsRef<Path>) -> Result<Self, ProjectionGenerationError> {
        let root = root.as_ref().to_path_buf();
        if !root.join("generations").is_dir() || !root.join("heads").is_dir() {
            return Err(ProjectionGenerationError::NotFound(format!(
                "projection generation catalog {} does not exist",
                root.display()
            )));
        }
        Ok(Self {
            root,
            read_only: true,
            shared: Arc::new(Mutex::new(ProjectionGenerationShared::default())),
        })
    }

    pub fn begin_candidate(
        &self,
        begin: ProjectionGenerationBegin,
        limits: ProjectionGenerationBatchLimits,
    ) -> Result<ProjectionGenerationWriter, ProjectionGenerationError> {
        self.ensure_writable()?;
        validate_begin(&begin)?;
        validate_batch_limits(limits)?;
        let key = identity_key(&begin.identity);
        {
            let mut shared = self.lock_shared()?;
            if !shared.open_writers.insert(key.clone()) {
                return Err(ProjectionGenerationError::Conflict(format!(
                    "generation {} already has an open writer",
                    begin.identity.generation
                )));
            }
        }
        match ProjectionGenerationWriter::open(self.clone(), begin, limits, key.clone()) {
            Ok(writer) => Ok(writer),
            Err(error) => {
                if let Ok(mut shared) = self.lock_shared() {
                    shared.open_writers.remove(&key);
                }
                Err(error)
            }
        }
    }

    pub fn publish(
        &self,
        sealed: &SealedProjectionGeneration,
    ) -> Result<ProjectionGenerationPublishReport, ProjectionGenerationError> {
        self.ensure_writable()?;
        let mut shared = self.lock_shared()?;
        let manifest = read_manifest(&self.manifest_path(&sealed.manifest.begin.identity))?;
        if manifest != sealed.manifest {
            return Err(ProjectionGenerationError::Conflict(
                "sealed generation token does not match durable candidate metadata".to_string(),
            ));
        }
        let head_path = self.head_path(
            &manifest.begin.identity.projection,
            &manifest.begin.identity.owner_key,
        );
        let current = read_optional_head(&head_path)?;
        if let Some(current) = &current {
            validate_head_owner(current, &manifest.begin.identity)?;
            if current.manifest.begin.identity.generation == manifest.begin.identity.generation {
                if current.manifest != manifest {
                    return Err(ProjectionGenerationError::Conflict(
                        "published generation identity was reused with different content"
                            .to_string(),
                    ));
                }
                return Ok(ProjectionGenerationPublishReport {
                    identity: manifest.begin.identity,
                    previous_generation: Some(current.manifest.begin.identity.generation.clone()),
                    publication_commit_epoch: current.publication_commit_epoch,
                    idempotent: true,
                });
            }
            if manifest.begin.source_watermark < current.manifest.begin.source_watermark {
                return Err(ProjectionGenerationError::Conflict(format!(
                    "source watermark {} is older than active watermark {}",
                    manifest.begin.source_watermark, current.manifest.begin.source_watermark
                )));
            }
        }
        let observed_head = current
            .as_ref()
            .map(|head| head.manifest.begin.identity.generation.as_str());
        if observed_head != manifest.begin.expected_head.as_deref() {
            return Err(ProjectionGenerationError::Conflict(format!(
                "expected head {:?}, observed {:?}",
                manifest.begin.expected_head, observed_head
            )));
        }
        let publication_commit_epoch = current
            .as_ref()
            .map_or(1, |head| head.publication_commit_epoch.saturating_add(1));
        let head = ProjectionGenerationHead {
            publication_commit_epoch,
            manifest: manifest.clone(),
        };
        let bytes = encode_head(&head)?;
        let temporary = temporary_path(&head_path, &mut shared);
        write_synchronized(&temporary, &bytes)?;
        durable_replace_file(&temporary, &head_path)?;
        Ok(ProjectionGenerationPublishReport {
            identity: manifest.begin.identity,
            previous_generation: current.map(|head| head.manifest.begin.identity.generation),
            publication_commit_epoch,
            idempotent: false,
        })
    }

    pub fn open_active(
        &self,
        projection: &str,
        owner_key: &[u8],
    ) -> Result<ProjectionGenerationReader, ProjectionGenerationError> {
        let head_path = self.head_path(projection, owner_key);
        let head = read_optional_head(&head_path)?.ok_or_else(|| {
            ProjectionGenerationError::NotFound(format!(
                "projection {projection} has no active generation for the requested owner"
            ))
        })?;
        if head.manifest.begin.identity.projection != projection
            || head.manifest.begin.identity.owner_key != owner_key
        {
            return Err(ProjectionGenerationError::Corruption(
                "owner head hash resolved to a different owner".to_string(),
            ));
        }
        let manifest = read_manifest(&self.manifest_path(&head.manifest.begin.identity))?;
        if manifest != head.manifest {
            return Err(ProjectionGenerationError::Corruption(
                "active head does not match its immutable generation manifest".to_string(),
            ));
        }
        let key = identity_key(&manifest.begin.identity);
        {
            let mut shared = self.lock_shared()?;
            let count = shared.pinned.entry(key.clone()).or_default();
            *count = count.checked_add(1).ok_or_else(|| {
                ProjectionGenerationError::Admission(
                    "projection generation pin count overflow".to_string(),
                )
            })?;
        }
        match ProjectionGenerationReader::open(
            self.clone(),
            head.publication_commit_epoch,
            manifest,
            key.clone(),
        ) {
            Ok(reader) => Ok(reader),
            Err(error) => {
                if let Ok(mut shared) = self.lock_shared() {
                    release_pin(&mut shared, &key);
                }
                Err(error)
            }
        }
    }

    pub fn status(
        &self,
        identity: &ProjectionGenerationIdentity,
    ) -> Result<ProjectionGenerationStatus, ProjectionGenerationError> {
        let head = read_optional_head(&self.head_path(&identity.projection, &identity.owner_key))?;
        let active_generation = head
            .as_ref()
            .map(|head| head.manifest.begin.identity.generation.clone());
        let publication_commit_epoch = head.as_ref().map(|head| head.publication_commit_epoch);
        let manifest_path = self.manifest_path(identity);
        let data_path = self.data_path(identity);
        let key = identity_key(identity);
        let pinned = self.lock_shared()?.pinned.contains_key(&key);
        match read_optional_manifest(&manifest_path) {
            Ok(Some(manifest)) => Ok(ProjectionGenerationStatus {
                identity: identity.clone(),
                state: if active_generation.as_deref() == Some(identity.generation.as_str()) {
                    ProjectionGenerationState::Published
                } else {
                    ProjectionGenerationState::Sealed
                },
                active_generation,
                source_watermark: Some(manifest.begin.source_watermark),
                member_count: Some(manifest.member_count),
                payload_bytes: Some(manifest.payload_bytes),
                publication_commit_epoch,
                pinned,
                blocker: None,
            }),
            Ok(None) if data_path.exists() => Ok(ProjectionGenerationStatus {
                identity: identity.clone(),
                state: if self.lock_shared()?.open_writers.contains(&key) {
                    ProjectionGenerationState::Staging
                } else {
                    ProjectionGenerationState::Abandoned
                },
                active_generation,
                source_watermark: None,
                member_count: None,
                payload_bytes: None,
                publication_commit_epoch,
                pinned,
                blocker: None,
            }),
            Ok(None) => Ok(ProjectionGenerationStatus {
                identity: identity.clone(),
                state: ProjectionGenerationState::Missing,
                active_generation,
                source_watermark: None,
                member_count: None,
                payload_bytes: None,
                publication_commit_epoch,
                pinned,
                blocker: None,
            }),
            Err(error) => Ok(ProjectionGenerationStatus {
                identity: identity.clone(),
                state: ProjectionGenerationState::Failed,
                active_generation,
                source_watermark: None,
                member_count: None,
                payload_bytes: None,
                publication_commit_epoch,
                pinned,
                blocker: Some(error.to_string()),
            }),
        }
    }

    pub fn reclaim(
        &self,
        limits: ProjectionGenerationGcLimits,
    ) -> Result<ProjectionGenerationGcReport, ProjectionGenerationError> {
        self.ensure_writable()?;
        let shared = self.lock_shared()?;
        let mut report = ProjectionGenerationGcReport::default();
        let mut candidates = fs::read_dir(self.root.join("generations"))?;
        while report.files_scanned < limits.max_files_to_scan.get() {
            let Some(entry) = candidates.next() else {
                break;
            };
            let entry = entry?;
            report.files_scanned = report.files_scanned.saturating_add(1);
            let path = entry.path();
            if !path.exists() {
                continue;
            }
            if path.extension().and_then(|value| value.to_str()) == Some(DATA_SUFFIX) {
                if path.with_extension(MANIFEST_SUFFIX).exists() {
                    continue;
                }
                let mut file = OpenOptions::new().read(true).open(&path)?;
                let (begin, _) = match read_data_header(&mut file) {
                    Ok(header) => header,
                    Err(_) => continue,
                };
                let key = identity_key(&begin.identity);
                if shared.open_writers.contains(&key) {
                    continue;
                }
                if report.generations_reclaimed >= limits.max_generations_to_reclaim.get() {
                    report.backlog_remaining = true;
                    break;
                }
                let bytes = file.metadata()?.len();
                if report.bytes_reclaimed.saturating_add(bytes)
                    > u64::try_from(limits.max_bytes_to_reclaim.get()).unwrap_or(u64::MAX)
                {
                    report.backlog_remaining = true;
                    break;
                }
                drop(file);
                fs::remove_file(&path)?;
                report.generations_reclaimed = report.generations_reclaimed.saturating_add(1);
                report.bytes_reclaimed = report.bytes_reclaimed.saturating_add(bytes);
                continue;
            }
            if path.extension().and_then(|value| value.to_str()) != Some(MANIFEST_SUFFIX) {
                continue;
            }
            let manifest = match read_manifest(&path) {
                Ok(manifest) => manifest,
                Err(_) => continue,
            };
            let key = identity_key(&manifest.begin.identity);
            let head = read_optional_head(&self.head_path(
                &manifest.begin.identity.projection,
                &manifest.begin.identity.owner_key,
            ))?;
            if head.as_ref().is_some_and(|head| {
                head.manifest.begin.identity.generation == manifest.begin.identity.generation
            }) {
                report.active_generations_skipped =
                    report.active_generations_skipped.saturating_add(1);
                continue;
            }
            if shared.pinned.contains_key(&key) {
                report.pinned_generations_skipped =
                    report.pinned_generations_skipped.saturating_add(1);
                continue;
            }
            if report.generations_reclaimed >= limits.max_generations_to_reclaim.get() {
                report.backlog_remaining = true;
                break;
            }
            let data_path = self.data_path(&manifest.begin.identity);
            let bytes = fs::metadata(&path)?.len().saturating_add(
                fs::metadata(&data_path)
                    .map(|metadata| metadata.len())
                    .unwrap_or(0),
            );
            if report.bytes_reclaimed.saturating_add(bytes)
                > u64::try_from(limits.max_bytes_to_reclaim.get()).unwrap_or(u64::MAX)
            {
                report.backlog_remaining = true;
                break;
            }
            if data_path.exists() {
                fs::remove_file(&data_path)?;
            }
            fs::remove_file(&path)?;
            report.generations_reclaimed = report.generations_reclaimed.saturating_add(1);
            report.bytes_reclaimed = report.bytes_reclaimed.saturating_add(bytes);
        }
        if candidates.next().is_some() {
            report.backlog_remaining = true;
        }
        sync_directory(&self.root.join("generations"))?;
        Ok(report)
    }

    fn lock_shared(
        &self,
    ) -> Result<MutexGuard<'_, ProjectionGenerationShared>, ProjectionGenerationError> {
        self.shared.lock().map_err(|_| {
            ProjectionGenerationError::Corruption(
                "projection generation process state is poisoned".to_string(),
            )
        })
    }

    fn ensure_writable(&self) -> Result<(), ProjectionGenerationError> {
        if self.read_only {
            Err(ProjectionGenerationError::Conflict(
                "read-only projection generation catalog rejects mutation".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn data_path(&self, identity: &ProjectionGenerationIdentity) -> PathBuf {
        self.root
            .join("generations")
            .join(format!("{}.{}", identity_key(identity), DATA_SUFFIX))
    }

    fn manifest_path(&self, identity: &ProjectionGenerationIdentity) -> PathBuf {
        self.root.join("generations").join(format!(
            "{}.{}",
            identity_key(identity),
            MANIFEST_SUFFIX
        ))
    }

    fn head_path(&self, projection: &str, owner_key: &[u8]) -> PathBuf {
        self.root.join("heads").join(format!(
            "{}.{}",
            owner_key_hash(projection, owner_key),
            HEAD_SUFFIX
        ))
    }
}

pub struct ProjectionGenerationWriter {
    store: ProjectionGenerationStore,
    begin: ProjectionGenerationBegin,
    limits: ProjectionGenerationBatchLimits,
    key: String,
    data_path: PathBuf,
    state: CandidateScan,
    sealed: Option<ProjectionGenerationManifest>,
    released: bool,
}

impl ProjectionGenerationWriter {
    fn open(
        store: ProjectionGenerationStore,
        begin: ProjectionGenerationBegin,
        limits: ProjectionGenerationBatchLimits,
        key: String,
    ) -> Result<Self, ProjectionGenerationError> {
        let data_path = store.data_path(&begin.identity);
        let manifest_path = store.manifest_path(&begin.identity);
        let sealed = read_optional_manifest(&manifest_path)?;
        if let Some(manifest) = &sealed
            && manifest.begin != begin
        {
            return Err(ProjectionGenerationError::Conflict(
                "generation identity was reused with different begin metadata".to_string(),
            ));
        }
        let state = if data_path.exists() {
            scan_candidate(&data_path, true, limits.max_record_bytes())?
        } else {
            create_candidate(&data_path, &begin)?
        };
        if state.begin != begin {
            return Err(ProjectionGenerationError::Conflict(
                "generation identity was reused with different candidate metadata".to_string(),
            ));
        }
        if let Some(manifest) = &sealed
            && (manifest.member_count != state.member_count
                || manifest.payload_bytes != state.payload_bytes
                || manifest.content_digest != state.digest.clone().finish()
                || manifest.data_bytes != state.valid_bytes)
        {
            return Err(ProjectionGenerationError::Corruption(
                "sealed generation does not match its candidate data".to_string(),
            ));
        }
        Ok(Self {
            store,
            begin,
            limits,
            key,
            data_path,
            state,
            sealed,
            released: false,
        })
    }

    pub fn append_batch(
        &mut self,
        members: &[ProjectionGenerationMember],
    ) -> Result<(), ProjectionGenerationError> {
        if self.sealed.is_some() {
            return Err(ProjectionGenerationError::Conflict(
                "sealed projection generation rejects additional members".to_string(),
            ));
        }
        if members.len() > self.limits.max_rows.get() {
            return Err(ProjectionGenerationError::Admission(format!(
                "batch contains {} rows, exceeding {}",
                members.len(),
                self.limits.max_rows
            )));
        }
        let payload_bytes = members.iter().try_fold(0usize, |total, member| {
            validate_member(member, self.limits)?;
            total.checked_add(member.payload.len()).ok_or_else(|| {
                ProjectionGenerationError::Admission(
                    "batch payload byte accounting overflow".to_string(),
                )
            })
        })?;
        if payload_bytes > self.limits.max_payload_bytes.get() {
            return Err(ProjectionGenerationError::Admission(format!(
                "batch contains {payload_bytes} payload bytes, exceeding {}",
                self.limits.max_payload_bytes
            )));
        }
        let mut previous = self.state.last_order.clone();
        let mut encoded = Vec::new();
        for member in members {
            let order = (member.collection.clone(), member.key.clone());
            if previous.as_ref().is_some_and(|previous| previous >= &order) {
                return Err(ProjectionGenerationError::Conflict(
                    "projection members must be strictly increasing by collection and key"
                        .to_string(),
                ));
            }
            let record = encode_member(member)?;
            append_record(&record, &mut encoded)?;
            previous = Some(order);
        }
        let mut file = OpenOptions::new().append(true).open(&self.data_path)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
        for member in members {
            update_member_digest(&mut self.state.digest, member)?;
        }
        self.state.member_count = self
            .state
            .member_count
            .checked_add(u64::try_from(members.len()).map_err(|_| {
                ProjectionGenerationError::Admission("batch row count exceeds u64".to_string())
            })?)
            .ok_or_else(|| {
                ProjectionGenerationError::Admission(
                    "candidate row count accounting overflow".to_string(),
                )
            })?;
        self.state.payload_bytes = self
            .state
            .payload_bytes
            .checked_add(u64::try_from(payload_bytes).map_err(|_| {
                ProjectionGenerationError::Admission("batch payload bytes exceed u64".to_string())
            })?)
            .ok_or_else(|| {
                ProjectionGenerationError::Admission(
                    "candidate payload byte accounting overflow".to_string(),
                )
            })?;
        self.state.valid_bytes = self
            .state
            .valid_bytes
            .checked_add(u64::try_from(encoded.len()).map_err(|_| {
                ProjectionGenerationError::Admission("encoded batch bytes exceed u64".to_string())
            })?)
            .ok_or_else(|| {
                ProjectionGenerationError::Admission(
                    "candidate data byte accounting overflow".to_string(),
                )
            })?;
        self.state.last_order = previous;
        Ok(())
    }

    pub fn seal(
        mut self,
        seal: ProjectionGenerationSeal,
    ) -> Result<SealedProjectionGeneration, ProjectionGenerationError> {
        validate_rollup(&seal.rollup_metadata, self.limits)?;
        let digest = self.state.digest.clone().finish();
        if self.state.member_count != seal.expected_member_count
            || self.state.payload_bytes != seal.expected_payload_bytes
            || digest != seal.expected_digest
        {
            return Err(ProjectionGenerationError::Conflict(format!(
                "seal expected count/bytes/digest {}/{}/{:?}, observed {}/{}/{:?}",
                seal.expected_member_count,
                seal.expected_payload_bytes,
                seal.expected_digest,
                self.state.member_count,
                self.state.payload_bytes,
                digest
            )));
        }
        let manifest = ProjectionGenerationManifest {
            begin: self.begin.clone(),
            member_count: self.state.member_count,
            payload_bytes: self.state.payload_bytes,
            content_digest: digest,
            data_bytes: self.state.valid_bytes,
            rollup_metadata: seal.rollup_metadata,
        };
        let idempotent = if let Some(existing) = &self.sealed {
            if existing != &manifest {
                return Err(ProjectionGenerationError::Conflict(
                    "sealed generation identity was reused with different metadata".to_string(),
                ));
            }
            true
        } else {
            let path = self.store.manifest_path(&self.begin.identity);
            let bytes = encode_manifest(&manifest)?;
            let mut shared = self.store.lock_shared()?;
            let temporary = temporary_path(&path, &mut shared);
            write_synchronized(&temporary, &bytes)?;
            if path.exists() {
                let existing = read_manifest(&path)?;
                if existing != manifest {
                    return Err(ProjectionGenerationError::Conflict(
                        "generation manifest appeared with different metadata".to_string(),
                    ));
                }
                fs::remove_file(temporary)?;
            } else {
                durable_replace_file(&temporary, &path)?;
            }
            false
        };
        self.release();
        Ok(SealedProjectionGeneration {
            manifest,
            idempotent,
        })
    }

    pub const fn recovered_torn_tail(&self) -> bool {
        self.state.recovered_torn_tail
    }

    fn release(&mut self) {
        if !self.released {
            if let Ok(mut shared) = self.store.lock_shared() {
                shared.open_writers.remove(&self.key);
            }
            self.released = true;
        }
    }
}

impl Drop for ProjectionGenerationWriter {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Debug)]
pub struct ProjectionGenerationReader {
    store: ProjectionGenerationStore,
    publication_commit_epoch: u64,
    manifest: ProjectionGenerationManifest,
    key: String,
    data_start: u64,
}

impl ProjectionGenerationReader {
    fn open(
        store: ProjectionGenerationStore,
        publication_commit_epoch: u64,
        manifest: ProjectionGenerationManifest,
        key: String,
    ) -> Result<Self, ProjectionGenerationError> {
        let mut file = File::open(store.data_path(&manifest.begin.identity))?;
        let file_bytes = file.metadata()?.len();
        let (begin, data_start) = read_data_header(&mut file)?;
        if begin != manifest.begin || file_bytes != manifest.data_bytes {
            return Err(ProjectionGenerationError::Corruption(
                "active generation data does not match its manifest boundary".to_string(),
            ));
        }
        Ok(Self {
            store,
            publication_commit_epoch,
            manifest,
            key,
            data_start,
        })
    }

    pub fn manifest(&self) -> &ProjectionGenerationManifest {
        &self.manifest
    }

    pub const fn publication_commit_epoch(&self) -> u64 {
        self.publication_commit_epoch
    }

    pub fn read_page(
        &self,
        cursor: Option<&ProjectionGenerationCursor>,
        limits: ProjectionGenerationReadLimits,
    ) -> Result<ProjectionGenerationPage, ProjectionGenerationError> {
        let offset = match cursor {
            Some(cursor) if cursor.generation == self.manifest.begin.identity.generation => {
                cursor.offset
            }
            Some(_) => {
                return Err(ProjectionGenerationError::Conflict(
                    "projection cursor belongs to a different generation".to_string(),
                ));
            }
            None => self.data_start,
        };
        if offset < self.data_start || offset > self.manifest.data_bytes {
            return Err(ProjectionGenerationError::Corruption(
                "projection cursor offset is outside the pinned generation".to_string(),
            ));
        }
        let mut file = File::open(self.store.data_path(&self.manifest.begin.identity))?;
        file.seek(SeekFrom::Start(offset))?;
        let mut members = Vec::new();
        let mut payload_bytes = 0usize;
        let mut next_offset = offset;
        while members.len() < limits.max_rows.get() && next_offset < self.manifest.data_bytes {
            let (member, consumed) = read_record(&mut file, limits.max_record_bytes.get())?;
            let next_payload =
                payload_bytes
                    .checked_add(member.payload.len())
                    .ok_or_else(|| {
                        ProjectionGenerationError::Admission(
                            "projection page payload accounting overflow".to_string(),
                        )
                    })?;
            if next_payload > limits.max_payload_bytes.get() {
                return Err(ProjectionGenerationError::Admission(format!(
                    "projection page exceeds {} payload bytes before reaching its row bound",
                    limits.max_payload_bytes
                )));
            }
            payload_bytes = next_payload;
            members.push(member);
            next_offset = next_offset.checked_add(consumed).ok_or_else(|| {
                ProjectionGenerationError::Corruption(
                    "projection cursor offset overflow".to_string(),
                )
            })?;
        }
        let next = (next_offset < self.manifest.data_bytes).then(|| ProjectionGenerationCursor {
            generation: self.manifest.begin.identity.generation.clone(),
            offset: next_offset,
        });
        let complete = next.is_none();
        Ok(ProjectionGenerationPage {
            report: ProjectionGenerationReadReport {
                generation: self.manifest.begin.identity.generation.clone(),
                source_watermark: self.manifest.begin.source_watermark,
                projection_version: self.manifest.begin.projection_version,
                publication_commit_epoch: self.publication_commit_epoch,
                rows_returned: members.len(),
                payload_bytes,
                complete,
            },
            members,
            next,
        })
    }

    pub fn scrub(&self) -> Result<(), ProjectionGenerationError> {
        let state = scan_candidate(
            &self.store.data_path(&self.manifest.begin.identity),
            false,
            MAX_ENCODED_MEMBER_BYTES,
        )?;
        if state.begin != self.manifest.begin
            || state.member_count != self.manifest.member_count
            || state.payload_bytes != self.manifest.payload_bytes
            || state.valid_bytes != self.manifest.data_bytes
            || state.digest.finish() != self.manifest.content_digest
        {
            return Err(ProjectionGenerationError::Corruption(
                "projection generation scrub does not match the selected manifest".to_string(),
            ));
        }
        Ok(())
    }
}

impl Drop for ProjectionGenerationReader {
    fn drop(&mut self) {
        if let Ok(mut shared) = self.store.lock_shared() {
            release_pin(&mut shared, &self.key);
        }
    }
}

#[derive(Clone)]
pub struct ProjectionGenerationDigestBuilder {
    hasher: IntegrityHasher,
    member_count: u64,
    payload_bytes: u64,
    last_order: Option<(String, Vec<u8>)>,
}

impl Default for ProjectionGenerationDigestBuilder {
    fn default() -> Self {
        Self {
            hasher: IntegrityHasher::new(),
            member_count: 0,
            payload_bytes: 0,
            last_order: None,
        }
    }
}

impl ProjectionGenerationDigestBuilder {
    pub fn update(
        &mut self,
        member: &ProjectionGenerationMember,
    ) -> Result<(), ProjectionGenerationError> {
        let order = (member.collection.clone(), member.key.clone());
        if self
            .last_order
            .as_ref()
            .is_some_and(|previous| previous >= &order)
        {
            return Err(ProjectionGenerationError::Conflict(
                "projection digest members must be strictly increasing".to_string(),
            ));
        }
        update_member_digest(&mut self.hasher, member)?;
        self.member_count = self.member_count.checked_add(1).ok_or_else(|| {
            ProjectionGenerationError::Admission(
                "projection digest member count overflow".to_string(),
            )
        })?;
        self.payload_bytes = self
            .payload_bytes
            .checked_add(u64::try_from(member.payload.len()).map_err(|_| {
                ProjectionGenerationError::Admission(
                    "projection digest payload bytes exceed u64".to_string(),
                )
            })?)
            .ok_or_else(|| {
                ProjectionGenerationError::Admission(
                    "projection digest payload byte accounting overflow".to_string(),
                )
            })?;
        self.last_order = Some(order);
        Ok(())
    }

    pub fn finish(self) -> ProjectionGenerationSeal {
        ProjectionGenerationSeal {
            expected_member_count: self.member_count,
            expected_payload_bytes: self.payload_bytes,
            expected_digest: self.hasher.finish(),
            rollup_metadata: Vec::new(),
        }
    }
}

#[derive(Clone)]
struct CandidateScan {
    begin: ProjectionGenerationBegin,
    member_count: u64,
    payload_bytes: u64,
    digest: IntegrityHasher,
    last_order: Option<(String, Vec<u8>)>,
    valid_bytes: u64,
    recovered_torn_tail: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectionGenerationHead {
    publication_commit_epoch: u64,
    manifest: ProjectionGenerationManifest,
}

impl ProjectionGenerationBatchLimits {
    fn max_record_bytes(self) -> usize {
        self.max_collection_bytes
            .get()
            .saturating_add(self.max_key_bytes.get())
            .saturating_add(self.max_payload_bytes.get())
            .saturating_add(32)
    }
}

fn validate_begin(begin: &ProjectionGenerationBegin) -> Result<(), ProjectionGenerationError> {
    if begin.identity.projection.is_empty()
        || begin.identity.projection.len() > MAX_COLLECTION_BYTES
    {
        return Err(ProjectionGenerationError::Admission(
            "projection name must contain between 1 and 1024 bytes".to_string(),
        ));
    }
    if begin.identity.owner_key.is_empty() || begin.identity.owner_key.len() > MAX_KEY_BYTES {
        return Err(ProjectionGenerationError::Admission(
            "owner key must contain between 1 and 65536 bytes".to_string(),
        ));
    }
    if begin.identity.generation.is_empty()
        || begin.identity.generation.len() > MAX_COLLECTION_BYTES
    {
        return Err(ProjectionGenerationError::Admission(
            "generation identity must contain between 1 and 1024 bytes".to_string(),
        ));
    }
    if begin.projection_version == 0 {
        return Err(ProjectionGenerationError::Admission(
            "projection version must be non-zero".to_string(),
        ));
    }
    Ok(())
}

fn validate_batch_limits(
    limits: ProjectionGenerationBatchLimits,
) -> Result<(), ProjectionGenerationError> {
    if limits.max_collection_bytes.get() > MAX_COLLECTION_BYTES {
        return Err(ProjectionGenerationError::Admission(format!(
            "collection byte limit {} exceeds protocol maximum {MAX_COLLECTION_BYTES}",
            limits.max_collection_bytes
        )));
    }
    if limits.max_key_bytes.get() > MAX_KEY_BYTES {
        return Err(ProjectionGenerationError::Admission(format!(
            "key byte limit {} exceeds protocol maximum {MAX_KEY_BYTES}",
            limits.max_key_bytes
        )));
    }
    if limits.max_payload_bytes.get() > MAX_MEMBER_PAYLOAD_BYTES {
        return Err(ProjectionGenerationError::Admission(format!(
            "payload byte limit {} exceeds protocol maximum {MAX_MEMBER_PAYLOAD_BYTES}",
            limits.max_payload_bytes
        )));
    }
    Ok(())
}

fn validate_member(
    member: &ProjectionGenerationMember,
    limits: ProjectionGenerationBatchLimits,
) -> Result<(), ProjectionGenerationError> {
    if member.collection.is_empty() || member.collection.len() > limits.max_collection_bytes.get() {
        return Err(ProjectionGenerationError::Admission(format!(
            "member collection contains {} bytes, expected 1..={}",
            member.collection.len(),
            limits.max_collection_bytes
        )));
    }
    if member.key.is_empty() || member.key.len() > limits.max_key_bytes.get() {
        return Err(ProjectionGenerationError::Admission(format!(
            "member key contains {} bytes, expected 1..={}",
            member.key.len(),
            limits.max_key_bytes
        )));
    }
    if member.payload.len() > limits.max_payload_bytes.get() {
        return Err(ProjectionGenerationError::Admission(format!(
            "member payload contains {} bytes, exceeding {}",
            member.payload.len(),
            limits.max_payload_bytes
        )));
    }
    Ok(())
}

fn validate_rollup(
    rollup: &[(String, Vec<u8>)],
    limits: ProjectionGenerationBatchLimits,
) -> Result<(), ProjectionGenerationError> {
    if rollup.len() > limits.max_rows.get() {
        return Err(ProjectionGenerationError::Admission(
            "rollup metadata exceeds the batch row limit".to_string(),
        ));
    }
    let mut bytes = 0usize;
    let mut previous = None;
    for (key, value) in rollup {
        if key.is_empty() || key.len() > limits.max_collection_bytes.get() {
            return Err(ProjectionGenerationError::Admission(
                "rollup key is empty or exceeds the collection byte limit".to_string(),
            ));
        }
        if previous.is_some_and(|previous: &String| previous >= key) {
            return Err(ProjectionGenerationError::Conflict(
                "rollup metadata keys must be strictly increasing".to_string(),
            ));
        }
        bytes = bytes
            .checked_add(key.len())
            .and_then(|total| total.checked_add(value.len()))
            .ok_or_else(|| {
                ProjectionGenerationError::Admission(
                    "rollup metadata byte accounting overflow".to_string(),
                )
            })?;
        previous = Some(key);
    }
    if bytes > limits.max_payload_bytes.get() {
        return Err(ProjectionGenerationError::Admission(
            "rollup metadata exceeds the batch payload byte limit".to_string(),
        ));
    }
    Ok(())
}

fn create_candidate(
    path: &Path,
    begin: &ProjectionGenerationBegin,
) -> Result<CandidateScan, ProjectionGenerationError> {
    let header = encode_begin(begin)?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(DATA_MAGIC);
    push_u32(&mut bytes, PROTOCOL_VERSION);
    push_bytes(&mut bytes, &header)?;
    push_u32(&mut bytes, crc32c(&header).get());
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    sync_parent_directory(path)?;
    Ok(CandidateScan {
        begin: begin.clone(),
        member_count: 0,
        payload_bytes: 0,
        digest: IntegrityHasher::new(),
        last_order: None,
        valid_bytes: u64::try_from(bytes.len()).map_err(|_| {
            ProjectionGenerationError::Admission("candidate header exceeds u64".to_string())
        })?,
        recovered_torn_tail: false,
    })
}

fn scan_candidate(
    path: &Path,
    repair_torn_tail: bool,
    max_record_bytes: usize,
) -> Result<CandidateScan, ProjectionGenerationError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(repair_torn_tail)
        .open(path)?;
    let file_len = file.metadata()?.len();
    let (begin, data_start) = read_data_header(&mut file)?;
    let mut state = CandidateScan {
        begin,
        member_count: 0,
        payload_bytes: 0,
        digest: IntegrityHasher::new(),
        last_order: None,
        valid_bytes: data_start,
        recovered_torn_tail: false,
    };
    while state.valid_bytes < file_len {
        file.seek(SeekFrom::Start(state.valid_bytes))?;
        match read_record(&mut file, max_record_bytes) {
            Ok((member, consumed)) => {
                let order = (member.collection.clone(), member.key.clone());
                if state
                    .last_order
                    .as_ref()
                    .is_some_and(|previous| previous >= &order)
                {
                    return Err(ProjectionGenerationError::Corruption(
                        "candidate members are not strictly ordered".to_string(),
                    ));
                }
                update_member_digest(&mut state.digest, &member)?;
                state.member_count = state.member_count.checked_add(1).ok_or_else(|| {
                    ProjectionGenerationError::Corruption(
                        "candidate member count overflow".to_string(),
                    )
                })?;
                state.payload_bytes = state
                    .payload_bytes
                    .checked_add(u64::try_from(member.payload.len()).map_err(|_| {
                        ProjectionGenerationError::Corruption(
                            "candidate payload bytes exceed u64".to_string(),
                        )
                    })?)
                    .ok_or_else(|| {
                        ProjectionGenerationError::Corruption(
                            "candidate payload byte accounting overflow".to_string(),
                        )
                    })?;
                state.valid_bytes = state.valid_bytes.checked_add(consumed).ok_or_else(|| {
                    ProjectionGenerationError::Corruption(
                        "candidate file offset overflow".to_string(),
                    )
                })?;
                state.last_order = Some(order);
            }
            Err(ProjectionGenerationError::Io(error))
                if repair_torn_tail && error.kind() == io::ErrorKind::UnexpectedEof =>
            {
                file.set_len(state.valid_bytes)?;
                file.sync_all()?;
                state.recovered_torn_tail = true;
                break;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(state)
}

fn read_data_header(
    file: &mut File,
) -> Result<(ProjectionGenerationBegin, u64), ProjectionGenerationError> {
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)?;
    if &magic != DATA_MAGIC {
        return Err(ProjectionGenerationError::Corruption(
            "candidate data magic does not match".to_string(),
        ));
    }
    let version = read_u32(file)?;
    if version != PROTOCOL_VERSION {
        return Err(ProjectionGenerationError::Corruption(format!(
            "candidate data protocol {version} is unsupported"
        )));
    }
    let header = read_sized_bytes(file, 1024 * 1024)?;
    let expected_crc = read_u32(file)?;
    if crc32c(&header).get() != expected_crc {
        return Err(ProjectionGenerationError::Corruption(
            "candidate header checksum mismatch".to_string(),
        ));
    }
    let offset = file.stream_position()?;
    Ok((decode_begin(&header)?, offset))
}

fn append_record(record: &[u8], output: &mut Vec<u8>) -> Result<(), ProjectionGenerationError> {
    push_bytes(output, record)?;
    push_u32(output, crc32c(record).get());
    Ok(())
}

fn read_record(
    file: &mut File,
    max_record_bytes: usize,
) -> Result<(ProjectionGenerationMember, u64), ProjectionGenerationError> {
    let start = file.stream_position()?;
    let body = read_sized_bytes(file, max_record_bytes)?;
    let expected_crc = read_u32(file)?;
    if crc32c(&body).get() != expected_crc {
        return Err(ProjectionGenerationError::Corruption(
            "candidate member checksum mismatch".to_string(),
        ));
    }
    let consumed = file.stream_position()?.checked_sub(start).ok_or_else(|| {
        ProjectionGenerationError::Corruption("candidate record offset underflow".to_string())
    })?;
    Ok((decode_member(&body)?, consumed))
}

fn update_member_digest(
    digest: &mut IntegrityHasher,
    member: &ProjectionGenerationMember,
) -> Result<(), ProjectionGenerationError> {
    let encoded = encode_member(member)?;
    let len = u64::try_from(encoded.len()).map_err(|_| {
        ProjectionGenerationError::Admission("encoded member length exceeds u64".to_string())
    })?;
    digest.update(&len.to_le_bytes());
    digest.update(&encoded);
    Ok(())
}

fn identity_key(identity: &ProjectionGenerationIdentity) -> String {
    let mut hasher = IntegrityHasher::new();
    hash_part(&mut hasher, identity.projection.as_bytes());
    hash_part(&mut hasher, &identity.owner_key);
    hash_part(&mut hasher, identity.generation.as_bytes());
    hasher.finish().sha256.to_string()
}

fn owner_key_hash(projection: &str, owner_key: &[u8]) -> String {
    let mut hasher = IntegrityHasher::new();
    hash_part(&mut hasher, projection.as_bytes());
    hash_part(&mut hasher, owner_key);
    hasher.finish().sha256.to_string()
}

fn hash_part(hasher: &mut IntegrityHasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn temporary_path(path: &Path, shared: &mut ProjectionGenerationShared) -> PathBuf {
    shared.temporary_sequence = shared.temporary_sequence.saturating_add(1);
    path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        shared.temporary_sequence
    ))
}

fn release_pin(shared: &mut ProjectionGenerationShared, key: &str) {
    let Some(count) = shared.pinned.get_mut(key) else {
        return;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        shared.pinned.remove(key);
    }
}

fn write_synchronized(path: &Path, bytes: &[u8]) -> Result<(), ProjectionGenerationError> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn read_optional_manifest(
    path: &Path,
) -> Result<Option<ProjectionGenerationManifest>, ProjectionGenerationError> {
    match read_bounded_file(path, MAX_ENVELOPE_BYTES) {
        Ok(bytes) => decode_manifest(&bytes).map(Some),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_manifest(path: &Path) -> Result<ProjectionGenerationManifest, ProjectionGenerationError> {
    read_optional_manifest(path)?.ok_or_else(|| {
        ProjectionGenerationError::NotFound(format!(
            "generation manifest {} is missing",
            path.display()
        ))
    })
}

fn read_optional_head(
    path: &Path,
) -> Result<Option<ProjectionGenerationHead>, ProjectionGenerationError> {
    match read_bounded_file(path, MAX_ENVELOPE_BYTES) {
        Ok(bytes) => decode_head(&bytes).map(Some),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_bounded_file(path: &Path, max_bytes: usize) -> io::Result<Vec<u8>> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    if file_len > u64::try_from(max_bytes).unwrap_or(u64::MAX) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "projection generation file {} contains {file_len} bytes, exceeding {max_bytes}",
                path.display()
            ),
        ));
    }
    let capacity = usize::try_from(file_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "projection generation file length exceeds usize",
        )
    })?;
    let read_limit = u64::try_from(max_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut bytes = Vec::with_capacity(capacity);
    file.take(read_limit).read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "projection generation file {} grew beyond {max_bytes} bytes while reading",
                path.display()
            ),
        ));
    }
    Ok(bytes)
}

fn validate_head_owner(
    head: &ProjectionGenerationHead,
    identity: &ProjectionGenerationIdentity,
) -> Result<(), ProjectionGenerationError> {
    if head.manifest.begin.identity.projection != identity.projection
        || head.manifest.begin.identity.owner_key != identity.owner_key
    {
        return Err(ProjectionGenerationError::Corruption(
            "owner head hash resolved to a different owner".to_string(),
        ));
    }
    Ok(())
}

fn encode_begin(begin: &ProjectionGenerationBegin) -> Result<Vec<u8>, ProjectionGenerationError> {
    let mut bytes = Vec::new();
    push_string(&mut bytes, &begin.identity.projection)?;
    push_bytes(&mut bytes, &begin.identity.owner_key)?;
    push_string(&mut bytes, &begin.identity.generation)?;
    push_u64(&mut bytes, begin.source_watermark);
    push_u64(&mut bytes, begin.projection_version);
    push_optional_string(&mut bytes, begin.expected_head.as_deref())?;
    Ok(bytes)
}

fn decode_begin(bytes: &[u8]) -> Result<ProjectionGenerationBegin, ProjectionGenerationError> {
    let mut decoder = Decoder::new(bytes);
    let begin = ProjectionGenerationBegin {
        identity: ProjectionGenerationIdentity {
            projection: decoder.string(1_024)?,
            owner_key: decoder.bytes(64 * 1024)?,
            generation: decoder.string(1_024)?,
        },
        source_watermark: decoder.u64()?,
        projection_version: decoder.u64()?,
        expected_head: decoder.optional_string(1_024)?,
    };
    decoder.finish()?;
    validate_begin(&begin)?;
    Ok(begin)
}

fn encode_member(
    member: &ProjectionGenerationMember,
) -> Result<Vec<u8>, ProjectionGenerationError> {
    let mut bytes = Vec::new();
    push_string(&mut bytes, &member.collection)?;
    push_bytes(&mut bytes, &member.key)?;
    push_bytes(&mut bytes, &member.payload)?;
    Ok(bytes)
}

fn decode_member(bytes: &[u8]) -> Result<ProjectionGenerationMember, ProjectionGenerationError> {
    let mut decoder = Decoder::new(bytes);
    let member = ProjectionGenerationMember {
        collection: decoder.string(1_024)?,
        key: decoder.bytes(64 * 1024)?,
        payload: decoder.bytes(64 * 1024 * 1024)?,
    };
    decoder.finish()?;
    Ok(member)
}

fn encode_manifest(
    manifest: &ProjectionGenerationManifest,
) -> Result<Vec<u8>, ProjectionGenerationError> {
    let body = encode_manifest_body(manifest)?;
    validate_envelope_body_len(body.len())?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MANIFEST_MAGIC);
    push_u32(&mut bytes, PROTOCOL_VERSION);
    push_bytes(&mut bytes, &body)?;
    push_u32(&mut bytes, crc32c(&body).get());
    Ok(bytes)
}

fn decode_manifest(
    bytes: &[u8],
) -> Result<ProjectionGenerationManifest, ProjectionGenerationError> {
    let body = decode_envelope(bytes, MANIFEST_MAGIC)?;
    decode_manifest_body(body)
}

fn encode_manifest_body(
    manifest: &ProjectionGenerationManifest,
) -> Result<Vec<u8>, ProjectionGenerationError> {
    let mut bytes = Vec::new();
    push_bytes(&mut bytes, &encode_begin(&manifest.begin)?)?;
    push_u64(&mut bytes, manifest.member_count);
    push_u64(&mut bytes, manifest.payload_bytes);
    push_u32(&mut bytes, manifest.content_digest.crc32c.get());
    bytes.extend_from_slice(manifest.content_digest.sha256.as_bytes());
    push_u64(&mut bytes, manifest.data_bytes);
    push_u32(
        &mut bytes,
        u32::try_from(manifest.rollup_metadata.len()).map_err(|_| {
            ProjectionGenerationError::Admission("rollup count exceeds u32".to_string())
        })?,
    );
    for (key, value) in &manifest.rollup_metadata {
        push_string(&mut bytes, key)?;
        push_bytes(&mut bytes, value)?;
    }
    Ok(bytes)
}

fn decode_manifest_body(
    bytes: &[u8],
) -> Result<ProjectionGenerationManifest, ProjectionGenerationError> {
    let mut decoder = Decoder::new(bytes);
    let begin = decode_begin(&decoder.bytes(1024 * 1024)?)?;
    let member_count = decoder.u64()?;
    let payload_bytes = decoder.u64()?;
    let digest_crc = Crc32c::new(decoder.u32()?);
    let digest_sha = Sha256Digest::from_bytes(decoder.array_32()?);
    let data_bytes = decoder.u64()?;
    let rollup_count = usize::try_from(decoder.u32()?).map_err(|_| {
        ProjectionGenerationError::Corruption("rollup count exceeds usize".to_string())
    })?;
    if rollup_count > 1_024 {
        return Err(ProjectionGenerationError::Corruption(
            "rollup count exceeds decode limit".to_string(),
        ));
    }
    let mut rollup_metadata = Vec::with_capacity(rollup_count);
    for _ in 0..rollup_count {
        rollup_metadata.push((decoder.string(1_024)?, decoder.bytes(8 * 1024 * 1024)?));
    }
    decoder.finish()?;
    Ok(ProjectionGenerationManifest {
        begin,
        member_count,
        payload_bytes,
        content_digest: IntegrityDigest {
            crc32c: digest_crc,
            sha256: digest_sha,
        },
        data_bytes,
        rollup_metadata,
    })
}

fn encode_head(head: &ProjectionGenerationHead) -> Result<Vec<u8>, ProjectionGenerationError> {
    let mut body = Vec::new();
    push_u64(&mut body, head.publication_commit_epoch);
    push_bytes(&mut body, &encode_manifest_body(&head.manifest)?)?;
    validate_envelope_body_len(body.len())?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(HEAD_MAGIC);
    push_u32(&mut bytes, PROTOCOL_VERSION);
    push_bytes(&mut bytes, &body)?;
    push_u32(&mut bytes, crc32c(&body).get());
    Ok(bytes)
}

fn validate_envelope_body_len(body_bytes: usize) -> Result<(), ProjectionGenerationError> {
    if body_bytes > MAX_ENVELOPE_BODY_BYTES {
        Err(ProjectionGenerationError::Admission(format!(
            "projection generation metadata contains {body_bytes} bytes, exceeding {MAX_ENVELOPE_BODY_BYTES}"
        )))
    } else {
        Ok(())
    }
}

fn decode_head(bytes: &[u8]) -> Result<ProjectionGenerationHead, ProjectionGenerationError> {
    let body = decode_envelope(bytes, HEAD_MAGIC)?;
    let mut decoder = Decoder::new(body);
    let publication_commit_epoch = decoder.u64()?;
    let manifest = decode_manifest_body(&decoder.bytes(16 * 1024 * 1024)?)?;
    decoder.finish()?;
    Ok(ProjectionGenerationHead {
        publication_commit_epoch,
        manifest,
    })
}

fn decode_envelope<'a>(
    bytes: &'a [u8],
    magic: &[u8; 8],
) -> Result<&'a [u8], ProjectionGenerationError> {
    let mut decoder = Decoder::new(bytes);
    if decoder.take(8)? != magic {
        return Err(ProjectionGenerationError::Corruption(
            "projection generation envelope magic does not match".to_string(),
        ));
    }
    if decoder.u32()? != PROTOCOL_VERSION {
        return Err(ProjectionGenerationError::Corruption(
            "projection generation envelope protocol is unsupported".to_string(),
        ));
    }
    let body = decoder.borrowed_bytes(MAX_ENVELOPE_BODY_BYTES)?;
    let expected_crc = decoder.u32()?;
    decoder.finish()?;
    if crc32c(body).get() != expected_crc {
        return Err(ProjectionGenerationError::Corruption(
            "projection generation envelope checksum mismatch".to_string(),
        ));
    }
    Ok(body)
}

fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), ProjectionGenerationError> {
    push_u32(
        bytes,
        u32::try_from(value.len()).map_err(|_| {
            ProjectionGenerationError::Admission("encoded field length exceeds u32".to_string())
        })?,
    );
    bytes.extend_from_slice(value);
    Ok(())
}

fn push_string(bytes: &mut Vec<u8>, value: &str) -> Result<(), ProjectionGenerationError> {
    push_bytes(bytes, value.as_bytes())
}

fn push_optional_string(
    bytes: &mut Vec<u8>,
    value: Option<&str>,
) -> Result<(), ProjectionGenerationError> {
    match value {
        Some(value) => {
            bytes.push(1);
            push_string(bytes, value)
        }
        None => {
            bytes.push(0);
            Ok(())
        }
    }
}

fn read_u32(reader: &mut File) -> Result<u32, ProjectionGenerationError> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_sized_bytes(
    reader: &mut File,
    max_bytes: usize,
) -> Result<Vec<u8>, ProjectionGenerationError> {
    let len = usize::try_from(read_u32(reader)?).map_err(|_| {
        ProjectionGenerationError::Corruption("record length exceeds usize".to_string())
    })?;
    if len > max_bytes {
        return Err(ProjectionGenerationError::Admission(format!(
            "record contains {len} bytes, exceeding {max_bytes}"
        )));
    }
    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}

struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], ProjectionGenerationError> {
        let end = self.position.checked_add(count).ok_or_else(|| {
            ProjectionGenerationError::Corruption("decode offset overflow".to_string())
        })?;
        let value = self.bytes.get(self.position..end).ok_or_else(|| {
            ProjectionGenerationError::Corruption(
                "projection generation record is truncated".to_string(),
            )
        })?;
        self.position = end;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32, ProjectionGenerationError> {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, ProjectionGenerationError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(bytes))
    }

    fn array_32(&mut self) -> Result<[u8; 32], ProjectionGenerationError> {
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(self.take(32)?);
        Ok(bytes)
    }

    fn borrowed_bytes(&mut self, max: usize) -> Result<&'a [u8], ProjectionGenerationError> {
        let len = usize::try_from(self.u32()?).map_err(|_| {
            ProjectionGenerationError::Corruption("field length exceeds usize".to_string())
        })?;
        if len > max {
            return Err(ProjectionGenerationError::Admission(format!(
                "decoded field contains {len} bytes, exceeding {max}"
            )));
        }
        self.take(len)
    }

    fn bytes(&mut self, max: usize) -> Result<Vec<u8>, ProjectionGenerationError> {
        self.borrowed_bytes(max).map(ToOwned::to_owned)
    }

    fn string(&mut self, max: usize) -> Result<String, ProjectionGenerationError> {
        String::from_utf8(self.bytes(max)?).map_err(|error| {
            ProjectionGenerationError::Corruption(format!(
                "projection generation string is not UTF-8: {error}"
            ))
        })
    }

    fn optional_string(&mut self, max: usize) -> Result<Option<String>, ProjectionGenerationError> {
        match self.take(1)?[0] {
            0 => Ok(None),
            1 => self.string(max).map(Some),
            value => Err(ProjectionGenerationError::Corruption(format!(
                "optional string tag {value} is invalid"
            ))),
        }
    }

    fn finish(self) -> Result<(), ProjectionGenerationError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(ProjectionGenerationError::Corruption(
                "projection generation record has trailing bytes".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein-projection-generation-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn begin(
        generation: &str,
        expected_head: Option<&str>,
        watermark: u64,
    ) -> ProjectionGenerationBegin {
        begin_for(b"session-1", generation, expected_head, watermark)
    }

    fn begin_for(
        owner_key: &[u8],
        generation: &str,
        expected_head: Option<&str>,
        watermark: u64,
    ) -> ProjectionGenerationBegin {
        ProjectionGenerationBegin {
            identity: ProjectionGenerationIdentity {
                projection: "session_spans".to_string(),
                owner_key: owner_key.to_vec(),
                generation: generation.to_string(),
            },
            source_watermark: watermark,
            projection_version: 3,
            expected_head: expected_head.map(str::to_string),
        }
    }

    fn members(values: &[(&str, &str)]) -> Vec<ProjectionGenerationMember> {
        values
            .iter()
            .map(|(key, payload)| ProjectionGenerationMember {
                collection: "spans".to_string(),
                key: key.as_bytes().to_vec(),
                payload: payload.as_bytes().to_vec(),
            })
            .collect()
    }

    fn seal_for(members: &[ProjectionGenerationMember]) -> ProjectionGenerationSeal {
        let mut digest = ProjectionGenerationDigestBuilder::default();
        for member in members {
            digest.update(member).expect("digest member");
        }
        digest.finish()
    }

    fn stage(
        store: &ProjectionGenerationStore,
        begin: ProjectionGenerationBegin,
        rows: &[ProjectionGenerationMember],
    ) -> SealedProjectionGeneration {
        let mut writer = store
            .begin_candidate(begin, ProjectionGenerationBatchLimits::default())
            .expect("begin generation");
        for batch in rows.chunks(2) {
            writer.append_batch(batch).expect("append bounded batch");
        }
        writer.seal(seal_for(rows)).expect("seal generation")
    }

    #[test]
    fn replacement_is_atomic_prunes_by_omission_and_retains_pinned_reader() {
        let root = path("replace");
        let store = ProjectionGenerationStore::open(&root).expect("open store");
        let first_rows = members(&[("a", "first-a"), ("b", "first-b")]);
        let first = stage(&store, begin("generation-1", None, 10), &first_rows);
        let first_publish = store.publish(&first).expect("publish first generation");
        assert_eq!(first_publish.publication_commit_epoch, 1);
        let pinned = store
            .open_active("session_spans", b"session-1")
            .expect("pin first generation");

        let second_rows = members(&[("b", "second-b"), ("c", "second-c")]);
        let second = stage(
            &store,
            begin("generation-2", Some("generation-1"), 11),
            &second_rows,
        );
        store.publish(&second).expect("publish replacement");
        let current = store
            .open_active("session_spans", b"session-1")
            .expect("open replacement");
        assert_eq!(
            current
                .read_page(None, ProjectionGenerationReadLimits::default())
                .expect("read replacement")
                .members,
            second_rows
        );
        assert_eq!(
            pinned
                .read_page(None, ProjectionGenerationReadLimits::default())
                .expect("read pinned predecessor")
                .members,
            first_rows
        );

        let before_drop = store
            .reclaim(ProjectionGenerationGcLimits::default())
            .expect("reclaim while pinned");
        assert_eq!(before_drop.generations_reclaimed, 0);
        assert_eq!(before_drop.pinned_generations_skipped, 1);
        drop(pinned);
        let after_drop = store
            .reclaim(ProjectionGenerationGcLimits::default())
            .expect("reclaim predecessor");
        assert_eq!(after_drop.generations_reclaimed, 1);
        drop(current);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn sealing_and_publication_enforce_bounds_digest_idempotency_and_cas() {
        let root = path("contracts");
        let store = ProjectionGenerationStore::open(&root).expect("open store");
        let rows = members(&[("a", "payload-a"), ("b", "payload-b")]);
        assert!(matches!(
            store.begin_candidate(
                begin("oversized-protocol", None, 1),
                ProjectionGenerationBatchLimits {
                    max_payload_bytes: NonZeroUsize::new(MAX_MEMBER_PAYLOAD_BYTES + 1).unwrap(),
                    ..ProjectionGenerationBatchLimits::default()
                },
            ),
            Err(ProjectionGenerationError::Admission(_))
        ));
        let mut too_small = store
            .begin_candidate(
                begin("generation-1", None, 10),
                ProjectionGenerationBatchLimits {
                    max_rows: NonZeroUsize::new(1).unwrap(),
                    ..ProjectionGenerationBatchLimits::default()
                },
            )
            .expect("begin bounded candidate");
        assert!(matches!(
            too_small.append_batch(&rows),
            Err(ProjectionGenerationError::Admission(_))
        ));
        drop(too_small);

        let first = stage(&store, begin("generation-1", None, 10), &rows);
        let first_publish = store.publish(&first).expect("publish first");
        assert!(!first_publish.idempotent);
        assert!(store.publish(&first).expect("retry publish").idempotent);

        let stale = stage(
            &store,
            begin("generation-stale", Some("generation-1"), 9),
            &rows,
        );
        assert!(matches!(
            store.publish(&stale),
            Err(ProjectionGenerationError::Conflict(_))
        ));
        let wrong_head = stage(&store, begin("generation-2", None, 11), &rows);
        assert!(matches!(
            store.publish(&wrong_head),
            Err(ProjectionGenerationError::Conflict(_))
        ));

        let retry = store
            .begin_candidate(
                begin("generation-1", None, 10),
                ProjectionGenerationBatchLimits::default(),
            )
            .expect("reopen sealed generation")
            .seal(seal_for(&rows))
            .expect("idempotent seal");
        assert!(retry.idempotent());
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn reopen_repairs_an_unpublished_torn_tail_and_pages_are_generation_bound() {
        let root = path("recovery");
        let store = ProjectionGenerationStore::open(&root).expect("open store");
        let rows = members(&[("a", "payload-a"), ("b", "payload-b"), ("c", "payload-c")]);
        {
            let mut writer = store
                .begin_candidate(
                    begin("generation-1", None, 10),
                    ProjectionGenerationBatchLimits::default(),
                )
                .expect("begin candidate");
            writer.append_batch(&rows[..2]).expect("append first batch");
        }
        let data_path = store.data_path(&begin("generation-1", None, 10).identity);
        OpenOptions::new()
            .append(true)
            .open(&data_path)
            .expect("open candidate tail")
            .write_all(&[3, 0])
            .expect("write torn tail");
        let reopened = ProjectionGenerationStore::open(&root).expect("reopen store");
        let mut writer = reopened
            .begin_candidate(
                begin("generation-1", None, 10),
                ProjectionGenerationBatchLimits::default(),
            )
            .expect("resume candidate");
        assert!(writer.recovered_torn_tail());
        writer.append_batch(&rows[2..]).expect("finish candidate");
        let sealed = writer
            .seal(seal_for(&rows))
            .expect("seal resumed candidate");
        reopened
            .publish(&sealed)
            .expect("publish recovered candidate");
        let reader = reopened
            .open_active("session_spans", b"session-1")
            .expect("open active generation");
        let limits = ProjectionGenerationReadLimits {
            max_rows: NonZeroUsize::new(2).unwrap(),
            ..ProjectionGenerationReadLimits::default()
        };
        let first = reader.read_page(None, limits).expect("read first page");
        assert_eq!(first.members, rows[..2]);
        assert_eq!(first.report.generation, "generation-1");
        assert_eq!(first.report.source_watermark, 10);
        assert_eq!(first.report.rows_returned, 2);
        assert!(!first.report.complete);
        let second = reader
            .read_page(first.next.as_ref(), limits)
            .expect("read second page");
        assert_eq!(second.members, rows[2..]);
        assert!(second.next.is_none());
        assert!(second.report.complete);
        reader.scrub().expect("scrub active generation");
        drop(reader);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn multiple_readers_keep_the_same_inactive_generation_pinned() {
        let root = path("multiple-pins");
        let store = ProjectionGenerationStore::open(&root).expect("open store");
        let first_rows = members(&[("a", "first")]);
        let first = stage(&store, begin("generation-1", None, 10), &first_rows);
        store.publish(&first).expect("publish first generation");
        let first_reader = store
            .open_active("session_spans", b"session-1")
            .expect("open first reader");
        let second_reader = store
            .open_active("session_spans", b"session-1")
            .expect("open second reader");

        let replacement_rows = members(&[("b", "replacement")]);
        let replacement = stage(
            &store,
            begin("generation-2", Some("generation-1"), 11),
            &replacement_rows,
        );
        store.publish(&replacement).expect("publish replacement");
        drop(first_reader);

        let one_reader_remains = store
            .reclaim(ProjectionGenerationGcLimits::default())
            .expect("reclaim with one remaining reader");
        assert_eq!(one_reader_remains.generations_reclaimed, 0);
        assert_eq!(one_reader_remains.pinned_generations_skipped, 1);
        drop(second_reader);

        let released = store
            .reclaim(ProjectionGenerationGcLimits::default())
            .expect("reclaim released predecessor");
        assert_eq!(released.generations_reclaimed, 1);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn owner_cas_conflicts_do_not_block_independent_owners() {
        let root = path("owner-cas");
        let store = ProjectionGenerationStore::open(&root).expect("open store");
        let rows = members(&[("a", "payload")]);
        let owner_one = stage(
            &store,
            begin_for(b"session-1", "generation-1", None, 10),
            &rows,
        );
        let stale_owner_one = stage(
            &store,
            begin_for(b"session-1", "generation-stale", None, 11),
            &rows,
        );
        let owner_two = stage(
            &store,
            begin_for(b"session-2", "generation-1", None, 10),
            &rows,
        );

        store.publish(&owner_one).expect("publish first owner");
        assert!(matches!(
            store.publish(&stale_owner_one),
            Err(ProjectionGenerationError::Conflict(_))
        ));
        store.publish(&owner_two).expect("publish second owner");
        assert_eq!(
            store
                .open_active("session_spans", b"session-2")
                .expect("open second owner")
                .manifest()
                .begin
                .identity
                .generation,
            "generation-1"
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn abandoned_candidates_are_reclaimed_with_explicit_limits() {
        let root = path("abandoned-gc");
        let store = ProjectionGenerationStore::open(&root).expect("open store");
        let rows = members(&[("a", "payload")]);
        let candidate = begin("generation-abandoned", None, 10);
        {
            let mut writer = store
                .begin_candidate(
                    candidate.clone(),
                    ProjectionGenerationBatchLimits::default(),
                )
                .expect("begin candidate");
            writer.append_batch(&rows).expect("append candidate");
        }
        assert_eq!(
            store
                .status(&candidate.identity)
                .expect("candidate status")
                .state,
            ProjectionGenerationState::Abandoned
        );

        let report = store
            .reclaim(ProjectionGenerationGcLimits {
                max_files_to_scan: NonZeroUsize::new(1).unwrap(),
                max_generations_to_reclaim: NonZeroUsize::new(1).unwrap(),
                max_bytes_to_reclaim: NonZeroUsize::new(1024 * 1024).unwrap(),
            })
            .expect("reclaim abandoned candidate");
        assert_eq!(report.generations_reclaimed, 1);
        assert_eq!(
            store
                .status(&candidate.identity)
                .expect("candidate removed")
                .state,
            ProjectionGenerationState::Missing
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn large_generation_streams_without_a_complete_keep_set() {
        let root = path("large-stream");
        let store = ProjectionGenerationStore::open(&root).expect("open store");
        let mut writer = store
            .begin_candidate(
                begin("generation-large", None, 10),
                ProjectionGenerationBatchLimits {
                    max_rows: NonZeroUsize::new(127).unwrap(),
                    ..ProjectionGenerationBatchLimits::default()
                },
            )
            .expect("begin large candidate");
        let mut digest = ProjectionGenerationDigestBuilder::default();
        let member_count = 4_096usize;
        for start in (0..member_count).step_by(127) {
            let batch = (start..member_count.min(start + 127))
                .map(|index| ProjectionGenerationMember {
                    collection: "spans".to_string(),
                    key: format!("span-{index:08}").into_bytes(),
                    payload: format!("payload-{index}").into_bytes(),
                })
                .collect::<Vec<_>>();
            for member in &batch {
                digest.update(member).expect("digest streamed member");
            }
            writer.append_batch(&batch).expect("append streamed batch");
        }
        let sealed = writer.seal(digest.finish()).expect("seal large candidate");
        store.publish(&sealed).expect("publish large candidate");

        let reader = store
            .open_active("session_spans", b"session-1")
            .expect("open large generation");
        let limits = ProjectionGenerationReadLimits {
            max_rows: NonZeroUsize::new(113).unwrap(),
            ..ProjectionGenerationReadLimits::default()
        };
        let mut cursor = None;
        let mut observed = 0usize;
        loop {
            let page = reader
                .read_page(cursor.as_ref(), limits)
                .expect("read bounded page");
            for member in &page.members {
                assert_eq!(member.key, format!("span-{observed:08}").into_bytes());
                observed += 1;
            }
            let Some(next) = page.next else {
                break;
            };
            cursor = Some(next);
        }
        assert_eq!(observed, member_count);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn oversized_active_metadata_is_rejected_before_decode() {
        let root = path("oversized-head");
        let store = ProjectionGenerationStore::open(&root).expect("open store");
        let rows = members(&[("a", "payload")]);
        let sealed = stage(&store, begin("generation-1", None, 10), &rows);
        store.publish(&sealed).expect("publish generation");
        let head_path = store.head_path("session_spans", b"session-1");
        OpenOptions::new()
            .write(true)
            .open(&head_path)
            .expect("open head")
            .set_len(u64::try_from(MAX_ENVELOPE_BYTES + 1).unwrap())
            .expect("expand head");

        assert!(matches!(
            store.open_active("session_spans", b"session-1"),
            Err(ProjectionGenerationError::Io(error))
                if error.kind() == io::ErrorKind::InvalidData
        ));
        fs::remove_dir_all(root).expect("remove fixture");
    }
}
