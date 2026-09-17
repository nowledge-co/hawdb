use super::ordered_key::{
    encode_ordered_relational_key, encode_ordered_relational_value,
    ordered_relational_key_prefix_ends, OrderedRelationalKeyError,
};
use super::{
    RelationalColumnDefault, RelationalError, RelationalForeignKeySchema, RelationalIndexRole,
    RelationalKey, RelationalReferentialAction, RelationalScalarType, RelationalState,
    RelationalTableSchema, RelationalValue, RELATIONAL_FOREIGN_KEY_INDEX_PREFIX,
    RELATIONAL_PRIMARY_INDEX_NAME, RELATIONAL_UNIQUE_INDEX_PREFIX,
};
use crate::cache::SegmentCacheIdentity;
use crate::io::read_exact_at;
use crate::{
    content_digest, durable_replace_file, ImmutableIndexPage, ImmutableIndexPageBody,
    ImmutableIndexPageError, ImmutableIndexPageLimits, IndexIdentity, IndexInteriorEntry,
    IndexInteriorPage, IndexLeafEntry, IndexLeafPage, IndexLeafPosting, IndexPageId,
    IndexPostingPage, IndexRootPage, IndexRowId, ManifestGeneration, RepresentationKind,
    SegmentCache, SegmentCacheError, SegmentCacheKey, StoreId,
};
use skein_integrity::{
    integrity_digest, IntegrityDigest, IntegrityHasher, Sha256Digest, SHA256_BYTES,
};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

mod build;
mod demand_read;
mod recovery;

pub use demand_read::{
    RelationalIndexReadLimits, RelationalIndexReadReport, DEFAULT_RELATIONAL_INDEX_READ_BYTES,
    DEFAULT_RELATIONAL_INDEX_READ_PAGES, DEFAULT_RELATIONAL_INDEX_READ_ROWS,
    DEFAULT_RELATIONAL_INDEX_READ_TREE_HEIGHT,
};
pub use recovery::{
    relational_index_recovery_delta_file, RelationalIndexRecoveryBuilder,
    RelationalIndexRecoveryConfig, RelationalIndexRecoveryManifest,
    RelationalIndexRecoveryReadReport, RelationalIndexRecoveryReader,
    RelationalIndexRecoveryReport, DEFAULT_RELATIONAL_INDEX_RECOVERY_DIRTY_BYTES,
    DEFAULT_RELATIONAL_INDEX_RECOVERY_DIRTY_ENTRIES,
    DEFAULT_RELATIONAL_INDEX_RECOVERY_MANIFEST_BYTES, DEFAULT_RELATIONAL_INDEX_RECOVERY_PAGES,
    RELATIONAL_INDEX_RECOVERY_MANIFEST_FILE,
};

const MANIFEST_MAGIC: &[u8; 8] = b"SKRIDXM1";
const MANIFEST_VERSION: u16 = 2;
const MANIFEST_INTEGRITY_OFFSET: usize = 120;
const MANIFEST_HEADER_BYTES: usize = 156;
const RELATIONAL_INDEX_SHADOW_LOCK_FILE: &str = "relational-index-shadow.lock";

pub const RELATIONAL_INDEX_SHADOW_MANIFEST_FILE: &str = "relational-index-shadow.manifest.skein";
pub const DEFAULT_RELATIONAL_INDEX_SHADOW_MANIFEST_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_RELATIONAL_INDEX_SHADOW_ROOTS: usize = 4096;
pub const DEFAULT_RELATIONAL_INDEX_SHADOW_BUILD_METADATA_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_RELATIONAL_INDEX_SORT_MEMORY_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_RELATIONAL_INDEX_SORT_SPILL_BYTES: u64 = 4 * 1024 * 1024 * 1024 * 1024;
pub const DEFAULT_RELATIONAL_INDEX_SORT_RUNS: usize = 4096;
pub const DEFAULT_RELATIONAL_INDEX_SORT_MERGE_FAN_IN: usize = 32;

pub fn relational_index_shadow_artifact_file(generation: u64) -> String {
    format!("relational-index-shadow-{generation}.pages.skein")
}

pub fn relational_index_shadow_manifest_generation_file(generation: u64) -> String {
    format!("relational-index-shadow-{generation}.manifest.skein")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalIndexGenerationIdentity {
    pub generation: u64,
    pub source_commit_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalIndexArtifactMetadata {
    pub encoded_len: u64,
    pub encoded_crc32c: u64,
    pub encoded_sha256: Sha256Digest,
}

impl RelationalIndexArtifactMetadata {
    fn from_digest(encoded_len: u64, digest: IntegrityDigest) -> Self {
        Self {
            encoded_len,
            encoded_crc32c: digest.crc32c.as_u64(),
            encoded_sha256: digest.sha256,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalIndexGenerationArtifacts {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub catalog_schema_digest: Sha256Digest,
    pub root_set_digest: Sha256Digest,
    pub page_artifact: RelationalIndexArtifactMetadata,
    pub manifest_artifact: RelationalIndexArtifactMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalIndexShadowConfig {
    pub page_limits: ImmutableIndexPageLimits,
    pub max_manifest_bytes: NonZeroUsize,
    pub max_roots: NonZeroUsize,
    pub max_build_metadata_bytes: NonZeroUsize,
    pub max_sort_memory_bytes: NonZeroUsize,
    pub max_sort_spill_bytes: NonZeroU64,
    pub max_sort_runs: NonZeroUsize,
    pub max_sort_merge_fan_in: NonZeroUsize,
}

impl Default for RelationalIndexShadowConfig {
    fn default() -> Self {
        Self {
            page_limits: ImmutableIndexPageLimits::default(),
            max_manifest_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_INDEX_SHADOW_MANIFEST_BYTES)
                .expect("default relational index manifest limit is non-zero"),
            max_roots: NonZeroUsize::new(DEFAULT_RELATIONAL_INDEX_SHADOW_ROOTS)
                .expect("default relational index root limit is non-zero"),
            max_build_metadata_bytes: NonZeroUsize::new(
                DEFAULT_RELATIONAL_INDEX_SHADOW_BUILD_METADATA_BYTES,
            )
            .expect("default relational index build metadata limit is non-zero"),
            max_sort_memory_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_INDEX_SORT_MEMORY_BYTES)
                .expect("default relational index sort memory limit is non-zero"),
            max_sort_spill_bytes: NonZeroU64::new(DEFAULT_RELATIONAL_INDEX_SORT_SPILL_BYTES)
                .expect("default relational index sort spill limit is non-zero"),
            max_sort_runs: NonZeroUsize::new(DEFAULT_RELATIONAL_INDEX_SORT_RUNS)
                .expect("default relational index sort run limit is non-zero"),
            max_sort_merge_fan_in: NonZeroUsize::new(DEFAULT_RELATIONAL_INDEX_SORT_MERGE_FAN_IN)
                .expect("default relational index sort merge fan-in is non-zero"),
        }
    }
}

/// Exact cardinalities for one leading index-key prefix at the manifest epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalIndexPrefixStatistics {
    /// Distinct prefixes whose components are all non-null.
    pub distinct_non_null_values: u64,
    /// Indexed rows whose prefix components are all non-null.
    pub non_null_rows: u64,
    /// Maximum rows observed for one non-null leading-prefix value.
    pub fanout: u64,
}

/// Prefix statistics published atomically with one immutable index root.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationalIndexStatistics {
    pub leading_prefixes: Vec<RelationalIndexPrefixStatistics>,
}

impl RelationalIndexStatistics {
    pub fn leading_prefix(&self, prefix_len: usize) -> Option<&RelationalIndexPrefixStatistics> {
        prefix_len
            .checked_sub(1)
            .and_then(|ordinal| self.leading_prefixes.get(ordinal))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexRootDescriptor {
    pub identity: IndexIdentity,
    pub role: RelationalIndexRole,
    pub schema_digest: Sha256Digest,
    pub root_page_id: IndexPageId,
    pub height: u32,
    pub statistics: RelationalIndexStatistics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexShadowManifest {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub page_bytes: u64,
    pub page_count: u64,
    pub catalog_schema_digest: Sha256Digest,
    pub root_set_digest: Sha256Digest,
    pub roots: Vec<RelationalIndexRootDescriptor>,
}

impl RelationalIndexShadowManifest {
    fn from_roots(
        generation: u64,
        source_commit_epoch: u64,
        page_bytes: u64,
        page_count: u64,
        roots: Vec<RelationalIndexRootDescriptor>,
    ) -> Result<Self, RelationalIndexShadowError> {
        let logical_roots = logical_roots(&roots);
        Ok(Self {
            generation,
            source_commit_epoch,
            page_bytes,
            page_count,
            catalog_schema_digest: catalog_schema_digest(&logical_roots, ErrorClass::Admission)?,
            root_set_digest: root_set_digest(&logical_roots),
            roots,
        })
    }

    pub fn root(&self, table: &str, index: &str) -> Option<&RelationalIndexRootDescriptor> {
        self.roots
            .binary_search_by(|root| {
                (
                    root.identity.namespace.as_str(),
                    root.identity.name.as_str(),
                )
                    .cmp(&(table, index))
            })
            .ok()
            .map(|position| &self.roots[position])
    }

    fn encode(
        &self,
        config: RelationalIndexShadowConfig,
    ) -> Result<Vec<u8>, RelationalIndexShadowError> {
        validate_manifest(self, config, ErrorClass::Admission)?;
        let mut payload = Vec::new();
        for root in &self.roots {
            encode_bytes(&mut payload, root.identity.namespace.as_bytes())?;
            encode_bytes(&mut payload, root.identity.name.as_bytes())?;
            payload.push(relational_index_role_tag(root.role));
            payload.extend_from_slice(root.schema_digest.as_bytes());
            payload.extend_from_slice(&root.root_page_id.get().to_le_bytes());
            payload.extend_from_slice(&root.height.to_le_bytes());
            encode_count(
                &mut payload,
                root.statistics.leading_prefixes.len(),
                "leading-prefix statistics",
            )?;
            for statistics in &root.statistics.leading_prefixes {
                payload.extend_from_slice(&statistics.distinct_non_null_values.to_le_bytes());
                payload.extend_from_slice(&statistics.non_null_rows.to_le_bytes());
                payload.extend_from_slice(&statistics.fanout.to_le_bytes());
            }
            if MANIFEST_HEADER_BYTES.saturating_add(payload.len()) > config.max_manifest_bytes.get()
            {
                return Err(RelationalIndexShadowError::Admission(format!(
                    "relational index manifest exceeds {} bytes",
                    config.max_manifest_bytes
                )));
            }
        }
        let payload_len = u64::try_from(payload.len()).map_err(|_| {
            RelationalIndexShadowError::Admission(
                "relational index manifest payload length does not fit u64".to_string(),
            )
        })?;
        let root_count = u32::try_from(self.roots.len()).map_err(|_| {
            RelationalIndexShadowError::Admission(
                "relational index root count does not fit u32".to_string(),
            )
        })?;
        let mut encoded = Vec::with_capacity(MANIFEST_HEADER_BYTES + payload.len());
        encoded.extend_from_slice(MANIFEST_MAGIC);
        encoded.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        encoded.extend_from_slice(&0_u16.to_le_bytes());
        encoded.extend_from_slice(&self.generation.to_le_bytes());
        encoded.extend_from_slice(&self.source_commit_epoch.to_le_bytes());
        encoded.extend_from_slice(&self.page_bytes.to_le_bytes());
        encoded.extend_from_slice(&self.page_count.to_le_bytes());
        encoded.extend_from_slice(&root_count.to_le_bytes());
        encoded.extend_from_slice(&payload_len.to_le_bytes());
        encoded.extend_from_slice(self.catalog_schema_digest.as_bytes());
        encoded.extend_from_slice(self.root_set_digest.as_bytes());
        let mut hasher = IntegrityHasher::new();
        hasher.update(&encoded);
        hasher.update(&payload);
        let digest = hasher.finish();
        encoded.extend_from_slice(&digest.crc32c.get().to_le_bytes());
        encoded.extend_from_slice(digest.sha256.as_bytes());
        debug_assert_eq!(encoded.len(), MANIFEST_HEADER_BYTES);
        encoded.extend_from_slice(&payload);
        Ok(encoded)
    }

    fn decode(
        encoded: &[u8],
        config: RelationalIndexShadowConfig,
    ) -> Result<Self, RelationalIndexShadowError> {
        if encoded.len() > config.max_manifest_bytes.get() {
            return Err(RelationalIndexShadowError::Admission(format!(
                "relational index manifest contains {} bytes, exceeding limit {}",
                encoded.len(),
                config.max_manifest_bytes
            )));
        }
        if encoded.len() < MANIFEST_HEADER_BYTES || &encoded[..8] != MANIFEST_MAGIC {
            return Err(RelationalIndexShadowError::Corrupt(
                "invalid relational index manifest header".to_string(),
            ));
        }
        let version = read_u16(&encoded[8..10]);
        let flags = read_u16(&encoded[10..12]);
        if version != MANIFEST_VERSION || flags != 0 {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "unsupported relational index manifest version {version} or flags {flags}"
            )));
        }
        let generation = read_u64(&encoded[12..20]);
        let source_commit_epoch = read_u64(&encoded[20..28]);
        let page_bytes = read_u64(&encoded[28..36]);
        let page_count = read_u64(&encoded[36..44]);
        let root_count = read_u32(&encoded[44..48]) as usize;
        if root_count > config.max_roots.get() {
            return Err(RelationalIndexShadowError::Admission(format!(
                "relational index manifest declares {root_count} roots, exceeding limit {}",
                config.max_roots
            )));
        }
        let payload_len = usize::try_from(read_u64(&encoded[48..56])).map_err(|_| {
            RelationalIndexShadowError::Corrupt(
                "relational index manifest payload length overflows usize".to_string(),
            )
        })?;
        let expected_len = MANIFEST_HEADER_BYTES
            .checked_add(payload_len)
            .ok_or_else(|| {
                RelationalIndexShadowError::Corrupt(
                    "relational index manifest length overflow".to_string(),
                )
            })?;
        if encoded.len() != expected_len {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "relational index manifest length mismatch: expected {expected_len}, got {}",
                encoded.len()
            )));
        }
        let catalog_schema_digest = Sha256Digest::from_bytes(
            encoded[56..88]
                .try_into()
                .expect("catalog schema digest length was checked"),
        );
        let root_set_digest = Sha256Digest::from_bytes(
            encoded[88..120]
                .try_into()
                .expect("root-set digest length was checked"),
        );
        let payload = &encoded[MANIFEST_HEADER_BYTES..];
        let mut hasher = IntegrityHasher::new();
        hasher.update(&encoded[..MANIFEST_INTEGRITY_OFFSET]);
        hasher.update(payload);
        let digest = hasher.finish();
        let expected_crc = read_u32(&encoded[120..124]);
        if digest.crc32c.get() != expected_crc
            || digest.sha256.as_bytes() != &encoded[124..124 + SHA256_BYTES]
        {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index manifest checksum mismatch".to_string(),
            ));
        }
        let mut offset = 0usize;
        let mut roots = Vec::with_capacity(root_count);
        for _ in 0..root_count {
            let (namespace, next) = decode_bytes(
                payload,
                offset,
                config.page_limits.max_identity_bytes.get(),
                "index namespace",
            )?;
            offset = next;
            let remaining = config
                .page_limits
                .max_identity_bytes
                .get()
                .saturating_sub(namespace.len());
            let (name, next) = decode_bytes(payload, offset, remaining, "index name")?;
            offset = next;
            let role =
                decode_relational_index_role(take(payload, &mut offset, 1, "index role")?[0])?;
            let digest_bytes = take(payload, &mut offset, SHA256_BYTES, "schema digest")?;
            let root_id_bytes = take(payload, &mut offset, 8, "root page id")?;
            let height_bytes = take(payload, &mut offset, 4, "root height")?;
            let prefix_count = read_u32(take(
                payload,
                &mut offset,
                4,
                "leading-prefix statistics count",
            )?) as usize;
            let statistics_bytes = prefix_count.checked_mul(3 * 8).ok_or_else(|| {
                RelationalIndexShadowError::Corrupt(
                    "leading-prefix statistics length overflow".to_string(),
                )
            })?;
            let statistics_bytes = take(
                payload,
                &mut offset,
                statistics_bytes,
                "leading-prefix statistics",
            )?;
            let leading_prefixes = statistics_bytes
                .chunks_exact(3 * 8)
                .map(|encoded| RelationalIndexPrefixStatistics {
                    distinct_non_null_values: read_u64(&encoded[..8]),
                    non_null_rows: read_u64(&encoded[8..16]),
                    fanout: read_u64(&encoded[16..24]),
                })
                .collect();
            roots.push(RelationalIndexRootDescriptor {
                identity: IndexIdentity {
                    namespace: decode_utf8(namespace, "index namespace")?,
                    name: decode_utf8(name, "index name")?,
                },
                role,
                schema_digest: Sha256Digest::from_bytes(
                    digest_bytes
                        .try_into()
                        .expect("schema digest length was checked"),
                ),
                root_page_id: page_id(read_u64(root_id_bytes), "root page id")?,
                height: read_u32(height_bytes),
                statistics: RelationalIndexStatistics { leading_prefixes },
            });
        }
        if offset != payload.len() {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index manifest contains trailing bytes".to_string(),
            ));
        }
        let manifest = Self {
            generation,
            source_commit_epoch,
            page_bytes,
            page_count,
            catalog_schema_digest,
            root_set_digest,
            roots,
        };
        validate_manifest(&manifest, config, ErrorClass::Corrupt)?;
        Ok(manifest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexShadowBuildReport {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub index_roots: usize,
    pub pages_written: u64,
    pub artifact_bytes: u64,
    pub manifest_bytes: u64,
    pub peak_build_metadata_bytes: usize,
    pub sort_spill_run_count: usize,
    pub sort_spill_bytes: u64,
    pub peak_sort_memory_bytes: usize,
    pub generation_artifacts: RelationalIndexGenerationArtifacts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalIndexShadowCheckpointStatus {
    Published,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexShadowCheckpointReport {
    pub status: RelationalIndexShadowCheckpointStatus,
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub index_roots: usize,
    pub pages_written: u64,
    pub artifact_bytes: u64,
    pub manifest_bytes: u64,
    pub peak_build_metadata_bytes: usize,
    pub sort_spill_run_count: usize,
    pub sort_spill_bytes: u64,
    pub peak_sort_memory_bytes: usize,
    pub generation_artifacts: Option<RelationalIndexGenerationArtifacts>,
    pub error: Option<String>,
}

impl RelationalIndexShadowCheckpointReport {
    pub fn published(report: RelationalIndexShadowBuildReport) -> Self {
        Self {
            status: RelationalIndexShadowCheckpointStatus::Published,
            generation: report.generation,
            source_commit_epoch: report.source_commit_epoch,
            index_roots: report.index_roots,
            pages_written: report.pages_written,
            artifact_bytes: report.artifact_bytes,
            manifest_bytes: report.manifest_bytes,
            peak_build_metadata_bytes: report.peak_build_metadata_bytes,
            sort_spill_run_count: report.sort_spill_run_count,
            sort_spill_bytes: report.sort_spill_bytes,
            peak_sort_memory_bytes: report.peak_sort_memory_bytes,
            generation_artifacts: Some(report.generation_artifacts),
            error: None,
        }
    }

    pub fn failed(generation: u64, source_commit_epoch: u64, error: String) -> Self {
        Self {
            status: RelationalIndexShadowCheckpointStatus::Failed,
            generation,
            source_commit_epoch,
            index_roots: 0,
            pages_written: 0,
            artifact_bytes: 0,
            manifest_bytes: 0,
            peak_build_metadata_bytes: 0,
            sort_spill_run_count: 0,
            sort_spill_bytes: 0,
            peak_sort_memory_bytes: 0,
            generation_artifacts: None,
            error: Some(error),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RelationalIndexShadowRecoveryStatus {
    #[default]
    Disabled,
    Missing,
    CheckpointReady {
        generation: u64,
        source_commit_epoch: u64,
        index_roots: usize,
        page_count: u64,
    },
    WalRecovered {
        base_generation: u64,
        base_commit_epoch: u64,
        recovered_commit_epoch: u64,
        delta_pages: usize,
        delta_entries: usize,
        peak_dirty_bytes: usize,
    },
    LiveCurrent {
        base_generation: u64,
        delta_generation: Option<u64>,
        base_commit_epoch: u64,
        visible_commit_epoch: u64,
        live_batches: usize,
        live_entries: usize,
        live_bytes: usize,
    },
    LiveUnavailable {
        base_generation: u64,
        base_commit_epoch: u64,
        last_visible_commit_epoch: u64,
        failed_commit_epoch: u64,
        reason: String,
    },
    RecoveryUnavailable {
        base_generation: u64,
        base_commit_epoch: u64,
        recovered_commit_epoch: u64,
        reason: String,
    },
    CandidateUnavailable {
        generation: u64,
        source_commit_epoch: u64,
        reason: String,
    },
    Stale {
        generation: u64,
        source_commit_epoch: u64,
        checkpoint_generation: u64,
        checkpoint_commit_epoch: u64,
    },
    DiscardedInvalid {
        error: String,
    },
    InvalidWritable {
        error: String,
    },
    InvalidReadOnly {
        error: String,
    },
}

#[derive(Debug)]
pub enum RelationalIndexShadowError {
    Admission(String),
    Corrupt(String),
    Durability(String),
    MissingIndex {
        table: String,
        index: String,
    },
    StaleGeneration {
        expected_previous: Option<u64>,
        actual_previous: Option<u64>,
    },
}

impl fmt::Display for RelationalIndexShadowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => {
                write!(formatter, "relational index shadow admission failed: {message}")
            }
            Self::Corrupt(message) => write!(formatter, "corrupt relational index shadow: {message}"),
            Self::Durability(message) => {
                write!(formatter, "relational index shadow durability failed: {message}")
            }
            Self::MissingIndex { table, index } => {
                write!(formatter, "relational index shadow has no root for {table}.{index}")
            }
            Self::StaleGeneration {
                expected_previous,
                actual_previous,
            } => write!(
                formatter,
                "relational index shadow generation changed: expected {expected_previous:?}, found {actual_previous:?}"
            ),
        }
    }
}

impl std::error::Error for RelationalIndexShadowError {}

impl From<ImmutableIndexPageError> for RelationalIndexShadowError {
    fn from(error: ImmutableIndexPageError) -> Self {
        match error {
            ImmutableIndexPageError::Admission(message) => Self::Admission(message),
            ImmutableIndexPageError::Corrupt(message) => Self::Corrupt(message),
        }
    }
}

impl From<OrderedRelationalKeyError> for RelationalIndexShadowError {
    fn from(error: OrderedRelationalKeyError) -> Self {
        Self::Corrupt(error.to_string())
    }
}

pub struct RelationalIndexShadowWriter {
    config: RelationalIndexShadowConfig,
}

/// Repeatable row traversal used by the bounded relational index builder.
///
/// Implementations may read resident rows or page a canonical row root. The
/// writer invokes the source once per logical index so secondary-key sorting
/// retains its existing bounded spill discipline.
pub trait RelationalIndexRowSource {
    fn visit_rows(
        &self,
        table: &str,
        visit: &mut dyn FnMut(
            &RelationalKey,
            &super::RelationalRow,
        ) -> Result<(), RelationalIndexShadowError>,
    ) -> Result<(), RelationalIndexShadowError>;
}

impl RelationalIndexRowSource for RelationalState {
    fn visit_rows(
        &self,
        table: &str,
        visit: &mut dyn FnMut(
            &RelationalKey,
            &super::RelationalRow,
        ) -> Result<(), RelationalIndexShadowError>,
    ) -> Result<(), RelationalIndexShadowError> {
        let segment = self.segments.get(table).ok_or_else(|| {
            RelationalIndexShadowError::Corrupt(format!(
                "table {table} is missing its relational segment"
            ))
        })?;
        for (primary_key, row) in segment.rows.iter() {
            visit(primary_key, row)?;
        }
        Ok(())
    }
}

impl RelationalIndexShadowWriter {
    pub const fn new(config: RelationalIndexShadowConfig) -> Self {
        Self { config }
    }

    pub fn publish(
        &self,
        directory: &Path,
        state: &RelationalState,
        generation: u64,
        source_commit_epoch: u64,
        expected_previous_generation: Option<u64>,
    ) -> Result<RelationalIndexShadowBuildReport, RelationalIndexShadowError> {
        if generation == 0 {
            return Err(RelationalIndexShadowError::Admission(
                "relational index generation must be non-zero".to_string(),
            ));
        }
        let _lock = acquire_publication_lock(directory)?;
        cleanup_stale_sort_runs(directory)?;
        let paths = ShadowPublicationPaths::new(directory, generation);
        let actual_previous = current_generation(&paths.manifest, self.config)?;
        if actual_previous != expected_previous_generation {
            return Err(RelationalIndexShadowError::StaleGeneration {
                expected_previous: expected_previous_generation,
                actual_previous,
            });
        }
        if actual_previous.is_some_and(|previous| generation <= previous) {
            return Err(RelationalIndexShadowError::Admission(format!(
                "new relational index generation {generation} must exceed published generation {}",
                actual_previous.expect("checked as present")
            )));
        }
        let result = self.build_and_publish(
            state,
            generation,
            source_commit_epoch,
            Some(expected_previous_generation),
            &paths,
        );
        if result.is_err() {
            let _ = fs::remove_file(&paths.artifact_tmp);
            let _ = fs::remove_file(&paths.manifest_tmp);
        }
        result
    }

    /// Writes one immutable candidate without changing the legacy latest
    /// pointer. The generation-specific manifest is durable before the
    /// canonical checkpoint with the same generation can be published.
    pub fn publish_generation(
        &self,
        directory: &Path,
        state: &RelationalState,
        generation: u64,
        source_commit_epoch: u64,
    ) -> Result<RelationalIndexShadowBuildReport, RelationalIndexShadowError> {
        if generation == 0 {
            return Err(RelationalIndexShadowError::Admission(
                "relational index generation must be non-zero".to_string(),
            ));
        }
        let _lock = acquire_publication_lock(directory)?;
        cleanup_stale_sort_runs(directory)?;
        let paths = ShadowPublicationPaths::for_generation(directory, generation);
        let result = self.build_and_publish(state, generation, source_commit_epoch, None, &paths);
        if result.is_err() {
            let _ = fs::remove_file(&paths.artifact_tmp);
            let _ = fs::remove_file(&paths.manifest_tmp);
        }
        result
    }

    pub fn publish_generation_from_source(
        &self,
        directory: &Path,
        state: &RelationalState,
        source: &dyn RelationalIndexRowSource,
        generation: u64,
        source_commit_epoch: u64,
    ) -> Result<RelationalIndexShadowBuildReport, RelationalIndexShadowError> {
        if generation == 0 {
            return Err(RelationalIndexShadowError::Admission(
                "relational index generation must be non-zero".to_string(),
            ));
        }
        let _lock = acquire_publication_lock(directory)?;
        cleanup_stale_sort_runs(directory)?;
        let paths = ShadowPublicationPaths::for_generation(directory, generation);
        let result = self.build_and_publish_from_source(
            state,
            source,
            generation,
            source_commit_epoch,
            None,
            &paths,
        );
        if result.is_err() {
            let _ = fs::remove_file(&paths.artifact_tmp);
            let _ = fs::remove_file(&paths.manifest_tmp);
        }
        result
    }

    fn build_and_publish(
        &self,
        state: &RelationalState,
        generation: u64,
        source_commit_epoch: u64,
        expected_previous_generation: Option<Option<u64>>,
        paths: &ShadowPublicationPaths,
    ) -> Result<RelationalIndexShadowBuildReport, RelationalIndexShadowError> {
        self.build_and_publish_from_source(
            state,
            state,
            generation,
            source_commit_epoch,
            expected_previous_generation,
            paths,
        )
    }

    fn build_and_publish_from_source(
        &self,
        state: &RelationalState,
        source: &dyn RelationalIndexRowSource,
        generation: u64,
        source_commit_epoch: u64,
        expected_previous_generation: Option<Option<u64>>,
        paths: &ShadowPublicationPaths,
    ) -> Result<RelationalIndexShadowBuildReport, RelationalIndexShadowError> {
        if self.config.max_sort_merge_fan_in.get() < 2 {
            return Err(RelationalIndexShadowError::Admission(
                "relational index sort merge fan-in must be at least two".to_string(),
            ));
        }
        required_root_count(state, self.config.max_roots.get(), ErrorClass::Admission)?;
        let file =
            File::create(&paths.artifact_tmp).map_err(durability("create shadow artifact"))?;
        let mut pages = SlotWriter::new(
            file,
            generation,
            source_commit_epoch,
            self.config.page_limits,
        );
        let mut roots = Vec::new();
        let mut peak_build_metadata_bytes = 0usize;
        let mut sort_spill_run_count = 0usize;
        let mut sort_spill_bytes = 0u64;
        let mut peak_sort_memory_bytes = 0usize;
        for (table, schema) in &state.schemas {
            let schema_digest = relational_schema_digest(schema)?;
            for definition in schema.required_index_definitions() {
                let identity = IndexIdentity {
                    namespace: table.clone(),
                    name: definition.name.clone(),
                };
                let mut tree = TreeWriter::new(
                    &mut pages,
                    identity,
                    definition.role,
                    schema_digest,
                    definition.columns.len(),
                    self.config.max_build_metadata_bytes.get(),
                );
                if definition.role == RelationalIndexRole::Primary {
                    source.visit_rows(table, &mut |primary_key, _| {
                        let encoded = encode_ordered_relational_key(primary_key)?;
                        tree.push(IndexLeafEntry {
                            key: encoded.clone(),
                            posting: IndexLeafPosting::Inline(vec![IndexRowId::new(encoded)]),
                        })
                    })?;
                } else {
                    let remaining_spill_bytes = self
                        .config
                        .max_sort_spill_bytes
                        .get()
                        .checked_sub(sort_spill_bytes)
                        .ok_or_else(|| {
                            RelationalIndexShadowError::Admission(
                                "relational index build exhausted its spill byte budget"
                                    .to_string(),
                            )
                        })?;
                    let sort_report = build::write_index_from_rows(
                        &mut tree,
                        build::IndexBuildInput {
                            source,
                            table,
                            schema,
                            definition: &definition,
                            spill_prefix: &paths.artifact_tmp,
                            generation,
                            root_ordinal: roots.len(),
                            config: self.config,
                            max_spill_bytes: remaining_spill_bytes,
                        },
                    )?;
                    sort_spill_run_count = sort_spill_run_count
                        .checked_add(sort_report.spill_run_count)
                        .ok_or_else(|| {
                            RelationalIndexShadowError::Admission(
                                "relational index spill run count overflow".to_string(),
                            )
                        })?;
                    sort_spill_bytes = sort_spill_bytes
                        .checked_add(sort_report.spill_bytes)
                        .ok_or_else(|| {
                            RelationalIndexShadowError::Admission(
                                "relational index spill byte count overflow".to_string(),
                            )
                        })?;
                    peak_sort_memory_bytes =
                        peak_sort_memory_bytes.max(sort_report.peak_memory_bytes);
                }
                let (root, peak) = tree.finish()?;
                peak_build_metadata_bytes = peak_build_metadata_bytes.max(peak);
                roots.push(root);
            }
            if roots.len() > self.config.max_roots.get() {
                return Err(RelationalIndexShadowError::Admission(format!(
                    "relational index shadow contains {} roots, exceeding limit {}",
                    roots.len(),
                    self.config.max_roots
                )));
            }
        }
        roots.sort_by(|left, right| {
            (&left.identity.namespace, &left.identity.name)
                .cmp(&(&right.identity.namespace, &right.identity.name))
        });
        if roots
            .windows(2)
            .any(|pair| pair[0].identity == pair[1].identity)
        {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index shadow contains duplicate index identities".to_string(),
            ));
        }
        let (page_count, page_artifact) = pages.finish()?;
        let page_bytes = self.config.page_limits.max_page_bytes.get() as u64;
        let artifact_bytes = page_artifact.encoded_len;
        durable_replace_file(&paths.artifact_tmp, &paths.artifact)
            .map_err(durability("publish shadow artifact"))?;

        let manifest = RelationalIndexShadowManifest::from_roots(
            generation,
            source_commit_epoch,
            page_bytes,
            page_count,
            roots,
        )?;
        let encoded_manifest = manifest.encode(self.config)?;
        let manifest_artifact = RelationalIndexArtifactMetadata::from_digest(
            encoded_manifest.len() as u64,
            integrity_digest(&encoded_manifest),
        );
        {
            let mut file = File::create(&paths.manifest_tmp)
                .map_err(durability("create shadow manifest candidate"))?;
            file.write_all(&encoded_manifest)
                .map_err(durability("write shadow manifest candidate"))?;
            file.sync_all()
                .map_err(durability("sync shadow manifest candidate"))?;
        }
        if let Some(expected_previous_generation) = expected_previous_generation {
            let actual_previous = current_generation(&paths.manifest, self.config)?;
            if actual_previous != expected_previous_generation {
                return Err(RelationalIndexShadowError::StaleGeneration {
                    expected_previous: expected_previous_generation,
                    actual_previous,
                });
            }
        }
        durable_replace_file(&paths.manifest_tmp, &paths.manifest)
            .map_err(durability("publish shadow manifest"))?;
        Ok(RelationalIndexShadowBuildReport {
            generation,
            source_commit_epoch,
            index_roots: manifest.roots.len(),
            pages_written: page_count,
            artifact_bytes,
            manifest_bytes: encoded_manifest.len() as u64,
            peak_build_metadata_bytes,
            sort_spill_run_count,
            sort_spill_bytes,
            peak_sort_memory_bytes,
            generation_artifacts: RelationalIndexGenerationArtifacts {
                generation,
                source_commit_epoch,
                catalog_schema_digest: manifest.catalog_schema_digest,
                root_set_digest: manifest.root_set_digest,
                page_artifact,
                manifest_artifact,
            },
        })
    }
}

fn acquire_publication_lock(directory: &Path) -> Result<File, RelationalIndexShadowError> {
    fs::create_dir_all(directory).map_err(durability("create relational index directory"))?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join(RELATIONAL_INDEX_SHADOW_LOCK_FILE))
        .map_err(durability("open relational index publication lock"))?;
    lock.lock()
        .map_err(durability("lock relational index publication"))?;
    Ok(lock)
}

fn cleanup_stale_sort_runs(directory: &Path) -> Result<(), RelationalIndexShadowError> {
    for entry in fs::read_dir(directory).map_err(durability("list relational index directory"))? {
        let entry = entry.map_err(durability("read relational index directory entry"))?;
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if file_name.starts_with(".relational-index.")
            && file_name.contains(".run.")
            && file_name.ends_with(".tmp")
        {
            fs::remove_file(entry.path()).map_err(durability("remove stale index sort run"))?;
        }
    }
    Ok(())
}

struct ShadowPublicationPaths {
    artifact: PathBuf,
    artifact_tmp: PathBuf,
    manifest: PathBuf,
    manifest_tmp: PathBuf,
}

impl ShadowPublicationPaths {
    fn new(directory: &Path, generation: u64) -> Self {
        let artifact = directory.join(relational_index_shadow_artifact_file(generation));
        let manifest = directory.join(RELATIONAL_INDEX_SHADOW_MANIFEST_FILE);
        Self {
            artifact_tmp: artifact.with_extension("skein.tmp"),
            manifest_tmp: manifest.with_extension("skein.tmp"),
            artifact,
            manifest,
        }
    }

    fn for_generation(directory: &Path, generation: u64) -> Self {
        let artifact = directory.join(relational_index_shadow_artifact_file(generation));
        let manifest = directory.join(relational_index_shadow_manifest_generation_file(generation));
        Self {
            artifact_tmp: artifact.with_extension("skein.tmp"),
            manifest_tmp: manifest.with_extension("skein.tmp"),
            artifact,
            manifest,
        }
    }
}

pub struct RelationalIndexShadowReader {
    directory: PathBuf,
    manifest: RelationalIndexShadowManifest,
    config: RelationalIndexShadowConfig,
    page_cache: Option<Arc<SegmentCache>>,
    store_id: StoreId,
    artifact: OnceLock<File>,
    poisoned: AtomicBool,
}

pub(super) struct RelationalIndexPageRead {
    pub page: ImmutableIndexPage,
    pub cache_hit: bool,
    pub cache_miss: bool,
    pub cache_admission_rejected: bool,
}

impl RelationalIndexShadowReader {
    pub fn open_latest(
        directory: &Path,
        config: RelationalIndexShadowConfig,
    ) -> Result<Self, RelationalIndexShadowError> {
        Self::open_latest_inner(directory, config, None, StoreId::default())
    }

    pub fn open_latest_with_cache(
        directory: &Path,
        config: RelationalIndexShadowConfig,
        page_cache: Arc<SegmentCache>,
        store_id: StoreId,
    ) -> Result<Self, RelationalIndexShadowError> {
        Self::open_latest_inner(directory, config, Some(page_cache), store_id)
    }

    pub fn open_generation(
        directory: &Path,
        expected: RelationalIndexGenerationIdentity,
        config: RelationalIndexShadowConfig,
    ) -> Result<Self, RelationalIndexShadowError> {
        Self::open_generation_inner(directory, expected, config, None, StoreId::default())
    }

    pub fn open_generation_with_cache(
        directory: &Path,
        expected: RelationalIndexGenerationIdentity,
        config: RelationalIndexShadowConfig,
        page_cache: Arc<SegmentCache>,
        store_id: StoreId,
    ) -> Result<Self, RelationalIndexShadowError> {
        Self::open_generation_inner(directory, expected, config, Some(page_cache), store_id)
    }

    pub fn open_bound_generation(
        directory: &Path,
        binding: RelationalIndexGenerationArtifacts,
        config: RelationalIndexShadowConfig,
    ) -> Result<Self, RelationalIndexShadowError> {
        Self::open_bound_generation_inner(directory, binding, config, None, StoreId::default())
    }

    pub fn open_bound_generation_with_cache(
        directory: &Path,
        binding: RelationalIndexGenerationArtifacts,
        config: RelationalIndexShadowConfig,
        page_cache: Arc<SegmentCache>,
        store_id: StoreId,
    ) -> Result<Self, RelationalIndexShadowError> {
        Self::open_bound_generation_inner(directory, binding, config, Some(page_cache), store_id)
    }

    fn open_bound_generation_inner(
        directory: &Path,
        binding: RelationalIndexGenerationArtifacts,
        config: RelationalIndexShadowConfig,
        page_cache: Option<Arc<SegmentCache>>,
        store_id: StoreId,
    ) -> Result<Self, RelationalIndexShadowError> {
        let manifest_path = directory.join(relational_index_shadow_manifest_generation_file(
            binding.generation,
        ));
        let encoded = read_bounded_file(
            &manifest_path,
            config.max_manifest_bytes.get(),
            "bound relational index manifest",
        )?;
        let digest = integrity_digest(&encoded);
        if encoded.len() as u64 != binding.manifest_artifact.encoded_len
            || digest.crc32c.as_u64() != binding.manifest_artifact.encoded_crc32c
            || digest.sha256 != binding.manifest_artifact.encoded_sha256
        {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index generation manifest does not match its canonical binding"
                    .to_string(),
            ));
        }
        let manifest = RelationalIndexShadowManifest::decode(&encoded, config)?;
        let expected_page_bytes = manifest
            .page_count
            .checked_mul(manifest.page_bytes)
            .ok_or_else(|| {
                RelationalIndexShadowError::Corrupt(
                    "relational index shadow artifact size overflow".to_string(),
                )
            })?;
        if manifest.generation != binding.generation
            || manifest.source_commit_epoch != binding.source_commit_epoch
            || manifest.catalog_schema_digest != binding.catalog_schema_digest
            || manifest.root_set_digest != binding.root_set_digest
            || expected_page_bytes != binding.page_artifact.encoded_len
        {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index generation identity does not match its canonical binding"
                    .to_string(),
            ));
        }
        Self::from_manifest(directory, manifest, config, page_cache, store_id)
    }

    fn open_generation_inner(
        directory: &Path,
        expected: RelationalIndexGenerationIdentity,
        config: RelationalIndexShadowConfig,
        page_cache: Option<Arc<SegmentCache>>,
        store_id: StoreId,
    ) -> Result<Self, RelationalIndexShadowError> {
        let manifest_path = directory.join(relational_index_shadow_manifest_generation_file(
            expected.generation,
        ));
        let reader =
            Self::open_manifest_inner(directory, &manifest_path, config, page_cache, store_id)?;
        if reader.manifest.generation != expected.generation
            || reader.manifest.source_commit_epoch != expected.source_commit_epoch
        {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "relational index generation/epoch {}/{} does not match checkpoint {}/{}",
                reader.manifest.generation,
                reader.manifest.source_commit_epoch,
                expected.generation,
                expected.source_commit_epoch,
            )));
        }
        Ok(reader)
    }

    fn open_latest_inner(
        directory: &Path,
        config: RelationalIndexShadowConfig,
        page_cache: Option<Arc<SegmentCache>>,
        store_id: StoreId,
    ) -> Result<Self, RelationalIndexShadowError> {
        let manifest_path = directory.join(RELATIONAL_INDEX_SHADOW_MANIFEST_FILE);
        Self::open_manifest_inner(directory, &manifest_path, config, page_cache, store_id)
    }

    fn open_manifest_inner(
        directory: &Path,
        manifest_path: &Path,
        config: RelationalIndexShadowConfig,
        page_cache: Option<Arc<SegmentCache>>,
        store_id: StoreId,
    ) -> Result<Self, RelationalIndexShadowError> {
        let encoded = read_bounded_file(
            manifest_path,
            config.max_manifest_bytes.get(),
            "relational index manifest",
        )?;
        let manifest = RelationalIndexShadowManifest::decode(&encoded, config)?;
        Self::from_manifest(directory, manifest, config, page_cache, store_id)
    }

    pub fn open(
        directory: &Path,
        expected_generation: u64,
        expected_source_commit_epoch: u64,
        config: RelationalIndexShadowConfig,
    ) -> Result<Self, RelationalIndexShadowError> {
        let reader = Self::open_latest(directory, config)?;
        if reader.manifest.generation != expected_generation
            || reader.manifest.source_commit_epoch != expected_source_commit_epoch
        {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "relational index shadow fence mismatch: expected generation/epoch {expected_generation}/{expected_source_commit_epoch}, found {}/{}",
                reader.manifest.generation, reader.manifest.source_commit_epoch
            )));
        }
        Ok(reader)
    }

    fn from_manifest(
        directory: &Path,
        manifest: RelationalIndexShadowManifest,
        config: RelationalIndexShadowConfig,
        page_cache: Option<Arc<SegmentCache>>,
        store_id: StoreId,
    ) -> Result<Self, RelationalIndexShadowError> {
        let artifact_path =
            directory.join(relational_index_shadow_artifact_file(manifest.generation));
        let actual_bytes = fs::metadata(&artifact_path)
            .map_err(durability("inspect shadow artifact"))?
            .len();
        let expected_bytes = manifest
            .page_count
            .checked_mul(manifest.page_bytes)
            .ok_or_else(|| {
                RelationalIndexShadowError::Corrupt(
                    "relational index shadow artifact size overflow".to_string(),
                )
            })?;
        if actual_bytes != expected_bytes {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "relational index shadow artifact length mismatch: expected {expected_bytes}, got {actual_bytes}"
            )));
        }
        Ok(Self {
            directory: directory.to_path_buf(),
            manifest,
            config,
            page_cache,
            store_id,
            artifact: OnceLock::new(),
            poisoned: AtomicBool::new(false),
        })
    }

    pub fn manifest(&self) -> &RelationalIndexShadowManifest {
        &self.manifest
    }

    pub fn validate_required_roots(
        &self,
        state: &RelationalState,
    ) -> Result<(), RelationalIndexShadowError> {
        let expected =
            required_logical_roots(state, self.config.max_roots.get(), ErrorClass::Admission)?;
        let actual = logical_roots(&self.manifest.roots);
        let expected_catalog = catalog_schema_digest(&expected, ErrorClass::Corrupt)?;
        let expected_root_set = root_set_digest(&expected);
        if self.manifest.catalog_schema_digest != expected_catalog
            || self.manifest.root_set_digest != expected_root_set
            || actual != expected
        {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "required relational index roots do not match the pinned schema: expected {} roots with catalog/root-set {}/{}, found {} roots with {}/{}",
                expected.len(),
                expected_catalog,
                expected_root_set,
                actual.len(),
                self.manifest.catalog_schema_digest,
                self.manifest.root_set_digest,
            )));
        }
        Ok(())
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    pub(super) fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    pub fn read_page(
        &self,
        page_id: IndexPageId,
    ) -> Result<ImmutableIndexPage, RelationalIndexShadowError> {
        self.read_page_accounted(page_id, usize::MAX)
            .map(|read| read.page)
    }

    pub(super) fn read_page_accounted(
        &self,
        page_id: IndexPageId,
        max_file_bytes: usize,
    ) -> Result<RelationalIndexPageRead, RelationalIndexShadowError> {
        if page_id.get() > self.manifest.page_count {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "page {} exceeds published page count {}",
                page_id.get(),
                self.manifest.page_count
            )));
        }
        if self.is_poisoned() {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index shadow reader is poisoned by an earlier page failure".to_string(),
            ));
        }
        let result = self.read_page_inner(page_id, max_file_bytes);
        if result
            .as_ref()
            .is_err_and(|error| !matches!(error, RelationalIndexShadowError::Admission(_)))
        {
            self.poison();
        }
        result
    }

    fn read_page_inner(
        &self,
        page_id: IndexPageId,
        max_file_bytes: usize,
    ) -> Result<RelationalIndexPageRead, RelationalIndexShadowError> {
        let page_bytes = self.config.page_limits.max_page_bytes.get();
        let cache_identity = SegmentCacheIdentity {
            store_id: self.store_id,
            manifest_generation: ManifestGeneration(self.manifest.generation),
            segment_id: page_id.get(),
            representation: RepresentationKind::RelationalIndexPageSlot,
        };
        if let Some(cache) = &self.page_cache
            && let Some(slot) = cache.get_by_identity(&cache_identity)
        {
            let page = self.validate_selected_page(
                ImmutableIndexPage::decode_cached_slot(&slot, self.config.page_limits)?,
                page_id,
            )?;
            return Ok(RelationalIndexPageRead {
                page,
                cache_hit: true,
                cache_miss: false,
                cache_admission_rejected: false,
            });
        }
        if page_bytes > max_file_bytes {
            return Err(RelationalIndexShadowError::Admission(format!(
                "index lookup needs {page_bytes} file bytes, exceeding remaining file byte budget {max_file_bytes}"
            )));
        }
        let offset = page_id
            .get()
            .checked_sub(1)
            .and_then(|ordinal| ordinal.checked_mul(page_bytes as u64))
            .ok_or_else(|| {
                RelationalIndexShadowError::Corrupt("index page offset overflow".to_string())
            })?;
        let mut slot = vec![0; page_bytes];
        read_exact_at(self.artifact()?, &mut slot, offset)
            .map_err(durability("read shadow page"))?;

        let page = self.validate_selected_page(
            ImmutableIndexPage::decode_slot(&slot, self.config.page_limits)?,
            page_id,
        )?;
        let mut cache_admission_rejected = false;
        if let Some(cache) = &self.page_cache {
            let key = SegmentCacheKey {
                store_id: cache_identity.store_id,
                manifest_generation: cache_identity.manifest_generation,
                segment_id: cache_identity.segment_id,
                content_digest: content_digest(&slot),
                representation: cache_identity.representation,
            };
            match cache.insert_page_verified(key, slot) {
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.error(),
                        SegmentCacheError::EntryTooLarge { .. }
                            | SegmentCacheError::PinnedCapacity { .. }
                    ) =>
                {
                    cache_admission_rejected = true;
                }
                Err(error) => {
                    return Err(RelationalIndexShadowError::Corrupt(format!(
                        "relational index page cache rejected immutable page identity: {error}"
                    )));
                }
            }
        }
        Ok(RelationalIndexPageRead {
            page,
            cache_hit: false,
            cache_miss: self.page_cache.is_some(),
            cache_admission_rejected,
        })
    }

    fn validate_selected_page(
        &self,
        page: ImmutableIndexPage,
        page_id: IndexPageId,
    ) -> Result<ImmutableIndexPage, RelationalIndexShadowError> {
        if page.generation != self.manifest.generation
            || page.source_commit_epoch != self.manifest.source_commit_epoch
            || page.page_id != page_id
        {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "page {} does not match the selected generation/epoch/id",
                page_id.get()
            )));
        }
        Ok(page)
    }

    fn artifact(&self) -> Result<&File, RelationalIndexShadowError> {
        if let Some(file) = self.artifact.get() {
            return Ok(file);
        }
        let path = self.directory.join(relational_index_shadow_artifact_file(
            self.manifest.generation,
        ));
        let opened = File::open(path).map_err(durability("open shadow artifact"))?;
        let _ = self.artifact.set(opened);
        Ok(self
            .artifact
            .get()
            .expect("the current or a concurrent reader opened the shadow artifact"))
    }

    pub fn read_root(
        &self,
        descriptor: &RelationalIndexRootDescriptor,
    ) -> Result<IndexRootPage, RelationalIndexShadowError> {
        if self
            .manifest
            .root(&descriptor.identity.namespace, &descriptor.identity.name)
            != Some(descriptor)
        {
            return Err(RelationalIndexShadowError::Corrupt(
                "root descriptor is not selected by this shadow manifest".to_string(),
            ));
        }
        let page = self.read_page(descriptor.root_page_id)?;
        let ImmutableIndexPageBody::Root(root) = page.body else {
            self.poison();
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "root descriptor {}.{} references a non-root page",
                descriptor.identity.namespace, descriptor.identity.name
            )));
        };
        if root.identity != descriptor.identity
            || root.schema_digest != descriptor.schema_digest
            || root.height != descriptor.height
        {
            self.poison();
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "root page {} disagrees with its manifest descriptor",
                descriptor.root_page_id.get()
            )));
        }
        Ok(root)
    }
}

struct SlotWriter {
    file: File,
    generation: u64,
    source_commit_epoch: u64,
    limits: ImmutableIndexPageLimits,
    next_page_id: u64,
    written_pages: u64,
    artifact_hasher: IntegrityHasher,
}

impl SlotWriter {
    fn new(
        file: File,
        generation: u64,
        source_commit_epoch: u64,
        limits: ImmutableIndexPageLimits,
    ) -> Self {
        Self {
            file,
            generation,
            source_commit_epoch,
            limits,
            next_page_id: 1,
            written_pages: 0,
            artifact_hasher: IntegrityHasher::new(),
        }
    }

    fn allocate(&mut self) -> Result<IndexPageId, RelationalIndexShadowError> {
        let page_id = page_id(self.next_page_id, "allocated page id")?;
        self.next_page_id = self.next_page_id.checked_add(1).ok_or_else(|| {
            RelationalIndexShadowError::Admission("index page id overflow".to_string())
        })?;
        Ok(page_id)
    }

    fn write(
        &mut self,
        page_id: IndexPageId,
        body: ImmutableIndexPageBody,
    ) -> Result<(), RelationalIndexShadowError> {
        let expected_page_id = self.written_pages.checked_add(1).ok_or_else(|| {
            RelationalIndexShadowError::Admission("written page count overflow".to_string())
        })?;
        if page_id.get() >= self.next_page_id || page_id.get() != expected_page_id {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "index page {} was written out of order; expected {expected_page_id}",
                page_id.get(),
            )));
        }
        let slot = ImmutableIndexPage {
            generation: self.generation,
            source_commit_epoch: self.source_commit_epoch,
            page_id,
            body,
        }
        .encode_slot(self.limits)?;
        let offset = page_id
            .get()
            .checked_sub(1)
            .and_then(|ordinal| ordinal.checked_mul(self.limits.max_page_bytes.get() as u64))
            .ok_or_else(|| {
                RelationalIndexShadowError::Admission("index page offset overflow".to_string())
            })?;
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(durability("seek shadow page slot"))?;
        self.file
            .write_all(&slot)
            .map_err(durability("write shadow page slot"))?;
        self.artifact_hasher.update(&slot);
        self.written_pages = expected_page_id;
        Ok(())
    }

    fn finish(self) -> Result<(u64, RelationalIndexArtifactMetadata), RelationalIndexShadowError> {
        let page_count = self.next_page_id.saturating_sub(1);
        if self.written_pages != page_count {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "relational index writer reserved {page_count} pages but wrote {}",
                self.written_pages
            )));
        }
        self.file
            .sync_all()
            .map_err(durability("sync shadow page artifact"))?;
        let encoded_len = page_count
            .checked_mul(self.limits.max_page_bytes.get() as u64)
            .ok_or_else(|| {
                RelationalIndexShadowError::Admission(
                    "relational index artifact length overflow".to_string(),
                )
            })?;
        Ok((
            page_count,
            RelationalIndexArtifactMetadata::from_digest(
                encoded_len,
                self.artifact_hasher.finish(),
            ),
        ))
    }
}

#[derive(Clone)]
struct ChildPage {
    upper_bound: Vec<u8>,
    page_id: IndexPageId,
}

struct LeadingPrefixStatisticsBuilder {
    statistics: Vec<RelationalIndexPrefixStatistics>,
    active_rows: Vec<u64>,
    previous_prefix_ends: Vec<usize>,
    current_prefix_ends: Vec<usize>,
}

impl LeadingPrefixStatisticsBuilder {
    fn new(prefix_count: usize) -> Self {
        Self {
            statistics: vec![
                RelationalIndexPrefixStatistics {
                    distinct_non_null_values: 0,
                    non_null_rows: 0,
                    fanout: 0,
                };
                prefix_count
            ],
            active_rows: vec![0; prefix_count],
            previous_prefix_ends: Vec::with_capacity(prefix_count),
            current_prefix_ends: Vec::with_capacity(prefix_count),
        }
    }

    fn push(
        &mut self,
        key: &[u8],
        rows: u64,
        previous_key: Option<&[u8]>,
    ) -> Result<(), RelationalIndexShadowError> {
        if rows == 0 {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index contains an empty posting list".to_string(),
            ));
        }
        let leading_non_null =
            ordered_relational_key_prefix_ends(key, &mut self.current_prefix_ends)?;
        if self.current_prefix_ends.len() != self.statistics.len() {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "relational index key contains {} values, expected {}",
                self.current_prefix_ends.len(),
                self.statistics.len()
            )));
        }
        for ordinal in 0..self.statistics.len() {
            if ordinal >= leading_non_null {
                self.finish_group(ordinal);
                continue;
            }
            let statistics = &mut self.statistics[ordinal];
            statistics.non_null_rows =
                statistics.non_null_rows.checked_add(rows).ok_or_else(|| {
                    RelationalIndexShadowError::Admission(
                        "leading-prefix non-null row count overflow".to_string(),
                    )
                })?;
            let same_prefix = self.active_rows[ordinal] != 0
                && previous_key.is_some_and(|previous_key| {
                    let previous_end = self.previous_prefix_ends[ordinal];
                    let current_end = self.current_prefix_ends[ordinal];
                    previous_key[..previous_end] == key[..current_end]
                });
            if same_prefix {
                self.active_rows[ordinal] =
                    self.active_rows[ordinal].checked_add(rows).ok_or_else(|| {
                        RelationalIndexShadowError::Admission(
                            "leading-prefix fanout count overflow".to_string(),
                        )
                    })?;
            } else {
                self.finish_group(ordinal);
                self.statistics[ordinal].distinct_non_null_values = self.statistics[ordinal]
                    .distinct_non_null_values
                    .checked_add(1)
                    .ok_or_else(|| {
                        RelationalIndexShadowError::Admission(
                            "leading-prefix distinct count overflow".to_string(),
                        )
                    })?;
                self.active_rows[ordinal] = rows;
            }
        }
        std::mem::swap(
            &mut self.previous_prefix_ends,
            &mut self.current_prefix_ends,
        );
        Ok(())
    }

    fn finish_group(&mut self, ordinal: usize) {
        self.statistics[ordinal].fanout = self.statistics[ordinal]
            .fanout
            .max(self.active_rows[ordinal]);
        self.active_rows[ordinal] = 0;
    }

    fn finish(mut self) -> RelationalIndexStatistics {
        for ordinal in 0..self.statistics.len() {
            self.finish_group(ordinal);
        }
        RelationalIndexStatistics {
            leading_prefixes: self.statistics,
        }
    }
}

struct TreeWriter<'a> {
    pages: &'a mut SlotWriter,
    identity: IndexIdentity,
    role: RelationalIndexRole,
    schema_digest: Sha256Digest,
    leaf_entries: Vec<IndexLeafEntry>,
    leaf_payload_bytes: usize,
    children: Vec<ChildPage>,
    child_metadata_bytes: usize,
    peak_metadata_bytes: usize,
    max_metadata_bytes: usize,
    last_key: Option<Vec<u8>>,
    statistics: LeadingPrefixStatisticsBuilder,
}

impl<'a> TreeWriter<'a> {
    fn new(
        pages: &'a mut SlotWriter,
        identity: IndexIdentity,
        role: RelationalIndexRole,
        schema_digest: Sha256Digest,
        prefix_count: usize,
        max_metadata_bytes: usize,
    ) -> Self {
        Self {
            pages,
            identity,
            role,
            schema_digest,
            leaf_entries: Vec::new(),
            leaf_payload_bytes: 0,
            children: Vec::new(),
            child_metadata_bytes: 0,
            peak_metadata_bytes: 0,
            max_metadata_bytes,
            last_key: None,
            statistics: LeadingPrefixStatisticsBuilder::new(prefix_count),
        }
    }

    fn push(&mut self, entry: IndexLeafEntry) -> Result<(), RelationalIndexShadowError> {
        if self
            .last_key
            .as_ref()
            .is_some_and(|previous| previous.as_slice() >= entry.key.as_slice())
        {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "index {}.{} does not encode to strictly ordered keys",
                self.identity.namespace, self.identity.name
            )));
        }
        let entry_bytes = leaf_entry_payload_bytes(&entry)?;
        let max_payload = max_page_payload(self.pages.limits)?;
        if entry_bytes > max_payload {
            return Err(RelationalIndexShadowError::Admission(format!(
                "one leaf entry for {}.{} needs {entry_bytes} bytes, exceeding page payload {max_payload}",
                self.identity.namespace, self.identity.name
            )));
        }
        self.statistics.push(
            &entry.key,
            index_leaf_posting_rows(&entry.posting)?,
            self.last_key.as_deref(),
        )?;
        if !self.leaf_entries.is_empty()
            && (self.leaf_entries.len() >= self.pages.limits.max_entries.get()
                || self.leaf_payload_bytes.saturating_add(entry_bytes) > max_payload)
        {
            self.flush_leaf()?;
        }
        self.last_key = Some(entry.key.clone());
        self.leaf_payload_bytes = self.leaf_payload_bytes.saturating_add(entry_bytes);
        self.leaf_entries.push(entry);
        Ok(())
    }

    fn write_encoded_postings<I>(
        &mut self,
        row_ids: I,
        unique: bool,
    ) -> Result<IndexLeafPosting, RelationalIndexShadowError>
    where
        I: IntoIterator<Item = Result<Vec<u8>, RelationalIndexShadowError>>,
    {
        let limits = self.pages.limits;
        let max_inline_postings = limits.max_inline_postings.get();
        let max_inline_bytes = max_page_payload(limits)? / 2;
        let identity = (self.identity.namespace.clone(), self.identity.name.clone());
        let mut inline = Vec::new();
        let mut inline_bytes = 0usize;
        let mut paged = None;
        let mut previous = None;
        let mut total_rows = 0u64;
        for row_id in row_ids {
            let row_id = row_id?;
            if row_id.is_empty() || row_id.len() > limits.max_row_id_bytes.get() {
                return Err(RelationalIndexShadowError::Admission(format!(
                    "encoded relational row id contains {} bytes, exceeding limit {}",
                    row_id.len(),
                    limits.max_row_id_bytes
                )));
            }
            if previous
                .as_ref()
                .is_some_and(|previous: &Vec<u8>| previous.as_slice() >= row_id.as_slice())
            {
                return Err(RelationalIndexShadowError::Corrupt(
                    "relational index posting row ids are not strictly ordered".to_string(),
                ));
            }
            total_rows = total_rows.checked_add(1).ok_or_else(|| {
                RelationalIndexShadowError::Admission(
                    "relational index posting row count overflow".to_string(),
                )
            })?;
            if unique && total_rows > 1 {
                return Err(RelationalIndexShadowError::Corrupt(format!(
                    "unique relational index {}.{} contains duplicate keys",
                    identity.0, identity.1
                )));
            }
            let row_id = IndexRowId::new(row_id);
            let entry_bytes = 4usize.checked_add(row_id.as_bytes().len()).ok_or_else(|| {
                RelationalIndexShadowError::Admission("inline posting size overflow".to_string())
            })?;
            if paged.is_none()
                && inline.len() < max_inline_postings
                && inline_bytes.saturating_add(entry_bytes) <= max_inline_bytes
            {
                previous = Some(row_id.as_bytes().to_vec());
                inline_bytes = inline_bytes.saturating_add(entry_bytes);
                inline.push(row_id);
                continue;
            }
            if paged.is_none() {
                let mut stream = PostingPageStream::new(self.pages)?;
                for buffered in std::mem::take(&mut inline) {
                    stream.push(buffered)?;
                }
                paged = Some(stream);
            }
            previous = Some(row_id.as_bytes().to_vec());
            paged
                .as_mut()
                .expect("posting page stream was initialized")
                .push(row_id)?;
        }
        if total_rows == 0 {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index contains an empty posting list".to_string(),
            ));
        }
        match paged {
            Some(stream) => stream.finish(total_rows),
            None => Ok(IndexLeafPosting::Inline(inline)),
        }
    }

    fn flush_leaf(&mut self) -> Result<(), RelationalIndexShadowError> {
        let page_id = self.pages.allocate()?;
        let upper_bound = self
            .leaf_entries
            .last()
            .map_or_else(Vec::new, |entry| entry.key.clone());
        let entries = std::mem::take(&mut self.leaf_entries);
        self.leaf_payload_bytes = 0;
        self.pages.write(
            page_id,
            ImmutableIndexPageBody::Leaf(IndexLeafPage { entries }),
        )?;
        self.push_child(ChildPage {
            upper_bound,
            page_id,
        })
    }

    fn push_child(&mut self, child: ChildPage) -> Result<(), RelationalIndexShadowError> {
        let bytes = child
            .upper_bound
            .len()
            .checked_add(std::mem::size_of::<ChildPage>())
            .ok_or_else(|| {
                RelationalIndexShadowError::Admission(
                    "index build metadata size overflow".to_string(),
                )
            })?;
        self.child_metadata_bytes =
            self.child_metadata_bytes
                .checked_add(bytes)
                .ok_or_else(|| {
                    RelationalIndexShadowError::Admission(
                        "index build metadata size overflow".to_string(),
                    )
                })?;
        if self.child_metadata_bytes > self.max_metadata_bytes {
            return Err(RelationalIndexShadowError::Admission(format!(
                "index build metadata uses {} bytes, exceeding limit {}",
                self.child_metadata_bytes, self.max_metadata_bytes
            )));
        }
        self.peak_metadata_bytes = self.peak_metadata_bytes.max(self.child_metadata_bytes);
        self.children.push(child);
        Ok(())
    }

    fn finish(
        mut self,
    ) -> Result<(RelationalIndexRootDescriptor, usize), RelationalIndexShadowError> {
        if !self.leaf_entries.is_empty() || self.children.is_empty() {
            self.flush_leaf()?;
        }
        let mut children = std::mem::take(&mut self.children);
        let mut height = 1u32;
        while children.len() > 1 {
            children = write_interior_level(
                self.pages,
                children,
                self.max_metadata_bytes,
                &mut self.peak_metadata_bytes,
            )?;
            height = height.checked_add(1).ok_or_else(|| {
                RelationalIndexShadowError::Admission("index tree height overflow".to_string())
            })?;
        }
        let child = children.pop().expect("tree always has one child page");
        let root_page_id = self.pages.allocate()?;
        self.pages.write(
            root_page_id,
            ImmutableIndexPageBody::Root(IndexRootPage {
                identity: self.identity.clone(),
                schema_digest: self.schema_digest,
                child: child.page_id,
                height,
            }),
        )?;
        let statistics = self.statistics.finish();
        Ok((
            RelationalIndexRootDescriptor {
                identity: self.identity,
                role: self.role,
                schema_digest: self.schema_digest,
                root_page_id,
                height,
                statistics,
            },
            self.peak_metadata_bytes,
        ))
    }
}

fn index_leaf_posting_rows(posting: &IndexLeafPosting) -> Result<u64, RelationalIndexShadowError> {
    match posting {
        IndexLeafPosting::Inline(row_ids) => u64::try_from(row_ids.len()).map_err(|_| {
            RelationalIndexShadowError::Admission(
                "inline posting row count does not fit u64".to_string(),
            )
        }),
        IndexLeafPosting::Page { total_rows, .. } => Ok(*total_rows),
    }
}

struct PostingPageStream<'a> {
    pages: &'a mut SlotWriter,
    first: IndexPageId,
    current: IndexPageId,
    row_ids: Vec<IndexRowId>,
    payload_bytes: usize,
}

impl<'a> PostingPageStream<'a> {
    fn new(pages: &'a mut SlotWriter) -> Result<Self, RelationalIndexShadowError> {
        let first = pages.allocate()?;
        Ok(Self {
            pages,
            first,
            current: first,
            row_ids: Vec::new(),
            payload_bytes: 6 + 8,
        })
    }

    fn push(&mut self, row_id: IndexRowId) -> Result<(), RelationalIndexShadowError> {
        let entry_bytes = 6usize.checked_add(row_id.as_bytes().len()).ok_or_else(|| {
            RelationalIndexShadowError::Admission("posting entry size overflow".to_string())
        })?;
        let max_payload = max_page_payload(self.pages.limits)?;
        if entry_bytes.saturating_add(6 + 8) > max_payload {
            return Err(RelationalIndexShadowError::Admission(
                "one posting row id exceeds the page payload limit".to_string(),
            ));
        }
        if !self.row_ids.is_empty()
            && (self.row_ids.len() >= self.pages.limits.max_entries.get()
                || self.payload_bytes.saturating_add(entry_bytes) > max_payload)
        {
            let next = self.pages.allocate()?;
            self.flush(Some(next))?;
            self.current = next;
        }
        self.payload_bytes = self.payload_bytes.saturating_add(entry_bytes);
        self.row_ids.push(row_id);
        Ok(())
    }

    fn finish(mut self, total_rows: u64) -> Result<IndexLeafPosting, RelationalIndexShadowError> {
        if self.row_ids.is_empty() {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index posting page stream is empty".to_string(),
            ));
        }
        self.flush(None)?;
        Ok(IndexLeafPosting::Page {
            first: self.first,
            total_rows,
        })
    }

    fn flush(&mut self, next: Option<IndexPageId>) -> Result<(), RelationalIndexShadowError> {
        let row_ids = std::mem::take(&mut self.row_ids);
        self.payload_bytes = 6 + 8;
        self.pages.write(
            self.current,
            ImmutableIndexPageBody::Posting(IndexPostingPage { next, row_ids }),
        )
    }
}

fn write_interior_level(
    pages: &mut SlotWriter,
    children: Vec<ChildPage>,
    max_metadata_bytes: usize,
    peak_metadata_bytes: &mut usize,
) -> Result<Vec<ChildPage>, RelationalIndexShadowError> {
    let max_payload = max_page_payload(pages.limits)?;
    let mut next = Vec::new();
    let mut entries = Vec::new();
    let mut payload_bytes = 0usize;
    for child in children {
        let entry_bytes = 6usize
            .checked_add(4)
            .and_then(|bytes| bytes.checked_add(child.upper_bound.len()))
            .and_then(|bytes| bytes.checked_add(8))
            .ok_or_else(|| {
                RelationalIndexShadowError::Admission("interior entry size overflow".to_string())
            })?;
        if entry_bytes > max_payload {
            return Err(RelationalIndexShadowError::Admission(
                "one interior separator exceeds the page payload limit".to_string(),
            ));
        }
        if !entries.is_empty()
            && (entries.len() >= pages.limits.max_entries.get()
                || payload_bytes.saturating_add(entry_bytes) > max_payload)
        {
            flush_interior(pages, &mut entries, &mut next)?;
            payload_bytes = 0;
        }
        payload_bytes = payload_bytes.saturating_add(entry_bytes);
        entries.push(IndexInteriorEntry {
            upper_bound: child.upper_bound,
            child: child.page_id,
        });
    }
    if !entries.is_empty() {
        flush_interior(pages, &mut entries, &mut next)?;
    }
    let metadata_bytes = next.iter().try_fold(0usize, |bytes, child| {
        bytes
            .checked_add(std::mem::size_of::<ChildPage>())
            .and_then(|bytes| bytes.checked_add(child.upper_bound.len()))
            .ok_or_else(|| {
                RelationalIndexShadowError::Admission(
                    "index build metadata size overflow".to_string(),
                )
            })
    })?;
    if metadata_bytes > max_metadata_bytes {
        return Err(RelationalIndexShadowError::Admission(format!(
            "index build metadata uses {metadata_bytes} bytes, exceeding limit {max_metadata_bytes}"
        )));
    }
    *peak_metadata_bytes = (*peak_metadata_bytes).max(metadata_bytes);
    Ok(next)
}

fn flush_interior(
    pages: &mut SlotWriter,
    entries: &mut Vec<IndexInteriorEntry>,
    next: &mut Vec<ChildPage>,
) -> Result<(), RelationalIndexShadowError> {
    let upper_bound = entries
        .last()
        .expect("interior flush requires at least one entry")
        .upper_bound
        .clone();
    let page_id = pages.allocate()?;
    pages.write(
        page_id,
        ImmutableIndexPageBody::Interior(IndexInteriorPage {
            entries: std::mem::take(entries),
        }),
    )?;
    next.push(ChildPage {
        upper_bound,
        page_id,
    });
    Ok(())
}

fn leaf_entry_payload_bytes(entry: &IndexLeafEntry) -> Result<usize, RelationalIndexShadowError> {
    let posting_bytes = match &entry.posting {
        IndexLeafPosting::Inline(row_ids) => {
            row_ids.iter().try_fold(1usize + 4, |bytes, row_id| {
                bytes
                    .checked_add(4)
                    .and_then(|bytes| bytes.checked_add(row_id.as_bytes().len()))
                    .ok_or_else(|| {
                        RelationalIndexShadowError::Admission(
                            "inline posting size overflow".to_string(),
                        )
                    })
            })?
        }
        IndexLeafPosting::Page { .. } => 1 + 8 + 8,
    };
    6usize
        .checked_add(4)
        .and_then(|bytes| bytes.checked_add(entry.key.len()))
        .and_then(|bytes| bytes.checked_add(posting_bytes))
        .ok_or_else(|| {
            RelationalIndexShadowError::Admission("leaf entry size overflow".to_string())
        })
}

fn max_page_payload(limits: ImmutableIndexPageLimits) -> Result<usize, RelationalIndexShadowError> {
    limits.max_payload_bytes().ok_or_else(|| {
        RelationalIndexShadowError::Admission(
            "immutable index page limit is smaller than its header".to_string(),
        )
    })
}

fn encode_relational_key(key: &RelationalKey) -> Result<Vec<u8>, RelationalIndexShadowError> {
    encode_ordered_relational_key(key).map_err(Into::into)
}

pub(super) fn relational_schema_digest(
    schema: &RelationalTableSchema,
) -> Result<Sha256Digest, RelationalIndexShadowError> {
    let mut encoded = Vec::new();
    encode_bytes(&mut encoded, schema.name.as_bytes())?;
    encode_count(&mut encoded, schema.columns.len(), "columns")?;
    for column in &schema.columns {
        encode_bytes(&mut encoded, column.name.as_bytes())?;
        encoded.push(scalar_type_tag(column.scalar_type));
        encoded.push(u8::from(column.nullable));
        match &column.default {
            None => encoded.push(0),
            Some(RelationalColumnDefault::Literal(value)) => {
                encoded.push(1);
                encode_ordered_relational_value(&mut encoded, value)?;
            }
            Some(RelationalColumnDefault::UuidV7) => encoded.push(2),
        }
    }
    encode_string_list(&mut encoded, &schema.primary_key)?;
    encode_count(
        &mut encoded,
        schema.unique_constraints.len(),
        "unique constraints",
    )?;
    for columns in &schema.unique_constraints {
        encode_string_list(&mut encoded, columns)?;
    }
    encode_count(&mut encoded, schema.foreign_keys.len(), "foreign keys")?;
    for foreign_key in &schema.foreign_keys {
        encode_foreign_key(&mut encoded, foreign_key)?;
    }
    encode_count(&mut encoded, schema.indexes.len(), "indexes")?;
    for index in &schema.indexes {
        encode_bytes(&mut encoded, index.name.as_bytes())?;
        encode_string_list(&mut encoded, &index.columns)?;
        encoded.push(u8::from(index.unique));
    }
    Ok(integrity_digest(&encoded).sha256)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RelationalIndexLogicalRoot {
    identity: IndexIdentity,
    role: RelationalIndexRole,
    schema_digest: Sha256Digest,
    column_count: usize,
}

fn logical_roots(roots: &[RelationalIndexRootDescriptor]) -> Vec<RelationalIndexLogicalRoot> {
    roots
        .iter()
        .map(|root| RelationalIndexLogicalRoot {
            identity: root.identity.clone(),
            role: root.role,
            schema_digest: root.schema_digest,
            column_count: root.statistics.leading_prefixes.len(),
        })
        .collect()
}

fn required_logical_roots(
    state: &RelationalState,
    max_roots: usize,
    error_class: ErrorClass,
) -> Result<Vec<RelationalIndexLogicalRoot>, RelationalIndexShadowError> {
    let expected_count = required_root_count(state, max_roots, error_class)?;
    let mut roots = Vec::with_capacity(expected_count);
    for (table, schema) in &state.schemas {
        let schema_digest = relational_schema_digest(schema)?;
        roots.extend(
            schema
                .required_index_definitions()
                .into_iter()
                .map(|definition| RelationalIndexLogicalRoot {
                    identity: IndexIdentity {
                        namespace: table.clone(),
                        name: definition.name,
                    },
                    role: definition.role,
                    schema_digest,
                    column_count: definition.columns.len(),
                }),
        );
    }
    roots.sort_by(|left, right| {
        (&left.identity.namespace, &left.identity.name)
            .cmp(&(&right.identity.namespace, &right.identity.name))
    });
    Ok(roots)
}

fn required_root_count(
    state: &RelationalState,
    max_roots: usize,
    error_class: ErrorClass,
) -> Result<usize, RelationalIndexShadowError> {
    let mut total = 0usize;
    for schema in state.schemas.values() {
        let table_roots = 1usize
            .checked_add(schema.unique_constraints.len())
            .and_then(|count| count.checked_add(schema.indexes.len()))
            .and_then(|count| count.checked_add(schema.foreign_keys.len()))
            .ok_or_else(|| invalid(error_class, "required index root count overflow"))?;
        total = total
            .checked_add(table_roots)
            .ok_or_else(|| invalid(error_class, "required index root count overflow"))?;
        if total > max_roots {
            return Err(invalid(
                error_class,
                format!("required index root count {total} exceeds limit {max_roots}"),
            ));
        }
    }
    Ok(total)
}

fn catalog_schema_digest(
    roots: &[RelationalIndexLogicalRoot],
    error_class: ErrorClass,
) -> Result<Sha256Digest, RelationalIndexShadowError> {
    let table_count = roots
        .iter()
        .enumerate()
        .filter(|(position, root)| {
            *position == 0 || roots[*position - 1].identity.namespace != root.identity.namespace
        })
        .count();
    let mut hasher = IntegrityHasher::new();
    hasher.update(b"skein-relational-index-catalog-schema-v1\0");
    hasher.update(&(table_count as u64).to_le_bytes());
    let mut previous: Option<(&str, Sha256Digest)> = None;
    for root in roots {
        match previous {
            Some((table, digest)) if table == root.identity.namespace.as_str() => {
                if digest != root.schema_digest {
                    return Err(invalid(
                        error_class,
                        format!(
                            "table {} has inconsistent schema digests across index roots",
                            root.identity.namespace
                        ),
                    ));
                }
            }
            _ => {
                hash_manifest_bytes(&mut hasher, root.identity.namespace.as_bytes());
                hasher.update(root.schema_digest.as_bytes());
            }
        }
        previous = Some((root.identity.namespace.as_str(), root.schema_digest));
    }
    Ok(hasher.finish().sha256)
}

fn root_set_digest(roots: &[RelationalIndexLogicalRoot]) -> Sha256Digest {
    let mut hasher = IntegrityHasher::new();
    hasher.update(b"skein-relational-index-required-roots-v2\0");
    hasher.update(&(roots.len() as u64).to_le_bytes());
    for root in roots {
        hash_manifest_bytes(&mut hasher, root.identity.namespace.as_bytes());
        hash_manifest_bytes(&mut hasher, root.identity.name.as_bytes());
        hasher.update(&[relational_index_role_tag(root.role)]);
        hasher.update(root.schema_digest.as_bytes());
        hasher.update(&(root.column_count as u64).to_le_bytes());
    }
    hasher.finish().sha256
}

fn hash_manifest_bytes(hasher: &mut IntegrityHasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn relational_index_role_tag(role: RelationalIndexRole) -> u8 {
    match role {
        RelationalIndexRole::Primary => 1,
        RelationalIndexRole::UniqueConstraint => 2,
        RelationalIndexRole::DeclaredUnique => 3,
        RelationalIndexRole::Secondary => 4,
        RelationalIndexRole::ForeignKeySupport => 5,
    }
}

fn decode_relational_index_role(
    tag: u8,
) -> Result<RelationalIndexRole, RelationalIndexShadowError> {
    match tag {
        1 => Ok(RelationalIndexRole::Primary),
        2 => Ok(RelationalIndexRole::UniqueConstraint),
        3 => Ok(RelationalIndexRole::DeclaredUnique),
        4 => Ok(RelationalIndexRole::Secondary),
        5 => Ok(RelationalIndexRole::ForeignKeySupport),
        _ => Err(RelationalIndexShadowError::Corrupt(format!(
            "unknown relational index role tag {tag}"
        ))),
    }
}

fn encode_foreign_key(
    encoded: &mut Vec<u8>,
    foreign_key: &RelationalForeignKeySchema,
) -> Result<(), RelationalIndexShadowError> {
    encode_string_list(encoded, &foreign_key.columns)?;
    encode_bytes(encoded, foreign_key.referenced_table.as_bytes())?;
    encode_string_list(encoded, &foreign_key.referenced_columns)?;
    encoded.push(referential_action_tag(foreign_key.on_delete));
    encoded.push(referential_action_tag(foreign_key.on_update));
    Ok(())
}

fn scalar_type_tag(scalar_type: RelationalScalarType) -> u8 {
    match scalar_type {
        RelationalScalarType::Boolean => 1,
        RelationalScalarType::BigInt => 2,
        RelationalScalarType::DoublePrecision => 3,
        RelationalScalarType::Text => 4,
        RelationalScalarType::Bytea => 5,
        RelationalScalarType::Uuid => 6,
    }
}

fn referential_action_tag(action: RelationalReferentialAction) -> u8 {
    match action {
        RelationalReferentialAction::NoAction => 0,
        RelationalReferentialAction::Restrict => 1,
        RelationalReferentialAction::Cascade => 2,
    }
}

fn encode_string_list(
    encoded: &mut Vec<u8>,
    values: &[String],
) -> Result<(), RelationalIndexShadowError> {
    encode_count(encoded, values.len(), "string list")?;
    for value in values {
        encode_bytes(encoded, value.as_bytes())?;
    }
    Ok(())
}

fn encode_count(
    encoded: &mut Vec<u8>,
    count: usize,
    context: &str,
) -> Result<(), RelationalIndexShadowError> {
    let count = u32::try_from(count).map_err(|_| {
        RelationalIndexShadowError::Admission(format!("{context} count does not fit u32"))
    })?;
    encoded.extend_from_slice(&count.to_le_bytes());
    Ok(())
}

fn validate_manifest(
    manifest: &RelationalIndexShadowManifest,
    config: RelationalIndexShadowConfig,
    error_class: ErrorClass,
) -> Result<(), RelationalIndexShadowError> {
    if manifest.generation == 0 {
        return Err(invalid(error_class, "manifest generation must be non-zero"));
    }
    if manifest.page_bytes != config.page_limits.max_page_bytes.get() as u64 {
        return Err(invalid(
            error_class,
            format!(
                "manifest page size {} does not match configured page size {}",
                manifest.page_bytes, config.page_limits.max_page_bytes
            ),
        ));
    }
    if manifest.roots.len() > config.max_roots.get() {
        return Err(invalid(
            error_class,
            "manifest root count exceeds its configured limit",
        ));
    }
    if manifest.roots.is_empty() != (manifest.page_count == 0) {
        return Err(invalid(
            error_class,
            "manifest root and page emptiness must agree",
        ));
    }
    let mut previous: Option<(&str, &str)> = None;
    for root in &manifest.roots {
        let identity = (
            root.identity.namespace.as_str(),
            root.identity.name.as_str(),
        );
        let identity_bytes = root
            .identity
            .namespace
            .len()
            .checked_add(root.identity.name.len())
            .ok_or_else(|| invalid(error_class, "root identity size overflow"))?;
        if root.identity.namespace.is_empty()
            || root.identity.name.is_empty()
            || identity_bytes > config.page_limits.max_identity_bytes.get()
            || !role_matches_identity(root.role, &root.identity.name)
            || root.height == 0
            || root.root_page_id.get() > manifest.page_count
        {
            return Err(invalid(
                error_class,
                "manifest contains an invalid root descriptor",
            ));
        }
        validate_index_statistics(&root.statistics, error_class)?;
        if previous.is_some_and(|previous| previous >= identity) {
            return Err(invalid(
                error_class,
                "manifest roots must be strictly ordered",
            ));
        }
        previous = Some(identity);
    }
    let logical_roots = logical_roots(&manifest.roots);
    if manifest.catalog_schema_digest != catalog_schema_digest(&logical_roots, error_class)?
        || manifest.root_set_digest != root_set_digest(&logical_roots)
    {
        return Err(invalid(
            error_class,
            "manifest catalog schema or root-set digest mismatch",
        ));
    }
    Ok(())
}

fn validate_index_statistics(
    statistics: &RelationalIndexStatistics,
    error_class: ErrorClass,
) -> Result<(), RelationalIndexShadowError> {
    if statistics.leading_prefixes.is_empty() {
        return Err(invalid(
            error_class,
            "index statistics must describe at least one leading prefix",
        ));
    }
    for prefix in &statistics.leading_prefixes {
        if prefix.non_null_rows == 0 {
            if prefix.distinct_non_null_values != 0 || prefix.fanout != 0 {
                return Err(invalid(
                    error_class,
                    "empty leading-prefix statistics must contain only zero counts",
                ));
            }
            continue;
        }
        if prefix.distinct_non_null_values == 0
            || prefix.distinct_non_null_values > prefix.non_null_rows
            || prefix.fanout == 0
            || prefix.fanout > prefix.non_null_rows
        {
            return Err(invalid(
                error_class,
                "leading-prefix statistics contain inconsistent counts",
            ));
        }
        let minimum_fanout = prefix.non_null_rows / prefix.distinct_non_null_values
            + u64::from(prefix.non_null_rows % prefix.distinct_non_null_values != 0);
        if prefix.fanout < minimum_fanout {
            return Err(invalid(
                error_class,
                "leading-prefix fanout is smaller than its exact average",
            ));
        }
    }
    Ok(())
}

fn role_matches_identity(role: RelationalIndexRole, name: &str) -> bool {
    match role {
        RelationalIndexRole::Primary => name == RELATIONAL_PRIMARY_INDEX_NAME,
        RelationalIndexRole::UniqueConstraint => {
            synthetic_index_name_has_ordinal(name, RELATIONAL_UNIQUE_INDEX_PREFIX)
        }
        RelationalIndexRole::ForeignKeySupport => {
            synthetic_index_name_has_ordinal(name, RELATIONAL_FOREIGN_KEY_INDEX_PREFIX)
        }
        RelationalIndexRole::DeclaredUnique | RelationalIndexRole::Secondary => {
            name != RELATIONAL_PRIMARY_INDEX_NAME
                && !name.starts_with(RELATIONAL_UNIQUE_INDEX_PREFIX)
                && !name.starts_with(RELATIONAL_FOREIGN_KEY_INDEX_PREFIX)
        }
    }
}

fn synthetic_index_name_has_ordinal(name: &str, prefix: &str) -> bool {
    name.strip_prefix(prefix).is_some_and(|ordinal| {
        !ordinal.is_empty() && ordinal.bytes().all(|byte| byte.is_ascii_digit())
    })
}

#[derive(Clone, Copy)]
enum ErrorClass {
    Admission,
    Corrupt,
}

fn invalid(error_class: ErrorClass, message: impl Into<String>) -> RelationalIndexShadowError {
    match error_class {
        ErrorClass::Admission => RelationalIndexShadowError::Admission(message.into()),
        ErrorClass::Corrupt => RelationalIndexShadowError::Corrupt(message.into()),
    }
}

fn current_generation(
    manifest_path: &Path,
    config: RelationalIndexShadowConfig,
) -> Result<Option<u64>, RelationalIndexShadowError> {
    match fs::metadata(manifest_path) {
        Ok(_) => read_bounded_file(
            manifest_path,
            config.max_manifest_bytes.get(),
            "current relational index shadow manifest",
        )
        .and_then(|bytes| RelationalIndexShadowManifest::decode(&bytes, config))
        .map(|manifest| Some(manifest.generation)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(RelationalIndexShadowError::Durability(format!(
            "failed to inspect current relational index shadow manifest: {error}"
        ))),
    }
}

fn read_bounded_file(
    path: &Path,
    max_bytes: usize,
    context: &str,
) -> Result<Vec<u8>, RelationalIndexShadowError> {
    let max_bytes_u64 = u64::try_from(max_bytes).map_err(|_| {
        RelationalIndexShadowError::Admission(format!("{context} limit overflows u64"))
    })?;
    let read_limit = max_bytes_u64.checked_add(1).ok_or_else(|| {
        RelationalIndexShadowError::Admission(format!("{context} read limit overflows u64"))
    })?;
    let file = File::open(path).map_err(durability("open bounded file"))?;
    let encoded_len = file
        .metadata()
        .map_err(durability("inspect bounded file"))?
        .len();
    if encoded_len > max_bytes_u64 {
        return Err(RelationalIndexShadowError::Admission(format!(
            "{context} contains {encoded_len} bytes, exceeding limit {max_bytes}"
        )));
    }
    let capacity = usize::try_from(encoded_len).map_err(|_| {
        RelationalIndexShadowError::Admission(format!("{context} length overflows usize"))
    })?;
    let mut encoded = Vec::with_capacity(capacity);
    file.take(read_limit)
        .read_to_end(&mut encoded)
        .map_err(durability("read bounded file"))?;
    if encoded.len() > max_bytes {
        return Err(RelationalIndexShadowError::Admission(format!(
            "{context} exceeds limit {max_bytes}"
        )));
    }
    Ok(encoded)
}

fn encode_bytes(encoded: &mut Vec<u8>, bytes: &[u8]) -> Result<(), RelationalIndexShadowError> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        RelationalIndexShadowError::Admission("byte string length does not fit u32".to_string())
    })?;
    encoded.extend_from_slice(&len.to_le_bytes());
    encoded.extend_from_slice(bytes);
    Ok(())
}

fn decode_bytes<'a>(
    encoded: &'a [u8],
    offset: usize,
    max_bytes: usize,
    context: &str,
) -> Result<(&'a [u8], usize), RelationalIndexShadowError> {
    let len_bytes = encoded.get(offset..offset + 4).ok_or_else(|| {
        RelationalIndexShadowError::Corrupt(format!("truncated {context} length"))
    })?;
    let len = read_u32(len_bytes) as usize;
    if len > max_bytes {
        return Err(RelationalIndexShadowError::Admission(format!(
            "{context} contains {len} bytes, exceeding limit {max_bytes}"
        )));
    }
    let start = offset + 4;
    let end = start
        .checked_add(len)
        .ok_or_else(|| RelationalIndexShadowError::Corrupt(format!("{context} length overflow")))?;
    let bytes = encoded
        .get(start..end)
        .ok_or_else(|| RelationalIndexShadowError::Corrupt(format!("truncated {context}")))?;
    Ok((bytes, end))
}

fn take<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    len: usize,
    context: &str,
) -> Result<&'a [u8], RelationalIndexShadowError> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| RelationalIndexShadowError::Corrupt(format!("{context} offset overflow")))?;
    let bytes = encoded
        .get(*offset..end)
        .ok_or_else(|| RelationalIndexShadowError::Corrupt(format!("truncated {context}")))?;
    *offset = end;
    Ok(bytes)
}

fn decode_utf8(bytes: &[u8], context: &str) -> Result<String, RelationalIndexShadowError> {
    std::str::from_utf8(bytes)
        .map(str::to_string)
        .map_err(|_| RelationalIndexShadowError::Corrupt(format!("{context} is not UTF-8")))
}

fn page_id(value: u64, context: &str) -> Result<IndexPageId, RelationalIndexShadowError> {
    NonZeroU64::new(value)
        .map(IndexPageId::new)
        .ok_or_else(|| RelationalIndexShadowError::Corrupt(format!("{context} must be non-zero")))
}

fn durability(context: &'static str) -> impl FnOnce(std::io::Error) -> RelationalIndexShadowError {
    move |error| RelationalIndexShadowError::Durability(format!("{context}: {error}"))
}

fn read_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().expect("u16 field has a fixed length"))
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("u32 field has a fixed length"))
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("u64 field has a fixed length"))
}

impl From<RelationalError> for RelationalIndexShadowError {
    fn from(error: RelationalError) -> Self {
        Self::Corrupt(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_prefix_statistics() -> RelationalIndexStatistics {
        RelationalIndexStatistics {
            leading_prefixes: vec![RelationalIndexPrefixStatistics {
                distinct_non_null_values: 1,
                non_null_rows: 1,
                fanout: 1,
            }],
        }
    }

    #[test]
    fn manifest_checksum_covers_generation_fence() {
        let config = RelationalIndexShadowConfig::default();
        let manifest = RelationalIndexShadowManifest::from_roots(
            7,
            11,
            config.page_limits.max_page_bytes.get() as u64,
            1,
            vec![RelationalIndexRootDescriptor {
                identity: IndexIdentity {
                    namespace: "documents".to_string(),
                    name: RELATIONAL_PRIMARY_INDEX_NAME.to_string(),
                },
                role: RelationalIndexRole::Primary,
                schema_digest: integrity_digest(b"documents schema").sha256,
                root_page_id: page_id(1, "test root").unwrap(),
                height: 1,
                statistics: one_prefix_statistics(),
            }],
        )
        .unwrap();
        let mut encoded = manifest.encode(config).unwrap();
        encoded[12] ^= 1;

        assert!(matches!(
            RelationalIndexShadowManifest::decode(&encoded, config),
            Err(RelationalIndexShadowError::Corrupt(message))
                if message.contains("checksum mismatch")
        ));
    }

    #[test]
    fn manifest_round_trip_binds_root_roles_and_logical_digests() {
        let config = RelationalIndexShadowConfig::default();
        let schema_digest = integrity_digest(b"documents schema").sha256;
        let manifest = RelationalIndexShadowManifest::from_roots(
            9,
            13,
            config.page_limits.max_page_bytes.get() as u64,
            2,
            vec![
                RelationalIndexRootDescriptor {
                    identity: IndexIdentity {
                        namespace: "documents".to_string(),
                        name: RELATIONAL_PRIMARY_INDEX_NAME.to_string(),
                    },
                    role: RelationalIndexRole::Primary,
                    schema_digest,
                    root_page_id: page_id(1, "test primary root").unwrap(),
                    height: 1,
                    statistics: one_prefix_statistics(),
                },
                RelationalIndexRootDescriptor {
                    identity: IndexIdentity {
                        namespace: "documents".to_string(),
                        name: "documents_owner_idx".to_string(),
                    },
                    role: RelationalIndexRole::Secondary,
                    schema_digest,
                    root_page_id: page_id(2, "test secondary root").unwrap(),
                    height: 1,
                    statistics: RelationalIndexStatistics {
                        leading_prefixes: vec![
                            RelationalIndexPrefixStatistics {
                                distinct_non_null_values: 2,
                                non_null_rows: 5,
                                fanout: 3,
                            },
                            RelationalIndexPrefixStatistics {
                                distinct_non_null_values: 4,
                                non_null_rows: 4,
                                fanout: 1,
                            },
                        ],
                    },
                },
            ],
        )
        .unwrap();

        let encoded = manifest.encode(config).unwrap();
        let decoded = RelationalIndexShadowManifest::decode(&encoded, config).unwrap();

        assert_eq!(decoded, manifest);
        assert_ne!(decoded.catalog_schema_digest, decoded.root_set_digest);
        assert_eq!(decoded.roots[0].role, RelationalIndexRole::Primary);
        assert_eq!(decoded.roots[1].role, RelationalIndexRole::Secondary);
    }

    #[test]
    fn manifest_rejects_role_and_root_set_identity_drift() {
        let config = RelationalIndexShadowConfig::default();
        let mut wrong_role = RelationalIndexShadowManifest::from_roots(
            1,
            1,
            config.page_limits.max_page_bytes.get() as u64,
            1,
            vec![RelationalIndexRootDescriptor {
                identity: IndexIdentity {
                    namespace: "documents".to_string(),
                    name: RELATIONAL_PRIMARY_INDEX_NAME.to_string(),
                },
                role: RelationalIndexRole::Primary,
                schema_digest: integrity_digest(b"documents schema").sha256,
                root_page_id: page_id(1, "test root").unwrap(),
                height: 1,
                statistics: one_prefix_statistics(),
            }],
        )
        .unwrap();
        wrong_role.roots[0].role = RelationalIndexRole::Secondary;
        assert!(matches!(
            wrong_role.encode(config),
            Err(RelationalIndexShadowError::Admission(message))
                if message.contains("invalid root descriptor")
        ));

        let mut wrong_digest = RelationalIndexShadowManifest::from_roots(
            1,
            1,
            config.page_limits.max_page_bytes.get() as u64,
            1,
            vec![RelationalIndexRootDescriptor {
                identity: IndexIdentity {
                    namespace: "documents".to_string(),
                    name: RELATIONAL_PRIMARY_INDEX_NAME.to_string(),
                },
                role: RelationalIndexRole::Primary,
                schema_digest: integrity_digest(b"documents schema").sha256,
                root_page_id: page_id(1, "test root").unwrap(),
                height: 1,
                statistics: one_prefix_statistics(),
            }],
        )
        .unwrap();
        wrong_digest.root_set_digest = integrity_digest(b"wrong root set").sha256;
        assert!(matches!(
            wrong_digest.encode(config),
            Err(RelationalIndexShadowError::Admission(message))
                if message.contains("root-set digest mismatch")
        ));
        assert!(decode_relational_index_role(0).is_err());
    }

    #[test]
    fn manifest_rejects_inconsistent_leading_prefix_statistics() {
        let config = RelationalIndexShadowConfig::default();
        let mut manifest = RelationalIndexShadowManifest::from_roots(
            1,
            1,
            config.page_limits.max_page_bytes.get() as u64,
            1,
            vec![RelationalIndexRootDescriptor {
                identity: IndexIdentity {
                    namespace: "documents".to_string(),
                    name: RELATIONAL_PRIMARY_INDEX_NAME.to_string(),
                },
                role: RelationalIndexRole::Primary,
                schema_digest: integrity_digest(b"documents schema").sha256,
                root_page_id: page_id(1, "test root").unwrap(),
                height: 1,
                statistics: one_prefix_statistics(),
            }],
        )
        .unwrap();
        manifest.roots[0].statistics.leading_prefixes[0].fanout = 0;

        assert!(matches!(
            manifest.encode(config),
            Err(RelationalIndexShadowError::Admission(message))
                if message.contains("inconsistent counts")
        ));
    }
}

#[cfg(test)]
mod lock_tests {
    use super::*;

    #[test]
    fn publication_lock_contract() {
        crate::file_lock_tests::assert_contract(
            RELATIONAL_INDEX_SHADOW_LOCK_FILE,
            acquire_publication_lock,
            "create relational index directory",
        );
    }

    #[test]
    #[ignore = "deterministic local publication lock campaign"]
    fn publication_lock_state_machine_campaign() {
        crate::file_lock_tests::assert_state_machine(
            RELATIONAL_INDEX_SHADOW_LOCK_FILE,
            acquire_publication_lock,
        );
    }
}
