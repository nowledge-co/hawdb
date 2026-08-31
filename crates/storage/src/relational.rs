use crate::{
    FileSegmentRangeReader, SegmentRangeReader, SegmentReadRange, SnapshotCommitError,
    SnapshotCoordinator, SnapshotReadGuard,
};
use skein_core::LogicalType;
use skein_integrity::Sha256Digest;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::Arc;

mod codec;
mod constraints;
mod index_shadow;
mod ordered_key;
pub(crate) mod overflow;
mod recovery;
mod row_page;

pub use codec::{
    decode_relational_checkpoint, decode_relational_checkpoint_file,
    decode_relational_checkpoint_file_with_index_load,
    decode_relational_checkpoint_with_index_load, decode_relational_wal_batch,
    encode_relational_checkpoint, encode_relational_checkpoint_to_writer,
    encode_relational_wal_batch, encode_relational_wal_batch_with_captures,
    encode_relational_wal_batch_with_replay_access, EncodedRelationalWalBatch,
    RelationalCheckpoint, RelationalCheckpointIndexLoad, RelationalDecodeLimits,
    RelationalWalBatch,
};
pub(crate) use codec::{
    decode_relational_row_payload, decode_relational_table_schema, encode_relational_row_payload,
    encode_relational_table_schema,
};
pub use constraints::RelationalConstraintIndex;
pub use index_shadow::{
    relational_index_recovery_delta_file, relational_index_shadow_artifact_file,
    relational_index_shadow_manifest_generation_file, RelationalIndexArtifactMetadata,
    RelationalIndexGenerationArtifacts, RelationalIndexGenerationIdentity,
    RelationalIndexPrefixStatistics, RelationalIndexReadLimits, RelationalIndexReadReport,
    RelationalIndexRecoveryBuilder, RelationalIndexRecoveryConfig, RelationalIndexRecoveryManifest,
    RelationalIndexRecoveryReadReport, RelationalIndexRecoveryReader,
    RelationalIndexRecoveryReport, RelationalIndexRootDescriptor, RelationalIndexRowSource,
    RelationalIndexShadowBuildReport, RelationalIndexShadowConfig, RelationalIndexShadowError,
    RelationalIndexShadowManifest, RelationalIndexShadowReader, RelationalIndexShadowWriter,
    RelationalIndexStatistics, DEFAULT_RELATIONAL_INDEX_READ_BYTES,
    DEFAULT_RELATIONAL_INDEX_READ_PAGES, DEFAULT_RELATIONAL_INDEX_READ_ROWS,
    DEFAULT_RELATIONAL_INDEX_READ_TREE_HEIGHT, DEFAULT_RELATIONAL_INDEX_RECOVERY_DIRTY_BYTES,
    DEFAULT_RELATIONAL_INDEX_RECOVERY_DIRTY_ENTRIES,
    DEFAULT_RELATIONAL_INDEX_RECOVERY_MANIFEST_BYTES, DEFAULT_RELATIONAL_INDEX_RECOVERY_PAGES,
    DEFAULT_RELATIONAL_INDEX_SHADOW_BUILD_METADATA_BYTES,
    DEFAULT_RELATIONAL_INDEX_SHADOW_MANIFEST_BYTES, DEFAULT_RELATIONAL_INDEX_SHADOW_ROOTS,
    DEFAULT_RELATIONAL_INDEX_SORT_MEMORY_BYTES, DEFAULT_RELATIONAL_INDEX_SORT_MERGE_FAN_IN,
    DEFAULT_RELATIONAL_INDEX_SORT_RUNS, DEFAULT_RELATIONAL_INDEX_SORT_SPILL_BYTES,
    RELATIONAL_INDEX_RECOVERY_MANIFEST_FILE, RELATIONAL_INDEX_SHADOW_MANIFEST_FILE,
};
pub use overflow::{
    relational_overflow_descriptor_file, relational_overflow_extent_file,
    relational_overflow_manifest_generation_file, RelationalHydrationBudget,
    RelationalOverflowArtifactMetadata, RelationalOverflowConfig,
    RelationalOverflowExactGenerationRequest, RelationalOverflowExactPublicationReport,
    RelationalOverflowExtentDescriptor, RelationalOverflowExtentInput,
    RelationalOverflowGenerationArtifacts, RelationalOverflowPublicationConfig,
    RelationalOverflowPublicationError, RelationalOverflowPublicationPhase,
    RelationalOverflowPublicationReport, RelationalOverflowPublisher, RelationalOverflowRef,
    RelationalOverflowReferenceSet, RelationalOverflowReferenceSetBuilder,
    RelationalOverflowReferenceSortConfig, RelationalOverflowReferenceSortReport,
    RelationalOverflowRootBinding, RelationalOverflowRootManifest, RelationalOverflowRootReader,
    DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES, DEFAULT_RELATIONAL_OVERFLOW_EXTENTS,
    DEFAULT_RELATIONAL_OVERFLOW_MANIFEST_BYTES, DEFAULT_RELATIONAL_OVERFLOW_NEW_EXTENT_BYTES,
    DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_OCCURRENCES, DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_RUNS,
    DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_SORT_MEMORY_BYTES,
    DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_SPILL_BYTES, DEFAULT_RELATIONAL_OVERFLOW_THRESHOLD_BYTES,
    RELATIONAL_OVERFLOW_MANIFEST_FILE,
};
pub(crate) use recovery::RELATIONAL_RECOVERY_SOURCE_BYTES;
pub use recovery::{
    RelationalRecoveryFence, RelationalRecoverySourceBuilder, RelationalRecoverySourceIdentity,
};
pub use row_page::{
    relational_row_delta_manifest_generation_file, relational_row_delta_run_file,
    relational_row_page_artifact_file, relational_row_page_manifest_generation_file,
    relational_row_page_root_descriptor_file, relational_row_page_root_key_file,
    ImmutableRelationalRowPage, RelationalProjectedField, RelationalProjectedFieldRef,
    RelationalProjectedRow, RelationalProjectedRowRef, RelationalProjectedRowView,
    RelationalRowDeltaBaseBinding, RelationalRowDeltaBuilder, RelationalRowDeltaConfig,
    RelationalRowDeltaError, RelationalRowDeltaGeneration, RelationalRowDeltaManifest,
    RelationalRowDeltaPublicationPhase, RelationalRowDeltaReadReport, RelationalRowDeltaReader,
    RelationalRowDeltaReport, RelationalRowDeltaTableMetadata, RelationalRowPageArtifactMetadata,
    RelationalRowPageBootstrap, RelationalRowPageBootstrapReport, RelationalRowPageCheckpointError,
    RelationalRowPageDemandReadError, RelationalRowPageDemandReadLimits,
    RelationalRowPageDemandReadReport, RelationalRowPageDemandReader, RelationalRowPageEntry,
    RelationalRowPageError, RelationalRowPageGenerationArtifacts,
    RelationalRowPageGenerationRequest, RelationalRowPageId, RelationalRowPageIdAllocator,
    RelationalRowPageLimits, RelationalRowPageLiveError, RelationalRowPageMutationError,
    RelationalRowPageMutationPlan, RelationalRowPageMutationPlanner,
    RelationalRowPageProjectedFields, RelationalRowPageProjectedRange,
    RelationalRowPageProjectedRangeFields, RelationalRowPagePublicationConfig,
    RelationalRowPagePublicationError, RelationalRowPagePublicationPhase,
    RelationalRowPagePublicationReport, RelationalRowPagePublisher, RelationalRowPageReadView,
    RelationalRowPageReadViewIdentity, RelationalRowPageRecoveredValue,
    RelationalRowPageRootDescriptor, RelationalRowPageRootManifest, RelationalRowPageRootReader,
    RelationalRowPageSlotIntegrity, RelationalRowPageSnapshotPointReport,
    RelationalRowPageSnapshotRangeReport, RelationalRowPageSnapshotReadError,
    RelationalRowPageSnapshotReadLimits, RelationalRowPageSnapshotReader,
    RelationalRowPageSnapshotRowSource, RelationalRowPageTableDelta, RelationalRowPageTableRoot,
    RelationalRowPageView, DEFAULT_RELATIONAL_ROW_DELTA_CHECKPOINT_RUNS,
    DEFAULT_RELATIONAL_ROW_DELTA_DIRTY_BYTES, DEFAULT_RELATIONAL_ROW_DELTA_DIRTY_ENTRIES,
    DEFAULT_RELATIONAL_ROW_DELTA_MANIFEST_BYTES, DEFAULT_RELATIONAL_ROW_DELTA_RUNS,
    DEFAULT_RELATIONAL_ROW_DELTA_RUN_BYTES, DEFAULT_RELATIONAL_ROW_PAGE_BYTES,
    DEFAULT_RELATIONAL_ROW_PAGE_COLUMNS, DEFAULT_RELATIONAL_ROW_PAGE_DIRTY_BYTES,
    DEFAULT_RELATIONAL_ROW_PAGE_DIRTY_PAGES, DEFAULT_RELATIONAL_ROW_PAGE_INLINE_VALUE_BYTES,
    DEFAULT_RELATIONAL_ROW_PAGE_KEY_BYTES, DEFAULT_RELATIONAL_ROW_PAGE_MANIFEST_BYTES,
    DEFAULT_RELATIONAL_ROW_PAGE_READ_BYTES, DEFAULT_RELATIONAL_ROW_PAGE_READ_PAGES,
    DEFAULT_RELATIONAL_ROW_PAGE_READ_PINS, DEFAULT_RELATIONAL_ROW_PAGE_READ_ROWS,
    DEFAULT_RELATIONAL_ROW_PAGE_READ_TREE_HEIGHT, DEFAULT_RELATIONAL_ROW_PAGE_ROOT_KEY_BYTES,
    DEFAULT_RELATIONAL_ROW_PAGE_ROOT_PAGES, DEFAULT_RELATIONAL_ROW_PAGE_ROWS,
    DEFAULT_RELATIONAL_ROW_PAGE_ROW_BYTES, DEFAULT_RELATIONAL_ROW_PAGE_TABLES,
    DEFAULT_RELATIONAL_ROW_PAGE_VALUE_BYTES, DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_BYTES,
    DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_ENTRIES, RELATIONAL_ROW_DELTA_MANIFEST_FILE,
    RELATIONAL_ROW_PAGE_MANIFEST_FILE,
};

pub const DEFAULT_MAX_RELATIONAL_MUTATION_ROWS: usize = 100_000;
pub const DEFAULT_MAX_RELATIONAL_MUTATION_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_MAX_RELATIONAL_INDEX_CHANGES: usize = 100_000;
pub const DEFAULT_MAX_RELATIONAL_INDEX_CHANGE_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_MAX_RELATIONAL_ROW_CHANGES: usize = 100_000;
pub const DEFAULT_MAX_RELATIONAL_ROW_CHANGE_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_MAX_RELATIONAL_PRIMARY_KEY_CHANGES: usize = 4_096;
pub const DEFAULT_MAX_RELATIONAL_PRIMARY_KEY_CHANGE_BYTES: usize = 256 * 1024;
pub const RELATIONAL_PRIMARY_INDEX_NAME: &str = "__primary__";
const RELATIONAL_UNIQUE_INDEX_PREFIX: &str = "__unique_";
const RELATIONAL_FOREIGN_KEY_INDEX_PREFIX: &str = "__foreign_key_";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalMutationLimits {
    pub max_rows: NonZeroUsize,
    pub max_payload_bytes: NonZeroUsize,
}

impl Default for RelationalMutationLimits {
    fn default() -> Self {
        Self {
            max_rows: NonZeroUsize::new(DEFAULT_MAX_RELATIONAL_MUTATION_ROWS)
                .expect("default relational mutation row limit is non-zero"),
            max_payload_bytes: NonZeroUsize::new(DEFAULT_MAX_RELATIONAL_MUTATION_BYTES)
                .expect("default relational mutation byte limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalIndexChangeCaptureLimits {
    pub max_entries: NonZeroUsize,
    pub max_bytes: NonZeroUsize,
}

impl Default for RelationalIndexChangeCaptureLimits {
    fn default() -> Self {
        Self {
            max_entries: NonZeroUsize::new(DEFAULT_MAX_RELATIONAL_INDEX_CHANGES)
                .expect("default relational index change limit is non-zero"),
            max_bytes: NonZeroUsize::new(DEFAULT_MAX_RELATIONAL_INDEX_CHANGE_BYTES)
                .expect("default relational index change byte limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowChangeCaptureLimits {
    pub max_entries: NonZeroUsize,
    pub max_bytes: NonZeroUsize,
}

impl Default for RelationalRowChangeCaptureLimits {
    fn default() -> Self {
        Self {
            max_entries: NonZeroUsize::new(DEFAULT_MAX_RELATIONAL_ROW_CHANGES)
                .expect("default relational row change limit is non-zero"),
            max_bytes: NonZeroUsize::new(DEFAULT_MAX_RELATIONAL_ROW_CHANGE_BYTES)
                .expect("default relational row change byte limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalPrimaryKeyChangeCaptureLimits {
    pub max_entries: NonZeroUsize,
    pub max_bytes: NonZeroUsize,
}

impl Default for RelationalPrimaryKeyChangeCaptureLimits {
    fn default() -> Self {
        Self {
            max_entries: NonZeroUsize::new(DEFAULT_MAX_RELATIONAL_PRIMARY_KEY_CHANGES)
                .expect("default relational primary-key change limit is non-zero"),
            max_bytes: NonZeroUsize::new(DEFAULT_MAX_RELATIONAL_PRIMARY_KEY_CHANGE_BYTES)
                .expect("default relational primary-key change byte limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RelationalScalarType {
    Boolean,
    BigInt,
    DoublePrecision,
    Text,
    Bytea,
}

impl RelationalScalarType {
    pub const fn logical_type(self) -> LogicalType {
        match self {
            Self::Boolean => LogicalType::Boolean,
            Self::BigInt => LogicalType::Int64,
            Self::DoublePrecision => LogicalType::Float64,
            Self::Text => LogicalType::Text,
            Self::Bytea => LogicalType::Binary,
        }
    }
}

#[derive(Debug, Clone)]
pub enum RelationalValue {
    Null,
    Boolean(bool),
    BigInt(i64),
    DoublePrecision(f64),
    Text(String),
    Bytea(Vec<u8>),
    Overflow(RelationalOverflowRef),
}

/// An allocation-free view over a relational scalar.
///
/// Variable-width inline values borrow their backing row page or resident row.
/// Call [`RelationalValueRef::to_owned_value`] only when the value must outlive
/// the current scan callback.
#[derive(Debug, Clone, Copy)]
pub enum RelationalValueRef<'a> {
    Null,
    Boolean(bool),
    BigInt(i64),
    DoublePrecision(f64),
    Text(&'a str),
    Bytea(&'a [u8]),
    Overflow(RelationalOverflowRef),
}

impl RelationalValue {
    pub fn as_ref(&self) -> RelationalValueRef<'_> {
        self.into()
    }

    pub fn scalar_type(&self) -> Option<RelationalScalarType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(RelationalScalarType::Boolean),
            Self::BigInt(_) => Some(RelationalScalarType::BigInt),
            Self::DoublePrecision(_) => Some(RelationalScalarType::DoublePrecision),
            Self::Text(_) => Some(RelationalScalarType::Text),
            Self::Bytea(_) => Some(RelationalScalarType::Bytea),
            Self::Overflow(reference) => Some(reference.scalar_type),
        }
    }

    pub fn logical_type(&self) -> Option<LogicalType> {
        self.scalar_type().map(RelationalScalarType::logical_type)
    }

    pub fn estimated_payload_bytes(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::BigInt(_) | Self::DoublePrecision(_) => 8,
            Self::Text(value) => value.len(),
            Self::Bytea(value) => value.len(),
            Self::Overflow(_) => std::mem::size_of::<RelationalOverflowRef>(),
        }
    }

    fn kind_rank(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::BigInt(_) => 2,
            Self::DoublePrecision(_) => 3,
            Self::Text(_) => 4,
            Self::Bytea(_) => 5,
            Self::Overflow(_) => 6,
        }
    }
}

impl<'a> RelationalValueRef<'a> {
    pub const fn scalar_type(self) -> Option<RelationalScalarType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(RelationalScalarType::Boolean),
            Self::BigInt(_) => Some(RelationalScalarType::BigInt),
            Self::DoublePrecision(_) => Some(RelationalScalarType::DoublePrecision),
            Self::Text(_) => Some(RelationalScalarType::Text),
            Self::Bytea(_) => Some(RelationalScalarType::Bytea),
            Self::Overflow(reference) => Some(reference.scalar_type),
        }
    }

    pub const fn logical_type(self) -> Option<LogicalType> {
        match self.scalar_type() {
            Some(scalar_type) => Some(scalar_type.logical_type()),
            None => None,
        }
    }

    pub const fn estimated_payload_bytes(self) -> usize {
        match self {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::BigInt(_) | Self::DoublePrecision(_) => 8,
            Self::Text(value) => value.len(),
            Self::Bytea(value) => value.len(),
            Self::Overflow(_) => std::mem::size_of::<RelationalOverflowRef>(),
        }
    }

    pub fn to_owned_value(self) -> RelationalValue {
        match self {
            Self::Null => RelationalValue::Null,
            Self::Boolean(value) => RelationalValue::Boolean(value),
            Self::BigInt(value) => RelationalValue::BigInt(value),
            Self::DoublePrecision(value) => RelationalValue::DoublePrecision(value),
            Self::Text(value) => RelationalValue::Text(value.to_owned()),
            Self::Bytea(value) => RelationalValue::Bytea(value.to_vec()),
            Self::Overflow(reference) => RelationalValue::Overflow(reference),
        }
    }

    const fn kind_rank(self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::BigInt(_) => 2,
            Self::DoublePrecision(_) => 3,
            Self::Text(_) => 4,
            Self::Bytea(_) => 5,
            Self::Overflow(_) => 6,
        }
    }
}

impl<'a> From<&'a RelationalValue> for RelationalValueRef<'a> {
    fn from(value: &'a RelationalValue) -> Self {
        match value {
            RelationalValue::Null => Self::Null,
            RelationalValue::Boolean(value) => Self::Boolean(*value),
            RelationalValue::BigInt(value) => Self::BigInt(*value),
            RelationalValue::DoublePrecision(value) => Self::DoublePrecision(*value),
            RelationalValue::Text(value) => Self::Text(value),
            RelationalValue::Bytea(value) => Self::Bytea(value),
            RelationalValue::Overflow(reference) => Self::Overflow(*reference),
        }
    }
}

impl PartialEq for RelationalValueRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for RelationalValueRef<'_> {}

impl PartialOrd for RelationalValueRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RelationalValueRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.kind_rank()
            .cmp(&other.kind_rank())
            .then_with(|| match (*self, *other) {
                (Self::Null, Self::Null) => Ordering::Equal,
                (Self::Boolean(left), Self::Boolean(right)) => left.cmp(&right),
                (Self::BigInt(left), Self::BigInt(right)) => left.cmp(&right),
                (Self::DoublePrecision(left), Self::DoublePrecision(right)) => {
                    left.total_cmp(&right)
                }
                (Self::Text(left), Self::Text(right)) => left.cmp(right),
                (Self::Bytea(left), Self::Bytea(right)) => left.cmp(right),
                (Self::Overflow(left), Self::Overflow(right)) => left.cmp(&right),
                _ => Ordering::Equal,
            })
    }
}

impl PartialEq for RelationalValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for RelationalValue {}

impl PartialOrd for RelationalValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RelationalValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.kind_rank()
            .cmp(&other.kind_rank())
            .then_with(|| match (self, other) {
                (Self::Null, Self::Null) => Ordering::Equal,
                (Self::Boolean(left), Self::Boolean(right)) => left.cmp(right),
                (Self::BigInt(left), Self::BigInt(right)) => left.cmp(right),
                (Self::DoublePrecision(left), Self::DoublePrecision(right)) => {
                    left.total_cmp(right)
                }
                (Self::Text(left), Self::Text(right)) => left.cmp(right),
                (Self::Bytea(left), Self::Bytea(right)) => left.cmp(right),
                (Self::Overflow(left), Self::Overflow(right)) => left.cmp(right),
                _ => Ordering::Equal,
            })
    }
}

impl Hash for RelationalValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind_rank().hash(state);
        match self {
            Self::Null => {}
            Self::Boolean(value) => value.hash(state),
            Self::BigInt(value) => value.hash(state),
            Self::DoublePrecision(value) => value.to_bits().hash(state),
            Self::Text(value) => value.hash(state),
            Self::Bytea(value) => value.hash(state),
            Self::Overflow(reference) => reference.hash(state),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalColumnSchema {
    pub name: String,
    pub scalar_type: RelationalScalarType,
    pub nullable: bool,
    pub default: Option<RelationalValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalReferentialAction {
    NoAction,
    Restrict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalForeignKeySchema {
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    pub on_delete: RelationalReferentialAction,
    pub on_update: RelationalReferentialAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexSchema {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalTableSchema {
    pub name: String,
    pub columns: Vec<RelationalColumnSchema>,
    pub primary_key: Vec<String>,
    pub unique_constraints: Vec<Vec<String>>,
    pub foreign_keys: Vec<RelationalForeignKeySchema>,
    pub indexes: Vec<RelationalIndexSchema>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RelationalIndexRole {
    Primary,
    UniqueConstraint,
    DeclaredUnique,
    Secondary,
    ForeignKeySupport,
}

impl RelationalIndexRole {
    pub const fn is_unique(self) -> bool {
        matches!(
            self,
            Self::Primary | Self::UniqueConstraint | Self::DeclaredUnique
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexDefinition {
    pub name: String,
    pub columns: Vec<String>,
    pub role: RelationalIndexRole,
}

impl RelationalTableSchema {
    pub fn column_position(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|column| column.name == name)
    }

    pub fn required_index_definitions(&self) -> Vec<RelationalIndexDefinition> {
        let mut definitions = Vec::with_capacity(
            1 + self.unique_constraints.len() + self.indexes.len() + self.foreign_keys.len(),
        );
        definitions.push(RelationalIndexDefinition {
            name: RELATIONAL_PRIMARY_INDEX_NAME.to_string(),
            columns: self.primary_key.clone(),
            role: RelationalIndexRole::Primary,
        });
        definitions.extend(
            self.unique_constraints
                .iter()
                .enumerate()
                .map(|(ordinal, columns)| RelationalIndexDefinition {
                    name: relational_unique_index_name(ordinal),
                    columns: columns.clone(),
                    role: RelationalIndexRole::UniqueConstraint,
                }),
        );
        definitions.extend(self.indexes.iter().map(|index| RelationalIndexDefinition {
            name: index.name.clone(),
            columns: index.columns.clone(),
            role: if index.unique {
                RelationalIndexRole::DeclaredUnique
            } else {
                RelationalIndexRole::Secondary
            },
        }));
        definitions.extend(
            self.foreign_keys
                .iter()
                .enumerate()
                .map(|(ordinal, foreign_key)| RelationalIndexDefinition {
                    name: relational_foreign_key_index_name(ordinal),
                    columns: foreign_key.columns.clone(),
                    role: RelationalIndexRole::ForeignKeySupport,
                }),
        );
        definitions
    }

    pub fn unique_index_definition(&self, columns: &[String]) -> Option<RelationalIndexDefinition> {
        if columns == self.primary_key {
            return Some(RelationalIndexDefinition {
                name: RELATIONAL_PRIMARY_INDEX_NAME.to_string(),
                columns: self.primary_key.clone(),
                role: RelationalIndexRole::Primary,
            });
        }
        if let Some((ordinal, columns)) = self
            .unique_constraints
            .iter()
            .enumerate()
            .find(|(_, candidate)| candidate.as_slice() == columns)
        {
            return Some(RelationalIndexDefinition {
                name: relational_unique_index_name(ordinal),
                columns: columns.clone(),
                role: RelationalIndexRole::UniqueConstraint,
            });
        }
        self.indexes
            .iter()
            .find(|index| index.unique && index.columns == columns)
            .map(|index| RelationalIndexDefinition {
                name: index.name.clone(),
                columns: index.columns.clone(),
                role: RelationalIndexRole::DeclaredUnique,
            })
    }
}

pub fn relational_unique_index_name(ordinal: usize) -> String {
    format!("{RELATIONAL_UNIQUE_INDEX_PREFIX}{ordinal}")
}

pub fn relational_foreign_key_index_name(ordinal: usize) -> String {
    format!("{RELATIONAL_FOREIGN_KEY_INDEX_PREFIX}{ordinal}")
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelationalKey(pub Vec<RelationalValue>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalIndexScanDirection {
    Forward,
    Backward,
}

/// One ordered composite-index range constrained by a leading equality prefix.
///
/// `exclusive_bound`, when present, is a complete index key. Forward scans
/// visit keys greater than the bound; backward scans visit keys less than it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexRangeScan {
    pub prefix: RelationalKey,
    pub exclusive_bound: Option<RelationalKey>,
    pub direction: RelationalIndexScanDirection,
}

impl RelationalIndexRangeScan {
    pub fn matches(&self, key: &RelationalKey) -> bool {
        relational_key_has_prefix(key, &self.prefix)
            && self
                .exclusive_bound
                .as_ref()
                .is_none_or(|bound| match self.direction {
                    RelationalIndexScanDirection::Forward => key > bound,
                    RelationalIndexScanDirection::Backward => key < bound,
                })
    }
}

pub fn encode_relational_primary_key(key: &RelationalKey) -> Result<Vec<u8>, RelationalError> {
    ordered_key::encode_ordered_relational_key(key)
        .map_err(|error| RelationalError::Corruption(error.to_string()))
}

pub fn decode_relational_primary_key(encoded: &[u8]) -> Result<RelationalKey, RelationalError> {
    ordered_key::decode_ordered_relational_key(encoded)
        .map_err(|error| RelationalError::Corruption(error.to_string()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRow {
    values: Arc<[RelationalValue]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalIndexChangeKind {
    Delete,
    Insert,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexChange {
    pub table: String,
    pub index: String,
    pub index_key: RelationalKey,
    pub primary_key: RelationalKey,
    pub kind: RelationalIndexChangeKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalIndexChangeCapture {
    Captured {
        changes: Vec<RelationalIndexChange>,
        encoded_bytes: usize,
    },
    Invalidated {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowChange {
    pub table: String,
    pub primary_key: RelationalKey,
    pub row: Option<RelationalRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalTablePrimaryKeyChanges {
    pub table: String,
    pub primary_keys: Vec<RelationalKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalPrimaryKeyChangeRebuildReason {
    SchemaRewrite,
    CaptureLimitExceeded,
    UnsupportedKeyEncoding,
    WalEncodingLimitExceeded,
    MissingWalCapture,
    SnapshotReplacement,
    MultipleRelationalTransactions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalPrimaryKeyChangeCapture {
    Captured {
        tables: Vec<RelationalTablePrimaryKeyChanges>,
        encoded_bytes: usize,
    },
    RequiresRebuild {
        reason: RelationalPrimaryKeyChangeRebuildReason,
    },
}

impl RelationalPrimaryKeyChangeCapture {
    pub fn operation_count(&self) -> usize {
        match self {
            Self::Captured { tables, .. } => tables
                .iter()
                .map(|table| table.primary_keys.len())
                .fold(0usize, usize::saturating_add),
            Self::RequiresRebuild { .. } => 0,
        }
    }

    pub const fn encoded_bytes(&self) -> usize {
        match self {
            Self::Captured { encoded_bytes, .. } => *encoded_bytes,
            Self::RequiresRebuild { .. } => 1,
        }
    }

    pub fn estimated_retained_bytes(&self) -> usize {
        let mut bytes = std::mem::size_of::<Self>();
        let Self::Captured { tables, .. } = self else {
            return bytes;
        };
        bytes = bytes.saturating_add(
            tables
                .capacity()
                .saturating_mul(std::mem::size_of::<RelationalTablePrimaryKeyChanges>()),
        );
        for table in tables {
            bytes = bytes.saturating_add(table.table.capacity());
            bytes = bytes.saturating_add(
                table
                    .primary_keys
                    .capacity()
                    .saturating_mul(std::mem::size_of::<RelationalKey>()),
            );
            for key in &table.primary_keys {
                bytes = bytes.saturating_add(
                    key.0
                        .capacity()
                        .saturating_mul(std::mem::size_of::<RelationalValue>()),
                );
                for value in &key.0 {
                    bytes = bytes.saturating_add(match value {
                        RelationalValue::Text(value) => value.capacity(),
                        RelationalValue::Bytea(value) => value.capacity(),
                        _ => 0,
                    });
                }
            }
        }
        bytes
    }

    pub const fn requires_rebuild(&self) -> bool {
        matches!(self, Self::RequiresRebuild { .. })
    }

    pub fn exceeds_limits(&self, limits: RelationalPrimaryKeyChangeCaptureLimits) -> bool {
        match self {
            Self::Captured { encoded_bytes, .. } => {
                self.operation_count() > limits.max_entries.get()
                    || *encoded_bytes > limits.max_bytes.get()
            }
            Self::RequiresRebuild { .. } => false,
        }
    }
}

#[derive(Debug)]
pub struct RelationalTransactionStageResult {
    pub state: RelationalState,
    pub index_capture: Option<RelationalIndexChangeCapture>,
    pub row_capture: Option<RelationalRowChangeCapture>,
    pub replay_access: Option<RelationalReplayAccessSet>,
    pub primary_key_changes: RelationalPrimaryKeyChangeCapture,
    pub mutation_outcomes: Vec<RelationalMutationOutcome>,
}

/// Deterministic result of one relational write after it has been staged.
///
/// Rows contain the logical values accepted by INSERT before overflow
/// externalization. Conflict no-ops contribute to `conflict_rows`, never to
/// `rows` or `affected_rows`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalMutationOutcome {
    pub table: String,
    pub affected_rows: usize,
    pub conflict_rows: usize,
    pub rows: Vec<RelationalRow>,
}

/// The exact primary-key working set used by one durable relational DML
/// transaction.
///
/// Entries are strictly ordered by table and primary key. Unlike the final row
/// change capture, this set retains every row evaluated by predicate DML, keys
/// whose values changed transiently and returned to their original value, and
/// conflict rows read by a no-op `UPSERT`. Recovery can therefore hydrate only
/// this bounded set before replaying predicate DML instead of scanning rows
/// that were not authenticated by the WAL record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalReplayAccessSet {
    entries: Vec<RelationalReplayAccess>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RelationalReplayAccess {
    pub table: String,
    pub primary_key: RelationalKey,
}

/// One exactly authenticated WAL access after bounded row hydration.
///
/// Missing rows are explicit so a sparse recovery caller cannot accidentally
/// omit a key that the durable transaction evaluated. Present rows must be
/// fully hydrated logical values; the sparse workspace externalizes large
/// values again only when replay mutates them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalSparseRecoveryRow {
    pub table: String,
    pub primary_key: RelationalKey,
    pub row: Option<RelationalRow>,
}

/// Incrementally admitted live hydration workspace.
///
/// Admission is atomic per key: a failed entry or byte reservation leaves the
/// workspace unchanged. The byte accounting deliberately includes the later
/// replay-access ledger because sparse preparation and final staging retain
/// both views at the same time.
#[derive(Debug)]
pub struct RelationalSparseWorkspaceBuilder {
    rows: BTreeMap<RelationalReplayAccess, Option<RelationalRow>>,
    resident_bytes: usize,
    limits: RelationalRowChangeCaptureLimits,
}

impl RelationalSparseWorkspaceBuilder {
    pub fn new(limits: RelationalRowChangeCaptureLimits) -> Self {
        Self {
            rows: BTreeMap::new(),
            resident_bytes: 0,
            limits,
        }
    }

    pub fn contains(&self, access: &RelationalReplayAccess) -> bool {
        self.rows.contains_key(access)
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    pub fn remaining_entries(&self) -> usize {
        self.limits
            .max_entries
            .get()
            .saturating_sub(self.rows.len())
    }

    pub fn insert(
        &mut self,
        hydrated: RelationalSparseRecoveryRow,
    ) -> Result<bool, RelationalError> {
        let access = RelationalReplayAccess {
            table: hydrated.table.clone(),
            primary_key: hydrated.primary_key.clone(),
        };
        if let Some(existing) = self.rows.get(&access) {
            if existing == &hydrated.row {
                return Ok(false);
            }
            return Err(RelationalError::Corruption(format!(
                "sparse relational live hydration returned conflicting values for {:?} in table {}",
                access.primary_key, access.table
            )));
        }
        let next_entries = self.rows.len().checked_add(1).ok_or_else(|| {
            RelationalError::Admission(
                "sparse relational live workspace entry count overflow".to_string(),
            )
        })?;
        if next_entries > self.limits.max_entries.get() {
            return Err(RelationalError::Admission(format!(
                "sparse relational live workspace requires {next_entries} entries, exceeding limit {}",
                self.limits.max_entries
            )));
        }
        let entry_bytes = relational_sparse_recovery_entry_bytes(&hydrated).ok_or_else(|| {
            RelationalError::Admission(
                "sparse relational live workspace byte count overflow".to_string(),
            )
        })?;
        let access_bytes = relational_replay_access_resident_bytes(&access).ok_or_else(|| {
            RelationalError::Admission(
                "sparse relational live workspace byte count overflow".to_string(),
            )
        })?;
        let next_bytes = self
            .resident_bytes
            .checked_add(entry_bytes)
            .and_then(|bytes| bytes.checked_add(access_bytes))
            .ok_or_else(|| {
                RelationalError::Admission(
                    "sparse relational live workspace byte count overflow".to_string(),
                )
            })?;
        if next_bytes > self.limits.max_bytes.get() {
            return Err(RelationalError::Admission(format!(
                "sparse relational live workspace requires {next_bytes} bytes, exceeding limit {}",
                self.limits.max_bytes
            )));
        }
        self.rows.insert(access, hydrated.row);
        self.resident_bytes = next_bytes;
        Ok(true)
    }

    pub fn snapshot(&self) -> Vec<RelationalSparseRecoveryRow> {
        self.rows
            .iter()
            .map(|(access, row)| RelationalSparseRecoveryRow {
                table: access.table.clone(),
                primary_key: access.primary_key.clone(),
                row: row.clone(),
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct RelationalSparseRecoveryStage<'a> {
    pub transaction: RelationalTransaction,
    pub hydrated_access: Vec<RelationalSparseRecoveryRow>,
    pub mutation_limits: RelationalMutationLimits,
    pub overflow_config: RelationalOverflowConfig,
    pub index_capture_limits: RelationalIndexChangeCaptureLimits,
    pub row_capture_limits: RelationalRowChangeCaptureLimits,
    pub expected_replay_access: &'a RelationalReplayAccessSet,
}

/// One schema-stable live transaction staged against a bounded hydrated
/// workspace and a generation-pinned authoritative constraint index.
///
/// The workspace may contain unchanged rows needed only to validate unique or
/// foreign-key postings. Every primary key actually evaluated or changed by
/// the transaction must still be represented explicitly as present or
/// missing; staging fails closed if the resulting replay-access set escapes
/// that supplied workspace.
pub struct RelationalSparseLiveStage<'a> {
    pub transaction: RelationalTransaction,
    pub hydrated_workspace: Vec<RelationalSparseRecoveryRow>,
    pub mutation_limits: RelationalMutationLimits,
    pub overflow_config: RelationalOverflowConfig,
    pub index_capture_limits: RelationalIndexChangeCaptureLimits,
    pub row_capture_limits: RelationalRowChangeCaptureLimits,
    pub constraint_index: &'a dyn RelationalConstraintIndex,
}

/// One exact persistent-index probe required while preparing a bounded live
/// relational workspace.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RelationalSparseIndexProbe {
    pub table: String,
    pub index: String,
    pub index_key: RelationalKey,
}

/// One primary-key-ordered insert batch that may prove all target keys absent
/// from immutable page descriptors and live-overlay bounds.
///
/// The partition is the primary-key prefix excluding its final component. An
/// empty prefix therefore represents a table with a single-column primary key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RelationalMonotonicAppendHydration {
    pub table: String,
    pub partition_prefix: RelationalKey,
    pub primary_keys: Vec<RelationalKey>,
}

/// Schema-derived hydration needed before a live transaction can be prepared.
///
/// Point accesses include explicit absence checks for direct keys. Predicate
/// tables require a bounded canonical range scan because every evaluated row
/// belongs to the authenticated WAL access set. UPSERT conflict probes are
/// resolved through the generation-pinned authoritative index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalSparseMutationHydrationPlan {
    point_access: Vec<RelationalReplayAccess>,
    monotonic_appends: Vec<RelationalMonotonicAppendHydration>,
    scan_tables: Vec<String>,
    index_probes: Vec<RelationalSparseIndexProbe>,
}

impl RelationalSparseMutationHydrationPlan {
    pub fn point_access(&self) -> &[RelationalReplayAccess] {
        &self.point_access
    }

    pub fn monotonic_appends(&self) -> &[RelationalMonotonicAppendHydration] {
        &self.monotonic_appends
    }

    pub fn scan_tables(&self) -> &[String] {
        &self.scan_tables
    }

    pub fn index_probes(&self) -> &[RelationalSparseIndexProbe] {
        &self.index_probes
    }
}

#[derive(Debug)]
pub struct RelationalSparseLivePreparationStage {
    pub transaction: RelationalTransaction,
    pub hydrated_workspace: Vec<RelationalSparseRecoveryRow>,
    pub mutation_limits: RelationalMutationLimits,
    pub overflow_config: RelationalOverflowConfig,
    pub index_capture_limits: RelationalIndexChangeCaptureLimits,
    pub row_capture_limits: RelationalRowChangeCaptureLimits,
}

/// Exact access and constraint probes discovered by replaying one live
/// transaction in a bounded, unpublished workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalSparseLivePreparation {
    replay_access: RelationalReplayAccessSet,
    constraint_probes: Vec<RelationalSparseIndexProbe>,
}

impl RelationalSparseLivePreparation {
    pub fn replay_access(&self) -> &RelationalReplayAccessSet {
        &self.replay_access
    }

    pub fn constraint_probes(&self) -> &[RelationalSparseIndexProbe] {
        &self.constraint_probes
    }
}

impl RelationalReplayAccessSet {
    pub fn entries(&self) -> &[RelationalReplayAccess] {
        &self.entries
    }

    pub(crate) fn from_decoded_entries(
        entries: Vec<RelationalReplayAccess>,
    ) -> Result<Self, RelationalError> {
        if entries.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(RelationalError::Corruption(
                "relational WAL replay access set is not strictly ordered".to_string(),
            ));
        }
        Ok(Self { entries })
    }
}

struct RelationalReplayAccessTracker {
    entries: BTreeMap<String, BTreeSet<RelationalKey>>,
    entry_count: usize,
    encoded_bytes: usize,
    limits: RelationalRowChangeCaptureLimits,
}

impl RelationalReplayAccessTracker {
    fn new(limits: RelationalRowChangeCaptureLimits) -> Self {
        Self {
            entries: BTreeMap::new(),
            entry_count: 0,
            encoded_bytes: 0,
            limits,
        }
    }

    fn record(&mut self, table: &str, primary_key: &RelationalKey) -> Result<(), RelationalError> {
        if self
            .entries
            .get(table)
            .is_some_and(|keys| keys.contains(primary_key))
        {
            return Ok(());
        }
        let entry = RelationalReplayAccess {
            table: table.to_string(),
            primary_key: primary_key.clone(),
        };
        let entry_bytes = estimated_replay_access_encoding_bytes(&entry).ok_or_else(|| {
            RelationalError::Admission(
                "relational WAL replay access-set byte count overflow".to_string(),
            )
        })?;
        let encoded_bytes = self.encoded_bytes.checked_add(entry_bytes).ok_or_else(|| {
            RelationalError::Admission(
                "relational WAL replay access-set byte count overflow".to_string(),
            )
        })?;
        if self.entry_count >= self.limits.max_entries.get()
            || encoded_bytes > self.limits.max_bytes.get()
        {
            return Err(RelationalError::Admission(format!(
                "relational WAL replay access set exceeds max_entries {} or max_bytes {} before WAL append",
                self.limits.max_entries, self.limits.max_bytes
            )));
        }
        self.entries
            .entry(table.to_string())
            .or_default()
            .insert(primary_key.clone());
        self.entry_count += 1;
        self.encoded_bytes = encoded_bytes;
        Ok(())
    }

    fn record_changed_keys(
        &mut self,
        changed_keys: &BTreeMap<String, BTreeSet<RelationalKey>>,
    ) -> Result<(), RelationalError> {
        for (table, keys) in changed_keys {
            for primary_key in keys {
                self.record(table, primary_key)?;
            }
        }
        Ok(())
    }

    fn finish(self) -> RelationalReplayAccessSet {
        let mut entries = Vec::with_capacity(self.entry_count);
        for (table, keys) in self.entries {
            entries.extend(keys.into_iter().map(|primary_key| RelationalReplayAccess {
                table: table.clone(),
                primary_key,
            }));
        }
        RelationalReplayAccessSet { entries }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalRowChangeCapture {
    Captured {
        changes: Vec<RelationalRowChange>,
        encoded_bytes: usize,
    },
    RequiresCheckpoint {
        tables: Vec<String>,
    },
    Invalidated {
        reason: String,
    },
}

impl RelationalIndexChangeCapture {
    fn changes(&self) -> Result<&[RelationalIndexChange], RelationalError> {
        match self {
            Self::Captured { changes, .. } => Ok(changes),
            Self::Invalidated { reason } => Err(RelationalError::Admission(format!(
                "authoritative relational index change capture is unavailable: {reason}"
            ))),
        }
    }
}

impl RelationalRow {
    pub fn new(values: Vec<RelationalValue>) -> Self {
        Self {
            values: Arc::from(values),
        }
    }

    pub fn values(&self) -> &[RelationalValue] {
        &self.values
    }

    fn estimated_payload_bytes(&self) -> usize {
        self.values
            .iter()
            .map(RelationalValue::estimated_payload_bytes)
            .sum()
    }
}

#[derive(Debug, Clone, Default)]
struct RelationalTableSegment {
    rows: RelationalRowPages,
    indexes: BTreeMap<String, RelationalIndexPages>,
}

const RELATIONAL_ROW_PAGE_MAX_ENTRIES: usize = 256;
const RELATIONAL_ROW_PAGE_TARGET_BYTES: usize = 1024 * 1024;

/// An ordered row map backed by immutable COW pages.
///
/// A relational snapshot shares every page. A mutation clones only the page
/// containing the affected primary key instead of cloning the entire table.
#[derive(Debug, Clone, Default)]
struct RelationalRowPages {
    pages: Arc<Vec<Arc<BTreeMap<RelationalKey, RelationalRow>>>>,
    len: usize,
}

impl RelationalRowPages {
    fn from_map(rows: BTreeMap<RelationalKey, RelationalRow>) -> Self {
        let len = rows.len();
        let mut pages = Vec::new();
        let mut page = BTreeMap::new();
        let mut page_bytes = 0usize;
        for (key, row) in rows {
            let entry_bytes = relational_row_entry_bytes(&key, &row);
            if !page.is_empty()
                && (page.len() >= RELATIONAL_ROW_PAGE_MAX_ENTRIES
                    || page_bytes.saturating_add(entry_bytes) > RELATIONAL_ROW_PAGE_TARGET_BYTES)
            {
                pages.push(Arc::new(std::mem::take(&mut page)));
                page_bytes = 0;
            }
            page_bytes = page_bytes.saturating_add(entry_bytes);
            page.insert(key, row);
        }
        if !page.is_empty() {
            pages.push(Arc::new(page));
        }
        Self {
            pages: Arc::new(pages),
            len,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn page_index(&self, key: &RelationalKey) -> Option<usize> {
        if self.pages.is_empty() {
            return None;
        }
        let index = self.pages.partition_point(|page| {
            page.last_key_value()
                .is_some_and(|(last_key, _)| last_key < key)
        });
        Some(index.min(self.pages.len() - 1))
    }

    fn get(&self, key: &RelationalKey) -> Option<&RelationalRow> {
        self.page_index(key)
            .and_then(|index| self.pages[index].get(key))
    }

    fn get_key_value(&self, key: &RelationalKey) -> Option<(&RelationalKey, &RelationalRow)> {
        self.page_index(key)
            .and_then(|index| self.pages[index].get_key_value(key))
    }

    fn contains_key(&self, key: &RelationalKey) -> bool {
        self.get(key).is_some()
    }

    fn iter(&self) -> impl DoubleEndedIterator<Item = (&RelationalKey, &RelationalRow)> {
        self.pages.iter().flat_map(|page| page.iter())
    }

    fn values(&self) -> impl DoubleEndedIterator<Item = &RelationalRow> {
        self.iter().map(|(_, row)| row)
    }

    fn append_value_to_all(&mut self, value: &RelationalValue) {
        let pages = Arc::make_mut(&mut self.pages);
        for page in pages {
            for row in Arc::make_mut(page).values_mut() {
                let mut values = row.values().to_vec();
                values.push(value.clone());
                *row = RelationalRow::new(values);
            }
        }
    }

    fn insert(&mut self, key: RelationalKey, row: RelationalRow) -> Option<RelationalRow> {
        if self.pages.is_empty() {
            self.pages = Arc::new(vec![Arc::new(BTreeMap::from([(key, row)]))]);
            self.len = 1;
            return None;
        }
        let index = self
            .page_index(&key)
            .expect("non-empty relational row pages have a target page");
        let pages = Arc::make_mut(&mut self.pages);
        let previous = Arc::make_mut(&mut pages[index]).insert(key, row);
        if previous.is_none() {
            self.len = self.len.saturating_add(1);
        }
        split_relational_row_page(pages, index);
        previous
    }

    fn remove(&mut self, key: &RelationalKey) -> Option<RelationalRow> {
        let index = self.page_index(key)?;
        if !self.pages[index].contains_key(key) {
            return None;
        }
        let pages = Arc::make_mut(&mut self.pages);
        let removed = Arc::make_mut(&mut pages[index]).remove(key);
        if removed.is_some() {
            self.len = self.len.saturating_sub(1);
            if pages[index].is_empty() {
                pages.remove(index);
            }
        }
        removed
    }

    #[cfg(test)]
    fn page_count(&self) -> usize {
        self.pages.len()
    }

    #[cfg(test)]
    fn shared_page_count(&self, other: &Self) -> usize {
        self.pages
            .iter()
            .filter(|left| other.pages.iter().any(|right| Arc::ptr_eq(left, right)))
            .count()
    }
}

fn relational_row_entry_bytes(key: &RelationalKey, row: &RelationalRow) -> usize {
    std::mem::size_of::<RelationalKey>()
        .saturating_add(std::mem::size_of::<RelationalRow>())
        .saturating_add(
            key.0
                .iter()
                .map(RelationalValue::estimated_payload_bytes)
                .sum::<usize>(),
        )
        .saturating_add(row.estimated_payload_bytes())
}

fn relational_sparse_recovery_entry_bytes(entry: &RelationalSparseRecoveryRow) -> Option<usize> {
    let key_payload_bytes = entry
        .primary_key
        .0
        .iter()
        .try_fold(0usize, |bytes, value| {
            bytes.checked_add(value.estimated_payload_bytes())
        })?;
    let key_bytes = std::mem::size_of::<RelationalKey>().checked_add(key_payload_bytes)?;
    let row_bytes = entry.row.as_ref().map_or(Some(0), |row| {
        let value_slots = row
            .values()
            .len()
            .checked_mul(std::mem::size_of::<RelationalValue>())?;
        let value_payload = row.values().iter().try_fold(0usize, |bytes, value| {
            bytes.checked_add(value.estimated_payload_bytes())
        })?;
        std::mem::size_of::<RelationalRow>()
            .checked_add(value_slots)?
            .checked_add(value_payload)
    })?;
    std::mem::size_of::<RelationalSparseRecoveryRow>()
        .checked_add(entry.table.len())?
        .checked_add(key_bytes)?
        .checked_add(row_bytes)
}

fn relational_replay_access_resident_bytes(entry: &RelationalReplayAccess) -> Option<usize> {
    let key_payload_bytes = entry
        .primary_key
        .0
        .iter()
        .try_fold(0usize, |bytes, value| {
            bytes.checked_add(value.estimated_payload_bytes())
        })?;
    std::mem::size_of::<RelationalReplayAccess>()
        .checked_add(entry.table.len())?
        .checked_add(std::mem::size_of::<RelationalKey>())?
        .checked_add(key_payload_bytes)
}

fn split_relational_row_page(
    pages: &mut Vec<Arc<BTreeMap<RelationalKey, RelationalRow>>>,
    index: usize,
) {
    let page = Arc::make_mut(&mut pages[index]);
    let page_bytes = page
        .iter()
        .map(|(key, row)| relational_row_entry_bytes(key, row))
        .sum::<usize>();
    if page.len() <= 1
        || (page.len() <= RELATIONAL_ROW_PAGE_MAX_ENTRIES
            && page_bytes <= RELATIONAL_ROW_PAGE_TARGET_BYTES)
    {
        return;
    }
    let split_key = page
        .keys()
        .nth(page.len() / 2)
        .cloned()
        .expect("oversized relational row page is non-empty");
    let right = page.split_off(&split_key);
    pages.insert(index + 1, Arc::new(right));
}

const RELATIONAL_INDEX_PAGE_MAX_KEYS: usize = 256;
const RELATIONAL_POSTING_PAGE_MAX_KEYS: usize = 256;

#[derive(Debug, Clone, Default)]
struct RelationalKeySetPages {
    pages: Arc<Vec<Arc<BTreeSet<RelationalKey>>>>,
    len: usize,
}

impl RelationalKeySetPages {
    fn page_index(&self, key: &RelationalKey) -> Option<usize> {
        if self.pages.is_empty() {
            return None;
        }
        let index = self
            .pages
            .partition_point(|page| page.last().is_some_and(|last_key| last_key < key));
        Some(index.min(self.pages.len() - 1))
    }

    fn insert(&mut self, key: RelationalKey) -> bool {
        if self.pages.is_empty() {
            self.pages = Arc::new(vec![Arc::new(BTreeSet::from([key]))]);
            self.len = 1;
            return true;
        }
        let index = self
            .page_index(&key)
            .expect("non-empty posting pages have a target page");
        let pages = Arc::make_mut(&mut self.pages);
        let inserted = Arc::make_mut(&mut pages[index]).insert(key);
        if inserted {
            self.len = self.len.saturating_add(1);
        }
        if pages[index].len() > RELATIONAL_POSTING_PAGE_MAX_KEYS {
            let page = Arc::make_mut(&mut pages[index]);
            let split_key = page
                .iter()
                .nth(page.len() / 2)
                .cloned()
                .expect("oversized posting page is non-empty");
            let right = page.split_off(&split_key);
            pages.insert(index + 1, Arc::new(right));
        }
        inserted
    }

    fn remove(&mut self, key: &RelationalKey) -> bool {
        let Some(index) = self.page_index(key) else {
            return false;
        };
        if !self.pages[index].contains(key) {
            return false;
        }
        let pages = Arc::make_mut(&mut self.pages);
        let removed = Arc::make_mut(&mut pages[index]).remove(key);
        if removed {
            self.len = self.len.saturating_sub(1);
            if pages[index].is_empty() {
                pages.remove(index);
            }
        }
        removed
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn iter(&self) -> impl DoubleEndedIterator<Item = &RelationalKey> {
        self.pages.iter().flat_map(|page| page.iter())
    }

    #[cfg(test)]
    fn page_count(&self) -> usize {
        self.pages.len()
    }

    #[cfg(test)]
    fn shared_page_count(&self, other: &Self) -> usize {
        self.pages
            .iter()
            .filter(|left| other.pages.iter().any(|right| Arc::ptr_eq(left, right)))
            .count()
    }
}

#[derive(Debug, Clone, Default)]
struct RelationalIndexPages {
    pages: Arc<Vec<Arc<BTreeMap<RelationalKey, RelationalKeySetPages>>>>,
}

impl RelationalIndexPages {
    fn page_index(&self, key: &RelationalKey) -> Option<usize> {
        if self.pages.is_empty() {
            return None;
        }
        let index = self.pages.partition_point(|page| {
            page.last_key_value()
                .is_some_and(|(last_key, _)| last_key < key)
        });
        Some(index.min(self.pages.len() - 1))
    }

    fn get(&self, key: &RelationalKey) -> Option<&RelationalKeySetPages> {
        self.page_index(key)
            .and_then(|index| self.pages[index].get(key))
    }

    fn insert_posting(&mut self, index_key: RelationalKey, primary_key: RelationalKey) -> bool {
        if self.pages.is_empty() {
            let mut postings = RelationalKeySetPages::default();
            postings.insert(primary_key);
            self.pages = Arc::new(vec![Arc::new(BTreeMap::from([(index_key, postings)]))]);
            return true;
        }
        let index = self
            .page_index(&index_key)
            .expect("non-empty index pages have a target page");
        let pages = Arc::make_mut(&mut self.pages);
        let page = Arc::make_mut(&mut pages[index]);
        let inserted = page.entry(index_key).or_default().insert(primary_key);
        if page.len() > RELATIONAL_INDEX_PAGE_MAX_KEYS {
            let split_key = page
                .keys()
                .nth(page.len() / 2)
                .cloned()
                .expect("oversized index page is non-empty");
            let right = page.split_off(&split_key);
            pages.insert(index + 1, Arc::new(right));
        }
        inserted
    }

    fn remove_posting(&mut self, index_key: &RelationalKey, primary_key: &RelationalKey) {
        let Some(index) = self.page_index(index_key) else {
            return;
        };
        if !self.pages[index].contains_key(index_key) {
            return;
        }
        let pages = Arc::make_mut(&mut self.pages);
        let page = Arc::make_mut(&mut pages[index]);
        let remove_key = page
            .get_mut(index_key)
            .is_some_and(|postings| postings.remove(primary_key) && postings.is_empty());
        if remove_key {
            page.remove(index_key);
        }
        if page.is_empty() {
            pages.remove(index);
        }
    }

    fn prefix_cardinality(&self, prefix: &RelationalKey) -> usize {
        self.prefix_cardinality_at_most(prefix, usize::MAX)
    }

    fn prefix_cardinality_at_most(&self, prefix: &RelationalKey, max_count: usize) -> usize {
        if max_count == 0 {
            return 0;
        }
        let mut cardinality = 0usize;
        self.visit_prefix(prefix, |postings| {
            cardinality = cardinality.saturating_add(postings.len);
            cardinality < max_count
        });
        cardinality.min(max_count)
    }

    fn prefix_primary_keys<'a>(
        &'a self,
        prefix: &RelationalKey,
        max_keys: usize,
    ) -> Vec<&'a RelationalKey> {
        if max_keys == 0 {
            return Vec::new();
        }
        let mut keys = Vec::new();
        self.visit_prefix(prefix, |postings| {
            for key in postings.iter() {
                keys.push(key);
                if keys.len() >= max_keys {
                    return false;
                }
            }
            true
        });
        keys
    }

    fn visit_prefix<'a>(
        &'a self,
        prefix: &RelationalKey,
        mut visit: impl FnMut(&'a RelationalKeySetPages) -> bool,
    ) {
        let Some(start) = self.page_index(prefix) else {
            return;
        };
        'pages: for page in &self.pages[start..] {
            for (key, postings) in page.range(prefix.clone()..) {
                if !relational_key_has_prefix(key, prefix) {
                    break 'pages;
                }
                if !visit(postings) {
                    break 'pages;
                }
            }
        }
    }

    fn visit_prefix_entries<'a>(
        &'a self,
        prefix: &RelationalKey,
        mut visit: impl FnMut(&'a RelationalKey, &'a RelationalKey) -> bool,
    ) {
        let Some(start) = self.page_index(prefix) else {
            return;
        };
        'pages: for page in &self.pages[start..] {
            for (index_key, postings) in page.range(prefix.clone()..) {
                if !relational_key_has_prefix(index_key, prefix) {
                    break 'pages;
                }
                for primary_key in postings.iter() {
                    if !visit(index_key, primary_key) {
                        break 'pages;
                    }
                }
            }
        }
    }

    fn visit_range_entries<'a>(
        &'a self,
        scan: &RelationalIndexRangeScan,
        mut visit: impl FnMut(&'a RelationalKey, &'a RelationalKey) -> bool,
    ) {
        let seek = scan.exclusive_bound.as_ref().unwrap_or(&scan.prefix);
        let Some(mut start) = self.page_index(seek) else {
            return;
        };
        if scan.direction == RelationalIndexScanDirection::Backward
            && scan.exclusive_bound.is_none()
        {
            while self.pages.get(start + 1).is_some_and(|page| {
                page.first_key_value()
                    .is_some_and(|(key, _)| relational_key_has_prefix(key, &scan.prefix))
            }) {
                start += 1;
            }
        }
        match scan.direction {
            RelationalIndexScanDirection::Forward => {
                'pages: for (page_ordinal, page) in self.pages[start..].iter().enumerate() {
                    let lower = if page_ordinal == 0 {
                        match &scan.exclusive_bound {
                            Some(bound) => std::ops::Bound::Excluded(bound.clone()),
                            None => std::ops::Bound::Included(scan.prefix.clone()),
                        }
                    } else {
                        std::ops::Bound::Unbounded
                    };
                    for (index_key, postings) in page.range((lower, std::ops::Bound::Unbounded)) {
                        if !relational_key_has_prefix(index_key, &scan.prefix) {
                            break 'pages;
                        }
                        for primary_key in postings.iter() {
                            if !visit(index_key, primary_key) {
                                break 'pages;
                            }
                        }
                    }
                }
            }
            RelationalIndexScanDirection::Backward => {
                'pages: for (page_ordinal, page) in self.pages[..=start].iter().rev().enumerate() {
                    let upper = if page_ordinal == 0 {
                        scan.exclusive_bound
                            .as_ref()
                            .map_or(std::ops::Bound::Unbounded, |bound| {
                                std::ops::Bound::Excluded(bound.clone())
                            })
                    } else {
                        std::ops::Bound::Unbounded
                    };
                    for (index_key, postings) in
                        page.range((std::ops::Bound::Unbounded, upper)).rev()
                    {
                        if !relational_key_has_prefix(index_key, &scan.prefix) {
                            if index_key < &scan.prefix {
                                break 'pages;
                            }
                            continue;
                        }
                        for primary_key in postings.iter() {
                            if !visit(index_key, primary_key) {
                                break 'pages;
                            }
                        }
                    }
                }
            }
        }
    }
}

fn relational_key_has_prefix(key: &RelationalKey, prefix: &RelationalKey) -> bool {
    key.0.len() >= prefix.0.len() && key.0[..prefix.0.len()] == prefix.0
}

#[derive(Debug, Clone, Copy)]
pub struct RelationalIndexPosting<'a> {
    postings: &'a RelationalKeySetPages,
}

impl<'a> RelationalIndexPosting<'a> {
    pub fn len(self) -> usize {
        self.postings.len
    }

    pub fn is_empty(self) -> bool {
        self.postings.is_empty()
    }

    pub fn contains(self, key: &RelationalKey) -> bool {
        self.postings
            .page_index(key)
            .is_some_and(|index| self.postings.pages[index].contains(key))
    }

    pub fn iter(self) -> impl DoubleEndedIterator<Item = &'a RelationalKey> {
        self.postings.iter()
    }
}

#[derive(Debug, Clone)]
pub struct RelationalState {
    schemas: BTreeMap<String, Arc<RelationalTableSchema>>,
    segments: BTreeMap<String, Arc<RelationalTableSegment>>,
    overflow_segments: BTreeMap<Sha256Digest, RelationalOverflowSegment>,
    materialized_index_postings_resident: bool,
    materialized_rows_resident: bool,
    canonical_row_metadata_only: bool,
    detached_row_counts: Arc<BTreeMap<String, usize>>,
    detached_total_row_count: usize,
    detached_row_bytes: u64,
}

impl Default for RelationalState {
    fn default() -> Self {
        Self {
            schemas: BTreeMap::new(),
            segments: BTreeMap::new(),
            overflow_segments: BTreeMap::new(),
            materialized_index_postings_resident: true,
            materialized_rows_resident: true,
            canonical_row_metadata_only: false,
            detached_row_counts: Arc::new(BTreeMap::new()),
            detached_total_row_count: 0,
            detached_row_bytes: 0,
        }
    }
}

#[derive(Debug, Clone)]
enum RelationalOverflowSegment {
    Inline(Arc<[u8]>),
    FileRange {
        reader: Arc<FileSegmentRangeReader>,
        range: SegmentReadRange,
    },
}

impl RelationalOverflowSegment {
    fn read(&self) -> Result<Arc<[u8]>, RelationalError> {
        match self {
            Self::Inline(bytes) => Ok(Arc::clone(bytes)),
            Self::FileRange { reader, range } => reader.read_range(range).map_err(|error| {
                RelationalError::Corruption(format!(
                    "failed to read file-backed overflow segment: {error}"
                ))
            }),
        }
    }

    fn is_file_backed(&self) -> bool {
        matches!(self, Self::FileRange { .. })
    }
}

impl RelationalState {
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }

    pub fn materialized_index_postings_resident(&self) -> bool {
        self.materialized_index_postings_resident
    }

    pub fn materialized_rows_resident(&self) -> bool {
        self.materialized_rows_resident
    }

    pub fn canonical_row_metadata_only(&self) -> bool {
        self.canonical_row_metadata_only
    }

    pub fn materialized_row_count(&self) -> usize {
        if !self.materialized_rows_resident {
            return 0;
        }
        self.segments
            .values()
            .map(|segment| segment.rows.len())
            .sum()
    }

    pub fn estimated_materialized_row_bytes(&self) -> u64 {
        if !self.materialized_rows_resident {
            return 0;
        }
        self.segments.values().fold(0u64, |bytes, segment| {
            segment.rows.iter().fold(bytes, |bytes, (key, row)| {
                bytes.saturating_add(relational_row_entry_bytes(key, row) as u64)
            })
        })
    }

    /// Releases the transitional checkpoint-row oracle after a current
    /// canonical row and authoritative index view have been pinned by the
    /// caller. Schemas, exact logical counts, and overflow resolvers remain
    /// available to the bounded SQL serving path.
    pub fn omit_materialized_rows(&mut self) {
        if !self.materialized_rows_resident {
            return;
        }
        let counts = self
            .segments
            .iter()
            .map(|(table, segment)| (table.clone(), segment.rows.len()))
            .collect::<BTreeMap<_, _>>();
        self.detached_total_row_count = counts.values().copied().sum();
        self.detached_row_bytes = self.estimated_materialized_row_bytes();
        self.detached_row_counts = Arc::new(counts);
        for segment in self.segments.values_mut() {
            *segment = Arc::new(RelationalTableSegment::default());
        }
        self.materialized_rows_resident = false;
        self.materialized_index_postings_resident = false;
    }

    /// Builds the canonical metadata-only relational catalog directly from a
    /// validated row root without decoding the transitional row checkpoint.
    pub fn from_canonical_row_root(
        manifest: &RelationalRowPageRootManifest,
    ) -> Result<Self, RelationalError> {
        let mut schemas = BTreeMap::new();
        let mut segments = BTreeMap::new();
        let mut row_counts = BTreeMap::new();
        let mut total_rows = 0usize;
        for table in &manifest.tables {
            if table.table != table.schema.name {
                return Err(RelationalError::Corruption(format!(
                    "canonical row root table {} carries schema {}",
                    table.table, table.schema.name
                )));
            }
            validate_table_schema(&table.schema).map_err(|error| {
                RelationalError::Corruption(format!(
                    "canonical row root table {} has invalid schema: {error}",
                    table.table
                ))
            })?;
            let schema_digest = index_shadow::relational_schema_digest(&table.schema)
                .map_err(|error| RelationalError::Corruption(error.to_string()))?;
            if schema_digest != table.schema_digest {
                return Err(RelationalError::Corruption(format!(
                    "canonical row root table {} schema digest mismatch",
                    table.table
                )));
            }
            if table.schema.columns.len() != table.column_count.get() as usize {
                return Err(RelationalError::Corruption(format!(
                    "canonical row root table {} declares {} columns for a {}-column schema",
                    table.table,
                    table.column_count,
                    table.schema.columns.len()
                )));
            }
            let row_count = usize::try_from(table.row_count).map_err(|_| {
                RelationalError::Admission(format!(
                    "canonical row root table {} row count {} exceeds platform capacity",
                    table.table, table.row_count
                ))
            })?;
            total_rows = total_rows.checked_add(row_count).ok_or_else(|| {
                RelationalError::Admission(
                    "canonical row root total row count exceeds platform capacity".to_string(),
                )
            })?;
            if schemas
                .insert(table.table.clone(), Arc::new(table.schema.clone()))
                .is_some()
            {
                return Err(RelationalError::Corruption(format!(
                    "canonical row root repeats table {}",
                    table.table
                )));
            }
            segments.insert(
                table.table.clone(),
                Arc::new(RelationalTableSegment::default()),
            );
            row_counts.insert(table.table.clone(), row_count);
        }
        Ok(Self {
            schemas,
            segments,
            overflow_segments: BTreeMap::new(),
            materialized_index_postings_resident: false,
            materialized_rows_resident: false,
            canonical_row_metadata_only: true,
            detached_row_counts: Arc::new(row_counts),
            detached_total_row_count: total_rows,
            detached_row_bytes: manifest.page_artifact.encoded_len,
        })
    }

    /// Advances the logical counts of a metadata-only state from an exact,
    /// source-fenced recovery artifact without materializing checkpoint rows.
    pub fn adopt_recovered_row_counts<'a>(
        &mut self,
        row_counts: impl IntoIterator<Item = (&'a str, u64)>,
    ) -> Result<(), RelationalError> {
        if self.materialized_rows_resident || !self.canonical_row_metadata_only {
            return Err(RelationalError::Corruption(
                "recovered row counts require a canonical metadata-only relational state"
                    .to_string(),
            ));
        }
        let mut counts = BTreeMap::new();
        let mut total = 0usize;
        for (table, row_count) in row_counts {
            if !self.schemas.contains_key(table) {
                return Err(RelationalError::Corruption(format!(
                    "recovered row counts contain unknown table {table}"
                )));
            }
            let row_count = usize::try_from(row_count).map_err(|_| {
                RelationalError::Admission(format!(
                    "recovered row count {row_count} for table {table} exceeds platform capacity"
                ))
            })?;
            total = total.checked_add(row_count).ok_or_else(|| {
                RelationalError::Admission(
                    "recovered total row count exceeds platform capacity".to_string(),
                )
            })?;
            if counts.insert(table.to_string(), row_count).is_some() {
                return Err(RelationalError::Corruption(format!(
                    "recovered row counts repeat table {table}"
                )));
            }
        }
        if counts.len() != self.schemas.len()
            || self.schemas.keys().any(|table| !counts.contains_key(table))
        {
            return Err(RelationalError::Corruption(
                "recovered row counts do not cover the canonical schema set".to_string(),
            ));
        }
        self.detached_total_row_count = total;
        self.detached_row_counts = Arc::new(counts);
        Ok(())
    }

    pub fn require_materialized_rows(&self, context: &str) -> Result<(), RelationalError> {
        if self.materialized_rows_resident {
            return Ok(());
        }
        Err(RelationalError::Admission(format!(
            "{context} requires materialized relational rows; this out-of-core state is served by canonical row pages"
        )))
    }

    /// Drops the transitional in-memory posting maps while preserving rows,
    /// schema, and overflow values. Ordinary materialized mutations fail
    /// closed afterward; callers must provide the authoritative index path.
    pub fn omit_materialized_index_postings(&mut self) {
        if !self.materialized_index_postings_resident {
            return;
        }
        for segment in self.segments.values_mut() {
            Arc::make_mut(segment).indexes.clear();
        }
        self.materialized_index_postings_resident = false;
    }

    /// Stages one canonical transaction and derives every bounded live-view
    /// capture from the same before/after state.
    ///
    /// The primary-key capture contains identities only. It is safe to persist
    /// beside the logical transaction without making any derived projection
    /// payload part of canonical storage.
    #[allow(clippy::too_many_arguments)]
    pub fn stage_transaction_with_primary_key_changes(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
        index_capture_limits: Option<RelationalIndexChangeCaptureLimits>,
        row_capture_limits: Option<RelationalRowChangeCaptureLimits>,
        primary_key_capture_limits: RelationalPrimaryKeyChangeCaptureLimits,
        constraint_index: Option<&dyn RelationalConstraintIndex>,
    ) -> Result<RelationalTransactionStageResult, RelationalError> {
        self.require_materialized_rows("relational transaction")?;
        admit_transaction(&transaction, limits)?;
        let index_mode = if constraint_index.is_some() {
            if index_capture_limits.is_none() {
                return Err(RelationalError::Corruption(
                    "authoritative relational constraints require index change capture".to_string(),
                ));
            }
            if transaction.changes_index_schema() {
                return Err(RelationalError::Admission(
                    "authoritative relational indexes reject schema-changing transactions until new canonical row and index generations are published"
                        .to_string(),
                ));
            }
            TransactionIndexMode::Authoritative
        } else {
            TransactionIndexMode::Materialized
        };
        let TransactionApplyResult {
            state,
            index_capture,
            row_capture,
            replay_access,
            primary_key_changes,
            mutation_outcomes,
        } = apply_transaction_inner(
            self,
            transaction,
            limits,
            overflow_config,
            TransactionApplyOptions {
                index_capture_limits,
                row_capture_limits,
                replay_access_limits: row_capture_limits,
                primary_key_capture_limits: Some(primary_key_capture_limits),
                constraint_index,
                index_mode,
                capture_mutation_outcomes: true,
            },
        )?;
        Ok(RelationalTransactionStageResult {
            state,
            index_capture,
            row_capture,
            replay_access,
            primary_key_changes: primary_key_changes
                .expect("primary-key change capture was requested for this transaction"),
            mutation_outcomes,
        })
    }

    pub fn stage_transaction_with_outcomes(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
    ) -> Result<(Self, Vec<RelationalMutationOutcome>), RelationalError> {
        self.require_materialized_rows("relational transaction")?;
        admit_transaction(&transaction, limits)?;
        let result = apply_transaction_inner(
            self,
            transaction,
            limits,
            overflow_config,
            TransactionApplyOptions {
                capture_mutation_outcomes: true,
                ..TransactionApplyOptions::materialized()
            },
        )?;
        Ok((result.state, result.mutation_outcomes))
    }

    pub fn stage_transaction(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
    ) -> Result<Self, RelationalError> {
        self.require_materialized_rows("relational transaction")?;
        admit_transaction(&transaction, limits)?;
        apply_transaction(self, transaction, limits, overflow_config)
    }

    pub fn stage_transaction_with_index_changes(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
        capture_limits: RelationalIndexChangeCaptureLimits,
    ) -> Result<(Self, RelationalIndexChangeCapture), RelationalError> {
        self.require_materialized_rows("relational transaction")?;
        admit_transaction(&transaction, limits)?;
        apply_transaction_with_index_changes(
            self,
            transaction,
            limits,
            overflow_config,
            capture_limits,
            None,
            TransactionIndexMode::Materialized,
        )
    }

    pub fn stage_transaction_with_index_and_row_changes(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
        index_capture_limits: RelationalIndexChangeCaptureLimits,
        row_capture_limits: RelationalRowChangeCaptureLimits,
    ) -> Result<
        (
            Self,
            RelationalIndexChangeCapture,
            RelationalRowChangeCapture,
        ),
        RelationalError,
    > {
        self.require_materialized_rows("relational transaction")?;
        admit_transaction(&transaction, limits)?;
        let TransactionApplyResult {
            state,
            index_capture,
            row_capture,
            ..
        } = apply_transaction_inner(
            self,
            transaction,
            limits,
            overflow_config,
            TransactionApplyOptions {
                index_capture_limits: Some(index_capture_limits),
                row_capture_limits: Some(row_capture_limits),
                ..TransactionApplyOptions::materialized()
            },
        )?;
        Ok((
            state,
            index_capture.expect("index change capture was requested for this transaction"),
            row_capture.expect("row change capture was requested for this transaction"),
        ))
    }

    pub fn stage_transaction_with_index_row_and_replay_access(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
        index_capture_limits: RelationalIndexChangeCaptureLimits,
        row_capture_limits: RelationalRowChangeCaptureLimits,
    ) -> Result<
        (
            Self,
            RelationalIndexChangeCapture,
            RelationalRowChangeCapture,
            RelationalReplayAccessSet,
        ),
        RelationalError,
    > {
        self.require_materialized_rows("relational transaction")?;
        admit_transaction(&transaction, limits)?;
        let TransactionApplyResult {
            state,
            index_capture,
            row_capture,
            replay_access,
            ..
        } = apply_transaction_inner(
            self,
            transaction,
            limits,
            overflow_config,
            TransactionApplyOptions {
                index_capture_limits: Some(index_capture_limits),
                row_capture_limits: Some(row_capture_limits),
                replay_access_limits: Some(row_capture_limits),
                ..TransactionApplyOptions::materialized()
            },
        )?;
        Ok((
            state,
            index_capture.expect("index change capture was requested for this transaction"),
            row_capture.expect("row change capture was requested for this transaction"),
            replay_access.expect("replay access capture was requested for this transaction"),
        ))
    }

    pub fn stage_transaction_with_row_changes(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
        capture_limits: RelationalRowChangeCaptureLimits,
    ) -> Result<(Self, RelationalRowChangeCapture), RelationalError> {
        self.require_materialized_rows("relational transaction")?;
        admit_transaction(&transaction, limits)?;
        let TransactionApplyResult {
            state,
            row_capture: capture,
            ..
        } = apply_transaction_inner(
            self,
            transaction,
            limits,
            overflow_config,
            TransactionApplyOptions {
                row_capture_limits: Some(capture_limits),
                ..TransactionApplyOptions::materialized()
            },
        )?;
        Ok((
            state,
            capture.expect("row change capture was requested for this transaction"),
        ))
    }

    pub fn stage_transaction_with_row_changes_and_replay_access(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
        capture_limits: RelationalRowChangeCaptureLimits,
    ) -> Result<(Self, RelationalRowChangeCapture, RelationalReplayAccessSet), RelationalError>
    {
        self.require_materialized_rows("relational transaction")?;
        admit_transaction(&transaction, limits)?;
        let TransactionApplyResult {
            state,
            row_capture: capture,
            replay_access,
            ..
        } = apply_transaction_inner(
            self,
            transaction,
            limits,
            overflow_config,
            TransactionApplyOptions {
                row_capture_limits: Some(capture_limits),
                replay_access_limits: Some(capture_limits),
                ..TransactionApplyOptions::materialized()
            },
        )?;
        Ok((
            state,
            capture.expect("row change capture was requested for this transaction"),
            replay_access.expect("replay access capture was requested for this transaction"),
        ))
    }

    /// Replays an already-durable authoritative transaction while deriving
    /// its recovery-index delta directly from before/after rows.
    ///
    /// Constraint checks happened before the WAL became durable. Recovery
    /// must not recreate the transitional materialized-posting oracle.
    pub fn stage_transaction_for_authoritative_recovery(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
        capture_limits: RelationalIndexChangeCaptureLimits,
    ) -> Result<(Self, RelationalIndexChangeCapture), RelationalError> {
        self.require_materialized_rows("relational recovery")?;
        admit_transaction(&transaction, limits)?;
        if transaction.changes_index_schema() {
            return Err(RelationalError::Admission(
                "authoritative relational index recovery rejects schema-changing WAL until a new canonical index generation is published"
                    .to_string(),
            ));
        }
        apply_transaction_with_index_changes(
            self,
            transaction,
            limits,
            overflow_config,
            capture_limits,
            None,
            TransactionIndexMode::AuthoritativeRecovery,
        )
    }

    pub fn stage_transaction_for_authoritative_recovery_with_row_changes(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
        index_capture_limits: RelationalIndexChangeCaptureLimits,
        row_capture_limits: RelationalRowChangeCaptureLimits,
    ) -> Result<
        (
            Self,
            RelationalIndexChangeCapture,
            RelationalRowChangeCapture,
        ),
        RelationalError,
    > {
        self.require_materialized_rows("relational recovery")?;
        admit_transaction(&transaction, limits)?;
        if transaction.changes_index_schema() {
            return Err(RelationalError::Admission(
                "authoritative relational recovery rejects schema-changing WAL until new canonical row and index generations are published"
                    .to_string(),
            ));
        }
        let TransactionApplyResult {
            state,
            index_capture,
            row_capture,
            ..
        } = apply_transaction_inner(
            self,
            transaction,
            limits,
            overflow_config,
            TransactionApplyOptions {
                index_capture_limits: Some(index_capture_limits),
                row_capture_limits: Some(row_capture_limits),
                replay_access_limits: None,
                primary_key_capture_limits: None,
                constraint_index: None,
                index_mode: TransactionIndexMode::AuthoritativeRecovery,
                capture_mutation_outcomes: false,
            },
        )?;
        Ok((
            state,
            index_capture.expect("index change capture was requested for recovery"),
            row_capture.expect("row change capture was requested for recovery"),
        ))
    }

    /// Stages one transaction against a constraint index pinned to this
    /// state's visibility epoch. This method does not publish the state or
    /// activate persistent indexes; the caller still owns the WAL boundary.
    pub fn stage_transaction_with_authoritative_index(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
        capture_limits: RelationalIndexChangeCaptureLimits,
        constraint_index: &dyn RelationalConstraintIndex,
    ) -> Result<(Self, RelationalIndexChangeCapture), RelationalError> {
        self.require_materialized_rows("relational transaction")?;
        admit_transaction(&transaction, limits)?;
        if transaction.changes_index_schema() {
            return Err(RelationalError::Admission(
                "authoritative relational indexes reject schema-changing transactions until a new canonical index generation is published"
                    .to_string(),
            ));
        }
        apply_transaction_with_index_changes(
            self,
            transaction,
            limits,
            overflow_config,
            capture_limits,
            Some(constraint_index),
            TransactionIndexMode::Authoritative,
        )
    }

    pub fn stage_transaction_with_authoritative_index_and_outcomes(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
        capture_limits: RelationalIndexChangeCaptureLimits,
        constraint_index: &dyn RelationalConstraintIndex,
    ) -> Result<
        (
            Self,
            RelationalIndexChangeCapture,
            Vec<RelationalMutationOutcome>,
        ),
        RelationalError,
    > {
        self.require_materialized_rows("relational transaction")?;
        admit_transaction(&transaction, limits)?;
        if transaction.changes_index_schema() {
            return Err(RelationalError::Admission(
                "authoritative relational indexes reject schema-changing transactions until a new canonical index generation is published"
                    .to_string(),
            ));
        }
        let TransactionApplyResult {
            state,
            index_capture,
            mutation_outcomes,
            ..
        } = apply_transaction_inner(
            self,
            transaction,
            limits,
            overflow_config,
            TransactionApplyOptions {
                index_capture_limits: Some(capture_limits),
                constraint_index: Some(constraint_index),
                index_mode: TransactionIndexMode::Authoritative,
                capture_mutation_outcomes: true,
                ..TransactionApplyOptions::materialized()
            },
        )?;
        Ok((
            state,
            index_capture.expect("index change capture was requested for this transaction"),
            mutation_outcomes,
        ))
    }

    /// Stages one transaction against a pinned constraint index while deriving
    /// both immutable index and row live-view batches from the same before/after
    /// state. The caller owns the WAL and publication boundary.
    pub fn stage_transaction_with_authoritative_index_and_row_changes(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
        index_capture_limits: RelationalIndexChangeCaptureLimits,
        row_capture_limits: RelationalRowChangeCaptureLimits,
        constraint_index: &dyn RelationalConstraintIndex,
    ) -> Result<
        (
            Self,
            RelationalIndexChangeCapture,
            RelationalRowChangeCapture,
        ),
        RelationalError,
    > {
        self.require_materialized_rows("relational transaction")?;
        admit_transaction(&transaction, limits)?;
        if transaction.changes_index_schema() {
            return Err(RelationalError::Admission(
                "authoritative relational indexes reject schema-changing transactions until new canonical row and index generations are published"
                    .to_string(),
            ));
        }
        let TransactionApplyResult {
            state,
            index_capture,
            row_capture,
            ..
        } = apply_transaction_inner(
            self,
            transaction,
            limits,
            overflow_config,
            TransactionApplyOptions {
                index_capture_limits: Some(index_capture_limits),
                row_capture_limits: Some(row_capture_limits),
                replay_access_limits: None,
                primary_key_capture_limits: None,
                constraint_index: Some(constraint_index),
                index_mode: TransactionIndexMode::Authoritative,
                capture_mutation_outcomes: false,
            },
        )?;
        Ok((
            state,
            index_capture.expect("index change capture was requested for this transaction"),
            row_capture.expect("row change capture was requested for this transaction"),
        ))
    }

    /// Stages authoritative DML and captures the exact bounded primary-key
    /// access set that must accompany the logical WAL transaction.
    pub fn stage_transaction_with_authoritative_replay_access(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
        index_capture_limits: RelationalIndexChangeCaptureLimits,
        row_capture_limits: RelationalRowChangeCaptureLimits,
        constraint_index: &dyn RelationalConstraintIndex,
    ) -> Result<
        (
            Self,
            RelationalIndexChangeCapture,
            RelationalRowChangeCapture,
            RelationalReplayAccessSet,
        ),
        RelationalError,
    > {
        self.require_materialized_rows("relational transaction")?;
        admit_transaction(&transaction, limits)?;
        if transaction.changes_index_schema() {
            return Err(RelationalError::Admission(
                "authoritative relational replay access rejects schema-changing transactions until new canonical row and index generations are published"
                    .to_string(),
            ));
        }
        let TransactionApplyResult {
            state,
            index_capture,
            row_capture,
            replay_access,
            ..
        } = apply_transaction_inner(
            self,
            transaction,
            limits,
            overflow_config,
            TransactionApplyOptions {
                index_capture_limits: Some(index_capture_limits),
                row_capture_limits: Some(row_capture_limits),
                replay_access_limits: Some(row_capture_limits),
                primary_key_capture_limits: None,
                constraint_index: Some(constraint_index),
                index_mode: TransactionIndexMode::Authoritative,
                capture_mutation_outcomes: false,
            },
        )?;
        Ok((
            state,
            index_capture.expect("index change capture was requested for this transaction"),
            row_capture.expect("row change capture was requested for this transaction"),
            replay_access.expect("replay access capture was requested for this transaction"),
        ))
    }

    pub fn stage_transaction_with_authoritative_replay_access_and_outcomes(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
        index_capture_limits: RelationalIndexChangeCaptureLimits,
        row_capture_limits: RelationalRowChangeCaptureLimits,
        constraint_index: &dyn RelationalConstraintIndex,
    ) -> Result<
        (
            Self,
            RelationalIndexChangeCapture,
            RelationalRowChangeCapture,
            RelationalReplayAccessSet,
            Vec<RelationalMutationOutcome>,
        ),
        RelationalError,
    > {
        self.require_materialized_rows("relational transaction")?;
        admit_transaction(&transaction, limits)?;
        if transaction.changes_index_schema() {
            return Err(RelationalError::Admission(
                "authoritative relational replay access rejects schema-changing transactions until new canonical row and index generations are published"
                    .to_string(),
            ));
        }
        let TransactionApplyResult {
            state,
            index_capture,
            row_capture,
            replay_access,
            mutation_outcomes,
            ..
        } = apply_transaction_inner(
            self,
            transaction,
            limits,
            overflow_config,
            TransactionApplyOptions {
                index_capture_limits: Some(index_capture_limits),
                row_capture_limits: Some(row_capture_limits),
                replay_access_limits: Some(row_capture_limits),
                primary_key_capture_limits: None,
                constraint_index: Some(constraint_index),
                index_mode: TransactionIndexMode::Authoritative,
                capture_mutation_outcomes: true,
            },
        )?;
        Ok((
            state,
            index_capture.expect("index change capture was requested for this transaction"),
            row_capture.expect("row change capture was requested for this transaction"),
            replay_access.expect("replay access capture was requested for this transaction"),
            mutation_outcomes,
        ))
    }

    /// Replays authoritative DML and rejects any drift from the access set
    /// authenticated by the WAL record.
    pub fn stage_transaction_for_authoritative_recovery_with_replay_access(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
        index_capture_limits: RelationalIndexChangeCaptureLimits,
        row_capture_limits: RelationalRowChangeCaptureLimits,
        expected_replay_access: &RelationalReplayAccessSet,
    ) -> Result<
        (
            Self,
            RelationalIndexChangeCapture,
            RelationalRowChangeCapture,
        ),
        RelationalError,
    > {
        self.require_materialized_rows("relational recovery")?;
        admit_transaction(&transaction, limits)?;
        if transaction.changes_index_schema() {
            return Err(RelationalError::Admission(
                "authoritative relational recovery rejects schema-changing WAL until new canonical row and index generations are published"
                    .to_string(),
            ));
        }
        let TransactionApplyResult {
            state,
            index_capture,
            row_capture,
            replay_access,
            ..
        } = apply_transaction_inner(
            self,
            transaction,
            limits,
            overflow_config,
            TransactionApplyOptions {
                index_capture_limits: Some(index_capture_limits),
                row_capture_limits: Some(row_capture_limits),
                replay_access_limits: Some(row_capture_limits),
                primary_key_capture_limits: None,
                constraint_index: None,
                index_mode: TransactionIndexMode::AuthoritativeRecovery,
                capture_mutation_outcomes: false,
            },
        )?;
        let replay_access =
            replay_access.expect("replay access capture was requested for recovery");
        if &replay_access != expected_replay_access {
            return Err(RelationalError::Corruption(
                "relational WAL replay access set does not match the recovered transaction"
                    .to_string(),
            ));
        }
        Ok((
            state,
            index_capture.expect("index change capture was requested for recovery"),
            row_capture.expect("row change capture was requested for recovery"),
        ))
    }

    /// Replays one schema-stable WAL transaction in a bounded materialized
    /// workspace containing exactly its authenticated access set.
    ///
    /// The receiver remains canonical metadata-only. The returned state
    /// advances only exact logical row counts and retains content-addressed
    /// overflow segments created by this transaction; checkpoint rows are
    /// never attached to it. Stale overflow segments may remain until the next
    /// checkpoint because removing them would require scanning prior recovery
    /// rows and defeat the bounded workspace.
    pub fn stage_sparse_transaction_for_authoritative_recovery_with_replay_access(
        &self,
        stage: RelationalSparseRecoveryStage<'_>,
    ) -> Result<
        (
            Self,
            RelationalIndexChangeCapture,
            RelationalRowChangeCapture,
        ),
        RelationalError,
    > {
        let RelationalSparseRecoveryStage {
            transaction,
            hydrated_access,
            mutation_limits,
            overflow_config,
            index_capture_limits,
            row_capture_limits,
            expected_replay_access,
        } = stage;
        let workspace = self.sparse_recovery_workspace(
            hydrated_access,
            expected_replay_access,
            row_capture_limits,
        )?;
        let (staged, index_capture, row_capture) = workspace
            .stage_transaction_for_authoritative_recovery_with_replay_access(
                transaction,
                mutation_limits,
                overflow_config,
                index_capture_limits,
                row_capture_limits,
                expected_replay_access,
            )?;
        let metadata = self.merge_sparse_recovery_workspace(&workspace, &staged)?;
        Ok((metadata, index_capture, row_capture))
    }

    /// Derives the canonical reads required to begin bounded live staging.
    ///
    /// This plan is intentionally incomplete with respect to constraints whose
    /// keys depend on existing row values. After these reads are hydrated, the
    /// caller must use [`Self::prepare_sparse_transaction_for_authoritative_live`]
    /// and close every returned replay access and constraint probe before WAL.
    pub fn plan_sparse_transaction_hydration(
        &self,
        transaction: &RelationalTransaction,
    ) -> Result<RelationalSparseMutationHydrationPlan, RelationalError> {
        if transaction.changes_index_schema() {
            return Err(RelationalError::Admission(
                "sparse relational live staging rejects schema-changing transactions until a new canonical row and index generation is published"
                    .to_string(),
            ));
        }
        let mut point_access = BTreeSet::new();
        let mut monotonic_appends = Vec::new();
        let mut scan_tables = BTreeSet::new();
        let mut index_probes = BTreeSet::new();
        let mut append_only_tables = BTreeMap::<&str, bool>::new();
        for write in &transaction.writes {
            let (table, eligible) = match write {
                RelationalWrite::Insert { table, mode, .. } => {
                    (table.as_str(), *mode == RelationalInsertMode::Error)
                }
                RelationalWrite::Upsert { table, .. }
                | RelationalWrite::DeleteByPrimaryKey { table, .. }
                | RelationalWrite::DeleteWhere { table, .. }
                | RelationalWrite::UpdateWhere { table, .. }
                | RelationalWrite::AddColumn { table, .. }
                | RelationalWrite::CreateIndex { table, .. } => (table.as_str(), false),
                RelationalWrite::CreateTable(schema) => (schema.name.as_str(), false),
            };
            append_only_tables
                .entry(table)
                .and_modify(|current| *current &= eligible)
                .or_insert(eligible);
        }
        let mut append_candidates = BTreeMap::<(String, RelationalKey), Vec<RelationalKey>>::new();
        for write in &transaction.writes {
            match write {
                RelationalWrite::Insert { table, rows, .. } => {
                    let schema = self.sparse_plan_table_schema(table)?;
                    let primary_key = column_positions(schema, &schema.primary_key)?;
                    for row in rows {
                        validate_row(schema, row)?;
                        let key = row_key(row, &primary_key);
                        let has_constraint_probes = !schema.unique_constraints.is_empty()
                            || !schema.foreign_keys.is_empty();
                        if append_only_tables.get(table.as_str()) == Some(&true)
                            && !has_constraint_probes
                        {
                            let prefix =
                                RelationalKey(key.0[..key.0.len().saturating_sub(1)].to_vec());
                            append_candidates
                                .entry((table.clone(), prefix))
                                .or_default()
                                .push(key);
                        } else {
                            point_access.insert(RelationalReplayAccess {
                                table: table.clone(),
                                primary_key: key,
                            });
                        }
                    }
                }
                RelationalWrite::Upsert {
                    table,
                    rows,
                    conflict_columns,
                    ..
                } => {
                    let schema = self.sparse_plan_table_schema(table)?;
                    let primary_key = column_positions(schema, &schema.primary_key)?;
                    let conflict_positions = column_positions(schema, conflict_columns)?;
                    let conflict_definition = schema
                        .unique_index_definition(conflict_columns)
                        .ok_or_else(|| {
                            RelationalError::Schema(format!(
                                "UPSERT conflict target on table {table} must name a primary or unique key"
                            ))
                        })?;
                    for row in rows {
                        validate_row(schema, row)?;
                        point_access.insert(RelationalReplayAccess {
                            table: table.clone(),
                            primary_key: row_key(row, &primary_key),
                        });
                        let conflict_key = row_key(row, &conflict_positions);
                        if !key_contains_null(&conflict_key) {
                            index_probes.insert(RelationalSparseIndexProbe {
                                table: table.clone(),
                                index: conflict_definition.name.clone(),
                                index_key: conflict_key,
                            });
                        }
                    }
                }
                RelationalWrite::DeleteByPrimaryKey { table, keys } => {
                    self.sparse_plan_table_schema(table)?;
                    point_access.extend(keys.iter().cloned().map(|primary_key| {
                        RelationalReplayAccess {
                            table: table.clone(),
                            primary_key,
                        }
                    }));
                }
                RelationalWrite::DeleteWhere { table, .. }
                | RelationalWrite::UpdateWhere { table, .. } => {
                    self.sparse_plan_table_schema(table)?;
                    scan_tables.insert(table.clone());
                }
                RelationalWrite::CreateTable(_)
                | RelationalWrite::AddColumn { .. }
                | RelationalWrite::CreateIndex { .. } => {
                    unreachable!("schema-changing transactions were rejected above")
                }
            }
        }
        for ((table, partition_prefix), primary_keys) in append_candidates {
            let strictly_increasing = primary_keys.windows(2).all(|pair| pair[0] < pair[1]);
            if primary_keys.len() > 1 && strictly_increasing {
                monotonic_appends.push(RelationalMonotonicAppendHydration {
                    table,
                    partition_prefix,
                    primary_keys,
                });
            } else {
                point_access.extend(primary_keys.into_iter().map(|primary_key| {
                    RelationalReplayAccess {
                        table: table.clone(),
                        primary_key,
                    }
                }));
            }
        }
        point_access.retain(|access| !scan_tables.contains(&access.table));
        Ok(RelationalSparseMutationHydrationPlan {
            point_access: point_access.into_iter().collect(),
            monotonic_appends,
            scan_tables: scan_tables.into_iter().collect(),
            index_probes: index_probes.into_iter().collect(),
        })
    }

    /// Replays one schema-stable transaction without constraint publication to
    /// discover its exact WAL access set and the persistent postings required
    /// by the final authoritative validation pass.
    ///
    /// The caller must hydrate newly discovered replay keys and every posting
    /// returned by the listed probes, then repeat preparation until the input
    /// set is closed. No detached counts or canonical state change here.
    pub fn prepare_sparse_transaction_for_authoritative_live(
        &self,
        stage: RelationalSparseLivePreparationStage,
    ) -> Result<RelationalSparseLivePreparation, RelationalError> {
        let RelationalSparseLivePreparationStage {
            transaction,
            hydrated_workspace,
            mutation_limits,
            overflow_config,
            index_capture_limits,
            row_capture_limits,
        } = stage;
        self.require_sparse_workspace_source("sparse relational live preparation")?;
        let (workspace, _) = self.sparse_workspace(
            hydrated_workspace,
            row_capture_limits,
            "sparse relational live preparation",
        )?;
        let (staged, index_capture, row_capture, replay_access) = workspace
            .prepare_transaction_for_authoritative_live_with_replay_access(
                transaction,
                mutation_limits,
                overflow_config,
                index_capture_limits,
                row_capture_limits,
            )?;
        let constraint_probes =
            staged.sparse_authoritative_constraint_probes(&index_capture, &row_capture)?;
        Ok(RelationalSparseLivePreparation {
            replay_access,
            constraint_probes,
        })
    }

    /// Stages one live authoritative transaction without attaching checkpoint
    /// rows to the canonical metadata-only state.
    ///
    /// The supplied workspace may be a strict superset of the transaction's
    /// replay-access set because unchanged unique-conflict and foreign-key
    /// target rows are required for constraint validation. The resulting
    /// replay-access set must be fully contained in that explicit workspace.
    /// The caller still owns the WAL and row/index publication boundary.
    pub fn stage_sparse_transaction_with_authoritative_replay_access(
        &self,
        stage: RelationalSparseLiveStage<'_>,
    ) -> Result<
        (
            Self,
            RelationalIndexChangeCapture,
            RelationalRowChangeCapture,
            RelationalReplayAccessSet,
        ),
        RelationalError,
    > {
        let RelationalSparseLiveStage {
            transaction,
            hydrated_workspace,
            mutation_limits,
            overflow_config,
            index_capture_limits,
            row_capture_limits,
            constraint_index,
        } = stage;
        self.require_sparse_workspace_source("sparse relational live staging")?;
        let (workspace, supplied_access) = self.sparse_workspace(
            hydrated_workspace,
            row_capture_limits,
            "sparse relational live staging",
        )?;
        let (staged, index_capture, row_capture, replay_access) = workspace
            .stage_transaction_with_authoritative_replay_access(
                transaction,
                mutation_limits,
                overflow_config,
                index_capture_limits,
                row_capture_limits,
                constraint_index,
            )?;
        if let Some(missing) = replay_access
            .entries
            .iter()
            .find(|entry| supplied_access.binary_search(entry).is_err())
        {
            return Err(RelationalError::Corruption(format!(
                "sparse relational live staging did not hydrate replay access {:?} in table {}",
                missing.primary_key, missing.table
            )));
        }
        let metadata = self.merge_sparse_recovery_workspace(&workspace, &staged)?;
        Ok((metadata, index_capture, row_capture, replay_access))
    }

    pub fn stage_sparse_transaction_with_authoritative_replay_access_and_outcomes(
        &self,
        stage: RelationalSparseLiveStage<'_>,
    ) -> Result<
        (
            Self,
            RelationalIndexChangeCapture,
            RelationalRowChangeCapture,
            RelationalReplayAccessSet,
            Vec<RelationalMutationOutcome>,
        ),
        RelationalError,
    > {
        let RelationalSparseLiveStage {
            transaction,
            hydrated_workspace,
            mutation_limits,
            overflow_config,
            index_capture_limits,
            row_capture_limits,
            constraint_index,
        } = stage;
        self.require_sparse_workspace_source("sparse relational live staging")?;
        let (workspace, supplied_access) = self.sparse_workspace(
            hydrated_workspace,
            row_capture_limits,
            "sparse relational live staging",
        )?;
        let (staged, index_capture, row_capture, replay_access, mutation_outcomes) = workspace
            .stage_transaction_with_authoritative_replay_access_and_outcomes(
                transaction,
                mutation_limits,
                overflow_config,
                index_capture_limits,
                row_capture_limits,
                constraint_index,
            )?;
        if let Some(missing) = replay_access
            .entries
            .iter()
            .find(|entry| supplied_access.binary_search(entry).is_err())
        {
            return Err(RelationalError::Corruption(format!(
                "sparse relational live staging did not hydrate replay access {:?} in table {}",
                missing.primary_key, missing.table
            )));
        }
        let metadata = self.merge_sparse_recovery_workspace(&workspace, &staged)?;
        Ok((
            metadata,
            index_capture,
            row_capture,
            replay_access,
            mutation_outcomes,
        ))
    }

    pub fn stage_sparse_transaction_with_primary_key_changes(
        &self,
        stage: RelationalSparseLiveStage<'_>,
        primary_key_capture_limits: RelationalPrimaryKeyChangeCaptureLimits,
    ) -> Result<RelationalTransactionStageResult, RelationalError> {
        let RelationalSparseLiveStage {
            transaction,
            hydrated_workspace,
            mutation_limits,
            overflow_config,
            index_capture_limits,
            row_capture_limits,
            constraint_index,
        } = stage;
        self.require_sparse_workspace_source("sparse relational live staging")?;
        let (workspace, supplied_access) = self.sparse_workspace(
            hydrated_workspace,
            row_capture_limits,
            "sparse relational live staging",
        )?;
        let RelationalTransactionStageResult {
            state: staged,
            index_capture,
            row_capture,
            replay_access,
            primary_key_changes,
            mutation_outcomes,
        } = workspace.stage_transaction_with_primary_key_changes(
            transaction,
            mutation_limits,
            overflow_config,
            Some(index_capture_limits),
            Some(row_capture_limits),
            primary_key_capture_limits,
            Some(constraint_index),
        )?;
        let replay_access = replay_access
            .expect("replay access capture was requested for sparse relational staging");
        if let Some(missing) = replay_access
            .entries
            .iter()
            .find(|entry| supplied_access.binary_search(entry).is_err())
        {
            return Err(RelationalError::Corruption(format!(
                "sparse relational live staging did not hydrate replay access {:?} in table {}",
                missing.primary_key, missing.table
            )));
        }
        let metadata = self.merge_sparse_recovery_workspace(&workspace, &staged)?;
        Ok(RelationalTransactionStageResult {
            state: metadata,
            index_capture,
            row_capture,
            replay_access: Some(replay_access),
            primary_key_changes,
            mutation_outcomes,
        })
    }

    fn sparse_recovery_workspace(
        &self,
        hydrated_access: Vec<RelationalSparseRecoveryRow>,
        expected_replay_access: &RelationalReplayAccessSet,
        limits: RelationalRowChangeCaptureLimits,
    ) -> Result<Self, RelationalError> {
        self.require_sparse_workspace_source("sparse relational recovery")?;
        if hydrated_access.len() != expected_replay_access.entries.len()
            || hydrated_access
                .iter()
                .zip(&expected_replay_access.entries)
                .any(|(hydrated, expected)| {
                    hydrated.table != expected.table || hydrated.primary_key != expected.primary_key
                })
        {
            return Err(RelationalError::Corruption(
                "sparse relational recovery hydration does not exactly cover the authenticated WAL access set"
                    .to_string(),
            ));
        }
        self.sparse_workspace(hydrated_access, limits, "sparse relational recovery")
            .map(|(workspace, _)| workspace)
    }

    fn sparse_workspace(
        &self,
        hydrated_access: Vec<RelationalSparseRecoveryRow>,
        limits: RelationalRowChangeCaptureLimits,
        context: &'static str,
    ) -> Result<(Self, Vec<RelationalReplayAccess>), RelationalError> {
        if hydrated_access.len() > limits.max_entries.get() {
            return Err(RelationalError::Admission(format!(
                "{context} workspace contains {} entries, exceeding limit {}",
                hydrated_access.len(),
                limits.max_entries
            )));
        }

        let mut workspace_bytes = 0usize;
        let mut supplied_access = Vec::with_capacity(hydrated_access.len());
        let mut previous_access = None;
        let mut segments = self
            .schemas
            .keys()
            .map(|table| (table.clone(), Arc::new(RelationalTableSegment::default())))
            .collect::<BTreeMap<_, _>>();
        for hydrated in hydrated_access {
            let access = RelationalReplayAccess {
                table: hydrated.table.clone(),
                primary_key: hydrated.primary_key.clone(),
            };
            if previous_access
                .as_ref()
                .is_some_and(|previous| previous >= &access)
            {
                return Err(RelationalError::Corruption(format!(
                    "{context} hydration is not strictly ordered or repeats a key"
                )));
            }
            let entry_bytes =
                relational_sparse_recovery_entry_bytes(&hydrated).ok_or_else(|| {
                    RelationalError::Admission(format!("{context} workspace byte count overflow"))
                })?;
            let access_bytes =
                relational_replay_access_resident_bytes(&access).ok_or_else(|| {
                    RelationalError::Admission(format!("{context} workspace byte count overflow"))
                })?;
            workspace_bytes = workspace_bytes
                .checked_add(entry_bytes)
                .and_then(|bytes| bytes.checked_add(access_bytes))
                .ok_or_else(|| {
                    RelationalError::Admission(format!("{context} workspace byte count overflow"))
                })?;
            if workspace_bytes > limits.max_bytes.get() {
                return Err(RelationalError::Admission(format!(
                    "{context} workspace requires {workspace_bytes} bytes, exceeding limit {}",
                    limits.max_bytes
                )));
            }
            previous_access = Some(access.clone());
            supplied_access.push(access);
            let schema = self.schemas.get(&hydrated.table).ok_or_else(|| {
                RelationalError::Corruption(format!(
                    "{context} references unknown table {}",
                    hydrated.table
                ))
            })?;
            let Some(row) = hydrated.row else {
                continue;
            };
            if row
                .values()
                .iter()
                .any(|value| matches!(value, RelationalValue::Overflow(_)))
            {
                return Err(RelationalError::Admission(format!(
                    "{context} row {:?} in table {} was not fully hydrated",
                    hydrated.primary_key, hydrated.table
                )));
            }
            validate_row(schema, &row)?;
            let primary_key_positions = column_positions(schema, &schema.primary_key)?;
            if row_key(&row, &primary_key_positions) != hydrated.primary_key {
                return Err(RelationalError::Corruption(format!(
                    "{context} row primary key differs from its hydrated key in table {}",
                    hydrated.table
                )));
            }
            let segment = Arc::make_mut(
                segments
                    .get_mut(&hydrated.table)
                    .expect("known sparse recovery table has a workspace segment"),
            );
            if segment.rows.insert(hydrated.primary_key, row).is_some() {
                return Err(RelationalError::Corruption(format!(
                    "{context} hydration repeats a key"
                )));
            }
        }

        Ok((
            Self {
                schemas: self.schemas.clone(),
                segments,
                overflow_segments: BTreeMap::new(),
                materialized_index_postings_resident: false,
                materialized_rows_resident: true,
                canonical_row_metadata_only: false,
                detached_row_counts: Arc::new(BTreeMap::new()),
                detached_total_row_count: 0,
                detached_row_bytes: 0,
            },
            supplied_access,
        ))
    }

    fn require_sparse_workspace_source(
        &self,
        context: &'static str,
    ) -> Result<(), RelationalError> {
        if self.materialized_rows_resident || !self.canonical_row_metadata_only {
            return Err(RelationalError::Admission(format!(
                "{context} requires canonical metadata-only state"
            )));
        }
        Ok(())
    }

    fn sparse_plan_table_schema(
        &self,
        table: &str,
    ) -> Result<&RelationalTableSchema, RelationalError> {
        self.table_schema(table)
            .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))
    }

    fn prepare_transaction_for_authoritative_live_with_replay_access(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
        index_capture_limits: RelationalIndexChangeCaptureLimits,
        row_capture_limits: RelationalRowChangeCaptureLimits,
    ) -> Result<
        (
            Self,
            RelationalIndexChangeCapture,
            RelationalRowChangeCapture,
            RelationalReplayAccessSet,
        ),
        RelationalError,
    > {
        self.require_materialized_rows("relational live preparation")?;
        admit_transaction(&transaction, limits)?;
        if transaction.changes_index_schema() {
            return Err(RelationalError::Admission(
                "authoritative relational live preparation rejects schema-changing transactions"
                    .to_string(),
            ));
        }
        let TransactionApplyResult {
            state,
            index_capture,
            row_capture,
            replay_access,
            ..
        } = apply_transaction_inner(
            self,
            transaction,
            limits,
            overflow_config,
            TransactionApplyOptions {
                index_capture_limits: Some(index_capture_limits),
                row_capture_limits: Some(row_capture_limits),
                replay_access_limits: Some(row_capture_limits),
                primary_key_capture_limits: None,
                constraint_index: None,
                index_mode: TransactionIndexMode::AuthoritativeRecovery,
                capture_mutation_outcomes: false,
            },
        )?;
        Ok((
            state,
            index_capture.expect("index capture was requested for live preparation"),
            row_capture.expect("row capture was requested for live preparation"),
            replay_access.expect("replay access was requested for live preparation"),
        ))
    }

    fn sparse_authoritative_constraint_probes(
        &self,
        index_capture: &RelationalIndexChangeCapture,
        row_capture: &RelationalRowChangeCapture,
    ) -> Result<Vec<RelationalSparseIndexProbe>, RelationalError> {
        let mut probes = BTreeSet::new();
        for change in index_capture.changes()? {
            let schema = self.sparse_plan_table_schema(&change.table)?;
            let definition = schema
                .required_index_definitions()
                .into_iter()
                .find(|definition| definition.name == change.index)
                .ok_or_else(|| {
                    RelationalError::Corruption(format!(
                        "relational live preparation captured unknown index {}.{}",
                        change.table, change.index
                    ))
                })?;
            if !definition.role.is_unique() {
                continue;
            }
            probes.insert(RelationalSparseIndexProbe {
                table: change.table.clone(),
                index: change.index.clone(),
                index_key: change.index_key.clone(),
            });
            if change.kind != RelationalIndexChangeKind::Delete {
                continue;
            }
            for referencing_schema in self.table_schemas() {
                for (ordinal, foreign_key) in referencing_schema.foreign_keys.iter().enumerate() {
                    if foreign_key.referenced_table == change.table
                        && foreign_key.referenced_columns == definition.columns
                    {
                        probes.insert(RelationalSparseIndexProbe {
                            table: referencing_schema.name.clone(),
                            index: relational_foreign_key_index_name(ordinal),
                            index_key: change.index_key.clone(),
                        });
                    }
                }
            }
        }

        let row_changes = match row_capture {
            RelationalRowChangeCapture::Captured { changes, .. } => changes,
            RelationalRowChangeCapture::RequiresCheckpoint { tables } => {
                return Err(RelationalError::Admission(format!(
                    "relational live preparation requires a checkpoint for tables {}",
                    tables.join(",")
                )));
            }
            RelationalRowChangeCapture::Invalidated { reason } => {
                return Err(RelationalError::Admission(format!(
                    "relational live preparation row capture is unavailable: {reason}"
                )));
            }
        };
        for change in row_changes {
            let Some(row) = &change.row else {
                continue;
            };
            let schema = self.sparse_plan_table_schema(&change.table)?;
            for foreign_key in &schema.foreign_keys {
                let positions = column_positions(schema, &foreign_key.columns)?;
                let index_key = row_key(row, &positions);
                if key_contains_null(&index_key) {
                    continue;
                }
                let referenced_schema = self
                    .table_schema(&foreign_key.referenced_table)
                    .ok_or_else(|| {
                        RelationalError::Schema(format!(
                            "foreign key references unknown table {}",
                            foreign_key.referenced_table
                        ))
                    })?;
                let definition = referenced_schema
                    .unique_index_definition(&foreign_key.referenced_columns)
                    .ok_or_else(|| {
                        RelationalError::Schema(format!(
                            "foreign key target {} is not unique",
                            foreign_key.referenced_table
                        ))
                    })?;
                probes.insert(RelationalSparseIndexProbe {
                    table: foreign_key.referenced_table.clone(),
                    index: definition.name,
                    index_key,
                });
            }
        }
        Ok(probes.into_iter().collect())
    }

    fn merge_sparse_recovery_workspace(
        &self,
        before: &RelationalState,
        after: &RelationalState,
    ) -> Result<Self, RelationalError> {
        if self.materialized_rows_resident
            || !self.canonical_row_metadata_only
            || !before.materialized_rows_resident
            || !after.materialized_rows_resident
            || before.schemas.keys().ne(self.schemas.keys())
            || after.schemas.keys().ne(self.schemas.keys())
        {
            return Err(RelationalError::Corruption(
                "invalid sparse relational recovery workspace merge".to_string(),
            ));
        }
        let mut metadata = self.clone();
        let counts = Arc::make_mut(&mut metadata.detached_row_counts);
        for table in self.schemas.keys() {
            let before_count = before
                .segments
                .get(table)
                .expect("sparse recovery workspace covers every table")
                .rows
                .len();
            let after_count = after
                .segments
                .get(table)
                .expect("staged sparse recovery workspace covers every table")
                .rows
                .len();
            let count = counts.get_mut(table).ok_or_else(|| {
                RelationalError::Corruption(format!(
                    "metadata-only relational state has no row count for table {table}"
                ))
            })?;
            if after_count >= before_count {
                *count = count
                    .checked_add(after_count - before_count)
                    .ok_or_else(|| {
                        RelationalError::Admission(format!(
                            "sparse relational recovery row count overflow for table {table}"
                        ))
                    })?;
            } else {
                *count = count
                    .checked_sub(before_count - after_count)
                    .ok_or_else(|| {
                        RelationalError::Corruption(format!(
                            "sparse relational recovery row count underflow for table {table}"
                        ))
                    })?;
            }
        }
        metadata.detached_total_row_count = counts.values().try_fold(0usize, |total, count| {
            total.checked_add(*count).ok_or_else(|| {
                RelationalError::Admission(
                    "sparse relational recovery total row count overflow".to_string(),
                )
            })
        })?;
        for (digest, segment) in &after.overflow_segments {
            metadata
                .overflow_segments
                .entry(*digest)
                .or_insert_with(|| segment.clone());
        }
        Ok(metadata)
    }

    pub fn table_schema(&self, table: &str) -> Option<&RelationalTableSchema> {
        self.schemas.get(table).map(Arc::as_ref)
    }

    pub fn table_schema_digest(
        &self,
        table: &str,
    ) -> Result<Option<skein_integrity::Sha256Digest>, RelationalError> {
        self.table_schema(table)
            .map(index_shadow::relational_schema_digest)
            .transpose()
            .map_err(|error| RelationalError::Corruption(error.to_string()))
    }

    pub fn table_schemas(&self) -> impl DoubleEndedIterator<Item = &RelationalTableSchema> {
        self.schemas.values().map(Arc::as_ref)
    }

    pub fn row(&self, table: &str, key: &RelationalKey) -> Option<&RelationalRow> {
        self.segments.get(table)?.rows.get(key)
    }

    pub fn row_entry(
        &self,
        table: &str,
        key: &RelationalKey,
    ) -> Option<(&RelationalKey, &RelationalRow)> {
        self.segments.get(table)?.rows.get_key_value(key)
    }

    pub fn rows(
        &self,
        table: &str,
    ) -> impl DoubleEndedIterator<Item = (&RelationalKey, &RelationalRow)> {
        self.segments
            .get(table)
            .into_iter()
            .flat_map(|segment| segment.rows.iter())
    }

    /// Packs the current relational snapshot into bounded immutable row pages
    /// for its first canonical checkpoint binding. Large payload bytes remain
    /// shared through `Arc` or overflow references; only row/page metadata is
    /// retained until generation publication.
    pub fn row_page_snapshot_deltas(
        &self,
        generation: u64,
        source_commit_epoch: u64,
        config: RelationalRowPagePublicationConfig,
    ) -> Result<Vec<RelationalRowPageTableDelta>, RelationalRowPageMutationError> {
        self.require_materialized_rows("relational row-page snapshot")
            .map_err(|error| RelationalRowPageMutationError::Admission(error.to_string()))?;
        if self.schemas.len() > config.max_tables.get() {
            return Err(RelationalRowPageMutationError::Admission(format!(
                "row-page snapshot contains {} tables, exceeding limit {}",
                self.schemas.len(),
                config.max_tables
            )));
        }
        let mut dirty_pages = 0usize;
        let mut dirty_bytes = 0u64;
        let slot_bytes = config.page_limits.max_page_bytes.get() as u64;
        self.table_schemas()
            .map(|schema| {
                let schema_digest = self
                    .table_schema_digest(&schema.name)
                    .map_err(|error| {
                        RelationalRowPageMutationError::Corrupt(error.to_string())
                    })?
                    .ok_or_else(|| {
                        RelationalRowPageMutationError::Corrupt(format!(
                            "table {} disappeared while packing its row-page snapshot",
                            schema.name
                        ))
                    })?;
                let mut pages = Vec::new();
                let mut emit = |page| {
                    let next_pages = dirty_pages.checked_add(1).ok_or_else(|| {
                        RelationalRowPageMutationError::Admission(
                            "row-page snapshot page count overflow".to_string(),
                        )
                    })?;
                    let next_bytes = dirty_bytes.checked_add(slot_bytes).ok_or_else(|| {
                        RelationalRowPageMutationError::Admission(
                            "row-page snapshot byte count overflow".to_string(),
                        )
                    })?;
                    if next_pages > config.max_dirty_pages.get()
                        || next_bytes > config.max_dirty_bytes.get()
                    {
                        return Err(RelationalRowPageMutationError::Admission(format!(
                            "row-page snapshot requires {next_pages} pages/{next_bytes} bytes, exceeding limits {}/{}",
                            config.max_dirty_pages, config.max_dirty_bytes
                        )));
                    }
                    pages.push(page);
                    dirty_pages = next_pages;
                    dirty_bytes = next_bytes;
                    Ok(())
                };
                let mut bootstrap = RelationalRowPageBootstrap::new(
                    generation,
                    source_commit_epoch,
                    schema_digest,
                    schema.columns.len(),
                    NonZeroU64::new(1).expect("initial row-page id is non-zero"),
                    config,
                )?;
                for (primary_key, row) in self.rows(&schema.name) {
                    bootstrap.push(
                        RelationalRowPageEntry {
                            primary_key: primary_key.clone(),
                            row: row.clone(),
                        },
                        &mut emit,
                    )?;
                }
                let report = bootstrap.finish(&mut emit)?;
                Ok(RelationalRowPageTableDelta {
                    table: schema.name.clone(),
                    schema: Some(schema.clone()),
                    schema_digest,
                    column_count: NonZeroU32::new(
                        u32::try_from(schema.columns.len()).map_err(|_| {
                            RelationalRowPageMutationError::Admission(format!(
                                "table {} column count does not fit u32",
                                schema.name
                            ))
                        })?,
                    )
                    .ok_or_else(|| {
                        RelationalRowPageMutationError::Admission(format!(
                            "table {} has no columns",
                            schema.name
                        ))
                    })?,
                    next_page_id: report.next_page_id,
                    dirty_pages: pages,
                    deleted_page_ids: Vec::new(),
                })
            })
            .collect()
    }

    pub fn row_count(&self, table: &str) -> usize {
        if !self.materialized_rows_resident {
            return self.detached_row_counts.get(table).copied().unwrap_or(0);
        }
        self.segments
            .get(table)
            .map_or(0, |segment| segment.rows.len())
    }

    pub fn total_row_count(&self) -> usize {
        if !self.materialized_rows_resident {
            return self.detached_total_row_count;
        }
        self.segments
            .values()
            .map(|segment| segment.rows.len())
            .sum()
    }

    pub fn estimated_checkpoint_bytes(&self) -> u64 {
        let row_bytes = if self.materialized_rows_resident {
            self.estimated_materialized_row_bytes()
        } else {
            self.detached_row_bytes
        };
        let overflow_bytes = self
            .overflow_segments
            .values()
            .fold(0u64, |bytes, segment| {
                bytes.saturating_add(match segment {
                    RelationalOverflowSegment::Inline(value) => value.len() as u64,
                    RelationalOverflowSegment::FileRange { range, .. } => range.length.get(),
                })
            });
        row_bytes.saturating_add(overflow_bytes)
    }

    pub fn index_lookup(
        &self,
        table: &str,
        index: &str,
        key: &RelationalKey,
    ) -> Option<RelationalIndexPosting<'_>> {
        self.segments
            .get(table)?
            .indexes
            .get(index)?
            .get(key)
            .map(|postings| RelationalIndexPosting { postings })
    }

    pub fn index_prefix_cardinality(
        &self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
    ) -> Option<usize> {
        Some(
            self.segments
                .get(table)?
                .indexes
                .get(index)?
                .prefix_cardinality(prefix),
        )
    }

    pub fn index_prefix_cardinality_at_most(
        &self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        max_count: usize,
    ) -> Option<usize> {
        Some(
            self.segments
                .get(table)?
                .indexes
                .get(index)?
                .prefix_cardinality_at_most(prefix, max_count),
        )
    }

    pub fn index_prefix_lookup<'a>(
        &'a self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        max_keys: usize,
    ) -> Option<Vec<&'a RelationalKey>> {
        Some(
            self.segments
                .get(table)?
                .indexes
                .get(index)?
                .prefix_primary_keys(prefix, max_keys),
        )
    }

    /// Visits rows selected by a leading index-key prefix without first
    /// materializing the complete posting list.
    ///
    /// Returning `false` from `visit` stops the scan. `None` means that the
    /// table or materialized index does not exist.
    pub fn visit_index_prefix_rows<'a>(
        &'a self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        mut visit: impl FnMut(&'a RelationalKey, &'a RelationalRow) -> bool,
    ) -> Option<()> {
        let segment = self.segments.get(table)?;
        let index = segment.indexes.get(index)?;
        index.visit_prefix(prefix, |postings| {
            for primary_key in postings.iter() {
                let row = segment
                    .rows
                    .get(primary_key)
                    .expect("materialized relational index points to an existing row");
                if !visit(primary_key, row) {
                    return false;
                }
            }
            true
        });
        Some(())
    }

    /// Visits ordered `(index_key, primary_key)` entries selected by a leading
    /// index-key prefix without materializing the posting list.
    ///
    /// The callback observes index order first and primary-key order within
    /// equal index keys. Returning `false` stops the scan.
    pub fn visit_index_prefix_entries<'a>(
        &'a self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        mut visit: impl FnMut(&'a RelationalKey, &'a RelationalKey) -> bool,
    ) -> Option<()> {
        let segment = self.segments.get(table)?;
        let index = segment.indexes.get(index)?;
        index.visit_prefix_entries(prefix, |index_key, primary_key| {
            visit(index_key, primary_key)
        });
        Some(())
    }

    /// Visits an exclusive ordered index range without materializing postings.
    pub fn visit_index_range_entries<'a>(
        &'a self,
        table: &str,
        index: &str,
        scan: &RelationalIndexRangeScan,
        visit: impl FnMut(&'a RelationalKey, &'a RelationalKey) -> bool,
    ) -> Option<()> {
        let segment = self.segments.get(table)?;
        let index = segment.indexes.get(index)?;
        index.visit_range_entries(scan, visit);
        Some(())
    }

    pub fn hydrate_row(
        &self,
        table: &str,
        key: &RelationalKey,
        budget: &mut RelationalHydrationBudget,
    ) -> Result<Option<RelationalRow>, RelationalError> {
        self.hydrate_row_with_context(table, key, budget, None)
    }

    pub fn hydrate_row_with_context(
        &self,
        table: &str,
        key: &RelationalKey,
        budget: &mut RelationalHydrationBudget,
        task_context: Option<&skein_core::RuntimeTaskContext>,
    ) -> Result<Option<RelationalRow>, RelationalError> {
        let Some(row) = self.row(table, key) else {
            return Ok(None);
        };
        overflow::hydrate_row(self, row, budget, task_context).map(Some)
    }

    /// Resolves overflow references in a row read from an unpublished recovery
    /// delta. Unlike `hydrate_row_with_context`, the row is intentionally not
    /// required to reside in this metadata-only state.
    pub fn hydrate_sparse_recovery_row_with_context(
        &self,
        row: &RelationalRow,
        budget: &mut RelationalHydrationBudget,
        task_context: Option<&skein_core::RuntimeTaskContext>,
    ) -> Result<RelationalRow, RelationalError> {
        if self.materialized_rows_resident || !self.canonical_row_metadata_only {
            return Err(RelationalError::Admission(
                "sparse recovery row hydration requires canonical metadata-only state".to_string(),
            ));
        }
        overflow::hydrate_row(self, row, budget, task_context)
    }

    /// Resolves only overflow references retained by one projected row.
    ///
    /// Every reference must still match the row pinned in this state. Budget
    /// publication is atomic, so admission or corruption leaves the caller's
    /// counters unchanged.
    pub fn hydrate_projected_row_with_context(
        &self,
        table: &str,
        row: &mut RelationalProjectedRow,
        budget: &mut RelationalHydrationBudget,
        task_context: Option<&skein_core::RuntimeTaskContext>,
    ) -> Result<(), RelationalError> {
        overflow::hydrate_projected_row(self, table, row, budget, task_context)
    }

    /// Resolves only the listed projected fields while preserving other
    /// overflow values as validated metadata references.
    ///
    /// This supports operators such as `OCTET_LENGTH` that can answer from the
    /// reference metadata without reading or decompressing the payload. Every
    /// retained reference is still checked against the schema, canonical row,
    /// and reachable overflow closure.
    pub fn hydrate_projected_row_fields_with_context(
        &self,
        table: &str,
        row: &mut RelationalProjectedRow,
        required_fields: &[usize],
        budget: &mut RelationalHydrationBudget,
        task_context: Option<&skein_core::RuntimeTaskContext>,
    ) -> Result<(), RelationalError> {
        overflow::hydrate_projected_row_fields(
            self,
            table,
            row,
            Some(required_fields),
            budget,
            task_context,
        )
    }

    pub fn overflow_segment_count(&self) -> usize {
        self.overflow_segments.len()
    }

    pub fn file_backed_overflow_segment_count(&self) -> usize {
        self.overflow_segments
            .values()
            .filter(|segment| segment.is_file_backed())
            .count()
    }

    /// Returns the newly encoded envelope for an exact closure reference.
    /// References backed by the pinned checkpoint intentionally return
    /// `None`; the overflow publisher resolves those from the base descriptor
    /// stream and copies at most one encoded envelope at a time without
    /// hydration.
    pub fn inline_overflow_envelope(&self, reference: &RelationalOverflowRef) -> Option<Arc<[u8]>> {
        match self.overflow_segments.get(&reference.digest) {
            Some(RelationalOverflowSegment::Inline(encoded)) => Some(Arc::clone(encoded)),
            Some(RelationalOverflowSegment::FileRange { .. }) | None => None,
        }
    }

    /// Derives the exact overflow closure for one canonical checkpoint.
    /// Inline envelopes retain their existing `Arc`; file-backed envelopes are
    /// reused from the checkpoint-selected base generation. Materializing a
    /// file-backed envelope is allowed only for the first generation and is
    /// bounded independently from the resident state.
    pub fn overflow_generation_inputs(
        &self,
        has_base_generation: bool,
        max_materialized_bytes: usize,
    ) -> Result<Vec<RelationalOverflowExtentInput>, RelationalError> {
        self.require_materialized_rows("relational overflow checkpoint")?;
        let mut references = BTreeMap::new();
        for row in self
            .segments
            .values()
            .flat_map(|segment| segment.rows.values())
        {
            for value in row.values.iter() {
                let RelationalValue::Overflow(reference) = value else {
                    continue;
                };
                if let Some(previous) = references.insert(reference.digest, *reference)
                    && previous != *reference
                {
                    return Err(RelationalError::Corruption(format!(
                        "overflow digest {} has conflicting reference metadata",
                        reference.digest
                    )));
                }
            }
        }
        if references.len() != self.overflow_segments.len()
            || references
                .keys()
                .any(|digest| !self.overflow_segments.contains_key(digest))
        {
            return Err(RelationalError::Corruption(
                "relational overflow segments do not match the reachable row closure".to_string(),
            ));
        }

        let mut materialized_bytes = 0usize;
        references
            .into_iter()
            .map(|(digest, reference)| {
                let segment = self.overflow_segments.get(&digest).ok_or_else(|| {
                    RelationalError::Corruption(format!(
                        "missing overflow segment for reachable digest {digest}"
                    ))
                })?;
                match segment {
                    RelationalOverflowSegment::Inline(encoded) => {
                        Ok(RelationalOverflowExtentInput::Write {
                            reference,
                            encoded: Arc::clone(encoded),
                        })
                    }
                    RelationalOverflowSegment::FileRange { .. } if has_base_generation => {
                        Ok(RelationalOverflowExtentInput::Reuse(reference))
                    }
                    RelationalOverflowSegment::FileRange { .. } => {
                        let encoded = segment.read()?;
                        materialized_bytes = materialized_bytes
                            .checked_add(encoded.len())
                            .ok_or_else(|| {
                                RelationalError::Admission(
                                    "checkpoint overflow materialization byte count overflow"
                                        .to_string(),
                                )
                            })?;
                        if materialized_bytes > max_materialized_bytes {
                            return Err(RelationalError::Admission(format!(
                                "checkpoint overflow materialization uses {materialized_bytes} bytes, exceeding limit {max_materialized_bytes}"
                            )));
                        }
                        Ok(RelationalOverflowExtentInput::Write { reference, encoded })
                    }
                }
            })
            .collect()
    }

    /// Collects only overflow references carried by bounded dirty row pages.
    ///
    /// The caller must publish these inputs with base retention enabled. A
    /// reference absent from this metadata-only state is expected to resolve
    /// from the pinned base generation; newly encoded values remain available
    /// in the sparse state's inline overflow map.
    pub fn overflow_delta_generation_inputs(
        &self,
        deltas: &[RelationalRowPageTableDelta],
    ) -> Result<Vec<RelationalOverflowExtentInput>, RelationalError> {
        self.require_sparse_workspace_source("relational overflow delta checkpoint")?;
        let mut references = BTreeMap::new();
        for row in deltas
            .iter()
            .flat_map(|delta| delta.dirty_pages.iter())
            .flat_map(|page| page.rows.iter())
        {
            for value in row.row.values.iter() {
                let RelationalValue::Overflow(reference) = value else {
                    continue;
                };
                if let Some(previous) = references.insert(reference.digest, *reference)
                    && previous != *reference
                {
                    return Err(RelationalError::Corruption(format!(
                        "overflow digest {} has conflicting reference metadata",
                        reference.digest
                    )));
                }
            }
        }
        references
            .into_iter()
            .map(
                |(digest, reference)| match self.overflow_segments.get(&digest) {
                    Some(RelationalOverflowSegment::Inline(encoded)) => {
                        Ok(RelationalOverflowExtentInput::Write {
                            reference,
                            encoded: Arc::clone(encoded),
                        })
                    }
                    Some(RelationalOverflowSegment::FileRange { .. }) | None => {
                        Ok(RelationalOverflowExtentInput::Reuse(reference))
                    }
                },
            )
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalInsertMode {
    Error,
    Replace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalComparisonOp {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalPredicate {
    And(Box<RelationalPredicate>, Box<RelationalPredicate>),
    Or(Box<RelationalPredicate>, Box<RelationalPredicate>),
    Not(Box<RelationalPredicate>),
    Compare {
        column: String,
        op: RelationalComparisonOp,
        value: RelationalValue,
    },
    IsNull {
        column: String,
        negated: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalUpsertValue {
    ExcludedColumn(String),
    Value(RelationalValue),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalUpsertAssignment {
    pub column: String,
    pub value: RelationalUpsertValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalUpdateValue {
    Column(String),
    Value(RelationalValue),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalUpdateAssignment {
    pub column: String,
    pub value: RelationalUpdateValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalConflictAction {
    DoNothing,
    Update(Vec<RelationalUpsertAssignment>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalWrite {
    CreateTable(RelationalTableSchema),
    AddColumn {
        table: String,
        column: RelationalColumnSchema,
    },
    CreateIndex {
        table: String,
        index: RelationalIndexSchema,
    },
    Insert {
        table: String,
        rows: Vec<RelationalRow>,
        mode: RelationalInsertMode,
    },
    Upsert {
        table: String,
        rows: Vec<RelationalRow>,
        conflict_columns: Vec<String>,
        action: RelationalConflictAction,
    },
    DeleteByPrimaryKey {
        table: String,
        keys: Vec<RelationalKey>,
    },
    DeleteWhere {
        table: String,
        predicate: RelationalPredicate,
    },
    UpdateWhere {
        table: String,
        assignments: Vec<RelationalUpdateAssignment>,
        predicate: RelationalPredicate,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationalTransaction {
    pub writes: Vec<RelationalWrite>,
}

impl RelationalTransaction {
    pub fn is_conflict_noop_only(&self) -> bool {
        !self.writes.is_empty()
            && self.writes.iter().all(|write| {
                matches!(
                    write,
                    RelationalWrite::Upsert {
                        action: RelationalConflictAction::DoNothing,
                        ..
                    }
                )
            })
    }

    pub fn changes_schema(&self) -> bool {
        self.writes.iter().any(|write| {
            matches!(
                write,
                RelationalWrite::CreateTable(_)
                    | RelationalWrite::AddColumn { .. }
                    | RelationalWrite::CreateIndex { .. }
            )
        })
    }

    fn changes_index_schema(&self) -> bool {
        self.changes_schema()
    }

    pub fn estimated_mutation_rows(&self) -> usize {
        self.writes
            .iter()
            .map(|write| match write {
                RelationalWrite::Insert { rows, .. } => rows.len(),
                RelationalWrite::Upsert { rows, .. } => rows.len(),
                RelationalWrite::DeleteByPrimaryKey { keys, .. } => keys.len(),
                RelationalWrite::CreateTable(_)
                | RelationalWrite::AddColumn { .. }
                | RelationalWrite::CreateIndex { .. }
                | RelationalWrite::DeleteWhere { .. }
                | RelationalWrite::UpdateWhere { .. } => 0,
            })
            .sum()
    }

    pub fn estimated_payload_bytes(&self) -> usize {
        self.writes
            .iter()
            .map(|write| match write {
                RelationalWrite::Insert { rows, .. } => rows
                    .iter()
                    .map(RelationalRow::estimated_payload_bytes)
                    .sum(),
                RelationalWrite::Upsert { rows, .. } => rows
                    .iter()
                    .map(RelationalRow::estimated_payload_bytes)
                    .sum(),
                RelationalWrite::DeleteByPrimaryKey { keys, .. } => keys
                    .iter()
                    .flat_map(|key| key.0.iter())
                    .map(RelationalValue::estimated_payload_bytes)
                    .sum(),
                RelationalWrite::CreateTable(_)
                | RelationalWrite::AddColumn { .. }
                | RelationalWrite::CreateIndex { .. }
                | RelationalWrite::DeleteWhere { .. }
                | RelationalWrite::UpdateWhere { .. } => 0,
            })
            .sum()
    }
}

#[derive(Debug)]
pub struct RelationalStore {
    snapshots: SnapshotCoordinator<RelationalState>,
    limits: RelationalMutationLimits,
    overflow_config: RelationalOverflowConfig,
}

impl RelationalStore {
    pub fn new(limits: RelationalMutationLimits) -> Self {
        Self::with_overflow_config(limits, RelationalOverflowConfig::default())
    }

    pub fn with_overflow_config(
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
    ) -> Self {
        Self {
            snapshots: SnapshotCoordinator::new(RelationalState::default()),
            limits,
            overflow_config,
        }
    }

    pub fn snapshot(&self) -> Result<SnapshotReadGuard<RelationalState>, SnapshotCommitError<()>> {
        self.snapshots.read()
    }

    pub fn commit(
        &self,
        transaction: RelationalTransaction,
        make_durable: impl FnOnce(u64, &RelationalState) -> Result<(), RelationalError>,
    ) -> Result<SnapshotReadGuard<RelationalState>, SnapshotCommitError<RelationalError>> {
        self.admit(&transaction)
            .map_err(SnapshotCommitError::Stage)?;
        let overflow_config = self.overflow_config;
        let limits = self.limits;
        self.snapshots.commit(
            move |state, _| apply_transaction(state, transaction, limits, overflow_config),
            make_durable,
        )
    }

    pub fn commit_with_wal(
        &self,
        transaction: RelationalTransaction,
        persist: impl FnOnce(u64, &[u8]) -> Result<(), RelationalError>,
    ) -> Result<SnapshotReadGuard<RelationalState>, SnapshotCommitError<RelationalError>> {
        let durable_transaction = transaction.clone();
        self.commit(transaction, move |epoch, _| {
            let record = encode_relational_wal_batch(epoch, &durable_transaction)?;
            let max_record_bytes = RelationalDecodeLimits::wal().max_record_bytes;
            if record.len() > max_record_bytes {
                return Err(RelationalError::Admission(format!(
                    "relational WAL record contains {} bytes, exceeding max_record_bytes {max_record_bytes}",
                    record.len()
                )));
            }
            persist(epoch, &record)
        })
    }

    pub fn replay_wal_record(
        &self,
        record: &[u8],
        decode_limits: RelationalDecodeLimits,
    ) -> Result<SnapshotReadGuard<RelationalState>, SnapshotCommitError<RelationalError>> {
        let batch = decode_relational_wal_batch(record, decode_limits)
            .map_err(SnapshotCommitError::Stage)?;
        self.admit(&batch.transaction)
            .map_err(SnapshotCommitError::Stage)?;
        let expected_epoch = batch.epoch;
        let transaction = batch.transaction;
        let overflow_config = self.overflow_config;
        let limits = self.limits;
        self.snapshots.commit(
            move |state, next_epoch| {
                if next_epoch != expected_epoch {
                    return Err(RelationalError::Corruption(format!(
                        "relational WAL epoch gap: expected {next_epoch}, found {expected_epoch}"
                    )));
                }
                apply_transaction(state, transaction, limits, overflow_config)
            },
            |_, _| Ok(()),
        )
    }

    pub fn encode_checkpoint(&self) -> Result<Vec<u8>, RelationalError> {
        let snapshot = self.snapshots.read().map_err(|error| {
            RelationalError::Durability(format!("failed to pin relational snapshot: {error:?}"))
        })?;
        encode_relational_checkpoint(snapshot.epoch(), snapshot.value())
    }

    pub fn from_checkpoint(
        checkpoint: &[u8],
        decode_limits: RelationalDecodeLimits,
        mutation_limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
    ) -> Result<Self, RelationalError> {
        let checkpoint = decode_relational_checkpoint(checkpoint, decode_limits)?;
        Ok(Self {
            snapshots: SnapshotCoordinator::new_at_epoch(checkpoint.state, checkpoint.epoch),
            limits: mutation_limits,
            overflow_config,
        })
    }

    fn admit(&self, transaction: &RelationalTransaction) -> Result<(), RelationalError> {
        admit_transaction(transaction, self.limits)
    }
}

impl Default for RelationalStore {
    fn default() -> Self {
        Self::new(RelationalMutationLimits::default())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalError {
    Admission(String),
    Schema(String),
    Constraint(String),
    Durability(String),
    Corruption(String),
}

impl fmt::Display for RelationalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => write!(formatter, "relational admission failed: {message}"),
            Self::Schema(message) => write!(formatter, "relational schema error: {message}"),
            Self::Constraint(message) => {
                write!(formatter, "relational constraint violation: {message}")
            }
            Self::Durability(message) => {
                write!(formatter, "relational durability error: {message}")
            }
            Self::Corruption(message) => {
                write!(formatter, "relational corruption: {message}")
            }
        }
    }
}

impl std::error::Error for RelationalError {}

fn admit_transaction(
    transaction: &RelationalTransaction,
    limits: RelationalMutationLimits,
) -> Result<(), RelationalError> {
    let rows = transaction.estimated_mutation_rows();
    if rows > limits.max_rows.get() {
        return Err(RelationalError::Admission(format!(
            "relational mutation contains {rows} rows, exceeding max_rows {}",
            limits.max_rows
        )));
    }
    let bytes = transaction.estimated_payload_bytes();
    if bytes > limits.max_payload_bytes.get() {
        return Err(RelationalError::Admission(format!(
            "relational mutation contains {bytes} payload bytes, exceeding max_payload_bytes {}",
            limits.max_payload_bytes
        )));
    }
    Ok(())
}

fn apply_transaction(
    state: &RelationalState,
    transaction: RelationalTransaction,
    limits: RelationalMutationLimits,
    overflow_config: RelationalOverflowConfig,
) -> Result<RelationalState, RelationalError> {
    apply_transaction_inner(
        state,
        transaction,
        limits,
        overflow_config,
        TransactionApplyOptions::materialized(),
    )
    .map(|result| result.state)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TransactionIndexMode {
    Materialized,
    Authoritative,
    AuthoritativeRecovery,
}

#[derive(Clone, Copy)]
struct TransactionApplyOptions<'a> {
    index_capture_limits: Option<RelationalIndexChangeCaptureLimits>,
    row_capture_limits: Option<RelationalRowChangeCaptureLimits>,
    replay_access_limits: Option<RelationalRowChangeCaptureLimits>,
    primary_key_capture_limits: Option<RelationalPrimaryKeyChangeCaptureLimits>,
    constraint_index: Option<&'a dyn RelationalConstraintIndex>,
    index_mode: TransactionIndexMode,
    capture_mutation_outcomes: bool,
}

impl TransactionApplyOptions<'_> {
    const fn materialized() -> Self {
        Self {
            index_capture_limits: None,
            row_capture_limits: None,
            replay_access_limits: None,
            primary_key_capture_limits: None,
            constraint_index: None,
            index_mode: TransactionIndexMode::Materialized,
            capture_mutation_outcomes: false,
        }
    }
}

fn apply_transaction_with_index_changes(
    state: &RelationalState,
    transaction: RelationalTransaction,
    limits: RelationalMutationLimits,
    overflow_config: RelationalOverflowConfig,
    capture_limits: RelationalIndexChangeCaptureLimits,
    constraint_index: Option<&dyn RelationalConstraintIndex>,
    index_mode: TransactionIndexMode,
) -> Result<(RelationalState, RelationalIndexChangeCapture), RelationalError> {
    let TransactionApplyResult {
        state,
        index_capture: capture,
        ..
    } = apply_transaction_inner(
        state,
        transaction,
        limits,
        overflow_config,
        TransactionApplyOptions {
            index_capture_limits: Some(capture_limits),
            row_capture_limits: None,
            replay_access_limits: None,
            primary_key_capture_limits: None,
            constraint_index,
            index_mode,
            capture_mutation_outcomes: false,
        },
    )?;
    Ok((
        state,
        capture.expect("index change capture was requested for this transaction"),
    ))
}

struct TransactionApplyResult {
    state: RelationalState,
    index_capture: Option<RelationalIndexChangeCapture>,
    row_capture: Option<RelationalRowChangeCapture>,
    replay_access: Option<RelationalReplayAccessSet>,
    primary_key_changes: Option<RelationalPrimaryKeyChangeCapture>,
    mutation_outcomes: Vec<RelationalMutationOutcome>,
}

fn apply_transaction_inner(
    state: &RelationalState,
    transaction: RelationalTransaction,
    limits: RelationalMutationLimits,
    overflow_config: RelationalOverflowConfig,
    options: TransactionApplyOptions<'_>,
) -> Result<TransactionApplyResult, RelationalError> {
    let TransactionApplyOptions {
        index_capture_limits,
        row_capture_limits,
        replay_access_limits,
        primary_key_capture_limits,
        constraint_index,
        index_mode,
        capture_mutation_outcomes,
    } = options;
    let mut next = state.clone();
    match index_mode {
        TransactionIndexMode::Materialized => {
            if !state.materialized_index_postings_resident {
                return Err(RelationalError::Corruption(
                    "materialized relational index maintenance was requested for a state whose postings were omitted"
                        .to_string(),
                ));
            }
        }
        TransactionIndexMode::Authoritative | TransactionIndexMode::AuthoritativeRecovery => {
            next.omit_materialized_index_postings();
        }
    }
    let mut touched = BTreeSet::new();
    let mut changed_keys = BTreeMap::<String, BTreeSet<RelationalKey>>::new();
    let mut replay_access_tracker = replay_access_limits.map(RelationalReplayAccessTracker::new);
    let mut full_index_rebuild = BTreeSet::new();
    let mut projection_rebuild_tables = BTreeSet::new();
    let mut mutation_outcomes = Vec::new();
    for write in transaction.writes {
        match write {
            RelationalWrite::CreateTable(schema) => {
                validate_table_schema(&schema)?;
                if next.schemas.contains_key(&schema.name) {
                    return Err(RelationalError::Schema(format!(
                        "table {} already exists",
                        schema.name
                    )));
                }
                touched.insert(schema.name.clone());
                full_index_rebuild.insert(schema.name.clone());
                next.segments.insert(
                    schema.name.clone(),
                    Arc::new(RelationalTableSegment::default()),
                );
                next.schemas.insert(schema.name.clone(), Arc::new(schema));
            }
            RelationalWrite::AddColumn { table, column } => {
                let mut schema = next
                    .schemas
                    .get(&table)
                    .map(|schema| schema.as_ref().clone())
                    .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
                if schema.column_position(&column.name).is_some() {
                    return Err(RelationalError::Schema(format!(
                        "table {table} already has column {}",
                        column.name
                    )));
                }
                let segment = next.segments.get(&table).ok_or_else(|| {
                    RelationalError::Schema(format!(
                        "table {table} is missing its relational row segment"
                    ))
                })?;
                let row_count = segment.rows.len();
                if row_count > limits.max_rows.get() {
                    return Err(RelationalError::Admission(format!(
                        "ALTER TABLE {table} rewrites {row_count} rows, exceeding max_rows {}",
                        limits.max_rows
                    )));
                }
                let fill = column.default.clone().unwrap_or(RelationalValue::Null);
                if !column.nullable && matches!(&fill, RelationalValue::Null) && row_count != 0 {
                    return Err(RelationalError::Constraint(format!(
                        "ALTER TABLE {table} cannot add NOT NULL column {} without a default to a non-empty table",
                        column.name
                    )));
                }
                let rewrite_bytes = segment
                    .rows
                    .iter()
                    .map(|(key, row)| {
                        relational_row_entry_bytes(key, row)
                            .saturating_add(fill.estimated_payload_bytes())
                    })
                    .fold(0usize, usize::saturating_add);
                if rewrite_bytes > limits.max_payload_bytes.get() {
                    return Err(RelationalError::Admission(format!(
                        "ALTER TABLE {table} rewrites {rewrite_bytes} resident bytes, exceeding max_payload_bytes {}",
                        limits.max_payload_bytes
                    )));
                }
                schema.columns.push(column);
                validate_table_schema(&schema)?;
                let mut fill_values = vec![RelationalValue::Null; schema.columns.len() - 1];
                fill_values.push(fill);
                let mut fill_row = RelationalRow::new(fill_values);
                overflow::externalize_row(&mut next, &schema, &mut fill_row, overflow_config)?;
                let fill = fill_row
                    .values()
                    .last()
                    .cloned()
                    .expect("validated relational schema contains the added column");
                let segment = next
                    .segments
                    .get_mut(&table)
                    .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
                Arc::make_mut(segment).rows.append_value_to_all(&fill);
                next.schemas.insert(table.clone(), Arc::new(schema));
                full_index_rebuild.insert(table.clone());
                if row_count != 0 {
                    projection_rebuild_tables.insert(table.clone());
                }
                touched.insert(table);
            }
            RelationalWrite::CreateIndex { table, index } => {
                let schema = next
                    .schemas
                    .get_mut(&table)
                    .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
                let schema = Arc::make_mut(schema);
                validate_column_list(schema, &index.columns, "index")?;
                if index.name.is_empty()
                    || is_reserved_relational_index_name(&index.name)
                    || schema.indexes.iter().any(|item| item.name == index.name)
                {
                    return Err(RelationalError::Schema(format!(
                        "invalid or duplicate index {}",
                        index.name
                    )));
                }
                schema.indexes.push(index);
                full_index_rebuild.insert(table.clone());
                touched.insert(table);
            }
            RelationalWrite::Insert { table, rows, mode } => {
                let schema =
                    Arc::clone(next.schemas.get(&table).ok_or_else(|| {
                        RelationalError::Schema(format!("unknown table {table}"))
                    })?);
                let primary_key = column_positions(&schema, &schema.primary_key)?;
                let affected_rows = rows.len();
                let logical_rows = capture_mutation_outcomes.then(|| rows.clone());
                let mut prepared_rows = Vec::with_capacity(rows.len());
                for mut row in rows {
                    validate_row(&schema, &row)?;
                    overflow::externalize_row(&mut next, &schema, &mut row, overflow_config)?;
                    prepared_rows.push(row);
                }
                let segment = next.segments.get_mut(&table).ok_or_else(|| {
                    RelationalError::Schema(format!("missing row segment for table {table}"))
                })?;
                let segment = Arc::make_mut(segment);
                for row in prepared_rows {
                    let key = row_key(&row, &primary_key);
                    if mode == RelationalInsertMode::Error && segment.rows.contains_key(&key) {
                        return Err(RelationalError::Constraint(format!(
                            "duplicate primary key in table {table}"
                        )));
                    }
                    segment.rows.insert(key.clone(), row);
                    changed_keys.entry(table.clone()).or_default().insert(key);
                }
                touched.insert(table.clone());
                if let Some(logical_rows) = logical_rows {
                    mutation_outcomes.push(RelationalMutationOutcome {
                        table,
                        affected_rows,
                        conflict_rows: 0,
                        rows: logical_rows,
                    });
                }
            }
            RelationalWrite::Upsert {
                table,
                rows,
                conflict_columns,
                action,
            } => {
                let outcome = apply_upsert(
                    &mut next,
                    &table,
                    rows,
                    &conflict_columns,
                    &action,
                    overflow_config,
                    UpsertIndexContext {
                        changed_keys: changed_keys.entry(table.clone()).or_default(),
                        constraint_index,
                        replay_access_tracker: replay_access_tracker.as_mut(),
                        capture_mutation_outcome: capture_mutation_outcomes,
                    },
                )?;
                touched.insert(table.clone());
                if capture_mutation_outcomes {
                    mutation_outcomes.push(outcome);
                }
            }
            RelationalWrite::DeleteByPrimaryKey { table, keys } => {
                let segment = next
                    .segments
                    .get_mut(&table)
                    .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
                let segment = Arc::make_mut(segment);
                for key in keys {
                    changed_keys
                        .entry(table.clone())
                        .or_default()
                        .insert(key.clone());
                    segment.rows.remove(&key);
                }
                touched.insert(table);
            }
            RelationalWrite::DeleteWhere { table, predicate } => {
                let schema = next
                    .schemas
                    .get(&table)
                    .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
                validate_predicate(schema, &predicate)?;
                let segment = next
                    .segments
                    .get_mut(&table)
                    .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
                let segment = Arc::make_mut(segment);
                let mut keys = Vec::new();
                for (key, row) in segment.rows.iter() {
                    if let Some(tracker) = replay_access_tracker.as_mut() {
                        tracker.record(&table, key)?;
                    }
                    if predicate_truth(schema, row, &predicate)? == Some(true) {
                        keys.push(key.clone());
                        if keys.len() > limits.max_rows.get() {
                            break;
                        }
                    }
                }
                if keys.len() > limits.max_rows.get() {
                    return Err(RelationalError::Admission(format!(
                        "relational DELETE affects more than max_rows {}",
                        limits.max_rows
                    )));
                }
                for key in keys {
                    changed_keys
                        .entry(table.clone())
                        .or_default()
                        .insert(key.clone());
                    segment.rows.remove(&key);
                }
                touched.insert(table);
            }
            RelationalWrite::UpdateWhere {
                table,
                assignments,
                predicate,
            } => {
                apply_update(
                    &mut next,
                    &table,
                    &assignments,
                    &predicate,
                    UpdateApplyContext {
                        limits,
                        overflow_config,
                        changed_keys: changed_keys.entry(table.clone()).or_default(),
                        replay_access_tracker: replay_access_tracker.as_mut(),
                    },
                )?;
                touched.insert(table);
            }
        }
    }
    overflow::prune_unreachable_segments(&mut next);
    if index_mode == TransactionIndexMode::Materialized {
        for table in &touched {
            if full_index_rebuild.contains(table) {
                rebuild_indexes(&mut next, table)?;
            } else if let Some(keys) = changed_keys.get(table) {
                refresh_indexes_for_keys(state, &mut next, table, keys, true)?;
            }
        }
    }
    let index_capture = index_capture_limits.map(|capture_limits| {
        capture_relational_index_changes(
            state,
            &next,
            &changed_keys,
            &full_index_rebuild,
            capture_limits,
        )
    });
    let row_capture = row_capture_limits.map(|capture_limits| {
        capture_relational_row_changes(
            state,
            &next,
            &changed_keys,
            &full_index_rebuild,
            capture_limits,
        )
    });
    let primary_key_changes = primary_key_capture_limits.map(|capture_limits| {
        capture_relational_primary_key_changes(
            state,
            &next,
            &changed_keys,
            &projection_rebuild_tables,
            capture_limits,
        )
    });
    let replay_access = if let Some(mut tracker) = replay_access_tracker {
        tracker.record_changed_keys(&changed_keys)?;
        Some(tracker.finish())
    } else {
        None
    };
    match (index_mode, constraint_index) {
        (TransactionIndexMode::Materialized, None) => {
            validate_foreign_keys_incremental(state, &next, &changed_keys, &full_index_rebuild)?;
        }
        (TransactionIndexMode::Authoritative, Some(constraint_index)) => {
            let changes = index_capture
                .as_ref()
                .expect("authoritative constraints require index change capture")
                .changes()?;
            constraints::validate_authoritative_constraints(
                &next,
                &changed_keys,
                changes,
                constraint_index,
            )?;
        }
        (TransactionIndexMode::AuthoritativeRecovery, None) => {}
        _ => {
            return Err(RelationalError::Corruption(
                "relational transaction index mode does not match its constraint source"
                    .to_string(),
            ));
        }
    }
    Ok(TransactionApplyResult {
        state: next,
        index_capture,
        row_capture,
        replay_access,
        primary_key_changes,
        mutation_outcomes,
    })
}

struct UpsertIndexContext<'a> {
    changed_keys: &'a mut BTreeSet<RelationalKey>,
    constraint_index: Option<&'a dyn RelationalConstraintIndex>,
    replay_access_tracker: Option<&'a mut RelationalReplayAccessTracker>,
    capture_mutation_outcome: bool,
}

fn apply_upsert(
    state: &mut RelationalState,
    table: &str,
    rows: Vec<RelationalRow>,
    conflict_columns: &[String],
    action: &RelationalConflictAction,
    overflow_config: RelationalOverflowConfig,
    index_context: UpsertIndexContext<'_>,
) -> Result<RelationalMutationOutcome, RelationalError> {
    let UpsertIndexContext {
        changed_keys,
        constraint_index,
        mut replay_access_tracker,
        capture_mutation_outcome,
    } = index_context;
    let schema = Arc::clone(
        state
            .schemas
            .get(table)
            .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?,
    );
    let conflict_positions = column_positions(&schema, conflict_columns)?;
    if conflict_columns.is_empty() || schema.unique_index_definition(conflict_columns).is_none() {
        return Err(RelationalError::Schema(format!(
            "UPSERT conflict target on table {table} must name a primary or unique key"
        )));
    }
    let primary_key_positions = column_positions(&schema, &schema.primary_key)?;
    let assignments = match action {
        RelationalConflictAction::DoNothing => Vec::new(),
        RelationalConflictAction::Update(assignments) => assignments
            .iter()
            .map(|assignment| {
                let target = schema.column_position(&assignment.column).ok_or_else(|| {
                    RelationalError::Schema(format!(
                        "UPSERT assignment references unknown column {}",
                        assignment.column
                    ))
                })?;
                let source = match &assignment.value {
                    RelationalUpsertValue::ExcludedColumn(column) => {
                        let position = schema.column_position(column).ok_or_else(|| {
                            RelationalError::Schema(format!(
                                "UPSERT EXCLUDED references unknown column {column}"
                            ))
                        })?;
                        Some(position)
                    }
                    RelationalUpsertValue::Value(value) => {
                        validate_value_type(&schema.columns[target], value)?;
                        None
                    }
                };
                Ok((target, source, &assignment.value))
            })
            .collect::<Result<Vec<_>, RelationalError>>()?,
    };
    let mut staged_conflicts = BTreeMap::<RelationalKey, Option<RelationalKey>>::new();
    let mut affected_rows = 0usize;
    let mut conflict_rows = 0usize;
    let mut returned_rows = Vec::new();
    for primary_key in changed_keys.iter() {
        let Some(row) = state.row(table, primary_key) else {
            continue;
        };
        let conflict_key = row_key(row, &conflict_positions);
        if key_contains_null(&conflict_key) {
            continue;
        }
        if staged_conflicts
            .insert(conflict_key, Some(primary_key.clone()))
            .is_some()
        {
            return Err(RelationalError::Constraint(format!(
                "UPSERT conflict target on table {table} is not unique within the transaction"
            )));
        }
    }

    for mut excluded in rows {
        validate_row(&schema, &excluded)?;
        let logical_excluded = capture_mutation_outcome.then(|| excluded.clone());
        overflow::externalize_row(state, &schema, &mut excluded, overflow_config)?;
        let conflict_key = row_key(&excluded, &conflict_positions);
        let conflict_has_null = conflict_key
            .0
            .iter()
            .any(|value| matches!(value, RelationalValue::Null));
        let existing_primary_key = if conflict_has_null {
            None
        } else if let Some(staged) = staged_conflicts.get(&conflict_key) {
            staged.clone()
        } else {
            conflict_primary_key(
                state,
                table,
                &schema,
                conflict_columns,
                &conflict_key,
                changed_keys,
                constraint_index,
            )?
        };

        let segment = state
            .segments
            .get_mut(table)
            .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
        let segment = Arc::make_mut(segment);
        if let Some(existing_primary_key) = existing_primary_key {
            conflict_rows = conflict_rows.saturating_add(1);
            if let Some(tracker) = replay_access_tracker.as_deref_mut() {
                tracker.record(table, &existing_primary_key)?;
            }
            if matches!(action, RelationalConflictAction::DoNothing) {
                continue;
            }
            let existing = segment
                .rows
                .get(&existing_primary_key)
                .expect("conflict row was found in the same segment");
            let mut values = existing.values().to_vec();
            for (target, source, assignment) in &assignments {
                values[*target] = match (source, *assignment) {
                    (Some(source), _) => excluded.values()[*source].clone(),
                    (None, RelationalUpsertValue::Value(value)) => value.clone(),
                    (None, RelationalUpsertValue::ExcludedColumn(_)) => {
                        unreachable!("excluded source position was resolved")
                    }
                };
            }
            let updated = RelationalRow::new(values);
            validate_row(&schema, &updated)?;
            let updated_primary_key = row_key(&updated, &primary_key_positions);
            let updated_conflict_key = row_key(&updated, &conflict_positions);
            changed_keys.insert(existing_primary_key.clone());
            changed_keys.insert(updated_primary_key.clone());
            segment.rows.remove(&existing_primary_key);
            segment.rows.insert(updated_primary_key.clone(), updated);
            staged_conflicts.insert(conflict_key, None);
            if !key_contains_null(&updated_conflict_key) {
                staged_conflicts.insert(updated_conflict_key, Some(updated_primary_key));
            }
            affected_rows = affected_rows.saturating_add(1);
        } else {
            let primary_key = row_key(&excluded, &primary_key_positions);
            changed_keys.insert(primary_key.clone());
            if segment.rows.insert(primary_key.clone(), excluded).is_some() {
                return Err(RelationalError::Constraint(format!(
                    "duplicate primary key in table {table}"
                )));
            }
            if !conflict_has_null {
                staged_conflicts.insert(conflict_key, Some(primary_key));
            }
            affected_rows = affected_rows.saturating_add(1);
            if let Some(logical_excluded) = logical_excluded {
                returned_rows.push(logical_excluded);
            }
        }
    }
    Ok(RelationalMutationOutcome {
        table: table.to_string(),
        affected_rows,
        conflict_rows,
        rows: returned_rows,
    })
}

fn conflict_primary_key(
    state: &RelationalState,
    table: &str,
    schema: &RelationalTableSchema,
    conflict_columns: &[String],
    conflict_key: &RelationalKey,
    changed_keys: &BTreeSet<RelationalKey>,
    constraint_index: Option<&dyn RelationalConstraintIndex>,
) -> Result<Option<RelationalKey>, RelationalError> {
    let definition = schema
        .unique_index_definition(conflict_columns)
        .ok_or_else(|| {
            RelationalError::Schema(format!(
                "UPSERT conflict target on table {table} is not materialized"
            ))
        })?;
    if let Some(constraint_index) = constraint_index {
        return constraints::authoritative_conflict_primary_key(
            state,
            table,
            &definition,
            conflict_columns,
            conflict_key,
            changed_keys,
            constraint_index,
        );
    }
    if definition.role == RelationalIndexRole::Primary {
        return Ok(state.row(table, conflict_key).map(|_| conflict_key.clone()));
    }
    if !state.materialized_index_postings_resident {
        let positions = column_positions(schema, conflict_columns)?;
        let mut matches = state
            .rows(table)
            .filter(|(_, row)| row_key(row, &positions) == *conflict_key)
            .map(|(primary_key, _)| primary_key.clone());
        let primary_key = matches.next();
        if matches.next().is_some() {
            return Err(RelationalError::Corruption(format!(
                "unique conflict target {} on table {table} has multiple visible rows during authoritative recovery",
                definition.name
            )));
        }
        return Ok(primary_key);
    }
    let Some(postings) = state.index_lookup(table, &definition.name, conflict_key) else {
        return Ok(None);
    };
    let mut keys = postings.iter();
    let primary_key = keys.next().cloned();
    if keys.next().is_some() {
        return Err(RelationalError::Corruption(format!(
            "unique conflict index {} on table {table} has multiple visible rows",
            definition.name
        )));
    }
    Ok(primary_key)
}

struct UpdateApplyContext<'a> {
    limits: RelationalMutationLimits,
    overflow_config: RelationalOverflowConfig,
    changed_keys: &'a mut BTreeSet<RelationalKey>,
    replay_access_tracker: Option<&'a mut RelationalReplayAccessTracker>,
}

fn apply_update(
    state: &mut RelationalState,
    table: &str,
    assignments: &[RelationalUpdateAssignment],
    predicate: &RelationalPredicate,
    context: UpdateApplyContext<'_>,
) -> Result<(), RelationalError> {
    let UpdateApplyContext {
        limits,
        overflow_config,
        changed_keys,
        mut replay_access_tracker,
    } = context;
    let schema = Arc::clone(
        state
            .schemas
            .get(table)
            .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?,
    );
    if assignments.is_empty() {
        return Err(RelationalError::Schema(
            "UPDATE requires at least one assignment".to_string(),
        ));
    }
    validate_predicate(&schema, predicate)?;
    let resolved = assignments
        .iter()
        .map(|assignment| {
            let target = schema.column_position(&assignment.column).ok_or_else(|| {
                RelationalError::Schema(format!(
                    "UPDATE assignment references unknown column {}",
                    assignment.column
                ))
            })?;
            let source = match &assignment.value {
                RelationalUpdateValue::Column(column) => {
                    let source = schema.column_position(column).ok_or_else(|| {
                        RelationalError::Schema(format!(
                            "UPDATE assignment references unknown source column {column}"
                        ))
                    })?;
                    if schema.columns[target].scalar_type != schema.columns[source].scalar_type {
                        return Err(RelationalError::Schema(format!(
                            "UPDATE assignment from {column} has an incompatible type"
                        )));
                    }
                    Some(source)
                }
                RelationalUpdateValue::Value(value) => {
                    validate_value_type(&schema.columns[target], value)?;
                    None
                }
            };
            Ok((target, source, &assignment.value))
        })
        .collect::<Result<Vec<_>, RelationalError>>()?;
    let segment = state
        .segments
        .get(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let mut matched = Vec::new();
    for (key, row) in segment.rows.iter() {
        if let Some(tracker) = replay_access_tracker.as_deref_mut() {
            tracker.record(table, key)?;
        }
        if predicate_truth(&schema, row, predicate)? == Some(true) {
            matched.push((key.clone(), row.clone()));
            if matched.len() > limits.max_rows.get() {
                return Err(RelationalError::Admission(format!(
                    "relational UPDATE affects more than max_rows {}",
                    limits.max_rows
                )));
            }
        }
    }
    let primary_key_positions = column_positions(&schema, &schema.primary_key)?;
    let mut updates = Vec::with_capacity(matched.len());
    for (old_key, row) in &matched {
        let mut values = row.values().to_vec();
        for (target, source, assignment) in &resolved {
            values[*target] = match (source, *assignment) {
                (Some(source), _) => row.values()[*source].clone(),
                (None, RelationalUpdateValue::Value(value)) => value.clone(),
                (None, RelationalUpdateValue::Column(_)) => {
                    unreachable!("update source position was resolved")
                }
            };
        }
        let mut updated = RelationalRow::new(values);
        validate_row(&schema, &updated)?;
        overflow::externalize_row(state, &schema, &mut updated, overflow_config)?;
        let new_key = row_key(&updated, &primary_key_positions);
        updates.push((old_key.clone(), new_key, updated));
    }
    let segment = state
        .segments
        .get_mut(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let segment = Arc::make_mut(segment);
    for (old_key, _, _) in &updates {
        changed_keys.insert(old_key.clone());
        segment.rows.remove(old_key);
    }
    for (_, new_key, row) in updates {
        changed_keys.insert(new_key.clone());
        if segment.rows.insert(new_key, row).is_some() {
            return Err(RelationalError::Constraint(format!(
                "UPDATE creates a duplicate primary key in table {table}"
            )));
        }
    }
    Ok(())
}

fn validate_predicate(
    schema: &RelationalTableSchema,
    predicate: &RelationalPredicate,
) -> Result<(), RelationalError> {
    match predicate {
        RelationalPredicate::And(left, right) | RelationalPredicate::Or(left, right) => {
            validate_predicate(schema, left)?;
            validate_predicate(schema, right)
        }
        RelationalPredicate::Not(predicate) => validate_predicate(schema, predicate),
        RelationalPredicate::Compare { column, value, .. } => {
            let position = schema.column_position(column).ok_or_else(|| {
                RelationalError::Schema(format!("predicate references unknown column {column}"))
            })?;
            if !matches!(value, RelationalValue::Null)
                && value.scalar_type() != Some(schema.columns[position].scalar_type)
            {
                return Err(RelationalError::Schema(format!(
                    "predicate on column {column} has an incompatible value type"
                )));
            }
            Ok(())
        }
        RelationalPredicate::IsNull { column, .. } => {
            if schema.column_position(column).is_none() {
                return Err(RelationalError::Schema(format!(
                    "predicate references unknown column {column}"
                )));
            }
            Ok(())
        }
    }
}

fn predicate_truth(
    schema: &RelationalTableSchema,
    row: &RelationalRow,
    predicate: &RelationalPredicate,
) -> Result<Option<bool>, RelationalError> {
    match predicate {
        RelationalPredicate::And(left, right) => match (
            predicate_truth(schema, row, left)?,
            predicate_truth(schema, row, right)?,
        ) {
            (Some(false), _) | (_, Some(false)) => Ok(Some(false)),
            (Some(true), Some(true)) => Ok(Some(true)),
            _ => Ok(None),
        },
        RelationalPredicate::Or(left, right) => match (
            predicate_truth(schema, row, left)?,
            predicate_truth(schema, row, right)?,
        ) {
            (Some(true), _) | (_, Some(true)) => Ok(Some(true)),
            (Some(false), Some(false)) => Ok(Some(false)),
            _ => Ok(None),
        },
        RelationalPredicate::Not(predicate) => {
            Ok(predicate_truth(schema, row, predicate)?.map(|value| !value))
        }
        RelationalPredicate::Compare { column, op, value } => {
            let position = schema
                .column_position(column)
                .expect("predicate schema was validated");
            let current = &row.values()[position];
            if matches!(current, RelationalValue::Overflow(_))
                || matches!(value, RelationalValue::Overflow(_))
            {
                return Err(RelationalError::Admission(format!(
                    "DELETE predicate on column {column} requires overflow hydration"
                )));
            }
            if matches!(current, RelationalValue::Null) || matches!(value, RelationalValue::Null) {
                return Ok(None);
            }
            Ok(Some(match op {
                RelationalComparisonOp::Eq => current == value,
                RelationalComparisonOp::NotEq => current != value,
                RelationalComparisonOp::Lt => current < value,
                RelationalComparisonOp::Lte => current <= value,
                RelationalComparisonOp::Gt => current > value,
                RelationalComparisonOp::Gte => current >= value,
            }))
        }
        RelationalPredicate::IsNull { column, negated } => {
            let position = schema
                .column_position(column)
                .expect("predicate schema was validated");
            Ok(Some(
                matches!(row.values()[position], RelationalValue::Null) != *negated,
            ))
        }
    }
}

fn validate_table_schema(schema: &RelationalTableSchema) -> Result<(), RelationalError> {
    if schema.name.is_empty() || schema.columns.is_empty() {
        return Err(RelationalError::Schema(
            "table name and columns must be non-empty".to_string(),
        ));
    }
    if schema.primary_key.is_empty() {
        return Err(RelationalError::Schema(format!(
            "table {} must declare a primary key",
            schema.name
        )));
    }
    let mut names = BTreeSet::new();
    for column in &schema.columns {
        if column.name.is_empty() || !names.insert(column.name.clone()) {
            return Err(RelationalError::Schema(format!(
                "duplicate or empty column {}",
                column.name
            )));
        }
        if let Some(default) = &column.default {
            validate_value_type(column, default)?;
        }
    }
    validate_column_list(schema, &schema.primary_key, "primary key")?;
    for column in &schema.primary_key {
        let position = schema.column_position(column).expect("validated column");
        if schema.columns[position].nullable {
            return Err(RelationalError::Schema(format!(
                "primary-key column {column} must be NOT NULL"
            )));
        }
    }
    for unique in &schema.unique_constraints {
        validate_column_list(schema, unique, "unique constraint")?;
    }
    for foreign_key in &schema.foreign_keys {
        validate_column_list(schema, &foreign_key.columns, "foreign key")?;
        if foreign_key.columns.len() != foreign_key.referenced_columns.len()
            || foreign_key.columns.is_empty()
        {
            return Err(RelationalError::Schema(
                "foreign-key column counts must match and be non-empty".to_string(),
            ));
        }
    }
    let mut index_names = BTreeSet::new();
    for index in &schema.indexes {
        if index.name.is_empty()
            || is_reserved_relational_index_name(&index.name)
            || !index_names.insert(index.name.as_str())
        {
            return Err(RelationalError::Schema(format!(
                "invalid, duplicate, or reserved index name {}",
                index.name
            )));
        }
        validate_column_list(schema, &index.columns, "index")?;
    }
    Ok(())
}

fn is_reserved_relational_index_name(name: &str) -> bool {
    name == RELATIONAL_PRIMARY_INDEX_NAME
        || name.starts_with(RELATIONAL_UNIQUE_INDEX_PREFIX)
        || name.starts_with(RELATIONAL_FOREIGN_KEY_INDEX_PREFIX)
}

fn validate_column_list(
    schema: &RelationalTableSchema,
    columns: &[String],
    kind: &str,
) -> Result<(), RelationalError> {
    if columns.is_empty() {
        return Err(RelationalError::Schema(format!(
            "{kind} must contain at least one column"
        )));
    }
    let mut unique = BTreeSet::new();
    for column in columns {
        if schema.column_position(column).is_none() || !unique.insert(column) {
            return Err(RelationalError::Schema(format!(
                "{kind} references unknown or duplicate column {column}"
            )));
        }
    }
    Ok(())
}

fn validate_row(
    schema: &RelationalTableSchema,
    row: &RelationalRow,
) -> Result<(), RelationalError> {
    if row.values.len() != schema.columns.len() {
        return Err(RelationalError::Schema(format!(
            "table {} expects {} columns but row has {}",
            schema.name,
            schema.columns.len(),
            row.values.len()
        )));
    }
    for (column, value) in schema.columns.iter().zip(row.values.iter()) {
        validate_value_type(column, value)?;
    }
    Ok(())
}

fn validate_value_type(
    column: &RelationalColumnSchema,
    value: &RelationalValue,
) -> Result<(), RelationalError> {
    if matches!(value, RelationalValue::Null) {
        if column.nullable {
            return Ok(());
        }
        return Err(RelationalError::Constraint(format!(
            "column {} is NOT NULL",
            column.name
        )));
    }
    if value.scalar_type() != Some(column.scalar_type) {
        return Err(RelationalError::Schema(format!(
            "column {} expects {:?} but received {:?}",
            column.name,
            column.scalar_type,
            value.scalar_type()
        )));
    }
    Ok(())
}

fn rebuild_indexes(state: &mut RelationalState, table: &str) -> Result<(), RelationalError> {
    let schema = state
        .schemas
        .get(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let index_definitions = schema.required_index_definitions();
    let segment = state
        .segments
        .get_mut(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let segment = Arc::make_mut(segment);
    segment.indexes.clear();
    for index in index_definitions
        .into_iter()
        .filter(|index| index.role != RelationalIndexRole::Primary)
    {
        let positions = column_positions(schema, &index.columns)?;
        let mut postings = RelationalIndexPages::default();
        for (primary_key, row) in segment.rows.iter() {
            let key = row_key(row, &positions);
            if index.role.is_unique()
                && key
                    .0
                    .iter()
                    .any(|value| matches!(value, RelationalValue::Null))
            {
                continue;
            }
            if index.role.is_unique() && postings.get(&key).is_some_and(|keys| !keys.is_empty()) {
                return Err(RelationalError::Constraint(format!(
                    "unique index {} on table {table} has a duplicate key",
                    index.name
                )));
            }
            postings.insert_posting(key, primary_key.clone());
        }
        segment.indexes.insert(index.name, postings);
    }
    Ok(())
}

fn refresh_indexes_for_keys(
    previous: &RelationalState,
    next: &mut RelationalState,
    table: &str,
    keys: &BTreeSet<RelationalKey>,
    enforce_unique: bool,
) -> Result<(), RelationalError> {
    let schema = Arc::clone(
        next.schemas
            .get(table)
            .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?,
    );
    let index_definitions = schema.required_index_definitions();
    let previous_segment = previous.segments.get(table);
    let next_segment = next
        .segments
        .get_mut(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let next_segment = Arc::make_mut(next_segment);

    for index in index_definitions
        .into_iter()
        .filter(|index| index.role != RelationalIndexRole::Primary)
    {
        let positions = column_positions(&schema, &index.columns)?;
        let postings = next_segment.indexes.get_mut(&index.name).ok_or_else(|| {
            RelationalError::Corruption(format!(
                "table {table} is missing materialized index {}",
                index.name
            ))
        })?;
        for primary_key in keys {
            if let Some(previous_row) =
                previous_segment.and_then(|segment| segment.rows.get(primary_key))
            {
                let index_key = row_key(previous_row, &positions);
                postings.remove_posting(&index_key, primary_key);
            }
        }
        for primary_key in keys {
            let Some(row) = next_segment.rows.get(primary_key) else {
                continue;
            };
            let index_key = row_key(row, &positions);
            if index.role.is_unique()
                && index_key
                    .0
                    .iter()
                    .any(|value| matches!(value, RelationalValue::Null))
            {
                continue;
            }
            if enforce_unique
                && index.role.is_unique()
                && postings
                    .get(&index_key)
                    .into_iter()
                    .flat_map(RelationalKeySetPages::iter)
                    .any(|existing| existing != primary_key)
            {
                return Err(RelationalError::Constraint(format!(
                    "unique index {} on table {table} has a duplicate key",
                    index.name
                )));
            }
            postings.insert_posting(index_key, primary_key.clone());
        }
    }
    Ok(())
}

fn capture_relational_index_changes(
    previous: &RelationalState,
    next: &RelationalState,
    changed_keys: &BTreeMap<String, BTreeSet<RelationalKey>>,
    full_index_rebuild: &BTreeSet<String>,
    limits: RelationalIndexChangeCaptureLimits,
) -> RelationalIndexChangeCapture {
    if !full_index_rebuild.is_empty() {
        return RelationalIndexChangeCapture::Invalidated {
            reason: format!(
                "schema-changing WAL requires new index roots for tables {}",
                full_index_rebuild
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        };
    }

    let mut changes = Vec::new();
    let mut encoded_bytes = 0usize;
    for (table, keys) in changed_keys {
        let Some(schema) = next
            .schemas
            .get(table)
            .or_else(|| previous.schemas.get(table))
        else {
            return RelationalIndexChangeCapture::Invalidated {
                reason: format!("changed table {table} has no relational schema"),
            };
        };
        let previous_segment = previous.segments.get(table);
        let next_segment = next.segments.get(table);
        let indexes = schema.required_index_definitions();

        for primary_key in keys {
            let previous_row = previous_segment.and_then(|segment| segment.rows.get(primary_key));
            let next_row = next_segment.and_then(|segment| segment.rows.get(primary_key));
            if previous_row.is_some() != next_row.is_some()
                && !push_captured_index_change(
                    &mut changes,
                    &mut encoded_bytes,
                    limits,
                    RelationalIndexChange {
                        table: table.clone(),
                        index: RELATIONAL_PRIMARY_INDEX_NAME.to_string(),
                        index_key: primary_key.clone(),
                        primary_key: primary_key.clone(),
                        kind: if next_row.is_some() {
                            RelationalIndexChangeKind::Insert
                        } else {
                            RelationalIndexChangeKind::Delete
                        },
                    },
                )
            {
                return capture_limit_invalidated(limits);
            }

            for index in indexes
                .iter()
                .filter(|index| index.role != RelationalIndexRole::Primary)
            {
                let positions = match column_positions(schema, &index.columns) {
                    Ok(positions) => positions,
                    Err(error) => {
                        return RelationalIndexChangeCapture::Invalidated {
                            reason: error.to_string(),
                        };
                    }
                };
                let previous_index_key = previous_row
                    .map(|row| row_key(row, &positions))
                    .filter(|key| index_includes_key(index, key));
                let next_index_key = next_row
                    .map(|row| row_key(row, &positions))
                    .filter(|key| index_includes_key(index, key));
                if previous_index_key == next_index_key {
                    continue;
                }
                if let Some(index_key) = previous_index_key
                    && !push_captured_index_change(
                        &mut changes,
                        &mut encoded_bytes,
                        limits,
                        RelationalIndexChange {
                            table: table.clone(),
                            index: index.name.clone(),
                            index_key,
                            primary_key: primary_key.clone(),
                            kind: RelationalIndexChangeKind::Delete,
                        },
                    )
                {
                    return capture_limit_invalidated(limits);
                }
                if let Some(index_key) = next_index_key
                    && !push_captured_index_change(
                        &mut changes,
                        &mut encoded_bytes,
                        limits,
                        RelationalIndexChange {
                            table: table.clone(),
                            index: index.name.clone(),
                            index_key,
                            primary_key: primary_key.clone(),
                            kind: RelationalIndexChangeKind::Insert,
                        },
                    )
                {
                    return capture_limit_invalidated(limits);
                }
            }
        }
    }
    RelationalIndexChangeCapture::Captured {
        changes,
        encoded_bytes,
    }
}

fn capture_relational_row_changes(
    previous: &RelationalState,
    next: &RelationalState,
    changed_keys: &BTreeMap<String, BTreeSet<RelationalKey>>,
    rewritten_tables: &BTreeSet<String>,
    limits: RelationalRowChangeCaptureLimits,
) -> RelationalRowChangeCapture {
    if !rewritten_tables.is_empty() {
        return RelationalRowChangeCapture::RequiresCheckpoint {
            tables: rewritten_tables.iter().cloned().collect(),
        };
    }

    let mut changes = Vec::new();
    let mut encoded_bytes = 0usize;
    for (table, keys) in changed_keys {
        for primary_key in keys {
            let previous_row = previous.row(table, primary_key);
            let next_row = next.row(table, primary_key);
            if previous_row == next_row {
                continue;
            }
            let change = RelationalRowChange {
                table: table.clone(),
                primary_key: primary_key.clone(),
                row: next_row.cloned(),
            };
            let Some(change_bytes) = estimated_row_change_encoding_bytes(&change) else {
                return row_capture_limit_invalidated(limits);
            };
            let Some(next_bytes) = encoded_bytes.checked_add(change_bytes) else {
                return row_capture_limit_invalidated(limits);
            };
            if changes.len() >= limits.max_entries.get() || next_bytes > limits.max_bytes.get() {
                return row_capture_limit_invalidated(limits);
            }
            changes.push(change);
            encoded_bytes = next_bytes;
        }
    }
    RelationalRowChangeCapture::Captured {
        changes,
        encoded_bytes,
    }
}

fn capture_relational_primary_key_changes(
    previous: &RelationalState,
    next: &RelationalState,
    changed_keys: &BTreeMap<String, BTreeSet<RelationalKey>>,
    rewritten_tables: &BTreeSet<String>,
    limits: RelationalPrimaryKeyChangeCaptureLimits,
) -> RelationalPrimaryKeyChangeCapture {
    if !rewritten_tables.is_empty() {
        return RelationalPrimaryKeyChangeCapture::RequiresRebuild {
            reason: RelationalPrimaryKeyChangeRebuildReason::SchemaRewrite,
        };
    }

    const TABLE_FIXED_BYTES: usize = 4;
    const KEY_FIXED_BYTES: usize = 4;
    let mut tables = Vec::new();
    let mut encoded_bytes = 0usize;
    let mut entry_count = 0usize;
    for (table, keys) in changed_keys {
        let mut primary_keys = Vec::new();
        let Some(table_bytes) = TABLE_FIXED_BYTES.checked_add(table.len()) else {
            return RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                reason: RelationalPrimaryKeyChangeRebuildReason::CaptureLimitExceeded,
            };
        };
        for primary_key in keys {
            if previous.row(table, primary_key) == next.row(table, primary_key) {
                continue;
            }
            let key_bytes = match ordered_key::encode_ordered_relational_key(primary_key) {
                Ok(encoded) => encoded.len(),
                Err(_) => {
                    return RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                        reason: RelationalPrimaryKeyChangeRebuildReason::UnsupportedKeyEncoding,
                    };
                }
            };
            let Some(next_entry_count) = entry_count.checked_add(1) else {
                return RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                    reason: RelationalPrimaryKeyChangeRebuildReason::CaptureLimitExceeded,
                };
            };
            let table_bytes = if primary_keys.is_empty() {
                table_bytes
            } else {
                0
            };
            let Some(next_encoded_bytes) = encoded_bytes
                .checked_add(table_bytes)
                .and_then(|bytes| bytes.checked_add(KEY_FIXED_BYTES))
                .and_then(|bytes| bytes.checked_add(key_bytes))
            else {
                return RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                    reason: RelationalPrimaryKeyChangeRebuildReason::CaptureLimitExceeded,
                };
            };
            if next_entry_count > limits.max_entries.get()
                || next_encoded_bytes > limits.max_bytes.get()
            {
                return RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                    reason: RelationalPrimaryKeyChangeRebuildReason::CaptureLimitExceeded,
                };
            }
            primary_keys.push(primary_key.clone());
            entry_count = next_entry_count;
            encoded_bytes = next_encoded_bytes;
        }
        if !primary_keys.is_empty() {
            tables.push(RelationalTablePrimaryKeyChanges {
                table: table.clone(),
                primary_keys,
            });
        }
    }
    RelationalPrimaryKeyChangeCapture::Captured {
        tables,
        encoded_bytes,
    }
}

fn estimated_row_change_encoding_bytes(change: &RelationalRowChange) -> Option<usize> {
    const FIXED_BYTES: usize = 1 + 3 * 4;
    let key_bytes = ordered_key::encode_ordered_relational_key(&change.primary_key)
        .ok()?
        .len();
    let row_bytes = match &change.row {
        Some(row) => row.estimated_payload_bytes().checked_add(
            row.values()
                .len()
                .checked_mul(std::mem::size_of::<RelationalValue>())?,
        )?,
        None => 0,
    };
    FIXED_BYTES
        .checked_add(change.table.len())?
        .checked_add(key_bytes)?
        .checked_add(row_bytes)
}

fn estimated_replay_access_encoding_bytes(change: &RelationalReplayAccess) -> Option<usize> {
    const FIXED_BYTES: usize = 2 * 8;
    let key_bytes = ordered_key::encode_ordered_relational_key(&change.primary_key)
        .ok()?
        .len();
    FIXED_BYTES
        .checked_add(change.table.len())?
        .checked_add(key_bytes)
}

fn row_capture_limit_invalidated(
    limits: RelationalRowChangeCaptureLimits,
) -> RelationalRowChangeCapture {
    RelationalRowChangeCapture::Invalidated {
        reason: format!(
            "relational row WAL delta cannot be encoded within capture limits: max_entries={}, max_bytes={}",
            limits.max_entries, limits.max_bytes
        ),
    }
}

fn index_includes_key(index: &RelationalIndexDefinition, key: &RelationalKey) -> bool {
    !index.role.is_unique()
        || !key
            .0
            .iter()
            .any(|value| matches!(value, RelationalValue::Null))
}

fn push_captured_index_change(
    changes: &mut Vec<RelationalIndexChange>,
    encoded_bytes: &mut usize,
    limits: RelationalIndexChangeCaptureLimits,
    change: RelationalIndexChange,
) -> bool {
    let Some(change_bytes) = estimated_index_change_encoding_bytes(&change) else {
        return false;
    };
    let Some(next_bytes) = encoded_bytes.checked_add(change_bytes) else {
        return false;
    };
    if changes.len() >= limits.max_entries.get() || next_bytes > limits.max_bytes.get() {
        return false;
    }
    changes.push(change);
    *encoded_bytes = next_bytes;
    true
}

fn capture_limit_invalidated(
    limits: RelationalIndexChangeCaptureLimits,
) -> RelationalIndexChangeCapture {
    RelationalIndexChangeCapture::Invalidated {
        reason: format!(
            "relational index WAL delta cannot be encoded within capture limits: max_entries={}, max_bytes={}",
            limits.max_entries, limits.max_bytes
        ),
    }
}

fn estimated_index_change_encoding_bytes(change: &RelationalIndexChange) -> Option<usize> {
    const FIXED_BYTES: usize = 1 + 4 * 4;
    FIXED_BYTES
        .checked_add(change.table.len())?
        .checked_add(change.index.len())?
        .checked_add(estimated_relational_key_encoding_bytes(&change.index_key)?)?
        .checked_add(estimated_relational_key_encoding_bytes(
            &change.primary_key,
        )?)
}

fn estimated_relational_key_encoding_bytes(key: &RelationalKey) -> Option<usize> {
    key.0.iter().try_fold(0usize, |bytes, value| {
        let value_bytes = match value {
            RelationalValue::Null => 1,
            RelationalValue::Boolean(_) => 2,
            RelationalValue::BigInt(_) | RelationalValue::DoublePrecision(_) => 9,
            RelationalValue::Text(value) => 3usize
                .checked_add(value.as_bytes().iter().filter(|byte| **byte == 0).count())?
                .checked_add(value.len())?,
            RelationalValue::Bytea(value) => 3usize
                .checked_add(value.iter().filter(|byte| **byte == 0).count())?
                .checked_add(value.len())?,
            RelationalValue::Overflow(_) => return None,
        };
        bytes.checked_add(value_bytes)
    })
}

fn validate_foreign_keys(state: &RelationalState) -> Result<(), RelationalError> {
    for (table, schema) in &state.schemas {
        let Some(segment) = state.segments.get(table) else {
            continue;
        };
        for foreign_key in &schema.foreign_keys {
            let local_positions = column_positions(schema, &foreign_key.columns)?;
            let referenced_schema = state
                .schemas
                .get(&foreign_key.referenced_table)
                .ok_or_else(|| {
                    RelationalError::Schema(format!(
                        "foreign key references unknown table {}",
                        foreign_key.referenced_table
                    ))
                })?;
            let referenced_positions =
                column_positions(referenced_schema, &foreign_key.referenced_columns)?;
            let references_unique_key = referenced_schema
                .unique_index_definition(&foreign_key.referenced_columns)
                .is_some();
            if !references_unique_key {
                return Err(RelationalError::Schema(format!(
                    "foreign key from {table} must reference a primary or unique key on {}",
                    foreign_key.referenced_table
                )));
            }
            for (local, referenced) in local_positions.iter().zip(&referenced_positions) {
                if schema.columns[*local].scalar_type
                    != referenced_schema.columns[*referenced].scalar_type
                {
                    return Err(RelationalError::Schema(format!(
                        "foreign key from {table} to {} has incompatible column types",
                        foreign_key.referenced_table
                    )));
                }
            }
            let referenced_rows = state
                .segments
                .get(&foreign_key.referenced_table)
                .ok_or_else(|| {
                    RelationalError::Schema(format!(
                        "missing row segment for table {}",
                        foreign_key.referenced_table
                    ))
                })?;
            let referenced_keys = referenced_rows
                .rows
                .values()
                .map(|row| row_key(row, &referenced_positions))
                .collect::<BTreeSet<_>>();
            for row in segment.rows.values() {
                let key = row_key(row, &local_positions);
                if key
                    .0
                    .iter()
                    .any(|value| matches!(value, RelationalValue::Null))
                {
                    continue;
                }
                if !referenced_keys.contains(&key) {
                    return Err(RelationalError::Constraint(format!(
                        "foreign key from {table} to {} has no visible target",
                        foreign_key.referenced_table
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_foreign_keys_incremental(
    previous: &RelationalState,
    next: &RelationalState,
    changed_keys: &BTreeMap<String, BTreeSet<RelationalKey>>,
    full_validation_tables: &BTreeSet<String>,
) -> Result<(), RelationalError> {
    for (table, schema) in &next.schemas {
        let Some(segment) = next.segments.get(table) else {
            continue;
        };
        for foreign_key in &schema.foreign_keys {
            let local_positions = column_positions(schema, &foreign_key.columns)?;
            let referenced_schema =
                next.schemas
                    .get(&foreign_key.referenced_table)
                    .ok_or_else(|| {
                        RelationalError::Schema(format!(
                            "foreign key references unknown table {}",
                            foreign_key.referenced_table
                        ))
                    })?;
            let referenced_positions =
                column_positions(referenced_schema, &foreign_key.referenced_columns)?;
            validate_foreign_key_shape(
                table,
                schema,
                foreign_key,
                referenced_schema,
                &local_positions,
                &referenced_positions,
            )?;

            if full_validation_tables.contains(table) {
                for row in segment.rows.values() {
                    validate_foreign_key_row(next, table, row, foreign_key, &local_positions)?;
                }
            } else if let Some(keys) = changed_keys.get(table) {
                for key in keys {
                    if let Some(row) = segment.rows.get(key) {
                        validate_foreign_key_row(next, table, row, foreign_key, &local_positions)?;
                    }
                }
            }

            let Some(referenced_changes) = changed_keys.get(&foreign_key.referenced_table) else {
                continue;
            };
            let previous_referenced = previous.segments.get(&foreign_key.referenced_table);
            for changed_key in referenced_changes {
                let Some(previous_row) =
                    previous_referenced.and_then(|segment| segment.rows.get(changed_key))
                else {
                    continue;
                };
                let previous_target = row_key(previous_row, &referenced_positions);
                if foreign_key_target_exists(next, foreign_key, &previous_target)? {
                    continue;
                }
                if segment.rows.values().any(|row| {
                    let local_key = row_key(row, &local_positions);
                    !key_contains_null(&local_key) && local_key == previous_target
                }) {
                    return Err(RelationalError::Constraint(format!(
                        "foreign key from {table} to {} prevents removing a visible target",
                        foreign_key.referenced_table
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_foreign_key_shape(
    table: &str,
    schema: &RelationalTableSchema,
    foreign_key: &RelationalForeignKeySchema,
    referenced_schema: &RelationalTableSchema,
    local_positions: &[usize],
    referenced_positions: &[usize],
) -> Result<(), RelationalError> {
    let references_unique_key = referenced_schema
        .unique_index_definition(&foreign_key.referenced_columns)
        .is_some();
    if !references_unique_key {
        return Err(RelationalError::Schema(format!(
            "foreign key from {table} must reference a primary or unique key on {}",
            foreign_key.referenced_table
        )));
    }
    for (local, referenced) in local_positions.iter().zip(referenced_positions) {
        if schema.columns[*local].scalar_type != referenced_schema.columns[*referenced].scalar_type
        {
            return Err(RelationalError::Schema(format!(
                "foreign key from {table} to {} has incompatible column types",
                foreign_key.referenced_table
            )));
        }
    }
    Ok(())
}

fn validate_foreign_key_row(
    state: &RelationalState,
    table: &str,
    row: &RelationalRow,
    foreign_key: &RelationalForeignKeySchema,
    local_positions: &[usize],
) -> Result<(), RelationalError> {
    let key = row_key(row, local_positions);
    if key_contains_null(&key) || foreign_key_target_exists(state, foreign_key, &key)? {
        return Ok(());
    }
    Err(RelationalError::Constraint(format!(
        "foreign key from {table} to {} has no visible target",
        foreign_key.referenced_table
    )))
}

fn foreign_key_target_exists(
    state: &RelationalState,
    foreign_key: &RelationalForeignKeySchema,
    key: &RelationalKey,
) -> Result<bool, RelationalError> {
    let schema = state
        .schemas
        .get(&foreign_key.referenced_table)
        .ok_or_else(|| {
            RelationalError::Schema(format!(
                "foreign key references unknown table {}",
                foreign_key.referenced_table
            ))
        })?;
    let definition = schema
        .unique_index_definition(&foreign_key.referenced_columns)
        .ok_or_else(|| {
            RelationalError::Schema(format!(
                "foreign key references non-unique columns on {}",
                foreign_key.referenced_table
            ))
        })?;
    if definition.role == RelationalIndexRole::Primary {
        return Ok(state.row(&foreign_key.referenced_table, key).is_some());
    }
    Ok(state
        .index_lookup(&foreign_key.referenced_table, &definition.name, key)
        .is_some_and(|postings| !postings.is_empty()))
}

fn key_contains_null(key: &RelationalKey) -> bool {
    key.0
        .iter()
        .any(|value| matches!(value, RelationalValue::Null))
}

fn column_positions(
    schema: &RelationalTableSchema,
    columns: &[String],
) -> Result<Vec<usize>, RelationalError> {
    columns
        .iter()
        .map(|column| {
            schema.column_position(column).ok_or_else(|| {
                RelationalError::Schema(format!("table {} has no column {column}", schema.name))
            })
        })
        .collect()
}

fn row_key(row: &RelationalRow, positions: &[usize]) -> RelationalKey {
    RelationalKey(
        positions
            .iter()
            .map(|position| row.values[*position].clone())
            .collect(),
    )
}

#[cfg(test)]
#[path = "relational/tests.rs"]
mod tests;
