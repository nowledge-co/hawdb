// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::{
    scan::{FileSegmentRangeReader, SegmentRangeReader, SegmentReadRange},
    snapshot::{SnapshotCommitError, SnapshotCoordinator, SnapshotReadGuard},
};
use hawdb_core::{LogicalType, Uuid};
use hawdb_integrity::Sha256Digest;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::Arc;

mod codec;
pub(crate) use codec::schema_encoding;
mod compaction;
mod constraints;
mod index_shadow;
pub(crate) use index_shadow::{
    IndexReadAdmission, IndexReadCharge, IndexReadObserver, IndexReadPreflightError,
};
mod ordered_key;
pub(crate) mod overflow;
mod recovery;
mod row_page;
pub(crate) use row_page::CheckpointSnapshotPointRequest;

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
    decode_relational_checkpoint_file_with_work_context, decode_relational_row_payload,
    decode_relational_table_schema, decode_relational_wal_batch_owned, decode_row_owned,
    decode_schema_owned, encode_relational_checkpoint_with_work_context,
    encode_relational_row_payload, encode_relational_row_payload_with_work_context,
    encode_relational_table_schema, CheckpointOutputIo,
};
#[doc(hidden)]
pub use compaction::relational_row_page_compaction_publication_config;
pub use compaction::{
    RelationalOverflowCompactionConfig, RelationalOverflowCompactionReport,
    RelationalRowPageCompactionConfig, RelationalRowPageCompactionReport,
};
pub use constraints::RelationalConstraintIndex;
pub use index_shadow::{
    relational_index_recovery_delta_file, relational_index_shadow_artifact_file,
    relational_index_shadow_manifest_generation_file, RelationalIndexArtifactMetadata,
    RelationalIndexGenerationArtifacts, RelationalIndexGenerationIdentity,
    RelationalIndexLiveUnavailable, RelationalIndexPrefixStatistics, RelationalIndexReadLimits,
    RelationalIndexReadReport, RelationalIndexRecoveryBuilder, RelationalIndexRecoveryConfig,
    RelationalIndexRecoveryManifest, RelationalIndexRecoveryReadReport,
    RelationalIndexRecoveryReader, RelationalIndexRecoveryReport, RelationalIndexRootDescriptor,
    RelationalIndexRowSource, RelationalIndexShadowBuildReport,
    RelationalIndexShadowCheckpointReport, RelationalIndexShadowCheckpointStatus,
    RelationalIndexShadowConfig, RelationalIndexShadowError, RelationalIndexShadowManifest,
    RelationalIndexShadowReader, RelationalIndexShadowRecoveryStatus, RelationalIndexShadowState,
    RelationalIndexShadowWriter, RelationalIndexStatistics, DEFAULT_RELATIONAL_INDEX_READ_BYTES,
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
pub(crate) use index_shadow::{
    relational_index_recovery_manifest_generation_file, relational_index_recovery_prefix_file,
};
pub use overflow::{
    relational_overflow_descriptor_file, relational_overflow_extent_file,
    relational_overflow_manifest_generation_file, RelationalHydrationBudget,
    RelationalOverflowArtifactMetadata, RelationalOverflowConfig,
    RelationalOverflowExactGenerationRequest, RelationalOverflowExactPublicationReport,
    RelationalOverflowExtentDescriptor, RelationalOverflowExtentInput,
    RelationalOverflowGenerationArtifacts, RelationalOverflowInputs, RelationalOverflowInputsIter,
    RelationalOverflowPublicationConfig, RelationalOverflowPublicationError,
    RelationalOverflowPublicationPhase, RelationalOverflowPublicationReport,
    RelationalOverflowPublisher, RelationalOverflowRef, RelationalOverflowReferenceSet,
    RelationalOverflowReferenceSetBuilder, RelationalOverflowReferenceSortConfig,
    RelationalOverflowReferenceSortReport, RelationalOverflowRootBinding,
    RelationalOverflowRootManifest, RelationalOverflowRootReader,
    DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES, DEFAULT_RELATIONAL_OVERFLOW_EXTENTS,
    DEFAULT_RELATIONAL_OVERFLOW_MANIFEST_BYTES, DEFAULT_RELATIONAL_OVERFLOW_NEW_EXTENT_BYTES,
    DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_OCCURRENCES, DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_RUNS,
    DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_SORT_MEMORY_BYTES,
    DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_SPILL_BYTES, DEFAULT_RELATIONAL_OVERFLOW_THRESHOLD_BYTES,
    RELATIONAL_OVERFLOW_MANIFEST_FILE,
};
pub(crate) use recovery::{PreparedRelationalRecoverySelector, RELATIONAL_RECOVERY_SOURCE_BYTES};
pub use recovery::{
    RelationalRecoveryFence, RelationalRecoverySourceBuilder, RelationalRecoverySourceIdentity,
};
pub(crate) use row_page::relational_row_delta_prefix_file;
pub use row_page::{
    relational_row_delta_manifest_generation_file, relational_row_delta_run_file,
    relational_row_page_artifact_file, relational_row_page_manifest_generation_file,
    relational_row_page_root_descriptor_file, relational_row_page_root_key_file,
    ImmutableRelationalRowPage, RelationalProjectedField, RelationalProjectedFieldRef,
    RelationalProjectedRow, RelationalProjectedRowRef, RelationalProjectedRowView,
    RelationalRowDeltaBaseBinding, RelationalRowDeltaBuilder, RelationalRowDeltaConfig,
    RelationalRowDeltaError, RelationalRowDeltaGeneration, RelationalRowDeltaManifest,
    RelationalRowDeltaPublicationPhase, RelationalRowDeltaReadReport, RelationalRowDeltaReader,
    RelationalRowDeltaReport, RelationalRowDeltaTableMetadata, RelationalRowLiveUnavailable,
    RelationalRowPageArtifactMetadata, RelationalRowPageBootstrap,
    RelationalRowPageBootstrapReport, RelationalRowPageCheckpointError,
    RelationalRowPageCumulativeReadRemaining, RelationalRowPageCumulativeReadReport,
    RelationalRowPageDemandReadError, RelationalRowPageDemandReadLimits,
    RelationalRowPageDemandReadReport, RelationalRowPageDemandReader, RelationalRowPageEntry,
    RelationalRowPageError, RelationalRowPageGenerationArtifacts,
    RelationalRowPageGenerationRequest, RelationalRowPageId, RelationalRowPageIdAllocator,
    RelationalRowPageLimits, RelationalRowPageLiveError, RelationalRowPageMutationError,
    RelationalRowPageMutationPlan, RelationalRowPageMutationPlanner,
    RelationalRowPagePhysicalGeneration, RelationalRowPageProjectedFields,
    RelationalRowPageProjectedRange, RelationalRowPageProjectedRangeFields,
    RelationalRowPagePublicationConfig, RelationalRowPagePublicationError,
    RelationalRowPagePublicationPhase, RelationalRowPagePublicationReport,
    RelationalRowPagePublisher, RelationalRowPageReadView, RelationalRowPageReadViewIdentity,
    RelationalRowPageRecoveredValue, RelationalRowPageRecoveryStatus,
    RelationalRowPageRewriteConfig, RelationalRowPageRootDescriptor, RelationalRowPageRootManifest,
    RelationalRowPageRootReader, RelationalRowPageServingResources, RelationalRowPageSlotIntegrity,
    RelationalRowPageSnapshotPointReport, RelationalRowPageSnapshotPointsReport,
    RelationalRowPageSnapshotRangeReport, RelationalRowPageSnapshotReadError,
    RelationalRowPageSnapshotReadLimits, RelationalRowPageSnapshotReader,
    RelationalRowPageSnapshotRowSource, RelationalRowPageState, RelationalRowPageTableDelta,
    RelationalRowPageTableRoot, RelationalRowPageView,
    DEFAULT_RELATIONAL_ROW_DELTA_CHECKPOINT_RUNS, DEFAULT_RELATIONAL_ROW_DELTA_DIRTY_BYTES,
    DEFAULT_RELATIONAL_ROW_DELTA_DIRTY_ENTRIES, DEFAULT_RELATIONAL_ROW_DELTA_MANIFEST_BYTES,
    DEFAULT_RELATIONAL_ROW_DELTA_RUNS, DEFAULT_RELATIONAL_ROW_DELTA_RUN_BYTES,
    DEFAULT_RELATIONAL_ROW_PAGE_BYTES, DEFAULT_RELATIONAL_ROW_PAGE_COLUMNS,
    DEFAULT_RELATIONAL_ROW_PAGE_DIRTY_BYTES, DEFAULT_RELATIONAL_ROW_PAGE_DIRTY_PAGES,
    DEFAULT_RELATIONAL_ROW_PAGE_INLINE_VALUE_BYTES, DEFAULT_RELATIONAL_ROW_PAGE_KEY_BYTES,
    DEFAULT_RELATIONAL_ROW_PAGE_MANIFEST_BYTES, DEFAULT_RELATIONAL_ROW_PAGE_READ_BYTES,
    DEFAULT_RELATIONAL_ROW_PAGE_READ_PAGES, DEFAULT_RELATIONAL_ROW_PAGE_READ_PINS,
    DEFAULT_RELATIONAL_ROW_PAGE_READ_ROWS, DEFAULT_RELATIONAL_ROW_PAGE_READ_TREE_HEIGHT,
    DEFAULT_RELATIONAL_ROW_PAGE_ROOT_KEY_BYTES, DEFAULT_RELATIONAL_ROW_PAGE_ROOT_PAGES,
    DEFAULT_RELATIONAL_ROW_PAGE_ROWS, DEFAULT_RELATIONAL_ROW_PAGE_ROW_BYTES,
    DEFAULT_RELATIONAL_ROW_PAGE_TABLES, DEFAULT_RELATIONAL_ROW_PAGE_VALUE_BYTES,
    DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_BYTES, DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_ENTRIES,
    RELATIONAL_ROW_DELTA_MANIFEST_FILE, RELATIONAL_ROW_PAGE_MANIFEST_FILE,
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
    Uuid,
}

impl RelationalScalarType {
    pub const fn logical_type(self) -> LogicalType {
        match self {
            Self::Boolean => LogicalType::Boolean,
            Self::BigInt => LogicalType::Int64,
            Self::DoublePrecision => LogicalType::Float64,
            Self::Text => LogicalType::Text,
            Self::Bytea => LogicalType::Binary,
            Self::Uuid => LogicalType::Uuid,
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
    Uuid(Uuid),
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
    Uuid(Uuid),
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
            Self::Uuid(_) => Some(RelationalScalarType::Uuid),
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
            Self::Uuid(_) => 16,
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
            Self::Uuid(_) => 6,
            Self::Overflow(_) => 7,
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
            Self::Uuid(_) => Some(RelationalScalarType::Uuid),
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
            Self::Uuid(_) => 16,
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
            Self::Uuid(value) => RelationalValue::Uuid(value),
            Self::Overflow(reference) => RelationalValue::Overflow(reference),
        }
    }

    pub(crate) fn to_owned_value_with_allocations(
        self,
        context: &crate::background::CheckpointDecodeContext,
    ) -> Result<RelationalValue, RelationalError> {
        let admission =
            |error: hawdb_core::HawDBError| RelationalError::Admission(error.to_string());
        let value = match self {
            Self::Text(value) => RelationalValue::Text(context.string(value).map_err(admission)?),
            Self::Bytea(value) => {
                let mut bytes = context.vector_capacity(value.len()).map_err(admission)?;
                for chunk in value.chunks(64 * 1024) {
                    let unit = context
                        .start_unit()
                        .map_err(|error| RelationalError::Admission(error.to_string()))?;
                    bytes.extend_from_slice(chunk);
                    unit.finish();
                }
                RelationalValue::Bytea(bytes)
            }
            _ => self.to_owned_value(),
        };
        context
            .checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        Ok(value)
    }

    const fn kind_rank(self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::BigInt(_) => 2,
            Self::DoublePrecision(_) => 3,
            Self::Text(_) => 4,
            Self::Bytea(_) => 5,
            Self::Uuid(_) => 6,
            Self::Overflow(_) => 7,
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
            RelationalValue::Uuid(value) => Self::Uuid(*value),
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
                (Self::Uuid(left), Self::Uuid(right)) => left.cmp(&right),
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
                (Self::Uuid(left), Self::Uuid(right)) => left.cmp(right),
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
            Self::Uuid(value) => value.hash(state),
            Self::Overflow(reference) => reference.hash(state),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalColumnSchema {
    pub name: String,
    pub scalar_type: RelationalScalarType,
    pub nullable: bool,
    pub default: Option<RelationalColumnDefault>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalColumnDefault {
    Literal(RelationalValue),
    UuidV7,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalReferentialAction {
    NoAction,
    Restrict,
    Cascade,
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

#[derive(Debug)]
struct RelationalSchemaData {
    schema: RelationalTableSchema,
    _memory: crate::background::CheckpointAllocationOwner,
}

impl PartialEq for RelationalSchemaData {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema
    }
}
impl Eq for RelationalSchemaData {}

impl RelationalSchemaData {
    fn checkpoint_copy(
        schema: &RelationalTableSchema,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<Arc<Self>, RelationalError> {
        let context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        context
            .reserve(std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let schema = clone_schema_with_allocations(schema, &context)?;
        let unit = work
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let output = Arc::new(Self {
            schema,
            _memory: context.memory.into_inner(),
        });
        unit.finish();
        work.checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        Ok(output)
    }

    fn checkpoint_edit(
        &self,
        work: &crate::background::CheckpointWorkContext,
        edit: impl FnOnce(
            &mut RelationalTableSchema,
            &crate::background::CheckpointDecodeContext,
        ) -> Result<(), RelationalError>,
    ) -> Result<Arc<Self>, RelationalError> {
        let context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        context
            .reserve(std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let mut schema = clone_schema_with_allocations(&self.schema, &context)?;
        edit(&mut schema, &context)?;
        let unit = work
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let output = Arc::new(Self {
            schema,
            _memory: context.memory.into_inner(),
        });
        unit.finish();
        work.checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        Ok(output)
    }
}

fn clone_schema_strings(
    values: &[String],
    context: &crate::background::CheckpointDecodeContext,
) -> Result<Vec<String>, RelationalError> {
    collect_results_with_allocations(
        values
            .iter()
            .map(|value| clone_string_with_allocations(value, Some(context))),
        values.len(),
        Some(context),
    )
}

fn clone_column_with_allocations(
    column: &RelationalColumnSchema,
    context: &crate::background::CheckpointDecodeContext,
) -> Result<RelationalColumnSchema, RelationalError> {
    let default = match &column.default {
        Some(RelationalColumnDefault::Literal(value)) => Some(RelationalColumnDefault::Literal(
            value.as_ref().to_owned_value_with_allocations(context)?,
        )),
        Some(RelationalColumnDefault::UuidV7) => Some(RelationalColumnDefault::UuidV7),
        None => None,
    };
    Ok(RelationalColumnSchema {
        name: clone_string_with_allocations(&column.name, Some(context))?,
        scalar_type: column.scalar_type,
        nullable: column.nullable,
        default,
    })
}

fn clone_index_schema_with_allocations(
    index: &RelationalIndexSchema,
    context: &crate::background::CheckpointDecodeContext,
) -> Result<RelationalIndexSchema, RelationalError> {
    Ok(RelationalIndexSchema {
        name: clone_string_with_allocations(&index.name, Some(context))?,
        columns: clone_schema_strings(&index.columns, context)?,
        unique: index.unique,
    })
}

fn clone_schema_with_allocations(
    schema: &RelationalTableSchema,
    context: &crate::background::CheckpointDecodeContext,
) -> Result<RelationalTableSchema, RelationalError> {
    Ok(RelationalTableSchema {
        name: clone_string_with_allocations(&schema.name, Some(context))?,
        columns: collect_results_with_allocations(
            schema
                .columns
                .iter()
                .map(|column| clone_column_with_allocations(column, context)),
            schema.columns.len(),
            Some(context),
        )?,
        primary_key: clone_schema_strings(&schema.primary_key, context)?,
        unique_constraints: collect_results_with_allocations(
            schema
                .unique_constraints
                .iter()
                .map(|columns| clone_schema_strings(columns, context)),
            schema.unique_constraints.len(),
            Some(context),
        )?,
        foreign_keys: collect_results_with_allocations(
            schema.foreign_keys.iter().map(|key| {
                Ok(RelationalForeignKeySchema {
                    columns: clone_schema_strings(&key.columns, context)?,
                    referenced_table: clone_string_with_allocations(
                        &key.referenced_table,
                        Some(context),
                    )?,
                    referenced_columns: clone_schema_strings(&key.referenced_columns, context)?,
                    on_delete: key.on_delete,
                    on_update: key.on_update,
                })
            }),
            schema.foreign_keys.len(),
            Some(context),
        )?,
        indexes: collect_results_with_allocations(
            schema
                .indexes
                .iter()
                .map(|index| clone_index_schema_with_allocations(index, context)),
            schema.indexes.len(),
            Some(context),
        )?,
    })
}

impl Clone for RelationalSchemaData {
    fn clone(&self) -> Self {
        Self::from(self.schema.clone())
    }
}

impl From<RelationalTableSchema> for RelationalSchemaData {
    fn from(schema: RelationalTableSchema) -> Self {
        Self {
            schema,
            _memory: Default::default(),
        }
    }
}

impl std::ops::Deref for RelationalSchemaData {
    type Target = RelationalTableSchema;
    fn deref(&self) -> &Self::Target {
        &self.schema
    }
}

impl std::ops::DerefMut for RelationalSchemaData {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.schema
    }
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

    /// A non-NULL equality-bound prefix covers all matching rows only if
    /// nullable suffixes cannot be omitted by the unique-index NULL policy.
    pub fn index_prefix_covers_matches(
        &self,
        columns: &[String],
        prefix_len: usize,
        unique: bool,
    ) -> bool {
        prefix_len > 0
            && prefix_len <= columns.len()
            && columns
                .iter()
                .all(|name| self.column_position(name).is_some())
            && (!unique
                || columns[prefix_len..].iter().all(|name| {
                    self.column_position(name)
                        .is_some_and(|p| !self.columns[p].nullable)
                }))
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

impl RelationalKey {
    pub(crate) fn clone_with_allocations(
        &self,
        context: &crate::background::CheckpointDecodeContext,
    ) -> Result<Self, RelationalError> {
        let mut values = context
            .vector_capacity(self.0.len())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        for value in &self.0 {
            let value = value.as_ref().to_owned_value_with_allocations(context)?;
            let unit = context
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            values.push(value);
            unit.finish();
        }
        context
            .checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        Ok(Self(values))
    }
}

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

pub(crate) use ordered_key::{CheckpointKeyEncodeError, CheckpointKeyEncoding};

pub(crate) fn encode_relational_primary_key_with_checkpoint_work(
    key: &RelationalKey,
    work: &crate::background::CheckpointWorkContext,
) -> Result<Vec<u8>, CheckpointKeyEncodeError> {
    ordered_key::encode_ordered_relational_key_with_work_context(key, work)
}

pub fn decode_relational_primary_key(encoded: &[u8]) -> Result<RelationalKey, RelationalError> {
    ordered_key::decode_ordered_relational_key(encoded)
        .map_err(|error| RelationalError::Corruption(error.to_string()))
}

pub(crate) fn decode_relational_primary_key_owned(
    encoded: &[u8],
    allocations: &crate::background::CheckpointDecodeContext,
) -> Result<RelationalKey, RelationalError> {
    ordered_key::decode_ordered_relational_key_owned(encoded, allocations).map_err(|error| {
        match error {
            ordered_key::OrderedRelationalKeyError::Admission(message) => {
                RelationalError::Admission(message)
            }
            error => RelationalError::Corruption(error.to_string()),
        }
    })
}

#[derive(Debug, Clone)]
pub struct RelationalRow {
    values: Arc<Vec<RelationalValue>>,
    _memory: crate::background::CheckpointAllocationOwner,
}

impl PartialEq for RelationalRow {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}
impl Eq for RelationalRow {}

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

impl RelationalIndexChange {
    /// Returns the bounded wire footprint used by index-change capture and
    /// derived live overlays.
    pub fn estimated_encoded_bytes(&self) -> Option<usize> {
        estimated_index_change_encoding_bytes(self)
    }
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
/// externalization. Count-only UPDATE and DELETE outcomes contain no rows.
/// Conflict no-ops contribute to `conflict_rows`, never to `rows` or
/// `affected_rows`.
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
#[derive(Debug)]
pub struct RelationalReplayAccessSet {
    entries: Vec<RelationalReplayAccess>,
    _memory: crate::background::CheckpointAllocationOwner,
}

impl Clone for RelationalReplayAccessSet {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
            _memory: Default::default(),
        }
    }
}

impl PartialEq for RelationalReplayAccessSet {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries
    }
}
impl Eq for RelationalReplayAccessSet {}

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
pub(crate) struct RelationalMaterializedRecoveryStage {
    pub(crate) transaction: RelationalTransaction,
    pub(crate) mutation_limits: RelationalMutationLimits,
    pub(crate) overflow_config: RelationalOverflowConfig,
    pub(crate) index_capture_limits: Option<RelationalIndexChangeCaptureLimits>,
    pub(crate) row_capture_limits: Option<RelationalRowChangeCaptureLimits>,
}

pub(crate) struct RelationalResidentRecoveryStage<'a> {
    pub(crate) transaction: RelationalTransaction,
    pub(crate) mutation_limits: RelationalMutationLimits,
    pub(crate) overflow_config: RelationalOverflowConfig,
    pub(crate) index_capture_limits: RelationalIndexChangeCaptureLimits,
    pub(crate) row_capture_limits: RelationalRowChangeCaptureLimits,
    pub(crate) expected_replay_access: &'a RelationalReplayAccessSet,
}

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
        Ok(Self {
            entries,
            _memory: Default::default(),
        })
    }
}

struct RelationalReplayAccessTracker {
    entries: BTreeMap<String, BTreeSet<RelationalKey>>,
    entry_count: usize,
    encoded_bytes: usize,
    limits: RelationalRowChangeCaptureLimits,
    allocations: Option<crate::background::CheckpointDecodeContext>,
}

impl RelationalReplayAccessTracker {
    fn new(
        limits: RelationalRowChangeCaptureLimits,
        allocations: Option<&crate::background::CheckpointDecodeContext>,
    ) -> Self {
        Self {
            allocations: allocations.map(|context| crate::background::CheckpointDecodeContext {
                work: context.work.clone(),
                memory: Default::default(),
            }),
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
        let entry_bytes =
            estimated_replay_access_encoding_bytes(table, primary_key).ok_or_else(|| {
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
        let (owned_table, owned_key) = if let Some(context) = &self.allocations {
            // One node per insertion conservatively covers B-tree splits.
            let mut bytes = crate::cow::checkpoint_tree_node_bytes::<RelationalKey, ()>();
            if !self.entries.contains_key(table) {
                bytes = bytes
                    .checked_add(crate::cow::checkpoint_tree_node_bytes::<
                        String,
                        BTreeSet<RelationalKey>,
                    >())
                    .ok_or_else(|| {
                        RelationalError::Admission("replay access node capacity overflow".into())
                    })?;
            }
            context
                .reserve(bytes)
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            let key = primary_key.clone_with_allocations(context)?;
            let name = if self.entries.contains_key(table) {
                None
            } else {
                Some(
                    context
                        .string(table)
                        .map_err(|error| RelationalError::Admission(error.to_string()))?,
                )
            };
            context
                .checkpoint()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            (name, key)
        } else {
            (Some(table.to_string()), primary_key.clone())
        };
        let unit = self
            .allocations
            .as_ref()
            .map(|context| context.start_unit())
            .transpose()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        if let Some(keys) = self.entries.get_mut(table) {
            keys.insert(owned_key);
        } else {
            self.entries.insert(
                owned_table.expect("new table has an owned name"),
                BTreeSet::from([owned_key]),
            );
        }
        self.entry_count += 1;
        self.encoded_bytes = encoded_bytes;
        if let Some(unit) = unit {
            unit.finish();
        }
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

    fn finish(self) -> Result<RelationalReplayAccessSet, RelationalError> {
        let allocations = self.allocations;
        let tables = self.entries;
        let entry_count = self.entry_count;
        // Keep the inventory alive through every temporary and error path.
        let mut entries = match &allocations {
            Some(context) => context
                .vector_capacity(entry_count)
                .map_err(|error| RelationalError::Admission(error.to_string()))?,
            None => Vec::with_capacity(entry_count),
        };
        for (table, keys) in tables {
            for primary_key in keys {
                let table = match &allocations {
                    Some(context) => context
                        .string(&table)
                        .map_err(|error| RelationalError::Admission(error.to_string()))?,
                    None => table.clone(),
                };
                let unit = allocations
                    .as_ref()
                    .map(|context| context.start_unit())
                    .transpose()
                    .map_err(|error| RelationalError::Admission(error.to_string()))?;
                entries.push(RelationalReplayAccess { table, primary_key });
                if let Some(unit) = unit {
                    unit.finish();
                }
            }
        }
        if let Some(context) = &allocations {
            context
                .checkpoint()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
        }
        // The conservative tree allowance stays with the moved keys until
        // the access set is destroyed; it cannot refund live key backing.
        Ok(RelationalReplayAccessSet {
            entries,
            _memory: allocations
                .map(|context| context.memory.into_inner())
                .unwrap_or_default(),
        })
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
    pub fn new(mut values: Vec<RelationalValue>) -> Self {
        // Do not retain arbitrary spare capacity supplied by a host caller.
        // Private decoding already constructs the vector at its admitted size.
        values.shrink_to_fit();
        Self::from_checkpoint_values(values)
    }

    fn from_checkpoint_values(values: Vec<RelationalValue>) -> Self {
        // Share the owned vector without an Arc-slice allocation and complete
        // value-array copy. The vector's actual retained capacity still needs
        // accounting in the checkpoint resource ledger.
        Self {
            values: Arc::new(values),
            _memory: Default::default(),
        }
    }

    fn reconstruct_with_allocations<'a>(
        source: &'a RelationalRow,
        allocations: Option<&crate::background::CheckpointDecodeContext>,
        select: impl FnMut(usize) -> Result<RelationalValueRef<'a>, RelationalError>,
    ) -> Result<Self, RelationalError> {
        Self::reconstruct_values_with_allocations(source.values().len(), allocations, select)
    }

    fn reconstruct_values_with_allocations<'a>(
        count: usize,
        allocations: Option<&crate::background::CheckpointDecodeContext>,
        mut select: impl FnMut(usize) -> Result<RelationalValueRef<'a>, RelationalError>,
    ) -> Result<Self, RelationalError> {
        let owned = allocations.map(|context| crate::background::CheckpointDecodeContext {
            work: context.work.clone(),
            memory: Default::default(),
        });
        let mut values = match &owned {
            Some(context) => context
                .vector_capacity(count)
                .map_err(|error| RelationalError::Admission(error.to_string()))?,
            None => Vec::with_capacity(count),
        };
        for ordinal in 0..count {
            let value = select(ordinal)?;
            let value = match &owned {
                Some(context) => value.to_owned_value_with_allocations(context)?,
                None => value.to_owned_value(),
            };
            let unit = owned
                .as_ref()
                .map(|context| context.start_unit())
                .transpose()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            values.push(value);
            if let Some(unit) = unit {
                unit.finish();
            }
        }
        if let Some(context) = owned {
            context
                .reserve(
                    std::mem::size_of::<Vec<RelationalValue>>() + 2 * std::mem::size_of::<usize>(),
                )
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            context
                .checkpoint()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            Ok(Self {
                values: Arc::new(values),
                _memory: context.memory.into_inner(),
            })
        } else {
            Ok(Self::new(values))
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

#[derive(Debug, Default)]
struct RelationalTableSegment {
    rows: RelationalRowPages,
    indexes: BTreeMap<String, RelationalIndexPages>,
    _memory: crate::background::CheckpointAllocationOwner,
}

impl Clone for RelationalTableSegment {
    fn clone(&self) -> Self {
        Self {
            rows: self.rows.clone(),
            indexes: self.indexes.clone(),
            _memory: Default::default(),
        }
    }
}

impl RelationalTableSegment {
    fn checkpoint_empty(
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<Arc<Self>, RelationalError> {
        let context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        context
            .reserve(std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let directory_context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        directory_context
            .reserve(RelationalRowDirectory::checkpoint_ownership_bytes())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let unit = work
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let rows = RelationalRowPages {
            pages: RelationalRowDirectory::from_checkpoint_value(
                Vec::new(),
                directory_context.memory.into_inner(),
            ),
            len: 0,
        };
        let output = Arc::new(Self {
            rows,
            indexes: BTreeMap::new(),
            _memory: context.memory.into_inner(),
        });
        unit.finish();
        work.checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        Ok(output)
    }

    fn checkpoint_clone(
        &self,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<Arc<Self>, RelationalError> {
        let context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        context
            .reserve(std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let mut indexes = BTreeMap::new();
        for (name, pages) in &self.indexes {
            context
                .reserve(crate::cow::checkpoint_tree_node_bytes::<
                    String,
                    RelationalIndexPages,
                >())
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            let name = context
                .string(name)
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            indexes.insert(name, pages.clone());
            unit.finish();
        }
        let unit = work
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let output = Arc::new(Self {
            rows: self.rows.clone(),
            indexes,
            _memory: context.memory.into_inner(),
        });
        unit.finish();
        work.checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        Ok(output)
    }

    fn checkpoint_without_indexes(
        &self,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<Arc<Self>, RelationalError> {
        let context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        context
            .reserve(std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let unit = context
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let output = Arc::new(Self {
            rows: self.rows.clone(),
            indexes: BTreeMap::new(),
            _memory: context.memory.into_inner(),
        });
        unit.finish();
        Ok(output)
    }
}

fn mutable_segment_with_allocations<'a>(
    segment: &'a mut Arc<RelationalTableSegment>,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<&'a mut RelationalTableSegment, RelationalError> {
    if let Some(context) = allocations
        && Arc::strong_count(segment) > 1
    {
        *segment = segment.checkpoint_clone(&context.work)?;
    }
    Ok(Arc::make_mut(segment))
}

const RELATIONAL_ROW_PAGE_MAX_ENTRIES: usize = 256;
const RELATIONAL_ROW_PAGE_TARGET_BYTES: usize = 1024 * 1024;

/// An ordered row map backed by immutable COW pages.
///
/// A relational snapshot shares every page. A mutation clones only the page
/// containing the affected primary key instead of cloning the entire table.
type RelationalRowPage = crate::cow::CowSegment<BTreeMap<RelationalKey, RelationalRow>>;
type RelationalRowDirectory = crate::cow::CowSegment<Vec<RelationalRowPage>>;

#[derive(Debug, Clone, Default)]
struct RelationalRowPages {
    pages: RelationalRowDirectory,
    len: usize,
}

impl RelationalRowPages {
    fn prepare_checkpoint_copy_for_key(
        &mut self,
        key: &RelationalKey,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<(), RelationalError> {
        let Some(index) = self.page_index(key) else {
            return Ok(());
        };
        let copy_directory = self.pages.is_shared();
        let copy_page = copy_directory || self.pages[index].is_shared();
        if !copy_page {
            return Ok(());
        }
        let admission =
            |error: hawdb_core::HawDBError| RelationalError::Admission(error.to_string());
        // Inventories precede temporary buffers, so denied copies destroy data first.
        let directory_allocations = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let page_allocations = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let mut directory = if copy_directory {
            let mut pages = directory_allocations
                .vector_capacity(self.pages.len())
                .map_err(admission)?;
            directory_allocations
                .reserve(RelationalRowDirectory::checkpoint_ownership_bytes())
                .map_err(admission)?;
            for page in self.pages.iter() {
                let unit = work
                    .start_unit()
                    .map_err(|error| RelationalError::Admission(error.to_string()))?;
                pages.push(page.clone());
                unit.finish();
            }
            Some(pages)
        } else {
            None
        };
        let node_bytes = (self.pages[index].len() / 4 + 2)
            .checked_mul(crate::cow::checkpoint_tree_node_bytes::<
                RelationalKey,
                RelationalRow,
            >())
            .ok_or_else(|| RelationalError::Admission("row page copy capacity overflow".into()))?;
        page_allocations.reserve(node_bytes).map_err(admission)?;
        page_allocations
            .reserve(RelationalRowPage::checkpoint_ownership_bytes())
            .map_err(admission)?;
        let mut rows = BTreeMap::new();
        for (key, row) in self.pages[index].iter() {
            let key = key.clone_with_allocations(&page_allocations)?;
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            rows.insert(key, row.clone());
            unit.finish();
        }
        work.checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let replacement = crate::cow::CowSegment::from_checkpoint_value(
            rows,
            page_allocations.memory.into_inner(),
        );
        if let Some(mut pages) = directory.take() {
            pages[index] = replacement;
            self.pages = crate::cow::CowSegment::from_checkpoint_value(
                pages,
                directory_allocations.memory.into_inner(),
            );
        } else {
            self.pages.get_unique_mut().expect("private row directory")[index] = replacement;
        }
        Ok(())
    }

    fn insert_with_allocations(
        &mut self,
        key: RelationalKey,
        row: RelationalRow,
        allocations: Option<&crate::background::CheckpointDecodeContext>,
    ) -> Result<Option<RelationalRow>, RelationalError> {
        let Some(context) = allocations else {
            return Ok(self.insert(key, row));
        };
        let work = &context.work;
        let admission =
            |error: hawdb_core::HawDBError| RelationalError::Admission(error.to_string());
        let directory_context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let page_context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let split_context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let index = self.page_index(&key).unwrap_or(0);
        let previous_len = self.pages.get(index).map_or(0, |page| page.len());
        let capacity =
            self.pages.len().checked_add(1).ok_or_else(|| {
                RelationalError::Admission("row directory growth overflow".into())
            })?;
        let mut directory = directory_context
            .vector_capacity(capacity)
            .map_err(admission)?;
        directory_context
            .reserve(RelationalRowDirectory::checkpoint_ownership_bytes())
            .map_err(admission)?;
        for page in self.pages.iter() {
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            directory.push(page.clone());
            unit.finish();
        }
        let nodes = previous_len
            .checked_add(3)
            .ok_or_else(|| RelationalError::Admission("row page growth overflow".into()))?;
        let bytes = nodes
            .checked_mul(crate::cow::checkpoint_tree_node_bytes::<
                RelationalKey,
                RelationalRow,
            >())
            .and_then(|bytes| {
                bytes.checked_add(2 * RelationalRowPage::checkpoint_ownership_bytes())
            })
            .ok_or_else(|| RelationalError::Admission("row page growth overflow".into()))?;
        page_context.reserve(bytes).map_err(admission)?;
        let mut rows = BTreeMap::new();
        if let Some(page) = self.pages.get(index) {
            for (key, row) in page.iter() {
                let key = key.clone_with_allocations(&page_context)?;
                let unit = work
                    .start_unit()
                    .map_err(|error| RelationalError::Admission(error.to_string()))?;
                rows.insert(key, row.clone());
                unit.finish();
            }
        }
        let key = key.clone_with_allocations(&page_context)?;
        let unit = work
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let previous = rows.insert(key, row);
        unit.finish();
        let mut page_bytes = 0usize;
        for (key, row) in &rows {
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            page_bytes = page_bytes.saturating_add(relational_row_entry_bytes(key, row));
            unit.finish();
        }
        let right = if rows.len() > 1
            && (rows.len() > RELATIONAL_ROW_PAGE_MAX_ENTRIES
                || page_bytes > RELATIONAL_ROW_PAGE_TARGET_BYTES)
        {
            let split_key = rows
                .keys()
                .nth(rows.len() / 2)
                .expect("nonempty split page")
                .clone_with_allocations(&split_context)?;
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            let right = rows.split_off(&split_key);
            unit.finish();
            Some(right)
        } else {
            None
        };
        work.checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let memory = page_context.memory.into_inner();
        let left = crate::cow::CowSegment::from_checkpoint_value(rows, memory.clone());
        if directory.is_empty() {
            directory.push(left);
        } else {
            directory[index] = left;
        }
        if let Some(right) = right {
            directory.insert(
                index + 1,
                crate::cow::CowSegment::from_checkpoint_value(right, memory),
            );
        }
        self.pages = crate::cow::CowSegment::from_checkpoint_value(
            directory,
            directory_context.memory.into_inner(),
        );
        if previous.is_none() {
            self.len += 1;
        }
        Ok(previous)
    }

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
                pages.push(crate::cow::CowSegment::from(std::mem::take(&mut page)));
                page_bytes = 0;
            }
            page_bytes = page_bytes.saturating_add(entry_bytes);
            page.insert(key, row);
        }
        if !page.is_empty() {
            pages.push(crate::cow::CowSegment::from(page));
        }
        Self {
            pages: pages.into(),
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

    fn first_key_after(&self, after: Option<&RelationalKey>) -> Option<&RelationalKey> {
        let Some(after) = after else {
            return self.pages.first()?.first_key_value().map(|(key, _)| key);
        };
        let page = self
            .pages
            .partition_point(|page| page.last_key_value().is_none_or(|(last, _)| last <= after));
        self.pages
            .get(page)?
            .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
            .next()
            .map(|(key, _)| key)
    }

    fn values(&self) -> impl DoubleEndedIterator<Item = &RelationalRow> {
        self.iter().map(|(_, row)| row)
    }

    fn append_value_to_all_with_allocations(
        &mut self,
        value: &RelationalValue,
        allocations: &crate::background::CheckpointDecodeContext,
    ) -> Result<(), RelationalError> {
        let work = &allocations.work;
        let directory_context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let mut pages = directory_context
            .vector_capacity(self.pages.len())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        directory_context
            .reserve(RelationalRowDirectory::checkpoint_ownership_bytes())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        for source in self.pages.iter() {
            let page_context = crate::background::CheckpointDecodeContext {
                work: work.clone(),
                memory: Default::default(),
            };
            let bytes = (source.len() / 4 + 2)
                .checked_mul(crate::cow::checkpoint_tree_node_bytes::<
                    RelationalKey,
                    RelationalRow,
                >())
                .and_then(|bytes| {
                    bytes.checked_add(RelationalRowPage::checkpoint_ownership_bytes())
                })
                .ok_or_else(|| {
                    RelationalError::Admission("schema row page capacity overflow".into())
                })?;
            page_context
                .reserve(bytes)
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            let mut rows = BTreeMap::new();
            for (key, row) in source.iter() {
                let key = key.clone_with_allocations(&page_context)?;
                let count = row.values().len().checked_add(1).ok_or_else(|| {
                    RelationalError::Admission("schema row value count overflow".into())
                })?;
                let row = RelationalRow::reconstruct_values_with_allocations(
                    count,
                    Some(allocations),
                    |ordinal| {
                        Ok(if ordinal + 1 == count {
                            value.as_ref()
                        } else {
                            row.values()[ordinal].as_ref()
                        })
                    },
                )?;
                let unit = work
                    .start_unit()
                    .map_err(|error| RelationalError::Admission(error.to_string()))?;
                rows.insert(key, row);
                unit.finish();
            }
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            pages.push(RelationalRowPage::from_checkpoint_value(
                rows,
                page_context.memory.into_inner(),
            ));
            unit.finish();
        }
        let unit = work
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let replacement = RelationalRowDirectory::from_checkpoint_value(
            pages,
            directory_context.memory.into_inner(),
        );
        unit.finish();
        work.checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        self.pages = replacement;
        Ok(())
    }

    fn append_value_to_all(&mut self, value: &RelationalValue) {
        let pages = &mut *self.pages;
        for page in pages {
            for row in page.values_mut() {
                let mut values = row.values().to_vec();
                values.push(value.clone());
                *row = RelationalRow::new(values);
            }
        }
    }

    fn insert(&mut self, key: RelationalKey, row: RelationalRow) -> Option<RelationalRow> {
        if self.pages.is_empty() {
            self.pages = vec![crate::cow::CowSegment::from(BTreeMap::from([(key, row)]))].into();
            self.len = 1;
            return None;
        }
        let index = self
            .page_index(&key)
            .expect("non-empty relational row pages have a target page");
        let pages = &mut *self.pages;
        let previous = pages[index].insert(key, row);
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
        let pages = &mut *self.pages;
        let removed = pages[index].remove(key);
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
            .filter(|left| {
                other
                    .pages
                    .iter()
                    .any(|right| left.shares_storage_with(right))
            })
            .count()
    }
}

fn relational_row_entry_bytes(key: &RelationalKey, row: &RelationalRow) -> usize {
    relational_row_entry_bytes_with_visit(key, row, &mut || Ok::<_, std::convert::Infallible>(()))
        .unwrap_or_else(|never| match never {})
}

fn relational_row_entry_bytes_with_visit<E>(
    key: &RelationalKey,
    row: &RelationalRow,
    visit: &mut impl FnMut() -> std::result::Result<(), E>,
) -> std::result::Result<usize, E> {
    visit()?;
    let mut payload = 0usize;
    for value in key.0.iter().chain(row.values.iter()) {
        visit()?;
        payload = payload.saturating_add(value.estimated_payload_bytes());
    }
    Ok(std::mem::size_of::<RelationalKey>()
        .saturating_add(std::mem::size_of::<RelationalRow>())
        .saturating_add(std::mem::size_of::<Vec<RelationalValue>>())
        .saturating_add(payload))
}

fn relational_key_payload_bytes(key: &RelationalKey) -> Option<usize> {
    key.0.iter().try_fold(0usize, |bytes, value| {
        bytes.checked_add(value.estimated_payload_bytes())
    })
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
    relational_replay_access_resident_bytes_parts(&entry.table, &entry.primary_key)
}

fn relational_replay_access_resident_bytes_parts(
    table: &str,
    primary_key: &RelationalKey,
) -> Option<usize> {
    let key_payload_bytes = primary_key.0.iter().try_fold(0usize, |bytes, value| {
        bytes.checked_add(value.estimated_payload_bytes())
    })?;
    std::mem::size_of::<RelationalReplayAccess>()
        .checked_add(table.len())?
        .checked_add(std::mem::size_of::<RelationalKey>())?
        .checked_add(key_payload_bytes)
}

fn split_relational_row_page(pages: &mut Vec<RelationalRowPage>, index: usize) {
    let page = &mut *pages[index];
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
    pages.insert(index + 1, right.into());
}

const RELATIONAL_INDEX_PAGE_MAX_KEYS: usize = 256;
const RELATIONAL_POSTING_PAGE_MAX_KEYS: usize = 256;

type RelationalPostingPage = crate::cow::CowSegment<BTreeSet<RelationalKey>>;
type RelationalPostingDirectory = crate::cow::CowSegment<Vec<RelationalPostingPage>>;

#[derive(Debug, Clone, Default)]
struct RelationalKeySetPages {
    pages: RelationalPostingDirectory,
    len: usize,
}

impl RelationalKeySetPages {
    fn checkpoint_singleton(
        key: &RelationalKey,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<Self, RelationalError> {
        let context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        context
            .reserve(RelationalPostingDirectory::checkpoint_ownership_bytes())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let unit = work
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let mut output = Self {
            pages: RelationalPostingDirectory::from_checkpoint_value(
                Vec::new(),
                context.memory.into_inner(),
            ),
            len: 0,
        };
        unit.finish();
        output.change_with_work_context(key, true, work)?;
        Ok(output)
    }

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
        self.insert_with_allocations(key, None)
            .expect("ordinary posting insertion has no allocation refusal")
    }

    fn insert_with_allocations(
        &mut self,
        key: RelationalKey,
        allocations: Option<&crate::background::CheckpointDecodeContext>,
    ) -> Result<bool, RelationalError> {
        match allocations {
            Some(context) => self.change_with_work_context(&key, true, &context.work),
            None => Ok(self.insert_ordinary(key)),
        }
    }

    fn remove(&mut self, key: &RelationalKey) -> bool {
        self.remove_with_allocations(key, None)
            .expect("ordinary posting removal has no allocation refusal")
    }

    fn remove_with_allocations(
        &mut self,
        key: &RelationalKey,
        allocations: Option<&crate::background::CheckpointDecodeContext>,
    ) -> Result<bool, RelationalError> {
        match allocations {
            Some(context) => self.change_with_work_context(key, false, &context.work),
            None => Ok(self.remove_ordinary(key)),
        }
    }

    fn change_with_work_context(
        &mut self,
        key: &RelationalKey,
        insert: bool,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<bool, RelationalError> {
        let target = self.page_index(key);
        let present = target.is_some_and(|index| self.pages[index].contains(key));
        if present == insert {
            return Ok(false);
        }
        let directory_context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let page_context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let split_context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let capacity = self.pages.len().checked_add(1).ok_or_else(|| {
            RelationalError::Admission("posting directory capacity overflow".into())
        })?;
        let mut directory = directory_context
            .vector_capacity(capacity)
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        directory_context
            .reserve(RelationalPostingDirectory::checkpoint_ownership_bytes())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        for page in self.pages.iter() {
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            directory.push(page.clone());
            unit.finish();
        }
        let index = target.unwrap_or(0);
        let previous_len = target.map_or(0, |index| self.pages[index].len());
        let node_bytes = previous_len
            .checked_add(3)
            .and_then(|count| {
                count.checked_mul(crate::cow::checkpoint_tree_node_bytes::<RelationalKey, ()>())
            })
            .and_then(|bytes| {
                bytes.checked_add(2 * RelationalPostingPage::checkpoint_ownership_bytes())
            })
            .ok_or_else(|| RelationalError::Admission("posting page capacity overflow".into()))?;
        page_context
            .reserve(node_bytes)
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let mut page = BTreeSet::new();
        if let Some(index) = target {
            for old in self.pages[index].iter() {
                let owned = old.clone_with_allocations(&page_context)?;
                let unit = work
                    .start_unit()
                    .map_err(|error| RelationalError::Admission(error.to_string()))?;
                page.insert(owned);
                unit.finish();
            }
        }
        let incoming = if insert {
            Some(key.clone_with_allocations(&page_context)?)
        } else {
            None
        };
        let unit = work
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        if let Some(incoming) = incoming {
            page.insert(incoming);
        } else {
            page.remove(key);
        }
        unit.finish();
        let right = if page.len() > RELATIONAL_POSTING_PAGE_MAX_KEYS {
            let split = page
                .iter()
                .nth(page.len() / 2)
                .expect("oversized posting page")
                .clone_with_allocations(&split_context)?;
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            let right = page.split_off(&split);
            unit.finish();
            Some(right)
        } else {
            None
        };
        if page.is_empty() {
            directory.remove(index);
        } else {
            let memory = page_context.memory.into_inner();
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            let page = RelationalPostingPage::from_checkpoint_value(page, memory.clone());
            if target.is_some() {
                directory[index] = page;
            } else {
                directory.push(page);
            }
            if let Some(right) = right {
                directory.insert(
                    index + 1,
                    RelationalPostingPage::from_checkpoint_value(right, memory),
                );
            }
            unit.finish();
        }
        let unit = work
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let output = RelationalPostingDirectory::from_checkpoint_value(
            directory,
            directory_context.memory.into_inner(),
        );
        unit.finish();
        work.checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        self.pages = output;
        self.len = if insert {
            self.len.saturating_add(1)
        } else {
            self.len.saturating_sub(1)
        };
        Ok(true)
    }

    fn insert_ordinary(&mut self, key: RelationalKey) -> bool {
        if self.pages.is_empty() {
            self.pages = vec![BTreeSet::from([key]).into()].into();
            self.len = 1;
            return true;
        }
        let index = self
            .page_index(&key)
            .expect("non-empty posting pages have a target page");
        let pages = &mut *self.pages;
        let inserted = pages[index].insert(key);
        if inserted {
            self.len = self.len.saturating_add(1);
        }
        if pages[index].len() > RELATIONAL_POSTING_PAGE_MAX_KEYS {
            let page = &mut *pages[index];
            let split_key = page
                .iter()
                .nth(page.len() / 2)
                .cloned()
                .expect("oversized posting page is non-empty");
            let right = page.split_off(&split_key);
            pages.insert(index + 1, right.into());
        }
        inserted
    }

    fn remove_ordinary(&mut self, key: &RelationalKey) -> bool {
        let Some(index) = self.page_index(key) else {
            return false;
        };
        if !self.pages[index].contains(key) {
            return false;
        }
        let pages = &mut *self.pages;
        let removed = pages[index].remove(key);
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
            .filter(|left| {
                other
                    .pages
                    .iter()
                    .any(|right| left.shares_storage_with(right))
            })
            .count()
    }
}

type RelationalIndexPage = crate::cow::CowSegment<BTreeMap<RelationalKey, RelationalKeySetPages>>;
type RelationalIndexDirectory = crate::cow::CowSegment<Vec<RelationalIndexPage>>;

#[derive(Debug, Clone, Default)]
struct RelationalIndexPages {
    pages: RelationalIndexDirectory,
}

impl RelationalIndexPages {
    fn checkpoint_empty(
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<Self, RelationalError> {
        let context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        context
            .reserve(RelationalIndexDirectory::checkpoint_ownership_bytes())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let unit = work
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let output = Self {
            pages: RelationalIndexDirectory::from_checkpoint_value(
                Vec::new(),
                context.memory.into_inner(),
            ),
        };
        unit.finish();
        Ok(output)
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

    fn get(&self, key: &RelationalKey) -> Option<&RelationalKeySetPages> {
        self.page_index(key)
            .and_then(|index| self.pages[index].get(key))
    }

    fn insert_posting(&mut self, index_key: RelationalKey, primary_key: RelationalKey) -> bool {
        self.insert_posting_with_allocations(index_key, primary_key, None)
            .expect("ordinary index insertion has no allocation refusal")
    }

    fn insert_posting_with_allocations(
        &mut self,
        index_key: RelationalKey,
        primary_key: RelationalKey,
        allocations: Option<&crate::background::CheckpointDecodeContext>,
    ) -> Result<bool, RelationalError> {
        match allocations {
            Some(context) => {
                self.change_posting_with_work_context(&index_key, &primary_key, true, &context.work)
            }
            None => Ok(self.insert_posting_ordinary(index_key, primary_key)),
        }
    }

    fn remove_posting(&mut self, index_key: &RelationalKey, primary_key: &RelationalKey) {
        self.remove_posting_with_allocations(index_key, primary_key, None)
            .expect("ordinary index removal has no allocation refusal");
    }

    fn remove_posting_with_allocations(
        &mut self,
        index_key: &RelationalKey,
        primary_key: &RelationalKey,
        allocations: Option<&crate::background::CheckpointDecodeContext>,
    ) -> Result<(), RelationalError> {
        match allocations {
            Some(context) => self
                .change_posting_with_work_context(index_key, primary_key, false, &context.work)
                .map(|_| ()),
            None => {
                self.remove_posting_ordinary(index_key, primary_key);
                Ok(())
            }
        }
    }

    fn change_posting_with_work_context(
        &mut self,
        index_key: &RelationalKey,
        primary_key: &RelationalKey,
        insert: bool,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<bool, RelationalError> {
        let target = self.page_index(index_key);
        let present = self.get(index_key).is_some_and(|postings| {
            postings
                .page_index(primary_key)
                .is_some_and(|index| postings.pages[index].contains(primary_key))
        });
        if present == insert {
            return Ok(false);
        }
        let directory_context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let page_context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let split_context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let capacity = self.pages.len().checked_add(1).ok_or_else(|| {
            RelationalError::Admission("index directory capacity overflow".into())
        })?;
        let mut directory = directory_context
            .vector_capacity(capacity)
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        directory_context
            .reserve(RelationalIndexDirectory::checkpoint_ownership_bytes())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        for page in self.pages.iter() {
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            directory.push(page.clone());
            unit.finish();
        }
        let index = target.unwrap_or(0);
        let previous_len = target.map_or(0, |index| self.pages[index].len());
        let node_bytes = previous_len
            .checked_add(3)
            .and_then(|count| {
                count.checked_mul(crate::cow::checkpoint_tree_node_bytes::<
                    RelationalKey,
                    RelationalKeySetPages,
                >())
            })
            .and_then(|bytes| {
                bytes.checked_add(2 * RelationalIndexPage::checkpoint_ownership_bytes())
            })
            .ok_or_else(|| RelationalError::Admission("index page capacity overflow".into()))?;
        page_context
            .reserve(node_bytes)
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let mut page = BTreeMap::new();
        if let Some(index) = target {
            for (key, postings) in self.pages[index].iter() {
                let key = key.clone_with_allocations(&page_context)?;
                let unit = work
                    .start_unit()
                    .map_err(|error| RelationalError::Admission(error.to_string()))?;
                page.insert(key, postings.clone());
                unit.finish();
            }
        }
        if let Some(postings) = page.get_mut(index_key) {
            postings.change_with_work_context(primary_key, insert, work)?;
            if postings.is_empty() {
                page.remove(index_key);
            }
        } else {
            let postings = RelationalKeySetPages::checkpoint_singleton(primary_key, work)?;
            let key = index_key.clone_with_allocations(&page_context)?;
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            page.insert(key, postings);
            unit.finish();
        }
        let right = if page.len() > RELATIONAL_INDEX_PAGE_MAX_KEYS {
            let split = page
                .keys()
                .nth(page.len() / 2)
                .expect("oversized index page")
                .clone_with_allocations(&split_context)?;
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            let right = page.split_off(&split);
            unit.finish();
            Some(right)
        } else {
            None
        };
        if page.is_empty() {
            directory.remove(index);
        } else {
            let memory = page_context.memory.into_inner();
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            let page = RelationalIndexPage::from_checkpoint_value(page, memory.clone());
            if target.is_some() {
                directory[index] = page;
            } else {
                directory.push(page);
            }
            if let Some(right) = right {
                directory.insert(
                    index + 1,
                    RelationalIndexPage::from_checkpoint_value(right, memory),
                );
            }
            unit.finish();
        }
        let unit = work
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let output = RelationalIndexDirectory::from_checkpoint_value(
            directory,
            directory_context.memory.into_inner(),
        );
        unit.finish();
        work.checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        self.pages = output;
        Ok(true)
    }

    fn insert_posting_ordinary(
        &mut self,
        index_key: RelationalKey,
        primary_key: RelationalKey,
    ) -> bool {
        if self.pages.is_empty() {
            let mut postings = RelationalKeySetPages::default();
            postings.insert(primary_key);
            self.pages = vec![BTreeMap::from([(index_key, postings)]).into()].into();
            return true;
        }
        let index = self
            .page_index(&index_key)
            .expect("non-empty index pages have a target page");
        let pages = &mut *self.pages;
        let page = &mut *pages[index];
        let inserted = page.entry(index_key).or_default().insert(primary_key);
        if page.len() > RELATIONAL_INDEX_PAGE_MAX_KEYS {
            let split_key = page
                .keys()
                .nth(page.len() / 2)
                .cloned()
                .expect("oversized index page is non-empty");
            let right = page.split_off(&split_key);
            pages.insert(index + 1, right.into());
        }
        inserted
    }

    fn remove_posting_ordinary(&mut self, index_key: &RelationalKey, primary_key: &RelationalKey) {
        let Some(index) = self.page_index(index_key) else {
            return;
        };
        if !self.pages[index].contains_key(index_key) {
            return;
        }
        let pages = &mut *self.pages;
        let page = &mut *pages[index];
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

#[derive(Debug)]
pub struct RelationalState {
    schemas: BTreeMap<String, Arc<RelationalSchemaData>>,
    segments: BTreeMap<String, Arc<RelationalTableSegment>>,
    overflow_segments: BTreeMap<Sha256Digest, RelationalOverflowSegment>,
    materialized_index_postings_resident: bool,
    materialized_rows_resident: bool,
    canonical_row_metadata_only: bool,
    detached_row_counts: crate::cow::CowSegment<BTreeMap<String, usize>>,
    detached_total_row_count: usize,
    detached_row_bytes: u64,
    _metadata_memory: crate::background::CheckpointAllocationOwner,
}

impl Clone for RelationalState {
    fn clone(&self) -> Self {
        Self {
            schemas: self.schemas.clone(),
            segments: self.segments.clone(),
            overflow_segments: self.overflow_segments.clone(),
            materialized_index_postings_resident: self.materialized_index_postings_resident,
            materialized_rows_resident: self.materialized_rows_resident,
            canonical_row_metadata_only: self.canonical_row_metadata_only,
            detached_row_counts: self.detached_row_counts.clone(),
            detached_total_row_count: self.detached_total_row_count,
            detached_row_bytes: self.detached_row_bytes,
            _metadata_memory: Default::default(),
        }
    }
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
            detached_row_counts: BTreeMap::new().into(),
            detached_total_row_count: 0,
            detached_row_bytes: 0,
            _metadata_memory: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
enum RelationalOverflowSegment {
    Inline(Arc<[u8]>),
    CheckpointInline(crate::background::CheckpointSharedBytes),
    FileRange {
        reader: Arc<FileSegmentRangeReader>,
        range: SegmentReadRange,
    },
}

impl RelationalOverflowSegment {
    fn read(&self) -> Result<RelationalOverflowRead<'_>, RelationalError> {
        self.read_with_work_context(None)
    }

    fn read_with_work_context(
        &self,
        work: Option<&crate::background::CheckpointWorkContext>,
    ) -> Result<RelationalOverflowRead<'_>, RelationalError> {
        if let (Self::FileRange { reader, range }, Some(work)) = (self, work) {
            return reader
                .checkpoint_range_with_work_context(range, work)
                .map(RelationalOverflowRead::Checkpoint)
                .map_err(|error| match error {
                    crate::scan::CheckpointRangeReadError::Read(error) => {
                        RelationalError::Corruption(format!(
                            "failed to read file-backed overflow segment: {error}"
                        ))
                    }
                    crate::scan::CheckpointRangeReadError::Work(error) => {
                        RelationalError::Admission(error.to_string())
                    }
                });
        }
        match self {
            Self::Inline(bytes) => Ok(RelationalOverflowRead::Inline(bytes)),
            Self::CheckpointInline(bytes) => Ok(RelationalOverflowRead::Inline(bytes)),
            Self::FileRange { reader, range } => reader
                .read_range(range)
                .map(RelationalOverflowRead::FileRange)
                .map_err(|error| {
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

enum RelationalOverflowRead<'a> {
    Inline(&'a [u8]),
    FileRange(crate::cache::SegmentBytes),
    Checkpoint(crate::background::CheckpointBytes),
}

impl std::ops::Deref for RelationalOverflowRead<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            Self::Inline(bytes) => bytes,
            Self::FileRange(bytes) => bytes,
            Self::Checkpoint(bytes) => bytes,
        }
    }
}

impl AsRef<[u8]> for RelationalOverflowRead<'_> {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl RelationalState {
    pub(crate) fn clone_metadata_with_work_context(
        &self,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<Self, RelationalError> {
        let admission =
            |error: hawdb_core::HawDBError| RelationalError::Admission(error.to_string());
        let allocations = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        fn reserve_tree<K, V>(
            len: usize,
            context: &crate::background::CheckpointDecodeContext,
        ) -> Result<(), RelationalError> {
            if len == 0 {
                return Ok(());
            }
            let bytes = (len / 4 + 2)
                .checked_mul(crate::cow::checkpoint_tree_node_bytes::<K, V>())
                .ok_or_else(|| {
                    RelationalError::Admission("relational metadata tree capacity overflow".into())
                })?;
            context
                .reserve(bytes)
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            Ok(())
        }
        reserve_tree::<String, Arc<RelationalSchemaData>>(self.schemas.len(), &allocations)?;
        reserve_tree::<String, Arc<RelationalTableSegment>>(self.segments.len(), &allocations)?;
        reserve_tree::<Sha256Digest, RelationalOverflowSegment>(
            self.overflow_segments.len(),
            &allocations,
        )?;
        let count_allocations = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        reserve_tree::<String, usize>(self.detached_row_counts.len(), &count_allocations)?;
        count_allocations
            .reserve(
                crate::cow::CowSegment::<BTreeMap<String, usize>>::checkpoint_ownership_bytes(),
            )
            .map_err(admission)?;
        let mut row_counts = BTreeMap::new();
        for (name, count) in self.detached_row_counts.iter() {
            let name = count_allocations.string(name).map_err(admission)?;
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            row_counts.insert(name, *count);
            unit.finish();
        }
        let detached_row_counts = crate::cow::CowSegment::from_checkpoint_value(
            row_counts,
            count_allocations.memory.into_inner(),
        );
        let mut schemas = BTreeMap::new();
        let mut segments = BTreeMap::new();
        let mut overflow_segments = BTreeMap::new();
        for (name, schema) in &self.schemas {
            let name = allocations.string(name).map_err(admission)?;
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            schemas.insert(name, schema.clone());
            unit.finish();
        }
        for (name, segment) in &self.segments {
            let name = allocations.string(name).map_err(admission)?;
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            segments.insert(name, segment.clone());
            unit.finish();
        }
        for (digest, segment) in &self.overflow_segments {
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            overflow_segments.insert(*digest, segment.clone());
            unit.finish();
        }
        work.checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        Ok(Self {
            schemas,
            segments,
            overflow_segments,
            materialized_index_postings_resident: self.materialized_index_postings_resident,
            materialized_rows_resident: self.materialized_rows_resident,
            canonical_row_metadata_only: self.canonical_row_metadata_only,
            detached_row_counts,
            detached_total_row_count: self.detached_total_row_count,
            detached_row_bytes: self.detached_row_bytes,
            _metadata_memory: allocations.memory.into_inner(),
        })
    }

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
        self.detached_row_counts = counts.into();
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
                .insert(table.table.clone(), Arc::new(table.schema.clone().into()))
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
            detached_row_counts: row_counts.into(),
            detached_total_row_count: total_rows,
            detached_row_bytes: manifest.page_artifact.encoded_len,
            _metadata_memory: Default::default(),
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
        self.detached_row_counts = counts.into();
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

    fn omit_materialized_index_postings_with_work_context(
        &mut self,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<(), RelationalError> {
        if !self.materialized_index_postings_resident {
            return Ok(());
        }
        for segment in self.segments.values_mut() {
            *segment = segment.checkpoint_without_indexes(work)?;
        }
        self.materialized_index_postings_resident = false;
        work.checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        Ok(())
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
                checkpoint_allocations: None,
                checkpoint_access: None,
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
                checkpoint_allocations: None,
                checkpoint_access: None,
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

    pub(crate) fn stage_materialized_recovery_with_allocations(
        &self,
        stage: RelationalMaterializedRecoveryStage,
        allocations: &crate::background::CheckpointDecodeContext,
    ) -> Result<
        (
            Self,
            Option<RelationalIndexChangeCapture>,
            Option<RelationalRowChangeCapture>,
        ),
        RelationalError,
    > {
        let RelationalMaterializedRecoveryStage {
            transaction,
            mutation_limits,
            overflow_config,
            index_capture_limits,
            row_capture_limits,
        } = stage;
        self.require_materialized_rows("relational transaction")?;
        admit_transaction(&transaction, mutation_limits)?;
        let result = apply_transaction_inner(
            self,
            transaction,
            mutation_limits,
            overflow_config,
            TransactionApplyOptions {
                index_capture_limits,
                row_capture_limits,
                checkpoint_allocations: Some(allocations),
                ..TransactionApplyOptions::materialized()
            },
        )?;
        Ok((result.state, result.index_capture, result.row_capture))
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
        self.stage_legacy_transaction_for_authoritative_recovery_with_allocations(
            transaction,
            limits,
            overflow_config,
            capture_limits,
            None,
        )
    }

    pub(crate) fn stage_legacy_transaction_for_authoritative_recovery_with_allocations(
        &self,
        transaction: RelationalTransaction,
        limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
        capture_limits: RelationalIndexChangeCaptureLimits,
        allocations: Option<&crate::background::CheckpointDecodeContext>,
    ) -> Result<(Self, RelationalIndexChangeCapture), RelationalError> {
        self.require_materialized_rows("relational recovery")?;
        admit_transaction(&transaction, limits)?;
        if transaction.changes_index_schema() {
            return Err(RelationalError::Admission(
                "authoritative relational index recovery rejects schema-changing WAL until a new canonical index generation is published"
                    .to_string(),
            ));
        }
        let result = apply_transaction_inner(
            self,
            transaction,
            limits,
            overflow_config,
            TransactionApplyOptions {
                index_capture_limits: Some(capture_limits),
                index_mode: TransactionIndexMode::AuthoritativeRecovery,
                checkpoint_allocations: allocations,
                ..TransactionApplyOptions::materialized()
            },
        )?;
        Ok((
            result.state,
            result.index_capture.expect("legacy index recovery capture"),
        ))
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
                checkpoint_allocations: None,
                checkpoint_access: None,
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
                checkpoint_allocations: None,
                checkpoint_access: None,
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
                checkpoint_allocations: None,
                checkpoint_access: None,
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
                checkpoint_allocations: None,
                checkpoint_access: None,
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
                checkpoint_allocations: None,
                checkpoint_access: None,
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
        self.stage_resident_transaction_for_authoritative_recovery_with_allocations(
            RelationalResidentRecoveryStage {
                transaction,
                mutation_limits: limits,
                overflow_config,
                index_capture_limits,
                row_capture_limits,
                expected_replay_access,
            },
            None,
        )
    }

    pub(crate) fn stage_resident_transaction_for_authoritative_recovery_with_allocations(
        &self,
        stage: RelationalResidentRecoveryStage<'_>,
        allocations: Option<&crate::background::CheckpointDecodeContext>,
    ) -> Result<
        (
            Self,
            RelationalIndexChangeCapture,
            RelationalRowChangeCapture,
        ),
        RelationalError,
    > {
        let RelationalResidentRecoveryStage {
            transaction,
            mutation_limits: limits,
            overflow_config,
            index_capture_limits,
            row_capture_limits,
            expected_replay_access,
        } = stage;
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
                checkpoint_allocations: allocations,
                checkpoint_access: Some(expected_replay_access),
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
        self.stage_sparse_transaction_for_authoritative_recovery_with_allocations(stage, None)
    }

    pub(crate) fn stage_sparse_transaction_for_authoritative_recovery_with_allocations(
        &self,
        stage: RelationalSparseRecoveryStage<'_>,
        allocations: Option<&crate::background::CheckpointDecodeContext>,
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
            allocations,
        )?;
        admit_transaction(&transaction, mutation_limits)?;
        if transaction.changes_index_schema() {
            return Err(RelationalError::Admission("authoritative relational recovery rejects schema-changing WAL until new canonical row and index generations are published".into()));
        }
        let TransactionApplyResult {
            state: staged,
            index_capture,
            row_capture,
            replay_access,
            ..
        } = apply_transaction_inner(
            &workspace,
            transaction,
            mutation_limits,
            overflow_config,
            TransactionApplyOptions {
                index_capture_limits: Some(index_capture_limits),
                row_capture_limits: Some(row_capture_limits),
                replay_access_limits: Some(row_capture_limits),
                primary_key_capture_limits: None,
                constraint_index: None,
                index_mode: TransactionIndexMode::AuthoritativeRecovery,
                capture_mutation_outcomes: false,
                checkpoint_allocations: allocations,
                checkpoint_access: Some(expected_replay_access),
            },
        )?;
        if replay_access.as_ref() != Some(expected_replay_access) {
            return Err(RelationalError::Corruption(
                "relational WAL replay access set does not match the recovered transaction".into(),
            ));
        }
        let index_capture = index_capture.expect("index recovery capture");
        let row_capture = row_capture.expect("row recovery capture");
        let metadata = self.merge_sparse_recovery_workspace_with_work_context(
            &workspace,
            &staged,
            allocations.map(|context| &context.work),
        )?;
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
        allocations: Option<&crate::background::CheckpointDecodeContext>,
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
        self.sparse_workspace_with_allocations(
            hydrated_access,
            limits,
            "sparse relational recovery",
            allocations,
        )
        .map(|(workspace, _)| workspace)
    }

    fn sparse_workspace(
        &self,
        hydrated_access: Vec<RelationalSparseRecoveryRow>,
        limits: RelationalRowChangeCaptureLimits,
        context: &'static str,
    ) -> Result<(Self, Vec<RelationalReplayAccess>), RelationalError> {
        self.sparse_workspace_with_allocations(hydrated_access, limits, context, None)
    }

    fn sparse_workspace_with_allocations(
        &self,
        hydrated_access: Vec<RelationalSparseRecoveryRow>,
        limits: RelationalRowChangeCaptureLimits,
        context: &'static str,
        allocations: Option<&crate::background::CheckpointDecodeContext>,
    ) -> Result<(Self, Vec<RelationalReplayAccess>), RelationalError> {
        if hydrated_access.len() > limits.max_entries.get() {
            return Err(RelationalError::Admission(format!(
                "{context} workspace contains {} entries, exceeding limit {}",
                hydrated_access.len(),
                limits.max_entries
            )));
        }

        let admission =
            |error: hawdb_core::HawDBError| RelationalError::Admission(error.to_string());
        let workspace_allocations =
            allocations.map(|context| crate::background::CheckpointDecodeContext {
                work: context.work.clone(),
                memory: Default::default(),
            });
        let directory_capacity = hydrated_access.len();
        let mut workspace_bytes = 0usize;
        let mut supplied_access = match allocations {
            Some(context) => context
                .vector_capacity(hydrated_access.len())
                .map_err(admission)?,
            None => Vec::with_capacity(hydrated_access.len()),
        };
        let mut schemas = BTreeMap::new();
        let mut segments = BTreeMap::new();
        if let Some(context) = &workspace_allocations {
            let nodes = self.schemas.len().checked_add(2).ok_or_else(|| {
                RelationalError::Admission("workspace metadata capacity overflow".into())
            })?;
            let node_bytes =
                crate::cow::checkpoint_tree_node_bytes::<String, Arc<RelationalSchemaData>>()
                    .checked_add(crate::cow::checkpoint_tree_node_bytes::<
                        String,
                        Arc<RelationalTableSegment>,
                    >())
                    .and_then(|bytes| bytes.checked_mul(nodes))
                    .ok_or_else(|| {
                        RelationalError::Admission("workspace metadata capacity overflow".into())
                    })?;
            context.reserve(node_bytes).map_err(admission)?;
            context
                .reserve(
                    crate::cow::CowSegment::<BTreeMap<String, usize>>::checkpoint_ownership_bytes(),
                )
                .map_err(admission)?;
        }
        for (table, schema) in &self.schemas {
            let (schema_name, segment_name) = match &workspace_allocations {
                Some(context) => {
                    context
                        .reserve(
                            std::mem::size_of::<RelationalTableSegment>()
                                + 2 * std::mem::size_of::<usize>()
                                + RelationalRowDirectory::checkpoint_ownership_bytes(),
                        )
                        .map_err(admission)?;
                    (
                        context.string(table).map_err(admission)?,
                        context.string(table).map_err(admission)?,
                    )
                }
                None => (table.clone(), table.clone()),
            };
            schemas.insert(schema_name, schema.clone());
            segments.insert(segment_name, Arc::new(RelationalTableSegment::default()));
        }
        for hydrated in hydrated_access {
            if supplied_access
                .last()
                .is_some_and(|previous: &RelationalReplayAccess| {
                    (&previous.table, &previous.primary_key)
                        >= (&hydrated.table, &hydrated.primary_key)
                })
            {
                return Err(RelationalError::Corruption(format!(
                    "{context} hydration is not strictly ordered or repeats a key"
                )));
            }
            let entry_bytes =
                relational_sparse_recovery_entry_bytes(&hydrated).ok_or_else(|| {
                    RelationalError::Admission(format!("{context} workspace byte count overflow"))
                })?;
            let access_bytes = relational_replay_access_resident_bytes_parts(
                &hydrated.table,
                &hydrated.primary_key,
            )
            .ok_or_else(|| {
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
            supplied_access.push(match allocations {
                Some(context) => RelationalReplayAccess {
                    table: context.string(&hydrated.table).map_err(admission)?,
                    primary_key: hydrated.primary_key.clone_with_allocations(context)?,
                },
                None => RelationalReplayAccess {
                    table: hydrated.table.clone(),
                    primary_key: hydrated.primary_key.clone(),
                },
            });
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
            let position_allocations = workspace_allocations.as_ref().map(|context| {
                crate::background::CheckpointDecodeContext {
                    work: context.work.clone(),
                    memory: Default::default(),
                }
            });
            let primary_key_positions = column_positions_with_allocations(
                schema,
                &schema.primary_key,
                position_allocations.as_ref(),
            )?;
            if !primary_key_positions
                .iter()
                .map(|position| &row.values()[*position])
                .eq(hydrated.primary_key.0.iter())
            {
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
            let replaced = if let Some(context) = &workspace_allocations {
                // Authenticated hydration is sorted. Append bounded pages by
                // moving keys, avoiding split-key clones and directory growth.
                let entry_bytes = relational_row_entry_bytes(&hydrated.primary_key, &row);
                let new_page = segment.rows.pages.last().is_none_or(|page| {
                    page.len() >= RELATIONAL_ROW_PAGE_MAX_ENTRIES
                        || (!page.is_empty()
                            && page
                                .iter()
                                .map(|(key, row)| relational_row_entry_bytes(key, row))
                                .sum::<usize>()
                                .saturating_add(entry_bytes)
                                > RELATIONAL_ROW_PAGE_TARGET_BYTES)
                });
                context
                    .reserve(crate::cow::checkpoint_tree_node_bytes::<
                        RelationalKey,
                        RelationalRow,
                    >())
                    .map_err(admission)?;
                if segment.rows.pages.is_empty() {
                    let pages = context
                        .vector_capacity(directory_capacity)
                        .map_err(admission)?;
                    context
                        .reserve(RelationalRowDirectory::checkpoint_ownership_bytes())
                        .map_err(admission)?;
                    segment.rows.pages = pages.into();
                }
                if new_page {
                    context
                        .reserve(RelationalRowPage::checkpoint_ownership_bytes())
                        .map_err(admission)?;
                    let unit = context
                        .start_unit()
                        .map_err(|error| RelationalError::Admission(error.to_string()))?;
                    segment
                        .rows
                        .pages
                        .get_unique_mut()
                        .expect("private sparse directory")
                        .push(BTreeMap::new().into());
                    unit.finish();
                }
                let unit = context
                    .start_unit()
                    .map_err(|error| RelationalError::Admission(error.to_string()))?;
                let pages = segment
                    .rows
                    .pages
                    .get_unique_mut()
                    .expect("private sparse directory");
                let replaced = pages
                    .last_mut()
                    .expect("sparse row page")
                    .get_unique_mut()
                    .expect("private sparse row page")
                    .insert(hydrated.primary_key, row);
                if replaced.is_none() {
                    segment.rows.len += 1;
                }
                unit.finish();
                replaced
            } else {
                segment.rows.insert(hydrated.primary_key, row)
            };
            if replaced.is_some() {
                return Err(RelationalError::Corruption(format!(
                    "{context} hydration repeats a key"
                )));
            }
        }

        Ok((
            Self {
                schemas,
                segments,
                overflow_segments: BTreeMap::new(),
                materialized_index_postings_resident: false,
                materialized_rows_resident: true,
                canonical_row_metadata_only: false,
                detached_row_counts: BTreeMap::new().into(),
                detached_total_row_count: 0,
                detached_row_bytes: 0,
                _metadata_memory: workspace_allocations
                    .map_or_else(Default::default, |context| context.memory.into_inner()),
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
                checkpoint_allocations: None,
                checkpoint_access: None,
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
        self.merge_sparse_recovery_workspace_with_work_context(before, after, None)
    }

    fn merge_sparse_recovery_workspace_with_work_context(
        &self,
        before: &RelationalState,
        after: &RelationalState,
        work: Option<&crate::background::CheckpointWorkContext>,
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
        let mut metadata = match work {
            Some(work) => self.clone_metadata_with_work_context(work)?,
            None => self.clone(),
        };
        let counts = &mut *metadata.detached_row_counts;
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
        self.schemas.get(table).map(|schema| &schema.schema)
    }

    pub fn table_schema_digest(
        &self,
        table: &str,
    ) -> Result<Option<hawdb_integrity::Sha256Digest>, RelationalError> {
        self.table_schema(table)
            .map(index_shadow::relational_schema_digest)
            .transpose()
            .map_err(|error| RelationalError::Corruption(error.to_string()))
    }

    pub fn table_schemas(&self) -> impl DoubleEndedIterator<Item = &RelationalTableSchema> {
        self.schemas.values().map(|schema| &schema.schema)
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

    /// Metadata-only ordered seek for native full-source enumeration. A sparse
    /// row state must use its pinned canonical row reader, never an empty map.
    #[doc(hidden)]
    pub fn first_primary_key_after(
        &self,
        table: &str,
        after: Option<&RelationalKey>,
    ) -> Result<Option<&RelationalKey>, RelationalError> {
        self.require_materialized_rows("full-source primary-key enumeration")?;
        Ok(self
            .segments
            .get(table)
            .and_then(|segment| segment.rows.first_key_after(after)))
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
                    RelationalOverflowSegment::CheckpointInline(value) => value.len() as u64,
                    RelationalOverflowSegment::FileRange { range, .. } => range.length.get(),
                })
            });
        row_bytes.saturating_add(overflow_bytes)
    }

    /// Computes both existing checkpoint admission terms in one borrowed,
    /// cooperatively checked traversal. No row/value copies are created.
    #[doc(hidden)]
    pub fn estimated_checkpoint_admission_bytes_with_visit<E>(
        &self,
        visit: &mut impl FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<(u64, u64), E> {
        let mut resident_rows = 0u64;
        if self.materialized_rows_resident {
            for segment in self.segments.values() {
                visit()?;
                for (key, row) in segment.rows.iter() {
                    resident_rows = resident_rows.saturating_add(
                        relational_row_entry_bytes_with_visit(key, row, visit)? as u64,
                    );
                }
            }
        }
        let mut checkpoint = if self.materialized_rows_resident {
            resident_rows
        } else {
            self.detached_row_bytes
        };
        for segment in self.overflow_segments.values() {
            visit()?;
            checkpoint = checkpoint.saturating_add(match segment {
                RelationalOverflowSegment::Inline(value) => value.len() as u64,
                RelationalOverflowSegment::CheckpointInline(value) => value.len() as u64,
                RelationalOverflowSegment::FileRange { range, .. } => range.length.get(),
            });
        }
        Ok((resident_rows, checkpoint))
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
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
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
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<RelationalRow, RelationalError> {
        if self.materialized_rows_resident || !self.canonical_row_metadata_only {
            return Err(RelationalError::Admission(
                "sparse recovery row hydration requires canonical metadata-only state".to_string(),
            ));
        }
        overflow::hydrate_row(self, row, budget, task_context)
    }

    pub(crate) fn hydrate_owned_sparse_recovery_row_with_context(
        &self,
        row: RelationalRow,
        budget: &mut RelationalHydrationBudget,
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
        allocations: Option<&crate::background::CheckpointDecodeContext>,
    ) -> Result<RelationalRow, RelationalError> {
        if self.materialized_rows_resident || !self.canonical_row_metadata_only {
            return Err(RelationalError::Admission(
                "sparse recovery row hydration requires canonical metadata-only state".to_string(),
            ));
        }
        overflow::hydrate_owned_row(self, row, budget, task_context, allocations)
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
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
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
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
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
            // The ordinary API returns a fresh host-owned copy; controlled
            // publishers use CheckpointWrite and preserve admitted ownership.
            Some(RelationalOverflowSegment::CheckpointInline(encoded)) => {
                Some(Arc::from(encoded.as_ref()))
            }
            Some(RelationalOverflowSegment::FileRange { .. }) | None => None,
        }
    }

    /// Returns an inline extent while retaining its encoded allocation ownership.
    #[doc(hidden)]
    pub fn inline_overflow_extent_input(
        &self,
        reference: &RelationalOverflowRef,
    ) -> Option<RelationalOverflowExtentInput> {
        match self.overflow_segments.get(&reference.digest) {
            Some(RelationalOverflowSegment::Inline(encoded)) => {
                Some(RelationalOverflowExtentInput::Write {
                    reference: *reference,
                    encoded: Arc::clone(encoded),
                })
            }
            Some(RelationalOverflowSegment::CheckpointInline(encoded)) => {
                Some(RelationalOverflowExtentInput::CheckpointWrite {
                    reference: *reference,
                    encoded: encoded.clone(),
                })
            }
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
                    RelationalOverflowSegment::CheckpointInline(encoded) => Ok(RelationalOverflowExtentInput::CheckpointWrite { reference, encoded: encoded.clone() }),
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
                        // Publication owns a detached envelope under the
                        // materialization budget above. Ordinary hydration keeps
                        // the tracked read handle and does not copy cached bytes.
                        let encoded = Arc::from(encoded.as_ref());
                        Ok(RelationalOverflowExtentInput::Write { reference, encoded })
                    }
                }
            })
            .collect()
    }

    #[doc(hidden)]
    pub fn overflow_generation_inputs_with_work_context(
        &self,
        has_base_generation: bool,
        max_materialized_bytes: usize,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<RelationalOverflowInputs, RelationalError> {
        overflow::checkpoint::generation_inputs(
            self,
            has_base_generation,
            max_materialized_bytes,
            work,
        )
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
                    Some(RelationalOverflowSegment::CheckpointInline(encoded)) => {
                        Ok(RelationalOverflowExtentInput::CheckpointWrite {
                            reference,
                            encoded: encoded.clone(),
                        })
                    }
                    Some(RelationalOverflowSegment::FileRange { .. }) | None => {
                        Ok(RelationalOverflowExtentInput::Reuse(reference))
                    }
                },
            )
            .collect()
    }

    #[doc(hidden)]
    pub fn overflow_delta_generation_inputs_with_work_context(
        &self,
        deltas: &[RelationalRowPageTableDelta],
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<RelationalOverflowInputs, RelationalError> {
        overflow::checkpoint::delta_inputs(self, deltas, work)
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
    BigIntArithmetic {
        left: RelationalBigIntOperand,
        operator: RelationalBigIntArithmeticOperator,
        right: RelationalBigIntOperand,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalBigIntOperand {
    Column(String),
    Value(RelationalValue),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalBigIntArithmeticOperator {
    Add,
    Subtract,
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
    FileDescriptors(hawdb_core::error::FileDescriptorError),
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
            Self::FileDescriptors(error) => fmt::Display::fmt(error, formatter),
            Self::Durability(message) => {
                write!(formatter, "relational durability error: {message}")
            }
            Self::Corruption(message) => {
                write!(formatter, "relational corruption: {message}")
            }
        }
    }
}

impl std::error::Error for RelationalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::FileDescriptors(error) => Some(error),
            _ => None,
        }
    }
}

impl RelationalError {
    pub(crate) fn into_wal_recovery_error(self) -> hawdb_core::HawDBError {
        match self {
            Self::Corruption(message) => hawdb_core::HawDBError::StorageIntegrity(format!(
                "relational WAL corruption: {message}"
            )),
            error => hawdb_core::HawDBError::from_storage_error(error),
        }
    }

    pub(crate) fn from_io(context: &str, error: std::io::Error) -> Self {
        match hawdb_core::error::file_descriptor_error(&error) {
            Some(error) => Self::FileDescriptors(error),
            None => Self::Durability(format!("{context}: {error}")),
        }
    }

    pub(crate) fn corrupt_io(context: &str, error: std::io::Error) -> Self {
        match hawdb_core::error::file_descriptor_error(&error) {
            Some(error) => Self::FileDescriptors(error),
            None => Self::Corruption(format!("{context}: {error}")),
        }
    }
}

pub(crate) fn admit_transaction(
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
    checkpoint_allocations: Option<&'a crate::background::CheckpointDecodeContext>,
    checkpoint_access: Option<&'a RelationalReplayAccessSet>,
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
            checkpoint_allocations: None,
            checkpoint_access: None,
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
            checkpoint_allocations: None,
            checkpoint_access: None,
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

struct CascadeDelete {
    table: String,
    row: RelationalRow,
}

struct DeleteCascadeContext<'a> {
    limits: RelationalMutationLimits,
    admitted_rows: usize,
    admitted_payload_bytes: usize,
    unbounded_delete_rows: usize,
    unbounded_delete_payload_bytes: usize,
    changed_keys: &'a mut BTreeMap<String, BTreeSet<RelationalKey>>,
    touched: &'a mut BTreeSet<String>,
    replay_access_tracker: Option<&'a mut RelationalReplayAccessTracker>,
    checkpoint_allocations: Option<&'a crate::background::CheckpointDecodeContext>,
    queue_allocations: Option<&'a crate::background::CheckpointDecodeContext>,
}

fn push_cascade_delete_with_allocations(
    queue: &mut VecDeque<CascadeDelete>,
    table: &str,
    row: RelationalRow,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<(), RelationalError> {
    let Some(context) = allocations else {
        queue.push_back(CascadeDelete {
            table: table.to_owned(),
            row,
        });
        return Ok(());
    };
    let table = context
        .string(table)
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    if queue.len() == queue.capacity() {
        let capacity =
            queue.capacity().max(2).checked_mul(2).ok_or_else(|| {
                RelationalError::Admission("cascade queue capacity overflow".into())
            })?;
        let mut replacement = context
            .vector_capacity(capacity)
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        // Conversion reuses the admitted Vec backing. Cancellation while
        // moving requires the caller to discard its private transaction stage.
        while !queue.is_empty() {
            let unit = context
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            replacement.push(queue.pop_front().expect("nonempty cascade queue"));
            unit.finish();
        }
        *queue = VecDeque::from(replacement);
        // Keep conservative credits for replaced capacities until this
        // statement's queue inventory drops, after every queued row.
    }
    let unit = context
        .start_unit()
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    queue.push_back(CascadeDelete { table, row });
    unit.finish();
    Ok(())
}

fn apply_delete_cascades(
    state: &mut RelationalState,
    deleted_rows: &mut VecDeque<CascadeDelete>,
    context: DeleteCascadeContext<'_>,
) -> Result<(), RelationalError> {
    let DeleteCascadeContext {
        limits,
        admitted_rows,
        admitted_payload_bytes,
        unbounded_delete_rows,
        unbounded_delete_payload_bytes,
        changed_keys,
        touched,
        mut replay_access_tracker,
        checkpoint_allocations,
        queue_allocations,
    } = context;
    let mut cascade_rows = 0usize;
    let mut cascade_payload_bytes = 0usize;
    while let Some(CascadeDelete { table, row }) = deleted_rows.pop_front() {
        let referenced_schema = Arc::clone(state.schemas.get(&table).ok_or_else(|| {
            RelationalError::Schema(format!("unknown table {table} during delete cascade"))
        })?);
        let inbound_context =
            checkpoint_allocations.map(|context| crate::background::CheckpointDecodeContext {
                work: context.work.clone(),
                memory: Default::default(),
            });
        let mut inbound = Vec::new();
        for (child_table, child_schema) in &state.schemas {
            for (ordinal, foreign_key) in child_schema.foreign_keys.iter().enumerate() {
                let unit = inbound_context
                    .as_ref()
                    .map(|context| context.start_unit())
                    .transpose()
                    .map_err(|error| RelationalError::Admission(error.to_string()))?;
                if foreign_key.referenced_table == table
                    && foreign_key.on_delete == RelationalReferentialAction::Cascade
                {
                    let name = match &inbound_context {
                        Some(context) => context
                            .string(child_table)
                            .map_err(|error| RelationalError::Admission(error.to_string()))?,
                        None => child_table.clone(),
                    };
                    let entry = (name, Arc::clone(child_schema), ordinal);
                    match &inbound_context {
                        Some(context) => context
                            .push(&mut inbound, entry)
                            .map_err(|error| RelationalError::Admission(error.to_string()))?,
                        None => inbound.push(entry),
                    }
                }
                if let Some(unit) = unit {
                    unit.finish();
                }
            }
        }
        for (child_table, child_schema, ordinal) in inbound {
            let foreign_key = &child_schema.foreign_keys[ordinal];
            let child_context =
                checkpoint_allocations.map(|context| crate::background::CheckpointDecodeContext {
                    work: context.work.clone(),
                    memory: Default::default(),
                });
            let referenced_positions = column_positions_with_allocations(
                &referenced_schema,
                &foreign_key.referenced_columns,
                child_context.as_ref(),
            )?;
            let referenced_key =
                row_key_with_allocations(&row, &referenced_positions, child_context.as_ref())?;
            if key_contains_null(&referenced_key) {
                continue;
            }
            let local_positions = column_positions_with_allocations(
                &child_schema,
                &foreign_key.columns,
                child_context.as_ref(),
            )?;
            let child_segment = state.segments.get(&child_table).ok_or_else(|| {
                RelationalError::Schema(format!(
                    "missing row segment for child table {child_table} during delete cascade"
                ))
            })?;
            let mut child_keys = Vec::new();
            for (key, candidate) in child_segment.rows.iter() {
                let unit = child_context
                    .as_ref()
                    .map(|context| context.start_unit())
                    .transpose()
                    .map_err(|error| RelationalError::Admission(error.to_string()))?;
                if row_matches_key(candidate, &local_positions, &referenced_key) {
                    push_cloned_key_with_allocations(&mut child_keys, key, child_context.as_ref())?;
                }
                if let Some(unit) = unit {
                    unit.finish();
                }
            }
            if child_keys.is_empty() {
                continue;
            }
            for key in child_keys {
                cascade_rows = cascade_rows.checked_add(1).ok_or_else(|| {
                    RelationalError::Admission(
                        "relational delete cascade row count overflow".to_string(),
                    )
                })?;
                let total_rows = admitted_rows
                    .checked_add(unbounded_delete_rows)
                    .and_then(|rows| rows.checked_add(cascade_rows))
                    .ok_or_else(|| {
                        RelationalError::Admission(
                            "relational delete cascade row count overflow".to_string(),
                        )
                    })?;
                if total_rows > limits.max_rows.get() {
                    return Err(RelationalError::Admission(format!(
                        "relational delete cascade affects {total_rows} rows, exceeding max_rows {}",
                        limits.max_rows
                    )));
                }
                cascade_payload_bytes = cascade_payload_bytes
                    .checked_add(relational_key_payload_bytes(&key).ok_or_else(|| {
                        RelationalError::Admission(
                            "relational delete cascade payload byte count overflow".to_string(),
                        )
                    })?)
                    .ok_or_else(|| {
                        RelationalError::Admission(
                            "relational delete cascade payload byte count overflow".to_string(),
                        )
                    })?;
                let total_payload_bytes = admitted_payload_bytes
                    .checked_add(unbounded_delete_payload_bytes)
                    .and_then(|bytes| bytes.checked_add(cascade_payload_bytes))
                    .ok_or_else(|| {
                        RelationalError::Admission(
                            "relational delete cascade payload byte count overflow".to_string(),
                        )
                    })?;
                if total_payload_bytes > limits.max_payload_bytes.get() {
                    return Err(RelationalError::Admission(format!(
                        "relational delete cascade contains {total_payload_bytes} payload bytes, exceeding max_payload_bytes {}",
                        limits.max_payload_bytes
                    )));
                }
                if let Some(tracker) = replay_access_tracker.as_deref_mut() {
                    tracker.record(&child_table, &key)?;
                }
                let changed = changed_table_keys_with_allocations(
                    changed_keys,
                    &child_table,
                    checkpoint_allocations,
                )?;
                insert_changed_key_with_allocations(changed, &key, checkpoint_allocations)?;
                insert_table_name_with_allocations(touched, &child_table, checkpoint_allocations)?;
                let segment = mutable_segment_with_allocations(state.segments.get_mut(&child_table).ok_or_else(|| {
                    RelationalError::Schema(format!("missing row segment for child table {child_table} during delete cascade"))
                })?, checkpoint_allocations)?;
                if let Some(context) = checkpoint_allocations {
                    segment
                        .rows
                        .prepare_checkpoint_copy_for_key(&key, &context.work)?;
                }
                let row = segment
                    .rows
                    .remove(&key)
                    .expect("cascade target was selected from the current child segment");
                push_cascade_delete_with_allocations(
                    deleted_rows,
                    &child_table,
                    row,
                    queue_allocations,
                )?;
            }
        }
    }
    Ok(())
}

fn add_column_with_allocations(
    state: &mut RelationalState,
    table: &str,
    column: &RelationalColumnSchema,
    limits: RelationalMutationLimits,
    overflow_config: RelationalOverflowConfig,
    allocations: &crate::background::CheckpointDecodeContext,
) -> Result<usize, RelationalError> {
    let work = &allocations.work;
    let fill_context = crate::background::CheckpointDecodeContext {
        work: work.clone(),
        memory: Default::default(),
    };
    let source_schema = state
        .schemas
        .get(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    if source_schema.column_position(&column.name).is_some() {
        return Err(RelationalError::Schema(format!(
            "table {table} already has column {}",
            column.name
        )));
    }
    let segment = state.segments.get(table).ok_or_else(|| {
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
    let fill = match &column.default {
        None => RelationalValue::Null,
        Some(RelationalColumnDefault::Literal(value)) => value
            .as_ref()
            .to_owned_value_with_allocations(&fill_context)?,
        Some(RelationalColumnDefault::UuidV7) if row_count == 0 => RelationalValue::Null,
        Some(RelationalColumnDefault::UuidV7) => {
            return Err(RelationalError::Schema(format!(
            "ALTER TABLE {table} cannot add column {} with uuidv7() default to a non-empty table",
            column.name
        )))
        }
    };
    if !column.nullable && matches!(&fill, RelationalValue::Null) && row_count != 0 {
        return Err(RelationalError::Constraint(format!("ALTER TABLE {table} cannot add NOT NULL column {} without a default to a non-empty table", column.name)));
    }
    let mut rewrite_bytes = 0usize;
    for (key, row) in segment.rows.iter() {
        let unit = work
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        rewrite_bytes = rewrite_bytes.saturating_add(
            relational_row_entry_bytes(key, row).saturating_add(fill.estimated_payload_bytes()),
        );
        unit.finish();
    }
    if rewrite_bytes > limits.max_payload_bytes.get() {
        return Err(RelationalError::Admission(format!("ALTER TABLE {table} rewrites {rewrite_bytes} resident bytes, exceeding max_payload_bytes {}", limits.max_payload_bytes)));
    }
    let schema = source_schema.checkpoint_edit(work, |schema, context| {
        let column = clone_column_with_allocations(column, context)?;
        context
            .push(&mut schema.columns, column)
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        validate_table_schema(schema)
    })?;
    let count = schema.columns.len();
    let mut fill_row =
        RelationalRow::reconstruct_values_with_allocations(count, Some(allocations), |ordinal| {
            Ok(if ordinal + 1 == count {
                fill.as_ref()
            } else {
                RelationalValueRef::Null
            })
        })?;
    overflow::externalize_row_with_allocations(
        state,
        &schema,
        &mut fill_row,
        overflow_config,
        Some(allocations),
    )?;
    let fill = fill_row.values().last().expect("added column fill");
    let mut rows = state.segments[table].rows.clone();
    rows.append_value_to_all_with_allocations(fill, allocations)?;
    let segment_context = crate::background::CheckpointDecodeContext {
        work: work.clone(),
        memory: Default::default(),
    };
    segment_context
        .reserve(std::mem::size_of::<RelationalTableSegment>() + 2 * std::mem::size_of::<usize>())
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    let unit = work
        .start_unit()
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    let replacement = Arc::new(RelationalTableSegment {
        rows,
        indexes: BTreeMap::new(),
        _memory: segment_context.memory.into_inner(),
    });
    unit.finish();
    work.checkpoint()
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    *state
        .segments
        .get_mut(table)
        .expect("validated alteration table") = replacement;
    *state
        .schemas
        .get_mut(table)
        .expect("validated alteration schema") = schema;
    Ok(row_count)
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
        checkpoint_allocations,
        checkpoint_access,
    } = options;
    let mut next = match checkpoint_allocations {
        Some(context) => state.clone_metadata_with_work_context(&context.work)?,
        None => state.clone(),
    };
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
            match checkpoint_allocations {
                Some(context) => {
                    next.omit_materialized_index_postings_with_work_context(&context.work)?
                }
                None => next.omit_materialized_index_postings(),
            }
        }
    }
    if let (Some(context), Some(access)) = (checkpoint_allocations, checkpoint_access) {
        for entry in access.entries() {
            let Some(segment) = next.segments.get_mut(&entry.table) else {
                continue;
            };
            if Arc::strong_count(segment) > 1 {
                if !segment.indexes.is_empty() {
                    return Err(RelationalError::Corruption(
                        "controlled recovery row copy requires omitted postings".into(),
                    ));
                }
                *segment = segment.checkpoint_without_indexes(&context.work)?;
            }
            Arc::make_mut(segment)
                .rows
                .prepare_checkpoint_copy_for_key(&entry.primary_key, &context.work)?;
        }
    }
    let admitted_rows = transaction.estimated_mutation_rows();
    let admitted_payload_bytes = transaction.estimated_payload_bytes();
    let mut unbounded_delete_rows = 0usize;
    let mut unbounded_delete_payload_bytes = 0usize;
    let deletion_context =
        checkpoint_allocations.map(|context| crate::background::CheckpointDecodeContext {
            work: context.work.clone(),
            memory: Default::default(),
        });
    let mut deleted_rows = VecDeque::new();
    let mut touched = BTreeSet::new();
    let mut changed_keys = BTreeMap::<String, BTreeSet<RelationalKey>>::new();
    let mut replay_access_tracker = replay_access_limits
        .map(|limits| RelationalReplayAccessTracker::new(limits, checkpoint_allocations));
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
                insert_table_name_with_allocations(
                    &mut touched,
                    &schema.name,
                    checkpoint_allocations,
                )?;
                insert_table_name_with_allocations(
                    &mut full_index_rebuild,
                    &schema.name,
                    checkpoint_allocations,
                )?;
                match checkpoint_allocations {
                    Some(context) => {
                        let owned_schema =
                            RelationalSchemaData::checkpoint_copy(&schema, &context.work)?;
                        let segment = RelationalTableSegment::checkpoint_empty(&context.work)?;
                        let map_context = crate::background::CheckpointDecodeContext {
                            work: context.work.clone(),
                            memory: Default::default(),
                        };
                        map_context
                            .reserve(crate::cow::checkpoint_tree_node_bytes::<
                                String,
                                Arc<RelationalSchemaData>,
                            >())
                            .map_err(|error| RelationalError::Admission(error.to_string()))?;
                        map_context
                            .reserve(crate::cow::checkpoint_tree_node_bytes::<
                                String,
                                Arc<RelationalTableSegment>,
                            >())
                            .map_err(|error| RelationalError::Admission(error.to_string()))?;
                        let schema_name =
                            clone_string_with_allocations(&schema.name, Some(&map_context))?;
                        let segment_name =
                            clone_string_with_allocations(&schema.name, Some(&map_context))?;
                        let mut memory = map_context.memory.into_inner();
                        next._metadata_memory
                            .append_retained(&mut memory, &context.work)
                            .map_err(|error| RelationalError::Admission(error.to_string()))?;
                        let unit = context
                            .start_unit()
                            .map_err(|error| RelationalError::Admission(error.to_string()))?;
                        next.segments.insert(segment_name, segment);
                        next.schemas.insert(schema_name, owned_schema);
                        unit.finish();
                    }
                    None => {
                        next.segments.insert(
                            schema.name.clone(),
                            Arc::new(RelationalTableSegment::default()),
                        );
                        next.schemas
                            .insert(schema.name.clone(), Arc::new(schema.into()));
                    }
                }
            }
            RelationalWrite::AddColumn { table, column } => {
                if let Some(context) = checkpoint_allocations {
                    let row_count = add_column_with_allocations(
                        &mut next,
                        &table,
                        &column,
                        limits,
                        overflow_config,
                        context,
                    )?;
                    insert_table_name_with_allocations(
                        &mut full_index_rebuild,
                        &table,
                        checkpoint_allocations,
                    )?;
                    if row_count != 0 {
                        insert_table_name_with_allocations(
                            &mut projection_rebuild_tables,
                            &table,
                            checkpoint_allocations,
                        )?;
                    }
                    insert_table_name_with_allocations(
                        &mut touched,
                        &table,
                        checkpoint_allocations,
                    )?;
                    continue;
                }

                let mut schema = next
                    .schemas
                    .get(&table)
                    .map(|schema| schema.schema.clone())
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
                let fill = match column.default.clone() {
                    None => RelationalValue::Null,
                    Some(RelationalColumnDefault::Literal(value)) => value,
                    Some(RelationalColumnDefault::UuidV7) if row_count == 0 => {
                        RelationalValue::Null
                    }
                    Some(RelationalColumnDefault::UuidV7) => {
                        return Err(RelationalError::Schema(format!(
                            "ALTER TABLE {table} cannot add column {} with uuidv7() default to a non-empty table",
                            column.name
                        )));
                    }
                };
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
                next.schemas.insert(table.clone(), Arc::new(schema.into()));
                insert_table_name_with_allocations(
                    &mut full_index_rebuild,
                    &table,
                    checkpoint_allocations,
                )?;
                if row_count != 0 {
                    insert_table_name_with_allocations(
                        &mut projection_rebuild_tables,
                        &table,
                        checkpoint_allocations,
                    )?;
                }
                insert_table_name_with_allocations(&mut touched, &table, checkpoint_allocations)?;
            }
            RelationalWrite::CreateIndex { table, index } => {
                let schema = next
                    .schemas
                    .get_mut(&table)
                    .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
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
                match checkpoint_allocations {
                    Some(context) => {
                        *schema =
                            schema.checkpoint_edit(&context.work, |schema, allocations| {
                                let index =
                                    clone_index_schema_with_allocations(&index, allocations)?;
                                allocations
                                    .push(&mut schema.indexes, index)
                                    .map_err(|error| RelationalError::Admission(error.to_string()))
                            })?;
                    }
                    None => Arc::make_mut(schema).indexes.push(index),
                }
                insert_table_name_with_allocations(
                    &mut full_index_rebuild,
                    &table,
                    checkpoint_allocations,
                )?;
                insert_table_name_with_allocations(&mut touched, &table, checkpoint_allocations)?;
            }
            RelationalWrite::Insert { table, rows, mode } => {
                let schema =
                    Arc::clone(next.schemas.get(&table).ok_or_else(|| {
                        RelationalError::Schema(format!("unknown table {table}"))
                    })?);
                let preparation_context = checkpoint_allocations.map(|context| {
                    crate::background::CheckpointDecodeContext {
                        work: context.work.clone(),
                        memory: Default::default(),
                    }
                });
                let primary_key = column_positions_with_allocations(
                    &schema,
                    &schema.primary_key,
                    preparation_context.as_ref(),
                )?;
                let affected_rows = rows.len();
                let logical_rows = capture_mutation_outcomes.then(|| rows.clone());
                let mut prepared_rows = match &preparation_context {
                    Some(context) => context
                        .vector_capacity(rows.len())
                        .map_err(|error| RelationalError::Admission(error.to_string()))?,
                    None => Vec::with_capacity(rows.len()),
                };
                for mut row in rows {
                    validate_row(&schema, &row)?;
                    overflow::externalize_row_with_allocations(
                        &mut next,
                        &schema,
                        &mut row,
                        overflow_config,
                        checkpoint_allocations,
                    )?;
                    let unit = preparation_context
                        .as_ref()
                        .map(|context| context.start_unit())
                        .transpose()
                        .map_err(|error| RelationalError::Admission(error.to_string()))?;
                    prepared_rows.push(row);
                    if let Some(unit) = unit {
                        unit.finish();
                    }
                }
                let segment = next.segments.get_mut(&table).ok_or_else(|| {
                    RelationalError::Schema(format!("missing row segment for table {table}"))
                })?;
                let segment = mutable_segment_with_allocations(segment, checkpoint_allocations)?;
                for row in prepared_rows {
                    let key = row_key_with_allocations(&row, &primary_key, checkpoint_allocations)?;
                    if mode == RelationalInsertMode::Error && segment.rows.contains_key(&key) {
                        return Err(RelationalError::Constraint(format!(
                            "duplicate primary key in table {table}"
                        )));
                    }
                    let keys = changed_table_keys_with_allocations(
                        &mut changed_keys,
                        &table,
                        checkpoint_allocations,
                    )?;
                    insert_changed_key_with_allocations(keys, &key, checkpoint_allocations)?;
                    segment
                        .rows
                        .insert_with_allocations(key, row, checkpoint_allocations)?;
                }
                insert_table_name_with_allocations(&mut touched, &table, checkpoint_allocations)?;
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
                        changed_keys: changed_table_keys_with_allocations(
                            &mut changed_keys,
                            &table,
                            checkpoint_allocations,
                        )?,
                        constraint_index,
                        replay_access_tracker: replay_access_tracker.as_mut(),
                        capture_mutation_outcome: capture_mutation_outcomes,
                        checkpoint_allocations,
                    },
                )?;
                insert_table_name_with_allocations(&mut touched, &table, checkpoint_allocations)?;
                if capture_mutation_outcomes {
                    mutation_outcomes.push(outcome);
                }
            }
            RelationalWrite::DeleteByPrimaryKey { table, keys } => {
                let segment = next
                    .segments
                    .get_mut(&table)
                    .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
                let segment = mutable_segment_with_allocations(segment, checkpoint_allocations)?;
                for key in keys {
                    let changed = changed_table_keys_with_allocations(
                        &mut changed_keys,
                        &table,
                        checkpoint_allocations,
                    )?;
                    insert_changed_key_with_allocations(changed, &key, checkpoint_allocations)?;
                    if let Some(context) = checkpoint_allocations {
                        segment
                            .rows
                            .prepare_checkpoint_copy_for_key(&key, &context.work)?;
                    }
                    if let Some(row) = segment.rows.remove(&key) {
                        push_cascade_delete_with_allocations(
                            &mut deleted_rows,
                            &table,
                            row,
                            deletion_context.as_ref(),
                        )?;
                    }
                }
                insert_table_name_with_allocations(&mut touched, &table, checkpoint_allocations)?;
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
                let segment = mutable_segment_with_allocations(segment, checkpoint_allocations)?;
                let match_context = checkpoint_allocations.map(|context| {
                    crate::background::CheckpointDecodeContext {
                        work: context.work.clone(),
                        memory: Default::default(),
                    }
                });
                let mut keys = Vec::new();
                for (key, row) in segment.rows.iter() {
                    if let Some(tracker) = replay_access_tracker.as_mut() {
                        tracker.record(&table, key)?;
                    }
                    if predicate_truth(schema, row, &predicate)? == Some(true) {
                        push_cloned_key_with_allocations(&mut keys, key, match_context.as_ref())?;
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
                unbounded_delete_rows =
                    unbounded_delete_rows
                        .checked_add(keys.len())
                        .ok_or_else(|| {
                            RelationalError::Admission(
                                "relational DELETE row count overflow".to_string(),
                            )
                        })?;
                if admitted_rows
                    .checked_add(unbounded_delete_rows)
                    .is_none_or(|rows| rows > limits.max_rows.get())
                {
                    return Err(RelationalError::Admission(format!(
                        "relational mutation affects more than max_rows {}",
                        limits.max_rows
                    )));
                }
                let delete_payload_bytes = keys.iter().try_fold(0usize, |bytes, key| {
                    bytes
                        .checked_add(relational_key_payload_bytes(key).ok_or_else(|| {
                            RelationalError::Admission(
                                "relational DELETE payload byte count overflow".to_string(),
                            )
                        })?)
                        .ok_or_else(|| {
                            RelationalError::Admission(
                                "relational DELETE payload byte count overflow".to_string(),
                            )
                        })
                })?;
                unbounded_delete_payload_bytes = unbounded_delete_payload_bytes
                    .checked_add(delete_payload_bytes)
                    .ok_or_else(|| {
                        RelationalError::Admission(
                            "relational DELETE payload byte count overflow".to_string(),
                        )
                    })?;
                if admitted_payload_bytes
                    .checked_add(unbounded_delete_payload_bytes)
                    .is_none_or(|bytes| bytes > limits.max_payload_bytes.get())
                {
                    return Err(RelationalError::Admission(format!(
                        "relational DELETE contains more than max_payload_bytes {}",
                        limits.max_payload_bytes
                    )));
                }
                let affected_rows = keys.len();
                for key in keys {
                    let changed = changed_table_keys_with_allocations(
                        &mut changed_keys,
                        &table,
                        checkpoint_allocations,
                    )?;
                    insert_changed_key_with_allocations(changed, &key, checkpoint_allocations)?;
                    if let Some(context) = checkpoint_allocations {
                        segment
                            .rows
                            .prepare_checkpoint_copy_for_key(&key, &context.work)?;
                    }
                    if let Some(row) = segment.rows.remove(&key) {
                        push_cascade_delete_with_allocations(
                            &mut deleted_rows,
                            &table,
                            row,
                            deletion_context.as_ref(),
                        )?;
                    }
                }
                if capture_mutation_outcomes {
                    mutation_outcomes.push(RelationalMutationOutcome {
                        table: table.clone(),
                        affected_rows,
                        conflict_rows: 0,
                        rows: Vec::new(),
                    });
                }
                insert_table_name_with_allocations(&mut touched, &table, checkpoint_allocations)?;
            }
            RelationalWrite::UpdateWhere {
                table,
                assignments,
                predicate,
            } => {
                let affected_rows = apply_update(
                    &mut next,
                    &table,
                    &assignments,
                    &predicate,
                    UpdateApplyContext {
                        limits,
                        overflow_config,
                        changed_keys: changed_table_keys_with_allocations(
                            &mut changed_keys,
                            &table,
                            checkpoint_allocations,
                        )?,
                        replay_access_tracker: replay_access_tracker.as_mut(),

                        checkpoint_allocations,
                    },
                )?;
                if capture_mutation_outcomes {
                    mutation_outcomes.push(RelationalMutationOutcome {
                        table: table.clone(),
                        affected_rows,
                        conflict_rows: 0,
                        rows: Vec::new(),
                    });
                }
                insert_table_name_with_allocations(&mut touched, &table, checkpoint_allocations)?;
            }
        }
    }
    apply_delete_cascades(
        &mut next,
        &mut deleted_rows,
        DeleteCascadeContext {
            limits,
            admitted_rows,
            admitted_payload_bytes,
            unbounded_delete_rows,
            unbounded_delete_payload_bytes,
            changed_keys: &mut changed_keys,
            touched: &mut touched,
            replay_access_tracker: replay_access_tracker.as_mut(),
            checkpoint_allocations,
            queue_allocations: deletion_context.as_ref(),
        },
    )?;
    overflow::prune_unreachable_segments_with_allocations(&mut next, checkpoint_allocations)?;
    if index_mode == TransactionIndexMode::Materialized {
        for table in &touched {
            if full_index_rebuild.contains(table) {
                match checkpoint_allocations {
                    Some(context) => rebuild_indexes_with_allocations(&mut next, table, context)?,
                    None => rebuild_indexes(&mut next, table)?,
                }
            } else if let Some(keys) = changed_keys.get(table) {
                match checkpoint_allocations {
                    Some(context) => refresh_indexes_for_keys_with_allocations(
                        state,
                        &mut next,
                        table,
                        keys,
                        true,
                        Some(context),
                    )?,
                    None => refresh_indexes_for_keys(state, &mut next, table, keys, true)?,
                }
            }
        }
    }
    let index_capture = index_capture_limits
        .map(|capture_limits| match checkpoint_allocations {
            Some(context) => capture_relational_index_changes_with_allocations(
                state,
                &next,
                &changed_keys,
                &full_index_rebuild,
                capture_limits,
                Some(context),
            ),
            None => Ok(capture_relational_index_changes(
                state,
                &next,
                &changed_keys,
                &full_index_rebuild,
                capture_limits,
            )),
        })
        .transpose()?;
    let row_capture = row_capture_limits
        .map(|capture_limits| match checkpoint_allocations {
            Some(context) => capture_relational_row_changes_with_allocations(
                state,
                &next,
                &changed_keys,
                &full_index_rebuild,
                capture_limits,
                Some(context),
            ),
            None => Ok(capture_relational_row_changes(
                state,
                &next,
                &changed_keys,
                &full_index_rebuild,
                capture_limits,
            )),
        })
        .transpose()?;
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
        Some(tracker.finish()?)
    } else {
        None
    };
    match (index_mode, constraint_index) {
        (TransactionIndexMode::Materialized, None) => {
            validate_foreign_keys_incremental(
                state,
                &next,
                &changed_keys,
                &full_index_rebuild,
                checkpoint_allocations,
            )?;
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
    checkpoint_allocations: Option<&'a crate::background::CheckpointDecodeContext>,
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
        checkpoint_allocations,
    } = index_context;
    let schema = Arc::clone(
        state
            .schemas
            .get(table)
            .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?,
    );
    let assignment_context =
        checkpoint_allocations.map(|context| crate::background::CheckpointDecodeContext {
            work: context.work.clone(),
            memory: Default::default(),
        });
    let conflict_positions =
        column_positions_with_allocations(&schema, conflict_columns, assignment_context.as_ref())?;
    if conflict_columns.is_empty()
        || !(conflict_columns == schema.primary_key
            || schema
                .unique_constraints
                .iter()
                .any(|columns| columns == conflict_columns)
            || schema
                .indexes
                .iter()
                .any(|index| index.unique && index.columns == conflict_columns))
    {
        return Err(RelationalError::Schema(format!(
            "UPSERT conflict target on table {table} must name a primary or unique key"
        )));
    }
    let primary_key_positions = column_positions_with_allocations(
        &schema,
        &schema.primary_key,
        assignment_context.as_ref(),
    )?;
    let assignments = match action {
        RelationalConflictAction::DoNothing => Vec::new(),
        RelationalConflictAction::Update(assignments) => collect_results_with_allocations(
            assignments.iter().map(|assignment| {
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
            }),
            assignments.len(),
            assignment_context.as_ref(),
        )?,
    };
    let conflict_context =
        checkpoint_allocations.map(|context| crate::background::CheckpointDecodeContext {
            work: context.work.clone(),
            memory: Default::default(),
        });
    let mut staged_conflicts = BTreeMap::<RelationalKey, Option<RelationalKey>>::new();
    let mut affected_rows = 0usize;
    let mut conflict_rows = 0usize;
    let mut returned_rows = Vec::new();
    for primary_key in changed_keys.iter() {
        let Some(row) = state.row(table, primary_key) else {
            continue;
        };
        let conflict_key =
            row_key_with_allocations(row, &conflict_positions, conflict_context.as_ref())?;
        if key_contains_null(&conflict_key) {
            continue;
        }
        let primary_key = clone_key_with_allocations(primary_key, conflict_context.as_ref())?;
        if insert_staged_conflict_with_allocations(
            &mut staged_conflicts,
            conflict_key,
            Some(primary_key),
            conflict_context.as_ref(),
        )?
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
        overflow::externalize_row_with_allocations(
            state,
            &schema,
            &mut excluded,
            overflow_config,
            checkpoint_allocations,
        )?;
        let conflict_key =
            row_key_with_allocations(&excluded, &conflict_positions, conflict_context.as_ref())?;
        let conflict_has_null = conflict_key
            .0
            .iter()
            .any(|value| matches!(value, RelationalValue::Null));
        let existing_primary_key = if conflict_has_null {
            None
        } else if let Some(staged) = staged_conflicts.get(&conflict_key) {
            staged
                .as_ref()
                .map(|key| clone_key_with_allocations(key, conflict_context.as_ref()))
                .transpose()?
        } else {
            conflict_primary_key(
                state,
                table,
                &schema,
                conflict_columns,
                &conflict_key,
                changed_keys,
                RelationalConflictProbeContext {
                    constraint_index,
                    allocations: conflict_context.as_ref(),
                },
            )?
        };

        let segment = state
            .segments
            .get_mut(table)
            .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
        let segment = mutable_segment_with_allocations(segment, checkpoint_allocations)?;
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
            let updated = RelationalRow::reconstruct_with_allocations(
                existing,
                checkpoint_allocations,
                |ordinal| {
                    let Some((_, source, assignment)) = assignments
                        .iter()
                        .rev()
                        .find(|(target, _, _)| *target == ordinal)
                    else {
                        return Ok(existing.values()[ordinal].as_ref());
                    };
                    Ok(match (source, *assignment) {
                        (Some(source), _) => excluded.values()[*source].as_ref(),
                        (None, RelationalUpsertValue::Value(value)) => value.as_ref(),
                        (None, RelationalUpsertValue::ExcludedColumn(_)) => {
                            unreachable!("excluded source position was resolved")
                        }
                    })
                },
            )?;
            validate_row(&schema, &updated)?;
            let updated_primary_key =
                row_key_with_allocations(&updated, &primary_key_positions, checkpoint_allocations)?;
            let updated_conflict_key =
                row_key_with_allocations(&updated, &conflict_positions, checkpoint_allocations)?;
            insert_changed_key_with_allocations(
                changed_keys,
                &existing_primary_key,
                checkpoint_allocations,
            )?;
            insert_changed_key_with_allocations(
                changed_keys,
                &updated_primary_key,
                checkpoint_allocations,
            )?;
            if let Some(context) = checkpoint_allocations {
                segment
                    .rows
                    .prepare_checkpoint_copy_for_key(&existing_primary_key, &context.work)?;
            }
            segment.rows.remove(&existing_primary_key);
            segment.rows.insert_with_allocations(
                clone_key_with_allocations(&updated_primary_key, conflict_context.as_ref())?,
                updated,
                checkpoint_allocations,
            )?;
            insert_staged_conflict_with_allocations(
                &mut staged_conflicts,
                conflict_key,
                None,
                conflict_context.as_ref(),
            )?;
            if !key_contains_null(&updated_conflict_key) {
                insert_staged_conflict_with_allocations(
                    &mut staged_conflicts,
                    updated_conflict_key,
                    Some(updated_primary_key),
                    conflict_context.as_ref(),
                )?;
            }
            affected_rows = affected_rows.saturating_add(1);
        } else {
            let primary_key = row_key_with_allocations(
                &excluded,
                &primary_key_positions,
                checkpoint_allocations,
            )?;
            insert_changed_key_with_allocations(
                changed_keys,
                &primary_key,
                checkpoint_allocations,
            )?;
            if segment
                .rows
                .insert_with_allocations(
                    clone_key_with_allocations(&primary_key, conflict_context.as_ref())?,
                    excluded,
                    checkpoint_allocations,
                )?
                .is_some()
            {
                return Err(RelationalError::Constraint(format!(
                    "duplicate primary key in table {table}"
                )));
            }
            if !conflict_has_null {
                insert_staged_conflict_with_allocations(
                    &mut staged_conflicts,
                    conflict_key,
                    Some(primary_key),
                    conflict_context.as_ref(),
                )?;
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

fn clone_key_with_allocations(
    key: &RelationalKey,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<RelationalKey, RelationalError> {
    match allocations {
        Some(context) => key.clone_with_allocations(context),
        None => Ok(key.clone()),
    }
}

fn insert_staged_conflict_with_allocations(
    conflicts: &mut BTreeMap<RelationalKey, Option<RelationalKey>>,
    key: RelationalKey,
    value: Option<RelationalKey>,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<Option<Option<RelationalKey>>, RelationalError> {
    if let Some(context) = allocations
        && !conflicts.contains_key(&key)
    {
        context
            .reserve(crate::cow::checkpoint_tree_node_bytes::<
                RelationalKey,
                Option<RelationalKey>,
            >())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
    }
    let unit = allocations
        .map(|context| context.start_unit())
        .transpose()
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    let previous = conflicts.insert(key, value);
    if let Some(unit) = unit {
        unit.finish();
    }
    Ok(previous)
}

#[derive(Clone, Copy)]
struct RelationalConflictProbeContext<'a> {
    constraint_index: Option<&'a dyn RelationalConstraintIndex>,
    allocations: Option<&'a crate::background::CheckpointDecodeContext>,
}

fn row_matches_key(row: &RelationalRow, positions: &[usize], key: &RelationalKey) -> bool {
    positions.len() == key.0.len()
        && positions
            .iter()
            .zip(&key.0)
            .all(|(position, value)| &row.values()[*position] == value)
}

fn conflict_primary_key(
    state: &RelationalState,
    table: &str,
    schema: &RelationalTableSchema,
    conflict_columns: &[String],
    conflict_key: &RelationalKey,
    changed_keys: &BTreeSet<RelationalKey>,
    context: RelationalConflictProbeContext<'_>,
) -> Result<Option<RelationalKey>, RelationalError> {
    let definition_context =
        context
            .allocations
            .map(|context| crate::background::CheckpointDecodeContext {
                work: context.work.clone(),
                memory: Default::default(),
            });
    let definition = unique_index_definition_with_allocations(
        schema,
        conflict_columns,
        definition_context.as_ref(),
    )?
    .ok_or_else(|| {
        RelationalError::Schema(format!(
            "UPSERT conflict target on table {table} is not materialized"
        ))
    })?;
    if context.constraint_index.is_some() {
        return constraints::authoritative_conflict_primary_key(
            state,
            table,
            &definition,
            conflict_columns,
            conflict_key,
            changed_keys,
            context,
        );
    }
    if definition.role == RelationalIndexRole::Primary {
        return state
            .row(table, conflict_key)
            .map(|_| clone_key_with_allocations(conflict_key, context.allocations))
            .transpose();
    }
    if !state.materialized_index_postings_resident {
        let positions =
            column_positions_with_allocations(schema, conflict_columns, context.allocations)?;
        let mut matches = state
            .rows(table)
            .filter(|(_, row)| row_matches_key(row, &positions, conflict_key))
            .map(|(primary_key, _)| primary_key);
        let primary_key = matches.next();
        if matches.next().is_some() {
            return Err(RelationalError::Corruption(format!(
                "unique conflict target {} on table {table} has multiple visible rows during authoritative recovery",
                definition.name
            )));
        }
        return primary_key
            .map(|key| clone_key_with_allocations(key, context.allocations))
            .transpose();
    }
    let Some(postings) = state.index_lookup(table, &definition.name, conflict_key) else {
        return Ok(None);
    };
    let mut keys = postings.iter();
    let primary_key = keys.next();
    if keys.next().is_some() {
        return Err(RelationalError::Corruption(format!(
            "unique conflict index {} on table {table} has multiple visible rows",
            definition.name
        )));
    }
    primary_key
        .map(|key| clone_key_with_allocations(key, context.allocations))
        .transpose()
}

struct UpdateApplyContext<'a> {
    limits: RelationalMutationLimits,
    overflow_config: RelationalOverflowConfig,
    changed_keys: &'a mut BTreeSet<RelationalKey>,
    checkpoint_allocations: Option<&'a crate::background::CheckpointDecodeContext>,
    replay_access_tracker: Option<&'a mut RelationalReplayAccessTracker>,
}

fn apply_update(
    state: &mut RelationalState,
    table: &str,
    assignments: &[RelationalUpdateAssignment],
    predicate: &RelationalPredicate,
    context: UpdateApplyContext<'_>,
) -> Result<usize, RelationalError> {
    let UpdateApplyContext {
        limits,
        overflow_config,
        changed_keys,
        mut replay_access_tracker,
        checkpoint_allocations,
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
    let assignment_context =
        checkpoint_allocations.map(|context| crate::background::CheckpointDecodeContext {
            work: context.work.clone(),
            memory: Default::default(),
        });
    let resolved = collect_results_with_allocations(
        assignments.iter().map(|assignment| {
            let target = schema.column_position(&assignment.column).ok_or_else(|| {
                RelationalError::Schema(format!(
                    "UPDATE assignment references unknown column {}",
                    assignment.column
                ))
            })?;
            let value = match &assignment.value {
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
                    ResolvedUpdateValue::Column(source)
                }
                RelationalUpdateValue::Value(value) => {
                    validate_value_type(&schema.columns[target], value)?;
                    ResolvedUpdateValue::Value(value)
                }
                RelationalUpdateValue::BigIntArithmetic {
                    left,
                    operator,
                    right,
                } => {
                    if schema.columns[target].scalar_type != RelationalScalarType::BigInt {
                        return Err(RelationalError::Schema(format!(
                            "UPDATE arithmetic assignment target {} must be BIGINT",
                            assignment.column
                        )));
                    }
                    ResolvedUpdateValue::BigIntArithmetic {
                        left: resolve_bigint_operand(&schema, left)?,
                        operator: *operator,
                        right: resolve_bigint_operand(&schema, right)?,
                    }
                }
            };
            Ok((target, value))
        }),
        assignments.len(),
        assignment_context.as_ref(),
    )?;
    let segment = state
        .segments
        .get(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let match_context =
        checkpoint_allocations.map(|context| crate::background::CheckpointDecodeContext {
            work: context.work.clone(),
            memory: Default::default(),
        });
    let mut matched = Vec::new();
    for (key, row) in segment.rows.iter() {
        if let Some(tracker) = replay_access_tracker.as_deref_mut() {
            tracker.record(table, key)?;
        }
        if predicate_truth(&schema, row, predicate)? == Some(true) {
            let matched_key = match &match_context {
                Some(context) => key.clone_with_allocations(context)?,
                None => key.clone(),
            };
            match &match_context {
                Some(context) => context
                    .push(&mut matched, (matched_key, row.clone()))
                    .map_err(|error| RelationalError::Admission(error.to_string()))?,
                None => matched.push((matched_key, row.clone())),
            }
            if matched.len() > limits.max_rows.get() {
                return Err(RelationalError::Admission(format!(
                    "relational UPDATE affects more than max_rows {}",
                    limits.max_rows
                )));
            }
        }
    }
    let primary_key_positions = column_positions_with_allocations(
        &schema,
        &schema.primary_key,
        assignment_context.as_ref(),
    )?;
    let update_context =
        checkpoint_allocations.map(|context| crate::background::CheckpointDecodeContext {
            work: context.work.clone(),
            memory: Default::default(),
        });
    let mut updates = match &update_context {
        Some(context) => context
            .vector_capacity(matched.len())
            .map_err(|error| RelationalError::Admission(error.to_string()))?,
        None => Vec::with_capacity(matched.len()),
    };
    for (old_key, row) in &matched {
        let replacement_context =
            checkpoint_allocations.map(|context| crate::background::CheckpointDecodeContext {
                work: context.work.clone(),
                memory: Default::default(),
            });
        let mut replacements = match &replacement_context {
            Some(context) => context
                .vector_capacity(resolved.len())
                .map_err(|error| RelationalError::Admission(error.to_string()))?,
            None => Vec::with_capacity(resolved.len()),
        };
        // Evaluate every assignment in source order, including one overwritten
        // later, so arithmetic errors keep the ordinary transaction contract.
        for (target, value) in &resolved {
            let value = match value {
                ResolvedUpdateValue::Column(source) => row.values()[*source].as_ref(),
                ResolvedUpdateValue::Value(value) => value.as_ref(),
                ResolvedUpdateValue::BigIntArithmetic {
                    left,
                    operator,
                    right,
                } => RelationalValueRef::BigInt(evaluate_bigint_arithmetic(
                    row, left, *operator, right,
                )?),
            };
            let unit = replacement_context
                .as_ref()
                .map(|context| context.start_unit())
                .transpose()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            replacements.push((*target, value));
            if let Some(unit) = unit {
                unit.finish();
            }
        }
        let mut updated =
            RelationalRow::reconstruct_with_allocations(row, checkpoint_allocations, |ordinal| {
                Ok(replacements
                    .iter()
                    .rev()
                    .find(|(target, _)| *target == ordinal)
                    .map_or_else(|| row.values()[ordinal].as_ref(), |(_, value)| *value))
            })?;
        validate_row(&schema, &updated)?;
        overflow::externalize_row_with_allocations(
            state,
            &schema,
            &mut updated,
            overflow_config,
            checkpoint_allocations,
        )?;
        let new_key =
            row_key_with_allocations(&updated, &primary_key_positions, checkpoint_allocations)?;
        let retained_old_key = match &update_context {
            Some(context) => old_key.clone_with_allocations(context)?,
            None => old_key.clone(),
        };
        let unit = update_context
            .as_ref()
            .map(|context| context.start_unit())
            .transpose()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        updates.push((retained_old_key, new_key, updated));
        if let Some(unit) = unit {
            unit.finish();
        }
    }
    let segment = state
        .segments
        .get_mut(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let segment = mutable_segment_with_allocations(segment, checkpoint_allocations)?;
    for (old_key, _, _) in &updates {
        insert_changed_key_with_allocations(changed_keys, old_key, checkpoint_allocations)?;
        if let Some(context) = checkpoint_allocations {
            segment
                .rows
                .prepare_checkpoint_copy_for_key(old_key, &context.work)?;
        }
        segment.rows.remove(old_key);
    }
    for (_, new_key, row) in updates {
        insert_changed_key_with_allocations(changed_keys, &new_key, checkpoint_allocations)?;
        if segment
            .rows
            .insert_with_allocations(new_key, row, checkpoint_allocations)?
            .is_some()
        {
            return Err(RelationalError::Constraint(format!(
                "UPDATE creates a duplicate primary key in table {table}"
            )));
        }
    }
    Ok(matched.len())
}

fn push_cloned_key_with_allocations(
    keys: &mut Vec<RelationalKey>,
    key: &RelationalKey,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<(), RelationalError> {
    let key = clone_key_with_allocations(key, allocations)?;
    match allocations {
        Some(context) => context
            .push(keys, key)
            .map_err(|error| RelationalError::Admission(error.to_string())),
        None => {
            keys.push(key);
            Ok(())
        }
    }
}

fn changed_table_keys_with_allocations<'a>(
    tables: &'a mut BTreeMap<String, BTreeSet<RelationalKey>>,
    table: &str,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<&'a mut BTreeSet<RelationalKey>, RelationalError> {
    if !tables.contains_key(table) {
        let name = match allocations {
            Some(context) => {
                context
                    .reserve(crate::cow::checkpoint_tree_node_bytes::<
                        String,
                        BTreeSet<RelationalKey>,
                    >())
                    .map_err(|error| RelationalError::Admission(error.to_string()))?;
                context
                    .string(table)
                    .map_err(|error| RelationalError::Admission(error.to_string()))?
            }
            None => table.to_owned(),
        };
        let unit = allocations
            .map(|context| context.start_unit())
            .transpose()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        tables.insert(name, BTreeSet::new());
        if let Some(unit) = unit {
            unit.finish();
        }
    }
    Ok(tables.get_mut(table).expect("admitted changed table"))
}

fn insert_table_name_with_allocations(
    tables: &mut BTreeSet<String>,
    table: &str,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<(), RelationalError> {
    if tables.contains(table) {
        return Ok(());
    }
    let name = match allocations {
        Some(context) => {
            context
                .reserve(crate::cow::checkpoint_tree_node_bytes::<String, ()>())
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            context
                .string(table)
                .map_err(|error| RelationalError::Admission(error.to_string()))?
        }
        None => table.to_owned(),
    };
    let unit = allocations
        .map(|context| context.start_unit())
        .transpose()
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    tables.insert(name);
    if let Some(unit) = unit {
        unit.finish();
    }
    Ok(())
}

fn insert_changed_key_with_allocations(
    keys: &mut BTreeSet<RelationalKey>,
    key: &RelationalKey,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<(), RelationalError> {
    if keys.contains(key) {
        return Ok(());
    }
    let owned = match allocations {
        Some(context) => {
            context
                .reserve(crate::cow::checkpoint_tree_node_bytes::<RelationalKey, ()>())
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            key.clone_with_allocations(context)?
        }
        None => key.clone(),
    };
    let unit = allocations
        .map(|context| context.start_unit())
        .transpose()
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    keys.insert(owned);
    if let Some(unit) = unit {
        unit.finish();
    }
    Ok(())
}

enum ResolvedUpdateValue<'a> {
    Column(usize),
    Value(&'a RelationalValue),
    BigIntArithmetic {
        left: ResolvedBigIntOperand,
        operator: RelationalBigIntArithmeticOperator,
        right: ResolvedBigIntOperand,
    },
}

enum ResolvedBigIntOperand {
    Column(usize),
    Value(i64),
}

fn resolve_bigint_operand(
    schema: &RelationalTableSchema,
    operand: &RelationalBigIntOperand,
) -> Result<ResolvedBigIntOperand, RelationalError> {
    match operand {
        RelationalBigIntOperand::Column(column) => {
            let position = schema.column_position(column).ok_or_else(|| {
                RelationalError::Schema(format!(
                    "UPDATE arithmetic references unknown source column {column}"
                ))
            })?;
            if schema.columns[position].scalar_type != RelationalScalarType::BigInt {
                return Err(RelationalError::Schema(format!(
                    "UPDATE arithmetic source column {column} must be BIGINT"
                )));
            }
            Ok(ResolvedBigIntOperand::Column(position))
        }
        RelationalBigIntOperand::Value(RelationalValue::BigInt(value)) => {
            Ok(ResolvedBigIntOperand::Value(*value))
        }
        RelationalBigIntOperand::Value(value) => Err(RelationalError::Schema(format!(
            "UPDATE arithmetic value must be a non-null BIGINT, got {:?}",
            value.scalar_type()
        ))),
    }
}

fn evaluate_bigint_arithmetic(
    row: &RelationalRow,
    left: &ResolvedBigIntOperand,
    operator: RelationalBigIntArithmeticOperator,
    right: &ResolvedBigIntOperand,
) -> Result<i64, RelationalError> {
    let operand = |operand: &ResolvedBigIntOperand| match operand {
        ResolvedBigIntOperand::Value(value) => Ok(*value),
        ResolvedBigIntOperand::Column(position) => match row.values()[*position] {
            RelationalValue::BigInt(value) => Ok(value),
            RelationalValue::Null => Err(RelationalError::Constraint(
                "UPDATE BIGINT arithmetic does not accept NULL operands".to_string(),
            )),
            ref value => Err(RelationalError::Corruption(format!(
                "UPDATE BIGINT arithmetic source has unexpected type {:?}",
                value.scalar_type()
            ))),
        },
    };
    let left = operand(left)?;
    let right = operand(right)?;
    match operator {
        RelationalBigIntArithmeticOperator::Add => left.checked_add(right),
        RelationalBigIntArithmeticOperator::Subtract => left.checked_sub(right),
    }
    .ok_or_else(|| RelationalError::Constraint("UPDATE BIGINT arithmetic overflow".to_string()))
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
            match default {
                RelationalColumnDefault::Literal(value) => validate_value_type(column, value)?,
                RelationalColumnDefault::UuidV7
                    if column.scalar_type == RelationalScalarType::Uuid => {}
                RelationalColumnDefault::UuidV7 => {
                    return Err(RelationalError::Schema(format!(
                        "column {} uses uuidv7() default but is not UUID",
                        column.name
                    )));
                }
            }
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

pub(crate) fn validate_row(
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

fn rebuild_indexes_with_allocations(
    state: &mut RelationalState,
    table: &str,
    allocations: &crate::background::CheckpointDecodeContext,
) -> Result<(), RelationalError> {
    let schema = state
        .schemas
        .get(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let source = state
        .segments
        .get(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let work = &allocations.work;
    let definition_context = crate::background::CheckpointDecodeContext {
        work: work.clone(),
        memory: Default::default(),
    };
    let owned_context = crate::background::CheckpointDecodeContext {
        work: work.clone(),
        memory: Default::default(),
    };
    owned_context
        .reserve(std::mem::size_of::<RelationalTableSegment>() + 2 * std::mem::size_of::<usize>())
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    let definitions =
        required_index_definitions_with_allocations(schema, Some(&definition_context))?;
    let mut indexes = BTreeMap::new();
    for index in definitions
        .iter()
        .filter(|index| index.role != RelationalIndexRole::Primary)
    {
        let position_context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let positions =
            column_positions_with_allocations(schema, &index.columns, Some(&position_context))?;
        let mut postings = RelationalIndexPages::checkpoint_empty(work)?;
        for (primary_key, row) in source.rows.iter() {
            let key_context = crate::background::CheckpointDecodeContext {
                work: work.clone(),
                memory: Default::default(),
            };
            let key = row_key_with_allocations(row, &positions, Some(&key_context))?;
            if index.role.is_unique() && key_contains_null(&key) {
                continue;
            }
            if index.role.is_unique() && postings.get(&key).is_some_and(|keys| !keys.is_empty()) {
                return Err(RelationalError::Constraint(format!(
                    "unique index {} on table {table} has a duplicate key",
                    index.name
                )));
            }
            let primary_key = clone_key_with_allocations(primary_key, Some(&key_context))?;
            postings.insert_posting_with_allocations(key, primary_key, Some(allocations))?;
        }
        owned_context
            .reserve(crate::cow::checkpoint_tree_node_bytes::<
                String,
                RelationalIndexPages,
            >())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let name = clone_string_with_allocations(&index.name, Some(&owned_context))?;
        let unit = work
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        indexes.insert(name, postings);
        unit.finish();
    }
    let unit = work
        .start_unit()
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    let replacement = Arc::new(RelationalTableSegment {
        rows: source.rows.clone(),
        indexes,
        _memory: owned_context.memory.into_inner(),
    });
    unit.finish();
    work.checkpoint()
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    *state
        .segments
        .get_mut(table)
        .expect("validated rebuild table") = replacement;
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
    refresh_indexes_for_keys_with_allocations(previous, next, table, keys, enforce_unique, None)
}

fn refresh_indexes_for_keys_with_allocations(
    previous: &RelationalState,
    next: &mut RelationalState,
    table: &str,
    keys: &BTreeSet<RelationalKey>,
    enforce_unique: bool,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<(), RelationalError> {
    let schema = Arc::clone(
        next.schemas
            .get(table)
            .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?,
    );
    let definition_context =
        allocations.map(|context| crate::background::CheckpointDecodeContext {
            work: context.work.clone(),
            memory: Default::default(),
        });
    let index_definitions =
        required_index_definitions_with_allocations(&schema, definition_context.as_ref())?;
    let previous_segment = previous.segments.get(table);
    let next_segment = next
        .segments
        .get_mut(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let next_segment = mutable_segment_with_allocations(next_segment, allocations)?;

    for index in index_definitions
        .into_iter()
        .filter(|index| index.role != RelationalIndexRole::Primary)
    {
        let position_context =
            allocations.map(|context| crate::background::CheckpointDecodeContext {
                work: context.work.clone(),
                memory: Default::default(),
            });
        let positions =
            column_positions_with_allocations(&schema, &index.columns, position_context.as_ref())?;
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
                let key_context =
                    allocations.map(|context| crate::background::CheckpointDecodeContext {
                        work: context.work.clone(),
                        memory: Default::default(),
                    });
                let index_key =
                    row_key_with_allocations(previous_row, &positions, key_context.as_ref())?;
                match allocations {
                    Some(context) => postings.remove_posting_with_allocations(
                        &index_key,
                        primary_key,
                        Some(context),
                    )?,
                    None => postings.remove_posting(&index_key, primary_key),
                }
            }
        }
        for primary_key in keys {
            let Some(row) = next_segment.rows.get(primary_key) else {
                continue;
            };
            let key_context =
                allocations.map(|context| crate::background::CheckpointDecodeContext {
                    work: context.work.clone(),
                    memory: Default::default(),
                });
            let index_key = row_key_with_allocations(row, &positions, key_context.as_ref())?;
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
            let primary_key = clone_key_with_allocations(primary_key, key_context.as_ref())?;
            postings.insert_posting_with_allocations(index_key, primary_key, allocations)?;
        }
    }
    Ok(())
}

fn clone_string_with_allocations(
    text: &str,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<String, RelationalError> {
    match allocations {
        Some(context) => context
            .string(text)
            .map_err(|error| RelationalError::Admission(error.to_string())),
        None => Ok(text.to_owned()),
    }
}

fn unique_index_definition_with_allocations(
    schema: &RelationalTableSchema,
    columns: &[String],
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<Option<RelationalIndexDefinition>, RelationalError> {
    let Some(context) = allocations else {
        return Ok(schema.unique_index_definition(columns));
    };
    let (name, role) = if columns == schema.primary_key {
        (
            clone_string_with_allocations(RELATIONAL_PRIMARY_INDEX_NAME, Some(context))?,
            RelationalIndexRole::Primary,
        )
    } else if let Some(ordinal) = schema
        .unique_constraints
        .iter()
        .position(|candidate| candidate.as_slice() == columns)
    {
        (
            admitted_ordinal_index_name(RELATIONAL_UNIQUE_INDEX_PREFIX, ordinal, context)?,
            RelationalIndexRole::UniqueConstraint,
        )
    } else if let Some(index) = schema
        .indexes
        .iter()
        .find(|index| index.unique && index.columns == columns)
    {
        (
            clone_string_with_allocations(&index.name, Some(context))?,
            RelationalIndexRole::DeclaredUnique,
        )
    } else {
        return Ok(None);
    };
    let columns = clone_schema_strings(columns, context)?;
    Ok(Some(RelationalIndexDefinition {
        name,
        columns,
        role,
    }))
}

fn required_index_definitions_with_allocations(
    schema: &RelationalTableSchema,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<Vec<RelationalIndexDefinition>, RelationalError> {
    let Some(context) = allocations else {
        return Ok(schema.required_index_definitions());
    };
    let count = 1usize
        .checked_add(schema.unique_constraints.len())
        .and_then(|count| count.checked_add(schema.indexes.len()))
        .and_then(|count| count.checked_add(schema.foreign_keys.len()))
        .ok_or_else(|| RelationalError::Admission("index definition capacity overflow".into()))?;
    let mut definitions = context
        .vector_capacity(count)
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    let mut push = |name, columns: &[String], role| -> Result<(), RelationalError> {
        let columns = collect_results_with_allocations(
            columns
                .iter()
                .map(|column| clone_string_with_allocations(column, Some(context))),
            columns.len(),
            Some(context),
        )?;
        let unit = context
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        definitions.push(RelationalIndexDefinition {
            name,
            columns,
            role,
        });
        unit.finish();
        Ok(())
    };
    push(
        clone_string_with_allocations(RELATIONAL_PRIMARY_INDEX_NAME, Some(context))?,
        &schema.primary_key,
        RelationalIndexRole::Primary,
    )?;
    for (ordinal, columns) in schema.unique_constraints.iter().enumerate() {
        push(
            admitted_ordinal_index_name(RELATIONAL_UNIQUE_INDEX_PREFIX, ordinal, context)?,
            columns,
            RelationalIndexRole::UniqueConstraint,
        )?;
    }
    for index in &schema.indexes {
        push(
            clone_string_with_allocations(&index.name, Some(context))?,
            &index.columns,
            if index.unique {
                RelationalIndexRole::DeclaredUnique
            } else {
                RelationalIndexRole::Secondary
            },
        )?;
    }
    for (ordinal, key) in schema.foreign_keys.iter().enumerate() {
        push(
            admitted_ordinal_index_name(RELATIONAL_FOREIGN_KEY_INDEX_PREFIX, ordinal, context)?,
            &key.columns,
            RelationalIndexRole::ForeignKeySupport,
        )?;
    }
    context
        .checkpoint()
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    Ok(definitions)
}

fn admitted_ordinal_index_name(
    prefix: &str,
    ordinal: usize,
    context: &crate::background::CheckpointDecodeContext,
) -> Result<String, RelationalError> {
    use std::fmt::Write;
    let digits = if ordinal == 0 {
        1
    } else {
        ordinal.ilog10() as usize + 1
    };
    let capacity = prefix
        .len()
        .checked_add(digits)
        .ok_or_else(|| RelationalError::Admission("index name capacity overflow".into()))?;
    let mut name = context
        .string_capacity(capacity)
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    let unit = context
        .start_unit()
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    write!(&mut name, "{prefix}{ordinal}")
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    unit.finish();
    Ok(name)
}

fn capture_relational_index_changes(
    previous: &RelationalState,
    next: &RelationalState,
    changed_keys: &BTreeMap<String, BTreeSet<RelationalKey>>,
    full_index_rebuild: &BTreeSet<String>,
    limits: RelationalIndexChangeCaptureLimits,
) -> RelationalIndexChangeCapture {
    capture_relational_index_changes_with_allocations(
        previous,
        next,
        changed_keys,
        full_index_rebuild,
        limits,
        None,
    )
    .expect("ordinary index capture has no allocation refusal")
}

fn capture_relational_index_changes_with_allocations(
    previous: &RelationalState,
    next: &RelationalState,
    changed_keys: &BTreeMap<String, BTreeSet<RelationalKey>>,
    full_index_rebuild: &BTreeSet<String>,
    limits: RelationalIndexChangeCaptureLimits,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<RelationalIndexChangeCapture, RelationalError> {
    if !full_index_rebuild.is_empty() {
        return Ok(RelationalIndexChangeCapture::Invalidated {
            reason: format!(
                "schema-changing WAL requires new index roots for tables {}",
                full_index_rebuild
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        });
    }

    let mut changes = Vec::new();
    let mut encoded_bytes = 0usize;
    for (table, keys) in changed_keys {
        let Some(schema) = next
            .schemas
            .get(table)
            .or_else(|| previous.schemas.get(table))
        else {
            return Ok(RelationalIndexChangeCapture::Invalidated {
                reason: format!("changed table {table} has no relational schema"),
            });
        };
        let previous_segment = previous.segments.get(table);
        let next_segment = next.segments.get(table);
        let definition_context =
            allocations.map(|context| crate::background::CheckpointDecodeContext {
                work: context.work.clone(),
                memory: Default::default(),
            });
        let indexes =
            required_index_definitions_with_allocations(schema, definition_context.as_ref())?;

        for primary_key in keys {
            let previous_row = previous_segment.and_then(|segment| segment.rows.get(primary_key));
            let next_row = next_segment.and_then(|segment| segment.rows.get(primary_key));
            if previous_row.is_some() != next_row.is_some()
                && !push_captured_index_change_with_allocations(
                    &mut changes,
                    &mut encoded_bytes,
                    limits,
                    estimated_index_change_encoding_bytes_parts(
                        table,
                        RELATIONAL_PRIMARY_INDEX_NAME,
                        primary_key,
                        primary_key,
                    ),
                    allocations,
                    || {
                        Ok(RelationalIndexChange {
                            table: clone_string_with_allocations(table, allocations)?,
                            index: clone_string_with_allocations(
                                RELATIONAL_PRIMARY_INDEX_NAME,
                                allocations,
                            )?,
                            index_key: clone_key_with_allocations(primary_key, allocations)?,
                            primary_key: clone_key_with_allocations(primary_key, allocations)?,
                            kind: if next_row.is_some() {
                                RelationalIndexChangeKind::Insert
                            } else {
                                RelationalIndexChangeKind::Delete
                            },
                        })
                    },
                )?
            {
                return Ok(capture_limit_invalidated(limits));
            }

            for index in indexes
                .iter()
                .filter(|index| index.role != RelationalIndexRole::Primary)
            {
                let position_context =
                    allocations.map(|context| crate::background::CheckpointDecodeContext {
                        work: context.work.clone(),
                        memory: Default::default(),
                    });
                let positions = match column_positions_with_allocations(
                    schema,
                    &index.columns,
                    position_context.as_ref(),
                ) {
                    Ok(positions) => positions,
                    Err(error @ RelationalError::Admission(_)) if allocations.is_some() => {
                        return Err(error)
                    }
                    Err(error) => {
                        return Ok(RelationalIndexChangeCapture::Invalidated {
                            reason: error.to_string(),
                        })
                    }
                };
                let includes_row = |row: &&RelationalRow| {
                    !index.role.is_unique()
                        || !positions.iter().any(|position| {
                            matches!(row.values()[*position], RelationalValue::Null)
                        })
                };
                let previous_index_row = previous_row.filter(includes_row);
                let next_index_row = next_row.filter(includes_row);
                let equal = match (previous_index_row, next_index_row) {
                    (None, None) => true,
                    (Some(previous), Some(next)) => positions
                        .iter()
                        .all(|position| previous.values()[*position] == next.values()[*position]),
                    _ => false,
                };
                if equal {
                    continue;
                }
                for (row, kind) in [
                    (previous_index_row, RelationalIndexChangeKind::Delete),
                    (next_index_row, RelationalIndexChangeKind::Insert),
                ] {
                    let Some(row) = row else { continue };
                    let change_bytes = estimated_index_change_encoding_bytes_with_key_size(
                        table,
                        &index.name,
                        estimated_relational_values_encoding_bytes(
                            positions.iter().map(|position| &row.values()[*position]),
                        ),
                        primary_key,
                    );
                    if !push_captured_index_change_with_allocations(
                        &mut changes,
                        &mut encoded_bytes,
                        limits,
                        change_bytes,
                        allocations,
                        || {
                            Ok(RelationalIndexChange {
                                table: clone_string_with_allocations(table, allocations)?,
                                index: clone_string_with_allocations(&index.name, allocations)?,
                                index_key: row_key_with_allocations(row, &positions, allocations)?,
                                primary_key: clone_key_with_allocations(primary_key, allocations)?,
                                kind,
                            })
                        },
                    )? {
                        return Ok(capture_limit_invalidated(limits));
                    }
                }
            }
        }
    }
    if let Some(context) = allocations {
        context
            .checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
    }
    Ok(RelationalIndexChangeCapture::Captured {
        changes,
        encoded_bytes,
    })
}

fn capture_relational_row_changes(
    previous: &RelationalState,
    next: &RelationalState,
    changed_keys: &BTreeMap<String, BTreeSet<RelationalKey>>,
    rewritten_tables: &BTreeSet<String>,
    limits: RelationalRowChangeCaptureLimits,
) -> RelationalRowChangeCapture {
    capture_relational_row_changes_with_allocations(
        previous,
        next,
        changed_keys,
        rewritten_tables,
        limits,
        None,
    )
    .expect("ordinary row capture has no allocation refusal")
}

fn capture_relational_row_changes_with_allocations(
    previous: &RelationalState,
    next: &RelationalState,
    changed_keys: &BTreeMap<String, BTreeSet<RelationalKey>>,
    rewritten_tables: &BTreeSet<String>,
    limits: RelationalRowChangeCaptureLimits,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<RelationalRowChangeCapture, RelationalError> {
    if !rewritten_tables.is_empty() {
        let tables = collect_results_with_allocations(
            rewritten_tables.iter().map(|name| match allocations {
                Some(context) => context
                    .string(name)
                    .map_err(|error| RelationalError::Admission(error.to_string())),
                None => Ok(name.clone()),
            }),
            rewritten_tables.len(),
            allocations,
        )?;
        return Ok(RelationalRowChangeCapture::RequiresCheckpoint { tables });
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
            let Some(change_bytes) =
                estimated_row_change_encoding_bytes_parts(table, primary_key, next_row)
            else {
                return Ok(row_capture_limit_invalidated(limits));
            };
            let Some(next_bytes) = encoded_bytes.checked_add(change_bytes) else {
                return Ok(row_capture_limit_invalidated(limits));
            };
            if changes.len() >= limits.max_entries.get() || next_bytes > limits.max_bytes.get() {
                return Ok(row_capture_limit_invalidated(limits));
            }
            let table = match allocations {
                Some(context) => context
                    .string(table)
                    .map_err(|error| RelationalError::Admission(error.to_string()))?,
                None => table.clone(),
            };
            let primary_key = clone_key_with_allocations(primary_key, allocations)?;
            let change = RelationalRowChange {
                table,
                primary_key,
                row: next_row.cloned(),
            };
            match allocations {
                Some(context) => context
                    .push(&mut changes, change)
                    .map_err(|error| RelationalError::Admission(error.to_string()))?,
                None => changes.push(change),
            }
            encoded_bytes = next_bytes;
        }
    }
    if let Some(context) = allocations {
        context
            .checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
    }
    Ok(RelationalRowChangeCapture::Captured {
        changes,
        encoded_bytes,
    })
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
    estimated_row_change_encoding_bytes_parts(
        &change.table,
        &change.primary_key,
        change.row.as_ref(),
    )
}

fn estimated_row_change_encoding_bytes_parts(
    table: &str,
    primary_key: &RelationalKey,
    row: Option<&RelationalRow>,
) -> Option<usize> {
    const FIXED_BYTES: usize = 1 + 3 * 4;
    let key_bytes = ordered_key::CheckpointKeyEncoding::new(
        primary_key,
        &[],
        &crate::background::CheckpointWorkContext::default(),
    )
    .ok()?
    .encoded_len();
    let row_bytes = match row {
        Some(row) => row.estimated_payload_bytes().checked_add(
            row.values()
                .len()
                .checked_mul(std::mem::size_of::<RelationalValue>())?,
        )?,
        None => 0,
    };
    FIXED_BYTES
        .checked_add(table.len())?
        .checked_add(key_bytes)?
        .checked_add(row_bytes)
}

fn estimated_replay_access_encoding_bytes(
    table: &str,
    primary_key: &RelationalKey,
) -> Option<usize> {
    const FIXED_BYTES: usize = 2 * 8;
    let key_bytes = ordered_key::CheckpointKeyEncoding::new(
        primary_key,
        &[],
        &crate::background::CheckpointWorkContext::default(),
    )
    .ok()?
    .encoded_len();
    FIXED_BYTES.checked_add(table.len())?.checked_add(key_bytes)
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

fn push_captured_index_change_with_allocations(
    changes: &mut Vec<RelationalIndexChange>,
    encoded_bytes: &mut usize,
    limits: RelationalIndexChangeCaptureLimits,
    change_bytes: Option<usize>,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
    make_change: impl FnOnce() -> Result<RelationalIndexChange, RelationalError>,
) -> Result<bool, RelationalError> {
    let Some(change_bytes) = change_bytes else {
        return Ok(false);
    };
    let Some(next_bytes) = encoded_bytes.checked_add(change_bytes) else {
        return Ok(false);
    };
    if changes.len() >= limits.max_entries.get() || next_bytes > limits.max_bytes.get() {
        return Ok(false);
    }
    let change = make_change()?;
    match allocations {
        Some(context) => context
            .push(changes, change)
            .map_err(|error| RelationalError::Admission(error.to_string()))?,
        None => changes.push(change),
    }
    *encoded_bytes = next_bytes;
    Ok(true)
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
    estimated_index_change_encoding_bytes_parts(
        &change.table,
        &change.index,
        &change.index_key,
        &change.primary_key,
    )
}

fn estimated_index_change_encoding_bytes_parts(
    table: &str,
    index: &str,
    index_key: &RelationalKey,
    primary_key: &RelationalKey,
) -> Option<usize> {
    estimated_index_change_encoding_bytes_with_key_size(
        table,
        index,
        estimated_relational_key_encoding_bytes(index_key),
        primary_key,
    )
}

fn estimated_index_change_encoding_bytes_with_key_size(
    table: &str,
    index: &str,
    index_key_bytes: Option<usize>,
    primary_key: &RelationalKey,
) -> Option<usize> {
    const FIXED_BYTES: usize = 1 + 4 * 4;
    FIXED_BYTES
        .checked_add(table.len())?
        .checked_add(index.len())?
        .checked_add(index_key_bytes?)?
        .checked_add(estimated_relational_key_encoding_bytes(primary_key)?)
}

fn estimated_relational_key_encoding_bytes(key: &RelationalKey) -> Option<usize> {
    estimated_relational_values_encoding_bytes(key.0.iter())
}

fn estimated_relational_values_encoding_bytes<'a>(
    mut values: impl Iterator<Item = &'a RelationalValue>,
) -> Option<usize> {
    values.try_fold(0usize, |bytes, value| {
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
            RelationalValue::Uuid(_) => 17,
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
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<(), RelationalError> {
    for (table, schema) in &next.schemas {
        let Some(segment) = next.segments.get(table) else {
            continue;
        };
        for foreign_key in &schema.foreign_keys {
            let position_context =
                allocations.map(|context| crate::background::CheckpointDecodeContext {
                    work: context.work.clone(),
                    memory: Default::default(),
                });
            let local_positions = column_positions_with_allocations(
                schema,
                &foreign_key.columns,
                position_context.as_ref(),
            )?;
            let referenced_schema =
                next.schemas
                    .get(&foreign_key.referenced_table)
                    .ok_or_else(|| {
                        RelationalError::Schema(format!(
                            "foreign key references unknown table {}",
                            foreign_key.referenced_table
                        ))
                    })?;
            let referenced_positions = column_positions_with_allocations(
                referenced_schema,
                &foreign_key.referenced_columns,
                position_context.as_ref(),
            )?;
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
                    validate_foreign_key_row(
                        next,
                        table,
                        row,
                        foreign_key,
                        &local_positions,
                        allocations,
                    )?;
                }
            } else if let Some(keys) = changed_keys.get(table) {
                for key in keys {
                    if let Some(row) = segment.rows.get(key) {
                        validate_foreign_key_row(
                            next,
                            table,
                            row,
                            foreign_key,
                            &local_positions,
                            allocations,
                        )?;
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
                let key_context =
                    allocations.map(|context| crate::background::CheckpointDecodeContext {
                        work: context.work.clone(),
                        memory: Default::default(),
                    });
                let previous_target = row_key_with_allocations(
                    previous_row,
                    &referenced_positions,
                    key_context.as_ref(),
                )?;
                if foreign_key_target_exists(next, foreign_key, &previous_target, allocations)? {
                    continue;
                }
                let mut referenced = false;
                for row in segment.rows.values() {
                    let unit = allocations
                        .map(|context| context.start_unit())
                        .transpose()
                        .map_err(|error| RelationalError::Admission(error.to_string()))?;
                    referenced = !key_contains_null(&previous_target)
                        && row_matches_key(row, &local_positions, &previous_target);
                    if let Some(unit) = unit {
                        unit.finish();
                    }
                    if referenced {
                        break;
                    }
                }
                if referenced {
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
    let columns = &foreign_key.referenced_columns;
    let references_unique_key = *columns == referenced_schema.primary_key
        || referenced_schema
            .unique_constraints
            .iter()
            .any(|candidate| candidate == columns)
        || referenced_schema
            .indexes
            .iter()
            .any(|index| index.unique && index.columns == *columns);
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
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<(), RelationalError> {
    let key_context = allocations.map(|context| crate::background::CheckpointDecodeContext {
        work: context.work.clone(),
        memory: Default::default(),
    });
    let key = row_key_with_allocations(row, local_positions, key_context.as_ref())?;
    if key_contains_null(&key) || foreign_key_target_exists(state, foreign_key, &key, allocations)?
    {
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
    allocations: Option<&crate::background::CheckpointDecodeContext>,
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
    let definition_context =
        allocations.map(|context| crate::background::CheckpointDecodeContext {
            work: context.work.clone(),
            memory: Default::default(),
        });
    let definition = unique_index_definition_with_allocations(
        schema,
        &foreign_key.referenced_columns,
        definition_context.as_ref(),
    )?
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
    column_positions_with_allocations(schema, columns, None)
}

fn collect_results_with_allocations<T>(
    items: impl IntoIterator<Item = Result<T, RelationalError>>,
    capacity: usize,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<Vec<T>, RelationalError> {
    let Some(context) = allocations else {
        return items.into_iter().collect();
    };
    let mut values = context
        .vector_capacity(capacity)
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    for item in items {
        let item = item?;
        if values.len() == capacity {
            return Err(RelationalError::Admission(
                "resolved input exceeds its admitted capacity".into(),
            ));
        }
        let unit = context
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        values.push(item);
        unit.finish();
    }
    context
        .checkpoint()
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    Ok(values)
}

fn column_positions_with_allocations(
    schema: &RelationalTableSchema,
    columns: &[String],
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<Vec<usize>, RelationalError> {
    let mut positions = match allocations {
        Some(context) => context
            .vector_capacity(columns.len())
            .map_err(|error| RelationalError::Admission(error.to_string()))?,
        None => Vec::with_capacity(columns.len()),
    };
    for column in columns {
        let position = schema.column_position(column).ok_or_else(|| {
            RelationalError::Schema(format!("table {} has no column {column}", schema.name))
        })?;
        let unit = allocations
            .map(|context| context.start_unit())
            .transpose()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        positions.push(position);
        if let Some(unit) = unit {
            unit.finish();
        }
    }
    if let Some(context) = allocations {
        context
            .checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
    }
    Ok(positions)
}

fn row_key_with_allocations(
    row: &RelationalRow,
    positions: &[usize],
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<RelationalKey, RelationalError> {
    let Some(context) = allocations else {
        return Ok(row_key(row, positions));
    };
    let mut values = context
        .vector_capacity(positions.len())
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    for position in positions {
        let value = row.values()[*position]
            .as_ref()
            .to_owned_value_with_allocations(context)?;
        let unit = context
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        values.push(value);
        unit.finish();
    }
    context
        .checkpoint()
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    Ok(RelationalKey(values))
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
