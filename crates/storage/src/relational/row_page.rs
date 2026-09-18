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

use super::ordered_key::{
    decode_ordered_relational_key, decode_ordered_relational_key_into,
    encode_ordered_relational_key, validate_ordered_relational_key,
};
#[cfg(test)]
use super::{RelationalColumnSchema, RelationalTableSchema};
use super::{
    RelationalKey, RelationalOverflowRef, RelationalRow, RelationalScalarType, RelationalValue,
    RelationalValueRef,
};
use crate::SegmentBytes;
use hawdb_integrity::{IntegrityHasher, Sha256Digest, SHA256_BYTES};
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::Range;

mod delta;
mod demand;
mod live;
mod mutation;
mod publication;
mod recovery;
mod snapshot;
mod state;
mod value;

pub use delta::{
    relational_row_delta_manifest_generation_file, relational_row_delta_run_file,
    RelationalRowDeltaBaseBinding, RelationalRowDeltaBuilder, RelationalRowDeltaConfig,
    RelationalRowDeltaError, RelationalRowDeltaGeneration, RelationalRowDeltaManifest,
    RelationalRowDeltaPublicationPhase, RelationalRowDeltaReadReport, RelationalRowDeltaReader,
    RelationalRowDeltaReport, RelationalRowDeltaTableMetadata,
    DEFAULT_RELATIONAL_ROW_DELTA_CHECKPOINT_RUNS, DEFAULT_RELATIONAL_ROW_DELTA_DIRTY_BYTES,
    DEFAULT_RELATIONAL_ROW_DELTA_DIRTY_ENTRIES, DEFAULT_RELATIONAL_ROW_DELTA_MANIFEST_BYTES,
    DEFAULT_RELATIONAL_ROW_DELTA_RUNS, DEFAULT_RELATIONAL_ROW_DELTA_RUN_BYTES,
    RELATIONAL_ROW_DELTA_MANIFEST_FILE,
};
pub use demand::{
    RelationalRowPageDemandReadError, RelationalRowPageDemandReadLimits,
    RelationalRowPageDemandReadReport, RelationalRowPageDemandReader,
    RelationalRowPageProjectedFields, RelationalRowPageProjectedRange,
    RelationalRowPageProjectedRangeFields, DEFAULT_RELATIONAL_ROW_PAGE_READ_BYTES,
    DEFAULT_RELATIONAL_ROW_PAGE_READ_PAGES, DEFAULT_RELATIONAL_ROW_PAGE_READ_PINS,
    DEFAULT_RELATIONAL_ROW_PAGE_READ_ROWS, DEFAULT_RELATIONAL_ROW_PAGE_READ_TREE_HEIGHT,
};
pub use live::{
    RelationalRowPageCheckpointError, RelationalRowPageLiveError, RelationalRowPageReadView,
    RelationalRowPageReadViewIdentity,
};
pub use mutation::{
    RelationalRowPageBootstrap, RelationalRowPageBootstrapReport, RelationalRowPageIdAllocator,
    RelationalRowPageMutationError, RelationalRowPageMutationPlan,
    RelationalRowPageMutationPlanner,
};
pub use publication::{
    relational_row_page_artifact_file, relational_row_page_manifest_generation_file,
    relational_row_page_root_descriptor_file, relational_row_page_root_key_file,
    RelationalRowPageArtifactMetadata, RelationalRowPageGenerationArtifacts,
    RelationalRowPageGenerationRequest, RelationalRowPagePhysicalGeneration,
    RelationalRowPagePublicationConfig, RelationalRowPagePublicationError,
    RelationalRowPagePublicationPhase, RelationalRowPagePublicationReport,
    RelationalRowPagePublisher, RelationalRowPageRewriteConfig, RelationalRowPageRootDescriptor,
    RelationalRowPageRootManifest, RelationalRowPageRootReader, RelationalRowPageSlotIntegrity,
    RelationalRowPageTableDelta, RelationalRowPageTableRoot,
    DEFAULT_RELATIONAL_ROW_PAGE_DIRTY_BYTES, DEFAULT_RELATIONAL_ROW_PAGE_DIRTY_PAGES,
    DEFAULT_RELATIONAL_ROW_PAGE_MANIFEST_BYTES, DEFAULT_RELATIONAL_ROW_PAGE_ROOT_KEY_BYTES,
    DEFAULT_RELATIONAL_ROW_PAGE_ROOT_PAGES, DEFAULT_RELATIONAL_ROW_PAGE_TABLES,
    RELATIONAL_ROW_PAGE_MANIFEST_FILE,
};
pub use recovery::RelationalRowPageRecoveryStatus;
pub use snapshot::{
    RelationalRowPageSnapshotPointReport, RelationalRowPageSnapshotPointsReport,
    RelationalRowPageSnapshotRangeReport, RelationalRowPageSnapshotReadError,
    RelationalRowPageSnapshotReadLimits, RelationalRowPageSnapshotReader,
    RelationalRowPageSnapshotRowSource, DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_BYTES,
    DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_ENTRIES,
};
pub use state::{
    RelationalRowLiveUnavailable, RelationalRowPageServingResources, RelationalRowPageState,
};

#[cfg(test)]
pub(crate) fn test_row_page_schema(table: &str, column_count: usize) -> RelationalTableSchema {
    let columns = (0..column_count)
        .map(|ordinal| RelationalColumnSchema {
            name: if ordinal == 0 {
                "id".to_string()
            } else {
                format!("value_{ordinal}")
            },
            scalar_type: if ordinal == 0 {
                RelationalScalarType::BigInt
            } else {
                RelationalScalarType::Text
            },
            nullable: ordinal != 0,
            default: None,
        })
        .collect();
    RelationalTableSchema {
        name: table.to_string(),
        columns,
        primary_key: vec!["id".to_string()],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
    }
}

#[cfg(test)]
pub(crate) fn test_row_page_schema_digest(table: &str, column_count: usize) -> Sha256Digest {
    super::index_shadow::relational_schema_digest(&test_row_page_schema(table, column_count))
        .expect("test row-page schema must encode")
}
use value::{decode_row_field_refs, decode_row_fields, encode_row, validate_requested_fields};

const ROW_PAGE_MAGIC: &[u8; 8] = b"SKINROW1";
const ROW_PAGE_VERSION: u16 = 1;
const ROW_PAGE_HEADER_BYTES: usize = 140;
const ROW_SLOT_BYTES: usize = 16;
const VALUE_SLOT_BYTES: usize = 8;
const INTEGRITY_PREFIX_BYTES: usize = 104;

pub const DEFAULT_RELATIONAL_ROW_PAGE_BYTES: usize = 1024 * 1024;
pub const DEFAULT_RELATIONAL_ROW_PAGE_ROWS: usize = 256;
pub const DEFAULT_RELATIONAL_ROW_PAGE_COLUMNS: usize = 4096;
pub const DEFAULT_RELATIONAL_ROW_PAGE_KEY_BYTES: usize = 64 * 1024;
pub const DEFAULT_RELATIONAL_ROW_PAGE_ROW_BYTES: usize = 1024 * 1024;
pub const DEFAULT_RELATIONAL_ROW_PAGE_INLINE_VALUE_BYTES: usize = 64 * 1024;
pub const DEFAULT_RELATIONAL_ROW_PAGE_VALUE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalRowPageRecoveredValue {
    Present(RelationalRow),
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelationalRowPageId(NonZeroU64);

impl RelationalRowPageId {
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowPageLimits {
    pub max_page_bytes: NonZeroUsize,
    pub max_rows: NonZeroUsize,
    pub max_columns: NonZeroUsize,
    pub max_key_bytes: NonZeroUsize,
    pub max_row_bytes: NonZeroUsize,
    pub max_inline_value_bytes: NonZeroUsize,
    pub max_value_bytes: NonZeroUsize,
    pub max_requested_fields: NonZeroUsize,
}

impl Default for RelationalRowPageLimits {
    fn default() -> Self {
        Self {
            max_page_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_BYTES)
                .expect("default relational row page byte limit is non-zero"),
            max_rows: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_ROWS)
                .expect("default relational row page row limit is non-zero"),
            max_columns: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_COLUMNS)
                .expect("default relational row page column limit is non-zero"),
            max_key_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_KEY_BYTES)
                .expect("default relational row page key limit is non-zero"),
            max_row_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_ROW_BYTES)
                .expect("default relational row page row byte limit is non-zero"),
            max_inline_value_bytes: NonZeroUsize::new(
                DEFAULT_RELATIONAL_ROW_PAGE_INLINE_VALUE_BYTES,
            )
            .expect("default relational row page inline value limit is non-zero"),
            max_value_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_VALUE_BYTES)
                .expect("default relational row page value limit is non-zero"),
            max_requested_fields: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_COLUMNS)
                .expect("default relational requested field limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowPageEntry {
    pub primary_key: RelationalKey,
    pub row: RelationalRow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImmutableRelationalRowPage {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub page_id: RelationalRowPageId,
    pub schema_digest: Sha256Digest,
    pub column_count: usize,
    pub rows: Vec<RelationalRowPageEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalProjectedField {
    pub ordinal: usize,
    pub value: RelationalValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalProjectedRow {
    pub primary_key: RelationalKey,
    pub fields: Vec<RelationalProjectedField>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RelationalProjectedFieldRef<'a> {
    pub ordinal: usize,
    pub value: RelationalValueRef<'a>,
}

#[derive(Debug, Clone, Copy)]
pub struct RelationalProjectedRowRef<'a> {
    primary_key: &'a RelationalKey,
    encoded_primary_key: &'a [u8],
    fields: &'a [RelationalProjectedFieldRef<'a>],
}

impl<'a> RelationalProjectedRowRef<'a> {
    pub fn primary_key(self) -> &'a RelationalKey {
        self.primary_key
    }

    pub fn fields(self) -> &'a [RelationalProjectedFieldRef<'a>] {
        self.fields
    }

    pub(crate) fn encoded_primary_key(self) -> &'a [u8] {
        self.encoded_primary_key
    }

    pub fn value(self, ordinal: usize) -> Option<RelationalValueRef<'a>> {
        self.fields
            .binary_search_by_key(&ordinal, |field| field.ordinal)
            .ok()
            .map(|position| self.fields[position].value)
    }

    pub fn to_owned_row(self) -> RelationalProjectedRow {
        RelationalProjectedRow {
            primary_key: self.primary_key.clone(),
            fields: self
                .fields
                .iter()
                .map(|field| RelationalProjectedField {
                    ordinal: field.ordinal,
                    value: field.value.to_owned_value(),
                })
                .collect(),
        }
    }
}

/// A projected row that is either borrowed from the current immutable page or
/// owned by a recovery/live overlay.
#[derive(Debug, Clone, Copy)]
pub enum RelationalProjectedRowView<'a> {
    Borrowed(RelationalProjectedRowRef<'a>),
    Owned(&'a RelationalProjectedRow),
}

impl<'a> RelationalProjectedRowView<'a> {
    pub fn primary_key(self) -> &'a RelationalKey {
        match self {
            Self::Borrowed(row) => row.primary_key(),
            Self::Owned(row) => &row.primary_key,
        }
    }

    pub fn value(self, ordinal: usize) -> Option<RelationalValueRef<'a>> {
        match self {
            Self::Borrowed(row) => row.value(ordinal),
            Self::Owned(row) => row
                .fields
                .binary_search_by_key(&ordinal, |field| field.ordinal)
                .ok()
                .map(|position| row.fields[position].value.as_ref()),
        }
    }

    pub fn to_owned_row(self) -> RelationalProjectedRow {
        match self {
            Self::Borrowed(row) => row.to_owned_row(),
            Self::Owned(row) => row.clone(),
        }
    }

    pub const fn is_borrowed(self) -> bool {
        matches!(self, Self::Borrowed(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalRowPageError {
    Admission(String),
    Corrupt(String),
}

impl fmt::Display for RelationalRowPageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => {
                write!(formatter, "relational row page admission failed: {message}")
            }
            Self::Corrupt(message) => write!(formatter, "corrupt relational row page: {message}"),
        }
    }
}

impl std::error::Error for RelationalRowPageError {}

impl ImmutableRelationalRowPage {
    pub fn encode_slot(
        &self,
        limits: RelationalRowPageLimits,
    ) -> Result<Vec<u8>, RelationalRowPageError> {
        let mut encoded = self.encode(limits)?;
        encoded.resize(limits.max_page_bytes.get(), 0);
        Ok(encoded)
    }

    pub fn encode(
        &self,
        limits: RelationalRowPageLimits,
    ) -> Result<Vec<u8>, RelationalRowPageError> {
        validate_page_for_encode(self, limits)?;

        let mut slots = Vec::with_capacity(self.rows.len());
        let mut key_payload = Vec::new();
        let mut row_payload = Vec::new();
        let mut lower_bound: Option<Vec<u8>> = None;
        let mut previous_key: Option<Vec<u8>> = None;
        for entry in &self.rows {
            let key = encode_ordered_relational_key(&entry.primary_key)
                .map_err(|error| RelationalRowPageError::Admission(error.to_string()))?;
            if key.len() > limits.max_key_bytes.get() {
                return Err(RelationalRowPageError::Admission(format!(
                    "primary key contains {} bytes, exceeding limit {}",
                    key.len(),
                    limits.max_key_bytes
                )));
            }
            if previous_key
                .as_ref()
                .is_some_and(|previous| previous >= &key)
            {
                return Err(RelationalRowPageError::Admission(
                    "row primary keys are not strictly increasing".to_string(),
                ));
            }

            let row = encode_row(&entry.row, self.column_count, limits)?;
            if row.len() > limits.max_row_bytes.get() {
                return Err(RelationalRowPageError::Admission(format!(
                    "encoded row contains {} bytes, exceeding limit {}",
                    row.len(),
                    limits.max_row_bytes
                )));
            }
            let slot = RowSlot {
                key_offset: u32_len(key_payload.len(), "key offset")?,
                key_len: u32_len(key.len(), "key length")?,
                row_offset: u32_len(row_payload.len(), "row offset")?,
                row_len: u32_len(row.len(), "row length")?,
            };
            let next_key_bytes = key_payload.len().checked_add(key.len()).ok_or_else(|| {
                RelationalRowPageError::Admission("key payload length overflow".to_string())
            })?;
            let next_row_bytes = row_payload.len().checked_add(row.len()).ok_or_else(|| {
                RelationalRowPageError::Admission("row payload length overflow".to_string())
            })?;
            let directory_bytes = self.rows.len().checked_mul(ROW_SLOT_BYTES).ok_or_else(|| {
                RelationalRowPageError::Admission("row slot directory length overflow".to_string())
            })?;
            let lower_len = lower_bound.as_ref().map_or(key.len(), Vec::len);
            let projected_bytes = ROW_PAGE_HEADER_BYTES
                .checked_add(lower_len)
                .and_then(|bytes| bytes.checked_add(key.len()))
                .and_then(|bytes| bytes.checked_add(directory_bytes))
                .and_then(|bytes| bytes.checked_add(next_key_bytes))
                .and_then(|bytes| bytes.checked_add(next_row_bytes))
                .ok_or_else(|| {
                    RelationalRowPageError::Admission("encoded page size overflow".to_string())
                })?;
            if projected_bytes > limits.max_page_bytes.get() {
                return Err(RelationalRowPageError::Admission(format!(
                    "encoded page would contain {projected_bytes} bytes, exceeding limit {}",
                    limits.max_page_bytes
                )));
            }
            key_payload.extend_from_slice(&key);
            row_payload.extend_from_slice(&row);
            slots.push(slot);
            if lower_bound.is_none() {
                lower_bound = Some(key.clone());
            }
            previous_key = Some(key);
        }

        let lower_bound = lower_bound.expect("validated row page has a lower key bound");
        let upper_bound = previous_key.expect("validated row page has an upper key bound");
        let directory_len = slots.len().checked_mul(ROW_SLOT_BYTES).ok_or_else(|| {
            RelationalRowPageError::Admission("row slot directory length overflow".to_string())
        })?;
        let payload_len = lower_bound
            .len()
            .checked_add(upper_bound.len())
            .and_then(|bytes| bytes.checked_add(directory_len))
            .and_then(|bytes| bytes.checked_add(key_payload.len()))
            .and_then(|bytes| bytes.checked_add(row_payload.len()))
            .ok_or_else(|| {
                RelationalRowPageError::Admission("encoded page payload overflow".to_string())
            })?;
        let total_len = ROW_PAGE_HEADER_BYTES
            .checked_add(payload_len)
            .ok_or_else(|| {
                RelationalRowPageError::Admission("encoded page length overflow".to_string())
            })?;
        if total_len > limits.max_page_bytes.get() {
            return Err(RelationalRowPageError::Admission(format!(
                "encoded page contains {total_len} bytes, exceeding limit {}",
                limits.max_page_bytes
            )));
        }

        let mut encoded = Vec::with_capacity(total_len);
        encoded.extend_from_slice(ROW_PAGE_MAGIC);
        encoded.extend_from_slice(&ROW_PAGE_VERSION.to_le_bytes());
        encoded.extend_from_slice(&0u16.to_le_bytes());
        encoded.extend_from_slice(&self.generation.to_le_bytes());
        encoded.extend_from_slice(&self.source_commit_epoch.to_le_bytes());
        encoded.extend_from_slice(&self.page_id.get().to_le_bytes());
        encoded.extend_from_slice(&u32_len(self.rows.len(), "row count")?.to_le_bytes());
        encoded.extend_from_slice(&u32_len(self.column_count, "column count")?.to_le_bytes());
        encoded.extend_from_slice(&u32_len(lower_bound.len(), "lower key length")?.to_le_bytes());
        encoded.extend_from_slice(&u32_len(upper_bound.len(), "upper key length")?.to_le_bytes());
        encoded.extend_from_slice(&u32_len(directory_len, "directory length")?.to_le_bytes());
        encoded.extend_from_slice(&u64_len(key_payload.len(), "key payload length")?.to_le_bytes());
        encoded.extend_from_slice(&u64_len(row_payload.len(), "row payload length")?.to_le_bytes());
        encoded.extend_from_slice(self.schema_digest.as_bytes());
        debug_assert_eq!(encoded.len(), INTEGRITY_PREFIX_BYTES);
        encoded.extend_from_slice(&[0; 4 + SHA256_BYTES]);
        debug_assert_eq!(encoded.len(), ROW_PAGE_HEADER_BYTES);
        encoded.extend_from_slice(&lower_bound);
        encoded.extend_from_slice(&upper_bound);
        for slot in slots {
            slot.encode(&mut encoded);
        }
        encoded.extend_from_slice(&key_payload);
        encoded.extend_from_slice(&row_payload);
        write_integrity(&mut encoded);
        Ok(encoded)
    }

    pub fn decode(
        encoded: &[u8],
        limits: RelationalRowPageLimits,
    ) -> Result<Self, RelationalRowPageError> {
        let view = RelationalRowPageView::open(encoded, limits)?;
        Self::decode_view(view)
    }

    fn decode_view(view: RelationalRowPageView<'_>) -> Result<Self, RelationalRowPageError> {
        let mut rows = Vec::with_capacity(view.row_count());
        for ordinal in 0..view.row_count() {
            rows.push(view.decode_entry(ordinal)?);
        }
        Ok(Self {
            generation: view.generation,
            source_commit_epoch: view.source_commit_epoch,
            page_id: view.page_id,
            schema_digest: view.schema_digest,
            column_count: view.column_count,
            rows,
        })
    }

    pub fn decode_slot(
        slot: &[u8],
        limits: RelationalRowPageLimits,
    ) -> Result<Self, RelationalRowPageError> {
        let view = RelationalRowPageView::open_slot(slot, limits)?;
        Self::decode_view(view)
    }
}

#[derive(Debug)]
pub struct RelationalRowPageView<'a> {
    limits: RelationalRowPageLimits,
    generation: u64,
    source_commit_epoch: u64,
    page_id: RelationalRowPageId,
    schema_digest: Sha256Digest,
    row_count: usize,
    column_count: usize,
    encoded_len: usize,
    lower_bound: &'a [u8],
    upper_bound: &'a [u8],
    directory: &'a [u8],
    key_payload: &'a [u8],
    row_payload: &'a [u8],
}

#[derive(Debug, Clone)]
pub(crate) struct VerifiedRowPage {
    bytes: SegmentBytes,
    metadata: VerifiedRowPageMetadata,
}

#[derive(Debug, Clone)]
pub(crate) struct VerifiedRowPageMetadata {
    limits: RelationalRowPageLimits,
    generation: u64,
    source_commit_epoch: u64,
    page_id: RelationalRowPageId,
    schema_digest: Sha256Digest,
    row_count: usize,
    column_count: usize,
    encoded_len: usize,
    lower_bound: Range<usize>,
    upper_bound: Range<usize>,
    directory: Range<usize>,
    key_payload: Range<usize>,
    row_payload: Range<usize>,
}

impl VerifiedRowPageMetadata {
    pub(crate) fn from_view(view: &RelationalRowPageView<'_>) -> Self {
        let mut offset = ROW_PAGE_HEADER_BYTES;
        let lower_bound = section_range(&mut offset, view.lower_bound.len());
        let upper_bound = section_range(&mut offset, view.upper_bound.len());
        let directory = section_range(&mut offset, view.directory.len());
        let key_payload = section_range(&mut offset, view.key_payload.len());
        let row_payload = section_range(&mut offset, view.row_payload.len());
        debug_assert_eq!(offset, view.encoded_len);
        Self {
            limits: view.limits,
            generation: view.generation,
            source_commit_epoch: view.source_commit_epoch,
            page_id: view.page_id,
            schema_digest: view.schema_digest,
            row_count: view.row_count,
            column_count: view.column_count,
            encoded_len: view.encoded_len,
            lower_bound,
            upper_bound,
            directory,
            key_payload,
            row_payload,
        }
    }
}

impl VerifiedRowPage {
    pub(crate) fn new(bytes: SegmentBytes, metadata: VerifiedRowPageMetadata) -> Self {
        debug_assert_eq!(bytes.len(), metadata.encoded_len);
        Self { bytes, metadata }
    }

    pub(crate) fn view(&self) -> RelationalRowPageView<'_> {
        let metadata = &self.metadata;
        RelationalRowPageView {
            limits: metadata.limits,
            generation: metadata.generation,
            source_commit_epoch: metadata.source_commit_epoch,
            page_id: metadata.page_id,
            schema_digest: metadata.schema_digest,
            row_count: metadata.row_count,
            column_count: metadata.column_count,
            encoded_len: metadata.encoded_len,
            lower_bound: &self.bytes[metadata.lower_bound.clone()],
            upper_bound: &self.bytes[metadata.upper_bound.clone()],
            directory: &self.bytes[metadata.directory.clone()],
            key_payload: &self.bytes[metadata.key_payload.clone()],
            row_payload: &self.bytes[metadata.row_payload.clone()],
        }
    }
}

fn section_range(offset: &mut usize, len: usize) -> Range<usize> {
    let start = *offset;
    *offset += len;
    start..*offset
}

impl<'a> RelationalRowPageView<'a> {
    pub fn open(
        encoded: &'a [u8],
        limits: RelationalRowPageLimits,
    ) -> Result<Self, RelationalRowPageError> {
        Self::open_with_validation(encoded, limits, true)
    }

    pub(crate) fn open_verified(
        encoded: &'a [u8],
        limits: RelationalRowPageLimits,
    ) -> Result<Self, RelationalRowPageError> {
        Self::open_with_validation(encoded, limits, false)
    }

    fn open_with_validation(
        encoded: &'a [u8],
        limits: RelationalRowPageLimits,
        validate_integrity: bool,
    ) -> Result<Self, RelationalRowPageError> {
        if encoded.len() > limits.max_page_bytes.get() {
            return Err(RelationalRowPageError::Admission(format!(
                "encoded page contains {} bytes, exceeding limit {}",
                encoded.len(),
                limits.max_page_bytes
            )));
        }
        let header = decode_header(encoded, limits)?;
        if encoded.len() != header.encoded_len {
            return Err(RelationalRowPageError::Corrupt(format!(
                "page length mismatch: expected {}, got {}",
                header.encoded_len,
                encoded.len()
            )));
        }
        if validate_integrity {
            verify_integrity(encoded)?;
        }

        let mut offset = ROW_PAGE_HEADER_BYTES;
        let lower_bound = take(
            encoded,
            &mut offset,
            header.lower_bound_len,
            "lower key bound",
        )?;
        let upper_bound = take(
            encoded,
            &mut offset,
            header.upper_bound_len,
            "upper key bound",
        )?;
        let directory = take(
            encoded,
            &mut offset,
            header.directory_len,
            "row slot directory",
        )?;
        let key_payload = take(encoded, &mut offset, header.key_payload_len, "key payload")?;
        let row_payload = take(encoded, &mut offset, header.row_payload_len, "row payload")?;
        if offset != encoded.len() {
            return Err(RelationalRowPageError::Corrupt(
                "page contains trailing bytes".to_string(),
            ));
        }

        let view = Self {
            limits,
            generation: header.generation,
            source_commit_epoch: header.source_commit_epoch,
            page_id: header.page_id,
            schema_digest: header.schema_digest,
            row_count: header.row_count,
            column_count: header.column_count,
            encoded_len: header.encoded_len,
            lower_bound,
            upper_bound,
            directory,
            key_payload,
            row_payload,
        };
        if validate_integrity {
            view.validate_directory()?;
        }
        Ok(view)
    }

    pub fn open_slot(
        slot: &'a [u8],
        limits: RelationalRowPageLimits,
    ) -> Result<Self, RelationalRowPageError> {
        if slot.len() != limits.max_page_bytes.get() {
            return Err(RelationalRowPageError::Corrupt(format!(
                "relational row page slot has {} bytes, expected {}",
                slot.len(),
                limits.max_page_bytes
            )));
        }
        let header = decode_header(slot, limits)?;
        if slot[header.encoded_len..].iter().any(|byte| *byte != 0) {
            return Err(RelationalRowPageError::Corrupt(
                "relational row page slot contains non-zero trailing bytes".to_string(),
            ));
        }
        Self::open(&slot[..header.encoded_len], limits)
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn source_commit_epoch(&self) -> u64 {
        self.source_commit_epoch
    }

    pub const fn page_id(&self) -> RelationalRowPageId {
        self.page_id
    }

    pub const fn schema_digest(&self) -> Sha256Digest {
        self.schema_digest
    }

    pub const fn row_count(&self) -> usize {
        self.row_count
    }

    pub const fn column_count(&self) -> usize {
        self.column_count
    }

    pub const fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    pub fn lower_bound_bytes(&self) -> &[u8] {
        self.lower_bound
    }

    pub fn upper_bound_bytes(&self) -> &[u8] {
        self.upper_bound
    }

    pub fn find_row(
        &self,
        primary_key: &RelationalKey,
    ) -> Result<Option<usize>, RelationalRowPageError> {
        let encoded = encode_ordered_relational_key(primary_key)
            .map_err(|error| RelationalRowPageError::Admission(error.to_string()))?;
        self.find_row_encoded(&encoded)
    }

    pub(crate) fn find_row_encoded(
        &self,
        encoded: &[u8],
    ) -> Result<Option<usize>, RelationalRowPageError> {
        if encoded.len() > self.limits.max_key_bytes.get() {
            return Err(RelationalRowPageError::Admission(format!(
                "lookup key contains {} bytes, exceeding limit {}",
                encoded.len(),
                self.limits.max_key_bytes
            )));
        }
        let mut lower = 0usize;
        let mut upper = self.row_count;
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            match self.key(middle)?.cmp(encoded) {
                std::cmp::Ordering::Less => lower = middle + 1,
                std::cmp::Ordering::Greater => upper = middle,
                std::cmp::Ordering::Equal => return Ok(Some(middle)),
            }
        }
        Ok(None)
    }

    fn lower_bound_row(
        &self,
        primary_key: &RelationalKey,
        inclusive: bool,
    ) -> Result<usize, RelationalRowPageError> {
        let encoded = encode_ordered_relational_key(primary_key)
            .map_err(|error| RelationalRowPageError::Admission(error.to_string()))?;
        if encoded.len() > self.limits.max_key_bytes.get() {
            return Err(RelationalRowPageError::Admission(format!(
                "range key contains {} bytes, exceeding limit {}",
                encoded.len(),
                self.limits.max_key_bytes
            )));
        }
        let mut lower = 0usize;
        let mut upper = self.row_count;
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            let ordering = self.key(middle)?.cmp(encoded.as_slice());
            if ordering.is_lt() || (!inclusive && ordering.is_eq()) {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }
        Ok(lower)
    }

    pub fn decode_row(&self, ordinal: usize) -> Result<RelationalRow, RelationalRowPageError> {
        let slot = self.slot(ordinal)?;
        let fields = decode_row_fields(
            self.row_for_slot(slot)?,
            self.column_count,
            None,
            self.limits,
        )?;
        Ok(RelationalRow::new(
            fields.into_iter().map(|field| field.value).collect(),
        ))
    }

    pub fn decode_projected_row(
        &self,
        ordinal: usize,
        requested_fields: &[usize],
    ) -> Result<RelationalProjectedRow, RelationalRowPageError> {
        validate_requested_fields(requested_fields, self.column_count, self.limits)?;
        let slot = self.slot(ordinal)?;
        let primary_key =
            decode_ordered_relational_key(self.key_for_slot(slot)?).map_err(|error| {
                RelationalRowPageError::Corrupt(format!("row {ordinal} primary key: {error}"))
            })?;
        let row = self.row_for_slot(slot)?;
        let fields =
            decode_row_fields(row, self.column_count, Some(requested_fields), self.limits)?;
        Ok(RelationalProjectedRow {
            primary_key,
            fields,
        })
    }

    fn decode_projected_row_ref_into<'row>(
        &self,
        ordinal: usize,
        requested_fields: &[usize],
        primary_key: &'row mut RelationalKey,
        fields: &'row mut Vec<RelationalProjectedFieldRef<'a>>,
    ) -> Result<RelationalProjectedRowRef<'row>, RelationalRowPageError>
    where
        'a: 'row,
    {
        let slot = self.slot(ordinal)?;
        let encoded_primary_key = self.key_for_slot(slot)?;
        decode_ordered_relational_key_into(encoded_primary_key, primary_key).map_err(|error| {
            RelationalRowPageError::Corrupt(format!("row {ordinal} primary key: {error}"))
        })?;
        decode_row_field_refs(
            self.row_for_slot(slot)?,
            self.column_count,
            requested_fields,
            self.limits,
            fields,
        )?;
        Ok(RelationalProjectedRowRef {
            primary_key,
            encoded_primary_key,
            fields,
        })
    }

    pub fn find_projected_row(
        &self,
        primary_key: &RelationalKey,
        requested_fields: &[usize],
    ) -> Result<Option<RelationalProjectedRow>, RelationalRowPageError> {
        self.find_row(primary_key)?
            .map(|ordinal| self.decode_projected_row(ordinal, requested_fields))
            .transpose()
    }

    pub(crate) fn find_projected_row_encoded(
        &self,
        encoded_primary_key: &[u8],
        requested_fields: &[usize],
    ) -> Result<Option<RelationalProjectedRow>, RelationalRowPageError> {
        self.find_row_encoded(encoded_primary_key)?
            .map(|ordinal| self.decode_projected_row(ordinal, requested_fields))
            .transpose()
    }

    fn decode_entry(
        &self,
        ordinal: usize,
    ) -> Result<RelationalRowPageEntry, RelationalRowPageError> {
        let (primary_key, row) = self.decode_complete_row(ordinal)?;
        Ok(RelationalRowPageEntry { primary_key, row })
    }

    fn decode_complete_row(
        &self,
        ordinal: usize,
    ) -> Result<(RelationalKey, RelationalRow), RelationalRowPageError> {
        let slot = self.slot(ordinal)?;
        let primary_key =
            decode_ordered_relational_key(self.key_for_slot(slot)?).map_err(|error| {
                RelationalRowPageError::Corrupt(format!("row {ordinal} primary key: {error}"))
            })?;
        let fields = decode_row_fields(
            self.row_for_slot(slot)?,
            self.column_count,
            None,
            self.limits,
        )?;
        Ok((
            primary_key,
            RelationalRow::new(fields.into_iter().map(|field| field.value).collect()),
        ))
    }

    fn validate_directory(&self) -> Result<(), RelationalRowPageError> {
        let mut next_key_offset = 0usize;
        let mut next_row_offset = 0usize;
        let mut previous_key: Option<&[u8]> = None;
        for ordinal in 0..self.row_count {
            let slot = self.slot(ordinal)?;
            if slot.key_offset as usize != next_key_offset {
                return Err(RelationalRowPageError::Corrupt(format!(
                    "row {ordinal} key offset is not contiguous"
                )));
            }
            if slot.row_offset as usize != next_row_offset {
                return Err(RelationalRowPageError::Corrupt(format!(
                    "row {ordinal} payload offset is not contiguous"
                )));
            }
            let key = self.key_for_slot(slot)?;
            if key.is_empty() {
                return Err(RelationalRowPageError::Corrupt(format!(
                    "row {ordinal} primary key is empty"
                )));
            }
            if key.len() > self.limits.max_key_bytes.get() {
                return Err(RelationalRowPageError::Admission(format!(
                    "row {ordinal} primary key contains {} bytes, exceeding limit {}",
                    key.len(),
                    self.limits.max_key_bytes
                )));
            }
            validate_ordered_relational_key(key).map_err(|error| {
                RelationalRowPageError::Corrupt(format!("row {ordinal} primary key: {error}"))
            })?;
            if previous_key.is_some_and(|previous| previous >= key) {
                return Err(RelationalRowPageError::Corrupt(
                    "row primary keys are not strictly increasing".to_string(),
                ));
            }
            let row = self.row_for_slot(slot)?;
            if row.len() > self.limits.max_row_bytes.get() {
                return Err(RelationalRowPageError::Admission(format!(
                    "row {ordinal} contains {} bytes, exceeding limit {}",
                    row.len(),
                    self.limits.max_row_bytes
                )));
            }
            next_key_offset = next_key_offset.checked_add(key.len()).ok_or_else(|| {
                RelationalRowPageError::Corrupt("key payload offset overflow".to_string())
            })?;
            next_row_offset = next_row_offset.checked_add(row.len()).ok_or_else(|| {
                RelationalRowPageError::Corrupt("row payload offset overflow".to_string())
            })?;
            previous_key = Some(key);
        }
        if next_key_offset != self.key_payload.len() || next_row_offset != self.row_payload.len() {
            return Err(RelationalRowPageError::Corrupt(
                "row slot directory does not cover the exact payload".to_string(),
            ));
        }
        if self.key(0)? != self.lower_bound || self.key(self.row_count - 1)? != self.upper_bound {
            return Err(RelationalRowPageError::Corrupt(
                "page primary-key bounds do not match its first and last rows".to_string(),
            ));
        }
        Ok(())
    }

    fn slot(&self, ordinal: usize) -> Result<RowSlot, RelationalRowPageError> {
        if ordinal >= self.row_count {
            return Err(RelationalRowPageError::Admission(format!(
                "row ordinal {ordinal} is outside page row count {}",
                self.row_count
            )));
        }
        let start = ordinal.checked_mul(ROW_SLOT_BYTES).ok_or_else(|| {
            RelationalRowPageError::Corrupt("row slot offset overflow".to_string())
        })?;
        RowSlot::decode(&self.directory[start..start + ROW_SLOT_BYTES])
    }

    fn key(&self, ordinal: usize) -> Result<&'a [u8], RelationalRowPageError> {
        self.key_for_slot(self.slot(ordinal)?)
    }

    fn key_for_slot(&self, slot: RowSlot) -> Result<&'a [u8], RelationalRowPageError> {
        bounded_slice(self.key_payload, slot.key_offset, slot.key_len, "row key")
    }

    fn row_for_slot(&self, slot: RowSlot) -> Result<&'a [u8], RelationalRowPageError> {
        bounded_slice(
            self.row_payload,
            slot.row_offset,
            slot.row_len,
            "row payload",
        )
    }
}

/// A cursor whose row view remains valid only until its next mutable step.
pub(super) trait LendingProjectedRowCursor {
    type Row<'row>
    where
        Self: 'row;

    fn next_row(&mut self) -> Result<Option<Self::Row<'_>>, RelationalRowPageError>;
}

pub(super) struct ProjectedRowPageCursor<'page> {
    view: RelationalRowPageView<'page>,
    next_ordinal: usize,
    requested_fields: &'page [usize],
    primary_key: RelationalKey,
    fields: Vec<RelationalProjectedFieldRef<'page>>,
}

impl<'page> ProjectedRowPageCursor<'page> {
    pub(super) fn new(
        view: RelationalRowPageView<'page>,
        next_ordinal: usize,
        requested_fields: &'page [usize],
    ) -> Result<Self, RelationalRowPageError> {
        validate_requested_fields(requested_fields, view.column_count, view.limits)?;
        Ok(Self {
            view,
            next_ordinal,
            requested_fields,
            primary_key: RelationalKey(Vec::new()),
            fields: Vec::with_capacity(requested_fields.len()),
        })
    }
}

impl LendingProjectedRowCursor for ProjectedRowPageCursor<'_> {
    type Row<'row>
        = RelationalProjectedRowRef<'row>
    where
        Self: 'row;

    fn next_row(&mut self) -> Result<Option<Self::Row<'_>>, RelationalRowPageError> {
        if self.next_ordinal >= self.view.row_count() {
            return Ok(None);
        }
        let ordinal = self.next_ordinal;
        self.next_ordinal += 1;
        self.view
            .decode_projected_row_ref_into(
                ordinal,
                self.requested_fields,
                &mut self.primary_key,
                &mut self.fields,
            )
            .map(Some)
    }
}

#[derive(Debug, Clone, Copy)]
struct RowSlot {
    key_offset: u32,
    key_len: u32,
    row_offset: u32,
    row_len: u32,
}

impl RowSlot {
    fn encode(self, encoded: &mut Vec<u8>) {
        encoded.extend_from_slice(&self.key_offset.to_le_bytes());
        encoded.extend_from_slice(&self.key_len.to_le_bytes());
        encoded.extend_from_slice(&self.row_offset.to_le_bytes());
        encoded.extend_from_slice(&self.row_len.to_le_bytes());
    }

    fn decode(encoded: &[u8]) -> Result<Self, RelationalRowPageError> {
        if encoded.len() != ROW_SLOT_BYTES {
            return Err(RelationalRowPageError::Corrupt(
                "row slot has an invalid length".to_string(),
            ));
        }
        Ok(Self {
            key_offset: read_u32(&encoded[0..4]),
            key_len: read_u32(&encoded[4..8]),
            row_offset: read_u32(&encoded[8..12]),
            row_len: read_u32(&encoded[12..16]),
        })
    }
}

struct DecodedHeader {
    generation: u64,
    source_commit_epoch: u64,
    page_id: RelationalRowPageId,
    row_count: usize,
    column_count: usize,
    lower_bound_len: usize,
    upper_bound_len: usize,
    directory_len: usize,
    key_payload_len: usize,
    row_payload_len: usize,
    schema_digest: Sha256Digest,
    encoded_len: usize,
}

fn decode_header(
    encoded: &[u8],
    limits: RelationalRowPageLimits,
) -> Result<DecodedHeader, RelationalRowPageError> {
    if encoded.len() < ROW_PAGE_HEADER_BYTES || &encoded[..8] != ROW_PAGE_MAGIC {
        return Err(RelationalRowPageError::Corrupt(
            "invalid relational row page header".to_string(),
        ));
    }
    let version = read_u16(&encoded[8..10]);
    let flags = read_u16(&encoded[10..12]);
    if version != ROW_PAGE_VERSION || flags != 0 {
        return Err(RelationalRowPageError::Corrupt(format!(
            "unsupported row page version {version} or flags {flags}"
        )));
    }
    let generation = read_u64(&encoded[12..20]);
    let source_commit_epoch = read_u64(&encoded[20..28]);
    if generation == 0 || source_commit_epoch == 0 {
        return Err(RelationalRowPageError::Corrupt(format!(
            "invalid generation {generation} or source commit epoch {source_commit_epoch}"
        )));
    }
    let page_id = NonZeroU64::new(read_u64(&encoded[28..36]))
        .map(RelationalRowPageId::new)
        .ok_or_else(|| RelationalRowPageError::Corrupt("page id must be non-zero".to_string()))?;
    let row_count = read_u32(&encoded[36..40]) as usize;
    let column_count = read_u32(&encoded[40..44]) as usize;
    if row_count == 0 || row_count > limits.max_rows.get() {
        return Err(RelationalRowPageError::Admission(format!(
            "page declares {row_count} rows, outside admitted range 1..={}",
            limits.max_rows
        )));
    }
    if column_count == 0 || column_count > limits.max_columns.get() {
        return Err(RelationalRowPageError::Admission(format!(
            "page declares {column_count} columns, outside admitted range 1..={}",
            limits.max_columns
        )));
    }
    let lower_bound_len = read_u32(&encoded[44..48]) as usize;
    let upper_bound_len = read_u32(&encoded[48..52]) as usize;
    if lower_bound_len == 0
        || upper_bound_len == 0
        || lower_bound_len > limits.max_key_bytes.get()
        || upper_bound_len > limits.max_key_bytes.get()
    {
        return Err(RelationalRowPageError::Admission(format!(
            "page key bounds contain {lower_bound_len} and {upper_bound_len} bytes, outside admitted range 1..={}",
            limits.max_key_bytes
        )));
    }
    let directory_len = read_u32(&encoded[52..56]) as usize;
    let expected_directory_len = row_count.checked_mul(ROW_SLOT_BYTES).ok_or_else(|| {
        RelationalRowPageError::Corrupt("row slot directory length overflow".to_string())
    })?;
    if directory_len != expected_directory_len {
        return Err(RelationalRowPageError::Corrupt(format!(
            "row slot directory contains {directory_len} bytes, expected {expected_directory_len}"
        )));
    }
    let key_payload_len = usize_from_u64(read_u64(&encoded[56..64]), "key payload length")?;
    let row_payload_len = usize_from_u64(read_u64(&encoded[64..72]), "row payload length")?;
    let schema_digest = Sha256Digest::from_bytes(
        encoded[72..104]
            .try_into()
            .expect("schema digest has a fixed length"),
    );
    let encoded_len = ROW_PAGE_HEADER_BYTES
        .checked_add(lower_bound_len)
        .and_then(|bytes| bytes.checked_add(upper_bound_len))
        .and_then(|bytes| bytes.checked_add(directory_len))
        .and_then(|bytes| bytes.checked_add(key_payload_len))
        .and_then(|bytes| bytes.checked_add(row_payload_len))
        .ok_or_else(|| RelationalRowPageError::Corrupt("page length overflow".to_string()))?;
    if encoded_len > limits.max_page_bytes.get() {
        return Err(RelationalRowPageError::Admission(format!(
            "page declares {encoded_len} bytes, exceeding limit {}",
            limits.max_page_bytes
        )));
    }
    Ok(DecodedHeader {
        generation,
        source_commit_epoch,
        page_id,
        row_count,
        column_count,
        lower_bound_len,
        upper_bound_len,
        directory_len,
        key_payload_len,
        row_payload_len,
        schema_digest,
        encoded_len,
    })
}

fn validate_page_for_encode(
    page: &ImmutableRelationalRowPage,
    limits: RelationalRowPageLimits,
) -> Result<(), RelationalRowPageError> {
    if limits.max_page_bytes.get() < ROW_PAGE_HEADER_BYTES {
        return Err(RelationalRowPageError::Admission(format!(
            "page byte limit {} is smaller than the fixed header",
            limits.max_page_bytes
        )));
    }
    if page.generation == 0 || page.source_commit_epoch == 0 {
        return Err(RelationalRowPageError::Admission(format!(
            "invalid generation {} or source commit epoch {}",
            page.generation, page.source_commit_epoch
        )));
    }
    if page.rows.is_empty() || page.rows.len() > limits.max_rows.get() {
        return Err(RelationalRowPageError::Admission(format!(
            "page contains {} rows, outside admitted range 1..={}",
            page.rows.len(),
            limits.max_rows
        )));
    }
    if page.column_count == 0 || page.column_count > limits.max_columns.get() {
        return Err(RelationalRowPageError::Admission(format!(
            "page contains {} columns, outside admitted range 1..={}",
            page.column_count, limits.max_columns
        )));
    }
    Ok(())
}

fn write_integrity(encoded: &mut [u8]) {
    let mut hasher = IntegrityHasher::new();
    hasher.update(&encoded[..INTEGRITY_PREFIX_BYTES]);
    hasher.update(&encoded[ROW_PAGE_HEADER_BYTES..]);
    let digest = hasher.finish();
    encoded[104..108].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    encoded[108..140].copy_from_slice(digest.sha256.as_bytes());
}

fn verify_integrity(encoded: &[u8]) -> Result<(), RelationalRowPageError> {
    let mut hasher = IntegrityHasher::new();
    hasher.update(&encoded[..INTEGRITY_PREFIX_BYTES]);
    hasher.update(&encoded[ROW_PAGE_HEADER_BYTES..]);
    let digest = hasher.finish();
    if digest.crc32c.get() != read_u32(&encoded[104..108])
        || digest.sha256.as_bytes() != &encoded[108..140]
    {
        return Err(RelationalRowPageError::Corrupt(
            "row page checksum mismatch".to_string(),
        ));
    }
    Ok(())
}

fn take<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    length: usize,
    context: &str,
) -> Result<&'a [u8], RelationalRowPageError> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| RelationalRowPageError::Corrupt(format!("{context} length overflow")))?;
    let bytes = encoded
        .get(*offset..end)
        .ok_or_else(|| RelationalRowPageError::Corrupt(format!("truncated {context}")))?;
    *offset = end;
    Ok(bytes)
}

fn bounded_slice<'a>(
    payload: &'a [u8],
    offset: u32,
    length: u32,
    context: &str,
) -> Result<&'a [u8], RelationalRowPageError> {
    let offset = offset as usize;
    let end = offset
        .checked_add(length as usize)
        .ok_or_else(|| RelationalRowPageError::Corrupt(format!("{context} range overflow")))?;
    payload
        .get(offset..end)
        .ok_or_else(|| RelationalRowPageError::Corrupt(format!("{context} exceeds its payload")))
}

fn u32_len(length: usize, context: &str) -> Result<u32, RelationalRowPageError> {
    u32::try_from(length)
        .map_err(|_| RelationalRowPageError::Admission(format!("{context} does not fit in u32")))
}

fn u64_len(length: usize, context: &str) -> Result<u64, RelationalRowPageError> {
    u64::try_from(length)
        .map_err(|_| RelationalRowPageError::Admission(format!("{context} does not fit in u64")))
}

fn usize_from_u64(value: u64, context: &str) -> Result<usize, RelationalRowPageError> {
    usize::try_from(value)
        .map_err(|_| RelationalRowPageError::Corrupt(format!("{context} overflows usize")))
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

#[cfg(test)]
mod tests;
