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

//! Materializes a complete relational state from a canonical row-page root.
//!
//! A checkpoint whose relational rows are carried by the row-page and overflow
//! roots does not write the legacy full-row `relational.N.hawdb` file (#1737).
//! Modes that serve SQL from resident rows rebuild the same state that the
//! legacy decoder produced: every row, file-backed overflow segments and, when
//! requested, materialized index postings.

use super::{
    rebuild_indexes, validate_checkpoint_overflow_reachability, validate_foreign_keys,
    validate_row, validate_table_schema, RelationalCheckpoint, RelationalCheckpointIndexLoad,
    RelationalDecodeLimits, RelationalError, RelationalOverflowSegment, RelationalState,
    RelationalTableSegment, RelationalValue,
};
use crate::cache::ContentDigest;
use crate::relational::{
    relational_overflow_extent_file, relational_row_page_artifact_file,
    RelationalOverflowExtentDescriptor, RelationalOverflowPublicationError, RelationalOverflowRef,
    RelationalOverflowRootReader, RelationalRowPagePublicationError, RelationalRowPageRootReader,
};
use crate::scan::{FileSegmentRangeReader, SegmentReadRange};
use hawdb_integrity::Sha256Digest;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;

struct ReleaseIdleHandles<'a>(&'a [std::path::PathBuf]);

impl Drop for ReleaseIdleHandles<'_> {
    fn drop(&mut self) {
        let Some(first) = self.0.first() else {
            return;
        };
        // Releasing idle cache entries is best effort; a failure leaves them
        // to ordinary LRU eviction.
        if let Ok(context) = crate::file_descriptors::context_for_path(first)
            && let Some(handles) = context.state.existing_immutable_handles()
        {
            let _ = handles.release_idle_paths(self.0);
        }
    }
}

/// Artifact identifiers for overflow extent files start above the legacy
/// relational checkpoint identifier and keep the physical generation.
const OVERFLOW_EXTENT_ARTIFACT_BASE: u64 = 1 << 62;

struct RowRootDecodeAdmission {
    limits: RelationalDecodeLimits,
    values: usize,
}

impl RowRootDecodeAdmission {
    fn new(limits: RelationalDecodeLimits) -> Self {
        Self { limits, values: 0 }
    }

    /// Applies the same schema string and literal-value limits as the legacy
    /// full-row checkpoint decoder. Row-root manifests have already decoded
    /// their schemas under the publication limits, which are intentionally
    /// independent from the caller's checkpoint decode limits.
    fn schema(&mut self, schema: &super::RelationalTableSchema) -> Result<(), RelationalError> {
        let encoded = super::encode_relational_table_schema(schema)?;
        let schema_limits = RelationalDecodeLimits {
            max_record_bytes: encoded.len(),
            max_tables: 1,
            max_writes: 0,
            max_rows: 0,
            max_values: self.limits.max_values,
            max_value_bytes: self.limits.max_value_bytes,
            max_overflow_segments: 0,
            max_overflow_bytes: 0,
        };
        let mut decoder = super::Decoder::from_slice(&encoded, schema_limits, false);
        drop(decoder.table_schema()?);
        let schema_values = decoder.values;
        decoder.finish()?;
        self.charge_values(schema_values)
    }

    fn string(&self, value: &str, context: &str) -> Result<(), RelationalError> {
        self.bytes(value.len(), context)
    }

    fn key(&mut self, key: &super::RelationalKey) -> Result<(), RelationalError> {
        self.charge_values(key.0.len())?;
        for value in &key.0 {
            self.value_bytes(value)?;
        }
        Ok(())
    }

    fn row(&mut self, row: &super::RelationalRow) -> Result<(), RelationalError> {
        self.charge_values(row.values().len())?;
        for value in row.values() {
            self.value_bytes(value)?;
        }
        Ok(())
    }

    fn charge_values(&mut self, count: usize) -> Result<(), RelationalError> {
        self.values = self.values.checked_add(count).ok_or_else(|| {
            RelationalError::Admission("decoded value count overflow".to_string())
        })?;
        if self.values > self.limits.max_values {
            return Err(RelationalError::Admission(format!(
                "decoded value count exceeds limit {}",
                self.limits.max_values
            )));
        }
        Ok(())
    }

    fn value_bytes(&self, value: &RelationalValue) -> Result<(), RelationalError> {
        match value {
            RelationalValue::Text(value) => self.bytes(value.len(), "TEXT value"),
            RelationalValue::Bytea(value) => self.bytes(value.len(), "BYTEA value"),
            RelationalValue::Vector(value) => self.bytes(value.encoded().len(), "VECTOR value"),
            RelationalValue::Overflow(reference) => {
                let max = u64::try_from(self.limits.max_value_bytes).map_err(|_| {
                    RelationalError::Admission(
                        "relational value limit does not fit u64".to_string(),
                    )
                })?;
                if reference.compressed_bytes == 0
                    || reference.uncompressed_bytes == 0
                    || reference.compressed_bytes > max
                    || reference.uncompressed_bytes > max
                {
                    return Err(RelationalError::Admission(format!(
                        "overflow reference contains {} compressed and {} decoded bytes, outside limit {}",
                        reference.compressed_bytes,
                        reference.uncompressed_bytes,
                        self.limits.max_value_bytes
                    )));
                }
                Ok(())
            }
            RelationalValue::Null
            | RelationalValue::Boolean(_)
            | RelationalValue::BigInt(_)
            | RelationalValue::DoublePrecision(_)
            | RelationalValue::Decimal(_)
            | RelationalValue::Uuid(_)
            | RelationalValue::Timestamp(_)
            | RelationalValue::TimestampTz(_) => Ok(()),
        }
    }

    fn bytes(&self, len: usize, context: &str) -> Result<(), RelationalError> {
        if len > self.limits.max_value_bytes {
            return Err(RelationalError::Admission(format!(
                "decoded {context} contains {len} bytes, exceeding limit {}",
                self.limits.max_value_bytes
            )));
        }
        Ok(())
    }
}

/// Keeps a typed overflow-root read failure typed. Descriptor exhaustion,
/// a newer format, a stopped task, admission and I/O failures are not store
/// damage; only content or structure that disagrees with the root is
/// `Corruption`.
fn overflow_error(error: RelationalOverflowPublicationError) -> RelationalError {
    match error {
        RelationalOverflowPublicationError::FileDescriptors(error) => {
            RelationalError::FileDescriptors(error)
        }
        RelationalOverflowPublicationError::UnsupportedFormat(error) => {
            RelationalError::UnsupportedFormat(error)
        }
        RelationalOverflowPublicationError::Stopped(reason) => RelationalError::Stopped(reason),
        RelationalOverflowPublicationError::Admission(message) => {
            RelationalError::Admission(message)
        }
        RelationalOverflowPublicationError::Durability(message) => {
            RelationalError::Durability(message)
        }
        // Reads never publish; keep an unexpected diagnostic as an I/O
        // failure rather than an integrity alarm.
        RelationalOverflowPublicationError::PublishOutcomeUnknown(error) => {
            RelationalError::Durability(error.to_string())
        }
        RelationalOverflowPublicationError::Corrupt(message) => {
            RelationalError::Corruption(message)
        }
        error @ (RelationalOverflowPublicationError::MissingExtent(_)
        | RelationalOverflowPublicationError::StaleGeneration { .. }) => {
            RelationalError::Corruption(error.to_string())
        }
    }
}

/// Keeps a typed row-page root read failure typed; see [`overflow_error`].
fn row_page_error(error: RelationalRowPagePublicationError) -> RelationalError {
    match error {
        RelationalRowPagePublicationError::FileDescriptors(error) => {
            RelationalError::FileDescriptors(error)
        }
        RelationalRowPagePublicationError::UnsupportedFormat(error) => {
            RelationalError::UnsupportedFormat(error)
        }
        RelationalRowPagePublicationError::Stopped(reason) => RelationalError::Stopped(reason),
        RelationalRowPagePublicationError::Admission(message) => {
            RelationalError::Admission(message)
        }
        RelationalRowPagePublicationError::Durability(message) => {
            RelationalError::Durability(message)
        }
        RelationalRowPagePublicationError::PublishOutcomeUnknown(error) => {
            RelationalError::Durability(error.to_string())
        }
        RelationalRowPagePublicationError::Corrupt(message) => RelationalError::Corruption(message),
        // The manifest names the table, so its pages must exist.
        error @ (RelationalRowPagePublicationError::MissingTable(_)
        | RelationalRowPagePublicationError::StaleGeneration { .. }) => {
            RelationalError::Corruption(error.to_string())
        }
    }
}

fn overflow_artifact_id(physical_generation: u64) -> Result<u64, RelationalError> {
    if physical_generation >= OVERFLOW_EXTENT_ARTIFACT_BASE {
        return Err(RelationalError::Corruption(format!(
            "overflow physical generation {physical_generation} exceeds the artifact identifier space"
        )));
    }
    Ok(OVERFLOW_EXTENT_ARTIFACT_BASE | physical_generation)
}

/// Returns whether a segment is already backed by an overflow extent file.
pub(in crate::relational) fn overflow_segment_is_root_backed(
    segment: &RelationalOverflowSegment,
) -> bool {
    matches!(
        segment,
        RelationalOverflowSegment::FileRange { range, .. }
            if range.artifact_id & OVERFLOW_EXTENT_ARTIFACT_BASE != 0
    )
}

/// Builds file-backed overflow segments for `digests` from one overflow root.
///
/// The returned segments read the immutable extent files named by each
/// descriptor's physical generation, so they remain valid while the root that
/// references those extents is retained.
fn overflow_segments_from_root(
    directory: &Path,
    overflow: &RelationalOverflowRootReader,
    references: &BTreeMap<Sha256Digest, RelationalOverflowRef>,
) -> Result<BTreeMap<Sha256Digest, RelationalOverflowSegment>, RelationalError> {
    let mut descriptors = Vec::with_capacity(references.len());
    // A small closure uses bounded binary searches; a large closure scans the
    // sorted descriptor artifact once.
    let extent_count = overflow.manifest().extent_count;
    let lookups = u64::try_from(references.len()).unwrap_or(u64::MAX);
    let searches = lookups.saturating_mul(u64::from(u64::BITS - extent_count.leading_zeros()));
    if searches < extent_count {
        for reference in references.values() {
            let descriptor = overflow
                .find_descriptor(reference)
                .map_err(overflow_error)?
                .ok_or_else(|| {
                    RelationalError::Corruption(format!(
                        "overflow root is missing reachable extent {}",
                        reference.digest
                    ))
                })?;
            descriptors.push(descriptor);
        }
    } else if !references.is_empty() {
        let mut wanted = references.values().peekable();
        overflow
            .visit_descriptors(|descriptor| {
                while wanted
                    .peek()
                    .is_some_and(|reference| reference.digest < descriptor.reference.digest)
                {
                    wanted.next();
                }
                if let Some(reference) = wanted.peek()
                    && reference.digest == descriptor.reference.digest
                {
                    if **reference != descriptor.reference {
                        return Err(RelationalOverflowPublicationError::Corrupt(format!(
                            "overflow descriptor {} metadata differs from its row reference",
                            reference.digest
                        )));
                    }
                    descriptors.push(*descriptor);
                    wanted.next();
                }
                Ok(())
            })
            .map_err(overflow_error)?;
        if descriptors.len() != references.len() {
            let found = descriptors
                .iter()
                .map(|descriptor| descriptor.reference.digest)
                .collect::<BTreeSet<_>>();
            let missing = references
                .keys()
                .find(|digest| !found.contains(*digest))
                .expect("a requested digest is missing");
            return Err(RelationalError::Corruption(format!(
                "overflow root is missing reachable extent {missing}"
            )));
        }
    }
    file_segments(directory, &descriptors)
}

/// Builds file-backed overflow segments for content digests whose reference
/// metadata is owned by already validated resident rows.
pub(in crate::relational) fn overflow_segments_by_digest_from_root(
    directory: &Path,
    overflow: &RelationalOverflowRootReader,
    digests: &BTreeSet<Sha256Digest>,
) -> Result<BTreeMap<Sha256Digest, RelationalOverflowSegment>, RelationalError> {
    let descriptors = digests
        .iter()
        .map(|digest| {
            overflow
                .find_descriptor_by_digest(digest)
                .map_err(overflow_error)?
                .ok_or_else(|| {
                    RelationalError::Corruption(format!(
                        "overflow root is missing resident extent {digest}"
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    file_segments(directory, &descriptors)
}

fn file_segments(
    directory: &Path,
    descriptors: &[RelationalOverflowExtentDescriptor],
) -> Result<BTreeMap<Sha256Digest, RelationalOverflowSegment>, RelationalError> {
    let mut reader = FileSegmentRangeReader::new();
    let physical_generations = descriptors
        .iter()
        .map(|descriptor| descriptor.physical_generation)
        .collect::<BTreeSet<_>>();
    for physical_generation in physical_generations {
        reader.register(
            overflow_artifact_id(physical_generation)?,
            directory.join(relational_overflow_extent_file(physical_generation)),
        );
    }
    let reader = Arc::new(reader);
    descriptors
        .iter()
        .enumerate()
        .map(|(ordinal, descriptor)| {
            Ok((
                descriptor.reference.digest,
                file_segment(&reader, ordinal, descriptor)?,
            ))
        })
        .collect()
}

fn file_segment(
    reader: &Arc<FileSegmentRangeReader>,
    ordinal: usize,
    descriptor: &RelationalOverflowExtentDescriptor,
) -> Result<RelationalOverflowSegment, RelationalError> {
    let length = NonZeroU64::new(descriptor.envelope_bytes).ok_or_else(|| {
        RelationalError::Corruption(format!(
            "overflow extent {} is empty",
            descriptor.reference.digest
        ))
    })?;
    let range = SegmentReadRange::new(
        overflow_artifact_id(descriptor.physical_generation)?,
        ordinal as u64,
        descriptor.physical_offset,
        length,
    )
    .with_content_digest(ContentDigest(u64::from(descriptor.envelope_crc32c)));
    Ok(RelationalOverflowSegment::FileRange {
        reader: Arc::clone(reader),
        range,
    })
}

/// Rebuilds the complete relational checkpoint state from a canonical
/// row-page root and its bound overflow root.
///
/// The row root applies `max_tables`, `max_rows`, `max_values`,
/// `max_value_bytes`, and `max_overflow_segments`. `max_writes` is a WAL-only
/// limit. `max_record_bytes` does not apply because this layout has no
/// monolithic relational record, and `max_overflow_bytes` does not apply
/// because overflow envelopes stay file-backed instead of being decoded into
/// memory; their bound root enforces its own artifact and extent limits.
pub fn decode_relational_checkpoint_from_row_root(
    directory: &Path,
    rows: &RelationalRowPageRootReader,
    overflow: &RelationalOverflowRootReader,
    limits: RelationalDecodeLimits,
    index_load: RelationalCheckpointIndexLoad,
) -> Result<RelationalCheckpoint, RelationalError> {
    let manifest = rows.manifest();
    if manifest.tables.len() > limits.max_tables {
        return Err(RelationalError::Admission(format!(
            "relational row root contains {} tables, exceeding limit {}",
            manifest.tables.len(),
            limits.max_tables
        )));
    }
    let mut state = RelationalState::default();
    let scanned_paths = manifest
        .physical_generations
        .iter()
        .map(|entry| directory.join(relational_row_page_artifact_file(entry.generation)))
        .collect::<Vec<_>>();
    // The scan reads every row page once. Release its idle page-artifact
    // handles afterwards so open keeps the descriptor footprint of demand
    // reads; the root descriptor and key artifacts stay cached for mount.
    let _release = ReleaseIdleHandles(&scanned_paths);
    let mut total_rows = 0usize;
    let mut reachable = BTreeMap::new();
    let mut admission = RowRootDecodeAdmission::new(limits);
    for table in &manifest.tables {
        let schema = &table.schema;
        // A legacy checkpoint carries the table name once outside the schema
        // and once inside it. Preserve both admission checks.
        admission.string(&table.table, "table name")?;
        admission.schema(schema)?;
        validate_table_schema(schema)?;
        if table.table != schema.name || state.schemas.contains_key(&table.table) {
            return Err(RelationalError::Corruption(format!(
                "relational row root has a duplicate or mismatched table {}",
                table.table
            )));
        }
        if !schema.primary_key.is_empty() && table.last_row_id != 0 {
            return Err(RelationalError::Corruption(
                "primary-key table has a hidden row ID allocator".into(),
            ));
        }
        let expected_rows = usize::try_from(table.row_count).map_err(|_| {
            RelationalError::Admission(format!(
                "relational row root table {} row count {} exceeds platform capacity",
                table.table, table.row_count
            ))
        })?;
        total_rows = total_rows
            .checked_add(expected_rows)
            .filter(|rows| *rows <= limits.max_rows)
            .ok_or_else(|| {
                RelationalError::Admission(format!(
                    "relational row root rows exceed limit {}",
                    limits.max_rows
                ))
            })?;
        // Collect the descriptors first so the descriptor and key artifacts
        // are closed before page reads: open holds one file at a time.
        let mut descriptors = Vec::new();
        rows.visit_table_pages(&table.table, |descriptor| {
            descriptors.push(descriptor.clone());
            Ok(())
        })
        .map_err(row_page_error)?;
        let mut table_rows = BTreeMap::new();
        for descriptor in &descriptors {
            let page = rows.read_page(descriptor).map_err(row_page_error)?;
            if page.schema_digest != table.schema_digest {
                return Err(RelationalError::Corruption(format!(
                    "row page {} schema digest differs from table {}",
                    descriptor.logical_page_id.get(),
                    table.table
                )));
            }
            for entry in page.rows {
                if schema.primary_key.is_empty() {
                    // Legacy checkpoints encode hidden row handles separately
                    // from their rows. Declared primary keys are derived from
                    // row values and must not be charged twice.
                    admission.key(&entry.primary_key)?;
                }
                admission.row(&entry.row)?;
                if table_rows.insert(entry.primary_key, entry.row).is_some() {
                    return Err(RelationalError::Corruption(format!(
                        "relational row root table {} repeats a primary key",
                        table.table
                    )));
                }
            }
        }
        if table_rows.len() != expected_rows {
            return Err(RelationalError::Corruption(format!(
                "relational row root table {} contains {} rows, expected {expected_rows}",
                table.table,
                table_rows.len()
            )));
        }
        for (key, row) in &table_rows {
            if schema.primary_key.is_empty() {
                super::super::validate_hidden_row_handle(key, table.last_row_id)?;
            }
            validate_row(schema, row)?;
            for value in row.values() {
                if let RelationalValue::Overflow(reference) = value
                    && let Some(previous) = reachable.insert(reference.digest, *reference)
                    && previous != *reference
                {
                    return Err(RelationalError::Corruption(format!(
                        "overflow digest {} has conflicting reference metadata",
                        reference.digest
                    )));
                }
            }
        }
        state
            .schemas
            .insert(table.table.clone(), Arc::new(schema.clone().into()));
        state.segments.insert(
            table.table.clone(),
            Arc::new(RelationalTableSegment {
                last_row_id: table.last_row_id,
                rows: super::super::RelationalRowPages::from_map(table_rows),
                indexes: BTreeMap::new(),
                _memory: Default::default(),
            }),
        );
    }
    if reachable.len() > limits.max_overflow_segments {
        return Err(RelationalError::Admission(format!(
            "relational row root references {} overflow segments, exceeding limit {}",
            reachable.len(),
            limits.max_overflow_segments
        )));
    }
    state.overflow_segments = overflow_segments_from_root(directory, overflow, &reachable)?;
    validate_checkpoint_overflow_reachability(&state)?;
    for schema in state.table_schemas() {
        super::super::materialized_view::validate_definition(&state, schema)?;
        if schema.materialized_view.is_some()
            && index_load == RelationalCheckpointIndexLoad::OmitMaterializedPostings
        {
            return Err(RelationalError::Admission(
                "materialized views do not support authoritative relational indexes".into(),
            ));
        }
    }
    if index_load == RelationalCheckpointIndexLoad::MaterializedPostings {
        let table_names = state.schemas.keys().cloned().collect::<Vec<_>>();
        for table in table_names {
            rebuild_indexes(&mut state, &table)?;
        }
    } else {
        state.materialized_index_postings_resident = false;
    }
    validate_foreign_keys(&state)?;
    Ok(RelationalCheckpoint {
        epoch: manifest.source_commit_epoch,
        state,
    })
}

#[cfg(test)]
#[path = "row_root/tests.rs"]
mod tests;
