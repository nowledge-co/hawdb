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

use super::{
    RelationalError, RelationalOverflowSegment, RelationalProjectedRow, RelationalRow,
    RelationalScalarType, RelationalState, RelationalTableSchema, RelationalValue,
};
use hawdb_integrity::Sha256Digest;
use std::collections::BTreeSet;
use std::sync::Arc;

mod envelope;
mod exact;
mod publication;

pub(in crate::relational) use envelope::admit_overflow_hydration;
use envelope::DEFAULT_ZSTD_LEVEL;
pub(crate) use envelope::{
    decode_overflow_envelope, encode_overflow_envelope, EncodedRelationalOverflow,
};
pub use exact::{
    RelationalOverflowReferenceSet, RelationalOverflowReferenceSetBuilder,
    RelationalOverflowReferenceSortConfig, RelationalOverflowReferenceSortReport,
    DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_OCCURRENCES, DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_RUNS,
    DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_SORT_MEMORY_BYTES,
    DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_SPILL_BYTES,
};
pub use publication::{
    relational_overflow_descriptor_file, relational_overflow_extent_file,
    relational_overflow_manifest_generation_file, RelationalOverflowArtifactMetadata,
    RelationalOverflowExactGenerationRequest, RelationalOverflowExactPublicationReport,
    RelationalOverflowExtentDescriptor, RelationalOverflowExtentInput,
    RelationalOverflowGenerationArtifacts, RelationalOverflowPublicationConfig,
    RelationalOverflowPublicationError, RelationalOverflowPublicationPhase,
    RelationalOverflowPublicationReport, RelationalOverflowPublisher,
    RelationalOverflowRootBinding, RelationalOverflowRootManifest, RelationalOverflowRootReader,
    DEFAULT_RELATIONAL_OVERFLOW_EXTENTS, DEFAULT_RELATIONAL_OVERFLOW_MANIFEST_BYTES,
    DEFAULT_RELATIONAL_OVERFLOW_NEW_EXTENT_BYTES, RELATIONAL_OVERFLOW_MANIFEST_FILE,
};

pub const DEFAULT_RELATIONAL_OVERFLOW_THRESHOLD_BYTES: usize = 4 * 1024;
pub const DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalOverflowConfig {
    pub threshold_bytes: usize,
    pub compression_level: i32,
    pub max_value_bytes: usize,
}

impl Default for RelationalOverflowConfig {
    fn default() -> Self {
        Self {
            threshold_bytes: DEFAULT_RELATIONAL_OVERFLOW_THRESHOLD_BYTES,
            compression_level: DEFAULT_ZSTD_LEVEL,
            max_value_bytes: DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelationalOverflowRef {
    pub digest: Sha256Digest,
    pub scalar_type: RelationalScalarType,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalHydrationBudget {
    pub max_rows: usize,
    pub max_compressed_bytes: usize,
    pub max_decompressed_bytes: usize,
    pub max_memory_bytes: usize,
    pub hydrated_rows: usize,
    pub compressed_bytes: usize,
    pub decompressed_bytes: usize,
    pub memory_bytes: usize,
}

impl Default for RelationalHydrationBudget {
    fn default() -> Self {
        Self {
            max_rows: 1_000,
            max_compressed_bytes: DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES,
            max_decompressed_bytes: DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES,
            max_memory_bytes: DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES,
            hydrated_rows: 0,
            compressed_bytes: 0,
            decompressed_bytes: 0,
            memory_bytes: 0,
        }
    }
}

pub(super) fn externalize_row(
    state: &mut RelationalState,
    schema: &RelationalTableSchema,
    row: &mut RelationalRow,
    config: RelationalOverflowConfig,
) -> Result<(), RelationalError> {
    let protected = protected_column_positions(schema)?;
    let values = Arc::make_mut(&mut row.values);
    for (position, value) in values.iter_mut().enumerate() {
        if protected.contains(&position) {
            continue;
        }
        let (scalar_type, raw) = match value {
            RelationalValue::Text(text) if text.len() >= config.threshold_bytes => {
                (RelationalScalarType::Text, text.as_bytes())
            }
            RelationalValue::Bytea(bytes) if bytes.len() >= config.threshold_bytes => {
                (RelationalScalarType::Bytea, bytes.as_slice())
            }
            _ => continue,
        };
        let encoded = encode_overflow_envelope(scalar_type, raw, config)?;
        let digest = encoded.reference.digest;
        state
            .overflow_segments
            .entry(digest)
            .or_insert_with(|| RelationalOverflowSegment::Inline(Arc::clone(&encoded.bytes)));
        *value = RelationalValue::Overflow(encoded.reference);
    }
    Ok(())
}

pub(super) fn hydrate_row(
    state: &RelationalState,
    row: &RelationalRow,
    budget: &mut RelationalHydrationBudget,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<RelationalRow, RelationalError> {
    runtime_checkpoint(task_context)?;
    let mut staged_budget = *budget;
    if staged_budget.hydrated_rows >= staged_budget.max_rows {
        return Err(RelationalError::Admission(format!(
            "relational hydration exceeds max_rows {}",
            staged_budget.max_rows
        )));
    }
    staged_budget.hydrated_rows += 1;
    let mut values = row.values.to_vec();
    for value in &mut values {
        let RelationalValue::Overflow(reference) = value else {
            continue;
        };
        let segment = state
            .overflow_segments
            .get(&reference.digest)
            .ok_or_else(|| {
                RelationalError::Corruption(format!(
                    "missing overflow segment {}",
                    reference.digest
                ))
            })?;
        runtime_checkpoint(task_context)?;
        let envelope = segment.read()?;
        *value = decode_overflow_envelope(reference, &envelope, &mut staged_budget, task_context)?;
    }
    *budget = staged_budget;
    Ok(RelationalRow::new(values))
}

pub(super) fn hydrate_projected_row(
    state: &RelationalState,
    table: &str,
    row: &mut RelationalProjectedRow,
    budget: &mut RelationalHydrationBudget,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<(), RelationalError> {
    hydrate_projected_row_fields(state, table, row, None, budget, task_context)
}

pub(super) fn hydrate_projected_row_fields(
    state: &RelationalState,
    table: &str,
    row: &mut RelationalProjectedRow,
    required_fields: Option<&[usize]>,
    budget: &mut RelationalHydrationBudget,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<(), RelationalError> {
    if !row
        .fields
        .iter()
        .any(|field| matches!(field.value, RelationalValue::Overflow(_)))
    {
        return Ok(());
    }
    runtime_checkpoint(task_context)?;
    let canonical = if state.canonical_row_metadata_only() {
        None
    } else {
        Some(state.row(table, &row.primary_key).ok_or_else(|| {
            RelationalError::Corruption(format!(
                "projected overflow resolver cannot find row in table {table}"
            ))
        })?)
    };
    let schema = state.table_schema(table).ok_or_else(|| {
        RelationalError::Corruption(format!(
            "projected overflow resolver cannot find schema for table {table}"
        ))
    })?;
    let mut staged_budget = *budget;
    let mut hydrated_row = false;
    for field in &mut row.fields {
        let RelationalValue::Overflow(reference) = &field.value else {
            continue;
        };
        let column = schema.columns.get(field.ordinal).ok_or_else(|| {
            RelationalError::Corruption(format!(
                "projected field {} is outside the canonical row shape for table {table}",
                field.ordinal
            ))
        })?;
        if column.scalar_type != reference.scalar_type
            || !matches!(
                column.scalar_type,
                RelationalScalarType::Text | RelationalScalarType::Bytea
            )
        {
            return Err(RelationalError::Corruption(format!(
                "projected overflow reference at field {} has type {:?}, expected {:?} in table {table}",
                field.ordinal, reference.scalar_type, column.scalar_type
            )));
        }
        if let Some(canonical) = canonical {
            let canonical_value = canonical.values().get(field.ordinal).ok_or_else(|| {
                RelationalError::Corruption(format!(
                    "projected field {} is outside the canonical row shape for table {table}",
                    field.ordinal
                ))
            })?;
            if canonical_value != &RelationalValue::Overflow(*reference) {
                return Err(RelationalError::Corruption(format!(
                    "projected overflow reference at field {} differs from the canonical row in table {table}",
                    field.ordinal
                )));
            }
        }
        let segment = state
            .overflow_segments
            .get(&reference.digest)
            .ok_or_else(|| {
                RelationalError::Corruption(format!(
                    "missing overflow segment {}",
                    reference.digest
                ))
            })?;
        if required_fields.is_some_and(|fields| !fields.contains(&field.ordinal)) {
            continue;
        }
        if !hydrated_row {
            if staged_budget.hydrated_rows >= staged_budget.max_rows {
                return Err(RelationalError::Admission(format!(
                    "relational hydration exceeds max_rows {}",
                    staged_budget.max_rows
                )));
            }
            staged_budget.hydrated_rows += 1;
            hydrated_row = true;
        }
        runtime_checkpoint(task_context)?;
        let envelope = segment.read()?;
        field.value =
            decode_overflow_envelope(reference, &envelope, &mut staged_budget, task_context)?;
    }
    *budget = staged_budget;
    Ok(())
}

fn runtime_checkpoint(
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<(), RelationalError> {
    task_context.map_or(Ok(()), |context| {
        context.checkpoint().map_err(|reason| {
            RelationalError::Admission(format!("relational hydration stopped: {reason}"))
        })
    })
}

pub(super) fn prune_unreachable_segments(state: &mut RelationalState) {
    let reachable = state
        .segments
        .values()
        .flat_map(|segment| segment.rows.values())
        .flat_map(|row| row.values.iter())
        .filter_map(|value| match value {
            RelationalValue::Overflow(reference) => Some(reference.digest),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    state
        .overflow_segments
        .retain(|digest, _| reachable.contains(digest));
}

fn protected_column_positions(
    schema: &RelationalTableSchema,
) -> Result<BTreeSet<usize>, RelationalError> {
    let names = schema
        .primary_key
        .iter()
        .chain(schema.unique_constraints.iter().flatten())
        .chain(
            schema
                .foreign_keys
                .iter()
                .flat_map(|key| key.columns.iter()),
        )
        .chain(schema.indexes.iter().flat_map(|index| index.columns.iter()))
        .collect::<BTreeSet<_>>();
    names
        .into_iter()
        .map(|name| {
            schema.column_position(name).ok_or_else(|| {
                RelationalError::Schema(format!(
                    "overflow protection references unknown column {name}"
                ))
            })
        })
        .collect()
}
