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
    RelationalError, RelationalKey, RelationalOverflowSegment, RelationalProjectedRow,
    RelationalRow, RelationalScalarType, RelationalState, RelationalTableSchema, RelationalValue,
};
use hawdb_integrity::Sha256Digest;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub(super) mod checkpoint;
mod envelope;
mod exact;
mod publication;

pub(in crate::relational) use envelope::admit_overflow_hydration;
use envelope::DEFAULT_ZSTD_LEVEL;
pub(crate) use envelope::{
    decode_overflow_envelope, encode_overflow_envelope, encode_overflow_envelope_with_work_context,
    validate_overflow_envelope_with_work_context, CheckpointRelationalOverflow,
    EncodedRelationalOverflow,
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
    RelationalOverflowGenerationArtifacts, RelationalOverflowInputs, RelationalOverflowInputsIter,
    RelationalOverflowPublicationConfig, RelationalOverflowPublicationError,
    RelationalOverflowPublicationPhase, RelationalOverflowPublicationReport,
    RelationalOverflowPublisher, RelationalOverflowRootBinding, RelationalOverflowRootManifest,
    RelationalOverflowRootReader, DEFAULT_RELATIONAL_OVERFLOW_EXTENTS,
    DEFAULT_RELATIONAL_OVERFLOW_MANIFEST_BYTES, DEFAULT_RELATIONAL_OVERFLOW_NEW_EXTENT_BYTES,
    RELATIONAL_OVERFLOW_MANIFEST_FILE,
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

pub(super) fn externalize_row_with_allocations(
    state: &mut RelationalState,
    schema: &RelationalTableSchema,
    row: &mut RelationalRow,
    config: RelationalOverflowConfig,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<(), RelationalError> {
    let Some(context) = allocations else {
        return externalize_row(state, schema, row, config);
    };
    let protection_context = crate::background::CheckpointDecodeContext {
        work: context.work.clone(),
        memory: Default::default(),
    };
    let protected = protected_column_positions_with_allocations(schema, &protection_context)?;
    if Arc::strong_count(&row.values) != 1 {
        *row = RelationalRow::reconstruct_with_allocations(row, Some(context), |ordinal| {
            Ok(row.values()[ordinal].as_ref())
        })?;
    }
    let values = Arc::get_mut(&mut row.values).expect("externalization has unique row backing");
    for (position, value) in values.iter_mut().enumerate() {
        let unit = context
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let input = if protected.contains(&position) {
            None
        } else {
            match value {
                RelationalValue::Text(text) if text.len() >= config.threshold_bytes => {
                    Some((RelationalScalarType::Text, text.as_bytes()))
                }
                RelationalValue::Bytea(bytes) if bytes.len() >= config.threshold_bytes => {
                    Some((RelationalScalarType::Bytea, bytes.as_slice()))
                }
                _ => None,
            }
        };
        unit.finish();
        let Some((scalar_type, raw)) = input else {
            continue;
        };
        let encoded =
            encode_overflow_envelope_with_work_context(scalar_type, raw, config, &context.work)?;
        let digest = encoded.reference.digest;
        if !state.overflow_segments.contains_key(&digest) {
            let node_context = crate::background::CheckpointDecodeContext {
                work: context.work.clone(),
                memory: Default::default(),
            };
            node_context
                .reserve(crate::cow::checkpoint_tree_node_bytes::<
                    Sha256Digest,
                    RelationalOverflowSegment,
                >())
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            // Transfer node ownership before allocating the map entry. Refusal
            // during transfer cannot leave a live node without its inventory.
            let mut memory = node_context.memory.into_inner();
            state
                ._metadata_memory
                .append_retained(&mut memory, &context.work)
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            let unit = context
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            state.overflow_segments.insert(
                digest,
                RelationalOverflowSegment::CheckpointInline(encoded.bytes),
            );
            unit.finish();
        }
        *value = RelationalValue::Overflow(encoded.reference);
    }
    context
        .checkpoint()
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    Ok(())
}

fn protected_column_positions_with_allocations(
    schema: &RelationalTableSchema,
    context: &crate::background::CheckpointDecodeContext,
) -> Result<BTreeSet<usize>, RelationalError> {
    let mut positions = BTreeSet::new();
    for name in schema
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
    {
        let position = schema.column_position(name).ok_or_else(|| {
            RelationalError::Schema(format!(
                "overflow protection references unknown column {name}"
            ))
        })?;
        if !positions.contains(&position) {
            context
                .reserve(crate::cow::checkpoint_tree_node_bytes::<usize, ()>())
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
        }
        let unit = context
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        positions.insert(position);
        unit.finish();
    }
    Ok(positions)
}

pub(super) fn hydrate_row(
    state: &RelationalState,
    row: &RelationalRow,
    budget: &mut RelationalHydrationBudget,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<RelationalRow, RelationalError> {
    hydrate_row_values(
        state,
        || RelationalRow::new(row.values.to_vec()),
        budget,
        task_context,
        None,
    )
}

pub(super) fn hydrate_owned_row(
    state: &RelationalState,
    row: RelationalRow,
    budget: &mut RelationalHydrationBudget,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<RelationalRow, RelationalError> {
    hydrate_row_values(state, || row, budget, task_context, allocations)
}

fn hydrate_row_values(
    state: &RelationalState,
    row: impl FnOnce() -> RelationalRow,
    budget: &mut RelationalHydrationBudget,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
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
    let mut row = row();
    let values = Arc::get_mut(&mut row.values).ok_or_else(|| {
        RelationalError::Admission("owned recovery hydration requires unique row backing".into())
    })?;
    for value in values {
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
        let envelope = segment.read_with_work_context(allocations.map(|context| &context.work))?;
        *value = envelope::decode_overflow_envelope_with_allocations(
            reference,
            &envelope,
            &mut staged_budget,
            task_context,
            allocations,
        )?;
    }
    *budget = staged_budget;
    Ok(row)
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
    hydrate_projected_row_fields_with_allocations(
        state,
        table,
        row,
        required_fields,
        budget,
        task_context,
        None,
    )
}

pub(super) fn hydrate_projected_row_fields_with_allocations(
    state: &RelationalState,
    table: &str,
    row: &mut RelationalProjectedRow,
    required_fields: Option<&[usize]>,
    budget: &mut RelationalHydrationBudget,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
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
        let envelope = segment.read_with_work_context(allocations.map(|context| &context.work))?;
        field.value = envelope::decode_overflow_envelope_with_allocations(
            reference,
            &envelope,
            &mut staged_budget,
            task_context,
            allocations,
        )?;
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

thread_local! {
    static REACHABILITY_ROWS_VISITED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Rows this thread has examined to keep `overflow_segments` equal to the set
/// of reachable overflow digests after a transaction. A test hook: the
/// structural bound that pruning work follows the rows a transaction changed,
/// not the size of the database, is asserted on this counter.
#[doc(hidden)]
pub fn relational_overflow_reachability_rows_visited() -> u64 {
    REACHABILITY_ROWS_VISITED.with(std::cell::Cell::get)
}

fn note_reachability_rows_visited(rows: usize) {
    REACHABILITY_ROWS_VISITED.with(|visited| {
        visited.set(
            visited
                .get()
                .saturating_add(u64::try_from(rows).unwrap_or(u64::MAX)),
        );
    });
}

fn overflow_digests(row: &RelationalRow) -> impl Iterator<Item = Sha256Digest> + '_ {
    row.values.iter().filter_map(|value| match value {
        RelationalValue::Overflow(reference) => Some(reference.digest),
        _ => None,
    })
}

/// Rows of `table` at `keys` in `state`, or `None` for an absent key.
fn rows_at_keys<'a>(
    state: &'a RelationalState,
    table: &str,
    keys: &'a BTreeSet<RelationalKey>,
) -> impl Iterator<Item = Option<&'a RelationalRow>> + 'a {
    let segment = state.segments.get(table);
    keys.iter()
        .map(move |key| segment.and_then(|segment| segment.rows.get(key)))
}

/// Keeps `next.overflow_segments` equal to the digests its rows reference,
/// examining only the rows the transaction changed whenever that is exact.
///
/// `previous` is the committed state the transaction started from; its
/// segments are exactly its reachable digests (every commit leaves that
/// invariant, and checkpoint decode rejects anything else). Segments are only
/// added during a transaction. So for a transaction whose row changes are all
/// listed in `changed_keys`:
///
/// - a new digest can only be referenced by a changed row;
/// - a previous digest stays reachable unless a changed row dropped a
///   reference to it; only then does a row scan decide, stopping as soon as
///   every such digest is found.
///
/// An insert-only transaction (bulk load) therefore costs its own rows, not
/// the database. Whole-table changes (DDL, view drops, column rewrites) and
/// non-resident row states use the full reachability pass.
pub(super) fn prune_unreachable_segments_after_changes(
    previous: &RelationalState,
    next: &mut RelationalState,
    changed_keys: &BTreeMap<String, BTreeSet<RelationalKey>>,
    whole_table_changes: bool,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<(), RelationalError> {
    if whole_table_changes
        || !previous.materialized_rows_resident
        || !next.materialized_rows_resident
    {
        return prune_unreachable_segments_with_allocations(next, allocations);
    }
    #[cfg(debug_assertions)]
    let expected = {
        let mut full = next.clone();
        let visited = relational_overflow_reachability_rows_visited();
        prune_unreachable_segments(&mut full);
        REACHABILITY_ROWS_VISITED.with(|counter| counter.set(visited));
        full.overflow_segments.keys().copied().collect::<Vec<_>>()
    };
    prune_changed_segments(previous, next, changed_keys, allocations)?;
    #[cfg(debug_assertions)]
    debug_assert!(
        next.overflow_segments.keys().copied().eq(expected),
        "incremental overflow pruning diverged from full reachability"
    );
    Ok(())
}

fn admission(error: impl ToString) -> RelationalError {
    RelationalError::Admission(error.to_string())
}

fn prune_changed_segments(
    previous: &RelationalState,
    next: &mut RelationalState,
    changed_keys: &BTreeMap<String, BTreeSet<RelationalKey>>,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<(), RelationalError> {
    if next.overflow_segments.is_empty() {
        return Ok(());
    }
    let scratch = allocations.map(|context| crate::background::CheckpointDecodeContext {
        work: context.work.clone(),
        memory: Default::default(),
    });
    let charge_unit = || -> Result<(), RelationalError> {
        if let Some(scratch) = &scratch {
            scratch.start_unit().map_err(admission)?.finish();
        }
        Ok(())
    };
    let insert_digest =
        |set: &mut BTreeSet<Sha256Digest>, digest: Sha256Digest| -> Result<(), RelationalError> {
            if let Some(scratch) = &scratch
                && !set.contains(&digest)
            {
                scratch
                    .reserve(crate::cow::checkpoint_tree_node_bytes::<Sha256Digest, ()>())
                    .map_err(admission)?;
            }
            set.insert(digest);
            Ok(())
        };
    let mut retained = BTreeSet::new();
    let mut released = BTreeSet::new();
    let mut visited = 0usize;
    for (table, keys) in changed_keys {
        for row in rows_at_keys(next, table, keys).flatten() {
            visited = visited.saturating_add(1);
            for digest in overflow_digests(row) {
                charge_unit()?;
                insert_digest(&mut retained, digest)?;
            }
        }
        for row in rows_at_keys(previous, table, keys).flatten() {
            visited = visited.saturating_add(1);
            for digest in overflow_digests(row) {
                charge_unit()?;
                if !retained.contains(&digest) {
                    insert_digest(&mut released, digest)?;
                }
            }
        }
    }
    // Digests released by one changed row may be retained by a later one.
    released.retain(|digest| !retained.contains(digest));
    let mut unreachable = BTreeSet::new();
    // Segments are only added during a transaction: when the counts match,
    // every new segment is referenced by a changed row.
    let retained_new = retained
        .iter()
        .filter(|digest| {
            !previous.overflow_segments.contains_key(*digest)
                && next.overflow_segments.contains_key(*digest)
        })
        .count();
    let added = next
        .overflow_segments
        .len()
        .checked_sub(previous.overflow_segments.len())
        .ok_or_else(|| {
            RelationalError::Corruption(
                "relational overflow segments shrank before transaction pruning".to_string(),
            )
        })?;
    if retained_new != added {
        // An externalized value whose row was not stored (for example an
        // upsert that did nothing). Only segments, not rows, are visited.
        for digest in next.overflow_segments.keys() {
            charge_unit()?;
            if !previous.overflow_segments.contains_key(digest) && !retained.contains(digest) {
                insert_digest(&mut unreachable, *digest)?;
            }
        }
    }
    if !released.is_empty() {
        // A released digest may still be shared by an unchanged row.
        'scan: for segment in next.segments.values() {
            for row in segment.rows.values() {
                visited = visited.saturating_add(1);
                for digest in overflow_digests(row) {
                    charge_unit()?;
                    released.remove(&digest);
                }
                if released.is_empty() {
                    break 'scan;
                }
            }
        }
        unreachable.append(&mut released);
    }
    note_reachability_rows_visited(visited);
    for digest in &unreachable {
        charge_unit()?;
        next.overflow_segments.remove(digest);
    }
    if let Some(scratch) = &scratch {
        scratch.checkpoint().map_err(admission)?;
    }
    Ok(())
}

pub(super) fn prune_unreachable_segments(state: &mut RelationalState) {
    note_reachability_rows_visited(
        state
            .segments
            .values()
            .map(|segment| segment.rows.len())
            .sum(),
    );
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

pub(super) fn prune_unreachable_segments_with_allocations(
    state: &mut RelationalState,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<(), RelationalError> {
    let Some(context) = allocations else {
        prune_unreachable_segments(state);
        return Ok(());
    };
    let scratch = crate::background::CheckpointDecodeContext {
        work: context.work.clone(),
        memory: Default::default(),
    };
    let mut reachable = BTreeSet::new();
    note_reachability_rows_visited(
        state
            .segments
            .values()
            .map(|segment| segment.rows.len())
            .sum(),
    );
    for segment in state.segments.values() {
        for row in segment.rows.values() {
            for value in row.values.iter() {
                if let RelationalValue::Overflow(reference) = value
                    && !reachable.contains(&reference.digest)
                {
                    scratch
                        .reserve(crate::cow::checkpoint_tree_node_bytes::<Sha256Digest, ()>())
                        .map_err(|error| RelationalError::Admission(error.to_string()))?;
                }
                let unit = scratch
                    .start_unit()
                    .map_err(|error| RelationalError::Admission(error.to_string()))?;
                if let RelationalValue::Overflow(reference) = value {
                    reachable.insert(reference.digest);
                }
                unit.finish();
            }
        }
    }
    let mut refusal = None;
    state.overflow_segments.retain(|digest, _| {
        if refusal.is_some() {
            return true;
        }
        match scratch.start_unit() {
            Ok(unit) => {
                let keep = reachable.contains(digest);
                unit.finish();
                keep
            }
            Err(error) => {
                refusal = Some(error);
                true
            }
        }
    });
    if let Some(error) = refusal {
        return Err(RelationalError::Admission(error.to_string()));
    }
    scratch
        .checkpoint()
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    Ok(())
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
