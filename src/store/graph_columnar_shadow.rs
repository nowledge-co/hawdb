//! Derived columnar shadow double-write for [`GraphStore`] checkpoints.
//!
//! With `graph_columnar_shadow_checkpoint` on, every checkpoint additionally
//! publishes column groups, per-table directories, and the layered
//! [`ColumnGroupManifest`] under the `column-groups/` subdirectory of the
//! database root, through the column-group manifest layer's own durable
//! publication protocol (§3.6). The shadow is derived, rebuildable state:
//! recovery validates it and discards a corrupt catalog exactly like a
//! corrupt projected-graph artifact (`docs/STORAGE.md` recovery step 10);
//! reads are never served from it.
//!
//! Table mapping (fixed for this phase):
//!
//! - A node's **primary table** is its minimum `LabelId`. Real labels map to
//!   `table_id = label_id + 1`; nodes without labels go to the reserved
//!   `table_id = 0`. The full label set rides in the label-set blob column.
//! - A relationship's table is its `RelTypeId`: `table_id = rel_type_id`
//!   under [`ColumnGroupTableKind::Relationship`]. Relational tables are out
//!   of scope for the shadow.
//!
//! Column identity: the shadow reserves low column ids for its structural
//! columns and interns property keys in its **own persistent key
//! dictionary** (`column-groups/property-keys.skein`) because checkpoints
//! hold only `&Catalog` and most graph labels have no catalog
//! `PropertyDescriptor` interning. Dictionary ids are assigned once in
//! first-seen order and never reused or reordered (§3.5.3(c)); the file is
//! rewritten append-only-in-content, so every earlier generation keeps
//! decoding under the newer dictionary.
//!
//! - `column id 0`: label-set blob column — LEB128 varint `LabelId`s of the
//!   full (sorted) label set, present on every node row.
//! - `column id 1` / `2`: relationship source / target — the `u64` node id
//!   stored bit-preserving as `i64` (`id as i64`; readers reverse it with
//!   `value as u64`).
//! - `column id 3`: residual blob column — §3.5.1 field-tagged varint rows
//!   (`skein-storage`'s `encode/decode_residual_row_properties`): each
//!   property is a length-delimited submessage of interned key id plus one
//!   wire-typed value field, with nested and null values carrying the
//!   canonical tagged-value codec; unknown field ids are skippable.
//! - `column id >= 4`: typed property columns, id = shadow dictionary id.
//!
//! Column typing is per-generation inference: a property key gets a typed
//! column in a table's snapshot iff every occurrence in that table has the
//! same scalar type (Int/Float/Bool/String). Mixed-type, List, Map, and
//! Null occurrences send the key to the residual column. Each generation's
//! directories are self-contained, so this inference is deterministic and
//! sound without cross-generation schema state.
//!
//! Write amplification in this phase is proportional to the **dirty table
//! count**, not to change volume: a dirty table is rebuilt whole, so a
//! one-row edit rewrites its entire table in the current implementation.
//! Untouched tables reuse their previous directory references without
//! rebuilding bytes (§3.6.5). Memory, by contrast, is bounded regardless
//! of table size: rows stream through per-table group buffers under one
//! global byte budget, flushing short groups when the budget fills, and a
//! post-publish sweep retains only the active catalog's reference closure
//! so disk stays bounded too. Dictionary, pass-1, layout, and dictionary
//! serialization allocations draw from a separate enforced metadata budget
//! that is included in the same up-front admission.
//!
//! Formal-model coverage note: `SkeinColumnarShadowIntegration.tla` models
//! the four-phase publication machine, recovery, and the post-publish
//! reclamation sweep (`ActiveClosureRetained`: a sweep never removes a
//! file the active shadow manifest references; the sweep here is exactly
//! that bounded best-effort action, with no reader pins to respect).
//! Resource admission stays out of the model deliberately: nested
//! admission is absent structurally — the builder receives a pre-admitted
//! [`ColumnarShadowAdmission`] by value and has no governor handle — and
//! the constrained-governor convergence test proves it; admission
//! semantics are modeled separately by `SkeinRuntimeAdmission.tla`
//! (landing via another PR).

use super::*;
#[cfg(test)]
use skein_storage::column_group::shadow_metadata::DEFAULT_SHADOW_METADATA_BUDGET_BYTES;
#[cfg(test)]
use skein_storage::column_group::shadow_metadata::FIRST_DICTIONARY_COLUMN;
use skein_storage::column_group::shadow_metadata::{
    shadow_table_layout, ShadowKeyDictionary, ShadowMetadataBudget, ShadowTableLayout,
    TablePropertyTypes, SHADOW_KEY_DICTIONARY_FILE, SHADOW_PASS1_TABLE_OVERHEAD_BYTES,
};
use skein_storage::{
    encode_residual_row_properties, residual_row_properties_encoded_len,
    write_residual_row_properties, ColumnGroupArtifactDescriptor, ColumnGroupError,
    ColumnGroupManifest, ColumnGroupTableDirectory, ColumnGroupTableDirectoryRef,
    ColumnGroupTableKey, ColumnGroupTableKind, ColumnGroupWriter, ColumnarShadowCheckpointReport,
    ColumnarShadowCheckpointStatus, ColumnarShadowRecoveryStatus, PublishedColumnGroupCatalog,
    COLUMN_GROUP_SHADOW_DIR, DEFAULT_GROUP_ROW_CAPACITY,
};

/// Reserved column id of the node label-set blob column.
const LABEL_SET_COLUMN: PropertyId = PropertyId(0);
/// Reserved column id of the relationship source endpoint column.
const SOURCE_COLUMN: PropertyId = PropertyId(1);
/// Reserved column id of the relationship target endpoint column.
const TARGET_COLUMN: PropertyId = PropertyId(2);
/// Reserved column id of the residual blob column.
const RESIDUAL_COLUMN: PropertyId = PropertyId(3);
/// Fixed per-row overhead charged against the buffer budget.
const SHADOW_ROW_OVERHEAD_BYTES: u64 = 16;
/// Encoder scratch allowance multiplier inside the admission reservation:
/// while a chunk encodes, the buffered input, the encoded body, and the
/// compressed body coexist; the largest chunk is bounded by the buffer
/// budget, so twice the budget bounds the scratch.
const SHADOW_ENCODER_SCRATCH_MULTIPLIER: u64 = 2;
/// Transient allowance one streamed single-row flush draws from the token.
/// The streaming path's memory is O(io block + framing scratch), never
/// O(value): arbitrarily large legal rows publish inside this fixed
/// allowance, which is what makes the shadow converge on any input.
const SHADOW_STREAMED_FLUSH_ALLOWANCE_BYTES: u64 = 64 * 1024;

pub(super) use skein_storage::ColumnarShadowState;
#[cfg(test)]
use skein_storage::DEFAULT_SHADOW_BUFFER_BUDGET_BYTES;

/// The shadow table key of a node with `labels` (minimum label = primary).
fn node_table_key(labels: &BTreeSet<LabelId>) -> ColumnGroupTableKey {
    let table_id = labels
        .first()
        .map_or(0, |label| u64::from(label.0).saturating_add(1));
    ColumnGroupTableKey::new(ColumnGroupTableKind::Node, table_id)
}

fn relationship_table_key(rel_type: RelTypeId) -> ColumnGroupTableKey {
    ColumnGroupTableKey::new(ColumnGroupTableKind::Relationship, u64::from(rel_type.0))
}

fn shadow_error(error: ColumnGroupError) -> SkeinError {
    SkeinError::Storage(format!("columnar shadow: {error}"))
}

fn encode_varint_u32(mut value: u32, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

#[cfg(test)]
fn decode_varint_u32(bytes: &[u8], position: &mut usize) -> Result<u32> {
    let mut value = 0u32;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(*position).ok_or_else(|| {
            SkeinError::Storage("columnar shadow: label set varint is truncated".to_string())
        })?;
        *position += 1;
        let bits = u32::from(byte & 0x7f);
        if shift >= 32 || (shift == 28 && bits > 0x0f) {
            return Err(SkeinError::Storage(
                "columnar shadow: label set varint overflows u32".to_string(),
            ));
        }
        value |= bits << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

fn encode_label_set(labels: &BTreeSet<LabelId>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(labels.len());
    for label in labels {
        encode_varint_u32(label.0, &mut bytes);
    }
    bytes
}

#[cfg(test)]
fn decode_label_set(bytes: &[u8]) -> Result<BTreeSet<LabelId>> {
    let mut labels = BTreeSet::new();
    let mut position = 0usize;
    while position < bytes.len() {
        labels.insert(LabelId(decode_varint_u32(bytes, &mut position)?));
    }
    Ok(labels)
}

/// Rough resident-byte estimate of one buffered value, mirroring the
/// existing record estimators' spirit: enough to keep the budget honest,
/// never exact.
fn estimated_shadow_value_bytes(value: &Value) -> u64 {
    match value {
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) => 16,
        Value::String(value) => 16 + value.len() as u64,
        Value::Binary(value) => 16 + value.len() as u64,
        Value::Uuid(_) => 16,
        Value::List(values) => 16 + values.iter().map(estimated_shadow_value_bytes).sum::<u64>(),
        Value::Map(entries) => {
            16 + entries
                .iter()
                .map(|(key, value)| key.len() as u64 + estimated_shadow_value_bytes(value))
                .sum::<u64>()
        }
    }
}

/// Pre-admitted resource context for one whole shadow build.
///
/// Structural non-reentrancy: the builder receives this token **by value**
/// and holds no governor handle at all, so a nested admission against a
/// permit the caller already holds is impossible by construction. The
/// token is one of:
///
/// - **pre-admitted** — the caller extended its own single admission's
///   memory request by [`GraphStore::columnar_shadow_admission_bytes`]
///   (the nowledge_mem typed checkpoint, which holds a background
///   maintenance permit for the duration);
/// - **owned** — the checkpoint entry acquired exactly one non-nested
///   `try_admit` for the whole build, with no waiting loop (plain
///   `Database::checkpoint` with a threaded governor);
/// - **unmetered** — no governor is threaded (plain `GraphStore` opens).
///
/// Flushes never talk to a governor; they only draw against the token's
/// byte allowance.
#[derive(Debug)]
pub struct ColumnarShadowAdmission {
    _permit: Option<Box<dyn skein_storage::BackgroundWorkPermit>>,
    /// `None` = unmetered; `Some` = the admitted builder-lifetime bytes.
    allowance_bytes: Option<u64>,
}

impl ColumnarShadowAdmission {
    /// The caller already admitted `allowance_bytes` for the shadow build
    /// inside its own governor permit. Crate-private on purpose: the
    /// token's meaning is that admission actually happened, so only code
    /// paths that perform it may issue one — external callers get facades
    /// that admit for themselves.
    pub(crate) fn pre_admitted(allowance_bytes: u64) -> Self {
        Self {
            _permit: None,
            allowance_bytes: Some(allowance_bytes),
        }
    }

    fn owned(permit: Box<dyn skein_storage::BackgroundWorkPermit>, allowance_bytes: u64) -> Self {
        Self {
            _permit: Some(permit),
            allowance_bytes: Some(allowance_bytes),
        }
    }

    fn unmetered() -> Self {
        Self {
            _permit: None,
            allowance_bytes: None,
        }
    }

    /// The admitted builder-lifetime byte allowance (0 when unmetered).
    fn admitted_budget_bytes(&self) -> u64 {
        self.allowance_bytes.unwrap_or(0)
    }

    /// Draws one streamed single-row flush: its transient memory is the
    /// fixed streaming allowance plus live metadata, independent of the
    /// value's size.
    fn draw_for_streamed_flush(&self, metadata_bytes: u64) -> Result<()> {
        let Some(allowance) = self.allowance_bytes else {
            return Ok(());
        };
        let transient = SHADOW_STREAMED_FLUSH_ALLOWANCE_BYTES.saturating_add(metadata_bytes);
        if transient > allowance {
            return Err(SkeinError::Storage(format!(
                "columnar shadow streamed flush needs {transient} bytes, \
                 exceeding its admitted {allowance} byte allowance"
            )));
        }
        Ok(())
    }

    /// Draws one flush against the allowance: the flush's transient need is
    /// its buffered bytes plus the documented encoder-scratch multiple. No
    /// governor is consulted — the whole build was admitted up front.
    fn draw_for_flush(&self, flush_bytes: u64, metadata_bytes: u64) -> Result<()> {
        let Some(allowance) = self.allowance_bytes else {
            return Ok(());
        };
        let transient = flush_bytes
            .saturating_mul(1 + SHADOW_ENCODER_SCRATCH_MULTIPLIER)
            .saturating_add(metadata_bytes);
        if transient > allowance {
            return Err(SkeinError::Storage(format!(
                "columnar shadow flush and metadata need {transient} bytes, exceeding their \
                 admitted {allowance} byte allowance"
            )));
        }
        Ok(())
    }
}

/// Streams a residual row from borrowed entries: exact length up front,
/// value bytes written through the canonical streaming writer — transient
/// memory O(recursion frame), never O(value).
struct ResidualRowBlob<'a> {
    entries: &'a [(u32, &'a Value)],
    encoded_len: u64,
}

impl<'a> ResidualRowBlob<'a> {
    fn new(entries: &'a [(u32, &'a Value)]) -> Result<Self> {
        let encoded_len = residual_row_properties_encoded_len(entries)
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        Ok(Self {
            entries,
            encoded_len,
        })
    }
}

impl skein_storage::StreamedBlob for ResidualRowBlob<'_> {
    fn blob_len(&self) -> u64 {
        self.encoded_len
    }

    fn write_blob(&self, out: &mut dyn std::io::Write) -> std::io::Result<()> {
        write_residual_row_properties(out, self.entries)
            .map_err(|error| std::io::Error::other(error.to_string()))
    }
}

/// One in-flight bounded group buffer (max one group's rows).
#[derive(Debug, Default)]
struct ShadowGroupBuffer {
    ids: Vec<u64>,
    /// Parallel to the table layout's typed columns.
    typed: Vec<Vec<Value>>,
    /// Node tables only.
    label_sets: Vec<Option<Vec<u8>>>,
    /// Relationship tables only: `(source, target)` as u64-as-i64 values.
    sources: Vec<Value>,
    targets: Vec<Value>,
    residuals: Vec<Option<Vec<u8>>>,
    estimated_bytes: u64,
}

struct BuiltShadowTables {
    references: Vec<(ColumnGroupTableDirectoryRef, u64)>,
    group_bytes_written: u64,
    peak_builder_bytes: u64,
    metadata_bytes_used: u64,
    peak_metadata_bytes: u64,
    flushed_group_count: usize,
    oversized_row_group_count: usize,
}

/// Streaming pass-2 builder: appends rows into per-table bounded group
/// buffers under one global byte budget. A full buffer flushes as a
/// complete group; exceeding the budget flushes the largest buffer as a
/// shorter group. The builder holds the pre-admitted token by value and no
/// governor handle: flushes only draw down the token's byte allowance.
///
/// The flush decision is taken from a size estimate BEFORE the row's cells
/// are moved or its residual is encoded, and the builder-footprint peak
/// folds in the pass-1 type-lattice state and the live key dictionary, so
/// `peak_builder_bytes` is an honest builder metric, not a logical count.
struct ShadowCheckpointBuilder {
    shadow_root: PathBuf,
    generation: ManifestGeneration,
    admission: ColumnarShadowAdmission,
    buffer_budget_bytes: u64,
    writer: ColumnGroupWriter,
    layouts: BTreeMap<ColumnGroupTableKey, ShadowTableLayout>,
    buffers: BTreeMap<ColumnGroupTableKey, ShadowGroupBuffer>,
    descriptors: BTreeMap<ColumnGroupTableKey, Vec<ColumnGroupArtifactDescriptor>>,
    next_group_index: BTreeMap<ColumnGroupTableKey, u64>,
    buffered_bytes: u64,
    /// Shared, enforced metadata budget carried from pass 1 through layout
    /// construction and pass 2.
    metadata_budget: ShadowMetadataBudget,
    peak_builder_bytes: u64,
    flushed_group_count: usize,
    oversized_row_group_count: usize,
    group_bytes_written: u64,
}

impl ShadowCheckpointBuilder {
    fn new(
        shadow_root: PathBuf,
        generation: ManifestGeneration,
        admission: ColumnarShadowAdmission,
        buffer_budget_bytes: u64,
        layouts: BTreeMap<ColumnGroupTableKey, ShadowTableLayout>,
        metadata_budget: ShadowMetadataBudget,
    ) -> Self {
        let mut builder = Self {
            shadow_root,
            generation,
            admission,
            buffer_budget_bytes,
            writer: ColumnGroupWriter::default(),
            layouts,
            buffers: BTreeMap::new(),
            descriptors: BTreeMap::new(),
            next_group_index: BTreeMap::new(),
            buffered_bytes: 0,
            metadata_budget,
            peak_builder_bytes: 0,
            flushed_group_count: 0,
            oversized_row_group_count: 0,
            group_bytes_written: 0,
        };
        builder.note_peak();
        builder
    }

    fn note_peak(&mut self) {
        let footprint = self
            .metadata_budget
            .used_bytes()
            .saturating_add(self.buffered_bytes);
        self.peak_builder_bytes = self.peak_builder_bytes.max(footprint);
    }

    fn append_node(&mut self, dictionary: &ShadowKeyDictionary, node: NodeRecord) -> Result<()> {
        let NodeRecord {
            id,
            labels,
            properties,
        } = node;
        let key = node_table_key(&labels);
        let label_set = encode_label_set(&labels);
        self.append_row(dictionary, key, id.0, Some(label_set), None, properties)
    }

    fn append_relationship(
        &mut self,
        dictionary: &ShadowKeyDictionary,
        relationship: RelRecord,
    ) -> Result<()> {
        let RelRecord {
            id,
            source,
            target,
            rel_type,
            properties,
        } = relationship;
        let key = relationship_table_key(rel_type);
        self.append_row(
            dictionary,
            key,
            id.0,
            None,
            Some((source.0, target.0)),
            properties,
        )
    }

    /// Estimated buffered cost of one row from borrowed values only — no
    /// clone, no residual encoding. The same figure later charges the
    /// buffer, so accounting is consistent on both sides of the flush
    /// decision.
    fn estimate_row_bytes(
        &self,
        dictionary: &ShadowKeyDictionary,
        table: ColumnGroupTableKey,
        label_set: Option<&Vec<u8>>,
        properties: &BTreeMap<String, Value>,
    ) -> u64 {
        let mut estimated =
            SHADOW_ROW_OVERHEAD_BYTES.saturating_add(label_set.map_or(0, |blob| blob.len() as u64));
        for (key, value) in properties {
            estimated = estimated.saturating_add(estimated_shadow_value_bytes(value));
            let typed = dictionary
                .id(key)
                .is_some_and(|id| self.layouts[&table].typed_index().contains_key(&id));
            if !typed {
                // Residual wire envelope: tags, lengths, and the key id.
                estimated = estimated.saturating_add(8);
            }
        }
        estimated
    }

    fn append_row(
        &mut self,
        dictionary: &ShadowKeyDictionary,
        table: ColumnGroupTableKey,
        id: u64,
        label_set: Option<Vec<u8>>,
        endpoints: Option<(u64, u64)>,
        properties: BTreeMap<String, Value>,
    ) -> Result<()> {
        // The flush decision comes from an estimate over borrowed values,
        // BEFORE the row's cells are moved or its residual encoded, so the
        // budget bounds materialization too. A row whose estimate alone
        // exceeds the budget flushes everything and is written as its own
        // single-row group immediately — a bounded transient, never
        // buffered behind other rows.
        let row_bytes = self.estimate_row_bytes(dictionary, table, label_set.as_ref(), &properties);
        if row_bytes > self.buffer_budget_bytes {
            // The row cannot fit any buffer: flush everything, then stream
            // it straight from the borrowed record to its own single-row
            // group. Nothing row-sized is ever moved, encoded, or
            // buffered, so arbitrarily large legal rows publish inside the
            // fixed streaming allowance — the convergence guarantee.
            let pending = self.buffers.keys().copied().collect::<Vec<_>>();
            for pending_table in pending {
                self.flush_table(pending_table)?;
            }
            return self.flush_streamed_single_row(
                dictionary,
                table,
                id,
                label_set,
                endpoints,
                &properties,
            );
        }
        while self.buffered_bytes > 0
            && self.buffered_bytes.saturating_add(row_bytes) > self.buffer_budget_bytes
        {
            self.flush_largest_buffer()?;
        }

        // Materialize only after the flush decision: one pass over the
        // row's properties routes each through the precomputed typed-column
        // index (O(P log C)); everything unindexed is residual.
        let layout_typed_len = self.layouts[&table].typed().len();
        let mut typed_cells = vec![Value::Null; layout_typed_len];
        let mut residual_values = Vec::new();
        for (key, value) in properties {
            let property_id = dictionary.id(&key).ok_or_else(|| {
                SkeinError::Storage(format!(
                    "columnar shadow pass 2 encountered property key {key:?} absent from pass 1"
                ))
            })?;
            match self.layouts[&table].typed_index().get(&property_id) {
                Some(index) => {
                    // Pass 2 owns the canonical scan record, so move large
                    // typed values into the group buffer instead of cloning
                    // their payload allocation.
                    typed_cells[*index] = value;
                }
                None => {
                    residual_values.push((property_id.0, value));
                }
            }
        }
        let residual = if residual_values.is_empty() {
            None
        } else {
            residual_values.sort_by_key(|(key_id, _)| *key_id);
            let residual_entries = residual_values
                .iter()
                .map(|(key_id, value)| (*key_id, value))
                .collect::<Vec<_>>();
            Some(
                encode_residual_row_properties(&residual_entries)
                    .map_err(|error| SkeinError::Storage(error.to_string()))?,
            )
        };

        let buffer = self.buffers.entry(table).or_default();
        if buffer.typed.is_empty() {
            buffer.typed = vec![Vec::new(); layout_typed_len];
        }
        buffer.ids.push(id);
        for (column, cell) in buffer.typed.iter_mut().zip(typed_cells) {
            column.push(cell);
        }
        match table.kind {
            ColumnGroupTableKind::Node => {
                buffer.label_sets.push(label_set);
            }
            ColumnGroupTableKind::Relationship => {
                let (source, target) = endpoints.expect("relationship row has endpoints");
                // u64 endpoints ride plain integer columns bit-preserving:
                // `id as i64` on write, `value as u64` on read.
                buffer.sources.push(Value::Int(source as i64));
                buffer.targets.push(Value::Int(target as i64));
            }
            ColumnGroupTableKind::Relational => {
                return Err(SkeinError::Storage(
                    "columnar shadow does not cover relational tables".to_string(),
                ));
            }
        }
        buffer.residuals.push(residual);
        buffer.estimated_bytes = buffer.estimated_bytes.saturating_add(row_bytes);
        let full = buffer.ids.len() >= DEFAULT_GROUP_ROW_CAPACITY as usize;
        self.buffered_bytes = self.buffered_bytes.saturating_add(row_bytes);
        self.note_peak();
        if full {
            self.flush_table(table)?;
        }
        Ok(())
    }

    /// Streams one oversized row as its own single-row group. Typed cells
    /// and residual values are borrowed from the scanned record; the
    /// residual envelope and every string stream straight to the file, so
    /// the transient memory is the fixed streaming allowance, independent
    /// of the row's size.
    fn flush_streamed_single_row(
        &mut self,
        dictionary: &ShadowKeyDictionary,
        table: ColumnGroupTableKey,
        id: u64,
        label_set: Option<Vec<u8>>,
        endpoints: Option<(u64, u64)>,
        properties: &BTreeMap<String, Value>,
    ) -> Result<()> {
        self.admission
            .draw_for_streamed_flush(self.metadata_budget.used_bytes())?;
        let mut typed_cells: Vec<(PropertyId, &Value)> = Vec::new();
        let mut residual_entries: Vec<(u32, &Value)> = Vec::new();
        for (key, value) in properties {
            let property_id = dictionary.id(key).ok_or_else(|| {
                SkeinError::Storage(format!(
                    "columnar shadow pass 2 encountered property key {key:?} absent from pass 1"
                ))
            })?;
            if self.layouts[&table]
                .typed_index()
                .contains_key(&property_id)
            {
                typed_cells.push((property_id, value));
            } else {
                residual_entries.push((property_id.0, value));
            }
        }
        residual_entries.sort_by_key(|(key_id, _)| *key_id);
        let endpoint_values = endpoints
            .map(|(source, target)| (Value::Int(source as i64), Value::Int(target as i64)));
        let mut value_columns = typed_cells;
        if let Some((source, target)) = &endpoint_values {
            value_columns.push((SOURCE_COLUMN, source));
            value_columns.push((TARGET_COLUMN, target));
        }
        let residual_blob = if residual_entries.is_empty() {
            None
        } else {
            Some(ResidualRowBlob::new(&residual_entries)?)
        };
        let label_slice: Option<&[u8]> = label_set.as_deref();
        let mut byte_columns: Vec<(PropertyId, &dyn skein_storage::StreamedBlob)> = Vec::new();
        if let Some(label) = &label_slice {
            byte_columns.push((LABEL_SET_COLUMN, label));
        }
        if let Some(blob) = &residual_blob {
            byte_columns.push((RESIDUAL_COLUMN, blob));
        }
        let kind_tag = match table.kind {
            ColumnGroupTableKind::Node => "node",
            ColumnGroupTableKind::Relationship => "relationship",
            ColumnGroupTableKind::Relational => {
                return Err(SkeinError::Storage(
                    "columnar shadow does not cover relational tables".to_string(),
                ));
            }
        };
        let group_index = self.next_group_index.entry(table).or_insert(0);
        let file_name = format!(
            "group-{kind_tag}-{}-{}-{}.skein",
            table.table_id, self.generation.0, group_index
        );
        let path = self.shadow_root.join(&file_name);
        self.writer
            .write_single_row_streaming(
                &path,
                *group_index,
                self.generation,
                id,
                &value_columns,
                &byte_columns,
            )
            .map_err(shadow_error)?;
        *group_index += 1;
        self.group_bytes_written = self
            .group_bytes_written
            .saturating_add(fs::metadata(&path)?.len());
        self.flushed_group_count += 1;
        self.oversized_row_group_count += 1;
        // The streamed transient is part of the honest peak.
        let footprint = self
            .metadata_budget
            .used_bytes()
            .saturating_add(self.buffered_bytes)
            .saturating_add(SHADOW_STREAMED_FLUSH_ALLOWANCE_BYTES);
        self.peak_builder_bytes = self.peak_builder_bytes.max(footprint);
        self.descriptors.entry(table).or_default().push(
            ColumnGroupArtifactDescriptor::inspect(&self.shadow_root, file_name, None)
                .map_err(shadow_error)?,
        );
        Ok(())
    }

    fn flush_largest_buffer(&mut self) -> Result<()> {
        let largest = self
            .buffers
            .iter()
            .filter(|(_, buffer)| !buffer.ids.is_empty())
            .max_by_key(|(_, buffer)| buffer.estimated_bytes)
            .map(|(table, _)| *table);
        match largest {
            Some(table) => self.flush_table(table),
            None => Ok(()),
        }
    }

    /// Flushes one table's buffer as an immutable group (possibly shorter
    /// than the group row capacity), drawing against the pre-admitted
    /// token's byte allowance — never against a governor.
    fn flush_table(&mut self, table: ColumnGroupTableKey) -> Result<()> {
        let Some(buffer) = self.buffers.remove(&table) else {
            return Ok(());
        };
        if buffer.ids.is_empty() {
            return Ok(());
        }
        self.admission
            .draw_for_flush(buffer.estimated_bytes, self.metadata_budget.used_bytes())?;
        let kind_tag = match table.kind {
            ColumnGroupTableKind::Node => "node",
            ColumnGroupTableKind::Relationship => "relationship",
            ColumnGroupTableKind::Relational => {
                return Err(SkeinError::Storage(
                    "columnar shadow does not cover relational tables".to_string(),
                ));
            }
        };
        let group_index = self.next_group_index.entry(table).or_insert(0);
        let file_name = format!(
            "group-{kind_tag}-{}-{}-{}.skein",
            table.table_id, self.generation.0, group_index
        );
        let mut value_columns: Vec<(PropertyId, Vec<Value>)> = self.layouts[&table]
            .typed()
            .iter()
            .copied()
            .zip(buffer.typed)
            .collect();
        let mut byte_columns: Vec<(PropertyId, Vec<Option<Vec<u8>>>)> = Vec::new();
        match table.kind {
            ColumnGroupTableKind::Node => {
                byte_columns.push((LABEL_SET_COLUMN, buffer.label_sets));
            }
            ColumnGroupTableKind::Relationship => {
                value_columns.push((SOURCE_COLUMN, buffer.sources));
                value_columns.push((TARGET_COLUMN, buffer.targets));
            }
            ColumnGroupTableKind::Relational => unreachable!("rejected above"),
        }
        byte_columns.push((RESIDUAL_COLUMN, buffer.residuals));
        let path = self.shadow_root.join(&file_name);
        self.writer
            .write_with_byte_columns(
                &path,
                *group_index,
                self.generation,
                &buffer.ids,
                &value_columns,
                &byte_columns,
            )
            .map_err(shadow_error)?;
        *group_index += 1;
        self.group_bytes_written = self
            .group_bytes_written
            .saturating_add(fs::metadata(&path)?.len());
        self.flushed_group_count += 1;
        self.buffered_bytes = self.buffered_bytes.saturating_sub(buffer.estimated_bytes);
        self.descriptors.entry(table).or_default().push(
            ColumnGroupArtifactDescriptor::inspect(&self.shadow_root, file_name, None)
                .map_err(shadow_error)?,
        );
        Ok(())
    }

    /// Flushes every remaining buffer and publishes one immutable directory
    /// per rebuilt table, returning the new references with their byte
    /// sizes.
    fn finish(mut self) -> Result<BuiltShadowTables> {
        let pending = self.buffers.keys().copied().collect::<Vec<_>>();
        for table in pending {
            self.flush_table(table)?;
        }
        let mut references = Vec::with_capacity(self.descriptors.len());
        for (table, descriptors) in std::mem::take(&mut self.descriptors) {
            let directory = ColumnGroupTableDirectory::new(table, self.generation, descriptors)
                .map_err(shadow_error)?;
            // A crashed earlier publication attempt of this same
            // (unpublished) generation may have left an immutable directory
            // file with different bytes; the active manifest never
            // references the candidate generation, so the orphan is garbage
            // and safe to drop before rewriting.
            let _ = fs::remove_file(self.shadow_root.join(directory.file_name()));
            let reference = directory
                .write_immutable(&self.shadow_root)
                .map_err(shadow_error)?;
            let directory_bytes = fs::metadata(self.shadow_root.join(reference.file_name()))?.len();
            references.push((reference, directory_bytes));
        }
        Ok(BuiltShadowTables {
            references,
            group_bytes_written: self.group_bytes_written,
            peak_builder_bytes: self.peak_builder_bytes,
            metadata_bytes_used: self.metadata_budget.used_bytes(),
            peak_metadata_bytes: self.metadata_budget.peak_bytes(),
            flushed_group_count: self.flushed_group_count,
            oversized_row_group_count: self.oversized_row_group_count,
        })
    }
}

/// Best-effort reclamation of superseded shadow artifacts, run strictly
/// AFTER a successful manifest publish. The keep set is the just-published
/// catalog's complete reference closure — the active manifest file, every
/// referenced table directory, group, and deletion-vector file — plus the
/// key dictionary. The shadow has no readers and no pins, so retaining
/// only the current closure is safe; the manifest layer stays generic and
/// this sweep touches only `*.skein` / `*.skein.tmp` names. Removal
/// failures are recorded and retried by the next publish (Windows
/// discipline: a transient sharing violation never fails a publication).
/// A crash between publish and sweep leaves only unreferenced garbage,
/// which the next sweep removes.
fn sweep_superseded_shadow_files(
    shadow_root: &Path,
    catalog: &PublishedColumnGroupCatalog,
) -> (usize, usize) {
    let mut keep = BTreeSet::new();
    keep.insert(skein_storage::COLUMN_GROUP_MANIFEST_FILE.to_string());
    keep.insert(SHADOW_KEY_DICTIONARY_FILE.to_string());
    for reference in catalog.manifest().tables() {
        keep.insert(reference.file_name().to_string());
    }
    for directory in catalog.directories() {
        for group in directory.groups() {
            keep.insert(group.group_file().to_string());
            if let Some(deletion_vector) = group.deletion_vector_file() {
                keep.insert(deletion_vector.to_string());
            }
        }
    }
    let mut reclaimed = 0usize;
    let mut failed = 0usize;
    let Ok(entries) = fs::read_dir(shadow_root) else {
        return (0, 0);
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if !(name.ends_with(".skein") || name.ends_with(".skein.tmp")) {
            continue;
        }
        if keep.contains(name) {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => reclaimed += 1,
            Err(_) => failed += 1,
        }
    }
    (reclaimed, failed)
}

// --- GraphStore hooks -------------------------------------------------------

impl GraphStore {
    /// Marks the shadow table of a node dirty. `labels` is the node's full
    /// label set; only its primary table stores the node, so only that table
    /// is marked. No-op when the shadow is disabled.
    pub(super) fn mark_columnar_node_dirty(&mut self, labels: &BTreeSet<LabelId>) {
        if !self.columnar_shadow.enabled {
            return;
        }
        let key = node_table_key(labels);
        self.columnar_shadow.dirty.insert(key);
    }

    /// Marks the shadow table of a relationship type dirty. No-op when the
    /// shadow is disabled.
    pub(super) fn mark_columnar_relationship_dirty(&mut self, rel_type: RelTypeId) {
        if !self.columnar_shadow.enabled {
            return;
        }
        let key = relationship_table_key(rel_type);
        self.columnar_shadow.dirty.insert(key);
    }

    /// The shadow report of the most recent checkpoint, `None` when the flag
    /// is off or no shadow checkpoint has been published yet.
    pub fn columnar_shadow_checkpoint_report(&self) -> Option<ColumnarShadowCheckpointReport> {
        self.columnar_shadow.report.clone()
    }

    /// What recovery observed about the shadow catalog.
    pub fn columnar_shadow_recovery_status(&self) -> ColumnarShadowRecoveryStatus {
        self.columnar_shadow.recovery.clone()
    }

    /// Mounts the shadow catalog during recovery (between checkpoint load
    /// and WAL replay, so replayed mutations mark dirty tables): opens and
    /// validates the published catalog per §3.6.6, and discards a corrupt
    /// shadow instead of failing the open — it is rebuildable derived state,
    /// the same policy `docs/STORAGE.md` recovery step 10 applies to
    /// projected-graph artifacts under the derived-projection contract.
    pub(super) fn mount_columnar_shadow_for_recovery(&mut self) -> Result<()> {
        self.columnar_shadow.enabled = true;
        self.columnar_shadow.all_dirty = true;
        let Some(durable) = &self.durable else {
            return Ok(());
        };
        let shadow_root = durable.root_path.join(COLUMN_GROUP_SHADOW_DIR);
        let read_only = durable.read_only;
        let checkpoint_commit_epoch = durable.checkpoint_commit_epoch;
        if !shadow_root.exists() {
            return Ok(());
        }
        let opened = ColumnGroupManifest::open(&shadow_root)
            .map_err(|error| error.to_string())
            .and_then(|catalog| match catalog {
                None => Ok(None),
                Some(catalog) => ShadowKeyDictionary::load(
                    &shadow_root,
                    self.columnar_shadow.metadata_budget_bytes,
                )
                .map_err(|error| error.to_string())
                .map(|_| Some(catalog)),
            });
        match opened {
            Ok(None) => {}
            Ok(Some(catalog)) => {
                // A shadow behind (or ahead of) the recovered checkpoint is
                // stale but intact: keep it as the reuse parent and rebuild
                // everything on the next checkpoint.
                self.columnar_shadow.all_dirty =
                    catalog.manifest().source_commit_epoch() != checkpoint_commit_epoch;
                self.columnar_shadow.catalog = Some(catalog);
                self.columnar_shadow.recovery.validated = true;
            }
            Err(error) => {
                if !read_only {
                    // Cleanup failure must not block open: the shadow is
                    // derived state, so a corrupt catalog is unmounted and
                    // recorded here, and leftover bytes are retried by the
                    // next checkpoint's stale-garbage sweep.
                    let _ = fs::remove_dir_all(&shadow_root);
                }
                self.columnar_shadow.recovery.discarded = true;
                self.columnar_shadow.recovery.error = Some(error);
            }
        }
        Ok(())
    }

    /// The builder-lifetime byte reservation one shadow build needs from
    /// the runtime governor: the buffer budget (buffered rows and the row
    /// being materialized), the documented encoder-scratch multiple of it,
    /// and the enforced metadata budget (pass-1 type-lattice state, typed
    /// layouts, key dictionary, and dictionary serialization). Callers that already
    /// hold a governor permit extend that single admission's memory request
    /// by this amount and pass [`ColumnarShadowAdmission::pre_admitted`]
    /// into the checkpoint. Zero when the shadow is disabled.
    pub fn columnar_shadow_admission_bytes(&self) -> u64 {
        if !self.columnar_shadow.enabled {
            return 0;
        }
        // The persisted dictionary is measured, not guessed: ×4 bounds its
        // decoded charge (key bytes twice plus fixed per-entry overheads),
        // so a build whose growth exhausted the reservation converges on
        // retry because this measurement covers the grown baseline.
        let dictionary_bytes = self
            .durable
            .as_ref()
            .and_then(|durable| {
                fs::metadata(
                    durable
                        .root_path
                        .join(COLUMN_GROUP_SHADOW_DIR)
                        .join(SHADOW_KEY_DICTIONARY_FILE),
                )
                .ok()
            })
            .map_or(0, |metadata| metadata.len());
        self.columnar_shadow
            .buffer_budget_bytes
            .saturating_mul(1 + SHADOW_ENCODER_SCRATCH_MULTIPLIER)
            .saturating_add(dictionary_bytes.saturating_mul(8))
            .saturating_add(self.columnar_shadow.metadata_budget_bytes.saturating_mul(2))
    }

    /// Acquires the whole build's resources in exactly one non-nested
    /// `try_admit` (no waiting loop): background `Control` work, the
    /// established `WorkClass::Shadow` mapping. Rejection fails the shadow
    /// build once — the checkpoint records `Failed` and the next checkpoint
    /// retries from the preserved dirty state.
    fn acquire_columnar_shadow_admission(&self) -> Result<ColumnarShadowAdmission> {
        let Some(governor) = &self.runtime_governor else {
            return Ok(ColumnarShadowAdmission::unmetered());
        };
        let allowance = self.columnar_shadow_admission_bytes();
        let request = skein_storage::BackgroundWorkRequest {
            cpu_slots: 1,
            memory_bytes: allowance,
            io_slots: 1,
        };
        match governor.try_admit(request) {
            Ok(permit) => Ok(ColumnarShadowAdmission::owned(permit, allowance)),
            Err(error) => Err(SkeinError::Storage(format!(
                "columnar shadow build admission denied: {error}"
            ))),
        }
    }

    /// Runs the shadow double-write for a just-published canonical
    /// checkpoint and records the outcome. The canonical checkpoint's
    /// `Result` reflects canonical publication only: a shadow failure here
    /// never fails the checkpoint call — it lands in the report as
    /// [`ColumnarShadowCheckpointStatus::Failed`] with the dirty state
    /// preserved (never cleared on failure), so the next checkpoint
    /// retries and converges.
    ///
    /// `admission` is the caller's explicit pre-admitted context (the
    /// nowledge_mem typed checkpoint, which extended its own held permit);
    /// `None` acquires exactly one non-nested `try_admit` here at the
    /// checkpoint entry, before the builder exists.
    pub(super) fn record_columnar_shadow_checkpoint(
        &mut self,
        source_commit_epoch: u64,
        admission: Option<ColumnarShadowAdmission>,
    ) {
        if !self.columnar_shadow.enabled {
            return;
        }
        let record_failure = |report: &mut Option<ColumnarShadowCheckpointReport>,
                              error: SkeinError| {
            *report = Some(ColumnarShadowCheckpointReport {
                status: ColumnarShadowCheckpointStatus::Failed {
                    error: error.to_string(),
                },
                source_commit_epoch,
                ..ColumnarShadowCheckpointReport::default()
            });
        };
        let admission = match admission {
            Some(admission) => admission,
            None => match self.acquire_columnar_shadow_admission() {
                Ok(admission) => admission,
                Err(error) => {
                    record_failure(&mut self.columnar_shadow.report, error);
                    return;
                }
            },
        };
        if let Err(error) = self.publish_columnar_shadow_checkpoint(source_commit_epoch, admission)
        {
            record_failure(&mut self.columnar_shadow.report, error);
        }
    }

    /// Builds and publishes the shadow for a just-published checkpoint.
    /// Untouched tables reuse their previous directory references without
    /// rebuilding bytes (§3.6.5); dirty tables are rebuilt whole. Durable
    /// order: immutable groups -> immutable table directories -> key
    /// dictionary -> manifest replace, all through temp-file/fsync/rename
    /// publication. State (dirty set, all-dirty flag, catalog) is mutated
    /// only after successful publication, so the failure path preserves
    /// everything the retry needs.
    fn publish_columnar_shadow_checkpoint(
        &mut self,
        source_commit_epoch: u64,
        admission: ColumnarShadowAdmission,
    ) -> Result<()> {
        if !self.columnar_shadow.enabled {
            return Ok(());
        }
        let started = std::time::Instant::now();
        let Some(durable) = self.durable.as_ref() else {
            return Ok(());
        };
        let shadow_root = durable.root_path.join(COLUMN_GROUP_SHADOW_DIR);
        let previous = self.columnar_shadow.catalog.clone();
        if previous.is_none() && shadow_root.exists() {
            // No mounted catalog means the artifacts on disk are stale
            // garbage (a failed attempt, or a shadow discarded at recovery
            // in a read-only process): restart the generation sequence
            // cleanly. The key dictionary survives — it is append-only
            // monotone state, and preserving it is what makes a
            // budget-exhausted attempt converge on retry.
            for entry in fs::read_dir(&shadow_root)? {
                let entry = entry?;
                if entry.file_name() == SHADOW_KEY_DICTIONARY_FILE {
                    continue;
                }
                let path = entry.path();
                if path.is_dir() {
                    let _ = fs::remove_dir_all(&path);
                } else {
                    let _ = fs::remove_file(&path);
                }
            }
        }
        fs::create_dir_all(&shadow_root)?;
        let (mut dictionary, mut metadata_budget) =
            ShadowKeyDictionary::load(&shadow_root, self.columnar_shadow.metadata_budget_bytes)?;
        let dictionary_len_before = dictionary.len();

        let all_dirty = self.columnar_shadow.all_dirty || previous.is_none();
        let dirty_set = self.columnar_shadow.dirty.clone();
        let is_dirty = |key: ColumnGroupTableKey| {
            all_dirty
                || dirty_set.contains(&key)
                || previous
                    .as_ref()
                    .is_none_or(|catalog| catalog.manifest().table(key).is_none())
        };

        // Any build failure persists the keys interned so far: monotone
        // progress is what makes a budget-exhausted attempt converge on
        // retry, because the next attempt's measured admission and loaded
        // baseline cover them.
        let build_result = (|| -> Result<(BuiltShadowTables, ManifestGeneration, Option<ManifestGeneration>, u64)> {
        // Pass 1: stream the canonical scan accumulating only per-table
        // per-property type-lattice state (O(1) per property), fixing each
        // dirty table's column layout before any row is buffered. Dirty
        // tables with zero remaining rows never appear here and are dropped
        // from the manifest instead of publishing empty directories.
        let mut table_types: BTreeMap<ColumnGroupTableKey, TablePropertyTypes> = BTreeMap::new();
        self.try_visit_nodes_owned(None, |node| {
            let key = node_table_key(&node.labels);
            if is_dirty(key) {
                if !table_types.contains_key(&key) {
                    metadata_budget
                        .charge(SHADOW_PASS1_TABLE_OVERHEAD_BYTES, "pass-1 table state")?;
                }
                table_types.entry(key).or_default().observe(
                    &node.properties,
                    &mut dictionary,
                    &mut metadata_budget,
                )?;
            }
            Ok(GraphScanControl::Continue)
        })?;
        self.try_visit_relationships_owned(None, |relationship| {
            let key = relationship_table_key(relationship.rel_type);
            if is_dirty(key) {
                if !table_types.contains_key(&key) {
                    metadata_budget
                        .charge(SHADOW_PASS1_TABLE_OVERHEAD_BYTES, "pass-1 table state")?;
                }
                table_types.entry(key).or_default().observe(
                    &relationship.properties,
                    &mut dictionary,
                    &mut metadata_budget,
                )?;
            }
            Ok(GraphScanControl::Continue)
        })?;
        let mut layouts = BTreeMap::new();
        for (table, types) in &table_types {
            layouts.insert(*table, shadow_table_layout(types, &mut metadata_budget)?);
        }

        let parent_generation = previous
            .as_ref()
            .map(|catalog| catalog.manifest().generation());
        let generation = ManifestGeneration(parent_generation.map_or(1, |parent| parent.0 + 1));

        // Pass 2: stream again, appending rows into bounded per-table group
        // buffers under the global byte budget. The builder takes the
        // pre-admitted token by value and never sees a governor; its peak
        // metric folds in the pass-1 state and the live dictionary.
        let admitted_budget_bytes = admission.admitted_budget_bytes();
        let mut builder = ShadowCheckpointBuilder::new(
            shadow_root.clone(),
            generation,
            admission,
            self.columnar_shadow.buffer_budget_bytes,
            layouts,
            metadata_budget,
        );
        self.try_visit_nodes_owned(None, |node| {
            if is_dirty(node_table_key(&node.labels)) {
                builder.append_node(&dictionary, node)?;
            }
            Ok(GraphScanControl::Continue)
        })?;
        self.try_visit_relationships_owned(None, |relationship| {
            if is_dirty(relationship_table_key(relationship.rel_type)) {
                builder.append_relationship(&dictionary, relationship)?;
            }
            Ok(GraphScanControl::Continue)
        })?;
        let built = builder.finish()?;
            Ok((built, generation, parent_generation, admitted_budget_bytes))
        })();
        let (built, generation, parent_generation, admitted_budget_bytes) = match build_result {
            Ok(values) => values,
            Err(error) => {
                if let Ok(mut persist_budget) = ShadowMetadataBudget::new(u64::MAX, 0) {
                    let _ = dictionary.persist(&shadow_root, &mut persist_budget);
                }
                return Err(error);
            }
        };
        let mut peak_builder_bytes = built.peak_builder_bytes;

        let mut tables = Vec::new();
        let mut reused_table_count = 0usize;
        let mut metadata_bytes_written = 0u64;
        if let Some(previous) = &previous {
            for reference in previous.manifest().tables() {
                if !is_dirty(reference.table()) {
                    // Untouched table: byte-identical directory reference,
                    // no bytes rebuilt (§3.6.5).
                    tables.push(reference.clone());
                    reused_table_count += 1;
                }
            }
        }
        let dirty_table_count = built.references.len();
        for (reference, directory_bytes) in built.references {
            metadata_bytes_written = metadata_bytes_written.saturating_add(directory_bytes);
            tables.push(reference);
        }

        if dictionary.len() != dictionary_len_before || dictionary_len_before == 0 {
            // Persisting is bookkeeping, not an enforcement point — the
            // budget was enforced during the build, and failing a finished
            // build here would discard the interned keys it must keep.
            let mut persist_budget =
                ShadowMetadataBudget::new(u64::MAX, built.metadata_bytes_used)?;
            metadata_bytes_written = metadata_bytes_written
                .saturating_add(dictionary.persist(&shadow_root, &mut persist_budget)?);
            peak_builder_bytes = peak_builder_bytes.max(persist_budget.peak_bytes());
            debug_assert!(
                persist_budget.peak_bytes() >= built.peak_metadata_bytes,
                "dictionary serialization carries the build's metadata baseline"
            );
        }

        let manifest =
            ColumnGroupManifest::new(generation, parent_generation, source_commit_epoch, tables)
                .map_err(shadow_error)?;
        let table_count = manifest.tables().len();
        let catalog = manifest.publish(&shadow_root).map_err(shadow_error)?;
        metadata_bytes_written = metadata_bytes_written.saturating_add(
            fs::metadata(shadow_root.join(skein_storage::COLUMN_GROUP_MANIFEST_FILE))?.len(),
        );
        // Strictly after the publish succeeded: best-effort reclamation of
        // everything outside the new catalog's reference closure, so disk
        // stays bounded by the current closure while the flag is on.
        let (reclaimed_file_count, reclaim_failed_count) =
            sweep_superseded_shadow_files(&shadow_root, &catalog);

        self.columnar_shadow.catalog = Some(catalog);
        self.columnar_shadow.all_dirty = false;
        self.columnar_shadow.dirty.clear();
        self.columnar_shadow.report = Some(ColumnarShadowCheckpointReport {
            status: ColumnarShadowCheckpointStatus::Published,
            generation: generation.0,
            source_commit_epoch,
            table_count,
            dirty_table_count,
            reused_table_count,
            group_bytes_written: built.group_bytes_written,
            metadata_bytes_written,
            peak_builder_bytes,
            admitted_budget_bytes,
            flushed_group_count: built.flushed_group_count,
            oversized_row_group_count: built.oversized_row_group_count,
            reclaimed_file_count,
            reclaim_failed_count,
            elapsed_micros: u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::TypeId;

    #[test]
    fn root_facade_preserves_storage_columnar_shadow_contract_identity() {
        assert_eq!(
            TypeId::of::<crate::ColumnarShadowCheckpointStatus>(),
            TypeId::of::<skein_storage::ColumnarShadowCheckpointStatus>()
        );
        assert_eq!(
            TypeId::of::<crate::ColumnarShadowCheckpointReport>(),
            TypeId::of::<skein_storage::ColumnarShadowCheckpointReport>()
        );
        assert_eq!(
            TypeId::of::<crate::ColumnarShadowRecoveryStatus>(),
            TypeId::of::<skein_storage::ColumnarShadowRecoveryStatus>()
        );
    }
    use skein_storage::{decode_residual_row_properties, ColumnGroupReader};

    fn unique_shadow_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_columnar_shadow_{name}_{nanos}"))
    }

    fn shadow_replay_config() -> WalReplayConfig {
        WalReplayConfig {
            graph_columnar_shadow_checkpoint: true,
            ..WalReplayConfig::default()
        }
    }

    fn open_shadow_store(path: &Path, catalog: &mut Catalog) -> GraphStore {
        GraphStore::open_with_durability_and_replay_config(
            path,
            catalog,
            DurabilityPolicy::SyncOnEveryWrite,
            shadow_replay_config(),
        )
        .unwrap()
    }

    fn properties(entries: &[(&str, Value)]) -> BTreeMap<String, Value> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    #[derive(Debug, Default)]
    struct ShadowInference {
        typed_column_count: usize,
        residual_row_count: usize,
    }

    /// Opens the shadow catalog fresh from disk and reconstructs every node
    /// and relationship from typed columns + residual + label-set columns.
    fn reconstruct_shadow(
        root: &Path,
    ) -> (
        BTreeMap<NodeId, NodeRecord>,
        BTreeMap<RelId, RelRecord>,
        ShadowInference,
    ) {
        let shadow_root = root.join(COLUMN_GROUP_SHADOW_DIR);
        let catalog = ColumnGroupManifest::open(&shadow_root).unwrap().unwrap();
        let (dictionary, _) =
            ShadowKeyDictionary::load(&shadow_root, DEFAULT_SHADOW_METADATA_BUDGET_BYTES).unwrap();
        let mut nodes = BTreeMap::new();
        let mut relationships = BTreeMap::new();
        let mut inference = ShadowInference::default();
        for directory in catalog.directories() {
            let table = directory.table();
            for descriptor in directory.groups() {
                let reader =
                    ColumnGroupReader::open_path(&shadow_root.join(descriptor.group_file()))
                        .unwrap();
                let ids = reader.read_ids().unwrap();
                // A group's directory lists only the columns it holds; a
                // missing column reads as all-null (streamed single-row
                // groups omit an empty residual column entirely).
                let residual = match reader.read_byte_column(RESIDUAL_COLUMN) {
                    Ok(residual) => residual,
                    Err(skein_storage::ColumnGroupError::PropertyMissing(_)) => {
                        vec![None; reader.directory().row_count as usize]
                    }
                    Err(error) => panic!("residual column read failed: {error}"),
                };
                let mut row_properties: Vec<BTreeMap<String, Value>> =
                    vec![BTreeMap::new(); ids.len()];
                for (row, blob) in residual.iter().enumerate() {
                    let Some(blob) = blob else { continue };
                    inference.residual_row_count += 1;
                    for (key_id, value) in decode_residual_row_properties(blob).unwrap() {
                        let key = dictionary.key(PropertyId(key_id)).unwrap().to_string();
                        assert!(row_properties[row].insert(key, value).is_none());
                    }
                }
                for column in &reader.directory().columns {
                    if column.property_id.0 < FIRST_DICTIONARY_COLUMN {
                        continue;
                    }
                    inference.typed_column_count += 1;
                    let key = dictionary.key(column.property_id).unwrap().to_string();
                    let values = reader.read_column(column.property_id, None).unwrap();
                    for (row, value) in values.into_iter().enumerate() {
                        if !matches!(value, Value::Null) {
                            assert!(row_properties[row].insert(key.clone(), value).is_none());
                        }
                    }
                }
                match table.kind {
                    ColumnGroupTableKind::Node => {
                        let label_sets = reader.read_byte_column(LABEL_SET_COLUMN).unwrap();
                        for (row, id) in ids.iter().enumerate() {
                            let labels = decode_label_set(
                                label_sets[row].as_deref().expect("label set present"),
                            )
                            .unwrap();
                            // The table id is the primary (minimum) label.
                            assert_eq!(node_table_key(&labels), table);
                            let record = NodeRecord {
                                id: NodeId(*id),
                                labels,
                                properties: std::mem::take(&mut row_properties[row]),
                            };
                            assert!(nodes.insert(record.id, record).is_none());
                        }
                    }
                    ColumnGroupTableKind::Relationship => {
                        let sources = reader.read_column(SOURCE_COLUMN, None).unwrap();
                        let targets = reader.read_column(TARGET_COLUMN, None).unwrap();
                        let rel_type = RelTypeId(u32::try_from(table.table_id).unwrap());
                        for (row, id) in ids.iter().enumerate() {
                            let source = match sources[row] {
                                Value::Int(value) => NodeId(value as u64),
                                ref other => panic!("source column held {other:?}"),
                            };
                            let target = match targets[row] {
                                Value::Int(value) => NodeId(value as u64),
                                ref other => panic!("target column held {other:?}"),
                            };
                            let record = RelRecord {
                                id: RelId(*id),
                                source,
                                target,
                                rel_type,
                                properties: std::mem::take(&mut row_properties[row]),
                            };
                            assert!(relationships.insert(record.id, record).is_none());
                        }
                    }
                    ColumnGroupTableKind::Relational => panic!("shadow holds no relational table"),
                }
            }
        }
        (nodes, relationships, inference)
    }

    fn canonical_scan(
        store: &GraphStore,
    ) -> (BTreeMap<NodeId, NodeRecord>, BTreeMap<RelId, RelRecord>) {
        let nodes = store
            .node_records_owned()
            .map(|record| record.map(|node| (node.id, node)))
            .collect::<Result<BTreeMap<_, _>>>()
            .unwrap();
        let relationships = store
            .relationship_records_owned()
            .map(|record| record.map(|relationship| (relationship.id, relationship)))
            .collect::<Result<BTreeMap<_, _>>>()
            .unwrap();
        (nodes, relationships)
    }

    fn assert_shadow_equivalence(root: &Path, store: &GraphStore) -> ShadowInference {
        let (expected_nodes, expected_relationships) = canonical_scan(store);
        let (nodes, relationships, inference) = reconstruct_shadow(root);
        assert_eq!(nodes, expected_nodes);
        assert_eq!(relationships, expected_relationships);
        inference
    }

    fn single_typed_string_layout(
        dictionary: &mut ShadowKeyDictionary,
        table: ColumnGroupTableKey,
        key: &str,
    ) -> (
        BTreeMap<ColumnGroupTableKey, ShadowTableLayout>,
        ShadowMetadataBudget,
    ) {
        let mut metadata_budget = ShadowMetadataBudget::new(
            DEFAULT_SHADOW_METADATA_BUDGET_BYTES,
            dictionary.estimated_bytes(),
        )
        .unwrap();
        dictionary.intern(key, &mut metadata_budget).unwrap();
        metadata_budget
            .charge(SHADOW_PASS1_TABLE_OVERHEAD_BYTES, "test pass-1 state")
            .unwrap();
        let mut types = TablePropertyTypes::default();
        types
            .observe(
                &BTreeMap::from([(key.to_string(), Value::String(String::new()))]),
                dictionary,
                &mut metadata_budget,
            )
            .unwrap();
        (
            BTreeMap::from([(
                table,
                shadow_table_layout(&types, &mut metadata_budget).unwrap(),
            )]),
            metadata_budget,
        )
    }

    #[test]
    fn shadow_builder_moves_owned_typed_payload_into_group_buffer() {
        let root = unique_shadow_dir("owned_typed_payload");
        fs::create_dir_all(&root).unwrap();
        let table = ColumnGroupTableKey::new(ColumnGroupTableKind::Node, 0);
        let mut dictionary = ShadowKeyDictionary::default();
        let (layouts, metadata_budget) = single_typed_string_layout(&mut dictionary, table, "body");
        let mut builder = ShadowCheckpointBuilder::new(
            root.clone(),
            ManifestGeneration(1),
            ColumnarShadowAdmission::unmetered(),
            DEFAULT_SHADOW_BUFFER_BUDGET_BYTES,
            layouts,
            metadata_budget,
        );
        let payload = "x".repeat(1024 * 1024);
        let payload_ptr = payload.as_ptr();
        builder
            .append_node(
                &dictionary,
                NodeRecord {
                    id: NodeId(1),
                    labels: BTreeSet::new(),
                    properties: BTreeMap::from([("body".to_string(), Value::String(payload))]),
                },
            )
            .unwrap();

        let Value::String(buffered) = &builder.buffers[&table].typed[0][0] else {
            panic!("typed body must remain a string")
        };
        assert_eq!(buffered.as_ptr(), payload_ptr);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn shadow_reconstruction_matches_canonical_scan_and_reuses_untouched_tables() {
        let root = unique_shadow_dir("equivalence");
        let mut catalog = Catalog::default();
        let mut store = open_shadow_store(&root, &mut catalog);

        let n1 = store
            .create_node(
                &mut catalog,
                "Person",
                properties(&[
                    ("name", Value::String("alice".to_string())),
                    ("age", Value::Int(30)),
                    ("score", Value::Float(1.5)),
                    ("flex", Value::Int(7)),
                    (
                        "tags",
                        Value::List(vec![Value::Int(1), Value::String("x".to_string())]),
                    ),
                ]),
            )
            .unwrap();
        let n2 = store
            .create_node(
                &mut catalog,
                "Person",
                properties(&[
                    ("name", Value::String("bob".to_string())),
                    ("age", Value::Int(41)),
                    ("active", Value::Bool(true)),
                    ("flex", Value::String("seven".to_string())),
                    ("ghost", Value::Null),
                ]),
            )
            .unwrap();
        let n3 = store
            .create_node(
                &mut catalog,
                "Person",
                properties(&[
                    ("name", Value::String("cara".to_string())),
                    ("score", Value::Float(-2.25)),
                    ("active", Value::Bool(false)),
                    ("meta", Value::Map(properties(&[("k", Value::Int(3))]))),
                ]),
            )
            .unwrap();
        // Multi-label node: created through the apply layer (the WAL surface
        // is single-label), primary table = minimum label id = Person.
        catalog.get_or_create_label("Extra");
        let n4 = NodeId(store.next_node_id);
        store.apply_create_node_with_labels(
            &catalog,
            n4,
            BTreeSet::from([
                catalog.label_id("Person").unwrap(),
                catalog.label_id("Extra").unwrap(),
            ]),
            properties(&[("name", Value::String("dora".to_string()))]),
        );
        store.commit_epoch += 1;
        // Unlabeled node: reserved table 0.
        let u1 = NodeId(store.next_node_id);
        store.apply_create_node_with_labels(
            &catalog,
            u1,
            BTreeSet::new(),
            properties(&[("kind", Value::String("floating".to_string()))]),
        );
        store.commit_epoch += 1;

        store
            .create_relationship(
                &mut catalog,
                n1,
                n2,
                "KNOWS",
                properties(&[("since", Value::Int(2019))]),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                n2,
                n3,
                "KNOWS",
                properties(&[("since", Value::Int(2021)), ("weight", Value::Float(0.5))]),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                n3,
                n1,
                "LIKES",
                properties(&[("strength", Value::Int(2))]),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                n4,
                n1,
                "LIKES",
                properties(&[("strength", Value::String("high".to_string()))]),
            )
            .unwrap();

        store.checkpoint(&catalog).unwrap();
        let inference = assert_shadow_equivalence(&root, &store);
        // Per-generation inference: Person typed {name, age, score, active},
        // unlabeled typed {kind}, KNOWS typed {since, weight}, LIKES none
        // (mixed strength). Residual rows: n1 (flex, tags), n2 (flex,
        // ghost), n3 (meta), and both mixed-strength LIKES rows.
        assert_eq!(inference.typed_column_count, 7);
        assert_eq!(inference.residual_row_count, 5);
        let first_report = store.columnar_shadow_checkpoint_report().unwrap();
        assert_eq!(first_report.generation, 1);
        assert_eq!(first_report.table_count, 4);
        assert_eq!(first_report.dirty_table_count, 4);
        assert_eq!(first_report.reused_table_count, 0);
        assert!(first_report.group_bytes_written > 0);
        assert!(first_report.metadata_bytes_written > 0);

        let shadow_root = root.join(COLUMN_GROUP_SHADOW_DIR);
        let untouched_tables = [
            ColumnGroupTableKey::new(ColumnGroupTableKind::Node, 0),
            relationship_table_key(catalog.rel_type_id("LIKES").unwrap()),
        ];
        let first_catalog = ColumnGroupManifest::open(&shadow_root).unwrap().unwrap();
        let first_refs = untouched_tables
            .iter()
            .map(|table| {
                let reference = first_catalog.manifest().table(*table).unwrap().clone();
                let bytes = fs::read(shadow_root.join(reference.file_name())).unwrap();
                (reference, bytes)
            })
            .collect::<Vec<_>>();

        // Mutate a subset: one Person property write and one new KNOWS
        // relationship; the unlabeled and LIKES tables stay untouched.
        store
            .set_node_properties_by_ids(
                &mut catalog,
                &[n1],
                &[NodeSetAssignment {
                    property: "age".to_string(),
                    value: NodeSetValue::Value(Value::Int(31)),
                }],
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                n3,
                n4,
                "KNOWS",
                properties(&[("since", Value::Int(2024))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();

        assert_shadow_equivalence(&root, &store);
        let second_report = store.columnar_shadow_checkpoint_report().unwrap();
        assert_eq!(second_report.generation, 2);
        assert_eq!(second_report.table_count, 4);
        assert_eq!(second_report.dirty_table_count, 2);
        assert_eq!(second_report.reused_table_count, 2);
        // Write amplification is proportional to the dirty TABLE COUNT:
        // the one-row edit still rewrote the whole Person table (no delta
        // groups until C4), but the two untouched tables cost zero group
        // bytes, so rebuilding 2 of 4 tables wrote strictly less than the
        // full first checkpoint.
        assert!(second_report.group_bytes_written < first_report.group_bytes_written);

        let second_catalog = ColumnGroupManifest::open(&shadow_root).unwrap().unwrap();
        assert_eq!(
            second_catalog.manifest().generation(),
            ManifestGeneration(2)
        );
        for (reference, bytes) in &first_refs {
            let reused = second_catalog.manifest().table(reference.table()).unwrap();
            assert_eq!(reused, reference, "untouched table reference is reused");
            assert_eq!(
                &fs::read(shadow_root.join(reused.file_name())).unwrap(),
                bytes,
                "untouched table directory bytes are identical"
            );
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn flag_off_checkpoints_produce_no_shadow_directory_or_report() {
        let root = unique_shadow_dir("flag_off");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &root,
            &mut catalog,
            DurabilityPolicy::SyncOnEveryWrite,
            WalReplayConfig::default(),
        )
        .unwrap();
        store
            .create_node(
                &mut catalog,
                "Person",
                properties(&[("name", Value::String("a".to_string()))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        assert!(!root.join(COLUMN_GROUP_SHADOW_DIR).exists());
        assert_eq!(store.columnar_shadow_checkpoint_report(), None);
        assert_eq!(
            store.columnar_shadow_recovery_status(),
            ColumnarShadowRecoveryStatus::default()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn restart_validates_the_shadow_and_replayed_mutations_mark_dirty_tables() {
        let root = unique_shadow_dir("restart");
        let mut catalog = Catalog::default();
        let mut store = open_shadow_store(&root, &mut catalog);
        let n1 = store
            .create_node(
                &mut catalog,
                "Person",
                properties(&[("name", Value::String("a".to_string()))]),
            )
            .unwrap();
        let n2 = store
            .create_node(
                &mut catalog,
                "City",
                properties(&[("name", Value::String("b".to_string()))]),
            )
            .unwrap();
        store
            .create_relationship(&mut catalog, n1, n2, "IN", properties(&[]))
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        // A WAL-only mutation between checkpoint and restart: replay must
        // mark exactly the Person table dirty.
        store
            .set_node_properties_by_ids(
                &mut catalog,
                &[n1],
                &[NodeSetAssignment {
                    property: "name".to_string(),
                    value: NodeSetValue::Value(Value::String("a2".to_string())),
                }],
            )
            .unwrap();
        drop(store);

        let mut catalog = Catalog::default();
        let mut store = open_shadow_store(&root, &mut catalog);
        let status = store.columnar_shadow_recovery_status();
        assert!(
            status.validated,
            "shadow catalog validates on reopen: {status:?}"
        );
        assert!(!status.discarded);
        store.checkpoint(&catalog).unwrap();
        let report = store.columnar_shadow_checkpoint_report().unwrap();
        assert_eq!(report.generation, 2);
        assert_eq!(report.table_count, 3);
        assert_eq!(report.dirty_table_count, 1);
        assert_eq!(report.reused_table_count, 2);
        assert_shadow_equivalence(&root, &store);
        drop(store);

        // Corrupt one manifest-referenced shadow group's checksummed footer:
        // the shadow is rebuildable derived state, so reopen discards it
        // (the projected-graph policy of STORAGE.md recovery step 10)
        // instead of failing closed, and the next checkpoint rebuilds every
        // table.
        let shadow_root = root.join(COLUMN_GROUP_SHADOW_DIR);
        let referenced_group = |shadow_root: &Path| {
            let catalog = ColumnGroupManifest::open(shadow_root).unwrap().unwrap();
            let file = catalog.directories()[0].groups()[0]
                .group_file()
                .to_string();
            shadow_root.join(file)
        };
        let group_file = referenced_group(&shadow_root);
        let mut bytes = fs::read(&group_file).unwrap();
        let footer_byte = bytes.len() - 10;
        bytes[footer_byte] ^= 0x40;
        fs::write(&group_file, &bytes).unwrap();

        let mut catalog = Catalog::default();
        let mut store = open_shadow_store(&root, &mut catalog);
        let status = store.columnar_shadow_recovery_status();
        assert!(status.discarded, "corrupt shadow is discarded: {status:?}");
        assert!(status.error.is_some());
        assert!(
            !shadow_root.exists(),
            "discarded shadow directory is removed"
        );
        store.checkpoint(&catalog).unwrap();
        let report = store.columnar_shadow_checkpoint_report().unwrap();
        assert_eq!(
            report.generation, 1,
            "shadow restarts its generation sequence"
        );
        assert_eq!(report.dirty_table_count, report.table_count);
        assert_eq!(report.reused_table_count, 0);
        assert_shadow_equivalence(&root, &store);
        drop(store);

        // With the flag off, a corrupt shadow is neither validated nor
        // touched.
        let group_file = referenced_group(&shadow_root);
        let mut bytes = fs::read(&group_file).unwrap();
        let footer_byte = bytes.len() - 10;
        bytes[footer_byte] ^= 0x40;
        fs::write(&group_file, &bytes).unwrap();
        let mut catalog = Catalog::default();
        let store = GraphStore::open_with_durability_and_replay_config(
            &root,
            &mut catalog,
            DurabilityPolicy::SyncOnEveryWrite,
            WalReplayConfig::default(),
        )
        .unwrap();
        assert_eq!(
            store.columnar_shadow_recovery_status(),
            ColumnarShadowRecoveryStatus::default()
        );
        assert!(
            shadow_root.exists(),
            "flag-off open leaves the shadow untouched"
        );
        drop(store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn each_mutation_kind_marks_its_shadow_table_dirty() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::default();
        store.columnar_shadow.enabled = true;

        let node_table = |label: &str, catalog: &Catalog| {
            node_table_key(&BTreeSet::from([catalog.label_id(label).unwrap()]))
        };

        // Node create marks the primary-label table.
        let a = store
            .create_node(&mut catalog, "A", properties(&[("p", Value::Int(1))]))
            .unwrap();
        assert_eq!(
            store.columnar_shadow.dirty,
            BTreeSet::from([node_table("A", &catalog)])
        );
        store.columnar_shadow.dirty.clear();

        // Node property set marks the primary-label table.
        store
            .apply_wal_op(
                &mut catalog,
                WalOp::SetNodeProperty {
                    id: a,
                    property: "p".to_string(),
                    value: Value::Int(2),
                },
            )
            .unwrap();
        assert_eq!(
            store.columnar_shadow.dirty,
            BTreeSet::from([node_table("A", &catalog)])
        );
        store.columnar_shadow.dirty.clear();

        // A label-set replacement marks BOTH primary tables.
        catalog.get_or_create_label("B");
        store.apply_create_node_with_labels(
            &catalog,
            a,
            BTreeSet::from([catalog.label_id("B").unwrap()]),
            properties(&[]),
        );
        assert_eq!(
            store.columnar_shadow.dirty,
            BTreeSet::from([node_table("A", &catalog), node_table("B", &catalog)])
        );
        store.columnar_shadow.dirty.clear();

        // An unlabeled create marks the reserved table 0.
        store.apply_create_node_with_labels(
            &catalog,
            NodeId(store.next_node_id),
            BTreeSet::new(),
            properties(&[]),
        );
        assert_eq!(
            store.columnar_shadow.dirty,
            BTreeSet::from([ColumnGroupTableKey::new(ColumnGroupTableKind::Node, 0)])
        );
        store.columnar_shadow.dirty.clear();

        // Relationship create marks the rel-type table (and only it).
        let b = store
            .create_node(&mut catalog, "B", properties(&[]))
            .unwrap();
        store.columnar_shadow.dirty.clear();
        let r = store
            .create_relationship(&mut catalog, a, b, "R", properties(&[]))
            .unwrap();
        let r_table = relationship_table_key(catalog.rel_type_id("R").unwrap());
        assert_eq!(store.columnar_shadow.dirty, BTreeSet::from([r_table]));
        store.columnar_shadow.dirty.clear();

        // Relationship property set marks the rel-type table.
        store
            .apply_wal_op(
                &mut catalog,
                WalOp::SetRelationshipProperty {
                    id: r,
                    property: "w".to_string(),
                    value: Value::Int(9),
                },
            )
            .unwrap();
        assert_eq!(store.columnar_shadow.dirty, BTreeSet::from([r_table]));
        store.columnar_shadow.dirty.clear();

        // Relationship delete marks the rel-type table.
        store
            .apply_wal_op(&mut catalog, WalOp::DeleteRelationship { id: r })
            .unwrap();
        assert_eq!(store.columnar_shadow.dirty, BTreeSet::from([r_table]));
        store.columnar_shadow.dirty.clear();

        // Node delete marks the primary-label table.
        store
            .apply_wal_op(&mut catalog, WalOp::DeleteNode { id: b })
            .unwrap();
        assert_eq!(
            store.columnar_shadow.dirty,
            BTreeSet::from([node_table("B", &catalog)])
        );
        store.columnar_shadow.dirty.clear();

        // With the shadow disabled, nothing is tracked.
        store.columnar_shadow.enabled = false;
        store
            .create_node(&mut catalog, "A", properties(&[]))
            .unwrap();
        assert!(store.columnar_shadow.dirty.is_empty());
    }

    #[test]
    fn shadow_publish_failure_never_fails_the_canonical_checkpoint_and_retries() {
        let root = unique_shadow_dir("shadow_failure");
        let mut catalog = Catalog::default();
        let mut store = open_shadow_store(&root, &mut catalog);
        let n1 = store
            .create_node(
                &mut catalog,
                "Person",
                properties(&[("name", Value::String("a".to_string()))]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "City",
                properties(&[("name", Value::String("b".to_string()))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        assert_eq!(
            store.columnar_shadow_checkpoint_report().unwrap().status,
            ColumnarShadowCheckpointStatus::Published
        );

        // Dirty exactly the Person table, then poison the shadow directory:
        // a directory squats on the path the next generation's Person group
        // must atomically replace, so the shadow build fails mid-flush.
        store
            .set_node_properties_by_ids(
                &mut catalog,
                &[n1],
                &[NodeSetAssignment {
                    property: "name".to_string(),
                    value: NodeSetValue::Value(Value::String("a2".to_string())),
                }],
            )
            .unwrap();
        let person_table = node_table_key(&BTreeSet::from([catalog.label_id("Person").unwrap()]));
        let shadow_root = root.join(COLUMN_GROUP_SHADOW_DIR);
        let poison = shadow_root.join(format!("group-node-{}-2-0.skein", person_table.table_id));
        fs::create_dir_all(&poison).unwrap();

        // The canonical checkpoint MUST succeed; only the shadow report
        // records the failure, with dirty state preserved for the retry.
        store.checkpoint(&catalog).unwrap();
        let report = store.columnar_shadow_checkpoint_report().unwrap();
        assert!(
            matches!(report.status, ColumnarShadowCheckpointStatus::Failed { .. }),
            "expected a failed shadow report, got {report:?}"
        );
        assert!(store.columnar_shadow.dirty.contains(&person_table));
        assert!(!store.columnar_shadow.all_dirty);
        // The canonical side is intact and the shadow on disk still selects
        // the previous complete generation.
        let (nodes, _) = canonical_scan(&store);
        assert_eq!(
            nodes[&n1].properties["name"],
            Value::String("a2".to_string())
        );
        assert_eq!(
            ColumnGroupManifest::open(&shadow_root)
                .unwrap()
                .unwrap()
                .manifest()
                .generation(),
            ManifestGeneration(1)
        );

        // Clearing the poison lets the NEXT checkpoint converge from the
        // preserved dirty state without any new mutation.
        fs::remove_dir_all(&poison).unwrap();
        store.checkpoint(&catalog).unwrap();
        let report = store.columnar_shadow_checkpoint_report().unwrap();
        assert_eq!(report.status, ColumnarShadowCheckpointStatus::Published);
        assert_eq!(report.generation, 2);
        assert_eq!(report.dirty_table_count, 1);
        assert_eq!(report.reused_table_count, 1);
        assert!(store.columnar_shadow.dirty.is_empty());
        assert_shadow_equivalence(&root, &store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn large_builds_stay_under_the_buffer_budget_with_short_groups() {
        let root = unique_shadow_dir("bounded");
        let mut catalog = Catalog::default();
        let mut store = open_shadow_store(&root, &mut catalog);
        // A small budget override against a graph an order of magnitude
        // larger: the build must flush short groups instead of collecting
        // whole tables (write amplification stays proportional to the dirty
        // table count, but memory must not scale with table size).
        const BUDGET: u64 = 8 * 1024;
        store.columnar_shadow.buffer_budget_bytes = BUDGET;

        let mut node_ids = Vec::new();
        for index in 0..600u32 {
            let label = if index % 2 == 0 { "Alpha" } else { "Beta" };
            let id = store
                .create_node(
                    &mut catalog,
                    label,
                    properties(&[
                        (
                            "name",
                            Value::String(format!("row-{index:05}-{}", "x".repeat(48))),
                        ),
                        ("rank", Value::Int(i64::from(index))),
                        (
                            "flex",
                            if index % 3 == 0 {
                                Value::Int(i64::from(index))
                            } else {
                                Value::String("mixed".to_string())
                            },
                        ),
                    ]),
                )
                .unwrap();
            node_ids.push(id);
        }
        for window in node_ids.windows(2).step_by(3) {
            store
                .create_relationship(
                    &mut catalog,
                    window[0],
                    window[1],
                    "LINKS",
                    properties(&[("weight", Value::Int(7))]),
                )
                .unwrap();
        }

        // One row whose estimate alone dwarfs the whole budget: it must
        // flush everything and become its own single-row group instead of
        // ever being buffered behind other rows.
        store
            .create_node(
                &mut catalog,
                "Alpha",
                properties(&[(
                    "name",
                    Value::String(format!("oversized-{}", "z".repeat(3 * BUDGET as usize))),
                )]),
            )
            .unwrap();

        store.checkpoint(&catalog).unwrap();
        let report = store.columnar_shadow_checkpoint_report().unwrap();
        // Honest builder-footprint accounting: pass-1 state, the key
        // dictionary, and buffered rows all count, and outside the single
        // oversized-row transient the footprint stays within budget plus
        // that fixed metadata overhead — well inside the admitted
        // reservation either way.
        assert!(report.peak_builder_bytes > 0);
        assert_eq!(report.admitted_budget_bytes, 0, "unmetered build");
        // The oversized row streams: no row-sized term appears in the
        // peak, only the fixed streaming allowance.
        assert!(
            report.peak_builder_bytes
                <= BUDGET
                    + DEFAULT_SHADOW_METADATA_BUDGET_BYTES
                    + SHADOW_STREAMED_FLUSH_ALLOWANCE_BYTES,
            "peak builder bytes {} exceed the budgeted footprint",
            report.peak_builder_bytes
        );
        assert!(
            report.peak_builder_bytes < store.columnar_shadow_admission_bytes(),
            "peak builder bytes {} exceed the admission reservation",
            report.peak_builder_bytes
        );
        assert_eq!(report.oversized_row_group_count, 1);
        // The data volume is far beyond one budget's worth, so the build
        // must have flushed many budget-driven short groups.
        assert!(report.group_bytes_written > BUDGET);
        assert!(
            report.flushed_group_count > report.table_count,
            "expected budget-driven short groups beyond one per table, got {}",
            report.flushed_group_count
        );
        // Short groups are legal (row capacity is a max, not a min) and the
        // multi-group reconstruction — including the single-row oversized
        // group — still matches the canonical scan.
        assert_shadow_equivalence(&root, &store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn metadata_budget_rejects_new_schema_before_allocating_or_publishing() {
        let root = unique_shadow_dir("metadata_budget");
        let mut catalog = Catalog::default();
        let mut store = open_shadow_store(&root, &mut catalog);
        store.columnar_shadow.metadata_budget_bytes = 128;
        store
            .create_node(
                &mut catalog,
                "Wide",
                properties(&[(
                    "property-key-too-wide-for-the-metadata-budget",
                    Value::Int(1),
                )]),
            )
            .unwrap();

        store.checkpoint(&catalog).unwrap();
        let report = store.columnar_shadow_checkpoint_report().unwrap();
        assert!(matches!(
            report.status,
            ColumnarShadowCheckpointStatus::Failed { ref error }
                if error.contains("metadata bytes") && error.contains("enforced 256 byte budget")
        ));
        assert!(store.columnar_shadow.all_dirty);
        assert!(
            ColumnGroupManifest::open(&root.join(COLUMN_GROUP_SHADOW_DIR))
                .unwrap()
                .is_none()
        );

        store.columnar_shadow.metadata_budget_bytes = DEFAULT_SHADOW_METADATA_BUDGET_BYTES;
        store.checkpoint(&catalog).unwrap();
        assert_eq!(
            store.columnar_shadow_checkpoint_report().unwrap().status,
            ColumnarShadowCheckpointStatus::Published
        );
        assert_shadow_equivalence(&root, &store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn shadow_builds_take_exactly_one_admission_for_the_whole_build() {
        let root = unique_shadow_dir("governor");
        let mut catalog = Catalog::default();
        let mut store = open_shadow_store(&root, &mut catalog);
        let governor = skein_qos::RuntimeGovernor::detect(
            skein_qos::RuntimeGovernorConfig::shared_host(),
            skein_qos::IoConcurrencyBudget::new(2, 1),
        );
        store.set_runtime_governor(governor.clone());
        store.columnar_shadow.buffer_budget_bytes = 4 * 1024;
        for index in 0..200u32 {
            store
                .create_node(
                    &mut catalog,
                    "Metered",
                    properties(&[(
                        "name",
                        Value::String(format!("metered-{index:04}-{}", "y".repeat(32))),
                    )]),
                )
                .unwrap();
        }
        let admissions_before = governor.snapshot().admissions;
        store.checkpoint(&catalog).unwrap();
        let report = store.columnar_shadow_checkpoint_report().unwrap();
        assert_eq!(report.status, ColumnarShadowCheckpointStatus::Published);
        assert!(report.flushed_group_count > 1);
        // The whole multi-flush build rode exactly one up-front admission:
        // flushes only draw down the token's byte allowance.
        assert_eq!(governor.snapshot().admissions, admissions_before + 1);
        assert_shadow_equivalence(&root, &store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn pre_admitted_shadow_converges_under_a_constrained_governor() {
        let root = unique_shadow_dir("constrained_governor");
        let mut catalog = Catalog::default();
        let mut store = open_shadow_store(&root, &mut catalog);
        // A mobile-style constraint: one background task, total. The outer
        // permit (the nowledge_mem typed checkpoint's background
        // maintenance admission) is held for the whole duration, so any
        // nested admission inside the shadow build could never succeed.
        let governor = skein_qos::RuntimeGovernor::detect(
            skein_qos::RuntimeGovernorConfig {
                background_task_limit: std::num::NonZeroUsize::new(1),
                ..skein_qos::RuntimeGovernorConfig::shared_host()
            },
            skein_qos::IoConcurrencyBudget::new(2, 1),
        );
        store.set_runtime_governor(governor.clone());
        for index in 0..40u32 {
            store
                .create_node(
                    &mut catalog,
                    "Constrained",
                    properties(&[("rank", Value::Int(i64::from(index)))]),
                )
                .unwrap();
        }
        let shadow_bytes = store.columnar_shadow_admission_bytes();
        assert!(shadow_bytes > 0);
        let _outer_permit = governor
            .try_admit(
                skein_qos::RuntimeWorkRequest::background_maintenance(shadow_bytes)
                    .with_io_slots(1),
            )
            .expect("outer maintenance permit is admitted");

        // The plain path's single non-nested try_admit is rejected while
        // the outer permit exhausts the background slot — the shadow fails
        // fast (no waiting loop) and the canonical checkpoint succeeds.
        store.checkpoint(&catalog).unwrap();
        let report = store.columnar_shadow_checkpoint_report().unwrap();
        assert!(
            matches!(report.status, ColumnarShadowCheckpointStatus::Failed { .. }),
            "un-annotated build under an exhausted governor fails fast: {report:?}"
        );

        // The pre-admitted token path — the outer permit's memory request
        // already covers the shadow reservation — PUBLISHES while the outer
        // permit stays held: structural non-reentrancy, no nested wait.
        store
            .checkpoint_with_shadow_admission(
                &catalog,
                ColumnarShadowAdmission::pre_admitted(shadow_bytes),
            )
            .unwrap();
        let report = store.columnar_shadow_checkpoint_report().unwrap();
        assert_eq!(
            report.status,
            ColumnarShadowCheckpointStatus::Published,
            "pre-admitted shadow converges under the held outer permit"
        );
        assert_shadow_equivalence(&root, &store);
        fs::remove_dir_all(root).unwrap();
    }

    fn shadow_disk_footprint(shadow_root: &Path) -> (usize, u64) {
        let mut files = 0usize;
        let mut bytes = 0u64;
        for entry in fs::read_dir(shadow_root).unwrap().flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.ends_with(".skein") || name.ends_with(".skein.tmp") {
                files += 1;
                bytes += entry.metadata().unwrap().len();
            }
        }
        (files, bytes)
    }

    #[test]
    fn superseded_shadow_artifacts_are_reclaimed_after_each_publish() {
        let root = unique_shadow_dir("reclaim");
        let mut catalog = Catalog::default();
        let mut store = open_shadow_store(&root, &mut catalog);
        let n1 = store
            .create_node(
                &mut catalog,
                "Person",
                properties(&[("name", Value::String("a".to_string()))]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "City",
                properties(&[("name", Value::String("b".to_string()))]),
            )
            .unwrap();
        let shadow_root = root.join(COLUMN_GROUP_SHADOW_DIR);

        // Five dirty checkpoints: without reclamation every round would add
        // a new generation of Person group + directory files. The sweep
        // keeps disk bounded by the active reference closure.
        let mut footprints = Vec::new();
        for round in 0..5u32 {
            store
                .set_node_properties_by_ids(
                    &mut catalog,
                    &[n1],
                    &[NodeSetAssignment {
                        property: "name".to_string(),
                        value: NodeSetValue::Value(Value::String(format!("round-{round}"))),
                    }],
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();
            footprints.push(shadow_disk_footprint(&shadow_root));
        }
        let report = store.columnar_shadow_checkpoint_report().unwrap();
        assert_eq!(report.status, ColumnarShadowCheckpointStatus::Published);
        assert!(report.reclaimed_file_count > 0, "old generations reclaimed");
        assert_eq!(report.reclaim_failed_count, 0);
        // Steady state: the footprint after round 5 matches round 2 — no
        // per-generation growth (round 1 may differ while the reused City
        // directory still carries its first-generation name).
        assert_eq!(
            footprints[1].0, footprints[4].0,
            "file count is bounded across dirty checkpoints: {footprints:?}"
        );
        // Every remaining artifact belongs to the active closure.
        let catalog_on_disk = ColumnGroupManifest::open(&shadow_root).unwrap().unwrap();
        let mut keep = BTreeSet::new();
        keep.insert(skein_storage::COLUMN_GROUP_MANIFEST_FILE.to_string());
        keep.insert(SHADOW_KEY_DICTIONARY_FILE.to_string());
        for reference in catalog_on_disk.manifest().tables() {
            keep.insert(reference.file_name().to_string());
        }
        for directory in catalog_on_disk.directories() {
            for group in directory.groups() {
                keep.insert(group.group_file().to_string());
            }
        }
        for entry in fs::read_dir(&shadow_root).unwrap().flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.ends_with(".skein") || name.ends_with(".skein.tmp") {
                assert!(
                    keep.contains(name),
                    "unreferenced artifact {name} survived the sweep"
                );
            }
        }
        assert_shadow_equivalence(&root, &store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sweep_removal_failure_is_recorded_and_never_fails_the_publish() {
        let root = unique_shadow_dir("reclaim_failure");
        let mut catalog = Catalog::default();
        let mut store = open_shadow_store(&root, &mut catalog);
        let n1 = store
            .create_node(
                &mut catalog,
                "Person",
                properties(&[("name", Value::String("a".to_string()))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();

        // A non-empty directory squatting on a stale-artifact name: the
        // sweep's remove_file fails on it, which must be recorded — never
        // propagated into the publication result.
        let shadow_root = root.join(COLUMN_GROUP_SHADOW_DIR);
        let stubborn = shadow_root.join("group-node-999-1-0.skein");
        fs::create_dir_all(stubborn.join("occupant")).unwrap();
        store
            .set_node_properties_by_ids(
                &mut catalog,
                &[n1],
                &[NodeSetAssignment {
                    property: "name".to_string(),
                    value: NodeSetValue::Value(Value::String("a2".to_string())),
                }],
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        let report = store.columnar_shadow_checkpoint_report().unwrap();
        assert_eq!(report.status, ColumnarShadowCheckpointStatus::Published);
        assert!(
            report.reclaim_failed_count >= 1,
            "failure recorded: {report:?}"
        );

        // Clearing the obstruction lets the next publish's retry reclaim it.
        fs::remove_dir_all(&stubborn).unwrap();
        fs::write(&stubborn, b"now a stale file").unwrap();
        store
            .set_node_properties_by_ids(
                &mut catalog,
                &[n1],
                &[NodeSetAssignment {
                    property: "name".to_string(),
                    value: NodeSetValue::Value(Value::String("a3".to_string())),
                }],
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        let report = store.columnar_shadow_checkpoint_report().unwrap();
        assert_eq!(report.status, ColumnarShadowCheckpointStatus::Published);
        assert_eq!(report.reclaim_failed_count, 0);
        assert!(!stubborn.exists(), "retried sweep reclaimed the stale file");
        assert_shadow_equivalence(&root, &store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn varint_label_sets_round_trip() {
        for labels in [
            BTreeSet::new(),
            BTreeSet::from([LabelId(0)]),
            BTreeSet::from([LabelId(0), LabelId(1), LabelId(127), LabelId(128)]),
            BTreeSet::from([LabelId(16_383), LabelId(16_384), LabelId(u32::MAX)]),
        ] {
            let encoded = encode_label_set(&labels);
            assert_eq!(decode_label_set(&encoded).unwrap(), labels);
        }
        assert!(decode_label_set(&[0x80]).is_err());
        assert!(decode_label_set(&[0xff, 0xff, 0xff, 0xff, 0x7f]).is_err());
    }

    /// The review's convergence case: a legal single row far larger than
    /// the whole admission must publish via the streaming path — never
    /// materialized, never a permanent build failure — and the peak stays
    /// row-size-independent.
    #[test]
    fn oversized_legal_row_publishes_via_streaming_and_converges() {
        let root = unique_shadow_dir("stream-oversized");
        let mut catalog = Catalog::default();
        let mut store = open_shadow_store(&root, &mut catalog);
        const BUDGET: u64 = 8 * 1024;
        store.columnar_shadow.buffer_budget_bytes = BUDGET;
        const ROW_BYTES: usize = 1024 * 1024;
        store
            .create_node(
                &mut catalog,
                "Doc",
                properties(&[
                    ("body", Value::String("z".repeat(ROW_BYTES))),
                    (
                        "attachments",
                        Value::List(vec![Value::String("a".repeat(64 * 1024)), Value::Int(7)]),
                    ),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Doc",
                properties(&[("body", Value::String("small".to_string()))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        let report = store.columnar_shadow_checkpoint_report().unwrap();
        assert_eq!(report.status, ColumnarShadowCheckpointStatus::Published);
        assert_eq!(report.oversized_row_group_count, 1);
        // Row-size independence: the 1 MiB+ row never appears in the peak.
        assert!(
            report.peak_builder_bytes < ROW_BYTES as u64 / 4,
            "peak {} suggests the oversized row was materialized",
            report.peak_builder_bytes
        );
        assert_shadow_equivalence(&root, &store);
        fs::remove_dir_all(root).unwrap();
    }

    /// The review's dictionary case: growth past the enforced reservation
    /// fails exactly once with the interned keys durable, and the next
    /// checkpoint converges because its measured admission and baseline
    /// cover them.
    #[test]
    fn dictionary_growth_past_reservation_fails_once_then_converges() {
        let root = unique_shadow_dir("dictionary-converges");
        let mut catalog = Catalog::default();
        let mut store = open_shadow_store(&root, &mut catalog);
        store.columnar_shadow.metadata_budget_bytes = 512;
        for index in 0..24u32 {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties(&[(
                        format!("distinct_property_key_number_{index:04}").as_str(),
                        Value::Int(i64::from(index)),
                    )]),
                )
                .unwrap();
        }
        let admission_before = store.columnar_shadow_admission_bytes();
        store.checkpoint(&catalog).unwrap();
        let first = store.columnar_shadow_checkpoint_report().unwrap();
        let ColumnarShadowCheckpointStatus::Failed { error } = &first.status else {
            panic!("dictionary growth past the reservation must fail this attempt");
        };
        assert!(
            error.contains("metadata bytes, exceeding"),
            "unexpected error: {error}"
        );
        // Monotone progress: every failed attempt leaves strictly more
        // interned keys durable, so the growth budget bounds work per
        // attempt while the measured baseline ratchets forward — bounded
        // attempts later, the build publishes.
        let shadow_root = root.join(COLUMN_GROUP_SHADOW_DIR);
        let mut dictionary_bytes = fs::metadata(shadow_root.join(SHADOW_KEY_DICTIONARY_FILE))
            .unwrap()
            .len();
        assert!(dictionary_bytes > 0);
        assert!(store.columnar_shadow_admission_bytes() > admission_before);
        let mut published = false;
        for _ in 0..16 {
            store.checkpoint(&catalog).unwrap();
            let report = store.columnar_shadow_checkpoint_report().unwrap();
            match report.status {
                ColumnarShadowCheckpointStatus::Published => {
                    published = true;
                    break;
                }
                ColumnarShadowCheckpointStatus::Failed { .. } => {
                    let grown = fs::metadata(shadow_root.join(SHADOW_KEY_DICTIONARY_FILE))
                        .unwrap()
                        .len();
                    assert!(
                        grown > dictionary_bytes,
                        "a failed attempt must make monotone dictionary progress"
                    );
                    dictionary_bytes = grown;
                }
            }
        }
        assert!(published, "bounded attempts must converge to publication");
        assert_shadow_equivalence(&root, &store);
        fs::remove_dir_all(root).unwrap();
    }
}
