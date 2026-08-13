//! Columnar shadow double-write for [`GraphStore`] checkpoints (spec §3.7).
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
//! one-row edit rewrites its entire table until C4's delta groups land.
//! Untouched tables reuse their previous directory references without
//! rebuilding bytes (§3.6.5). Memory, by contrast, is bounded regardless
//! of table size: rows stream through per-table group buffers under one
//! global byte budget, flushing short groups when the budget fills.

use super::*;
use skein_integrity::crc32c;
use skein_storage::{
    durable_replace_file, encode_residual_row_properties, ColumnGroupArtifactDescriptor,
    ColumnGroupError, ColumnGroupManifest, ColumnGroupTableDirectory, ColumnGroupTableDirectoryRef,
    ColumnGroupTableKey, ColumnGroupTableKind, ColumnGroupWriter, PublishedColumnGroupCatalog,
    DEFAULT_GROUP_ROW_CAPACITY,
};

/// Subdirectory of the database root holding the self-contained shadow.
pub const COLUMN_GROUP_SHADOW_DIR: &str = "column-groups";
/// Persistent shadow key dictionary inside the shadow directory.
const SHADOW_KEY_DICTIONARY_FILE: &str = "property-keys.skein";
const SHADOW_KEY_DICTIONARY_MAGIC: &[u8; 9] = b"SKNSHKEY1";
const SHADOW_KEY_DICTIONARY_VERSION: u32 = 1;
const MAX_SHADOW_KEY_DICTIONARY_BYTES: u64 = 64 * 1024 * 1024;

/// Reserved column id of the node label-set blob column.
const LABEL_SET_COLUMN: PropertyId = PropertyId(0);
/// Reserved column id of the relationship source endpoint column.
const SOURCE_COLUMN: PropertyId = PropertyId(1);
/// Reserved column id of the relationship target endpoint column.
const TARGET_COLUMN: PropertyId = PropertyId(2);
/// Reserved column id of the residual blob column.
const RESIDUAL_COLUMN: PropertyId = PropertyId(3);
/// First shadow key-dictionary id; everything below is reserved.
const FIRST_DICTIONARY_COLUMN: u32 = 4;
/// Default global byte budget across all in-flight shadow group buffers.
const DEFAULT_SHADOW_BUFFER_BUDGET_BYTES: u64 = 64 * 1024 * 1024;
/// Fixed per-row overhead charged against the buffer budget.
const SHADOW_ROW_OVERHEAD_BYTES: u64 = 16;

/// Outcome of the shadow double-write attempted by one checkpoint. The
/// canonical checkpoint's `Result` reflects canonical publication only; a
/// shadow failure lands here instead of failing the checkpoint call, and
/// the preserved dirty state makes the next checkpoint retry. A disabled
/// shadow has no report at all (`columnar_shadow_checkpoint_report()`
/// returns `None`), so no `Disabled` variant exists.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ColumnarShadowCheckpointStatus {
    /// The shadow manifest for this checkpoint's epoch was published.
    #[default]
    Published,
    /// The shadow build or publication failed after the canonical
    /// checkpoint succeeded; dirty state is preserved for the retry.
    Failed { error: String },
}

/// Write-amplification evidence for one shadow checkpoint, in the style of
/// the existing storage reports. A `Failed` report carries only the status
/// and source epoch; its remaining counters stay zero.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnarShadowCheckpointReport {
    /// Whether the shadow published or failed for this checkpoint.
    pub status: ColumnarShadowCheckpointStatus,
    /// Shadow manifest generation this checkpoint published.
    pub generation: u64,
    /// Storage commit epoch the checkpoint publishes (§3.6.1).
    pub source_commit_epoch: u64,
    /// Tables referenced by the published shadow manifest.
    pub table_count: usize,
    /// Tables rebuilt because they were dirty since the previous checkpoint.
    pub dirty_table_count: usize,
    /// Untouched tables whose directory references were reused byte-for-byte.
    pub reused_table_count: usize,
    /// Immutable column-group artifact bytes written by this checkpoint.
    pub group_bytes_written: u64,
    /// Table-directory, key-dictionary, and manifest bytes written.
    pub metadata_bytes_written: u64,
    /// Largest total held across all group buffers at any point of the
    /// build; bounded by the shadow buffer budget (spec §8 discipline).
    pub peak_buffered_bytes: u64,
    /// Column groups flushed by this checkpoint, including budget-driven
    /// short groups (the group row capacity is a maximum, not a minimum).
    pub flushed_group_count: usize,
    /// Wall-clock time spent building and publishing the shadow.
    pub elapsed_micros: u64,
}

/// What recovery observed about the shadow catalog when the flag is on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnarShadowRecoveryStatus {
    /// The published shadow catalog opened and validated cleanly.
    pub validated: bool,
    /// A corrupt shadow was discarded (rebuildable derived state, like a
    /// corrupt projected-graph artifact); the next checkpoint rebuilds it.
    pub discarded: bool,
    /// Validation error of the discarded shadow, when any.
    pub error: Option<String>,
}

/// Shadow bookkeeping carried by [`GraphStore`].
#[derive(Debug, Clone)]
pub(super) struct ColumnarShadowState {
    /// The config flag; when off, every shadow hook is a no-op.
    pub(super) enabled: bool,
    /// All tables must be rebuilt at the next checkpoint (first checkpoint,
    /// discarded shadow, or a shadow behind the recovered checkpoint).
    pub(super) all_dirty: bool,
    /// Tables touched by mutations since the last successful shadow publish.
    pub(super) dirty: BTreeSet<ColumnGroupTableKey>,
    /// The active published shadow catalog, for untouched-table reuse.
    pub(super) catalog: Option<PublishedColumnGroupCatalog>,
    /// Global byte budget across all in-flight group buffers during a
    /// shadow build; exceeding it flushes the largest buffer as a short
    /// group.
    pub(super) buffer_budget_bytes: u64,
    pub(super) recovery: ColumnarShadowRecoveryStatus,
    pub(super) report: Option<ColumnarShadowCheckpointReport>,
}

impl Default for ColumnarShadowState {
    fn default() -> Self {
        Self {
            enabled: false,
            all_dirty: false,
            dirty: BTreeSet::new(),
            catalog: None,
            buffer_budget_bytes: DEFAULT_SHADOW_BUFFER_BUDGET_BYTES,
            recovery: ColumnarShadowRecoveryStatus::default(),
            report: None,
        }
    }
}

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

// --- shadow key dictionary --------------------------------------------------

/// The shadow's persistent property-key interning: id = `4 + index` in
/// first-seen order, ids never reused or reordered. The file is rewritten
/// whole (temp file, fsync, atomic rename) but its content only ever grows
/// by appending keys, so older generations keep decoding.
#[derive(Debug, Default)]
struct ShadowKeyDictionary {
    ids: BTreeMap<String, u32>,
    keys: Vec<String>,
}

impl ShadowKeyDictionary {
    fn load(shadow_root: &Path) -> Result<Self> {
        let path = shadow_root.join(SHADOW_KEY_DICTIONARY_FILE);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default())
            }
            Err(error) => return Err(error.into()),
        };
        Self::decode(&bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let corrupt = |message: &str| {
            SkeinError::Storage(format!("columnar shadow key dictionary {message}"))
        };
        if bytes.len() as u64 > MAX_SHADOW_KEY_DICTIONARY_BYTES {
            return Err(corrupt("exceeds its size limit"));
        }
        let magic = SHADOW_KEY_DICTIONARY_MAGIC.len();
        let footer = 8 + 4 + magic;
        if bytes.len() < magic + footer
            || &bytes[..magic] != SHADOW_KEY_DICTIONARY_MAGIC
            || &bytes[bytes.len() - magic..] != SHADOW_KEY_DICTIONARY_MAGIC
        {
            return Err(corrupt("framing is invalid"));
        }
        let body = &bytes[magic..bytes.len() - footer];
        let footer_bytes = &bytes[bytes.len() - footer..bytes.len() - magic];
        let stored_len = u64::from_le_bytes(footer_bytes[..8].try_into().expect("8 bytes"));
        let stored_crc = u32::from_le_bytes(footer_bytes[8..12].try_into().expect("4 bytes"));
        if stored_len != body.len() as u64 || crc32c(body).get() != stored_crc {
            return Err(corrupt("checksum or length is invalid"));
        }
        let mut position = 0usize;
        let read_u32 = |position: &mut usize| -> Result<u32> {
            let end = position
                .checked_add(4)
                .filter(|end| *end <= body.len())
                .ok_or_else(|| corrupt("is truncated"))?;
            let value = u32::from_le_bytes(body[*position..end].try_into().expect("4 bytes"));
            *position = end;
            Ok(value)
        };
        if read_u32(&mut position)? != SHADOW_KEY_DICTIONARY_VERSION {
            return Err(corrupt("has an unsupported version"));
        }
        let count = read_u32(&mut position)? as usize;
        let mut dictionary = Self::default();
        for _ in 0..count {
            let length = read_u32(&mut position)? as usize;
            let end = position
                .checked_add(length)
                .filter(|end| *end <= body.len())
                .ok_or_else(|| corrupt("is truncated"))?;
            let key = std::str::from_utf8(&body[position..end])
                .map_err(|_| corrupt("holds a non-UTF-8 key"))?
                .to_string();
            position = end;
            let id = FIRST_DICTIONARY_COLUMN + dictionary.keys.len() as u32;
            if dictionary.ids.insert(key.clone(), id).is_some() {
                return Err(corrupt("repeats a key"));
            }
            dictionary.keys.push(key);
        }
        if position != body.len() {
            return Err(corrupt("has trailing bytes"));
        }
        Ok(dictionary)
    }

    fn len(&self) -> usize {
        self.keys.len()
    }

    fn intern(&mut self, key: &str) -> Result<PropertyId> {
        if let Some(id) = self.ids.get(key) {
            return Ok(PropertyId(*id));
        }
        let id = u32::try_from(self.keys.len())
            .ok()
            .and_then(|index| index.checked_add(FIRST_DICTIONARY_COLUMN))
            .ok_or_else(|| {
                SkeinError::Storage(
                    "columnar shadow key dictionary exceeds the u32 id space".to_string(),
                )
            })?;
        self.ids.insert(key.to_string(), id);
        self.keys.push(key.to_string());
        Ok(PropertyId(id))
    }

    #[cfg(test)]
    fn key(&self, id: PropertyId) -> Option<&str> {
        id.0.checked_sub(FIRST_DICTIONARY_COLUMN)
            .and_then(|index| self.keys.get(index as usize))
            .map(String::as_str)
    }

    fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend(SHADOW_KEY_DICTIONARY_VERSION.to_le_bytes());
        body.extend((self.keys.len() as u32).to_le_bytes());
        for key in &self.keys {
            body.extend((key.len() as u32).to_le_bytes());
            body.extend(key.as_bytes());
        }
        let mut bytes = Vec::with_capacity(2 * SHADOW_KEY_DICTIONARY_MAGIC.len() + 12 + body.len());
        bytes.extend(SHADOW_KEY_DICTIONARY_MAGIC);
        bytes.extend(&body);
        bytes.extend((body.len() as u64).to_le_bytes());
        bytes.extend(crc32c(&body).get().to_le_bytes());
        bytes.extend(SHADOW_KEY_DICTIONARY_MAGIC);
        bytes
    }

    /// Publishes the dictionary durably: temp file, fsync, atomic rename
    /// (never truncating or syncing an already-published handle). Returns
    /// the bytes written.
    fn persist(&self, shadow_root: &Path) -> Result<u64> {
        let bytes = self.encode();
        if bytes.len() as u64 > MAX_SHADOW_KEY_DICTIONARY_BYTES {
            return Err(SkeinError::Storage(
                "columnar shadow key dictionary exceeds its size limit".to_string(),
            ));
        }
        let path = shadow_root.join(SHADOW_KEY_DICTIONARY_FILE);
        let tmp_path = shadow_root.join(format!(".{SHADOW_KEY_DICTIONARY_FILE}.tmp"));
        let result = (|| -> Result<()> {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            durable_replace_file(&tmp_path, &path)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&tmp_path);
        }
        result.map(|()| bytes.len() as u64)
    }
}

// --- per-table build --------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InferredType {
    Int,
    Float,
    Bool,
    Str,
    Residual,
}

impl InferredType {
    fn of(value: &Value) -> Self {
        match value {
            Value::Int(_) => Self::Int,
            Value::Float(_) => Self::Float,
            Value::Bool(_) => Self::Bool,
            Value::String(_) => Self::Str,
            Value::Null | Value::List(_) | Value::Map(_) => Self::Residual,
        }
    }

    fn merge(self, other: Self) -> Self {
        if self == other {
            self
        } else {
            Self::Residual
        }
    }
}

/// Per-table pass-1 accumulator: O(1) state per property (current
/// type-lattice point), never buffered rows.
#[derive(Debug, Default)]
struct TablePropertyTypes {
    inferred: BTreeMap<String, InferredType>,
}

impl TablePropertyTypes {
    fn observe(&mut self, properties: &BTreeMap<String, Value>) {
        for (key, value) in properties {
            let observed = InferredType::of(value);
            self.inferred
                .entry(key.clone())
                .and_modify(|current| *current = current.merge(observed))
                .or_insert(observed);
        }
    }
}

/// The fixed column layout of one dirty table for this generation, derived
/// from pass 1 before any row is buffered.
struct ShadowTableLayout {
    /// Typed columns, sorted by key: `(shadow column id, property key)`.
    typed: Vec<(PropertyId, String)>,
}

fn shadow_table_layout(
    types: &TablePropertyTypes,
    dictionary: &mut ShadowKeyDictionary,
) -> Result<ShadowTableLayout> {
    let mut typed = Vec::new();
    for (key, inferred) in &types.inferred {
        if *inferred != InferredType::Residual {
            typed.push((dictionary.intern(key)?, key.clone()));
        }
    }
    Ok(ShadowTableLayout { typed })
}

/// Rough resident-byte estimate of one buffered value, mirroring the
/// existing record estimators' spirit: enough to keep the budget honest,
/// never exact.
fn estimated_shadow_value_bytes(value: &Value) -> u64 {
    match value {
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) => 16,
        Value::String(value) => 16 + value.len() as u64,
        Value::List(values) => 16 + values.iter().map(estimated_shadow_value_bytes).sum::<u64>(),
        Value::Map(entries) => {
            16 + entries
                .iter()
                .map(|(key, value)| key.len() as u64 + estimated_shadow_value_bytes(value))
                .sum::<u64>()
        }
    }
}

/// Requests background admission for one group flush from the engine's
/// runtime governor (`WorkClass::Shadow` maps to background `Control`
/// work, mirroring `runtime_work_request_with_capacity`). Retryable
/// rejections back off briefly; persistent denial fails the shadow build,
/// which the checkpoint records as a failed shadow and retries later.
/// Without a threaded governor (plain `GraphStore` opens) the flush
/// proceeds unmetered.
fn admit_shadow_flush(
    governor: Option<&skein_qos::RuntimeGovernor>,
    flush_bytes: u64,
) -> Result<Option<skein_qos::RuntimePermit>> {
    const RETRY_LIMIT: u32 = 200;
    const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(5);
    let Some(governor) = governor else {
        return Ok(None);
    };
    let request = skein_qos::RuntimeWorkRequest {
        priority: skein_qos::RuntimeWorkPriority::Background,
        kind: skein_qos::RuntimeWorkKind::Control,
        cpu_slots: 1,
        memory_bytes: flush_bytes,
        io_slots: 1,
        result_bytes: 0,
        blocking: false,
    };
    let mut attempt = 0;
    loop {
        match governor.try_admit(request) {
            Ok(permit) => return Ok(Some(permit)),
            Err(error) if error.is_retryable() && attempt < RETRY_LIMIT => {
                attempt += 1;
                std::thread::sleep(RETRY_DELAY);
            }
            Err(error) => {
                return Err(SkeinError::Storage(format!(
                    "columnar shadow flush admission denied: {error}"
                )));
            }
        }
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
    peak_buffered_bytes: u64,
    flushed_group_count: usize,
}

/// Streaming pass-2 builder: appends rows into per-table bounded group
/// buffers under one global byte budget. A full buffer flushes as a
/// complete group; exceeding the budget flushes the largest buffer as a
/// shorter group. Every flush requests governor admission first.
struct ShadowCheckpointBuilder {
    shadow_root: PathBuf,
    generation: ManifestGeneration,
    governor: Option<skein_qos::RuntimeGovernor>,
    buffer_budget_bytes: u64,
    writer: ColumnGroupWriter,
    layouts: BTreeMap<ColumnGroupTableKey, ShadowTableLayout>,
    buffers: BTreeMap<ColumnGroupTableKey, ShadowGroupBuffer>,
    descriptors: BTreeMap<ColumnGroupTableKey, Vec<ColumnGroupArtifactDescriptor>>,
    next_group_index: BTreeMap<ColumnGroupTableKey, u64>,
    buffered_bytes: u64,
    peak_buffered_bytes: u64,
    flushed_group_count: usize,
    group_bytes_written: u64,
}

impl ShadowCheckpointBuilder {
    fn new(
        shadow_root: PathBuf,
        generation: ManifestGeneration,
        governor: Option<skein_qos::RuntimeGovernor>,
        buffer_budget_bytes: u64,
        layouts: BTreeMap<ColumnGroupTableKey, ShadowTableLayout>,
    ) -> Self {
        Self {
            shadow_root,
            generation,
            governor,
            buffer_budget_bytes,
            writer: ColumnGroupWriter::default(),
            layouts,
            buffers: BTreeMap::new(),
            descriptors: BTreeMap::new(),
            next_group_index: BTreeMap::new(),
            buffered_bytes: 0,
            peak_buffered_bytes: 0,
            flushed_group_count: 0,
            group_bytes_written: 0,
        }
    }

    fn append_node(
        &mut self,
        dictionary: &mut ShadowKeyDictionary,
        node: NodeRecord,
    ) -> Result<()> {
        let key = node_table_key(&node.labels);
        let label_set = encode_label_set(&node.labels);
        self.append_row(
            dictionary,
            key,
            node.id.0,
            Some(label_set),
            None,
            &node.properties,
        )
    }

    fn append_relationship(
        &mut self,
        dictionary: &mut ShadowKeyDictionary,
        relationship: RelRecord,
    ) -> Result<()> {
        let key = relationship_table_key(relationship.rel_type);
        self.append_row(
            dictionary,
            key,
            relationship.id.0,
            None,
            Some((relationship.source.0, relationship.target.0)),
            &relationship.properties,
        )
    }

    fn append_row(
        &mut self,
        dictionary: &mut ShadowKeyDictionary,
        table: ColumnGroupTableKey,
        id: u64,
        label_set: Option<Vec<u8>>,
        endpoints: Option<(u64, u64)>,
        properties: &BTreeMap<String, Value>,
    ) -> Result<()> {
        // Materialize the row's column cells first so its cost is known
        // before it is admitted against the budget.
        let layout_typed_len = self.layouts[&table].typed.len();
        let mut typed_cells = Vec::with_capacity(layout_typed_len);
        let mut row_bytes = SHADOW_ROW_OVERHEAD_BYTES;
        for index in 0..layout_typed_len {
            let key = &self.layouts[&table].typed[index].1;
            let cell = properties.get(key).cloned().unwrap_or(Value::Null);
            row_bytes = row_bytes.saturating_add(estimated_shadow_value_bytes(&cell));
            typed_cells.push(cell);
        }
        let mut residual_entries = Vec::new();
        for (key, value) in properties {
            if self.layouts[&table]
                .typed
                .iter()
                .any(|(_, typed_key)| typed_key == key)
            {
                continue;
            }
            residual_entries.push((dictionary.intern(key)?.0, value));
        }
        let residual = if residual_entries.is_empty() {
            None
        } else {
            residual_entries.sort_by_key(|(key_id, _)| *key_id);
            Some(
                encode_residual_row_properties(&residual_entries)
                    .map_err(|error| SkeinError::Storage(error.to_string()))?,
            )
        };
        row_bytes = row_bytes
            .saturating_add(residual.as_ref().map_or(0, |blob| blob.len() as u64))
            .saturating_add(label_set.as_ref().map_or(0, |blob| blob.len() as u64));

        // Budget admission: flush the largest buffer as a shorter group
        // until this row fits. A row larger than the whole budget is the
        // sole occupant of an otherwise empty buffer set.
        while self.buffered_bytes > 0
            && self.buffered_bytes.saturating_add(row_bytes) > self.buffer_budget_bytes
        {
            self.flush_largest_buffer()?;
        }

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
        self.peak_buffered_bytes = self.peak_buffered_bytes.max(self.buffered_bytes);
        if full {
            self.flush_table(table)?;
        }
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
    /// than the group row capacity), gated by governor admission.
    fn flush_table(&mut self, table: ColumnGroupTableKey) -> Result<()> {
        let Some(buffer) = self.buffers.remove(&table) else {
            return Ok(());
        };
        if buffer.ids.is_empty() {
            return Ok(());
        }
        let _permit = admit_shadow_flush(self.governor.as_ref(), buffer.estimated_bytes)?;
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
            .typed
            .iter()
            .map(|(property_id, _)| *property_id)
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
            peak_buffered_bytes: self.peak_buffered_bytes,
            flushed_group_count: self.flushed_group_count,
        })
    }
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
    /// projected-graph artifacts (spec §3.7.3).
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
                Some(catalog) => ShadowKeyDictionary::load(&shadow_root)
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

    /// Runs the shadow double-write for a just-published canonical
    /// checkpoint and records the outcome. The canonical checkpoint's
    /// `Result` reflects canonical publication only: a shadow failure here
    /// never fails the checkpoint call — it lands in the report as
    /// [`ColumnarShadowCheckpointStatus::Failed`] with the dirty state
    /// preserved (never cleared on failure), so the next checkpoint
    /// retries and converges.
    pub(super) fn record_columnar_shadow_checkpoint(&mut self, source_commit_epoch: u64) {
        if !self.columnar_shadow.enabled {
            return;
        }
        if let Err(error) = self.publish_columnar_shadow_checkpoint(source_commit_epoch) {
            self.columnar_shadow.report = Some(ColumnarShadowCheckpointReport {
                status: ColumnarShadowCheckpointStatus::Failed {
                    error: error.to_string(),
                },
                source_commit_epoch,
                ..ColumnarShadowCheckpointReport::default()
            });
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
    fn publish_columnar_shadow_checkpoint(&mut self, source_commit_epoch: u64) -> Result<()> {
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
            // No mounted catalog means anything on disk is stale garbage
            // (for example a shadow discarded at recovery in a read-only
            // process): restart the shadow's generation sequence cleanly.
            fs::remove_dir_all(&shadow_root)?;
        }
        fs::create_dir_all(&shadow_root)?;
        let mut dictionary = ShadowKeyDictionary::load(&shadow_root)?;
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

        // Pass 1: stream the canonical scan accumulating only per-table
        // per-property type-lattice state (O(1) per property), fixing each
        // dirty table's column layout before any row is buffered. Dirty
        // tables with zero remaining rows never appear here and are dropped
        // from the manifest instead of publishing empty directories.
        let mut table_types: BTreeMap<ColumnGroupTableKey, TablePropertyTypes> = BTreeMap::new();
        for record in self.node_records_owned() {
            let node = record?;
            let key = node_table_key(&node.labels);
            if is_dirty(key) {
                table_types
                    .entry(key)
                    .or_default()
                    .observe(&node.properties);
            }
        }
        for record in self.relationship_records_owned() {
            let relationship = record?;
            let key = relationship_table_key(relationship.rel_type);
            if is_dirty(key) {
                table_types
                    .entry(key)
                    .or_default()
                    .observe(&relationship.properties);
            }
        }
        let mut layouts = BTreeMap::new();
        for (table, types) in &table_types {
            layouts.insert(*table, shadow_table_layout(types, &mut dictionary)?);
        }

        let parent_generation = previous
            .as_ref()
            .map(|catalog| catalog.manifest().generation());
        let generation = ManifestGeneration(parent_generation.map_or(1, |parent| parent.0 + 1));

        // Pass 2: stream again, appending rows into bounded per-table group
        // buffers under the global byte budget; every flush requests
        // governor admission.
        let mut builder = ShadowCheckpointBuilder::new(
            shadow_root.clone(),
            generation,
            self.runtime_governor.clone(),
            self.columnar_shadow.buffer_budget_bytes,
            layouts,
        );
        for record in self.node_records_owned() {
            let node = record?;
            if is_dirty(node_table_key(&node.labels)) {
                builder.append_node(&mut dictionary, node)?;
            }
        }
        for record in self.relationship_records_owned() {
            let relationship = record?;
            if is_dirty(relationship_table_key(relationship.rel_type)) {
                builder.append_relationship(&mut dictionary, relationship)?;
            }
        }
        let built = builder.finish()?;

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
            metadata_bytes_written =
                metadata_bytes_written.saturating_add(dictionary.persist(&shadow_root)?);
        }

        let manifest =
            ColumnGroupManifest::new(generation, parent_generation, source_commit_epoch, tables)
                .map_err(shadow_error)?;
        let table_count = manifest.tables().len();
        let catalog = manifest.publish(&shadow_root).map_err(shadow_error)?;
        metadata_bytes_written = metadata_bytes_written.saturating_add(
            fs::metadata(shadow_root.join(skein_storage::COLUMN_GROUP_MANIFEST_FILE))?.len(),
        );

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
            peak_buffered_bytes: built.peak_buffered_bytes,
            flushed_group_count: built.flushed_group_count,
            elapsed_micros: u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let dictionary = ShadowKeyDictionary::load(&shadow_root).unwrap();
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
                let residual = reader.read_byte_column(RESIDUAL_COLUMN).unwrap();
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

        store.checkpoint(&catalog).unwrap();
        let report = store.columnar_shadow_checkpoint_report().unwrap();
        assert!(
            report.peak_buffered_bytes <= BUDGET,
            "peak buffered bytes {} exceed the {BUDGET} byte budget",
            report.peak_buffered_bytes
        );
        assert!(report.peak_buffered_bytes > 0);
        // The data volume is far beyond one budget's worth, so the build
        // must have flushed many budget-driven short groups.
        assert!(report.group_bytes_written > BUDGET);
        assert!(
            report.flushed_group_count > report.table_count,
            "expected budget-driven short groups beyond one per table, got {}",
            report.flushed_group_count
        );
        // Short groups are legal (row capacity is a max, not a min) and the
        // multi-group reconstruction still matches the canonical scan.
        assert_shadow_equivalence(&root, &store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn shadow_flushes_request_admission_from_the_threaded_governor() {
        let root = unique_shadow_dir("governor");
        let mut catalog = Catalog::default();
        let mut store = open_shadow_store(&root, &mut catalog);
        let governor = skein_qos::RuntimeGovernor::detect(
            skein_qos::RuntimeGovernorConfig::desktop_bound(),
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
        assert!(report.flushed_group_count > 0);
        let admissions_after = governor.snapshot().admissions;
        assert!(
            admissions_after >= admissions_before + report.flushed_group_count as u64,
            "every group flush requests one background admission \
             ({admissions_before} -> {admissions_after}, {} flushes)",
            report.flushed_group_count
        );
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

    #[test]
    fn shadow_key_dictionary_persists_append_only_and_fails_closed_on_corruption() {
        let root = unique_shadow_dir("dictionary");
        fs::create_dir_all(&root).unwrap();
        let mut dictionary = ShadowKeyDictionary::load(&root).unwrap();
        assert_eq!(dictionary.len(), 0);
        let name = dictionary.intern("name").unwrap();
        let age = dictionary.intern("age").unwrap();
        assert_eq!(name, PropertyId(FIRST_DICTIONARY_COLUMN));
        assert_eq!(age, PropertyId(FIRST_DICTIONARY_COLUMN + 1));
        assert_eq!(dictionary.intern("name").unwrap(), name);
        dictionary.persist(&root).unwrap();

        let mut reloaded = ShadowKeyDictionary::load(&root).unwrap();
        assert_eq!(reloaded.key(name), Some("name"));
        assert_eq!(reloaded.key(age), Some("age"));
        assert_eq!(reloaded.key(PropertyId(0)), None);
        // Ids are stable across reload-and-extend.
        assert_eq!(reloaded.intern("age").unwrap(), age);
        assert_eq!(
            reloaded.intern("score").unwrap(),
            PropertyId(FIRST_DICTIONARY_COLUMN + 2)
        );

        let path = root.join(SHADOW_KEY_DICTIONARY_FILE);
        let mut bytes = fs::read(&path).unwrap();
        let flip = bytes.len() / 2;
        bytes[flip] ^= 0x01;
        fs::write(&path, &bytes).unwrap();
        assert!(ShadowKeyDictionary::load(&root).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
