//! Columnar shadow double-write for [`GraphStore`] checkpoints (spec §3.7).
//!
//! With `columnar_shadow_checkpoint` on, every checkpoint additionally
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
//! - `column id 3`: residual blob column — the canonical
//!   `(interned key id, tagged value)` row codec from `skein-storage`'s
//!   canonical module; no second value encoding exists.
//! - `column id >= 4`: typed property columns, id = shadow dictionary id.
//!
//! Column typing is per-generation inference: a property key gets a typed
//! column in a table's snapshot iff every occurrence in that table has the
//! same scalar type (Int/Float/Bool/String). Mixed-type, List, Map, and
//! Null occurrences send the key to the residual column. Each generation's
//! directories are self-contained, so this inference is deterministic and
//! sound without cross-generation schema state.

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

/// Write-amplification evidence for one shadow checkpoint, in the style of
/// the existing storage reports.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnarShadowCheckpointReport {
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
#[derive(Debug, Clone, Default)]
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
    pub(super) recovery: ColumnarShadowRecoveryStatus,
    pub(super) report: Option<ColumnarShadowCheckpointReport>,
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

/// Per-generation type inference over one table snapshot: a key is typed
/// iff every occurrence has one scalar type; anything else is residual.
fn infer_typed_keys<'a>(
    property_maps: impl Iterator<Item = &'a BTreeMap<String, Value>>,
) -> BTreeMap<String, InferredType> {
    let mut inferred: BTreeMap<String, InferredType> = BTreeMap::new();
    for properties in property_maps {
        for (key, value) in properties {
            let observed = InferredType::of(value);
            inferred
                .entry(key.clone())
                .and_modify(|current| *current = current.merge(observed))
                .or_insert(observed);
        }
    }
    inferred
}

struct BuiltTable {
    reference: ColumnGroupTableDirectoryRef,
    group_bytes: u64,
    directory_bytes: u64,
}

struct TableRow {
    id: u64,
    /// Label-set blob for node tables, `None` for relationship tables.
    label_set: Option<Vec<u8>>,
    /// `(source, target)` endpoints for relationship tables.
    endpoints: Option<(u64, u64)>,
    properties: BTreeMap<String, Value>,
}

fn build_table(
    shadow_root: &Path,
    table: ColumnGroupTableKey,
    generation: ManifestGeneration,
    rows: &[TableRow],
    dictionary: &mut ShadowKeyDictionary,
) -> Result<BuiltTable> {
    let typed_keys = infer_typed_keys(rows.iter().map(|row| &row.properties))
        .into_iter()
        .filter(|(_, inferred)| *inferred != InferredType::Residual)
        .map(|(key, _)| key)
        .collect::<Vec<_>>();
    let mut typed_columns = Vec::with_capacity(typed_keys.len());
    for key in &typed_keys {
        typed_columns.push((dictionary.intern(key)?, key.as_str()));
    }
    let typed_key_set = typed_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();

    let kind_tag = match table.kind {
        ColumnGroupTableKind::Node => "node",
        ColumnGroupTableKind::Relationship => "relationship",
        ColumnGroupTableKind::Relational => {
            return Err(SkeinError::Storage(
                "columnar shadow does not cover relational tables".to_string(),
            ))
        }
    };
    let writer = ColumnGroupWriter::default();
    let mut descriptors = Vec::new();
    let mut group_bytes = 0u64;
    for (group_index, chunk) in rows.chunks(DEFAULT_GROUP_ROW_CAPACITY as usize).enumerate() {
        let ids = chunk.iter().map(|row| row.id).collect::<Vec<_>>();
        let mut value_columns: Vec<(PropertyId, Vec<Value>)> = Vec::new();
        for (property_id, key) in &typed_columns {
            let values = chunk
                .iter()
                .map(|row| row.properties.get(*key).cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>();
            value_columns.push((*property_id, values));
        }
        if table.kind == ColumnGroupTableKind::Relationship {
            // u64 endpoints ride plain integer columns bit-preserving:
            // `id as i64` on write, `value as u64` on read.
            value_columns.push((
                SOURCE_COLUMN,
                chunk
                    .iter()
                    .map(|row| Value::Int(row.endpoints.expect("relationship row").0 as i64))
                    .collect(),
            ));
            value_columns.push((
                TARGET_COLUMN,
                chunk
                    .iter()
                    .map(|row| Value::Int(row.endpoints.expect("relationship row").1 as i64))
                    .collect(),
            ));
        }
        let mut byte_columns: Vec<(PropertyId, Vec<Option<Vec<u8>>>)> = Vec::new();
        if table.kind == ColumnGroupTableKind::Node {
            byte_columns.push((
                LABEL_SET_COLUMN,
                chunk
                    .iter()
                    .map(|row| row.label_set.clone())
                    .collect::<Vec<_>>(),
            ));
        }
        let mut residual_rows = Vec::with_capacity(chunk.len());
        for row in chunk {
            let mut entries = Vec::new();
            for (key, value) in &row.properties {
                if typed_key_set.contains(key.as_str()) {
                    continue;
                }
                entries.push((dictionary.intern(key)?.0, value));
            }
            if entries.is_empty() {
                residual_rows.push(None);
            } else {
                entries.sort_by_key(|(key_id, _)| *key_id);
                residual_rows.push(Some(
                    encode_residual_row_properties(&entries)
                        .map_err(|error| SkeinError::Storage(error.to_string()))?,
                ));
            }
        }
        byte_columns.push((RESIDUAL_COLUMN, residual_rows));

        let file_name = format!(
            "group-{kind_tag}-{}-{}-{}.skein",
            table.table_id, generation.0, group_index
        );
        let path = shadow_root.join(&file_name);
        writer
            .write_with_byte_columns(
                &path,
                group_index as u64,
                generation,
                &ids,
                &value_columns,
                &byte_columns,
            )
            .map_err(shadow_error)?;
        group_bytes = group_bytes.saturating_add(fs::metadata(&path)?.len());
        descriptors.push(
            ColumnGroupArtifactDescriptor::inspect(shadow_root, file_name, None)
                .map_err(shadow_error)?,
        );
    }

    let directory =
        ColumnGroupTableDirectory::new(table, generation, descriptors).map_err(shadow_error)?;
    // A crashed earlier publication attempt of this same (unpublished)
    // generation may have left an immutable directory file with different
    // bytes; the active manifest never references the candidate generation,
    // so the orphan is garbage and safe to drop before rewriting.
    let _ = fs::remove_file(shadow_root.join(directory.file_name()));
    let reference = directory
        .write_immutable(shadow_root)
        .map_err(shadow_error)?;
    let directory_bytes = fs::metadata(shadow_root.join(reference.file_name()))?.len();
    Ok(BuiltTable {
        reference,
        group_bytes,
        directory_bytes,
    })
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
                    fs::remove_dir_all(&shadow_root)?;
                }
                self.columnar_shadow.recovery.discarded = true;
                self.columnar_shadow.recovery.error = Some(error);
            }
        }
        Ok(())
    }

    /// Builds and publishes the shadow for a just-published checkpoint.
    /// Untouched tables reuse their previous directory references without
    /// rebuilding bytes (§3.6.5); dirty tables are rebuilt whole. Durable
    /// order: immutable groups -> immutable table directories -> key
    /// dictionary -> manifest replace, all through temp-file/fsync/rename
    /// publication.
    pub(super) fn publish_columnar_shadow_checkpoint(
        &mut self,
        source_commit_epoch: u64,
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

        let mut dirty_tables: BTreeMap<ColumnGroupTableKey, Vec<TableRow>> = BTreeMap::new();
        for record in self.node_records_owned() {
            let node = record?;
            let key = node_table_key(&node.labels);
            if !is_dirty(key) {
                continue;
            }
            dirty_tables.entry(key).or_default().push(TableRow {
                id: node.id.0,
                label_set: Some(encode_label_set(&node.labels)),
                endpoints: None,
                properties: node.properties,
            });
        }
        for record in self.relationship_records_owned() {
            let relationship = record?;
            let key = relationship_table_key(relationship.rel_type);
            if !is_dirty(key) {
                continue;
            }
            dirty_tables.entry(key).or_default().push(TableRow {
                id: relationship.id.0,
                label_set: None,
                endpoints: Some((relationship.source.0, relationship.target.0)),
                properties: relationship.properties,
            });
        }

        let parent_generation = previous
            .as_ref()
            .map(|catalog| catalog.manifest().generation());
        let generation = ManifestGeneration(parent_generation.map_or(1, |parent| parent.0 + 1));

        let mut tables = Vec::new();
        let mut dirty_table_count = 0usize;
        let mut reused_table_count = 0usize;
        let mut group_bytes_written = 0u64;
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
        for (table, rows) in &dirty_tables {
            let built = build_table(&shadow_root, *table, generation, rows, &mut dictionary)?;
            group_bytes_written = group_bytes_written.saturating_add(built.group_bytes);
            metadata_bytes_written = metadata_bytes_written.saturating_add(built.directory_bytes);
            tables.push(built.reference);
            dirty_table_count += 1;
        }
        // Dirty tables that ended up with zero visible rows are dropped from
        // the manifest entirely rather than published as empty directories.

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
            generation: generation.0,
            source_commit_epoch,
            table_count,
            dirty_table_count,
            reused_table_count,
            group_bytes_written,
            metadata_bytes_written,
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
            columnar_shadow_checkpoint: true,
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
        // The second checkpoint rewrote strictly less than the first even
        // though it added a row: untouched tables cost no bytes.
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
