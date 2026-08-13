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
