//! Encoding, decoding, and framing of write-ahead-log records.

pub mod binary;
pub mod frame;
pub mod group_commit;
use crate::text::{
    encode_nullable, encode_properties, encode_property_type, encode_schema_object_state,
    encode_string, encode_string_vec, encode_table_kind, encode_value,
};
pub use crate::wire;
use crate::{NodeId, RelId};
pub use group_commit::{
    WalGroupCommitActivation, WalGroupCommitAdaptiveColdStartEvidence,
    WalGroupCommitAdaptivePolicyEvidence, WalGroupCommitAdaptiveSteadyStateEvidence,
    WalGroupCommitConfig, WalGroupCommitDelayPolicy, WalGroupCommitEvidence,
    WalGroupCommitSnapshot, WalGroupCommitTailLatencyEvidence, WalGroupCommitWaitDecision,
    DEFAULT_WAL_GROUP_COMMIT_MAX_BYTES, DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY,
    DEFAULT_WAL_GROUP_COMMIT_MAX_ENTRIES,
};
use skein_core::Value;
use skein_core::{PropertyType, SchemaObjectState, TableKind};
use skein_core::{Result, SkeinError};
use skein_integrity::{IntegrityHasher, Sha256Digest};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

struct WalQuarantineEntry {
    path: PathBuf,
    encoded_len: u64,
    modified: std::time::SystemTime,
}

pub fn quarantine_corrupt_wal(
    path: &Path,
    generation: u64,
    read_only: bool,
    max_quarantine_bytes: u64,
) -> Result<()> {
    if read_only {
        return Ok(());
    }
    let root = path
        .parent()
        .ok_or_else(|| SkeinError::Storage("WAL path has no database directory".to_string()))?;
    let quarantine_dir = root.join("quarantine");
    fs::create_dir_all(&quarantine_dir)?;
    crate::sync_parent_directory(&quarantine_dir)?;

    let (wal_len, wal_sha256) = wal_file_identity(path)?;
    let quarantine_path = quarantine_dir.join(format!(
        "wal.{generation}.corrupt.{wal_len}.{wal_sha256}.skein"
    ));
    if quarantine_path.exists() && wal_file_identity(&quarantine_path)? != (wal_len, wal_sha256) {
        fs::remove_file(&quarantine_path)?;
    }

    let existing_copy = quarantine_path.exists();
    let incoming_bytes = if existing_copy { 0 } else { wal_len };
    let mut entries = wal_quarantine_entries(&quarantine_dir)?;
    entries.sort_by(|left, right| {
        left.modified
            .cmp(&right.modified)
            .then_with(|| left.path.cmp(&right.path))
    });
    let mut retained_bytes = entries
        .iter()
        .fold(0u64, |total, entry| total.saturating_add(entry.encoded_len));
    for entry in entries {
        if retained_bytes.saturating_add(incoming_bytes) <= max_quarantine_bytes {
            break;
        }
        if existing_copy && entry.path == quarantine_path && wal_len <= max_quarantine_bytes {
            continue;
        }
        fs::remove_file(&entry.path)?;
        retained_bytes = retained_bytes.saturating_sub(entry.encoded_len);
    }

    if wal_len > max_quarantine_bytes {
        crate::sync_parent_directory(&quarantine_path)?;
        return Ok(());
    }
    if existing_copy {
        crate::sync_parent_directory(&quarantine_path)?;
        return Ok(());
    }

    copy_wal_exclusive(path, &quarantine_path, (wal_len, wal_sha256))?;
    crate::sync_parent_directory(&quarantine_path)?;
    Ok(())
}

fn wal_file_identity(path: &Path) -> Result<(u64, Sha256Digest)> {
    let mut file = File::open(path)?;
    let mut integrity = IntegrityHasher::new();
    let mut encoded_len = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        integrity.update(&buffer[..read]);
        encoded_len = encoded_len
            .checked_add(read as u64)
            .ok_or_else(|| SkeinError::Storage("WAL quarantine byte count overflow".to_string()))?;
    }
    Ok((encoded_len, integrity.finish().sha256))
}

fn wal_quarantine_entries(directory: &Path) -> Result<Vec<WalQuarantineEntry>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !file_type.is_file() || !name.starts_with("wal.") || !name.contains(".corrupt.") {
            continue;
        }
        let metadata = entry.metadata()?;
        entries.push(WalQuarantineEntry {
            path: entry.path(),
            encoded_len: metadata.len(),
            modified: metadata
                .modified()
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
        });
    }
    Ok(entries)
}

fn copy_wal_exclusive(
    source: &Path,
    destination: &Path,
    expected_identity: (u64, Sha256Digest),
) -> Result<()> {
    let mut source = File::open(source)?;
    let mut destination_file = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if wal_file_identity(destination)? == expected_identity {
                return Ok(());
            }
            return Err(SkeinError::Storage(
                "concurrent WAL quarantine copy has the wrong identity".to_string(),
            ));
        }
        Err(error) => return Err(error.into()),
    };
    let copy_result = (|| {
        let mut integrity = IntegrityHasher::new();
        let mut encoded_len = 0u64;
        let mut buffer = vec![0u8; 1024 * 1024];
        loop {
            let read = source.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            destination_file.write_all(&buffer[..read])?;
            integrity.update(&buffer[..read]);
            encoded_len = encoded_len.checked_add(read as u64).ok_or_else(|| {
                SkeinError::Storage("WAL quarantine byte count overflow".to_string())
            })?;
        }
        destination_file.sync_all()?;
        if (encoded_len, integrity.finish().sha256) != expected_identity {
            return Err(SkeinError::Storage(
                "WAL changed while its corrupt content was being quarantined".to_string(),
            ));
        }
        Ok(())
    })();
    if copy_result.is_err() {
        drop(destination_file);
        let _ = fs::remove_file(destination);
    }
    copy_result
}

pub fn reject_corrupt_wal_record<T>(
    path: &Path,
    generation: u64,
    read_only: bool,
    max_quarantine_bytes: u64,
    record_start: u64,
    reason: impl std::fmt::Display,
) -> Result<T> {
    quarantine_corrupt_wal(path, generation, read_only, max_quarantine_bytes)?;
    Err(SkeinError::Storage(format!(
        "WAL corruption at byte offset {record_start}: {reason}"
    )))
}

/// One decoded event from a WAL scan. Offsets are absolute
/// file offsets; `encoded_len` covers the framed bytes of the record.
pub enum WalCursorEvent {
    Entry {
        entry: WalEntry,
        start_offset: u64,
        encoded_len: u64,
        payload_len: u64,
        payload_sha256: Sha256Digest,
    },
    /// Damage inside the durable prefix; recovery fails closed.
    Corrupt {
        offset: u64,
        reason: String,
    },
    /// An incomplete record or fragment chain at end of file; repairable
    /// only through the explicit doctor protocol by truncating to
    /// `valid_prefix_len`.
    TornTail {
        valid_prefix_len: u64,
        reason: String,
    },
    Eof,
}

/// Outcome of opening a WAL file and validating its generation header.
pub enum WalOpenOutcome {
    Cursor(WalRecordCursor),
    /// The file has no header at all (it is empty).
    MissingHeader,
    /// The header itself is incomplete at end of file.
    HeaderTorn {
        reason: String,
    },
    /// The header is present but invalid.
    HeaderCorrupt {
        reason: String,
    },
}

/// Reader over the single v1 binary WAL generation format.
pub struct WalRecordCursor {
    generation: u64,
    start_lsn: u64,
    reader: frame::BinaryWalReader<std::io::BufReader<File>>,
}

impl WalRecordCursor {
    pub fn open(path: &Path, max_record_bytes: Option<usize>) -> Result<WalOpenOutcome> {
        let mut reader = std::io::BufReader::new(File::open(path)?);
        let mut header = [0u8; frame::WAL_BINARY_FILE_HEADER_BYTES];
        let mut filled = 0usize;
        while filled < header.len() {
            let read = std::io::Read::read(&mut reader, &mut header[filled..])?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        if filled == 0 {
            return Ok(WalOpenOutcome::MissingHeader);
        }
        if filled < header.len() {
            return Ok(WalOpenOutcome::HeaderTorn {
                reason: "binary WAL file header is truncated".to_string(),
            });
        }
        let (generation, start_lsn) = match frame::decode_binary_wal_header(&header) {
            Ok(header) => header,
            Err(error) => {
                return Ok(WalOpenOutcome::HeaderCorrupt {
                    reason: error.to_string(),
                });
            }
        };
        Ok(WalOpenOutcome::Cursor(WalRecordCursor {
            generation,
            start_lsn,
            reader: frame::BinaryWalReader::new(reader, generation, max_record_bytes),
        }))
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn start_lsn(&self) -> u64 {
        self.start_lsn
    }

    #[allow(
        clippy::should_implement_trait,
        reason = "the fallible WAL event API reports an explicit Eof event"
    )]
    pub fn next(&mut self) -> Result<WalCursorEvent> {
        match self.reader.next_event()? {
            frame::BinaryWalReadEvent::Record {
                payload,
                start_offset,
                end_offset,
            } => {
                let payload_len = payload.len() as u64;
                let payload_sha256 = skein_integrity::sha256(&payload);
                match binary::decode_binary_wal_record(&payload)? {
                    binary::BinaryWalRecordDecode::Entry { entry, .. } => {
                        Ok(WalCursorEvent::Entry {
                            entry,
                            start_offset,
                            encoded_len: end_offset - start_offset,
                            payload_len,
                            payload_sha256,
                        })
                    }
                    binary::BinaryWalRecordDecode::Corrupt(reason) => Ok(WalCursorEvent::Corrupt {
                        offset: start_offset,
                        reason,
                    }),
                }
            }
            frame::BinaryWalReadEvent::TornTail {
                valid_prefix_len,
                reason,
            } => Ok(WalCursorEvent::TornTail {
                valid_prefix_len,
                reason,
            }),
            frame::BinaryWalReadEvent::Corrupt { offset, reason } => {
                Ok(WalCursorEvent::Corrupt { offset, reason })
            }
            frame::BinaryWalReadEvent::Eof => Ok(WalCursorEvent::Eof),
        }
    }
}

#[derive(Debug)]
pub struct WalEntry {
    pub lsn: u64,
    pub op: WalOp,
}

#[derive(Debug, Clone)]
pub enum WalOp {
    CreateNodeLabel {
        label: String,
    },
    CreateRelationshipType {
        rel_type: String,
    },
    CreateNodeTable {
        name: String,
    },
    CreateRelationshipTable {
        name: String,
    },
    CreateProperty {
        table_kind: TableKind,
        table: String,
        property: String,
        value_type: PropertyType,
        nullable: bool,
    },
    AlterTableState {
        table_kind: TableKind,
        table: String,
        state: SchemaObjectState,
    },
    AlterPropertyState {
        table_kind: TableKind,
        table: String,
        property: String,
        state: SchemaObjectState,
    },
    GcTableDescriptor {
        table_kind: TableKind,
        table: String,
    },
    GcPropertyDescriptor {
        table_kind: TableKind,
        table: String,
        property: String,
    },
    CreateIndex {
        label: String,
        property: String,
    },
    CreateCompositeIndex {
        label: String,
        properties: Vec<String>,
    },
    CreateRangeIndex {
        label: String,
        property: String,
    },
    CreateFullTextIndex {
        label: String,
        property: String,
    },
    CreateUniqueConstraint {
        label: String,
        property: String,
    },
    CreateNodePropertyExistsConstraint {
        label: String,
        property: String,
    },
    CreateRelationshipUniqueConstraint {
        rel_type: String,
        property: String,
    },
    CreateRelationshipPropertyExistsConstraint {
        rel_type: String,
        property: String,
    },
    CreateNode {
        id: NodeId,
        label: String,
        properties: BTreeMap<String, Value>,
    },
    CreateRelationship {
        id: RelId,
        source: NodeId,
        target: NodeId,
        rel_type: String,
        properties: BTreeMap<String, Value>,
    },
    SetNodeProperty {
        id: NodeId,
        property: String,
        value: Value,
    },
    SetRelationshipProperty {
        id: RelId,
        property: String,
        value: Value,
    },
    DeleteNode {
        id: NodeId,
    },
    DeleteRelationship {
        id: RelId,
    },
    ProjectGraph {
        name: String,
        node_labels: Vec<String>,
        rel_types: Vec<String>,
    },
    MarkInitialImportSource {
        source_fingerprint: String,
    },
    Relational {
        record: Arc<[u8]>,
    },
    RelationalSnapshot {
        record: Arc<[u8]>,
    },
    Append {
        record: Arc<[u8]>,
    },
    Batch(Vec<WalOp>),
}

/// Validates graph property values before they enter the WAL or live state.
pub fn validate_wal_op_values(ops: &[WalOp]) -> Result<()> {
    for op in ops {
        match op {
            WalOp::CreateNode { properties, .. } | WalOp::CreateRelationship { properties, .. } => {
                for value in properties.values() {
                    validate_wal_value(value)?;
                }
            }
            WalOp::SetNodeProperty { value, .. } | WalOp::SetRelationshipProperty { value, .. } => {
                validate_wal_value(value)?;
            }
            WalOp::Batch(ops) => validate_wal_op_values(ops)?,
            _ => {}
        }
    }
    Ok(())
}

fn validate_wal_value(value: &Value) -> Result<()> {
    crate::canonical::validate_property_value(value).map_err(|error| {
        SkeinError::Storage(format!(
            "WAL value violates canonical storage limits: {error}"
        ))
    })
}

impl WalEntry {
    #[doc(hidden)]
    pub fn encode(&self) -> Result<String> {
        let payload = match &self.op {
            WalOp::CreateNodeLabel { label } => {
                format!("create_node_label\t{}", encode_string(label))
            }
            WalOp::CreateRelationshipType { rel_type } => {
                format!("create_rel_type\t{}", encode_string(rel_type))
            }
            WalOp::CreateNodeTable { name } => {
                format!("create_node_table\t{}", encode_string(name))
            }
            WalOp::CreateRelationshipTable { name } => {
                format!("create_rel_table\t{}", encode_string(name))
            }
            WalOp::CreateProperty {
                table_kind,
                table,
                property,
                value_type,
                nullable,
            } => format!(
                "create_property\t{}\t{}\t{}\t{}\t{}",
                encode_table_kind(*table_kind),
                encode_string(table),
                encode_string(property),
                encode_property_type(*value_type),
                encode_nullable(*nullable)
            ),
            WalOp::AlterTableState {
                table_kind,
                table,
                state,
            } => format!(
                "alter_table_state\t{}\t{}\t{}",
                encode_table_kind(*table_kind),
                encode_string(table),
                encode_schema_object_state(*state)
            ),
            WalOp::AlterPropertyState {
                table_kind,
                table,
                property,
                state,
            } => format!(
                "alter_property_state\t{}\t{}\t{}\t{}",
                encode_table_kind(*table_kind),
                encode_string(table),
                encode_string(property),
                encode_schema_object_state(*state)
            ),
            WalOp::GcTableDescriptor { table_kind, table } => format!(
                "gc_table_descriptor\t{}\t{}",
                encode_table_kind(*table_kind),
                encode_string(table)
            ),
            WalOp::GcPropertyDescriptor {
                table_kind,
                table,
                property,
            } => format!(
                "gc_property_descriptor\t{}\t{}\t{}",
                encode_table_kind(*table_kind),
                encode_string(table),
                encode_string(property)
            ),
            WalOp::CreateIndex { label, property } => format!(
                "create_index\t{}\t{}",
                encode_string(label),
                encode_string(property)
            ),
            WalOp::CreateCompositeIndex { label, properties } => format!(
                "create_composite_index\t{}\t{}",
                encode_string(label),
                encode_string_vec(properties)
            ),
            WalOp::CreateRangeIndex { label, property } => format!(
                "create_range_index\t{}\t{}",
                encode_string(label),
                encode_string(property)
            ),
            WalOp::CreateFullTextIndex { label, property } => format!(
                "create_fulltext_index\t{}\t{}",
                encode_string(label),
                encode_string(property)
            ),
            WalOp::CreateUniqueConstraint { label, property } => format!(
                "create_unique_constraint\t{}\t{}",
                encode_string(label),
                encode_string(property)
            ),
            WalOp::CreateNodePropertyExistsConstraint { label, property } => format!(
                "create_node_property_exists_constraint\t{}\t{}",
                encode_string(label),
                encode_string(property)
            ),
            WalOp::CreateRelationshipUniqueConstraint { rel_type, property } => format!(
                "create_relationship_unique_constraint\t{}\t{}",
                encode_string(rel_type),
                encode_string(property)
            ),
            WalOp::CreateRelationshipPropertyExistsConstraint { rel_type, property } => format!(
                "create_relationship_property_exists_constraint\t{}\t{}",
                encode_string(rel_type),
                encode_string(property)
            ),
            WalOp::CreateNode {
                id,
                label,
                properties,
            } => format!(
                "create_node\t{}\t{}\t{}",
                id.0,
                encode_string(label),
                encode_properties(properties)
            ),
            WalOp::CreateRelationship {
                id,
                source,
                target,
                rel_type,
                properties,
            } => format!(
                "create_rel\t{}\t{}\t{}\t{}\t{}",
                id.0,
                source.0,
                target.0,
                encode_string(rel_type),
                encode_properties(properties)
            ),
            WalOp::SetNodeProperty {
                id,
                property,
                value,
            } => format!(
                "set_node_property\t{}\t{}\t{}",
                id.0,
                encode_string(property),
                encode_value(value)
            ),
            WalOp::SetRelationshipProperty {
                id,
                property,
                value,
            } => format!(
                "set_rel_property\t{}\t{}\t{}",
                id.0,
                encode_string(property),
                encode_value(value)
            ),
            WalOp::DeleteNode { id } => {
                format!("delete_node\t{}", id.0)
            }
            WalOp::DeleteRelationship { id } => {
                format!("delete_rel\t{}", id.0)
            }
            WalOp::ProjectGraph {
                name,
                node_labels,
                rel_types,
            } => format!(
                "project_graph\t{}\t{}\t{}",
                encode_string(name),
                encode_string_vec(node_labels),
                encode_string_vec(rel_types)
            ),
            WalOp::MarkInitialImportSource { source_fingerprint } => format!(
                "mark_initial_import_source\t{}",
                encode_string(source_fingerprint)
            ),
            WalOp::Relational { record } => {
                format!("relational\t{}", encode_bytes_base64(record))
            }
            WalOp::RelationalSnapshot { record } => {
                format!("relational_snapshot\t{}", encode_bytes_base64(record))
            }
            WalOp::Append { record } => {
                format!("append\t{}", encode_bytes_base64(record))
            }
            WalOp::Batch(ops) => {
                let encoded = ops
                    .iter()
                    .map(encode_wal_op_for_batch)
                    .collect::<Result<Vec<_>>>()?
                    .join("|");
                format!("batch\t{encoded}")
            }
        };
        let body = format!("{}\t{payload}", self.lsn);
        let checksum = checksum_bytes(body.as_bytes());
        Ok(format!("{body}\t{checksum}"))
    }
}

fn encode_wal_op_for_batch(op: &WalOp) -> Result<String> {
    let encoded = match op {
        WalOp::CreateNodeLabel { label } => {
            format!("create_node_label,{}", encode_string(label))
        }
        WalOp::CreateRelationshipType { rel_type } => {
            format!("create_rel_type,{}", encode_string(rel_type))
        }
        WalOp::CreateNodeTable { name } => {
            format!("create_node_table,{}", encode_string(name))
        }
        WalOp::CreateRelationshipTable { name } => {
            format!("create_rel_table,{}", encode_string(name))
        }
        WalOp::CreateProperty {
            table_kind,
            table,
            property,
            value_type,
            nullable,
        } => format!(
            "create_property,{},{},{},{},{}",
            encode_table_kind(*table_kind),
            encode_string(table),
            encode_string(property),
            encode_property_type(*value_type),
            encode_nullable(*nullable)
        ),
        WalOp::AlterTableState {
            table_kind,
            table,
            state,
        } => format!(
            "alter_table_state,{},{},{}",
            encode_table_kind(*table_kind),
            encode_string(table),
            encode_schema_object_state(*state)
        ),
        WalOp::AlterPropertyState {
            table_kind,
            table,
            property,
            state,
        } => format!(
            "alter_property_state,{},{},{},{}",
            encode_table_kind(*table_kind),
            encode_string(table),
            encode_string(property),
            encode_schema_object_state(*state)
        ),
        WalOp::GcTableDescriptor { table_kind, table } => {
            format!(
                "gc_table_descriptor,{},{}",
                encode_table_kind(*table_kind),
                encode_string(table)
            )
        }
        WalOp::GcPropertyDescriptor {
            table_kind,
            table,
            property,
        } => format!(
            "gc_property_descriptor,{},{},{}",
            encode_table_kind(*table_kind),
            encode_string(table),
            encode_string(property)
        ),
        WalOp::CreateIndex { label, property } => {
            format!(
                "create_index,{},{}",
                encode_string(label),
                encode_string(property)
            )
        }
        WalOp::CreateCompositeIndex { label, properties } => {
            format!(
                "create_composite_index,{},{}",
                encode_string(label),
                encode_string_vec(properties)
            )
        }
        WalOp::CreateRangeIndex { label, property } => {
            format!(
                "create_range_index,{},{}",
                encode_string(label),
                encode_string(property)
            )
        }
        WalOp::CreateFullTextIndex { label, property } => {
            format!(
                "create_fulltext_index,{},{}",
                encode_string(label),
                encode_string(property)
            )
        }
        WalOp::CreateUniqueConstraint { label, property } => {
            format!(
                "create_unique_constraint,{},{}",
                encode_string(label),
                encode_string(property)
            )
        }
        WalOp::CreateNodePropertyExistsConstraint { label, property } => {
            format!(
                "create_node_property_exists_constraint,{},{}",
                encode_string(label),
                encode_string(property)
            )
        }
        WalOp::CreateRelationshipUniqueConstraint { rel_type, property } => {
            format!(
                "create_relationship_unique_constraint,{},{}",
                encode_string(rel_type),
                encode_string(property)
            )
        }
        WalOp::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
            format!(
                "create_relationship_property_exists_constraint,{},{}",
                encode_string(rel_type),
                encode_string(property)
            )
        }
        WalOp::CreateNode {
            id,
            label,
            properties,
        } => format!(
            "create_node,{},{},{}",
            id.0,
            encode_string(label),
            encode_properties(properties)
        ),
        WalOp::CreateRelationship {
            id,
            source,
            target,
            rel_type,
            properties,
        } => format!(
            "create_rel,{},{},{},{},{}",
            id.0,
            source.0,
            target.0,
            encode_string(rel_type),
            encode_properties(properties)
        ),
        WalOp::SetNodeProperty {
            id,
            property,
            value,
        } => format!(
            "set_node_property,{},{},{}",
            id.0,
            encode_string(property),
            encode_value(value)
        ),
        WalOp::SetRelationshipProperty {
            id,
            property,
            value,
        } => format!(
            "set_rel_property,{},{},{}",
            id.0,
            encode_string(property),
            encode_value(value)
        ),
        WalOp::DeleteNode { id } => {
            format!("delete_node,{}", id.0)
        }
        WalOp::DeleteRelationship { id } => {
            format!("delete_rel,{}", id.0)
        }
        WalOp::ProjectGraph {
            name,
            node_labels,
            rel_types,
        } => format!(
            "project_graph,{},{},{}",
            encode_string(name),
            encode_string_vec(node_labels),
            encode_string_vec(rel_types)
        ),
        WalOp::MarkInitialImportSource { source_fingerprint } => format!(
            "mark_initial_import_source,{}",
            encode_string(source_fingerprint)
        ),
        WalOp::Relational { record } => {
            format!("relational,{}", encode_bytes_base64(record))
        }
        WalOp::RelationalSnapshot { record } => {
            format!("relational_snapshot,{}", encode_bytes_base64(record))
        }
        WalOp::Append { record } => {
            format!("append,{}", encode_bytes_base64(record))
        }
        WalOp::Batch(_) => {
            return Err(SkeinError::Storage(
                "nested WAL batches cannot be encoded".to_string(),
            ));
        }
    };
    Ok(encoded)
}

fn checksum_bytes(bytes: &[u8]) -> u64 {
    skein_integrity::checksum_u64(bytes)
}

fn encode_bytes_base64(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(input.len().div_ceil(3).saturating_mul(4));
    for chunk in input.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        output.push(ALPHABET[(first >> 2) as usize] as char);
        output.push(ALPHABET[(((first & 0x03) << 4) | (second >> 4)) as usize] as char);
        output.push(if chunk.len() > 1 {
            ALPHABET[(((second & 0x0f) << 2) | (third >> 6)) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            ALPHABET[(third & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    output
}
