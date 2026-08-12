//! Encoding, decoding, and framing of write-ahead-log records.

#[path = "wal_codec/binary.rs"]
pub(crate) mod binary;
#[path = "wal_codec/frame.rs"]
pub(crate) mod frame;
#[path = "wal_codec/wire.rs"]
pub(crate) mod wire;

use super::{
    checksum_bytes, decode_bytes_base64, decode_nullable, decode_properties, decode_property_type,
    decode_schema_object_state, decode_string, decode_string_vec, decode_table_kind, decode_value,
    encode_bytes_base64, encode_nullable, encode_properties, encode_property_type,
    encode_schema_object_state, encode_string, encode_string_vec, encode_table_kind, encode_value,
    parse_u64, sync_parent_dir, WAL_HEADER_V1,
};
use crate::error::{Result, SkeinError};
use crate::schema::{PropertyType, SchemaObjectState, TableKind};
use crate::value::Value;
use skein_storage::{NodeId, RelId};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::BufRead;
use std::path::Path;
use std::sync::Arc;

pub(super) fn encode_wal_header(generation: u64, start_lsn: u64) -> String {
    let body = format!("{WAL_HEADER_V1}\t{generation}\t{start_lsn}");
    let checksum = checksum_bytes(body.as_bytes());
    format!("{body}\t{checksum}")
}

pub(super) struct BoundedWalRecord {
    pub(super) bytes: Vec<u8>,
    pub(super) encoded_len: u64,
    pub(super) terminated_by_newline: bool,
}

pub(super) fn read_bounded_wal_record<R: BufRead>(
    reader: &mut R,
    max_record_bytes: Option<usize>,
) -> Result<Option<BoundedWalRecord>> {
    let mut bytes = Vec::new();
    let mut encoded_len = 0u64;
    let mut terminated_by_newline = false;
    loop {
        let (consumed, complete) = {
            let available = reader.fill_buf()?;
            if available.is_empty() {
                if bytes.is_empty() {
                    return Ok(None);
                }
                break;
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let consumed = newline.map_or(available.len(), |index| index + 1);
            let next_len = bytes
                .len()
                .checked_add(consumed)
                .ok_or_else(|| SkeinError::Storage("WAL record length overflow".to_string()))?;
            if max_record_bytes.is_some_and(|limit| next_len > limit) {
                return Err(SkeinError::Storage(format!(
                    "WAL record byte limit exceeded: max_wal_record_bytes={}",
                    max_record_bytes.unwrap_or_default()
                )));
            }
            bytes.extend_from_slice(&available[..consumed]);
            (consumed, newline.is_some())
        };
        reader.consume(consumed);
        encoded_len = encoded_len.saturating_add(consumed as u64);
        if complete {
            terminated_by_newline = true;
            break;
        }
    }
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
    }
    if bytes.last() == Some(&b'\r') {
        bytes.pop();
    }
    Ok(Some(BoundedWalRecord {
        bytes,
        encoded_len,
        terminated_by_newline,
    }))
}

pub(super) fn decode_wal_header(line: &str) -> Result<(u64, u64)> {
    let Some((body, raw_checksum)) = line.rsplit_once('\t') else {
        return Err(SkeinError::Storage(
            "WAL header is missing its checksum".to_string(),
        ));
    };
    let expected = parse_u64(raw_checksum, "WAL header checksum")?;
    let actual = checksum_bytes(body.as_bytes());
    if expected != actual {
        return Err(SkeinError::Storage(format!(
            "WAL header checksum mismatch: expected {expected}, got {actual}"
        )));
    }
    let fields = body.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        [header, raw_generation, raw_start_lsn] if *header == WAL_HEADER_V1 => Ok((
            parse_u64(raw_generation, "WAL generation")?,
            parse_u64(raw_start_lsn, "WAL start LSN")?,
        )),
        _ => Err(SkeinError::Storage(
            "WAL is missing a supported generation header".to_string(),
        )),
    }
}

pub(super) fn quarantine_corrupt_wal(path: &Path, generation: u64, read_only: bool) -> Result<()> {
    if read_only {
        return Ok(());
    }
    let root = path
        .parent()
        .ok_or_else(|| SkeinError::Storage("WAL path has no database directory".to_string()))?;
    let quarantine_dir = root.join("quarantine");
    fs::create_dir_all(&quarantine_dir)?;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let quarantine_path = quarantine_dir.join(format!(
        "wal.{generation}.corrupt.{}.{}",
        std::process::id(),
        nonce
    ));
    fs::copy(path, &quarantine_path)?;
    File::open(&quarantine_path)?.sync_all()?;
    sync_parent_dir(&quarantine_path)
}

pub(super) fn reject_corrupt_wal_record<T>(
    path: &Path,
    generation: u64,
    read_only: bool,
    record_start: u64,
    reason: impl std::fmt::Display,
) -> Result<T> {
    quarantine_corrupt_wal(path, generation, read_only)?;
    Err(SkeinError::Storage(format!(
        "WAL corruption at byte offset {record_start}: {reason}"
    )))
}

/// The on-disk encoding of one WAL generation file.
///
/// Text (V1) files begin with the `SKEIN_WAL_V1` header line; binary (V2)
/// files begin with the `SKWALB01` magic. The writer emits the binary
/// format for every new WAL generation, so an existing text database
/// upgrades at its next checkpoint rotation; the V1 reader and encoder
/// stay in the codebase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WalFileFormat {
    TextV1,
    BinaryV2,
}

/// Sniffs the format of an existing WAL file for the append path. Missing
/// and empty files are treated as binary: any new WAL content is binary.
pub(super) fn sniff_wal_format(path: &Path) -> Result<WalFileFormat> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(WalFileFormat::BinaryV2);
        }
        Err(error) => return Err(error.into()),
    };
    let mut probe = [0u8; 8];
    let mut filled = 0usize;
    while filled < probe.len() {
        let read = std::io::Read::read(&mut file, &mut probe[filled..])?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    if filled == 0 {
        return Ok(WalFileFormat::BinaryV2);
    }
    if probe[..filled] == frame::WAL_BINARY_MAGIC[..filled.min(8)] && filled == 8 {
        Ok(WalFileFormat::BinaryV2)
    } else {
        Ok(WalFileFormat::TextV1)
    }
}

/// One decoded event from a format-agnostic WAL scan. Offsets are absolute
/// file offsets; `encoded_len` covers the framed bytes of the record.
pub(super) enum WalCursorEvent {
    Entry {
        entry: WalEntry,
        start_offset: u64,
        encoded_len: u64,
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
pub(super) enum WalOpenOutcome {
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

enum WalCursorInner {
    Text {
        reader: std::io::BufReader<File>,
        byte_offset: u64,
        max_record_bytes: Option<usize>,
        finished: bool,
    },
    Binary(frame::BinaryWalReader<std::io::BufReader<File>>),
}

/// Format-agnostic reader over one WAL generation file: it sniffs the V1
/// text or V2 binary encoding, validates the generation header, and yields
/// records with the shared recovery-policy vocabulary.
pub(super) struct WalRecordCursor {
    generation: u64,
    start_lsn: u64,
    format: WalFileFormat,
    inner: WalCursorInner,
}

impl WalRecordCursor {
    pub(super) fn open(path: &Path, max_record_bytes: Option<usize>) -> Result<WalOpenOutcome> {
        let file = File::open(path)?;
        let mut reader = std::io::BufReader::new(file);
        let probe = std::io::BufRead::fill_buf(&mut reader)?;
        if probe.is_empty() {
            return Ok(WalOpenOutcome::MissingHeader);
        }
        if probe.starts_with(frame::WAL_BINARY_MAGIC) || frame::WAL_BINARY_MAGIC.starts_with(probe)
        {
            return Self::open_binary(reader, max_record_bytes);
        }
        Self::open_text(reader, max_record_bytes)
    }

    fn open_binary(
        mut reader: std::io::BufReader<File>,
        max_record_bytes: Option<usize>,
    ) -> Result<WalOpenOutcome> {
        let mut header = [0u8; frame::WAL_BINARY_FILE_HEADER_BYTES];
        let mut filled = 0usize;
        while filled < header.len() {
            let read = std::io::Read::read(&mut reader, &mut header[filled..])?;
            if read == 0 {
                break;
            }
            filled += read;
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
            format: WalFileFormat::BinaryV2,
            inner: WalCursorInner::Binary(frame::BinaryWalReader::new(
                reader,
                generation,
                max_record_bytes,
            )),
        }))
    }

    fn open_text(
        mut reader: std::io::BufReader<File>,
        max_record_bytes: Option<usize>,
    ) -> Result<WalOpenOutcome> {
        let Some(record) = read_bounded_wal_record(&mut reader, max_record_bytes)? else {
            return Ok(WalOpenOutcome::MissingHeader);
        };
        if !record.terminated_by_newline {
            return Ok(WalOpenOutcome::HeaderTorn {
                reason: "WAL header is not newline-terminated".to_string(),
            });
        }
        let line = match std::str::from_utf8(&record.bytes) {
            Ok(line) => line,
            Err(error) => {
                return Ok(WalOpenOutcome::HeaderCorrupt {
                    reason: format!("record is not valid UTF-8: {error}"),
                });
            }
        };
        let (generation, start_lsn) = match decode_wal_header(line) {
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
            format: WalFileFormat::TextV1,
            inner: WalCursorInner::Text {
                byte_offset: record.encoded_len,
                reader,
                max_record_bytes,
                finished: false,
            },
        }))
    }

    pub(super) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(super) const fn start_lsn(&self) -> u64 {
        self.start_lsn
    }

    #[allow(dead_code)]
    pub(super) const fn format(&self) -> WalFileFormat {
        self.format
    }

    pub(super) fn next(&mut self) -> Result<WalCursorEvent> {
        match &mut self.inner {
            WalCursorInner::Text {
                reader,
                byte_offset,
                max_record_bytes,
                finished,
            } => {
                if *finished {
                    return Ok(WalCursorEvent::Eof);
                }
                let Some(record) = read_bounded_wal_record(reader, *max_record_bytes)? else {
                    *finished = true;
                    return Ok(WalCursorEvent::Eof);
                };
                let start_offset = *byte_offset;
                *byte_offset += record.encoded_len;
                if !record.terminated_by_newline {
                    *finished = true;
                    return Ok(WalCursorEvent::TornTail {
                        valid_prefix_len: start_offset,
                        reason: "WAL tail record is not newline-terminated".to_string(),
                    });
                }
                let line = match std::str::from_utf8(&record.bytes) {
                    Ok(line) => line,
                    Err(error) => {
                        *finished = true;
                        return Ok(WalCursorEvent::Corrupt {
                            offset: start_offset,
                            reason: format!("record is not valid UTF-8: {error}"),
                        });
                    }
                };
                if line.is_empty() {
                    *finished = true;
                    return Ok(WalCursorEvent::Corrupt {
                        offset: start_offset,
                        reason: "record is empty".to_string(),
                    });
                }
                match WalEntry::decode(line) {
                    Ok(WalDecodeResult::Entry(entry)) => Ok(WalCursorEvent::Entry {
                        entry,
                        start_offset,
                        encoded_len: record.encoded_len,
                    }),
                    Ok(WalDecodeResult::Corrupt(reason)) => {
                        *finished = true;
                        Ok(WalCursorEvent::Corrupt {
                            offset: start_offset,
                            reason,
                        })
                    }
                    Err(error) => {
                        *finished = true;
                        Ok(WalCursorEvent::Corrupt {
                            offset: start_offset,
                            reason: error.to_string(),
                        })
                    }
                }
            }
            WalCursorInner::Binary(reader) => match reader.next_event()? {
                frame::BinaryWalReadEvent::Record {
                    payload,
                    start_offset,
                    end_offset,
                } => match binary::decode_binary_wal_record(&payload)? {
                    binary::BinaryWalRecordDecode::Entry { entry, .. } => {
                        Ok(WalCursorEvent::Entry {
                            entry,
                            start_offset,
                            encoded_len: end_offset - start_offset,
                        })
                    }
                    binary::BinaryWalRecordDecode::Corrupt(reason) => Ok(WalCursorEvent::Corrupt {
                        offset: start_offset,
                        reason,
                    }),
                },
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
            },
        }
    }
}

#[derive(Debug)]
pub(super) struct WalEntry {
    pub(super) lsn: u64,
    pub(super) op: WalOp,
}

pub(super) enum WalDecodeResult {
    Entry(WalEntry),
    Corrupt(String),
}

#[derive(Debug, Clone)]
pub(super) enum WalOp {
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
    Batch(Vec<WalOp>),
}

impl WalEntry {
    pub(super) fn encode(&self) -> String {
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
            WalOp::Batch(ops) => format!(
                "batch\t{}",
                ops.iter()
                    .map(encode_wal_op_for_batch)
                    .collect::<Vec<_>>()
                    .join("|")
            ),
        };
        let body = format!("{}\t{payload}", self.lsn);
        let checksum = checksum_bytes(body.as_bytes());
        format!("{body}\t{checksum}")
    }

    pub(super) fn decode(line: &str) -> Result<WalDecodeResult> {
        let Some((body, raw_checksum)) = line.rsplit_once('\t') else {
            return Ok(WalDecodeResult::Corrupt(
                "missing checksum field".to_string(),
            ));
        };
        let Ok(expected) = raw_checksum.parse::<u64>() else {
            return Ok(WalDecodeResult::Corrupt(
                "invalid checksum field".to_string(),
            ));
        };
        let actual = checksum_bytes(body.as_bytes());
        if expected != actual {
            return Ok(WalDecodeResult::Corrupt(format!(
                "checksum mismatch: expected {expected}, got {actual}"
            )));
        }
        let fields = body.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            [raw_lsn, "create_node_label", raw_label] => Ok(WalDecodeResult::Entry(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::CreateNodeLabel {
                    label: decode_string(raw_label)?,
                },
            })),
            [raw_lsn, "create_rel_type", raw_type] => Ok(WalDecodeResult::Entry(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::CreateRelationshipType {
                    rel_type: decode_string(raw_type)?,
                },
            })),
            [raw_lsn, "create_node_table", raw_name] => Ok(WalDecodeResult::Entry(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::CreateNodeTable {
                    name: decode_string(raw_name)?,
                },
            })),
            [raw_lsn, "create_rel_table", raw_name] => Ok(WalDecodeResult::Entry(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::CreateRelationshipTable {
                    name: decode_string(raw_name)?,
                },
            })),
            [raw_lsn, "create_property", raw_kind, raw_table, raw_property, raw_type, raw_nullable] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateProperty {
                        table_kind: decode_table_kind(raw_kind)?,
                        table: decode_string(raw_table)?,
                        property: decode_string(raw_property)?,
                        value_type: decode_property_type(raw_type)?,
                        nullable: decode_nullable(raw_nullable)?,
                    },
                }))
            }
            [raw_lsn, "alter_table_state", raw_kind, raw_table, raw_state] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::AlterTableState {
                        table_kind: decode_table_kind(raw_kind)?,
                        table: decode_string(raw_table)?,
                        state: decode_schema_object_state(raw_state)?,
                    },
                }))
            }
            [raw_lsn, "alter_property_state", raw_kind, raw_table, raw_property, raw_state] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::AlterPropertyState {
                        table_kind: decode_table_kind(raw_kind)?,
                        table: decode_string(raw_table)?,
                        property: decode_string(raw_property)?,
                        state: decode_schema_object_state(raw_state)?,
                    },
                }))
            }
            [raw_lsn, "gc_table_descriptor", raw_kind, raw_table] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::GcTableDescriptor {
                        table_kind: decode_table_kind(raw_kind)?,
                        table: decode_string(raw_table)?,
                    },
                }))
            }
            [raw_lsn, "gc_property_descriptor", raw_kind, raw_table, raw_property] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::GcPropertyDescriptor {
                        table_kind: decode_table_kind(raw_kind)?,
                        table: decode_string(raw_table)?,
                        property: decode_string(raw_property)?,
                    },
                }))
            }
            [raw_lsn, "create_index", raw_label, raw_property] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateIndex {
                        label: decode_string(raw_label)?,
                        property: decode_string(raw_property)?,
                    },
                }))
            }
            [raw_lsn, "create_composite_index", raw_label, raw_properties] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateCompositeIndex {
                        label: decode_string(raw_label)?,
                        properties: decode_string_vec(raw_properties)?,
                    },
                }))
            }
            [raw_lsn, "create_range_index", raw_label, raw_property] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateRangeIndex {
                        label: decode_string(raw_label)?,
                        property: decode_string(raw_property)?,
                    },
                }))
            }
            [raw_lsn, "create_fulltext_index", raw_label, raw_property] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateFullTextIndex {
                        label: decode_string(raw_label)?,
                        property: decode_string(raw_property)?,
                    },
                }))
            }
            [raw_lsn, "create_unique_constraint", raw_label, raw_property] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateUniqueConstraint {
                        label: decode_string(raw_label)?,
                        property: decode_string(raw_property)?,
                    },
                }))
            }
            [raw_lsn, "create_node_property_exists_constraint", raw_label, raw_property] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateNodePropertyExistsConstraint {
                        label: decode_string(raw_label)?,
                        property: decode_string(raw_property)?,
                    },
                }))
            }
            [raw_lsn, "create_relationship_unique_constraint", raw_rel_type, raw_property] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateRelationshipUniqueConstraint {
                        rel_type: decode_string(raw_rel_type)?,
                        property: decode_string(raw_property)?,
                    },
                }))
            }
            [raw_lsn, "create_relationship_property_exists_constraint", raw_rel_type, raw_property] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateRelationshipPropertyExistsConstraint {
                        rel_type: decode_string(raw_rel_type)?,
                        property: decode_string(raw_property)?,
                    },
                }))
            }
            [raw_lsn, "create_node", raw_id, raw_label, raw_properties] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateNode {
                        id: NodeId(parse_u64(raw_id, "wal node id")?),
                        label: decode_string(raw_label)?,
                        properties: decode_properties(raw_properties)?,
                    },
                }))
            }
            [raw_lsn, "create_rel", raw_id, raw_source, raw_target, raw_type, raw_properties] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::CreateRelationship {
                        id: RelId(parse_u64(raw_id, "wal rel id")?),
                        source: NodeId(parse_u64(raw_source, "wal rel source")?),
                        target: NodeId(parse_u64(raw_target, "wal rel target")?),
                        rel_type: decode_string(raw_type)?,
                        properties: decode_properties(raw_properties)?,
                    },
                }))
            }
            [raw_lsn, "set_node_property", raw_id, raw_property, raw_value] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::SetNodeProperty {
                        id: NodeId(parse_u64(raw_id, "wal node id")?),
                        property: decode_string(raw_property)?,
                        value: decode_value(raw_value)?,
                    },
                }))
            }
            [raw_lsn, "set_rel_property", raw_id, raw_property, raw_value] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::SetRelationshipProperty {
                        id: RelId(parse_u64(raw_id, "wal rel id")?),
                        property: decode_string(raw_property)?,
                        value: decode_value(raw_value)?,
                    },
                }))
            }
            [raw_lsn, "delete_node", raw_id] => Ok(WalDecodeResult::Entry(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::DeleteNode {
                    id: NodeId(parse_u64(raw_id, "wal node id")?),
                },
            })),
            [raw_lsn, "delete_rel", raw_id] => Ok(WalDecodeResult::Entry(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::DeleteRelationship {
                    id: RelId(parse_u64(raw_id, "wal rel id")?),
                },
            })),
            [raw_lsn, "project_graph", raw_name, raw_node_labels, raw_rel_types] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::ProjectGraph {
                        name: decode_string(raw_name)?,
                        node_labels: decode_string_vec(raw_node_labels)?,
                        rel_types: decode_string_vec(raw_rel_types)?,
                    },
                }))
            }
            [raw_lsn, "mark_initial_import_source", raw_source_fingerprint] => {
                Ok(WalDecodeResult::Entry(WalEntry {
                    lsn: parse_u64(raw_lsn, "wal lsn")?,
                    op: WalOp::MarkInitialImportSource {
                        source_fingerprint: decode_string(raw_source_fingerprint)?,
                    },
                }))
            }
            [raw_lsn, "relational", raw_record] => Ok(WalDecodeResult::Entry(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::Relational {
                    record: Arc::from(decode_bytes_base64(raw_record)?),
                },
            })),
            [raw_lsn, "relational_snapshot", raw_record] => Ok(WalDecodeResult::Entry(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::RelationalSnapshot {
                    record: Arc::from(decode_bytes_base64(raw_record)?),
                },
            })),
            [raw_lsn, "batch", raw_ops] => Ok(WalDecodeResult::Entry(WalEntry {
                lsn: parse_u64(raw_lsn, "wal lsn")?,
                op: WalOp::Batch(decode_wal_batch(raw_ops)?),
            })),
            _ => Err(SkeinError::Storage(format!("invalid wal entry: {line}"))),
        }
    }
}

fn encode_wal_op_for_batch(op: &WalOp) -> String {
    match op {
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
        WalOp::Batch(_) => unreachable!("nested wal batches are not encoded"),
    }
}

fn decode_wal_batch(input: &str) -> Result<Vec<WalOp>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input.split('|').map(decode_wal_op_from_batch).collect()
}

fn decode_wal_op_from_batch(input: &str) -> Result<WalOp> {
    let fields = input.split(',').collect::<Vec<_>>();
    match fields.as_slice() {
        ["create_node_label", raw_label] => Ok(WalOp::CreateNodeLabel {
            label: decode_string(raw_label)?,
        }),
        ["create_rel_type", raw_type] => Ok(WalOp::CreateRelationshipType {
            rel_type: decode_string(raw_type)?,
        }),
        ["create_node_table", raw_name] => Ok(WalOp::CreateNodeTable {
            name: decode_string(raw_name)?,
        }),
        ["create_rel_table", raw_name] => Ok(WalOp::CreateRelationshipTable {
            name: decode_string(raw_name)?,
        }),
        ["create_property", raw_kind, raw_table, raw_property, raw_type, raw_nullable] => {
            Ok(WalOp::CreateProperty {
                table_kind: decode_table_kind(raw_kind)?,
                table: decode_string(raw_table)?,
                property: decode_string(raw_property)?,
                value_type: decode_property_type(raw_type)?,
                nullable: decode_nullable(raw_nullable)?,
            })
        }
        ["alter_table_state", raw_kind, raw_table, raw_state] => Ok(WalOp::AlterTableState {
            table_kind: decode_table_kind(raw_kind)?,
            table: decode_string(raw_table)?,
            state: decode_schema_object_state(raw_state)?,
        }),
        ["alter_property_state", raw_kind, raw_table, raw_property, raw_state] => {
            Ok(WalOp::AlterPropertyState {
                table_kind: decode_table_kind(raw_kind)?,
                table: decode_string(raw_table)?,
                property: decode_string(raw_property)?,
                state: decode_schema_object_state(raw_state)?,
            })
        }
        ["gc_table_descriptor", raw_kind, raw_table] => Ok(WalOp::GcTableDescriptor {
            table_kind: decode_table_kind(raw_kind)?,
            table: decode_string(raw_table)?,
        }),
        ["gc_property_descriptor", raw_kind, raw_table, raw_property] => {
            Ok(WalOp::GcPropertyDescriptor {
                table_kind: decode_table_kind(raw_kind)?,
                table: decode_string(raw_table)?,
                property: decode_string(raw_property)?,
            })
        }
        ["create_index", raw_label, raw_property] => Ok(WalOp::CreateIndex {
            label: decode_string(raw_label)?,
            property: decode_string(raw_property)?,
        }),
        ["create_composite_index", raw_label, raw_properties] => Ok(WalOp::CreateCompositeIndex {
            label: decode_string(raw_label)?,
            properties: decode_string_vec(raw_properties)?,
        }),
        ["create_range_index", raw_label, raw_property] => Ok(WalOp::CreateRangeIndex {
            label: decode_string(raw_label)?,
            property: decode_string(raw_property)?,
        }),
        ["create_fulltext_index", raw_label, raw_property] => Ok(WalOp::CreateFullTextIndex {
            label: decode_string(raw_label)?,
            property: decode_string(raw_property)?,
        }),
        ["create_unique_constraint", raw_label, raw_property] => {
            Ok(WalOp::CreateUniqueConstraint {
                label: decode_string(raw_label)?,
                property: decode_string(raw_property)?,
            })
        }
        ["create_node_property_exists_constraint", raw_label, raw_property] => {
            Ok(WalOp::CreateNodePropertyExistsConstraint {
                label: decode_string(raw_label)?,
                property: decode_string(raw_property)?,
            })
        }
        ["create_relationship_unique_constraint", raw_rel_type, raw_property] => {
            Ok(WalOp::CreateRelationshipUniqueConstraint {
                rel_type: decode_string(raw_rel_type)?,
                property: decode_string(raw_property)?,
            })
        }
        ["create_relationship_property_exists_constraint", raw_rel_type, raw_property] => {
            Ok(WalOp::CreateRelationshipPropertyExistsConstraint {
                rel_type: decode_string(raw_rel_type)?,
                property: decode_string(raw_property)?,
            })
        }
        ["create_node", raw_id, raw_label, raw_properties] => Ok(WalOp::CreateNode {
            id: NodeId(parse_u64(raw_id, "batch node id")?),
            label: decode_string(raw_label)?,
            properties: decode_properties(raw_properties)?,
        }),
        ["create_rel", raw_id, raw_source, raw_target, raw_type, raw_properties] => {
            Ok(WalOp::CreateRelationship {
                id: RelId(parse_u64(raw_id, "batch rel id")?),
                source: NodeId(parse_u64(raw_source, "batch rel source")?),
                target: NodeId(parse_u64(raw_target, "batch rel target")?),
                rel_type: decode_string(raw_type)?,
                properties: decode_properties(raw_properties)?,
            })
        }
        ["set_node_property", raw_id, raw_property, raw_value] => Ok(WalOp::SetNodeProperty {
            id: NodeId(parse_u64(raw_id, "batch node id")?),
            property: decode_string(raw_property)?,
            value: decode_value(raw_value)?,
        }),
        ["set_rel_property", raw_id, raw_property, raw_value] => {
            Ok(WalOp::SetRelationshipProperty {
                id: RelId(parse_u64(raw_id, "batch rel id")?),
                property: decode_string(raw_property)?,
                value: decode_value(raw_value)?,
            })
        }
        ["delete_node", raw_id] => Ok(WalOp::DeleteNode {
            id: NodeId(parse_u64(raw_id, "batch node id")?),
        }),
        ["delete_rel", raw_id] => Ok(WalOp::DeleteRelationship {
            id: RelId(parse_u64(raw_id, "batch rel id")?),
        }),
        ["project_graph", raw_name, raw_node_labels, raw_rel_types] => Ok(WalOp::ProjectGraph {
            name: decode_string(raw_name)?,
            node_labels: decode_string_vec(raw_node_labels)?,
            rel_types: decode_string_vec(raw_rel_types)?,
        }),
        ["mark_initial_import_source", raw_source_fingerprint] => {
            Ok(WalOp::MarkInitialImportSource {
                source_fingerprint: decode_string(raw_source_fingerprint)?,
            })
        }
        ["relational", raw_record] => Ok(WalOp::Relational {
            record: Arc::from(decode_bytes_base64(raw_record)?),
        }),
        ["relational_snapshot", raw_record] => Ok(WalOp::RelationalSnapshot {
            record: Arc::from(decode_bytes_base64(raw_record)?),
        }),
        _ => Err(SkeinError::Storage(format!(
            "invalid batch wal op: {input}"
        ))),
    }
}
