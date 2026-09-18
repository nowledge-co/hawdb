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
    column_positions, rebuild_indexes, validate_foreign_keys, validate_row, validate_table_schema,
    RelationalBigIntArithmeticOperator, RelationalBigIntOperand, RelationalColumnDefault,
    RelationalColumnSchema, RelationalComparisonOp, RelationalConflictAction, RelationalError,
    RelationalForeignKeySchema, RelationalIndexSchema, RelationalInsertMode, RelationalKey,
    RelationalOverflowRef, RelationalOverflowSegment, RelationalPredicate,
    RelationalPrimaryKeyChangeCapture, RelationalPrimaryKeyChangeRebuildReason,
    RelationalReferentialAction, RelationalReplayAccess, RelationalReplayAccessSet, RelationalRow,
    RelationalScalarType, RelationalState, RelationalTablePrimaryKeyChanges, RelationalTableSchema,
    RelationalTableSegment, RelationalTransaction, RelationalUpdateAssignment,
    RelationalUpdateValue, RelationalUpsertAssignment, RelationalUpsertValue, RelationalValue,
    RelationalWrite, Uuid,
};
use crate::{
    ContentDigest, FileSegmentRangeReader, SegmentReadRange, DEFAULT_MAX_CHECKPOINT_ENCODED_BYTES,
    DEFAULT_MAX_WAL_RECORD_BYTES,
};
use hawdb_integrity::{integrity_digest, IntegrityHasher, Sha256Digest, SHA256_BYTES};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;

const WAL_MAGIC: &[u8; 8] = b"SKRLWAL1";
const CHECKPOINT_MAGIC: &[u8; 8] = b"SKRLCKP1";
const CODEC_VERSION: u16 = 1;
const HEADER_BYTES: usize = 64;
const RELATIONAL_CHECKPOINT_ARTIFACT_ID: u64 = 1;

pub(crate) fn encode_relational_table_schema(
    schema: &RelationalTableSchema,
) -> Result<Vec<u8>, RelationalError> {
    validate_table_schema(schema)?;
    let mut encoder = Encoder::default();
    encoder.table_schema(schema)?;
    Ok(encoder.finish())
}

pub(crate) fn decode_relational_table_schema(
    encoded: &[u8],
    max_encoded_bytes: usize,
    max_schema_items: usize,
) -> Result<RelationalTableSchema, RelationalError> {
    if encoded.len() > max_encoded_bytes {
        return Err(RelationalError::Admission(format!(
            "relational table schema contains {} bytes, exceeding limit {max_encoded_bytes}",
            encoded.len()
        )));
    }
    let limits = RelationalDecodeLimits {
        max_record_bytes: max_encoded_bytes,
        max_tables: 1,
        max_writes: 0,
        max_rows: 0,
        max_values: max_schema_items,
        max_value_bytes: max_encoded_bytes,
        max_overflow_segments: 0,
        max_overflow_bytes: 0,
    };
    let mut decoder = Decoder::from_slice(encoded, limits, true);
    let schema = decoder.table_schema()?;
    decoder.finish()?;
    validate_table_schema(&schema)?;
    Ok(schema)
}

pub(crate) fn encode_relational_row_payload(
    row: &RelationalRow,
) -> Result<Vec<u8>, RelationalError> {
    let mut encoder = Encoder::default();
    encoder.row(row)?;
    Ok(encoder.finish())
}

pub(crate) fn decode_relational_row_payload(
    encoded: &[u8],
    max_values: usize,
    max_value_bytes: usize,
) -> Result<RelationalRow, RelationalError> {
    let limits = RelationalDecodeLimits {
        max_record_bytes: encoded.len(),
        max_tables: 0,
        max_writes: 0,
        max_rows: 1,
        max_values,
        max_value_bytes,
        max_overflow_segments: 0,
        max_overflow_bytes: 0,
    };
    let mut decoder = Decoder::from_slice(encoded, limits, false);
    let row = decoder.row()?;
    decoder.finish()?;
    Ok(row)
}

pub(super) fn validate_relational_table_schema_codec_shape(
    schema: &RelationalTableSchema,
    max_schema_items: usize,
) -> Result<(), RelationalError> {
    validate_table_schema(schema)?;
    for (context, count) in [
        ("columns", schema.columns.len()),
        ("primary-key columns", schema.primary_key.len()),
        ("unique constraints", schema.unique_constraints.len()),
        ("foreign keys", schema.foreign_keys.len()),
        ("indexes", schema.indexes.len()),
    ] {
        if count > max_schema_items {
            return Err(RelationalError::Admission(format!(
                "relational table schema contains {count} {context}, exceeding limit {max_schema_items}"
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalDecodeLimits {
    pub max_record_bytes: usize,
    pub max_tables: usize,
    pub max_writes: usize,
    pub max_rows: usize,
    pub max_values: usize,
    pub max_value_bytes: usize,
    pub max_overflow_segments: usize,
    pub max_overflow_bytes: usize,
}

impl RelationalDecodeLimits {
    pub fn wal() -> Self {
        Self {
            max_record_bytes: DEFAULT_MAX_WAL_RECORD_BYTES,
            max_tables: 1_024,
            max_writes: 100_000,
            max_rows: 100_000,
            max_values: 2_000_000,
            max_value_bytes: 64 * 1024 * 1024,
            max_overflow_segments: 0,
            max_overflow_bytes: 0,
        }
    }

    pub fn checkpoint() -> Self {
        Self {
            max_record_bytes: usize::try_from(DEFAULT_MAX_CHECKPOINT_ENCODED_BYTES)
                .unwrap_or(usize::MAX),
            max_tables: 4_096,
            max_writes: 0,
            max_rows: 10_000_000,
            max_values: 100_000_000,
            max_value_bytes: 64 * 1024 * 1024,
            max_overflow_segments: 10_000_000,
            max_overflow_bytes: usize::try_from(DEFAULT_MAX_CHECKPOINT_ENCODED_BYTES)
                .unwrap_or(usize::MAX),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalWalBatch {
    pub epoch: u64,
    pub transaction: RelationalTransaction,
    pub replay_access: Option<RelationalReplayAccessSet>,
    pub primary_key_changes: Option<RelationalPrimaryKeyChangeCapture>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedRelationalWalBatch {
    pub record: Vec<u8>,
    pub primary_key_changes: RelationalPrimaryKeyChangeCapture,
}

#[derive(Debug, Clone)]
pub struct RelationalCheckpoint {
    pub epoch: u64,
    pub state: RelationalState,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RelationalCheckpointIndexLoad {
    #[default]
    MaterializedPostings,
    OmitMaterializedPostings,
}

pub fn encode_relational_wal_batch(
    epoch: u64,
    transaction: &RelationalTransaction,
) -> Result<Vec<u8>, RelationalError> {
    encode_relational_wal_batch_inner(epoch, transaction, None, None)
}

pub fn encode_relational_wal_batch_with_replay_access(
    epoch: u64,
    transaction: &RelationalTransaction,
    replay_access: Option<&RelationalReplayAccessSet>,
) -> Result<Vec<u8>, RelationalError> {
    encode_relational_wal_batch_inner(epoch, transaction, replay_access, None)
}

pub fn encode_relational_wal_batch_with_captures(
    epoch: u64,
    transaction: &RelationalTransaction,
    replay_access: Option<&RelationalReplayAccessSet>,
    primary_key_changes: &RelationalPrimaryKeyChangeCapture,
) -> Result<EncodedRelationalWalBatch, RelationalError> {
    match encode_relational_wal_batch_inner(
        epoch,
        transaction,
        replay_access,
        Some(primary_key_changes),
    ) {
        Ok(record) => Ok(EncodedRelationalWalBatch {
            record,
            primary_key_changes: primary_key_changes.clone(),
        }),
        Err(exact_error @ RelationalError::Admission(_))
            if matches!(
                primary_key_changes,
                RelationalPrimaryKeyChangeCapture::Captured { .. }
            ) =>
        {
            let fallback = RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                reason: RelationalPrimaryKeyChangeRebuildReason::WalEncodingLimitExceeded,
            };
            match encode_relational_wal_batch_inner(
                epoch,
                transaction,
                replay_access,
                Some(&fallback),
            ) {
                Ok(record) => Ok(EncodedRelationalWalBatch {
                    record,
                    primary_key_changes: fallback,
                }),
                Err(_) => Err(exact_error),
            }
        }
        Err(error) => Err(error),
    }
}

fn encode_relational_wal_batch_inner(
    epoch: u64,
    transaction: &RelationalTransaction,
    replay_access: Option<&RelationalReplayAccessSet>,
    primary_key_changes: Option<&RelationalPrimaryKeyChangeCapture>,
) -> Result<Vec<u8>, RelationalError> {
    let limits = RelationalDecodeLimits::wal();
    if transaction.writes.len() > limits.max_writes {
        return Err(RelationalError::Admission(format!(
            "WAL contains {} writes, exceeding decoder limit {}",
            transaction.writes.len(),
            limits.max_writes
        )));
    }
    if replay_access.is_some_and(|access| access.entries().len() > limits.max_rows) {
        return Err(RelationalError::Admission(format!(
            "WAL replay access set exceeds decoder entry limit {} before WAL append",
            limits.max_rows
        )));
    }
    let mut payload = Encoder::default();
    payload.count(transaction.writes.len(), "WAL writes")?;
    for write in &transaction.writes {
        payload.write(write)?;
    }
    payload.u8(u8::from(replay_access.is_some()));
    if let Some(replay_access) = replay_access {
        payload.count(replay_access.entries().len(), "WAL replay access entries")?;
        for entry in replay_access.entries() {
            payload.string(&entry.table)?;
            payload.key(&entry.primary_key)?;
        }
    }
    encode_primary_key_changes(&mut payload, primary_key_changes, limits)?;
    if payload.value_count > limits.max_values {
        return Err(RelationalError::Admission(format!(
            "WAL contains {} values, exceeding decoder limit {} before WAL append",
            payload.value_count, limits.max_values
        )));
    }
    let payload = payload.finish();
    let record_bytes = HEADER_BYTES.checked_add(payload.len()).ok_or_else(|| {
        RelationalError::Admission("relational WAL record size overflow".to_string())
    })?;
    if record_bytes > limits.max_record_bytes {
        return Err(RelationalError::Admission(format!(
            "relational WAL record contains {record_bytes} bytes, exceeding decoder limit {} before WAL append",
            limits.max_record_bytes
        )));
    }
    encode_envelope(WAL_MAGIC, epoch, payload)
}

fn encode_primary_key_changes(
    payload: &mut Encoder,
    capture: Option<&RelationalPrimaryKeyChangeCapture>,
    limits: RelationalDecodeLimits,
) -> Result<(), RelationalError> {
    match capture {
        None => payload.u8(0),
        Some(RelationalPrimaryKeyChangeCapture::Captured { tables, .. }) => {
            if !tables.windows(2).all(|pair| pair[0].table < pair[1].table) {
                return Err(RelationalError::Corruption(
                    "relational primary-key change tables must be strictly ordered".to_string(),
                ));
            }
            if let Some(table) = tables.iter().find(|table| {
                table.primary_keys.is_empty()
                    || !table.primary_keys.windows(2).all(|pair| pair[0] < pair[1])
            }) {
                return Err(RelationalError::Corruption(format!(
                    "relational primary-key changes for table {} must be non-empty and strictly ordered",
                    table.table
                )));
            }
            let entry_count = tables
                .iter()
                .map(|table| table.primary_keys.len())
                .try_fold(0usize, usize::checked_add)
                .ok_or_else(|| {
                    RelationalError::Admission(
                        "relational primary-key change count overflow".to_string(),
                    )
                })?;
            if tables.len() > limits.max_tables || entry_count > limits.max_rows {
                return Err(RelationalError::Admission(format!(
                    "relational primary-key changes contain {} tables and {entry_count} keys, exceeding decoder limits {}/{}",
                    tables.len(), limits.max_tables, limits.max_rows
                )));
            }
            payload.u8(1);
            payload.count(tables.len(), "relational primary-key change tables")?;
            for table in tables {
                payload.string(&table.table)?;
                payload.count(table.primary_keys.len(), "relational primary-key changes")?;
                for primary_key in &table.primary_keys {
                    payload.key(primary_key)?;
                }
            }
        }
        Some(RelationalPrimaryKeyChangeCapture::RequiresRebuild { reason }) => {
            payload.u8(2);
            payload.u8(primary_key_rebuild_reason_tag(*reason));
        }
    }
    Ok(())
}

const fn primary_key_rebuild_reason_tag(reason: RelationalPrimaryKeyChangeRebuildReason) -> u8 {
    match reason {
        RelationalPrimaryKeyChangeRebuildReason::SchemaRewrite => 0,
        RelationalPrimaryKeyChangeRebuildReason::CaptureLimitExceeded => 1,
        RelationalPrimaryKeyChangeRebuildReason::UnsupportedKeyEncoding => 2,
        RelationalPrimaryKeyChangeRebuildReason::WalEncodingLimitExceeded => 3,
        RelationalPrimaryKeyChangeRebuildReason::MissingWalCapture => 4,
        RelationalPrimaryKeyChangeRebuildReason::SnapshotReplacement => 5,
        RelationalPrimaryKeyChangeRebuildReason::MultipleRelationalTransactions => 6,
    }
}

fn decode_primary_key_rebuild_reason(
    tag: u8,
) -> Result<RelationalPrimaryKeyChangeRebuildReason, RelationalError> {
    match tag {
        0 => Ok(RelationalPrimaryKeyChangeRebuildReason::SchemaRewrite),
        1 => Ok(RelationalPrimaryKeyChangeRebuildReason::CaptureLimitExceeded),
        2 => Ok(RelationalPrimaryKeyChangeRebuildReason::UnsupportedKeyEncoding),
        3 => Ok(RelationalPrimaryKeyChangeRebuildReason::WalEncodingLimitExceeded),
        4 => Ok(RelationalPrimaryKeyChangeRebuildReason::MissingWalCapture),
        5 => Ok(RelationalPrimaryKeyChangeRebuildReason::SnapshotReplacement),
        6 => Ok(RelationalPrimaryKeyChangeRebuildReason::MultipleRelationalTransactions),
        _ => Err(RelationalError::Corruption(format!(
            "unknown relational primary-key rebuild reason tag {tag}"
        ))),
    }
}

pub fn decode_relational_wal_batch(
    bytes: &[u8],
    limits: RelationalDecodeLimits,
) -> Result<RelationalWalBatch, RelationalError> {
    let (epoch, payload) = decode_envelope(bytes, WAL_MAGIC, limits.max_record_bytes)?;
    let mut decoder = Decoder::from_slice(payload, limits, false);
    let write_count = decoder.count(limits.max_writes, "WAL writes")?;
    let mut writes = Vec::with_capacity(write_count);
    for _ in 0..write_count {
        writes.push(decoder.write()?);
    }
    let replay_access = if decoder.boolean("WAL replay access presence")? {
        let entry_count = decoder.count(limits.max_rows, "WAL replay access entries")?;
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            entries.push(RelationalReplayAccess {
                table: decoder.string()?,
                primary_key: decoder.key()?,
            });
        }
        Some(RelationalReplayAccessSet::from_decoded_entries(entries)?)
    } else {
        None
    };
    let primary_key_changes = match decoder.u8()? {
        0 => None,
        1 => {
            const TABLE_FIXED_BYTES: usize = 4;
            const KEY_FIXED_BYTES: usize = 4;
            let table_count =
                decoder.count(limits.max_tables, "relational primary-key change tables")?;
            let mut tables = Vec::with_capacity(table_count);
            let mut encoded_bytes = 0usize;
            let mut entry_count = 0usize;
            for _ in 0..table_count {
                let table = decoder.string()?;
                let key_count = decoder.count(
                    limits.max_rows.saturating_sub(entry_count),
                    "relational primary-key changes",
                )?;
                if key_count == 0 {
                    return Err(RelationalError::Corruption(format!(
                        "relational primary-key change table {table} contains no keys"
                    )));
                }
                entry_count = entry_count.checked_add(key_count).ok_or_else(|| {
                    RelationalError::Corruption(
                        "relational primary-key change count overflow".to_string(),
                    )
                })?;
                encoded_bytes = encoded_bytes
                    .checked_add(TABLE_FIXED_BYTES)
                    .and_then(|bytes| bytes.checked_add(table.len()))
                    .ok_or_else(|| {
                        RelationalError::Corruption(
                            "relational primary-key change byte count overflow".to_string(),
                        )
                    })?;
                let mut primary_keys = Vec::with_capacity(key_count);
                for _ in 0..key_count {
                    let primary_key = decoder.key()?;
                    let key_bytes = super::ordered_key::encode_ordered_relational_key(&primary_key)
                        .map_err(|error| RelationalError::Corruption(error.to_string()))?
                        .len();
                    encoded_bytes = encoded_bytes
                        .checked_add(KEY_FIXED_BYTES)
                        .and_then(|bytes| bytes.checked_add(key_bytes))
                        .ok_or_else(|| {
                            RelationalError::Corruption(
                                "relational primary-key change byte count overflow".to_string(),
                            )
                        })?;
                    primary_keys.push(primary_key);
                }
                if !primary_keys.windows(2).all(|pair| pair[0] < pair[1]) {
                    return Err(RelationalError::Corruption(format!(
                        "relational primary-key changes for table {table} are unordered or duplicated"
                    )));
                }
                tables.push(RelationalTablePrimaryKeyChanges {
                    table,
                    primary_keys,
                });
            }
            if !tables.windows(2).all(|pair| pair[0].table < pair[1].table) {
                return Err(RelationalError::Corruption(
                    "relational primary-key change tables are unordered or duplicated".to_string(),
                ));
            }
            Some(RelationalPrimaryKeyChangeCapture::Captured {
                tables,
                encoded_bytes,
            })
        }
        2 => Some(RelationalPrimaryKeyChangeCapture::RequiresRebuild {
            reason: decode_primary_key_rebuild_reason(decoder.u8()?)?,
        }),
        tag => {
            return Err(RelationalError::Corruption(format!(
                "unknown relational primary-key change capture tag {tag}"
            )))
        }
    };
    decoder.finish()?;
    Ok(RelationalWalBatch {
        epoch,
        transaction: RelationalTransaction { writes },
        replay_access,
        primary_key_changes,
    })
}

pub fn encode_relational_checkpoint(
    epoch: u64,
    state: &RelationalState,
) -> Result<Vec<u8>, RelationalError> {
    let mut cursor = Cursor::new(Vec::new());
    encode_relational_checkpoint_to_writer(
        &mut cursor,
        epoch,
        state,
        RelationalDecodeLimits::checkpoint().max_record_bytes,
    )?;
    Ok(cursor.into_inner())
}

pub fn encode_relational_checkpoint_to_writer<W: Write + Seek>(
    writer: &mut W,
    epoch: u64,
    state: &RelationalState,
    max_record_bytes: usize,
) -> Result<u64, RelationalError> {
    validate_checkpoint_overflow_reachability(state)?;
    if max_record_bytes < HEADER_BYTES {
        return Err(RelationalError::Admission(format!(
            "relational checkpoint max_record_bytes {max_record_bytes} is smaller than its header"
        )));
    }
    writer
        .seek(SeekFrom::Start(HEADER_BYTES as u64))
        .map_err(|error| {
            RelationalError::Durability(format!(
                "failed to reserve relational checkpoint header: {error}"
            ))
        })?;
    let mut payload = CheckpointPayloadWriter::new(writer, max_record_bytes - HEADER_BYTES);

    let mut tables = Encoder::default();
    tables.count(state.schemas.len(), "checkpoint tables")?;
    payload.write_all(&tables.finish())?;
    for (name, schema) in &state.schemas {
        let segment = state.segments.get(name).ok_or_else(|| {
            RelationalError::Corruption(format!("table {name} is missing its row segment"))
        })?;
        let mut table = Encoder::default();
        table.string(name)?;
        table.table_schema(schema)?;
        table.count(segment.rows.len(), "checkpoint rows")?;
        payload.write_all(&table.finish())?;
        for row in segment.rows.values() {
            let mut encoded_row = Encoder::default();
            encoded_row.row(row)?;
            payload.write_all(&encoded_row.finish())?;
        }
    }
    let mut overflow_count = Encoder::default();
    overflow_count.count(
        state.overflow_segments.len(),
        "checkpoint overflow segments",
    )?;
    payload.write_all(&overflow_count.finish())?;
    for (digest, envelope) in &state.overflow_segments {
        let envelope = envelope.read()?;
        if integrity_digest(envelope.as_ref()).sha256 != *digest {
            return Err(RelationalError::Corruption(format!(
                "relational checkpoint overflow segment {digest} has an invalid digest"
            )));
        }
        let mut metadata = Encoder::default();
        metadata.sha256(*digest);
        metadata.u64(u64::try_from(envelope.len()).map_err(|_| {
            RelationalError::Admission("overflow envelope length does not fit u64".to_string())
        })?);
        payload.write_all(&metadata.finish())?;
        payload.write_all(&envelope)?;
    }
    let (payload_len, digest, writer) = payload.finish();
    let total_len = HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(|| RelationalError::Admission("checkpoint size overflow".to_string()))?;
    let mut header = Vec::with_capacity(HEADER_BYTES);
    encode_envelope_header(
        &mut header,
        CHECKPOINT_MAGIC,
        epoch,
        payload_len as u64,
        digest,
    );
    writer.seek(SeekFrom::Start(0)).map_err(|error| {
        RelationalError::Durability(format!(
            "failed to seek relational checkpoint header: {error}"
        ))
    })?;
    writer.write_all(&header).map_err(|error| {
        RelationalError::Durability(format!(
            "failed to write relational checkpoint header: {error}"
        ))
    })?;
    writer
        .seek(SeekFrom::Start(total_len as u64))
        .map_err(|error| {
            RelationalError::Durability(format!("failed to finish relational checkpoint: {error}"))
        })?;
    Ok(total_len as u64)
}

pub fn decode_relational_checkpoint(
    bytes: &[u8],
    limits: RelationalDecodeLimits,
) -> Result<RelationalCheckpoint, RelationalError> {
    decode_relational_checkpoint_with_index_load(
        bytes,
        limits,
        RelationalCheckpointIndexLoad::MaterializedPostings,
    )
}

pub fn decode_relational_checkpoint_with_index_load(
    bytes: &[u8],
    limits: RelationalDecodeLimits,
    index_load: RelationalCheckpointIndexLoad,
) -> Result<RelationalCheckpoint, RelationalError> {
    decode_relational_checkpoint_with_storage(
        bytes,
        limits,
        OverflowDecodeStorage::Inline,
        index_load,
    )
}

pub fn decode_relational_checkpoint_file(
    path: &Path,
    limits: RelationalDecodeLimits,
) -> Result<RelationalCheckpoint, RelationalError> {
    decode_relational_checkpoint_file_with_index_load(
        path,
        limits,
        RelationalCheckpointIndexLoad::MaterializedPostings,
    )
}

pub fn decode_relational_checkpoint_file_with_index_load(
    path: &Path,
    limits: RelationalDecodeLimits,
    index_load: RelationalCheckpointIndexLoad,
) -> Result<RelationalCheckpoint, RelationalError> {
    let encoded_len = std::fs::metadata(path)
        .map_err(|error| {
            RelationalError::Durability(format!(
                "failed to inspect relational checkpoint {}: {error}",
                path.display()
            ))
        })?
        .len();
    if encoded_len > limits.max_record_bytes as u64 {
        return Err(RelationalError::Admission(format!(
            "relational checkpoint {} contains {encoded_len} bytes, exceeding max_record_bytes {}",
            path.display(),
            limits.max_record_bytes
        )));
    }
    let (epoch, input) = FileDecodeInput::open_checkpoint(path, encoded_len, limits)?;
    let mut reader = FileSegmentRangeReader::new();
    reader.register(RELATIONAL_CHECKPOINT_ARTIFACT_ID, path);
    decode_relational_checkpoint_from_decoder(
        epoch,
        Decoder::new(input, limits, false),
        limits,
        OverflowDecodeStorage::File(Arc::new(reader)),
        index_load,
    )
}

enum OverflowDecodeStorage {
    Inline,
    File(Arc<FileSegmentRangeReader>),
}

fn decode_relational_checkpoint_with_storage(
    bytes: &[u8],
    limits: RelationalDecodeLimits,
    storage: OverflowDecodeStorage,
    index_load: RelationalCheckpointIndexLoad,
) -> Result<RelationalCheckpoint, RelationalError> {
    let (epoch, payload) = decode_envelope(bytes, CHECKPOINT_MAGIC, limits.max_record_bytes)?;
    decode_relational_checkpoint_from_decoder(
        epoch,
        Decoder::from_slice(payload, limits, true),
        limits,
        storage,
        index_load,
    )
}

fn decode_relational_checkpoint_from_decoder<I: DecodeInput>(
    epoch: u64,
    mut decoder: Decoder<I>,
    limits: RelationalDecodeLimits,
    storage: OverflowDecodeStorage,
    index_load: RelationalCheckpointIndexLoad,
) -> Result<RelationalCheckpoint, RelationalError> {
    let table_count = decoder.count(limits.max_tables, "checkpoint tables")?;
    let mut state = RelationalState::default();
    for _ in 0..table_count {
        let name = decoder.string()?;
        let schema = decoder.table_schema()?;
        if name != schema.name || state.schemas.contains_key(&name) {
            return Err(RelationalError::Corruption(format!(
                "checkpoint has a duplicate or mismatched table {name}"
            )));
        }
        validate_table_schema(&schema)?;
        let row_count = decoder.row_count()?;
        let primary_key = column_positions(&schema, &schema.primary_key)?;
        let mut rows = BTreeMap::new();
        for _ in 0..row_count {
            let row = decoder.row()?;
            validate_row(&schema, &row)?;
            let key = super::row_key(&row, &primary_key);
            if rows.insert(key, row).is_some() {
                return Err(RelationalError::Corruption(format!(
                    "checkpoint table {name} contains duplicate primary keys"
                )));
            }
        }
        state.schemas.insert(name.clone(), Arc::new(schema));
        state.segments.insert(
            name,
            Arc::new(RelationalTableSegment {
                rows: super::RelationalRowPages::from_map(rows),
                indexes: BTreeMap::new(),
            }),
        );
    }
    let overflow_count =
        decoder.count(limits.max_overflow_segments, "checkpoint overflow segments")?;
    for ordinal in 0..overflow_count {
        let digest = decoder.sha256()?;
        let overflow = decoder.overflow_segment()?;
        if overflow.digest.sha256 != digest {
            return Err(RelationalError::Corruption(format!(
                "checkpoint overflow segment {digest} has an invalid digest"
            )));
        }
        let segment = match &storage {
            OverflowDecodeStorage::Inline => {
                RelationalOverflowSegment::Inline(Arc::from(overflow.bytes.ok_or_else(|| {
                    RelationalError::Corruption(
                        "inline checkpoint decoder discarded overflow bytes".to_string(),
                    )
                })?))
            }
            OverflowDecodeStorage::File(reader) => {
                let offset = HEADER_BYTES
                    .checked_add(overflow.payload_offset)
                    .ok_or_else(|| {
                        RelationalError::Corruption(
                            "checkpoint overflow file offset overflow".to_string(),
                        )
                    })?;
                let length = NonZeroU64::new(u64::try_from(overflow.len).map_err(|_| {
                    RelationalError::Corruption(
                        "checkpoint overflow length does not fit u64".to_string(),
                    )
                })?)
                .ok_or_else(|| {
                    RelationalError::Corruption(
                        "checkpoint contains an empty overflow segment".to_string(),
                    )
                })?;
                let range = SegmentReadRange::new(
                    RELATIONAL_CHECKPOINT_ARTIFACT_ID,
                    ordinal as u64,
                    offset as u64,
                    length,
                )
                .with_content_digest(ContentDigest(overflow.digest.crc32c.as_u64()));
                RelationalOverflowSegment::FileRange {
                    reader: Arc::clone(reader),
                    range,
                }
            }
        };
        if state.overflow_segments.insert(digest, segment).is_some() {
            return Err(RelationalError::Corruption(format!(
                "checkpoint contains duplicate overflow segment {digest}"
            )));
        }
    }
    decoder.finish()?;
    validate_checkpoint_overflow_reachability(&state)?;
    if index_load == RelationalCheckpointIndexLoad::MaterializedPostings {
        let table_names = state.schemas.keys().cloned().collect::<Vec<_>>();
        for table in table_names {
            rebuild_indexes(&mut state, &table)?;
        }
    } else {
        state.materialized_index_postings_resident = false;
    }
    validate_foreign_keys(&state)?;
    Ok(RelationalCheckpoint { epoch, state })
}

fn validate_checkpoint_overflow_reachability(
    state: &RelationalState,
) -> Result<(), RelationalError> {
    let reachable = state
        .segments
        .values()
        .flat_map(|segment| segment.rows.values())
        .flat_map(|row| row.values())
        .filter_map(|value| match value {
            RelationalValue::Overflow(reference) => Some(&reference.digest),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    for digest in &reachable {
        if !state.overflow_segments.contains_key(*digest) {
            return Err(RelationalError::Corruption(format!(
                "checkpoint row references missing overflow segment {digest}"
            )));
        }
    }
    if reachable.len() != state.overflow_segments.len() {
        return Err(RelationalError::Corruption(
            "checkpoint contains unreachable overflow segments".to_string(),
        ));
    }
    Ok(())
}

struct CheckpointPayloadWriter<'a, W> {
    writer: &'a mut W,
    max_payload_bytes: usize,
    payload_bytes: usize,
    hasher: IntegrityHasher,
}

impl<'a, W: Write> CheckpointPayloadWriter<'a, W> {
    fn new(writer: &'a mut W, max_payload_bytes: usize) -> Self {
        Self {
            writer,
            max_payload_bytes,
            payload_bytes: 0,
            hasher: IntegrityHasher::new(),
        }
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<(), RelationalError> {
        let next = self.payload_bytes.checked_add(bytes.len()).ok_or_else(|| {
            RelationalError::Admission("relational checkpoint size overflow".to_string())
        })?;
        if next > self.max_payload_bytes {
            return Err(RelationalError::Admission(format!(
                "relational checkpoint payload contains {next} bytes, exceeding limit {}",
                self.max_payload_bytes
            )));
        }
        self.writer.write_all(bytes).map_err(|error| {
            RelationalError::Durability(format!(
                "failed to stream relational checkpoint payload: {error}"
            ))
        })?;
        self.hasher.update(bytes);
        self.payload_bytes = next;
        Ok(())
    }

    fn finish(self) -> (usize, hawdb_integrity::IntegrityDigest, &'a mut W) {
        (self.payload_bytes, self.hasher.finish(), self.writer)
    }
}

fn encode_envelope(
    magic: &[u8; 8],
    epoch: u64,
    payload: Vec<u8>,
) -> Result<Vec<u8>, RelationalError> {
    let payload_len = u64::try_from(payload.len()).map_err(|_| {
        RelationalError::Admission("relational durable payload is too large".to_string())
    })?;
    let digest = integrity_digest(&payload);
    let total = HEADER_BYTES.checked_add(payload.len()).ok_or_else(|| {
        RelationalError::Admission("relational durable record size overflow".to_string())
    })?;
    let mut output = Vec::with_capacity(total);
    encode_envelope_header(&mut output, magic, epoch, payload_len, digest);
    output.extend_from_slice(&payload);
    Ok(output)
}

fn encode_envelope_header(
    output: &mut Vec<u8>,
    magic: &[u8; 8],
    epoch: u64,
    payload_len: u64,
    digest: hawdb_integrity::IntegrityDigest,
) {
    output.extend_from_slice(magic);
    output.extend_from_slice(&CODEC_VERSION.to_le_bytes());
    output.extend_from_slice(&0_u16.to_le_bytes());
    output.extend_from_slice(&epoch.to_le_bytes());
    output.extend_from_slice(&payload_len.to_le_bytes());
    output.extend_from_slice(&digest.crc32c.get().to_le_bytes());
    output.extend_from_slice(digest.sha256.as_bytes());
    debug_assert_eq!(output.len(), HEADER_BYTES);
}

fn decode_envelope<'a>(
    bytes: &'a [u8],
    expected_magic: &[u8; 8],
    max_record_bytes: usize,
) -> Result<(u64, &'a [u8]), RelationalError> {
    if bytes.len() > max_record_bytes {
        return Err(RelationalError::Admission(format!(
            "relational durable record contains {} bytes, exceeding max_record_bytes {max_record_bytes}",
            bytes.len()
        )));
    }
    if bytes.len() < HEADER_BYTES || &bytes[..8] != expected_magic {
        return Err(RelationalError::Corruption(
            "invalid relational durable record header".to_string(),
        ));
    }
    let version = u16::from_le_bytes(bytes[8..10].try_into().expect("fixed header"));
    let flags = u16::from_le_bytes(bytes[10..12].try_into().expect("fixed header"));
    if version != CODEC_VERSION || flags != 0 {
        return Err(RelationalError::Corruption(format!(
            "unsupported relational durable codec version {version} or flags {flags}"
        )));
    }
    let epoch = u64::from_le_bytes(bytes[12..20].try_into().expect("fixed header"));
    let payload_len = usize::try_from(u64::from_le_bytes(
        bytes[20..28].try_into().expect("fixed header"),
    ))
    .map_err(|_| RelationalError::Corruption("durable payload length overflows usize".into()))?;
    let expected_len = HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(|| RelationalError::Corruption("durable record length overflow".to_string()))?;
    if bytes.len() != expected_len {
        return Err(RelationalError::Corruption(format!(
            "relational durable record length mismatch: expected {expected_len}, got {}",
            bytes.len()
        )));
    }
    let payload = &bytes[HEADER_BYTES..];
    let digest = integrity_digest(payload);
    let expected_crc = u32::from_le_bytes(bytes[28..32].try_into().expect("fixed header"));
    if digest.crc32c.get() != expected_crc
        || digest.sha256.as_bytes() != &bytes[32..32 + SHA256_BYTES]
    {
        return Err(RelationalError::Corruption(
            "relational durable record checksum mismatch".to_string(),
        ));
    }
    Ok((epoch, payload))
}

#[derive(Default)]
struct Encoder {
    bytes: Vec<u8>,
    value_count: usize,
}

impl Encoder {
    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn count(&mut self, count: usize, context: &str) -> Result<(), RelationalError> {
        let count = u32::try_from(count).map_err(|_| {
            RelationalError::Admission(format!("{context} count exceeds durable codec limit"))
        })?;
        self.u32(count);
        Ok(())
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn sha256(&mut self, digest: Sha256Digest) {
        self.bytes.extend_from_slice(digest.as_bytes());
    }

    fn string(&mut self, value: &str) -> Result<(), RelationalError> {
        self.bytes(value.as_bytes())
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), RelationalError> {
        let len = u64::try_from(value.len()).map_err(|_| {
            RelationalError::Admission("durable byte string is too large".to_string())
        })?;
        self.u64(len);
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn string_list(&mut self, values: &[String]) -> Result<(), RelationalError> {
        self.count(values.len(), "string list")?;
        for value in values {
            self.string(value)?;
        }
        Ok(())
    }

    fn scalar_type(&mut self, scalar_type: RelationalScalarType) {
        self.u8(match scalar_type {
            RelationalScalarType::Boolean => 1,
            RelationalScalarType::BigInt => 2,
            RelationalScalarType::DoublePrecision => 3,
            RelationalScalarType::Text => 4,
            RelationalScalarType::Bytea => 5,
            RelationalScalarType::Uuid => 6,
        });
    }

    fn value(&mut self, value: &RelationalValue) -> Result<(), RelationalError> {
        self.value_count = self.value_count.checked_add(1).ok_or_else(|| {
            RelationalError::Admission("encoded value count overflow".to_string())
        })?;
        match value {
            RelationalValue::Null => self.u8(0),
            RelationalValue::Boolean(value) => {
                self.u8(1);
                self.u8(u8::from(*value));
            }
            RelationalValue::BigInt(value) => {
                self.u8(2);
                self.bytes.extend_from_slice(&value.to_le_bytes());
            }
            RelationalValue::DoublePrecision(value) => {
                self.u8(3);
                self.u64(value.to_bits());
            }
            RelationalValue::Text(value) => {
                self.u8(4);
                self.string(value)?;
            }
            RelationalValue::Bytea(value) => {
                self.u8(5);
                self.bytes(value)?;
            }
            RelationalValue::Uuid(value) => {
                self.u8(7);
                self.bytes.extend_from_slice(value.as_bytes());
            }
            RelationalValue::Overflow(reference) => {
                self.u8(6);
                self.sha256(reference.digest);
                self.scalar_type(reference.scalar_type);
                self.u64(reference.compressed_bytes);
                self.u64(reference.uncompressed_bytes);
            }
        }
        Ok(())
    }

    fn row(&mut self, row: &RelationalRow) -> Result<(), RelationalError> {
        self.count(row.values().len(), "row values")?;
        for value in row.values() {
            self.value(value)?;
        }
        Ok(())
    }

    fn key(&mut self, key: &RelationalKey) -> Result<(), RelationalError> {
        self.count(key.0.len(), "key values")?;
        for value in &key.0 {
            self.value(value)?;
        }
        Ok(())
    }

    fn table_schema(&mut self, schema: &RelationalTableSchema) -> Result<(), RelationalError> {
        self.string(&schema.name)?;
        self.count(schema.columns.len(), "table columns")?;
        for column in &schema.columns {
            self.string(&column.name)?;
            self.scalar_type(column.scalar_type);
            self.u8(u8::from(column.nullable));
            self.column_default(&column.default)?;
        }
        self.string_list(&schema.primary_key)?;
        self.count(schema.unique_constraints.len(), "unique constraints")?;
        for columns in &schema.unique_constraints {
            self.string_list(columns)?;
        }
        self.count(schema.foreign_keys.len(), "foreign keys")?;
        for foreign_key in &schema.foreign_keys {
            self.foreign_key(foreign_key)?;
        }
        self.count(schema.indexes.len(), "indexes")?;
        for index in &schema.indexes {
            self.index(index)?;
        }
        Ok(())
    }

    fn foreign_key(
        &mut self,
        foreign_key: &RelationalForeignKeySchema,
    ) -> Result<(), RelationalError> {
        self.string_list(&foreign_key.columns)?;
        self.string(&foreign_key.referenced_table)?;
        self.string_list(&foreign_key.referenced_columns)?;
        self.referential_action(foreign_key.on_delete);
        self.referential_action(foreign_key.on_update);
        Ok(())
    }

    fn column_default(
        &mut self,
        default: &Option<RelationalColumnDefault>,
    ) -> Result<(), RelationalError> {
        match default {
            None => self.u8(0),
            Some(RelationalColumnDefault::Literal(value)) => {
                self.u8(1);
                self.value(value)?;
            }
            Some(RelationalColumnDefault::UuidV7) => self.u8(2),
        }
        Ok(())
    }

    fn column_default_logical(
        &mut self,
        default: &Option<RelationalColumnDefault>,
    ) -> Result<(), RelationalError> {
        match default {
            None => self.u8(0),
            Some(RelationalColumnDefault::Literal(value)) => {
                self.u8(1);
                self.logical_value(value)?;
            }
            Some(RelationalColumnDefault::UuidV7) => self.u8(2),
        }
        Ok(())
    }

    fn referential_action(&mut self, action: RelationalReferentialAction) {
        self.u8(match action {
            RelationalReferentialAction::NoAction => 0,
            RelationalReferentialAction::Restrict => 1,
            RelationalReferentialAction::Cascade => 2,
        });
    }

    fn index(&mut self, index: &RelationalIndexSchema) -> Result<(), RelationalError> {
        self.string(&index.name)?;
        self.string_list(&index.columns)?;
        self.u8(u8::from(index.unique));
        Ok(())
    }

    fn write(&mut self, write: &RelationalWrite) -> Result<(), RelationalError> {
        match write {
            RelationalWrite::CreateTable(schema) => {
                self.u8(1);
                self.table_schema(schema)?;
            }
            RelationalWrite::AddColumn { table, column } => {
                self.u8(8);
                self.string(table)?;
                self.string(&column.name)?;
                self.scalar_type(column.scalar_type);
                self.u8(u8::from(column.nullable));
                self.column_default_logical(&column.default)?;
            }
            RelationalWrite::CreateIndex { table, index } => {
                self.u8(2);
                self.string(table)?;
                self.index(index)?;
            }
            RelationalWrite::Insert { table, rows, mode } => {
                self.u8(3);
                self.string(table)?;
                self.u8(match mode {
                    RelationalInsertMode::Error => 0,
                    RelationalInsertMode::Replace => 1,
                });
                self.count(rows.len(), "insert rows")?;
                for row in rows {
                    if row
                        .values()
                        .iter()
                        .any(|value| matches!(value, RelationalValue::Overflow(_)))
                    {
                        return Err(RelationalError::Durability(
                            "WAL transactions must contain logical values, not overflow references"
                                .to_string(),
                        ));
                    }
                    self.row(row)?;
                }
            }
            RelationalWrite::Upsert {
                table,
                rows,
                conflict_columns,
                action,
            } => {
                self.u8(5);
                self.string(table)?;
                self.string_list(conflict_columns)?;
                match action {
                    RelationalConflictAction::DoNothing => self.u8(0),
                    RelationalConflictAction::Update(assignments) => {
                        self.u8(1);
                        self.count(assignments.len(), "upsert assignments")?;
                        for assignment in assignments {
                            self.string(&assignment.column)?;
                            match &assignment.value {
                                RelationalUpsertValue::ExcludedColumn(column) => {
                                    self.u8(0);
                                    self.string(column)?;
                                }
                                RelationalUpsertValue::Value(value) => {
                                    self.u8(1);
                                    self.logical_value(value)?;
                                }
                            }
                        }
                    }
                }
                self.count(rows.len(), "upsert rows")?;
                for row in rows {
                    if row
                        .values()
                        .iter()
                        .any(|value| matches!(value, RelationalValue::Overflow(_)))
                    {
                        return Err(RelationalError::Durability(
                            "WAL transactions must contain logical values, not overflow references"
                                .to_string(),
                        ));
                    }
                    self.row(row)?;
                }
            }
            RelationalWrite::DeleteByPrimaryKey { table, keys } => {
                self.u8(4);
                self.string(table)?;
                self.count(keys.len(), "delete keys")?;
                for key in keys {
                    self.count(key.0.len(), "key values")?;
                    for value in &key.0 {
                        self.value(value)?;
                    }
                }
            }
            RelationalWrite::DeleteWhere { table, predicate } => {
                self.u8(6);
                self.string(table)?;
                self.predicate(predicate)?;
            }
            RelationalWrite::UpdateWhere {
                table,
                assignments,
                predicate,
            } => {
                self.u8(7);
                self.string(table)?;
                self.count(assignments.len(), "update assignments")?;
                for assignment in assignments {
                    self.string(&assignment.column)?;
                    match &assignment.value {
                        RelationalUpdateValue::Column(column) => {
                            self.u8(0);
                            self.string(column)?;
                        }
                        RelationalUpdateValue::Value(value) => {
                            self.u8(1);
                            self.logical_value(value)?;
                        }
                        RelationalUpdateValue::BigIntArithmetic {
                            left,
                            operator,
                            right,
                        } => {
                            self.u8(2);
                            self.bigint_arithmetic_operand(left)?;
                            self.u8(match operator {
                                RelationalBigIntArithmeticOperator::Add => 0,
                                RelationalBigIntArithmeticOperator::Subtract => 1,
                            });
                            self.bigint_arithmetic_operand(right)?;
                        }
                    }
                }
                self.predicate(predicate)?;
            }
        }
        Ok(())
    }

    fn logical_value(&mut self, value: &RelationalValue) -> Result<(), RelationalError> {
        if matches!(value, RelationalValue::Overflow(_)) {
            return Err(RelationalError::Durability(
                "WAL predicates and assignments must contain logical values".to_string(),
            ));
        }
        self.value(value)
    }

    fn bigint_arithmetic_operand(
        &mut self,
        operand: &RelationalBigIntOperand,
    ) -> Result<(), RelationalError> {
        match operand {
            RelationalBigIntOperand::Column(column) => {
                self.u8(0);
                self.string(column)
            }
            RelationalBigIntOperand::Value(value) => {
                self.u8(1);
                self.logical_value(value)
            }
        }
    }

    fn predicate(&mut self, predicate: &RelationalPredicate) -> Result<(), RelationalError> {
        match predicate {
            RelationalPredicate::And(left, right) => {
                self.u8(0);
                self.predicate(left)?;
                self.predicate(right)?;
            }
            RelationalPredicate::Or(left, right) => {
                self.u8(1);
                self.predicate(left)?;
                self.predicate(right)?;
            }
            RelationalPredicate::Not(predicate) => {
                self.u8(2);
                self.predicate(predicate)?;
            }
            RelationalPredicate::Compare { column, op, value } => {
                self.u8(3);
                self.string(column)?;
                self.u8(match op {
                    RelationalComparisonOp::Eq => 0,
                    RelationalComparisonOp::NotEq => 1,
                    RelationalComparisonOp::Lt => 2,
                    RelationalComparisonOp::Lte => 3,
                    RelationalComparisonOp::Gt => 4,
                    RelationalComparisonOp::Gte => 5,
                });
                self.logical_value(value)?;
            }
            RelationalPredicate::IsNull { column, negated } => {
                self.u8(4);
                self.string(column)?;
                self.u8(u8::from(*negated));
            }
        }
        Ok(())
    }
}

trait DecodeInput {
    fn len(&self) -> usize;
    fn position(&self) -> usize;
    fn read_exact(&mut self, output: &mut [u8]) -> Result<(), RelationalError>;
    fn finish(self) -> Result<(), RelationalError>;
}

struct SliceDecodeInput<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl DecodeInput for SliceDecodeInput<'_> {
    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn position(&self) -> usize {
        self.offset
    }

    fn read_exact(&mut self, output: &mut [u8]) -> Result<(), RelationalError> {
        let end = self.offset.checked_add(output.len()).ok_or_else(|| {
            RelationalError::Corruption("durable decoder offset overflow".to_string())
        })?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(|| {
            RelationalError::Corruption("truncated relational durable payload".to_string())
        })?;
        output.copy_from_slice(bytes);
        self.offset = end;
        Ok(())
    }

    fn finish(self) -> Result<(), RelationalError> {
        Ok(())
    }
}

struct FileDecodeInput {
    file: File,
    payload_len: usize,
    offset: usize,
    expected_crc32c: u32,
    expected_sha256: [u8; SHA256_BYTES],
    hasher: IntegrityHasher,
}

impl FileDecodeInput {
    fn open_checkpoint(
        path: &Path,
        encoded_len: u64,
        limits: RelationalDecodeLimits,
    ) -> Result<(u64, Self), RelationalError> {
        let mut file = File::open(path).map_err(|error| {
            RelationalError::Durability(format!(
                "failed to open relational checkpoint {}: {error}",
                path.display()
            ))
        })?;
        let mut header = [0_u8; HEADER_BYTES];
        file.read_exact(&mut header).map_err(|error| {
            RelationalError::Corruption(format!(
                "failed to read relational checkpoint header {}: {error}",
                path.display()
            ))
        })?;
        if &header[..8] != CHECKPOINT_MAGIC {
            return Err(RelationalError::Corruption(
                "invalid relational durable record header".to_string(),
            ));
        }
        let version = u16::from_le_bytes(header[8..10].try_into().expect("fixed header"));
        let flags = u16::from_le_bytes(header[10..12].try_into().expect("fixed header"));
        if version != CODEC_VERSION || flags != 0 {
            return Err(RelationalError::Corruption(format!(
                "unsupported relational durable codec version {version} or flags {flags}"
            )));
        }
        let epoch = u64::from_le_bytes(header[12..20].try_into().expect("fixed header"));
        let payload_len = usize::try_from(u64::from_le_bytes(
            header[20..28].try_into().expect("fixed header"),
        ))
        .map_err(|_| {
            RelationalError::Corruption("durable payload length overflows usize".into())
        })?;
        let expected_len = HEADER_BYTES
            .checked_add(payload_len)
            .ok_or_else(|| RelationalError::Corruption("durable record length overflow".into()))?;
        if encoded_len != expected_len as u64 || expected_len > limits.max_record_bytes {
            return Err(RelationalError::Corruption(format!(
                "relational durable record length mismatch: expected {expected_len}, got {encoded_len}"
            )));
        }
        let expected_crc32c = u32::from_le_bytes(header[28..32].try_into().expect("fixed header"));
        let expected_sha256 = header[32..32 + SHA256_BYTES]
            .try_into()
            .expect("fixed SHA-256 digest");
        Ok((
            epoch,
            Self {
                file,
                payload_len,
                offset: 0,
                expected_crc32c,
                expected_sha256,
                hasher: IntegrityHasher::new(),
            },
        ))
    }
}

impl DecodeInput for FileDecodeInput {
    fn len(&self) -> usize {
        self.payload_len
    }

    fn position(&self) -> usize {
        self.offset
    }

    fn read_exact(&mut self, output: &mut [u8]) -> Result<(), RelationalError> {
        let end = self.offset.checked_add(output.len()).ok_or_else(|| {
            RelationalError::Corruption("durable decoder offset overflow".to_string())
        })?;
        if end > self.payload_len {
            return Err(RelationalError::Corruption(
                "truncated relational durable payload".to_string(),
            ));
        }
        self.file.read_exact(output).map_err(|error| {
            RelationalError::Corruption(format!(
                "failed to read relational durable payload: {error}"
            ))
        })?;
        self.hasher.update(output);
        self.offset = end;
        Ok(())
    }

    fn finish(self) -> Result<(), RelationalError> {
        let digest = self.hasher.finish();
        if digest.crc32c.get() != self.expected_crc32c
            || digest.sha256.as_bytes() != &self.expected_sha256
        {
            return Err(RelationalError::Corruption(
                "relational durable record checksum mismatch".to_string(),
            ));
        }
        Ok(())
    }
}

struct DecodedOverflowSegment {
    payload_offset: usize,
    len: usize,
    bytes: Option<Vec<u8>>,
    digest: hawdb_integrity::IntegrityDigest,
}

struct Decoder<I> {
    input: I,
    limits: RelationalDecodeLimits,
    rows: usize,
    values: usize,
    value_bytes: usize,
    overflow_bytes: usize,
    retain_overflow_bytes: bool,
}

impl<'a> Decoder<SliceDecodeInput<'a>> {
    fn from_slice(
        bytes: &'a [u8],
        limits: RelationalDecodeLimits,
        retain_overflow_bytes: bool,
    ) -> Self {
        Self::new(
            SliceDecodeInput { bytes, offset: 0 },
            limits,
            retain_overflow_bytes,
        )
    }
}

impl<I: DecodeInput> Decoder<I> {
    fn new(input: I, limits: RelationalDecodeLimits, retain_overflow_bytes: bool) -> Self {
        Self {
            input,
            limits,
            rows: 0,
            values: 0,
            value_bytes: 0,
            overflow_bytes: 0,
            retain_overflow_bytes,
        }
    }

    fn finish(self) -> Result<(), RelationalError> {
        if self.input.position() != self.input.len() {
            return Err(RelationalError::Corruption(format!(
                "relational durable payload has {} trailing bytes",
                self.input.len() - self.input.position()
            )));
        }
        self.input.finish()
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], RelationalError> {
        let mut bytes = [0_u8; N];
        self.input.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, RelationalError> {
        Ok(self.fixed::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, RelationalError> {
        Ok(u32::from_le_bytes(self.fixed()?))
    }

    fn u64(&mut self) -> Result<u64, RelationalError> {
        Ok(u64::from_le_bytes(self.fixed()?))
    }

    fn sha256(&mut self) -> Result<Sha256Digest, RelationalError> {
        Ok(Sha256Digest::from_bytes(self.fixed()?))
    }

    fn count(&mut self, max: usize, context: &str) -> Result<usize, RelationalError> {
        let count = self.u32()? as usize;
        if count > max {
            return Err(RelationalError::Admission(format!(
                "decoded {context} count {count} exceeds limit {max}"
            )));
        }
        Ok(count)
    }

    fn bounded_bytes(&mut self, max: usize, context: &str) -> Result<Vec<u8>, RelationalError> {
        let len = usize::try_from(self.u64()?).map_err(|_| {
            RelationalError::Corruption(format!("decoded {context} length overflows usize"))
        })?;
        if len > max {
            return Err(RelationalError::Admission(format!(
                "decoded {context} contains {len} bytes, exceeding limit {max}"
            )));
        }
        let mut bytes = vec![0_u8; len];
        self.input.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn string(&mut self) -> Result<String, RelationalError> {
        let bytes = self.bounded_bytes(self.limits.max_value_bytes, "string")?;
        self.value_bytes = self.value_bytes.checked_add(bytes.len()).ok_or_else(|| {
            RelationalError::Admission("decoded value byte count overflow".to_string())
        })?;
        if self.value_bytes > self.limits.max_record_bytes {
            return Err(RelationalError::Admission(
                "decoded string bytes exceed record budget".to_string(),
            ));
        }
        String::from_utf8(bytes).map_err(|error| {
            RelationalError::Corruption(format!("durable string is not valid UTF-8: {error}"))
        })
    }

    fn overflow_segment(&mut self) -> Result<DecodedOverflowSegment, RelationalError> {
        let remaining = self
            .limits
            .max_overflow_bytes
            .saturating_sub(self.overflow_bytes);
        let len = usize::try_from(self.u64()?).map_err(|_| {
            RelationalError::Corruption(
                "decoded overflow segment length overflows usize".to_string(),
            )
        })?;
        if len > remaining {
            return Err(RelationalError::Admission(format!(
                "decoded overflow segment contains {len} bytes, exceeding remaining overflow budget {remaining}"
            )));
        }
        let payload_offset = self.input.position();
        let mut bytes = self.retain_overflow_bytes.then(|| Vec::with_capacity(len));
        let mut hasher = IntegrityHasher::new();
        let mut remaining = len;
        let mut chunk = [0_u8; 64 * 1024];
        while remaining != 0 {
            let chunk_len = remaining.min(chunk.len());
            self.input.read_exact(&mut chunk[..chunk_len])?;
            hasher.update(&chunk[..chunk_len]);
            if let Some(bytes) = &mut bytes {
                bytes.extend_from_slice(&chunk[..chunk_len]);
            }
            remaining -= chunk_len;
        }
        self.overflow_bytes += len;
        Ok(DecodedOverflowSegment {
            payload_offset,
            len,
            bytes,
            digest: hasher.finish(),
        })
    }

    fn string_list(&mut self) -> Result<Vec<String>, RelationalError> {
        let count = self.count(self.limits.max_values, "string list")?;
        (0..count).map(|_| self.string()).collect()
    }

    fn scalar_type(&mut self) -> Result<RelationalScalarType, RelationalError> {
        match self.u8()? {
            1 => Ok(RelationalScalarType::Boolean),
            2 => Ok(RelationalScalarType::BigInt),
            3 => Ok(RelationalScalarType::DoublePrecision),
            4 => Ok(RelationalScalarType::Text),
            5 => Ok(RelationalScalarType::Bytea),
            6 => Ok(RelationalScalarType::Uuid),
            tag => Err(RelationalError::Corruption(format!(
                "invalid relational scalar type tag {tag}"
            ))),
        }
    }

    fn value(&mut self) -> Result<RelationalValue, RelationalError> {
        self.values = self.values.checked_add(1).ok_or_else(|| {
            RelationalError::Admission("decoded value count overflow".to_string())
        })?;
        if self.values > self.limits.max_values {
            return Err(RelationalError::Admission(format!(
                "decoded value count exceeds limit {}",
                self.limits.max_values
            )));
        }
        match self.u8()? {
            0 => Ok(RelationalValue::Null),
            1 => match self.u8()? {
                0 => Ok(RelationalValue::Boolean(false)),
                1 => Ok(RelationalValue::Boolean(true)),
                tag => Err(RelationalError::Corruption(format!(
                    "invalid boolean tag {tag}"
                ))),
            },
            2 => Ok(RelationalValue::BigInt(i64::from_le_bytes(self.fixed()?))),
            3 => Ok(RelationalValue::DoublePrecision(f64::from_bits(
                self.u64()?,
            ))),
            4 => Ok(RelationalValue::Text(self.string()?)),
            5 => {
                let bytes = self.bounded_bytes(self.limits.max_value_bytes, "BYTEA value")?;
                self.value_bytes = self.value_bytes.checked_add(bytes.len()).ok_or_else(|| {
                    RelationalError::Admission("decoded value byte count overflow".to_string())
                })?;
                Ok(RelationalValue::Bytea(bytes))
            }
            6 => {
                let digest = self.sha256()?;
                let scalar_type = self.scalar_type()?;
                let compressed_bytes = self.u64()?;
                let uncompressed_bytes = self.u64()?;
                let max_value_bytes = u64::try_from(self.limits.max_value_bytes).map_err(|_| {
                    RelationalError::Admission(
                        "relational value limit does not fit u64".to_string(),
                    )
                })?;
                if uncompressed_bytes == 0
                    || compressed_bytes == 0
                    || uncompressed_bytes > max_value_bytes
                    || compressed_bytes > max_value_bytes
                {
                    return Err(RelationalError::Admission(format!(
                        "overflow reference contains {compressed_bytes} compressed and {uncompressed_bytes} decoded bytes, outside limit {}",
                        self.limits.max_value_bytes
                    )));
                }
                Ok(RelationalValue::Overflow(RelationalOverflowRef {
                    digest,
                    scalar_type,
                    compressed_bytes,
                    uncompressed_bytes,
                }))
            }
            7 => Ok(RelationalValue::Uuid(Uuid::from_bytes(self.fixed()?))),
            tag => Err(RelationalError::Corruption(format!(
                "invalid relational value tag {tag}"
            ))),
        }
    }

    fn row_count(&mut self) -> Result<usize, RelationalError> {
        let count = self.count(self.limits.max_rows.saturating_sub(self.rows), "rows")?;
        self.rows += count;
        Ok(count)
    }

    fn row(&mut self) -> Result<RelationalRow, RelationalError> {
        let count = self.count(
            self.limits.max_values.saturating_sub(self.values),
            "row values",
        )?;
        let values = (0..count)
            .map(|_| self.value())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RelationalRow::new(values))
    }

    fn key(&mut self) -> Result<RelationalKey, RelationalError> {
        let value_count = self.count(
            self.limits.max_values.saturating_sub(self.values),
            "key values",
        )?;
        let values = (0..value_count)
            .map(|_| self.value())
            .collect::<Result<_, _>>()?;
        Ok(RelationalKey(values))
    }

    fn table_schema(&mut self) -> Result<RelationalTableSchema, RelationalError> {
        let name = self.string()?;
        let column_count = self.count(self.limits.max_values, "table columns")?;
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let name = self.string()?;
            let scalar_type = self.scalar_type()?;
            let nullable = self.boolean("column nullable")?;
            let default = self.column_default()?;
            columns.push(RelationalColumnSchema {
                name,
                scalar_type,
                nullable,
                default,
            });
        }
        let primary_key = self.string_list()?;
        let unique_count = self.count(self.limits.max_values, "unique constraints")?;
        let unique_constraints = (0..unique_count)
            .map(|_| self.string_list())
            .collect::<Result<_, _>>()?;
        let foreign_key_count = self.count(self.limits.max_values, "foreign keys")?;
        let foreign_keys = (0..foreign_key_count)
            .map(|_| self.foreign_key())
            .collect::<Result<_, _>>()?;
        let index_count = self.count(self.limits.max_values, "indexes")?;
        let indexes = (0..index_count)
            .map(|_| self.index())
            .collect::<Result<_, _>>()?;
        Ok(RelationalTableSchema {
            name,
            columns,
            primary_key,
            unique_constraints,
            foreign_keys,
            indexes,
        })
    }

    fn boolean(&mut self, context: &str) -> Result<bool, RelationalError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            tag => Err(RelationalError::Corruption(format!(
                "invalid {context} tag {tag}"
            ))),
        }
    }

    fn column_default(&mut self) -> Result<Option<RelationalColumnDefault>, RelationalError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(RelationalColumnDefault::Literal(self.value()?))),
            2 => Ok(Some(RelationalColumnDefault::UuidV7)),
            tag => Err(RelationalError::Corruption(format!(
                "invalid column default tag {tag}"
            ))),
        }
    }

    fn column_default_logical(
        &mut self,
    ) -> Result<Option<RelationalColumnDefault>, RelationalError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(RelationalColumnDefault::Literal(
                self.logical_value()?,
            ))),
            2 => Ok(Some(RelationalColumnDefault::UuidV7)),
            tag => Err(RelationalError::Corruption(format!(
                "invalid column default tag {tag}"
            ))),
        }
    }

    fn foreign_key(&mut self) -> Result<RelationalForeignKeySchema, RelationalError> {
        Ok(RelationalForeignKeySchema {
            columns: self.string_list()?,
            referenced_table: self.string()?,
            referenced_columns: self.string_list()?,
            on_delete: self.referential_action()?,
            on_update: self.referential_action()?,
        })
    }

    fn referential_action(&mut self) -> Result<RelationalReferentialAction, RelationalError> {
        match self.u8()? {
            0 => Ok(RelationalReferentialAction::NoAction),
            1 => Ok(RelationalReferentialAction::Restrict),
            2 => Ok(RelationalReferentialAction::Cascade),
            tag => Err(RelationalError::Corruption(format!(
                "invalid referential action tag {tag}"
            ))),
        }
    }

    fn index(&mut self) -> Result<RelationalIndexSchema, RelationalError> {
        Ok(RelationalIndexSchema {
            name: self.string()?,
            columns: self.string_list()?,
            unique: self.boolean("index unique")?,
        })
    }

    fn write(&mut self) -> Result<RelationalWrite, RelationalError> {
        match self.u8()? {
            1 => Ok(RelationalWrite::CreateTable(self.table_schema()?)),
            2 => Ok(RelationalWrite::CreateIndex {
                table: self.string()?,
                index: self.index()?,
            }),
            3 => {
                let table = self.string()?;
                let mode = match self.u8()? {
                    0 => RelationalInsertMode::Error,
                    1 => RelationalInsertMode::Replace,
                    tag => {
                        return Err(RelationalError::Corruption(format!(
                            "invalid relational insert mode {tag}"
                        )))
                    }
                };
                let row_count = self.row_count()?;
                let rows = (0..row_count)
                    .map(|_| self.row())
                    .collect::<Result<_, _>>()?;
                Ok(RelationalWrite::Insert { table, rows, mode })
            }
            4 => {
                let table = self.string()?;
                let key_count = self.row_count()?;
                let mut keys = Vec::with_capacity(key_count);
                for _ in 0..key_count {
                    let value_count = self.count(
                        self.limits.max_values.saturating_sub(self.values),
                        "key values",
                    )?;
                    let values = (0..value_count)
                        .map(|_| self.value())
                        .collect::<Result<_, _>>()?;
                    keys.push(RelationalKey(values));
                }
                Ok(RelationalWrite::DeleteByPrimaryKey { table, keys })
            }
            5 => {
                let table = self.string()?;
                let conflict_columns = self.string_list()?;
                let action = match self.u8()? {
                    0 => RelationalConflictAction::DoNothing,
                    1 => {
                        let count = self.count(self.limits.max_values, "upsert assignments")?;
                        let mut assignments = Vec::with_capacity(count);
                        for _ in 0..count {
                            let column = self.string()?;
                            let value = match self.u8()? {
                                0 => RelationalUpsertValue::ExcludedColumn(self.string()?),
                                1 => RelationalUpsertValue::Value(self.logical_value()?),
                                tag => {
                                    return Err(RelationalError::Corruption(format!(
                                        "invalid upsert assignment tag {tag}"
                                    )))
                                }
                            };
                            assignments.push(RelationalUpsertAssignment { column, value });
                        }
                        RelationalConflictAction::Update(assignments)
                    }
                    tag => {
                        return Err(RelationalError::Corruption(format!(
                            "invalid upsert action tag {tag}"
                        )))
                    }
                };
                let row_count = self.row_count()?;
                let rows = (0..row_count)
                    .map(|_| self.row())
                    .collect::<Result<_, _>>()?;
                Ok(RelationalWrite::Upsert {
                    table,
                    rows,
                    conflict_columns,
                    action,
                })
            }
            6 => Ok(RelationalWrite::DeleteWhere {
                table: self.string()?,
                predicate: self.predicate()?,
            }),
            7 => {
                let table = self.string()?;
                let count = self.count(self.limits.max_values, "update assignments")?;
                let mut assignments = Vec::with_capacity(count);
                for _ in 0..count {
                    let column = self.string()?;
                    let value = match self.u8()? {
                        0 => RelationalUpdateValue::Column(self.string()?),
                        1 => RelationalUpdateValue::Value(self.logical_value()?),
                        2 => RelationalUpdateValue::BigIntArithmetic {
                            left: self.bigint_arithmetic_operand()?,
                            operator: match self.u8()? {
                                0 => RelationalBigIntArithmeticOperator::Add,
                                1 => RelationalBigIntArithmeticOperator::Subtract,
                                tag => {
                                    return Err(RelationalError::Corruption(format!(
                                        "invalid BIGINT arithmetic operator tag {tag}"
                                    )))
                                }
                            },
                            right: self.bigint_arithmetic_operand()?,
                        },
                        tag => {
                            return Err(RelationalError::Corruption(format!(
                                "invalid update assignment tag {tag}"
                            )))
                        }
                    };
                    assignments.push(RelationalUpdateAssignment { column, value });
                }
                Ok(RelationalWrite::UpdateWhere {
                    table,
                    assignments,
                    predicate: self.predicate()?,
                })
            }
            8 => {
                let table = self.string()?;
                let name = self.string()?;
                let scalar_type = self.scalar_type()?;
                let nullable = self.boolean("column nullable")?;
                let default = self.column_default_logical()?;
                Ok(RelationalWrite::AddColumn {
                    table,
                    column: RelationalColumnSchema {
                        name,
                        scalar_type,
                        nullable,
                        default,
                    },
                })
            }
            tag => Err(RelationalError::Corruption(format!(
                "invalid relational WAL write tag {tag}"
            ))),
        }
    }

    fn bigint_arithmetic_operand(&mut self) -> Result<RelationalBigIntOperand, RelationalError> {
        match self.u8()? {
            0 => Ok(RelationalBigIntOperand::Column(self.string()?)),
            1 => Ok(RelationalBigIntOperand::Value(self.logical_value()?)),
            tag => Err(RelationalError::Corruption(format!(
                "invalid BIGINT arithmetic operand tag {tag}"
            ))),
        }
    }

    fn logical_value(&mut self) -> Result<RelationalValue, RelationalError> {
        let value = self.value()?;
        if matches!(value, RelationalValue::Overflow(_)) {
            return Err(RelationalError::Corruption(
                "WAL contains a physical overflow reference".to_string(),
            ));
        }
        Ok(value)
    }

    fn predicate(&mut self) -> Result<RelationalPredicate, RelationalError> {
        match self.u8()? {
            0 => Ok(RelationalPredicate::And(
                Box::new(self.predicate()?),
                Box::new(self.predicate()?),
            )),
            1 => Ok(RelationalPredicate::Or(
                Box::new(self.predicate()?),
                Box::new(self.predicate()?),
            )),
            2 => Ok(RelationalPredicate::Not(Box::new(self.predicate()?))),
            3 => {
                let column = self.string()?;
                let op = match self.u8()? {
                    0 => RelationalComparisonOp::Eq,
                    1 => RelationalComparisonOp::NotEq,
                    2 => RelationalComparisonOp::Lt,
                    3 => RelationalComparisonOp::Lte,
                    4 => RelationalComparisonOp::Gt,
                    5 => RelationalComparisonOp::Gte,
                    tag => {
                        return Err(RelationalError::Corruption(format!(
                            "invalid comparison operator tag {tag}"
                        )))
                    }
                };
                Ok(RelationalPredicate::Compare {
                    column,
                    op,
                    value: self.logical_value()?,
                })
            }
            4 => Ok(RelationalPredicate::IsNull {
                column: self.string()?,
                negated: self.boolean("predicate negation")?,
            }),
            tag => Err(RelationalError::Corruption(format!(
                "invalid relational predicate tag {tag}"
            ))),
        }
    }
}
