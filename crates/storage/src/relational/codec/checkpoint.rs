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

//! Borrowed row payload encoding for a cooperatively admitted checkpoint.

mod decode;
pub(super) use decode::{
    bytes as decode_bytes_with_work_context, string as decode_string_with_work_context,
};
mod runtime;
pub(crate) use runtime::clone_string as clone_string_with_work_context;
pub(super) use runtime::{
    rebuild_indexes as rebuild_indexes_with_work_context, row_key as row_key_with_work_context,
    row_pages as row_pages_with_work_context,
    validate_foreign_keys as validate_foreign_keys_with_work_context,
};
mod mount;
pub(crate) use mount::decode_relational_checkpoint_file_with_work_context;
mod schema;
mod validation;
pub(crate) use schema::decode_schema as decode_relational_table_schema_with_work_context;
pub(super) use validation::{
    primary_key_positions_with_work_context, validate_row_with_work_context,
    validate_table_schema_with_work_context,
};

use super::*;
use crate::background::{CheckpointWorkContext, CheckpointWorkError};

pub(crate) fn encode_relational_table_schema_with_work_context(
    schema: &RelationalTableSchema,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, RelationalError> {
    validate_table_schema_with_work_context(schema, work)?;
    let mut encoder = Encoder::default();
    table_schema_with_work_context(&mut encoder, schema, work)?;
    work.checkpoint().map_err(work_error)?;
    Ok(encoder.finish())
}

pub(crate) fn validate_relational_table_schema_codec_shape_with_work_context(
    schema: &RelationalTableSchema,
    max_schema_items: usize,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    validate_table_schema_with_work_context(schema, work)?;
    for (context, count) in [
        ("columns", schema.columns.len()),
        ("primary-key columns", schema.primary_key.len()),
        ("unique constraints", schema.unique_constraints.len()),
        ("foreign keys", schema.foreign_keys.len()),
        ("indexes", schema.indexes.len()),
    ] {
        let unit = work.start_unit().map_err(work_error)?;
        if count > max_schema_items {
            return Err(RelationalError::Admission(format!(
                "relational table schema contains {count} {context}, exceeding limit {max_schema_items}"
            )));
        }
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}

pub(crate) fn encode_relational_row_payload_with_work_context(
    row: &RelationalRow,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, RelationalError> {
    let unit = work.start_unit().map_err(work_error)?;
    let mut encoder = Encoder::default();
    encoder.count(row.values().len(), "row values")?;
    unit.finish();
    for value in row.values() {
        encode_value_with_work_context(&mut encoder, value, work)?;
    }
    work.checkpoint().map_err(work_error)?;
    Ok(encoder.finish())
}

pub(super) fn work_error(error: CheckpointWorkError) -> RelationalError {
    RelationalError::Admission(error.to_string())
}

fn encode_value_with_work_context(
    encoder: &mut Encoder,
    value: &RelationalValue,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    let unit = work.start_unit().map_err(work_error)?;
    let variable = match value {
        RelationalValue::Text(text) => Some((4, text.as_bytes())),
        RelationalValue::Bytea(bytes) => Some((5, bytes.as_slice())),
        _ => None,
    };
    if let Some((tag, bytes)) = variable {
        encoder.value_count = encoder.value_count.checked_add(1).ok_or_else(|| {
            RelationalError::Admission("encoded value count overflow".to_string())
        })?;
        encoder.u8(tag);
        encoder.u64(u64::try_from(bytes.len()).map_err(|_| {
            RelationalError::Admission("durable byte string is too large".to_string())
        })?);
        unit.finish();
        for chunk in bytes.chunks(64 * 1024) {
            let unit = work.start_unit().map_err(work_error)?;
            encoder.bytes.extend_from_slice(chunk);
            unit.finish();
        }
    } else {
        encoder.value(value)?;
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}

#[derive(Clone, Copy)]
pub(crate) enum CheckpointOutputIo {
    #[cfg(test)]
    Memory,
    File,
}

impl CheckpointOutputIo {
    fn wave(
        self,
        work: &CheckpointWorkContext,
    ) -> Result<Option<Box<dyn hawdb_core::RuntimeIoWavePermit>>, RelationalError> {
        match self {
            #[cfg(test)]
            Self::Memory => Ok(None),
            Self::File => work.io_wave().map_err(work_error),
        }
    }

    fn seek<W: Seek>(
        self,
        writer: &mut W,
        position: SeekFrom,
        operation: &str,
        work: &CheckpointWorkContext,
    ) -> Result<(), RelationalError> {
        let unit = work.start_unit().map_err(work_error)?;
        let _wave = self.wave(work)?;
        writer
            .seek(position)
            .map_err(|error| RelationalError::from_io(operation, error))?;
        unit.finish();
        work.checkpoint().map_err(work_error)
    }

    fn write<W: Write>(
        self,
        writer: &mut W,
        bytes: &[u8],
        operation: &str,
        work: &CheckpointWorkContext,
    ) -> Result<(), RelationalError> {
        for block in bytes.chunks(64 * 1024) {
            let unit = work.start_unit().map_err(work_error)?;
            let _wave = self.wave(work)?;
            writer
                .write_all(block)
                .map_err(|error| RelationalError::from_io(operation, error))?;
            unit.finish();
        }
        work.checkpoint().map_err(work_error)
    }
}

fn string_with_work_context(
    encoder: &mut Encoder,
    value: &str,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    let unit = work.start_unit().map_err(work_error)?;
    encoder.u64(
        u64::try_from(value.len())
            .map_err(|_| RelationalError::Admission("durable byte string is too large".into()))?,
    );
    unit.finish();
    for block in value.as_bytes().chunks(64 * 1024) {
        let unit = work.start_unit().map_err(work_error)?;
        encoder.bytes.extend_from_slice(block);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}

fn string_list_with_work_context(
    encoder: &mut Encoder,
    values: &[String],
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    let unit = work.start_unit().map_err(work_error)?;
    encoder.count(values.len(), "string list")?;
    unit.finish();
    for value in values {
        string_with_work_context(encoder, value, work)?;
    }
    work.checkpoint().map_err(work_error)
}

fn table_schema_with_work_context(
    encoder: &mut Encoder,
    schema: &RelationalTableSchema,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    string_with_work_context(encoder, &schema.name, work)?;
    let unit = work.start_unit().map_err(work_error)?;
    encoder.count(schema.columns.len(), "table columns")?;
    unit.finish();
    for column in &schema.columns {
        string_with_work_context(encoder, &column.name, work)?;
        let unit = work.start_unit().map_err(work_error)?;
        encoder.scalar_type(column.scalar_type);
        encoder.u8(u8::from(column.nullable));
        match &column.default {
            None => encoder.u8(0),
            Some(RelationalColumnDefault::Literal(_)) => encoder.u8(1),
            Some(RelationalColumnDefault::UuidV7) => encoder.u8(2),
        }
        unit.finish();
        if let Some(RelationalColumnDefault::Literal(value)) = &column.default {
            encode_value_with_work_context(encoder, value, work)?;
        }
    }
    string_list_with_work_context(encoder, &schema.primary_key, work)?;
    let unit = work.start_unit().map_err(work_error)?;
    encoder.count(schema.unique_constraints.len(), "unique constraints")?;
    unit.finish();
    for columns in &schema.unique_constraints {
        string_list_with_work_context(encoder, columns, work)?;
    }
    let unit = work.start_unit().map_err(work_error)?;
    encoder.count(schema.foreign_keys.len(), "foreign keys")?;
    unit.finish();
    for foreign in &schema.foreign_keys {
        string_list_with_work_context(encoder, &foreign.columns, work)?;
        string_with_work_context(encoder, &foreign.referenced_table, work)?;
        string_list_with_work_context(encoder, &foreign.referenced_columns, work)?;
        let unit = work.start_unit().map_err(work_error)?;
        encoder.referential_action(foreign.on_delete);
        encoder.referential_action(foreign.on_update);
        unit.finish();
    }
    let unit = work.start_unit().map_err(work_error)?;
    encoder.count(schema.indexes.len(), "indexes")?;
    unit.finish();
    for index in &schema.indexes {
        string_with_work_context(encoder, &index.name, work)?;
        string_list_with_work_context(encoder, &index.columns, work)?;
        let unit = work.start_unit().map_err(work_error)?;
        encoder.u8(u8::from(index.unique));
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}

pub(super) fn validate_reachability(
    state: &RelationalState,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    let mut reachable = BTreeSet::new();
    for segment in state.segments.values() {
        for row in segment.rows.values() {
            for value in row.values() {
                let unit = work.start_unit().map_err(work_error)?;
                if let RelationalValue::Overflow(reference) = value {
                    reachable.insert(&reference.digest);
                }
                unit.finish();
            }
        }
        work.checkpoint().map_err(work_error)?;
    }
    for digest in &reachable {
        let unit = work.start_unit().map_err(work_error)?;
        if !state.overflow_segments.contains_key(*digest) {
            return Err(RelationalError::Corruption(format!(
                "checkpoint row references missing overflow segment {digest}"
            )));
        }
        unit.finish();
    }
    if reachable.len() != state.overflow_segments.len() {
        return Err(RelationalError::Corruption(
            "checkpoint contains unreachable overflow segments".into(),
        ));
    }
    work.checkpoint().map_err(work_error)
}

fn overflow_with_work_context<'a>(
    segment: &'a RelationalOverflowSegment,
    work: &CheckpointWorkContext,
) -> Result<std::borrow::Cow<'a, [u8]>, RelationalError> {
    work.checkpoint().map_err(work_error)?;
    match segment {
        RelationalOverflowSegment::Inline(bytes) => Ok(std::borrow::Cow::Borrowed(bytes)),
        RelationalOverflowSegment::FileRange { reader, range } => reader
            .checkpoint_range_with_work_context(range, work)
            .map(std::borrow::Cow::Owned)
            .map_err(|error| match error {
                crate::scan::CheckpointRangeReadError::Read(error) => RelationalError::Corruption(
                    format!("failed to read file-backed overflow segment: {error}"),
                ),
                crate::scan::CheckpointRangeReadError::Work(error) => work_error(error),
            }),
    }
}

struct Payload<'a, W> {
    writer: &'a mut W,
    max_payload_bytes: usize,
    payload_bytes: usize,
    hasher: IntegrityHasher,
    io: CheckpointOutputIo,
    work: &'a CheckpointWorkContext,
}

impl<'a, W: Write> Payload<'a, W> {
    fn write_all(&mut self, bytes: &[u8]) -> Result<(), RelationalError> {
        let unit = self.work.start_unit().map_err(work_error)?;
        let next = self.payload_bytes.checked_add(bytes.len()).ok_or_else(|| {
            RelationalError::Admission("relational checkpoint size overflow".into())
        })?;
        if next > self.max_payload_bytes {
            return Err(RelationalError::Admission(format!(
                "relational checkpoint payload contains {next} bytes, exceeding limit {}",
                self.max_payload_bytes
            )));
        }
        unit.finish();
        for block in bytes.chunks(64 * 1024) {
            let unit = self.work.start_unit().map_err(work_error)?;
            let _wave = self.io.wave(self.work)?;
            self.writer.write_all(block).map_err(|error| {
                RelationalError::from_io("failed to stream relational checkpoint payload", error)
            })?;
            self.hasher.update(block);
            self.payload_bytes += block.len();
            unit.finish();
        }
        self.work.checkpoint().map_err(work_error)
    }
}

pub(crate) fn encode_relational_checkpoint_with_work_context<W: Write + Seek>(
    writer: &mut W,
    epoch: u64,
    state: &RelationalState,
    max_record_bytes: usize,
    io: CheckpointOutputIo,
    work: &CheckpointWorkContext,
) -> Result<u64, RelationalError> {
    validate_reachability(state, work)?;
    if max_record_bytes < HEADER_BYTES {
        return Err(RelationalError::Admission(format!(
            "relational checkpoint max_record_bytes {max_record_bytes} is smaller than its header"
        )));
    }
    io.seek(
        writer,
        SeekFrom::Start(HEADER_BYTES as u64),
        "failed to reserve relational checkpoint header",
        work,
    )?;
    let mut payload = Payload {
        writer,
        max_payload_bytes: max_record_bytes - HEADER_BYTES,
        payload_bytes: 0,
        hasher: IntegrityHasher::new(),
        io,
        work,
    };
    let unit = work.start_unit().map_err(work_error)?;
    let mut tables = Encoder::default();
    tables.count(state.schemas.len(), "checkpoint tables")?;
    unit.finish();
    payload.write_all(&tables.finish())?;
    for (name, schema) in &state.schemas {
        let segment = state.segments.get(name).ok_or_else(|| {
            RelationalError::Corruption(format!("table {name} is missing its row segment"))
        })?;
        let mut table = Encoder::default();
        string_with_work_context(&mut table, name, work)?;
        table_schema_with_work_context(&mut table, schema, work)?;
        let unit = work.start_unit().map_err(work_error)?;
        table.count(segment.rows.len(), "checkpoint rows")?;
        unit.finish();
        payload.write_all(&table.finish())?;
        for row in segment.rows.values() {
            payload.write_all(&encode_relational_row_payload_with_work_context(row, work)?)?;
        }
    }
    let unit = work.start_unit().map_err(work_error)?;
    let mut count = Encoder::default();
    count.count(
        state.overflow_segments.len(),
        "checkpoint overflow segments",
    )?;
    unit.finish();
    payload.write_all(&count.finish())?;
    for (digest, segment) in &state.overflow_segments {
        let envelope = overflow_with_work_context(segment, work)?;
        if work.integrity(&envelope).map_err(work_error)?.sha256 != *digest {
            return Err(RelationalError::Corruption(format!(
                "relational checkpoint overflow segment {digest} has an invalid digest"
            )));
        }
        let unit = work.start_unit().map_err(work_error)?;
        let mut metadata = Encoder::default();
        metadata.sha256(*digest);
        metadata.u64(u64::try_from(envelope.len()).map_err(|_| {
            RelationalError::Admission("overflow envelope length does not fit u64".into())
        })?);
        unit.finish();
        payload.write_all(&metadata.finish())?;
        payload.write_all(&envelope)?;
    }
    let unit = work.start_unit().map_err(work_error)?;
    let Payload {
        writer,
        payload_bytes: payload_len,
        hasher,
        ..
    } = payload;
    let total_len = HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(|| RelationalError::Admission("checkpoint size overflow".into()))?;
    let mut header = Vec::with_capacity(HEADER_BYTES);
    encode_envelope_header(
        &mut header,
        CHECKPOINT_MAGIC,
        epoch,
        payload_len as u64,
        hasher.finish(),
    );
    unit.finish();
    io.seek(
        writer,
        SeekFrom::Start(0),
        "failed to seek relational checkpoint header",
        work,
    )?;
    io.write(
        writer,
        &header,
        "failed to write relational checkpoint header",
        work,
    )?;
    io.seek(
        writer,
        SeekFrom::Start(total_len as u64),
        "failed to finish relational checkpoint",
        work,
    )?;
    work.checkpoint().map_err(work_error)?;
    Ok(total_len as u64)
}

#[cfg(test)]
mod tests;
