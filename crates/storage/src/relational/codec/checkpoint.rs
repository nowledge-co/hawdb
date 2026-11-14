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
mod row_encoding;
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
pub(super) mod schema;
pub(crate) use schema::{decode_row_owned, decode_schema_owned};
mod validation;
pub(crate) use schema::decode_schema as decode_relational_table_schema_with_work_context;
pub(super) use validation::{
    primary_key_positions_with_work_context, validate_row_with_work_context,
    validate_table_schema_with_work_context,
};

use super::*;
use crate::background::{CheckpointBytes, CheckpointWorkContext, CheckpointWorkError};

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
) -> Result<CheckpointBytes, RelationalError> {
    row_encoding::encode(row, work)
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
        RelationalValue::Vector(value) => Some((8, value.encoded())),
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

fn table_schema_with_work_context(
    encoder: &mut Encoder,
    schema: &RelationalTableSchema,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    struct Controlled<'a> {
        encoder: &'a mut Encoder,
        work: &'a CheckpointWorkContext,
    }
    impl schema_encoding::Sink for Controlled<'_> {
        fn raw(&mut self, bytes: &[u8]) -> Result<(), RelationalError> {
            for block in bytes.chunks(64 * 1024) {
                let unit = self.work.start_unit().map_err(work_error)?;
                self.encoder.bytes.extend_from_slice(block);
                unit.finish();
            }
            self.work.checkpoint().map_err(work_error)
        }
        fn value(&mut self, value: &RelationalValue) -> Result<(), RelationalError> {
            encode_value_with_work_context(self.encoder, value, self.work)
        }
    }
    schema_encoding::schema(&mut Controlled { encoder, work }, schema.into())
}

/// Values examined by one reachability unit; each is a constant-time tag test.
pub(super) const REACHABILITY_VALUES_PER_UNIT: usize = 4096;

pub(super) fn validate_reachability(
    state: &RelationalState,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    let mut reachable = BTreeSet::new();
    for segment in state.segments.values() {
        // One unit per bounded group of values, not per value: each value is
        // a tag test, so a per-value unit (two clock reads) dominated the scan.
        let mut values = segment.rows.values().flat_map(RelationalRow::values);
        loop {
            let unit = work.start_unit().map_err(work_error)?;
            let mut examined = 0;
            for value in values.by_ref().take(REACHABILITY_VALUES_PER_UNIT) {
                if let RelationalValue::Overflow(reference) = value {
                    reachable.insert(&reference.digest);
                }
                examined += 1;
            }
            unit.finish();
            if examined < REACHABILITY_VALUES_PER_UNIT {
                break;
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

enum OverflowBytes<'a> {
    Borrowed(&'a [u8]),
    Owned(CheckpointBytes),
}

impl std::ops::Deref for OverflowBytes<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Owned(bytes) => bytes,
        }
    }
}

fn overflow_with_work_context<'a>(
    segment: &'a RelationalOverflowSegment,
    work: &CheckpointWorkContext,
) -> Result<OverflowBytes<'a>, RelationalError> {
    work.checkpoint().map_err(work_error)?;
    match segment {
        RelationalOverflowSegment::Inline(bytes) => Ok(OverflowBytes::Borrowed(bytes)),
        RelationalOverflowSegment::CheckpointInline(bytes) => Ok(OverflowBytes::Borrowed(bytes)),
        RelationalOverflowSegment::FileRange { reader, range } => reader
            .checkpoint_range_with_work_context(range, work)
            .map(OverflowBytes::Owned)
            .map_err(|error| match error {
                crate::scan::CheckpointRangeReadError::Read(error) => RelationalError::Corruption(
                    format!("failed to read file-backed overflow segment: {error}"),
                ),
                crate::scan::CheckpointRangeReadError::Work(error) => work_error(error),
            }),
    }
}

/// Upper bound of one staged payload block. It matches the 64 KiB bounded
/// unit used by every other checkpoint writer: one block is one `write`, one
/// hash update, one I/O wave and one QoS unit, so cancellation and QoS
/// admission are observed at least once per 64 KiB of encoded output.
pub(super) const PAYLOAD_BLOCK_BYTES: usize = 64 * 1024;
/// Below this admitted size, staging saves too little to justify a lease.
const MIN_PAYLOAD_BLOCK_BYTES: usize = 4 * 1024;

/// Physical payload output. Every byte reaches the writer and the hasher in
/// order, in blocks of at most `PAYLOAD_BLOCK_BYTES`.
struct PayloadOutput<'a, W> {
    writer: &'a mut W,
    hasher: IntegrityHasher,
    io: CheckpointOutputIo,
    work: &'a CheckpointWorkContext,
}

impl<W: Write> PayloadOutput<'_, W> {
    fn emit(&mut self, block: &[u8]) -> Result<(), RelationalError> {
        let unit = self.work.start_unit().map_err(work_error)?;
        let _wave = self.io.wave(self.work)?;
        self.writer.write_all(block).map_err(|error| {
            RelationalError::from_io("failed to stream relational checkpoint payload", error)
        })?;
        self.hasher.update(block);
        unit.finish();
        Ok(())
    }
}

/// Encoder sink for the legacy row checkpoint payload.
///
/// The row grammar emits one `raw` call per field (a tag, then 1-16 bytes).
/// Writing those straight to the file cost one `write(2)` and two QoS units
/// per field. Fields are instead staged in an admitted block and leave it
/// once per full block. Size admission (`payload_bytes`) is still checked per
/// call, so the same field is refused at the same point; the digest covers the
/// same bytes in the same order. A staged block is never written on drop: an
/// error or stop abandons it, and the caller removes the temporary file.
struct Payload<'a, W> {
    output: PayloadOutput<'a, W>,
    max_payload_bytes: usize,
    /// Logical payload bytes accepted so far, written or staged.
    payload_bytes: usize,
    staged: Option<CheckpointBytes>,
}

impl<W: Write> schema_encoding::Sink for Payload<'_, W> {
    fn raw(&mut self, bytes: &[u8]) -> Result<(), RelationalError> {
        self.write_all(bytes)
    }
}

impl<'a, W: Write> Payload<'a, W> {
    fn new(
        writer: &'a mut W,
        max_payload_bytes: usize,
        io: CheckpointOutputIo,
        work: &'a CheckpointWorkContext,
    ) -> Result<Self, RelationalError> {
        // Leave half of the task's working-memory target for any other
        // admitted allocation of the same task, and honour host pressure.
        let target = work
            .task_context()
            .working_memory_target(work.memory_ceiling())
            / 2;
        let capacity = usize::try_from(target)
            .unwrap_or(usize::MAX)
            .min(PAYLOAD_BLOCK_BYTES);
        let staged = if capacity >= MIN_PAYLOAD_BLOCK_BYTES {
            CheckpointBytes::try_new_staging(capacity, work).map_err(work_error)?
        } else {
            None
        };
        Ok(Self {
            output: PayloadOutput {
                writer,
                hasher: IntegrityHasher::new(),
                io,
                work,
            },
            max_payload_bytes,
            payload_bytes: 0,
            staged,
        })
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<(), RelationalError> {
        let next = self.payload_bytes.checked_add(bytes.len()).ok_or_else(|| {
            RelationalError::Admission("relational checkpoint size overflow".into())
        })?;
        if next > self.max_payload_bytes {
            return Err(RelationalError::Admission(format!(
                "relational checkpoint payload contains {next} bytes, exceeding limit {}",
                self.max_payload_bytes
            )));
        }
        let Some(staged) = self.staged.as_mut() else {
            // Unadmitted fallback: the original per-call write-through path.
            for block in bytes.chunks(PAYLOAD_BLOCK_BYTES) {
                self.output.emit(block)?;
            }
            self.payload_bytes = next;
            return self.output.work.checkpoint().map_err(work_error);
        };
        let mut rest = bytes;
        loop {
            rest = &rest[staged.fill_within_capacity(rest)..];
            if rest.is_empty() {
                break;
            }
            // The block is full and more bytes follow.
            self.output.emit(staged)?;
            staged.clear();
        }
        self.payload_bytes = next;
        Ok(())
    }

    /// Write every staged byte. Write errors surface here, before the header.
    fn flush(&mut self) -> Result<(), RelationalError> {
        if let Some(staged) = self.staged.as_mut().filter(|staged| !staged.is_empty()) {
            self.output.emit(staged)?;
            staged.clear();
        }
        self.output.work.checkpoint().map_err(work_error)
    }

    /// Flush and release the staging lease. Later sections write through, so
    /// the lease is not held while they admit their own memory.
    fn unstage(&mut self) -> Result<(), RelationalError> {
        self.flush()?;
        self.staged = None;
        Ok(())
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
    let mut payload = Payload::new(writer, max_record_bytes - HEADER_BYTES, io, work)?;
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
        table.u64(segment.last_row_id as u64);
        table.count(segment.rows.len(), "checkpoint rows")?;
        unit.finish();
        payload.write_all(&table.finish())?;
        for (key, row) in segment.rows.iter() {
            if schema.primary_key.is_empty() {
                crate::relational::validate_hidden_row_handle(key, segment.last_row_id)?;
                let mut encoded = Encoder::default();
                encoded.key(key)?;
                payload.write_all(&encoded.finish())?;
            }
            schema_encoding::row(&mut payload, row)?;
        }
    }
    // Overflow segments are few, large and may admit file-backed reads.
    payload.unstage()?;
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
    payload.unstage()?;
    let unit = work.start_unit().map_err(work_error)?;
    let Payload {
        output: PayloadOutput { writer, hasher, .. },
        payload_bytes: payload_len,
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
