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

//! Borrow an append schema while emitting its existing create-table WAL record.

use super::*;
use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
use crate::projection::predicate_checkpoint::decode::MapMemory;
use crate::relational::{schema_encoding as relational, RelationalError};
use hawdb_integrity::IntegrityHasher;

fn view(schema: &AppendTableSchema) -> relational::Schema<'_> {
    relational::Schema {
        materialized_view: None,
        name: &schema.name,
        columns: &schema.columns,
        primary_key: &schema.partition_key,
        primary_key_suffix: &schema.order_key,
        unique_constraints: &[],
        foreign_keys: &[],
        indexes: &[],
    }
}

pub(super) struct Size<'a> {
    pub(super) bytes: usize,
    pub(super) work: Option<&'a CheckpointWorkContext>,
}

impl relational::Sink for Size<'_> {
    fn raw(&mut self, bytes: &[u8]) -> Result<(), RelationalError> {
        if let Some(work) = self.work {
            work.start_unit()
                .map_err(RelationalError::from_work)?
                .finish();
        }
        self.bytes = self.bytes.checked_add(bytes.len()).ok_or_else(|| {
            RelationalError::Admission("append schema encoding size overflows usize".into())
        })?;
        Ok(())
    }
}

pub(in crate::append_table) fn schema_record_len(
    schema: &AppendTableSchema,
    work: Option<&CheckpointWorkContext>,
) -> Result<usize, AppendTableError> {
    let mut size = Size { bytes: 0, work };
    relational::schema(&mut size, view(schema)).map_err(map_relational)?;
    let limits = AppendDecodeLimits::wal();
    if size.bytes > limits.max_schema_bytes {
        return Err(AppendTableError::Admission(format!(
            "append schema contains {} bytes, exceeding limit {}",
            size.bytes, limits.max_schema_bytes
        )));
    }
    size.bytes
        .checked_add(APPEND_HEADER_BYTES + 18)
        .ok_or_else(|| AppendTableError::Admission("append WAL size overflow".into()))
}

struct Hash<'a> {
    hasher: IntegrityHasher,
    work: Option<&'a CheckpointWorkContext>,
}

impl relational::Sink for Hash<'_> {
    fn raw(&mut self, bytes: &[u8]) -> Result<(), RelationalError> {
        for block in bytes.chunks(64 * 1024) {
            let unit = self
                .work
                .map(CheckpointWorkContext::start_unit)
                .transpose()
                .map_err(RelationalError::from_work)?;
            self.hasher.update(block);
            if let Some(unit) = unit {
                unit.finish();
            }
        }
        Ok(())
    }
}

fn payload(
    sink: &mut impl relational::Sink,
    schema: &AppendTableSchema,
    schema_len: usize,
) -> Result<(), RelationalError> {
    relational::count(sink, 1, "append WAL writes")?;
    sink.raw(&[CREATE_TABLE_TAG])?;
    relational::count(
        sink,
        schema.partition_key.len(),
        "append partition key columns",
    )?;
    sink.raw(&[match schema.order_mode {
        AppendOrderMode::CallerProvided => 0,
        AppendOrderMode::CommitSequence => 1,
    }])?;
    sink.raw(&(schema_len as u64).to_le_bytes())?;
    relational::schema(sink, view(schema))
}

pub(in crate::append_table) fn schema_record(
    sink: &mut impl relational::Sink,
    schema: &AppendTableSchema,
    record_len: usize,
    work: Option<&CheckpointWorkContext>,
) -> Result<(), AppendTableError> {
    let scratch = work.map(|work| CheckpointDecodeContext {
        work: work.clone(),
        memory: std::cell::RefCell::default(),
    });
    if let Some(scratch) = &scratch {
        let mut names = MapMemory::<()>::default();
        if !schema.columns.is_empty() {
            names
                .before_insert(schema.columns.len() - 1, scratch)
                .map_err(map_core)?;
        }
        let mut keys = MapMemory::<()>::default();
        let count = schema
            .partition_key
            .len()
            .checked_add(schema.order_key.len())
            .ok_or_else(|| AppendTableError::Admission("append key cardinality overflow".into()))?;
        if count > 0 {
            keys.before_insert(count - 1, scratch).map_err(map_core)?;
        }
    }
    super::super::validate_table_schema_visiting(schema, || {
        if let Some(work) = work {
            work.start_unit()
                .map_err(super::super::checkpoint::work_error)?
                .finish();
        }
        Ok(())
    })?;
    let payload_len = record_len - APPEND_HEADER_BYTES;
    let schema_len = payload_len - 18;
    let mut hash = Hash {
        hasher: IntegrityHasher::new(),
        work,
    };
    payload(&mut hash, schema, schema_len).map_err(map_relational)?;
    let digest = hash.hasher.finish();
    sink.raw(APPEND_WAL_MAGIC).map_err(map_relational)?;
    sink.raw(&APPEND_CODEC_VERSION.to_le_bytes())
        .map_err(map_relational)?;
    sink.raw(&0u16.to_le_bytes()).map_err(map_relational)?;
    sink.raw(&0u64.to_le_bytes()).map_err(map_relational)?;
    sink.raw(&(payload_len as u64).to_le_bytes())
        .map_err(map_relational)?;
    sink.raw(&digest.crc32c.get().to_le_bytes())
        .map_err(map_relational)?;
    sink.raw(digest.sha256.as_bytes()).map_err(map_relational)?;
    payload(sink, schema, schema_len).map_err(map_relational)
}

fn map_core(error: hawdb_core::HawDBError) -> AppendTableError {
    super::super::binary::allocation_error(error)
}
