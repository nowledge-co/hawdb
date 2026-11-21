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

//! Borrowed schema decoding through the existing controlled field decoder.
//! An optional allocation inventory admits backing capacity before allocation;
//! callers retain it with the decoded schema or row for its full lifetime.

use super::*;

pub(in crate::relational::codec) struct Input<'a> {
    pub(in crate::relational::codec) source: SliceDecodeInput<'a>,
    pub(in crate::relational::codec) work: &'a CheckpointWorkContext,
    pub(in crate::relational::codec) allocations:
        Option<&'a crate::background::CheckpointDecodeContext>,
}

impl DecodeInput for Input<'_> {
    fn checkpoint_work_context(&self) -> Option<&CheckpointWorkContext> {
        Some(self.work)
    }

    fn checkpoint_allocation_context(&self) -> Option<&crate::background::CheckpointDecodeContext> {
        self.allocations
    }

    fn len(&self) -> usize {
        self.source.len()
    }

    fn position(&self) -> usize {
        self.source.position()
    }

    fn read_exact(&mut self, output: &mut [u8]) -> Result<(), RelationalError> {
        let end = self
            .source
            .offset
            .checked_add(output.len())
            .ok_or_else(|| RelationalError::Corruption("durable decoder offset overflow".into()))?;
        if end > self.source.len() {
            return Err(RelationalError::Corruption(
                "truncated relational durable payload".into(),
            ));
        }
        for block in output.chunks_mut(64 * 1024) {
            let unit = self.work.start_unit().map_err(work_error)?;
            self.source.read_exact(block)?;
            unit.finish();
        }
        self.work.checkpoint().map_err(work_error)
    }

    fn finish(self) -> Result<(), RelationalError> {
        self.source.finish()?;
        self.work.checkpoint().map_err(work_error)
    }
}

pub(crate) fn decode_schema(
    encoded: &[u8],
    max_encoded_bytes: usize,
    max_schema_items: usize,
    work: &CheckpointWorkContext,
) -> Result<RelationalTableSchema, RelationalError> {
    decode_schema_inner(
        encoded,
        max_encoded_bytes,
        max_schema_items,
        work,
        None,
        true,
    )
}

pub(crate) fn decode_legacy_schema(
    encoded: &[u8],
    max_encoded_bytes: usize,
    max_schema_items: usize,
    work: &CheckpointWorkContext,
) -> Result<RelationalTableSchema, RelationalError> {
    decode_schema_inner(
        encoded,
        max_encoded_bytes,
        max_schema_items,
        work,
        None,
        false,
    )
}

pub(crate) fn decode_schema_owned(
    encoded: &[u8],
    max_encoded_bytes: usize,
    max_schema_items: usize,
    allocations: &crate::background::CheckpointDecodeContext,
) -> Result<RelationalTableSchema, RelationalError> {
    decode_schema_inner(
        encoded,
        max_encoded_bytes,
        max_schema_items,
        allocations,
        Some(allocations),
        true,
    )
}

fn decode_schema_inner(
    encoded: &[u8],
    max_encoded_bytes: usize,
    max_schema_items: usize,
    work: &CheckpointWorkContext,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
    index_predicates: bool,
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
    let input = Input {
        source: SliceDecodeInput {
            bytes: encoded,
            offset: 0,
        },
        work,
        allocations,
    };
    let mut decoder = Decoder::new(input, limits, true);
    let schema = decoder.table_schema_with_index_predicates(index_predicates)?;
    decoder.finish()?;
    let scratch = allocations.map(|_| crate::background::CheckpointDecodeContext {
        work: work.clone(),
        memory: std::cell::RefCell::default(),
    });
    if let Some(scratch) = &scratch {
        use crate::projection::predicate_checkpoint::decode::MapMemory;
        if !schema.columns.is_empty() {
            MapMemory::<usize>::default()
                .before_insert(schema.columns.len() - 1, scratch)
                .map_err(RelationalError::from_admission)?;
        }
        let keys = std::iter::once(schema.primary_key.len())
            .chain(schema.unique_constraints.iter().map(Vec::len))
            .chain(
                schema
                    .foreign_keys
                    .iter()
                    .map(|foreign| foreign.columns.len()),
            )
            .chain(schema.indexes.iter().map(|index| index.columns.len()))
            .max()
            .unwrap_or(0);
        if keys > 0 {
            MapMemory::<()>::default()
                .before_insert(keys - 1, scratch)
                .map_err(RelationalError::from_admission)?;
        }
        if !schema.indexes.is_empty() {
            MapMemory::<()>::default()
                .before_insert(schema.indexes.len() - 1, scratch)
                .map_err(RelationalError::from_admission)?;
        }
    }
    validate_table_schema_with_work_context(&schema, work)?;
    work.checkpoint().map_err(work_error)?;
    Ok(schema)
}

pub(crate) fn decode_row_owned(
    encoded: &[u8],
    max_values: usize,
    max_value_bytes: usize,
    allocations: &crate::background::CheckpointDecodeContext,
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
    let input = Input {
        source: SliceDecodeInput {
            bytes: encoded,
            offset: 0,
        },
        work: allocations,
        allocations: Some(allocations),
    };
    let mut decoder = Decoder::new(input, limits, false);
    let row = decoder.row()?;
    decoder.finish()?;
    Ok(row)
}
