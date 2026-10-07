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
//! Allocation, retained capacity and destruction require a shared hard ledger.

use super::*;

struct Input<'a> {
    source: SliceDecodeInput<'a>,
    work: &'a CheckpointWorkContext,
}

impl DecodeInput for Input<'_> {
    fn checkpoint_work_context(&self) -> Option<&CheckpointWorkContext> {
        Some(self.work)
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
    };
    let mut decoder = Decoder::new(input, limits, true);
    let schema = decoder.table_schema()?;
    decoder.finish()?;
    validate_table_schema_with_work_context(&schema, work)?;
    work.checkpoint().map_err(work_error)?;
    Ok(schema)
}
