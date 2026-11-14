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

//! Fixed-grammar descriptor admission before maps, sets and record vectors.

use crate::build_memory::read::{ReadAdmission, ReaderMemory};
use crate::build_memory::{
    checked_add as add, checked_mul as mul, MAP_ENTRY_BYTES, SET_ENTRY_BYTES,
};
use crate::{HawDBError, Result, SearchSegmentDescriptor, SearchSegmentDescriptorEntry};
use std::mem::size_of;

#[derive(Debug)]
pub(crate) struct DecodedDescriptor {
    value: SearchSegmentDescriptor,
    // Neither this guard nor the decoded result retains construction execution.
    _memory: Option<ReaderMemory>,
}

impl std::ops::Deref for DecodedDescriptor {
    type Target = SearchSegmentDescriptor;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DecodedDescriptor {
    pub(crate) fn decode(text: &str, admission: Option<&ReadAdmission<'_>>) -> Result<Self> {
        let (memory, scratch) = if let Some(admission) = admission {
            let plan = Plan::inspect(text, admission)?;
            (
                Some(admission.retained(plan.retained)?),
                Some(admission.scratch(plan.scratch)?),
            )
        } else {
            (None, None)
        };
        // Bound value sets while decoding, including descriptors written
        // before #1746, so a reader never keeps document bodies resident.
        let value = crate::decode_search_segment_descriptor_with_context(
            text,
            admission.map(|context| context.task),
            true,
        );
        if let Some(context) = admission {
            crate::build_control::checkpoint(context.task)?;
        }
        let value = value?;
        drop(scratch);
        Ok(Self {
            value,
            _memory: memory,
        })
    }
}

struct Plan {
    retained: usize,
    scratch: usize,
}

impl Plan {
    fn inspect(text: &str, admission: &ReadAdmission<'_>) -> Result<Self> {
        let mut records = 0;
        let mut fields = 0;
        let mut values = 0;
        let mut largest = 0;
        let mut line_bytes = 0;
        let mut prefix = [0; 8];
        let mut prefix_len = 0;
        let mut is_field = false;
        let mut column = 0;
        // Value columns the bounded decode omits retain no strings (#1746).
        let mut omitted = OmittedValues::default();
        let mut value_column = ValueColumn::default();
        // Inspect actual bytes between checkpoints, including a single huge
        // line/value. No tab/line split table or token is allocated here.
        for chunk in text.as_bytes().chunks(8192) {
            admission
                .checkpoint()
                .map_err(|reason| HawDBError::Execution(reason.to_string()))?;
            for &byte in chunk {
                if byte == b'\n' {
                    if is_field && column == 3 {
                        value_column.finish(&mut omitted);
                    }
                    largest = largest.max(line_bytes);
                    line_bytes = 0;
                    prefix_len = 0;
                    is_field = false;
                    column = 0;
                    continue;
                }
                line_bytes += 1;
                if prefix_len < prefix.len() {
                    prefix[prefix_len] = byte;
                    prefix_len += 1;
                    if prefix_len == 6 && &prefix[..6] == b"field\t" {
                        fields = add(fields, 1)?;
                        is_field = true;
                    } else if prefix_len == 8 && &prefix == b"segment\t" {
                        records = add(records, 1)?;
                    }
                }
                if is_field {
                    if byte == b'\t' {
                        if column == 3 {
                            value_column.finish(&mut omitted);
                        }
                        column += 1;
                        if column == 3 {
                            values = add(values, 1)?;
                        }
                    } else if column == 3 && byte == b',' {
                        values = add(values, 1)?;
                        value_column.next_value();
                    } else if column == 3 {
                        value_column.byte();
                    }
                }
            }
        }
        if is_field && column == 3 {
            value_column.finish(&mut omitted);
        }
        largest = largest.max(line_bytes);
        let text_bytes = text.len().saturating_sub(omitted.bytes);
        let values = values.saturating_sub(omitted.values);
        // The fixed table avoids a transient Vec proportional to excess tabs.
        // Every owned key/value comes from hex bytes in this input. Counting
        // duplicate field/value records also covers replacement overlap.
        let vector = mul(add(records, 4)?, size_of::<SearchSegmentDescriptorEntry>())?;
        let retained = add(
            add(
                add(
                    add(text_bytes, mul(vector, 2)?)?,
                    mul(fields, MAP_ENTRY_BYTES)?,
                )?,
                mul(values, SET_ENTRY_BYTES)?,
            )?,
            size_of::<SearchSegmentDescriptor>(),
        )?;
        let scratch = add(add(vector, mul(largest, 2)?)?, 4096)?;
        Ok(Self { retained, scratch })
    }
}

/// Hex bytes and value records of columns the bounded decode omits.
#[derive(Default)]
struct OmittedValues {
    bytes: usize,
    values: usize,
}

/// One field line's value column, measured as the bounded decode measures it.
#[derive(Default)]
struct ValueColumn {
    hex_bytes: usize,
    value_hex_bytes: usize,
    value_bytes: usize,
    largest_value_bytes: usize,
    values: usize,
}

impl ValueColumn {
    fn byte(&mut self) {
        self.hex_bytes += 1;
        self.value_hex_bytes += 1;
    }

    fn next_value(&mut self) {
        self.hex_bytes += 1;
        self.end_value();
    }

    fn end_value(&mut self) {
        let bytes = self.value_hex_bytes / 2;
        self.value_bytes = self.value_bytes.saturating_add(bytes);
        self.largest_value_bytes = self.largest_value_bytes.max(bytes);
        self.value_hex_bytes = 0;
        self.values += 1;
    }

    fn finish(&mut self, omitted: &mut OmittedValues) {
        self.end_value();
        if self.largest_value_bytes > crate::SEARCH_SEGMENT_VALUE_SUMMARY_MAX_VALUE_BYTES
            || self.value_bytes > crate::SEARCH_SEGMENT_VALUE_SUMMARY_MAX_FIELD_BYTES
        {
            omitted.bytes = omitted.bytes.saturating_add(self.hex_bytes);
            omitted.values = omitted.values.saturating_add(self.values);
        }
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests;
