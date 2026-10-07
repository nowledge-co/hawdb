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

//! Hash the existing schema wire identity without retaining a full encoding.
//! Schema/map allocations, comparisons and destruction need separate accounting.

use super::*;
use crate::background::{CheckpointWorkContext, CheckpointWorkError};

fn work_error(error: CheckpointWorkError) -> RelationalIndexShadowError {
    RelationalIndexShadowError::Admission(error.to_string())
}

pub(in crate::relational) fn relational_schema_digest_with_work_context(
    schema: &RelationalTableSchema,
    work: &CheckpointWorkContext,
) -> Result<Sha256Digest, RelationalIndexShadowError> {
    let mut output = SchemaDigest {
        hasher: IntegrityHasher::new(),
        work,
    };
    output.bytes(schema.name.as_bytes())?;
    output.count(schema.columns.len(), "columns")?;
    for column in &schema.columns {
        output.bytes(column.name.as_bytes())?;
        output.update(&[
            scalar_type_tag(column.scalar_type),
            u8::from(column.nullable),
        ])?;
        match &column.default {
            None => output.update(&[0])?,
            Some(RelationalColumnDefault::Literal(value)) => {
                output.update(&[1])?;
                output.value(value)?;
            }
            Some(RelationalColumnDefault::UuidV7) => output.update(&[2])?,
        }
    }
    output.strings(&schema.primary_key)?;
    output.count(schema.unique_constraints.len(), "unique constraints")?;
    for columns in &schema.unique_constraints {
        output.strings(columns)?;
    }
    output.count(schema.foreign_keys.len(), "foreign keys")?;
    for foreign in &schema.foreign_keys {
        output.strings(&foreign.columns)?;
        output.bytes(foreign.referenced_table.as_bytes())?;
        output.strings(&foreign.referenced_columns)?;
        output.update(&[
            referential_action_tag(foreign.on_delete),
            referential_action_tag(foreign.on_update),
        ])?;
    }
    output.count(schema.indexes.len(), "indexes")?;
    for index in &schema.indexes {
        output.bytes(index.name.as_bytes())?;
        output.strings(&index.columns)?;
        output.update(&[u8::from(index.unique)])?;
    }
    work.checkpoint().map_err(work_error)?;
    Ok(output.hasher.finish().sha256)
}

struct SchemaDigest<'a> {
    hasher: IntegrityHasher,
    work: &'a CheckpointWorkContext,
}

impl SchemaDigest<'_> {
    fn update(&mut self, bytes: &[u8]) -> Result<(), RelationalIndexShadowError> {
        for block in bytes.chunks(64 * 1024) {
            let unit = self.work.start_unit().map_err(work_error)?;
            self.hasher.update(block);
            unit.finish();
        }
        self.work.checkpoint().map_err(work_error)
    }

    fn bytes(&mut self, bytes: &[u8]) -> Result<(), RelationalIndexShadowError> {
        let len = u32::try_from(bytes.len()).map_err(|_| {
            RelationalIndexShadowError::Admission("byte string length does not fit u32".into())
        })?;
        self.update(&len.to_le_bytes())?;
        self.update(bytes)
    }

    fn count(&mut self, count: usize, context: &str) -> Result<(), RelationalIndexShadowError> {
        let count = u32::try_from(count).map_err(|_| {
            RelationalIndexShadowError::Admission(format!("{context} count does not fit u32"))
        })?;
        self.update(&count.to_le_bytes())
    }

    fn strings(&mut self, values: &[String]) -> Result<(), RelationalIndexShadowError> {
        self.count(values.len(), "string list")?;
        for value in values {
            self.bytes(value.as_bytes())?;
        }
        self.work.checkpoint().map_err(work_error)
    }

    fn value(&mut self, value: &RelationalValue) -> Result<(), RelationalIndexShadowError> {
        let variable = match value {
            RelationalValue::Text(text) => Some((4, text.as_bytes())),
            RelationalValue::Bytea(bytes) => Some((5, bytes.as_slice())),
            _ => None,
        };
        if let Some((tag, bytes)) = variable {
            self.update(&[tag])?;
            let unit = self.work.start_unit().map_err(work_error)?;
            let mut escaped = Vec::with_capacity(64 * 1024);
            unit.finish();
            // Zero escaping expands at most twofold. Each output/hash remains
            // at most 64 KiB, including an input made entirely of zero bytes.
            for block in bytes.chunks(32 * 1024) {
                let unit = self.work.start_unit().map_err(work_error)?;
                escaped.clear();
                for byte in block {
                    if *byte == 0 {
                        escaped.extend_from_slice(&[0, 255]);
                    } else {
                        escaped.push(*byte);
                    }
                }
                self.hasher.update(&escaped);
                unit.finish();
            }
            self.update(&[0, 0])
        } else {
            let unit = self.work.start_unit().map_err(work_error)?;
            let mut encoded = Vec::with_capacity(32);
            encode_ordered_relational_value(&mut encoded, value)?;
            self.hasher.update(&encoded);
            unit.finish();
            self.work.checkpoint().map_err(work_error)
        }
    }
}
