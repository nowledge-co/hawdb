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

use super::*;
use crate::background::{CheckpointWorkContext, CheckpointWorkError};

pub(crate) fn encode_relational_row_payload_with_work_context(
    row: &RelationalRow,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, RelationalError> {
    let unit = work.start_unit().map_err(work_error)?;
    let mut encoder = Encoder::default();
    encoder.count(row.values().len(), "row values")?;
    unit.finish();
    for value in row.values() {
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
    }
    work.checkpoint().map_err(work_error)?;
    Ok(encoder.finish())
}

fn work_error(error: CheckpointWorkError) -> RelationalError {
    RelationalError::Admission(error.to_string())
}
