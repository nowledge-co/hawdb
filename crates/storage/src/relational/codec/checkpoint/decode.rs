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

//! Cooperative field reads and UTF-8 validation for private reconstruction.
//! Capacity/reallocation and retained decoder state still need the shared hard
//! resource ledger; a unit alone does not establish their memory/time bounds.

use super::*;

const BLOCK_BYTES: usize = 64 * 1024;

fn length<I: DecodeInput>(
    decoder: &mut Decoder<I>,
    max: usize,
    context: &str,
) -> Result<usize, RelationalError> {
    let len = usize::try_from(decoder.u64()?).map_err(|_| {
        RelationalError::Corruption(format!("decoded {context} length overflows usize"))
    })?;
    if len > max {
        return Err(RelationalError::Admission(format!(
            "decoded {context} contains {len} bytes, exceeding limit {max}"
        )));
    }
    // Preserve the ordinary input's whole-field truncation check before any
    // payload read; chunking must not consume a partial logical field first.
    let end = decoder
        .input
        .position()
        .checked_add(len)
        .ok_or_else(|| RelationalError::Corruption("durable decoder offset overflow".into()))?;
    if end > decoder.input.len() {
        return Err(RelationalError::Corruption(
            "truncated relational durable payload".into(),
        ));
    }
    Ok(len)
}

pub(crate) fn bytes<I: DecodeInput>(
    decoder: &mut Decoder<I>,
    max: usize,
    context: &str,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, RelationalError> {
    let len = length(decoder, max, context)?;
    let unit = work.start_unit().map_err(work_error)?;
    let mut bytes = Vec::with_capacity(len);
    unit.finish();
    while bytes.len() < len {
        let start = bytes.len();
        let end = start.saturating_add(BLOCK_BYTES).min(len);
        let unit = work.start_unit().map_err(work_error)?;
        bytes.resize(end, 0);
        unit.finish();
        decoder.input.read_exact(&mut bytes[start..end])?;
    }
    work.checkpoint().map_err(work_error)?;
    Ok(bytes)
}

fn utf8_error(base: usize, error: std::str::Utf8Error) -> RelationalError {
    let at = base + error.valid_up_to();
    let description = match error.error_len() {
        Some(len) => format!("invalid utf-8 sequence of {len} bytes from index {at}"),
        None => format!("incomplete utf-8 byte sequence from index {at}"),
    };
    RelationalError::Corruption(format!("durable string is not valid UTF-8: {description}"))
}

pub(crate) fn string<I: DecodeInput>(
    decoder: &mut Decoder<I>,
    work: &CheckpointWorkContext,
) -> Result<String, RelationalError> {
    let len = length(decoder, decoder.limits.max_value_bytes, "string")?;
    let unit = work.start_unit().map_err(work_error)?;
    let mut output = String::with_capacity(len);
    unit.finish();
    let mut buffer = [0u8; BLOCK_BYTES + 3];
    let mut read = 0usize;
    let mut carry = 0usize;
    let mut first_error = None;
    while read < len {
        let next = (len - read).min(BLOCK_BYTES);
        decoder.input.read_exact(&mut buffer[carry..carry + next])?;
        let base = read - carry;
        read += next;
        let available = carry + next;
        let unit = work.start_unit().map_err(work_error)?;
        if first_error.is_none() {
            match std::str::from_utf8(&buffer[..available]) {
                Ok(text) => {
                    output.push_str(text);
                    carry = 0;
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    output.push_str(
                        std::str::from_utf8(&buffer[..valid])
                            .expect("UTF-8 error's valid prefix is valid"),
                    );
                    if error.error_len().is_none() && read < len {
                        carry = available - valid;
                        debug_assert!(carry <= 3);
                        buffer.copy_within(valid..available, 0);
                    } else {
                        first_error = Some(utf8_error(base, error));
                        carry = 0;
                    }
                }
            }
        }
        unit.finish();
    }
    // Ordinary string decode checks the cumulative byte budget after reading
    // the complete field, before reporting UTF-8 failure. Retain that order.
    decoder.value_bytes = decoder
        .value_bytes
        .checked_add(len)
        .ok_or_else(|| RelationalError::Admission("decoded value byte count overflow".into()))?;
    if decoder.value_bytes > decoder.limits.max_record_bytes {
        return Err(RelationalError::Admission(
            "decoded string bytes exceed record budget".into(),
        ));
    }
    work.checkpoint().map_err(work_error)?;
    if let Some(error) = first_error {
        return Err(error);
    }
    debug_assert_eq!(carry, 0);
    debug_assert_eq!(output.len(), len);
    Ok(output)
}

#[cfg(test)]
mod tests;
