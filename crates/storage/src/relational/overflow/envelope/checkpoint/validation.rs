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

//! Validate a publication input without retaining a hydrated value. Zstd's
//! internal workspace, allocator costs and retained inputs still need hard
//! resource accounting; bounded Rust buffers alone do not qualify the job.

use super::*;
use hawdb_integrity::Crc32cHasher;

pub(crate) fn validate_overflow_envelope_with_work_context(
    reference: &RelationalOverflowRef,
    encoded: &[u8],
    budget: &mut RelationalHydrationBudget,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    let unit = work.start_unit().map_err(work_error)?;
    validate_header_prefix(encoded)?;
    unit.finish();
    if work.integrity(encoded).map_err(work_error)?.sha256 != reference.digest {
        return Err(RelationalError::Corruption(
            "overflow envelope digest mismatch".into(),
        ));
    }
    let unit = work.start_unit().map_err(work_error)?;
    let header = decode_verified_header(reference, encoded)?;
    let next_budget = admitted_hydration_budget(reference, budget)?;
    let mut validation = PayloadValidation {
        checksum: Crc32cHasher::new(),
        decoded: 0,
        text: (header.scalar_type == RelationalScalarType::Text).then(Utf8Validation::default),
    };
    unit.finish();
    let payload = &encoded[OVERFLOW_HEADER_BYTES..];
    match header.codec {
        OVERFLOW_CODEC_RAW => {
            if header.compressed_bytes != header.uncompressed_bytes {
                return Err(RelationalError::Corruption(
                    "raw overflow envelope length mismatch".into(),
                ));
            }
            for chunk in payload.chunks(DECODE_CHUNK_BYTES) {
                let unit = work.start_unit().map_err(work_error)?;
                validation.update(chunk, header.uncompressed_bytes)?;
                unit.finish();
            }
        }
        OVERFLOW_CODEC_ZSTD => {
            let unit = work.start_unit().map_err(work_error)?;
            let decoder =
                crate::compression::Decoder::new(Cursor::new(payload)).map_err(|error| {
                    RelationalError::Corruption(format!(
                        "failed to initialize overflow decoder: {error}"
                    ))
                })?;
            let read_limit = header
                .uncompressed_bytes
                .checked_add(1)
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or_else(|| {
                    RelationalError::Corruption("overflow decode limit overflow".into())
                })?;
            let mut decoder = decoder.take(read_limit);
            unit.finish();
            let mut chunk = [0u8; DECODE_CHUNK_BYTES];
            loop {
                let unit = work.start_unit().map_err(work_error)?;
                let read = decoder.read(&mut chunk).map_err(|error| {
                    RelationalError::Corruption(format!("failed to decode overflow value: {error}"))
                })?;
                unit.finish();
                if read == 0 {
                    break;
                }
                let unit = work.start_unit().map_err(work_error)?;
                validation.update(&chunk[..read], header.uncompressed_bytes)?;
                unit.finish();
            }
        }
        _ => unreachable!("validated overflow codec"),
    }
    if validation.decoded != header.uncompressed_bytes
        || validation.checksum.finish_u32() != header.checksum
    {
        return Err(RelationalError::Corruption(
            "overflow decoded length or checksum mismatch".into(),
        ));
    }
    if let Some(error) = validation.text.and_then(|text| text.error()) {
        return Err(error);
    }
    // Cancellation on the final completion must not consume caller budget.
    work.checkpoint().map_err(work_error)?;
    *budget = next_budget;
    Ok(())
}

struct PayloadValidation {
    checksum: Crc32cHasher,
    decoded: usize,
    text: Option<Utf8Validation>,
}

impl PayloadValidation {
    fn update(&mut self, chunk: &[u8], declared: usize) -> Result<(), RelationalError> {
        let next = self.decoded.checked_add(chunk.len()).ok_or_else(|| {
            RelationalError::Corruption("overflow decoded length overflow".into())
        })?;
        if next > declared {
            return Err(RelationalError::Corruption(
                "overflow payload expands beyond its declared length".into(),
            ));
        }
        self.checksum.update(chunk);
        if let Some(text) = &mut self.text {
            text.update(chunk, self.decoded);
        }
        self.decoded = next;
        Ok(())
    }
}

#[derive(Default)]
struct Utf8Validation {
    carry: [u8; 3],
    carry_len: usize,
    incomplete_at: usize,
    first_error: Option<(usize, Option<usize>)>,
}

impl Utf8Validation {
    fn update(&mut self, chunk: &[u8], consumed: usize) {
        if self.first_error.is_some() {
            return;
        }
        debug_assert!(chunk.len() <= DECODE_CHUNK_BYTES);
        let mut buffer = [0u8; DECODE_CHUNK_BYTES + 3];
        buffer[..self.carry_len].copy_from_slice(&self.carry[..self.carry_len]);
        buffer[self.carry_len..self.carry_len + chunk.len()].copy_from_slice(chunk);
        let bytes = &buffer[..self.carry_len + chunk.len()];
        let base = consumed - self.carry_len;
        match std::str::from_utf8(bytes) {
            Ok(_) => self.carry_len = 0,
            Err(error) => {
                let valid = error.valid_up_to();
                let at = base + valid;
                if error.error_len().is_none() {
                    self.carry_len = bytes.len() - valid;
                    debug_assert!(self.carry_len <= 3);
                    self.carry[..self.carry_len].copy_from_slice(&bytes[valid..]);
                    self.incomplete_at = at;
                } else {
                    self.first_error = Some((at, error.error_len()));
                    self.carry_len = 0;
                }
            }
        }
    }

    fn error(self) -> Option<RelationalError> {
        let (at, len) = self
            .first_error
            .or_else(|| (self.carry_len != 0).then_some((self.incomplete_at, None)))?;
        let message = match len {
            Some(len) => format!("invalid utf-8 sequence of {len} bytes from index {at}"),
            None => format!("incomplete utf-8 byte sequence from index {at}"),
        };
        Some(RelationalError::Corruption(format!(
            "overflow text is not valid UTF-8: {message}"
        )))
    }
}

#[cfg(test)]
mod tests;
