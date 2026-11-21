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

//! Existing overflow envelope with chunked compression, copying and integrity.

use super::*;
use crate::background::{
    CheckpointBytes, CheckpointSharedBytes, CheckpointWorkContext, CheckpointWorkError,
};
use std::io::Write;

pub(super) mod validation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckpointRelationalOverflow {
    pub(crate) reference: RelationalOverflowRef,
    pub(crate) bytes: CheckpointSharedBytes,
}

impl PartialEq<EncodedRelationalOverflow> for CheckpointRelationalOverflow {
    fn eq(&self, other: &EncodedRelationalOverflow) -> bool {
        self.reference == other.reference && self.bytes.as_ref() == other.bytes.as_ref()
    }
}

pub(crate) fn encode_overflow_envelope_with_work_context(
    scalar_type: RelationalScalarType,
    raw: &[u8],
    config: RelationalOverflowConfig,
    work: &CheckpointWorkContext,
) -> Result<CheckpointRelationalOverflow, RelationalError> {
    work.checkpoint().map_err(work_error)?;
    let scalar_tag = encode_scalar_type(scalar_type)?;
    if raw.is_empty() {
        return Err(RelationalError::Admission(
            "overflow value must not be empty".to_string(),
        ));
    }
    if raw.len() > config.max_value_bytes {
        return Err(RelationalError::Admission(format!(
            "overflow value contains {} bytes, exceeding max_value_bytes {}",
            raw.len(),
            config.max_value_bytes
        )));
    }

    let sample = &raw[..raw.len().min(COMPRESSION_SAMPLE_BYTES)];
    let compressed_sample = compress(sample, config.compression_level, work)?;
    let should_compress = compression_is_worthwhile(sample.len(), compressed_sample.len());
    let compressed = if should_compress && sample.len() != raw.len() {
        Some(compress(raw, config.compression_level, work)?)
    } else if should_compress {
        Some(compressed_sample)
    } else {
        None
    };
    let (codec, payload) = compressed
        .as_ref()
        .map_or((OVERFLOW_CODEC_RAW, raw), |compressed| {
            (OVERFLOW_CODEC_ZSTD, &compressed[..])
        });
    let encoded_len = OVERFLOW_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or_else(|| RelationalError::Admission("overflow envelope size overflow".to_string()))?;
    let uncompressed_bytes = u64::try_from(raw.len()).map_err(|_| {
        RelationalError::Admission("overflow value length does not fit u64".to_string())
    })?;
    let compressed_bytes = u64::try_from(payload.len()).map_err(|_| {
        RelationalError::Admission("overflow payload length does not fit u64".to_string())
    })?;
    let checksum = work.checksum(raw).map_err(work_error)?;
    let header = encode_header(
        codec,
        scalar_tag,
        uncompressed_bytes,
        compressed_bytes,
        checksum as u32,
    );
    let encoded = CheckpointSharedBytes::build(encoded_len, work, |encoded| {
        encoded.append(&header, work)?;
        encoded.append(payload, work)
    })
    .map_err(work_error)?;
    debug_assert_eq!(encoded.len(), encoded_len);

    let digest = work.integrity(&encoded).map_err(work_error)?.sha256;
    let unit = work.start_unit().map_err(work_error)?;
    let output = CheckpointRelationalOverflow {
        reference: RelationalOverflowRef {
            digest,
            scalar_type,
            compressed_bytes,
            uncompressed_bytes,
        },
        bytes: encoded,
    };
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(output)
}

fn work_error(error: CheckpointWorkError) -> RelationalError {
    match error.stop_reason() {
        Some(reason) => RelationalError::Stopped(reason),
        None => RelationalError::Admission(error.to_string()),
    }
}

fn compress(
    raw: &[u8],
    level: i32,
    work: &CheckpointWorkContext,
) -> Result<CheckpointBytes, RelationalError> {
    let error =
        |error| RelationalError::Durability(format!("failed to compress overflow value: {error}"));
    let output =
        crate::compression::AdmittedCompressionOutput::new(raw.len(), work).map_err(work_error)?;
    let unit = work.start_unit().map_err(work_error)?;
    let mut encoder =
        crate::compression::Encoder::with_staging_capacity(output, level, 2 * 1024, work)
            .map_err(error)?;
    encoder
        .set_pledged_src_size(raw.len() as u64)
        .map_err(error)?;
    unit.finish();
    for chunk in raw.chunks(64 * 1024) {
        let unit = work.start_unit().map_err(work_error)?;
        encoder.write_all(chunk).map_err(error)?;
        unit.finish();
    }
    let unit = work.start_unit().map_err(work_error)?;
    let compressed = encoder.finish().map_err(error)?.into_bytes();
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(compressed)
}
