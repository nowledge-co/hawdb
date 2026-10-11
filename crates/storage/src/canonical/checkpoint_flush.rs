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

//! Flush borrowed record bytes with a stack header instead of a second full
//! segment buffer. Controlled digest/write steps release each unit and I/O wave.
//! Source arrays and returned descriptor Bloom ownership are separate resources.

use super::*;
use crate::background::{
    CheckpointAllocationOwner, CheckpointDecodeContext, CheckpointOperationError,
};
use hawdb_integrity::Crc32cHasher;
use std::cell::RefCell;

pub(super) fn flush(
    source: SegmentAccumulator,
    file: &mut File,
    artifact_digest: &mut IntegrityHasher,
    offset: u64,
    work: Option<&CheckpointWorkContext>,
) -> Result<CanonicalSegmentDescriptor, CanonicalSegmentError> {
    flush_inner(source, file, artifact_digest, offset, work, None)
}

pub(super) fn flush_admitted(
    source: SegmentAccumulator,
    file: &mut File,
    artifact_digest: &mut IntegrityHasher,
    offset: u64,
    work: &CheckpointWorkContext,
) -> Result<checkpoint_writer::descriptor::Descriptor, CanonicalSegmentError> {
    work.classify(|work| {
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: RefCell::new(CheckpointAllocationOwner::default()),
        };
        let descriptor = flush_inner(
            source,
            file,
            artifact_digest,
            offset,
            Some(work),
            Some(&context),
        )?;
        Ok(checkpoint_writer::descriptor::Descriptor::new(
            descriptor,
            context.memory.into_inner(),
        ))
    })
    .map_err(|error| match error {
        CheckpointOperationError::Work(error) => CanonicalSegmentError::Work(error),
        CheckpointOperationError::Operation(error) => error,
    })
}

fn flush_inner(
    source: SegmentAccumulator,
    file: &mut File,
    artifact_digest: &mut IntegrityHasher,
    offset: u64,
    work: Option<&CheckpointWorkContext>,
    admission: Option<&CheckpointDecodeContext>,
) -> Result<CanonicalSegmentDescriptor, CanonicalSegmentError> {
    let mut header = [0u8; segment_header_len()];
    header[..8].copy_from_slice(SEGMENT_HEADER);
    header[8] = source.kind.tag();
    header[9..17].copy_from_slice(&source.generation.0.to_le_bytes());
    header[17..25].copy_from_slice(&source.segment_id.to_le_bytes());
    header[25..29].copy_from_slice(&source.record_count.to_le_bytes());
    let length = NonZeroU64::new(header.len().saturating_add(source.records.len()) as u64)
        .expect("segment bytes are non-zero");
    let hard_max = source.config.target_segment_bytes.get().max(
        source
            .config
            .max_record_bytes
            .get()
            .saturating_add(header.len() as u64),
    );
    if length.get() > hard_max {
        return Err(CanonicalSegmentError::SegmentTooLarge {
            segment_bytes: length.get(),
            max_bytes: hard_max,
        });
    }
    let mut crc = Crc32cHasher::new();
    for input in [&header[..], &source.records] {
        for block in input.chunks(64 * 1024) {
            let unit = work.map(CheckpointWorkContext::start_unit).transpose()?;
            crc.update(block);
            if let Some(unit) = unit {
                unit.finish();
            }
            if let Some(work) = work {
                work.checkpoint()?;
            }
        }
    }
    // Failure may leave partial uncommitted bytes, as the ordinary writer does.
    // Do not alter the caller's digest until this complete segment succeeds.
    let mut next_digest = artifact_digest.clone();
    for input in [&header[..], &source.records] {
        for block in input.chunks(64 * 1024) {
            let unit = work.map(CheckpointWorkContext::start_unit).transpose()?;
            {
                let _wave = work.map(CheckpointWorkContext::io_wave).transpose()?;
                file.write_all(block)?;
            }
            next_digest.update(block);
            if let Some(unit) = unit {
                unit.finish();
            }
            if let Some(work) = work {
                work.checkpoint()?;
            }
        }
    }
    // The ordinary Bloom layout and allocation ownership are retained here;
    // only its key insertion loop is split into cooperative fixed-size units.
    let build_bloom = |keys: &[u64]| match admission {
        Some(context) => bloom_admitted(keys, context),
        None => bloom(keys, work),
    };
    let source_endpoint_bloom = build_bloom(&source.source_endpoint_keys)?;
    let target_endpoint_bloom = build_bloom(&source.target_endpoint_keys)?;
    let node_property_bloom = build_bloom(&source.node_property_keys)?;
    if let Some(work) = work {
        work.checkpoint()?;
    }
    *artifact_digest = next_digest;
    Ok(CanonicalSegmentDescriptor {
        segment_id: source.segment_id,
        kind: source.kind,
        offset,
        length,
        content_digest: ContentDigest(crc.finish()),
        min_record_id: source.min_record_id.expect("flushed segment is non-empty"),
        max_record_id: source.max_record_id,
        record_count: source.record_count,
        source_endpoint_bloom,
        target_endpoint_bloom,
        node_property_bloom,
    })
}

fn bloom(
    keys: &[u64],
    work: Option<&CheckpointWorkContext>,
) -> Result<CanonicalEndpointBloom, CanonicalSegmentError> {
    let Some(work) = work else {
        return Ok(CanonicalEndpointBloom::from_keys(keys));
    };
    let unit = work.start_unit()?;
    let word_count = keys
        .len()
        .saturating_mul(BLOOM_BITS_PER_ITEM)
        .div_ceil(u64::BITS as usize)
        .clamp(BLOOM_MIN_WORDS, BLOOM_MAX_WORDS);
    let mut bloom = CanonicalEndpointBloom {
        words: vec![0; word_count].into_boxed_slice(),
        hash_count: BLOOM_HASHES,
    };
    unit.finish();
    insert_keys(&mut bloom, keys, work)?;
    Ok(bloom)
}

fn bloom_admitted(
    keys: &[u64],
    context: &CheckpointDecodeContext,
) -> Result<CanonicalEndpointBloom, CanonicalSegmentError> {
    let unit = context.start_unit()?;
    let word_count = keys
        .len()
        .saturating_mul(BLOOM_BITS_PER_ITEM)
        .div_ceil(u64::BITS as usize)
        .clamp(BLOOM_MIN_WORDS, BLOOM_MAX_WORDS);
    let bytes = word_count * std::mem::size_of::<u64>();
    let token = context
        .reserve(bytes)
        .map_err(|error| CanonicalSegmentError::Corrupt(error.to_string()))?;
    let mut words = Vec::new();
    words.try_reserve_exact(word_count).map_err(|error| {
        CanonicalSegmentError::Work(context.record_failure(CheckpointWorkError::Allocation {
            bytes: bytes as u64,
            reason: error.to_string(),
        }))
    })?;
    if words.capacity() != word_count {
        return Err(CanonicalSegmentError::Work(context.record_failure(
            CheckpointWorkError::Allocation {
                bytes: bytes as u64,
                reason: "segment Bloom capacity differs from admitted capacity".into(),
            },
        )));
    }
    token.address(words.as_ptr() as usize);
    unit.finish();
    for start in (0..word_count).step_by(64 * 1024 / std::mem::size_of::<u64>()) {
        let unit = context.start_unit()?;
        words.resize(
            (start + 64 * 1024 / std::mem::size_of::<u64>()).min(word_count),
            0,
        );
        unit.finish();
        context.checkpoint()?;
    }
    let unit = context.start_unit()?;
    let mut bloom = CanonicalEndpointBloom {
        words: words.into_boxed_slice(),
        hash_count: BLOOM_HASHES,
    };
    unit.finish();
    insert_keys(&mut bloom, keys, context)?;
    Ok(bloom)
}

fn insert_keys(
    bloom: &mut CanonicalEndpointBloom,
    keys: &[u64],
    work: &CheckpointWorkContext,
) -> Result<(), CanonicalSegmentError> {
    for block in keys.chunks(256) {
        let unit = work.start_unit()?;
        for key in block {
            bloom.insert(*key);
        }
        unit.finish();
        work.checkpoint()?;
    }
    work.checkpoint()?;
    Ok(())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod descriptor_tests;
