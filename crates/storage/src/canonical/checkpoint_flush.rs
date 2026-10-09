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
use hawdb_integrity::Crc32cHasher;

pub(super) fn flush(
    source: SegmentAccumulator,
    file: &mut File,
    artifact_digest: &mut IntegrityHasher,
    offset: u64,
    work: Option<&CheckpointWorkContext>,
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
    let source_endpoint_bloom = bloom(&source.source_endpoint_keys, work)?;
    let target_endpoint_bloom = bloom(&source.target_endpoint_keys, work)?;
    let node_property_bloom = bloom(&source.node_property_keys, work)?;
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
    for block in keys.chunks(256) {
        let unit = work.start_unit()?;
        for key in block {
            bloom.insert(*key);
        }
        unit.finish();
        work.checkpoint()?;
    }
    work.checkpoint()?;
    Ok(bloom)
}

#[cfg(test)]
mod tests;
