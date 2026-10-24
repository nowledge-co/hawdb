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

#[derive(Default)]
pub(in crate::canonical) struct FlushProgress {
    crc: Crc32cHasher,
    pub(in crate::canonical) completed_blocks: usize,
    pub(in crate::canonical) dirty_tail: bool,
    pub(in crate::canonical) written_blocks: usize,
    pub(in crate::canonical) written_bytes: u64,
    next_digest: Option<IntegrityHasher>,
    blooms: [BloomProgress; 4],
    // Bloom buffers drop before the admission inventory on every path.
    admission: Option<CheckpointDecodeContext>,
    #[cfg(test)]
    pub(in crate::canonical) partial_write_probe: Option<(usize, usize)>,
}

#[derive(Default)]
struct BloomProgress {
    words: Option<Vec<u64>>,
    bloom: Option<CanonicalEndpointBloom>,
    keys_inserted: usize,
    complete: bool,
}

impl FlushProgress {
    #[cfg(test)]
    pub(in crate::canonical) fn bloom_progress_for_test(&self) -> usize {
        self.blooms
            .iter()
            .map(|state| {
                usize::from(state.words.is_some() || state.bloom.is_some() || state.complete)
                    + state.words.as_ref().map_or_else(
                        || state.bloom.as_ref().map_or(0, |bloom| bloom.words.len()),
                        Vec::len,
                    )
                    + usize::from(state.bloom.is_some() || state.complete)
                    + state.keys_inserted
                    + usize::from(state.complete)
            })
            .sum()
    }
}

impl std::fmt::Debug for FlushProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlushProgress")
            .field("completed_blocks", &self.completed_blocks)
            .field("written_blocks", &self.written_blocks)
            .field("written_bytes", &self.written_bytes)
            .field("dirty_tail", &self.dirty_tail)
            .finish_non_exhaustive()
    }
}

pub(super) fn flush(
    source: SegmentAccumulator,
    file: &mut File,
    artifact_digest: &mut IntegrityHasher,
    offset: u64,
    work: Option<&CheckpointWorkContext>,
) -> Result<CanonicalSegmentDescriptor, CanonicalSegmentError> {
    flush_inner(
        &source,
        file,
        artifact_digest,
        offset,
        work,
        &mut FlushProgress::default(),
    )
}

pub(super) fn flush_admitted(
    source: &SegmentAccumulator,
    file: &mut File,
    artifact_digest: &mut IntegrityHasher,
    offset: u64,
    work: &CheckpointWorkContext,
    checksum: &mut FlushProgress,
) -> Result<checkpoint_writer::descriptor::Descriptor, CanonicalSegmentError> {
    work.classify(|work| {
        let context = checksum
            .admission
            .get_or_insert_with(|| CheckpointDecodeContext {
                work: work.clone(),
                memory: RefCell::new(CheckpointAllocationOwner::default()),
            });
        context.work = work.clone();
        let descriptor = flush_inner(source, file, artifact_digest, offset, Some(work), checksum)?;
        let context = checksum
            .admission
            .take()
            .expect("admitted flush owns its inventory");
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
    source: &SegmentAccumulator,
    file: &mut File,
    artifact_digest: &mut IntegrityHasher,
    offset: u64,
    work: Option<&CheckpointWorkContext>,
    checksum: &mut FlushProgress,
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
    let blocks = header
        .chunks(64 * 1024)
        .chain(source.records.chunks(64 * 1024));
    for block in blocks.skip(checksum.completed_blocks) {
        let unit = work.map(CheckpointWorkContext::start_unit).transpose()?;
        checksum.crc.update(block);
        checksum.completed_blocks += 1;
        if let Some(unit) = unit {
            unit.finish();
        }
        if let Some(work) = work {
            work.checkpoint()?;
        }
    }
    // Failure may leave partial uncommitted bytes, as the ordinary writer does.
    // Do not alter the caller's digest until this complete segment succeeds.
    checksum
        .next_digest
        .get_or_insert_with(|| artifact_digest.clone());
    let blocks = header
        .chunks(64 * 1024)
        .chain(source.records.chunks(64 * 1024));
    for block in blocks.skip(checksum.written_blocks) {
        let unit = work.map(CheckpointWorkContext::start_unit).transpose()?;
        {
            let _wave = work.map(CheckpointWorkContext::io_wave).transpose()?;
            checksum.dirty_tail = true;
            #[cfg(test)]
            if let Some((at, count)) = checksum.partial_write_probe
                && at == checksum.written_blocks
            {
                checksum.partial_write_probe = None;
                file.write_all(&block[..count.min(block.len())])?;
                return Err(CanonicalSegmentError::Io(std::io::Error::other(
                    "controlled partial private segment write",
                )));
            }
            file.write_all(block)?;
        }
        checksum
            .next_digest
            .as_mut()
            .expect("private flush owns its digest")
            .update(block);
        checksum.written_bytes = checksum
            .written_bytes
            .checked_add(block.len() as u64)
            .ok_or_else(|| {
                CanonicalSegmentError::Corrupt("private flush byte counter overflow".into())
            })?;
        checksum.written_blocks += 1;
        checksum.dirty_tail = false;
        if let Some(unit) = unit {
            unit.finish();
        }
        if let Some(work) = work {
            work.checkpoint()?;
        }
    }
    let keys = [
        &source.source_endpoint_keys[..],
        &source.target_endpoint_keys[..],
        &source.node_property_keys[..],
        &source.node_label_keys[..],
    ];
    let (source_endpoint_bloom, target_endpoint_bloom, node_property_bloom, node_label_bloom) =
        if let Some(context) = checksum.admission.as_ref() {
            for (state, keys) in checksum.blooms.iter_mut().zip(keys) {
                prepare_bloom_admitted(state, keys, context)?;
            }
            if let Some(work) = work {
                work.checkpoint()?;
            }
            let [source, target, properties, labels] = &mut checksum.blooms;
            (
                source.bloom.take().expect("completed source Bloom"),
                target.bloom.take().expect("completed target Bloom"),
                properties.bloom.take().expect("completed property Bloom"),
                labels.bloom.take().expect("completed label Bloom"),
            )
        } else {
            let source_bloom = bloom(keys[0], work)?;
            let target_bloom = bloom(keys[1], work)?;
            let property_bloom = bloom(keys[2], work)?;
            let label_bloom = bloom(keys[3], work)?;
            if let Some(work) = work {
                work.checkpoint()?;
            }
            (source_bloom, target_bloom, property_bloom, label_bloom)
        };
    *artifact_digest = checksum
        .next_digest
        .take()
        .expect("completed flush owns its digest");
    Ok(CanonicalSegmentDescriptor {
        segment_id: source.segment_id,
        kind: source.kind,
        offset,
        length,
        content_digest: ContentDigest(checksum.crc.finish()),
        min_record_id: source.min_record_id.expect("flushed segment is non-empty"),
        max_record_id: source.max_record_id,
        record_count: source.record_count,
        source_endpoint_bloom,
        target_endpoint_bloom,
        node_property_bloom,
        node_label_bloom,
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

#[cfg(test)]
fn bloom_admitted(
    keys: &[u64],
    context: &CheckpointDecodeContext,
) -> Result<CanonicalEndpointBloom, CanonicalSegmentError> {
    let mut state = BloomProgress::default();
    prepare_bloom_admitted(&mut state, keys, context)?;
    Ok(state.bloom.take().expect("completed Bloom"))
}

fn prepare_bloom_admitted(
    state: &mut BloomProgress,
    keys: &[u64],
    context: &CheckpointDecodeContext,
) -> Result<(), CanonicalSegmentError> {
    if state.complete {
        return Ok(());
    }
    let word_count = keys
        .len()
        .saturating_mul(BLOOM_BITS_PER_ITEM)
        .div_ceil(u64::BITS as usize)
        .clamp(BLOOM_MIN_WORDS, BLOOM_MAX_WORDS);
    if state.words.is_none() && state.bloom.is_none() {
        let unit = context.start_unit()?;
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
        state.words = Some(words);
        unit.finish();
        context.checkpoint()?;
    }
    if let Some(words) = &mut state.words {
        while words.len() < word_count {
            let unit = context.start_unit()?;
            words.resize(
                (words.len() + 64 * 1024 / std::mem::size_of::<u64>()).min(word_count),
                0,
            );
            unit.finish();
            context.checkpoint()?;
        }
        let unit = context.start_unit()?;
        state.bloom = Some(CanonicalEndpointBloom {
            words: state
                .words
                .take()
                .expect("initialized words")
                .into_boxed_slice(),
            hash_count: BLOOM_HASHES,
        });
        unit.finish();
        context.checkpoint()?;
    }
    for block in keys[state.keys_inserted..].chunks(256) {
        let unit = context.start_unit()?;
        for key in block {
            state
                .bloom
                .as_mut()
                .expect("initialized Bloom")
                .insert(*key);
        }
        state.keys_inserted += block.len();
        unit.finish();
        context.checkpoint()?;
    }
    state.complete = true;
    Ok(())
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
