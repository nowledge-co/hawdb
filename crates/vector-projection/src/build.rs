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

use crate::codec::encode_vector;
use crate::error::{ProjectionError, Result};
use crate::model::{
    InMemoryProjection, ProjectionBuildConfig, ProjectionBuildReport, ProjectionManifest,
    QuantizedSegment, SegmentDescriptor, ValidatedBuildConfig,
};

const DIGEST_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const DIGEST_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Debug)]
pub struct ProjectionBuilder {
    state: BuildState,
    segments: Vec<QuantizedSegment>,
}

impl ProjectionBuilder {
    pub fn new(config: ProjectionBuildConfig) -> Result<Self> {
        let config = config.validated()?;
        Ok(Self {
            state: BuildState::new(config)?,
            segments: Vec::new(),
        })
    }

    pub fn push(&mut self, id: u64, vector: &[f32]) -> Result<()> {
        self.state.push(id, vector)?;
        if self.state.pending_is_full() {
            self.flush(true);
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<InMemoryProjection> {
        self.flush(false);
        let payload_bytes = self
            .segments
            .iter()
            .map(|segment| {
                segment.ids.len() * std::mem::size_of::<u64>()
                    + segment.reconstruction_scales.len() * std::mem::size_of::<f32>()
                    + segment.reconstruction_offsets.len() * std::mem::size_of::<f32>()
                    + segment.codes.len()
            })
            .sum::<usize>() as u64;
        let mut offset = 0u64;
        let segment_descriptors = self
            .segments
            .iter()
            .enumerate()
            .map(|(index, segment)| {
                let payload_bytes = (segment.ids.len() * std::mem::size_of::<u64>()
                    + segment.reconstruction_scales.len() * std::mem::size_of::<f32>()
                    + segment.reconstruction_offsets.len() * std::mem::size_of::<f32>()
                    + segment.codes.len()) as u64;
                let descriptor = SegmentDescriptor {
                    index,
                    base_ordinal: segment.base_ordinal,
                    row_count: segment.row_count(),
                    payload_offset: offset,
                    payload_bytes,
                    payload_checksum: 0,
                };
                offset = offset.saturating_add(payload_bytes);
                descriptor
            })
            .collect::<Vec<_>>();
        let manifest = ProjectionManifest::new(
            &self.state.config,
            self.state.document_count,
            self.state.source_digest,
            payload_bytes,
            0,
            segment_descriptors,
        );
        let build_report = self.state.build_report(self.segments.len(), payload_bytes);
        Ok(InMemoryProjection {
            manifest,
            segments: self.segments,
            build_report,
        })
    }

    fn flush(&mut self, reserve_next_segment: bool) {
        if self.state.pending.is_empty() {
            return;
        }
        let segment = self.state.take_pending();
        self.segments.push(segment);
        if reserve_next_segment {
            self.state.reserve_pending();
        }
    }
}

#[derive(Debug)]
pub(crate) struct BuildState {
    pub config: ValidatedBuildConfig,
    pub pending: QuantizedSegment,
    pub transformed: Vec<f32>,
    pub packed: Vec<u8>,
    pub last_id: Option<u64>,
    pub document_count: usize,
    pub raw_vector_bytes: u64,
    pub source_digest: u64,
}

impl BuildState {
    pub fn new(config: ValidatedBuildConfig) -> Result<Self> {
        Ok(Self {
            pending: QuantizedSegment::with_capacity(&config, 0),
            transformed: vec![0.0; config.dimension],
            packed: Vec::with_capacity(config.bytes_per_vector),
            last_id: None,
            document_count: 0,
            raw_vector_bytes: 0,
            source_digest: DIGEST_OFFSET,
            config,
        })
    }

    pub fn push(&mut self, id: u64, vector: &[f32]) -> Result<()> {
        if vector.len() != self.config.dimension {
            return Err(ProjectionError::InvalidVector(format!(
                "expected dimension {}, got {} for id {id}",
                self.config.dimension,
                vector.len()
            )));
        }
        if let Some(previous) = self.last_id {
            if id == previous {
                return Err(ProjectionError::DuplicateId(id));
            }
            if id < previous {
                return Err(ProjectionError::NonMonotonicId { previous, next: id });
            }
        }
        self.last_id = Some(id);
        let encoding = encode_vector(
            vector,
            self.config.transform_seed,
            self.config.bit_width,
            &mut self.transformed,
            &mut self.packed,
        )?;
        self.pending.ids.push(id);
        self.pending
            .reconstruction_scales
            .push(encoding.reconstruction_scale);
        self.pending
            .reconstruction_offsets
            .push(encoding.reconstruction_offset);
        self.pending.codes.extend_from_slice(&self.packed);
        self.document_count = self.document_count.saturating_add(1);
        self.raw_vector_bytes = self
            .raw_vector_bytes
            .saturating_add((vector.len().saturating_mul(std::mem::size_of::<f32>())) as u64);
        digest_bytes(&mut self.source_digest, &id.to_le_bytes());
        for value in vector {
            digest_bytes(&mut self.source_digest, &value.to_bits().to_le_bytes());
        }
        Ok(())
    }

    pub fn pending_is_full(&self) -> bool {
        self.pending.row_count() >= self.config.admitted_segment_rows
    }

    pub fn take_pending(&mut self) -> QuantizedSegment {
        std::mem::replace(
            &mut self.pending,
            QuantizedSegment::empty(self.document_count),
        )
    }

    pub fn reserve_pending(&mut self) {
        debug_assert!(self.pending.is_empty());
        self.pending = QuantizedSegment::with_capacity(&self.config, self.document_count);
    }

    pub fn build_report(&self, segment_count: usize, payload_bytes: u64) -> ProjectionBuildReport {
        ProjectionBuildReport {
            document_count: self.document_count,
            segment_count,
            raw_vector_bytes: self.raw_vector_bytes,
            projection_payload_bytes: payload_bytes,
            configured_working_bytes: self.config.max_working_bytes,
            peak_working_bytes: self.config.peak_working_bytes,
            requested_segment_rows: self.config.requested_segment_rows,
            admitted_segment_rows: self.config.admitted_segment_rows,
        }
    }
}

pub fn source_digest<'a>(records: impl IntoIterator<Item = (u64, &'a [f32])>) -> u64 {
    let mut digest = DIGEST_OFFSET;
    for (id, vector) in records {
        digest_bytes(&mut digest, &id.to_le_bytes());
        for value in vector {
            digest_bytes(&mut digest, &value.to_bits().to_le_bytes());
        }
    }
    digest
}

fn digest_bytes(digest: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *digest ^= u64::from(*byte);
        *digest = digest.wrapping_mul(DIGEST_PRIME);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ProjectionIdentity, RaBitQBitWidth};

    #[test]
    fn builder_bounds_raw_intermediates_by_segment_budget() {
        let dimension = 64;
        let row_bytes = 8 + 8 + dimension / 2;
        let scratch_bytes = dimension * 4 + dimension / 2 + crate::model::BUILD_FIXED_WORKING_BYTES;
        let budget = scratch_bytes + row_bytes * 3;
        let config = ProjectionBuildConfig::new(dimension, ProjectionIdentity::new(1))
            .with_bit_width(RaBitQBitWidth::Four)
            .with_segment_rows(100)
            .with_max_working_bytes(budget);
        let mut builder = ProjectionBuilder::new(config).unwrap();
        for id in 0..10 {
            let vector = (0..dimension)
                .map(|value| (value as f32 + id as f32).sin())
                .collect::<Vec<_>>();
            builder.push(id, &vector).unwrap();
        }
        let projection = builder.finish().unwrap();

        assert_eq!(projection.manifest.document_count, 10);
        assert_eq!(projection.manifest.admitted_segment_rows, 3);
        assert_eq!(projection.manifest.segments.len(), 4);
        assert!(projection.build_report.peak_working_bytes <= budget);
        assert!(projection.build_report.raw_vector_bytes > budget as u64);
    }

    #[test]
    fn duplicate_ids_fail_closed() {
        let config = ProjectionBuildConfig::new(8, ProjectionIdentity::new(1));
        let mut builder = ProjectionBuilder::new(config).unwrap();
        builder.push(7, &[1.0; 8]).unwrap();
        assert!(matches!(
            builder.push(7, &[2.0; 8]),
            Err(ProjectionError::DuplicateId(7))
        ));
    }

    #[test]
    fn non_monotonic_ids_fail_without_an_unbounded_uniqueness_set() {
        let config = ProjectionBuildConfig::new(8, ProjectionIdentity::new(1));
        let mut builder = ProjectionBuilder::new(config).unwrap();
        builder.push(9, &[1.0; 8]).unwrap();
        assert!(matches!(
            builder.push(7, &[2.0; 8]),
            Err(ProjectionError::NonMonotonicId {
                previous: 9,
                next: 7
            })
        ));
    }
}
