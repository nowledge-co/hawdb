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

use crate::model::QuantizedSegment;
use crate::transform::normalize_and_transform;
use crate::{
    FileProjection, InMemoryProjection, ProjectionBuildConfig, ProjectionError, ProjectionHit,
    ProjectionIdentity, ProjectionWriter, RaBitQBitWidth, Result,
};
use std::path::Path;

pub(super) const SEED: u64 = 0x9940_2026_8a31_725f;

#[derive(Clone, Copy)]
pub(super) struct Case {
    pub documents: usize,
    pub dimension: usize,
    pub bit_width: RaBitQBitWidth,
}

impl Case {
    pub fn name(self) -> String {
        format!(
            "n{}-d{}-b{}",
            self.documents,
            self.dimension,
            self.bit_width.bits()
        )
    }

    fn config(self) -> ProjectionBuildConfig {
        let mut identity = ProjectionIdentity::new(1);
        identity.embedding_model = Some("hawdb-rabitq-benchmark-splitmix64".into());
        identity.embedding_version = Some(format!("v1-{SEED:016x}"));
        ProjectionBuildConfig::new(self.dimension, identity).with_bit_width(self.bit_width)
    }
}

pub(super) struct Fixture {
    pub case: Case,
    pub file: FileProjection,
    pub memory: InMemoryProjection,
    pub query: Vec<f32>,
    pub transformed_query: Vec<f32>,
}

impl Fixture {
    pub fn open(root: &Path, case: Case, build: bool) -> Result<Self> {
        let directory = root.join(case.name());
        let path = directory.join("projection.rabitq");
        let config = case.config();
        let file = if build {
            // Reserve ownership before the writer can replace any path.
            std::fs::create_dir(&directory)?;
            let mut writer = ProjectionWriter::create(&path, config.clone())?;
            let mut vector = vec![0.0; case.dimension];
            for id in 0..case.documents as u64 {
                fill_vector(id, &mut vector);
                writer.push(id, &vector)?;
            }
            writer.finish()?
        } else {
            FileProjection::open(&path)?
        };
        let manifest = file.manifest();
        if manifest.document_count != case.documents
            || manifest.dimension != case.dimension
            || manifest.bit_width != case.bit_width.bits()
            || manifest.identity != config.identity
            || manifest.transform_seed != config.transform_seed
            || manifest.requested_segment_rows != config.segment_rows
        {
            return Err(ProjectionError::InvalidConfiguration(format!(
                "benchmark artifact does not match {}",
                case.name()
            )));
        }

        // Both backends must score the same persisted codes, not independently
        // rebuilt approximations of the same raw vectors.
        let mut segments = Vec::with_capacity(manifest.segments.len());
        for descriptor in &manifest.segments {
            let buffer = file.read_segment(descriptor.index)?;
            let parts = buffer.parts(case.dimension, case.bit_width, descriptor.row_count)?;
            segments.push(QuantizedSegment {
                base_ordinal: descriptor.base_ordinal,
                ids: (0..descriptor.row_count).map(|row| parts.id(row)).collect(),
                reconstruction_scales: (0..descriptor.row_count)
                    .map(|row| parts.reconstruction_scale(row))
                    .collect(),
                reconstruction_offsets: (0..descriptor.row_count)
                    .map(|row| parts.reconstruction_offset(row))
                    .collect(),
                original_norms: (0..descriptor.row_count)
                    .map(|row| parts.original_norm(row))
                    .collect(),
                codes: parts.codes.to_vec(),
            });
        }
        let memory = InMemoryProjection {
            manifest: manifest.clone(),
            build_report: file.build_report(),
            segments,
        };
        let mut query = vec![0.0; case.dimension];
        fill_vector(u64::MAX - 17, &mut query);
        let mut transformed_query = vec![0.0; case.dimension];
        normalize_and_transform(&query, manifest.transform_seed, &mut transformed_query)?;
        Ok(Self {
            case,
            file,
            memory,
            query,
            transformed_query,
        })
    }
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn fill_vector(id: u64, vector: &mut [f32]) {
    let mut state = SEED ^ id.wrapping_mul(0xd6e8_feb8_6659_fd93);
    for value in vector.iter_mut() {
        *value = (splitmix64(&mut state) >> 40) as f32 / 8_388_608.0 - 1.0;
    }
    let norm = vector
        .iter()
        .map(|&value| f64::from(value) * f64::from(value))
        .sum::<f64>()
        .sqrt();
    assert!(norm.is_finite() && norm > 0.0);
    for value in vector {
        *value = (f64::from(*value) / norm) as f32;
    }
}

pub(super) fn hit_bits(hits: &[ProjectionHit]) -> Vec<(u64, u64)> {
    hits.iter()
        .map(|hit| (hit.id, hit.score.to_bits()))
        .collect()
}
