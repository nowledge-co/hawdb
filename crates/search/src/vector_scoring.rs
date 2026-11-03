// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Projection-owned score scales. Ranking stays positive until pagination.

use crate::{HawDBError, Result, SearchIndex};
use hawdb_core::{VectorDistance, VectorIndexDescriptor};
use serde::{Deserialize, Serialize};

/// Immutable vector scoring policy for an ordinary search projection.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchVectorScoring {
    /// Positive cosine for ordinary projections; a schema binding owns its metric.
    #[default]
    ProjectionMetric,
    /// Squared L2 over Float32 vectors, exposed as `max(0, 1 - distance / 2)`.
    /// Candidate selection precedes clipping, including zero-score neighbors.
    /// No normalization is applied. Supported by exact search and DiskANN.
    LinearSquaredL2,
}

impl SearchVectorScoring {
    pub(crate) fn is_default(&self) -> bool {
        *self == Self::ProjectionMetric
    }

    pub(crate) fn metric(self, binding: Option<&VectorIndexDescriptor>) -> VectorDistance {
        match self {
            Self::ProjectionMetric => binding.map_or(VectorDistance::Cosine, |d| d.distance()),
            Self::LinearSquaredL2 => VectorDistance::Euclidean,
        }
    }

    pub(crate) fn validate(self, bound: bool, compressed: bool) -> Result<()> {
        if self == Self::LinearSquaredL2 && (bound || compressed) {
            return Err(HawDBError::Semantic(
                "linear squared-L2 scoring requires an ordinary exact/DiskANN projection".into(),
            ));
        }
        Ok(())
    }

    /// Recover the Float32 squared distance from its positive ranking key.
    /// The key is never the public score: clipping it earlier loses Top-K order.
    pub(crate) fn exposed(self, ranking_score: f64) -> f64 {
        if self.is_default() || ranking_score <= 0.0 {
            return ranking_score;
        }
        let distance_bits = (1_u32 << 31) - (ranking_score * f64::from(1_u32 << 31)) as u32;
        let squared_distance = f32::from_bits(distance_bits);
        (1.0 - f64::from(squared_distance) / 2.0).max(0.0)
    }

    pub(crate) fn ranking_score(self, left: &[f32], right: &[f32]) -> Option<f64> {
        if left.len() != right.len() || left.is_empty() {
            return None;
        }
        // Fixed Float32 lanes keep the portable accumulation stable. Different
        // legacy SIMD reductions may differ by a few ulps; do not normalize.
        if left.iter().chain(right).any(|value| !value.is_finite()) {
            return None;
        }
        let mut lanes = [0.0_f32; 16];
        let left_chunks = left.chunks_exact(16);
        let right_chunks = right.chunks_exact(16);
        let tail = left_chunks
            .remainder()
            .iter()
            .zip(right_chunks.remainder())
            .map(|(left, right)| {
                let difference = left - right;
                difference * difference
            })
            .sum::<f32>();
        for (left, right) in left_chunks.zip(right_chunks) {
            for lane in 0..16 {
                let difference = left[lane] - right[lane];
                lanes[lane] += difference * difference;
            }
        }
        let distance = tail + lanes.iter().sum::<f32>();
        // Positive Float32 bit patterns have the same order as their values.
        // Encode a lossless, positive, higher-is-better key in [0, 1]. This
        // retains overflowed distances and every distance beyond the score floor
        // without introducing zero ties or rounding during score conversion.
        Some(f64::from((1_u32 << 31) - distance.to_bits()) / f64::from(1_u32 << 31))
    }
}

impl SearchIndex {
    /// Select scoring on an empty, unregistered ordinary projection builder.
    /// Existing projections require a full replacement to change this policy.
    pub fn set_vector_scoring(&mut self, scoring: SearchVectorScoring) -> Result<()> {
        self.require_unregistered_publication()?;
        if !self.documents.is_empty() || self.vector_document_summary.is_some() {
            return Err(HawDBError::Semantic(
                "vector scoring must be selected before inserting documents".into(),
            ));
        }
        scoring.validate(
            self.vector_index_descriptor.is_some(),
            self.rabitq_checkpoint.is_some(),
        )?;
        #[cfg(feature = "vector-search")]
        self.resident_diskann.reset();
        self.vector_scoring = scoring;
        Ok(())
    }

    pub fn vector_scoring(&self) -> SearchVectorScoring {
        self.vector_scoring
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn squared_distance_keeps_float32_rounding_tail_and_overflow() {
        let scoring = SearchVectorScoring::LinearSquaredL2;
        // Exact powers of two have no reduction-order ambiguity. Include every
        // lane and a tail; no normalization or cosine score can satisfy this.
        for dimension in [1, 16, 17, 1536, 1537] {
            let query = vec![1.0; dimension];
            let embedding = vec![1.0 + 1.0 / 64.0; dimension];
            let key = scoring.ranking_score(&query, &embedding).unwrap();
            assert_eq!(scoring.exposed(key), 1.0 - dimension as f64 / 8192.0);
        }
        let overflow = scoring.ranking_score(&[f32::MAX], &[-f32::MAX]).unwrap();
        let far = scoring.ranking_score(&[0.0], &[10.0]).unwrap();
        assert!(overflow > 0.0 && overflow < far);
        assert_eq!(scoring.exposed(overflow), 0.0);
        assert_eq!(scoring.ranking_score(&[f32::NAN], &[1.0]), None);
        assert_eq!(scoring.ranking_score(&[1.0], &[f32::INFINITY]), None);
        assert_eq!(scoring.ranking_score(&[], &[]), None);
        assert_eq!(scoring.ranking_score(&[1.0], &[1.0, 2.0]), None);
    }
}
