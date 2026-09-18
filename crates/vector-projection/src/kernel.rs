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

use crate::codec::code_at;
use crate::error::{ProjectionError, Result};
use crate::model::RaBitQBitWidth;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanKernel {
    Scalar,
    Avx2,
    Neon,
}

impl ScanKernel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Avx2 => "avx2",
            Self::Neon => "neon",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KernelPreference {
    #[default]
    Auto,
    Scalar,
    Avx2,
    Neon,
}

pub(crate) fn select_kernel(preference: KernelPreference) -> Result<ScanKernel> {
    match preference {
        // Faiss's current ARM_NEON RaBitQ entry points intentionally delegate
        // to their scalar reference implementation. Keep the same correctness
        // boundary while reporting the actual scorer as Scalar.
        KernelPreference::Auto | KernelPreference::Scalar => Ok(ScanKernel::Scalar),
        KernelPreference::Avx2 => Err(ProjectionError::UnsupportedKernel("avx2")),
        KernelPreference::Neon => select_neon_scalar_fallback(),
    }
}

#[cfg(any(target_arch = "aarch64", target_arch = "arm"))]
fn select_neon_scalar_fallback() -> Result<ScanKernel> {
    Ok(ScanKernel::Scalar)
}

#[cfg(not(any(target_arch = "aarch64", target_arch = "arm")))]
fn select_neon_scalar_fallback() -> Result<ScanKernel> {
    Err(ProjectionError::UnsupportedKernel("neon"))
}

/// Returns the integer-code inner product. RaBitQ reconstruction parameters
/// are applied by the scan layer because they vary per vector.
pub(crate) fn score_codes(codes: &[u8], query: &[f32], bit_width: RaBitQBitWidth) -> f32 {
    query
        .iter()
        .enumerate()
        .map(|(dimension, value)| {
            *value * f32::from(code_at(codes, query.len(), dimension, bit_width))
        })
        .sum()
}

pub(crate) type ScoreFunction = fn(&[u8], &[f32], RaBitQBitWidth) -> f32;

pub(crate) fn score_function(_kernel: ScanKernel) -> ScoreFunction {
    // The initial RaBitQ switch retains a single scalar reference kernel on
    // every target. SIMD implementations are a later optimization and must
    // prove exact ordering parity against this code before dispatch changes.
    score_codes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_kernel_preserves_scalar_reference() {
        let dimension: usize = 67;
        let codes = (0..crate::model::encoded_vector_bytes(dimension, RaBitQBitWidth::Four))
            .map(|index| ((index * 29 + 17) & 0xff) as u8)
            .collect::<Vec<_>>();
        let query = (0..dimension)
            .map(|index| (index as f32 * 0.17).sin())
            .collect::<Vec<_>>();
        let scalar = score_codes(&codes, &query, RaBitQBitWidth::Four);
        let selected = score_function(select_kernel(KernelPreference::Auto).unwrap())(
            &codes,
            &query,
            RaBitQBitWidth::Four,
        );
        assert_eq!(scalar, selected);
    }

    #[test]
    fn explicit_avx2_kernel_selection_is_rejected_until_implemented() {
        assert!(matches!(
            select_kernel(KernelPreference::Avx2),
            Err(ProjectionError::UnsupportedKernel("avx2"))
        ));
    }

    #[cfg(not(any(target_arch = "aarch64", target_arch = "arm")))]
    #[test]
    fn explicit_neon_kernel_selection_is_rejected_off_arm() {
        assert!(matches!(
            select_kernel(KernelPreference::Neon),
            Err(ProjectionError::UnsupportedKernel("neon"))
        ));
    }

    #[cfg(any(target_arch = "aarch64", target_arch = "arm"))]
    #[test]
    fn explicit_neon_kernel_selection_uses_the_scalar_fallback() {
        assert_eq!(
            select_kernel(KernelPreference::Neon).unwrap(),
            ScanKernel::Scalar
        );
    }
}
