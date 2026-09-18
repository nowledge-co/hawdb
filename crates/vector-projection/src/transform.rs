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

use crate::error::{ProjectionError, Result};

pub(crate) fn normalize_and_transform(input: &[f32], seed: u64, output: &mut [f32]) -> Result<()> {
    if input.len() != output.len() {
        return Err(ProjectionError::InvalidVector(format!(
            "expected dimension {}, got {}",
            output.len(),
            input.len()
        )));
    }
    if input.is_empty() {
        return Err(ProjectionError::InvalidVector(
            "dimension must be greater than zero".to_string(),
        ));
    }
    if !input.iter().all(|value| value.is_finite()) {
        return Err(ProjectionError::InvalidVector(
            "coordinates must be finite".to_string(),
        ));
    }

    let squared_norm = input
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>();
    if squared_norm <= f64::EPSILON {
        output.fill(0.0);
        return Ok(());
    }
    let inverse_norm = squared_norm.sqrt().recip() as f32;
    for (index, (source, target)) in input.iter().zip(output.iter_mut()).enumerate() {
        let sign = if splitmix64(seed ^ index as u64) & 1 == 0 {
            1.0
        } else {
            -1.0
        };
        *target = *source * inverse_norm * sign;
    }

    let mut offset = 0;
    while offset < output.len() {
        let block_len = largest_power_of_two(output.len() - offset);
        hadamard(&mut output[offset..offset + block_len]);
        let scale = (block_len as f32).sqrt().recip();
        for value in &mut output[offset..offset + block_len] {
            *value *= scale;
        }
        offset += block_len;
    }
    Ok(())
}

fn largest_power_of_two(value: usize) -> usize {
    debug_assert!(value > 0);
    1usize << (usize::BITS - 1 - value.leading_zeros())
}

fn hadamard(values: &mut [f32]) {
    debug_assert!(values.len().is_power_of_two());
    let mut width = 1;
    while width < values.len() {
        for base in (0..values.len()).step_by(width * 2) {
            for lane in 0..width {
                let left = values[base + lane];
                let right = values[base + width + lane];
                values[base + lane] = left + right;
                values[base + width + lane] = left - right;
            }
        }
        width *= 2;
    }
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transform_is_deterministic_and_preserves_norm() {
        let input = [1.0, -2.0, 3.0, 4.0, -1.0, 2.0, 0.5];
        let mut first = [0.0; 7];
        let mut second = [0.0; 7];
        normalize_and_transform(&input, 7, &mut first).unwrap();
        normalize_and_transform(&input, 7, &mut second).unwrap();

        assert_eq!(first, second);
        let norm = first.iter().map(|value| value * value).sum::<f32>();
        assert!((norm - 1.0).abs() < 1e-5, "norm={norm}");
    }

    #[test]
    fn zero_vector_remains_zero() {
        let mut output = [1.0; 5];
        normalize_and_transform(&[0.0; 5], 11, &mut output).unwrap();
        assert_eq!(output, [0.0; 5]);
    }
}
