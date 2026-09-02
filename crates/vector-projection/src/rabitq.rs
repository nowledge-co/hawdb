use crate::error::{ProjectionError, Result};
use crate::model::{encoded_vector_bytes, RaBitQBitWidth};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

const RESCALE_EPSILON: f64 = 1e-5;
const RESCALE_ENUMERATION_SLACK: usize = 10;
const TIGHT_START: [f64; 9] = [0.0, 0.15, 0.20, 0.52, 0.59, 0.71, 0.75, 0.77, 0.81];

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct RaBitQEncoding {
    pub reconstruction_scale: f32,
    pub reconstruction_offset: f32,
}

/// Encodes one transformed unit vector in Faiss's scalar RaBitQ code layout:
/// one LSB-first sign plane followed by an optional LSB-first refinement
/// plane. The Skein artifact stores reconstruction factors separately from
/// these code planes.
///
/// The reference algorithm chooses the per-vector rescaling factor that
/// maximizes the alignment between the vector and its offset-binary code. The
/// returned affine reconstruction is used only for approximate candidate
/// scoring; raw embeddings remain the final ranking source.
pub(crate) fn encode(
    transformed: &[f32],
    bit_width: RaBitQBitWidth,
    packed: &mut Vec<u8>,
) -> Result<RaBitQEncoding> {
    if transformed.is_empty() || !transformed.iter().all(|value| value.is_finite()) {
        return Err(ProjectionError::InvalidVector(
            "RaBitQ requires a non-empty finite transformed vector".to_string(),
        ));
    }

    let refinement_bits = bit_width.refinement_bits();
    let sign_code_bytes = bit_width.sign_code_bytes(transformed.len());
    packed.clear();
    packed.resize(encoded_vector_bytes(transformed.len(), bit_width), 0);
    let squared_norm = transformed
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>();
    if squared_norm <= f64::EPSILON {
        return Ok(RaBitQEncoding {
            reconstruction_scale: 0.0,
            reconstruction_offset: 0.0,
        });
    }
    let norm = squared_norm.sqrt();
    let absolute = transformed
        .iter()
        .map(|value| f64::from(value.abs()) / norm)
        .collect::<Vec<_>>();
    let rescale = if refinement_bits == 0 {
        0.0
    } else {
        best_rescale_factor(&absolute, refinement_bits)?
    };
    let refinement_limit = (1usize << refinement_bits) - 1;
    let center = -((1usize << refinement_bits) as f32 - 0.5);
    let mut code_squared_norm = 0.0f64;
    let mut alignment = 0.0f64;
    for (dimension, (&value, &absolute)) in transformed.iter().zip(&absolute).enumerate() {
        let refinement = if refinement_bits == 0 {
            0
        } else {
            ((rescale * absolute) + RESCALE_EPSILON)
                .floor()
                .clamp(0.0, refinement_limit as f64) as u8
        };
        let refinement = if value < 0.0 {
            (!refinement) & refinement_limit as u8
        } else {
            refinement
        };
        let code = refinement | if value > 0.0 { 1 << refinement_bits } else { 0 };
        if value > 0.0 {
            set_bit(&mut packed[..sign_code_bytes], dimension);
        }
        if refinement_bits > 0 {
            set_refinement_code(
                &mut packed[sign_code_bytes..],
                dimension,
                refinement_bits,
                refinement,
            );
        }
        let centered = f64::from(code) + f64::from(center);
        code_squared_norm += centered * centered;
        alignment += f64::from(value) * centered;
    }
    if code_squared_norm <= f64::EPSILON || !alignment.is_finite() {
        return Err(ProjectionError::InvalidVector(
            "RaBitQ produced an invalid reconstruction basis".to_string(),
        ));
    }
    let reconstruction_scale = (alignment / code_squared_norm) as f32;
    if !reconstruction_scale.is_finite() || reconstruction_scale < 0.0 {
        return Err(ProjectionError::InvalidVector(
            "RaBitQ produced an invalid reconstruction scale".to_string(),
        ));
    }
    Ok(RaBitQEncoding {
        reconstruction_scale,
        reconstruction_offset: reconstruction_scale * center,
    })
}

pub(crate) fn code_at(
    packed: &[u8],
    vector_dimension: usize,
    dimension: usize,
    bit_width: RaBitQBitWidth,
) -> u8 {
    let sign_code_bytes = bit_width.sign_code_bytes(vector_dimension);
    let sign = u8::from(extract_bit(&packed[..sign_code_bytes], dimension));
    let refinement_bits = bit_width.refinement_bits();
    if refinement_bits == 0 {
        return sign;
    }
    let refinement =
        extract_refinement_code(&packed[sign_code_bytes..], dimension, refinement_bits);
    refinement | (sign << refinement_bits)
}

fn set_bit(packed: &mut [u8], dimension: usize) {
    packed[dimension / u8::BITS as usize] |= 1 << (dimension % u8::BITS as usize);
}

fn extract_bit(packed: &[u8], dimension: usize) -> bool {
    (packed[dimension / u8::BITS as usize] & (1 << (dimension % u8::BITS as usize))) != 0
}

fn set_refinement_code(packed: &mut [u8], dimension: usize, refinement_bits: usize, code: u8) {
    debug_assert!(matches!(refinement_bits, 1 | 3));
    debug_assert!(code < (1 << refinement_bits));
    let start = dimension * refinement_bits;
    for bit in 0..refinement_bits {
        if (code & (1 << bit)) != 0 {
            set_bit(packed, start + bit);
        }
    }
}

fn extract_refinement_code(packed: &[u8], dimension: usize, refinement_bits: usize) -> u8 {
    let start = dimension * refinement_bits;
    let mut code = 0;
    for bit in 0..refinement_bits {
        if extract_bit(packed, start + bit) {
            code |= 1 << bit;
        }
    }
    code
}

fn best_rescale_factor(absolute: &[f64], refinement_bits: usize) -> Result<f64> {
    let maximum = absolute.iter().copied().fold(0.0f64, f64::max);
    if maximum <= f64::EPSILON {
        return Ok(0.0);
    }
    let refinement_limit = (1usize << refinement_bits) - 1;
    let end = (refinement_limit + RESCALE_ENUMERATION_SLACK) as f64 / maximum;
    let start = end * TIGHT_START[refinement_bits];
    let mut codes = Vec::with_capacity(absolute.len());
    let mut squared_norm = absolute.len() as f64 * 0.25;
    let mut alignment = 0.0f64;
    let mut pending = BinaryHeap::new();
    for (index, value) in absolute.iter().copied().enumerate() {
        let code = ((start * value) + RESCALE_EPSILON).floor() as usize;
        codes.push(code);
        squared_norm += (code * code + code) as f64;
        alignment += (code as f64 + 0.5) * value;
        if value > f64::EPSILON {
            pending.push(RescaleEvent::new((code + 1) as f64 / value, index));
        }
    }

    let mut best_alignment = 0.0f64;
    let mut best_rescale = 0.0f64;
    while let Some(event) = pending.pop() {
        let code = &mut codes[event.index];
        *code += 1;
        squared_norm += (2 * *code) as f64;
        alignment += absolute[event.index];
        let normalized_alignment = alignment / squared_norm.sqrt();
        if normalized_alignment > best_alignment {
            best_alignment = normalized_alignment;
            best_rescale = event.threshold;
        }
        if *code < refinement_limit {
            let next_threshold = (*code + 1) as f64 / absolute[event.index];
            if next_threshold < end {
                pending.push(RescaleEvent::new(next_threshold, event.index));
            }
        }
    }
    if !best_rescale.is_finite() || best_rescale <= 0.0 {
        return Err(ProjectionError::InvalidVector(
            "RaBitQ could not derive a positive rescaling factor".to_string(),
        ));
    }
    Ok(best_rescale)
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct RescaleEvent {
    threshold: f64,
    index: usize,
}

impl RescaleEvent {
    fn new(threshold: f64, index: usize) -> Self {
        Self { threshold, index }
    }
}

impl Eq for RescaleEvent {}

impl Ord for RescaleEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .threshold
            .total_cmp(&self.threshold)
            .then_with(|| other.index.cmp(&self.index))
    }
}

impl PartialOrd for RescaleEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_four_bit_sign_and_refinement_codes() {
        let values = [0.4, -0.2, 0.1, -0.6, 0.3];
        let mut packed = Vec::new();
        let encoding = encode(&values, RaBitQBitWidth::Four, &mut packed).unwrap();
        assert_eq!(packed.len(), 1 + values.len().saturating_mul(3).div_ceil(8));
        assert_eq!(packed[0], 0b0001_0101);
        assert!(encoding.reconstruction_scale.is_finite());
        assert!(encoding.reconstruction_offset.is_finite());
        assert!(code_at(&packed, values.len(), 0, RaBitQBitWidth::Four) >= 8);
        assert!(code_at(&packed, values.len(), 1, RaBitQBitWidth::Four) < 8);
    }

    #[test]
    fn encodes_one_bit_sign_codes_in_little_endian_bit_order() {
        let values = [1.0, -1.0, 0.0, 2.0, -3.0, 4.0, -5.0, 6.0, -7.0];
        let mut packed = Vec::new();
        let encoding = encode(&values, RaBitQBitWidth::One, &mut packed).unwrap();

        assert_eq!(packed, [0b1010_1001, 0]);
        assert_eq!(packed.len(), 2);
        assert!(encoding.reconstruction_scale.is_finite());
        assert_eq!(
            encoding.reconstruction_offset,
            -0.5 * encoding.reconstruction_scale
        );
        assert_eq!(code_at(&packed, values.len(), 0, RaBitQBitWidth::One), 1);
        assert_eq!(code_at(&packed, values.len(), 1, RaBitQBitWidth::One), 0);
        assert_eq!(code_at(&packed, values.len(), 8, RaBitQBitWidth::One), 0);
    }

    #[test]
    fn four_bit_refinement_plane_matches_faiss_little_endian_packing() {
        let dimension = 3;
        let mut packed = vec![0; encoded_vector_bytes(dimension, RaBitQBitWidth::Four)];
        set_bit(&mut packed[..1], 0);
        set_bit(&mut packed[..1], 2);
        set_refinement_code(&mut packed[1..], 0, 3, 0b001);
        set_refinement_code(&mut packed[1..], 1, 3, 0b010);
        set_refinement_code(&mut packed[1..], 2, 3, 0b111);

        assert_eq!(packed, [0b0000_0101, 0b1101_0001, 0b0000_0001]);
        assert_eq!(code_at(&packed, dimension, 0, RaBitQBitWidth::Four), 0b1001);
        assert_eq!(code_at(&packed, dimension, 1, RaBitQBitWidth::Four), 0b0010);
        assert_eq!(code_at(&packed, dimension, 2, RaBitQBitWidth::Four), 0b1111);
    }

    #[test]
    fn zero_vector_has_zero_affine_reconstruction() {
        let mut packed = Vec::new();
        let encoding = encode(&[0.0; 8], RaBitQBitWidth::One, &mut packed).unwrap();
        assert_eq!(encoding.reconstruction_scale, 0.0);
        assert_eq!(encoding.reconstruction_offset, 0.0);
    }

    #[test]
    fn affine_score_matches_the_reconstructed_coordinates() {
        let values = [0.55, -0.31, 0.14, -0.72, 0.09, 0.38, -0.26];
        let query = [-0.4, 0.8, 0.1, -0.2, 0.3, -0.5, 0.7];
        let mut packed = Vec::new();
        let encoding = encode(&values, RaBitQBitWidth::Four, &mut packed).unwrap();

        let reconstructed_score = values
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let coordinate = encoding.reconstruction_scale
                    * f32::from(code_at(&packed, values.len(), index, RaBitQBitWidth::Four))
                    + encoding.reconstruction_offset;
                query[index] * coordinate
            })
            .sum::<f32>();
        let affine_score = encoding.reconstruction_scale
            * query
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    *value * f32::from(code_at(&packed, values.len(), index, RaBitQBitWidth::Four))
                })
                .sum::<f32>()
            + encoding.reconstruction_offset * query.iter().sum::<f32>();

        assert!((reconstructed_score - affine_score).abs() <= f32::EPSILON);
    }
}
