pub(super) fn cosine_similarity(left: &[f32], right: &[f32]) -> Option<f64> {
    if left.len() != right.len() || left.is_empty() {
        return None;
    }
    cosine_similarity_impl(left, right)
}

#[cfg(all(feature = "simd", target_arch = "aarch64"))]
fn cosine_similarity_impl(left: &[f32], right: &[f32]) -> Option<f64> {
    if std::arch::is_aarch64_feature_detected!("neon") {
        // SAFETY: The runtime feature check above guarantees NEON is available.
        unsafe { cosine_similarity_neon(left, right) }
    } else {
        cosine_similarity_scalar(left, right)
    }
}

#[cfg(not(all(feature = "simd", target_arch = "aarch64")))]
fn cosine_similarity_impl(left: &[f32], right: &[f32]) -> Option<f64> {
    cosine_similarity_scalar(left, right)
}

fn cosine_similarity_scalar(left: &[f32], right: &[f32]) -> Option<f64> {
    let mut dot = 0.0_f64;
    let mut left_norm = 0.0_f64;
    let mut right_norm = 0.0_f64;
    for (l, r) in left.iter().zip(right.iter()) {
        let l = f64::from(*l);
        let r = f64::from(*r);
        dot += l * r;
        left_norm += l * l;
        right_norm += r * r;
    }
    finish_cosine_similarity(dot, left_norm, right_norm)
}

fn finish_cosine_similarity(dot: f64, left_norm: f64, right_norm: f64) -> Option<f64> {
    if left_norm == 0.0 || right_norm == 0.0 {
        return None;
    }
    Some((dot / (left_norm.sqrt() * right_norm.sqrt())).max(0.0))
}

#[cfg(all(feature = "simd", target_arch = "aarch64"))]
#[target_feature(enable = "neon")]
unsafe fn cosine_similarity_neon(left: &[f32], right: &[f32]) -> Option<f64> {
    use std::arch::aarch64::{vdupq_n_f32, vld1q_f32, vmlaq_f32};

    let mut dot = vdupq_n_f32(0.0);
    let mut left_norm = vdupq_n_f32(0.0);
    let mut right_norm = vdupq_n_f32(0.0);

    let chunks = left.len() / 4;
    for chunk in 0..chunks {
        let offset = chunk * 4;
        let l = vld1q_f32(left.as_ptr().add(offset));
        let r = vld1q_f32(right.as_ptr().add(offset));
        dot = vmlaq_f32(dot, l, r);
        left_norm = vmlaq_f32(left_norm, l, l);
        right_norm = vmlaq_f32(right_norm, r, r);
    }

    let dot = horizontal_sum_f32x4(dot);
    let left_norm = horizontal_sum_f32x4(left_norm);
    let right_norm = horizontal_sum_f32x4(right_norm);
    let (dot, left_norm, right_norm) = left[chunks * 4..].iter().zip(&right[chunks * 4..]).fold(
        (f64::from(dot), f64::from(left_norm), f64::from(right_norm)),
        |(dot, left_norm, right_norm), (l, r)| {
            let l = f64::from(*l);
            let r = f64::from(*r);
            (dot + l * r, left_norm + l * l, right_norm + r * r)
        },
    );

    finish_cosine_similarity(dot, left_norm, right_norm)
}

#[cfg(all(feature = "simd", target_arch = "aarch64"))]
#[inline]
unsafe fn horizontal_sum_f32x4(value: std::arch::aarch64::float32x4_t) -> f32 {
    use std::arch::aarch64::vgetq_lane_f32;

    vgetq_lane_f32::<0>(value)
        + vgetq_lane_f32::<1>(value)
        + vgetq_lane_f32::<2>(value)
        + vgetq_lane_f32::<3>(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cosine_similarity_rejects_invalid_inputs() {
        assert_eq!(cosine_similarity(&[], &[]), None);
        assert_eq!(cosine_similarity(&[1.0], &[1.0, 0.0]), None);
        assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 0.0]), None);
    }

    #[test]
    fn cosine_similarity_matches_expected_values() {
        assert_close(cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]).unwrap(), 1.0);
        assert_close(cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]).unwrap(), 0.0);
        assert_close(
            cosine_similarity(&[1.0, 2.0, 3.0, 4.0, 5.0], &[5.0, 4.0, 3.0, 2.0, 1.0]).unwrap(),
            35.0 / 55.0,
        );
    }

    #[cfg(all(feature = "simd", target_arch = "aarch64"))]
    #[test]
    fn neon_cosine_similarity_matches_scalar() {
        let left = [0.25, 1.5, -2.0, 3.0, 4.5, -0.75, 8.0];
        let right = [2.0, -1.0, 0.5, 0.25, 1.25, 3.0, -4.0];

        let scalar = cosine_similarity_scalar(&left, &right).unwrap();
        let neon = unsafe { cosine_similarity_neon(&left, &right).unwrap() };

        assert_close(neon, scalar);
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1.0e-6,
            "actual={actual}, expected={expected}"
        );
    }
}
