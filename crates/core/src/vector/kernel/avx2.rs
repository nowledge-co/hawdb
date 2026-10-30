// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::{scalar_coordinate, Input, Mode, Sums};
#[cfg(target_arch = "x86")]
use std::arch::x86::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[target_feature(enable = "avx2")]
pub(super) unsafe fn run(left: Input<'_>, right: Input<'_>, mode: Mode, sums: &mut Sums) -> bool {
    // SAFETY: caller established ISA support and equally sized initialized inputs.
    unsafe {
        match mode {
            Mode::Cosine => run_mode::<0>(left, right, sums),
            Mode::PreparedCosine => run_mode::<1>(left, right, sums),
            Mode::Euclidean => run_mode::<2>(left, right, sums),
            Mode::Norm => run_mode::<3>(left, right, sums),
        }
    }
}

#[target_feature(enable = "avx2")]
unsafe fn run_mode<const MODE: u8>(left: Input<'_>, right: Input<'_>, sums: &mut Sums) -> bool {
    let mut index = 0;
    while index + 4 <= left.len() {
        // SAFETY: full unaligned loads stay inside the input blocks; no references
        // to potentially unaligned byte-backed f32 values are constructed.
        let (l, r) = unsafe {
            (
                _mm_loadu_ps(left.as_ptr().add(index)),
                _mm_loadu_ps(right.as_ptr().add(index)),
            )
        };
        let abs_mask = _mm_castsi128_ps(_mm_set1_epi32(i32::MAX));
        let infinity = _mm_set1_ps(f32::INFINITY);
        let finite = _mm_and_ps(
            _mm_cmplt_ps(_mm_and_ps(l, abs_mask), infinity),
            _mm_cmplt_ps(_mm_and_ps(r, abs_mask), infinity),
        );
        if _mm_movemask_ps(finite) != 15 {
            return false;
        }
        let (l, r) = (_mm256_cvtps_pd(l), _mm256_cvtps_pd(r));
        let mut dot = [0.0; 4];
        let mut left_norm = [0.0; 4];
        let mut right_norm = [0.0; 4];
        let mut squared = [0.0; 4];
        // SAFETY: each output array contains exactly one full SIMD register.
        unsafe {
            if MODE == 0 || MODE == 1 {
                _mm256_storeu_pd(dot.as_mut_ptr(), _mm256_mul_pd(l, r));
                _mm256_storeu_pd(right_norm.as_mut_ptr(), _mm256_mul_pd(r, r));
            }
            if MODE == 0 || MODE == 3 {
                _mm256_storeu_pd(left_norm.as_mut_ptr(), _mm256_mul_pd(l, l));
            }
            if MODE == 2 {
                let delta = _mm256_sub_pd(l, r);
                _mm256_storeu_pd(squared.as_mut_ptr(), _mm256_mul_pd(delta, delta));
            }
        }
        // Preserve each accumulator's original order, including across blocks.
        // No FMA or horizontal reduction: exact scores and ties remain unchanged.
        for lane in 0..4 {
            if MODE == 0 || MODE == 1 {
                sums.dot += dot[lane];
                sums.right += right_norm[lane];
            }
            if MODE == 0 || MODE == 3 {
                sums.left += left_norm[lane];
            }
            if MODE == 2 {
                sums.squared += squared[lane];
            }
        }
        index += 4;
    }
    let mode = match MODE {
        0 => Mode::Cosine,
        1 => Mode::PreparedCosine,
        2 => Mode::Euclidean,
        _ => Mode::Norm,
    };
    for index in index..left.len() {
        if !scalar_coordinate(left.get(index), right.get(index), mode, sums) {
            return false;
        }
    }
    true
}
