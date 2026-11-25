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

/// Scores four right operands at once. Each SIMD lane owns one operand and adds
/// its coordinates in order, so every sum has the bits of the one-operand kernel.
/// Returns false on any non-finite coordinate. The four blocks and `left` have
/// equal lengths.
#[target_feature(enable = "avx2")]
pub(super) unsafe fn run_four<const EUCLIDEAN: bool>(
    left: Input<'_>,
    right: [Input<'_>; 4],
    sums: &mut [Sums; 4],
) -> bool {
    let mut first = [0.0; 4];
    let mut second = [0.0; 4];
    for lane in 0..4 {
        first[lane] = if EUCLIDEAN {
            sums[lane].squared
        } else {
            sums[lane].dot
        };
        second[lane] = sums[lane].right;
    }
    // SAFETY: each array holds exactly one full SIMD register.
    let (mut first_sum, mut second_sum) = unsafe {
        (
            _mm256_loadu_pd(first.as_ptr()),
            _mm256_loadu_pd(second.as_ptr()),
        )
    };
    let abs_mask = _mm_castsi128_ps(_mm_set1_epi32(i32::MAX));
    let infinity = _mm_set1_ps(f32::INFINITY);
    let finite = |value| _mm_cmplt_ps(_mm_and_ps(value, abs_mask), infinity);
    let mut index = 0;
    while index + 4 <= left.len() {
        // SAFETY: full unaligned loads stay inside the equally sized blocks; no
        // references to potentially unaligned byte-backed f32 values are made.
        let (l, r) = unsafe {
            (
                _mm_loadu_ps(left.as_ptr().add(index)),
                right.map(|input| _mm_loadu_ps(input.as_ptr().add(index))),
            )
        };
        let valid = _mm_and_ps(
            _mm_and_ps(finite(r[0]), finite(r[1])),
            _mm_and_ps(finite(r[2]), finite(r[3])),
        );
        if _mm_movemask_ps(_mm_and_ps(valid, finite(l))) != 15 {
            return false;
        }
        // Transpose, so each vector holds one coordinate of the four operands.
        let low01 = _mm_unpacklo_ps(r[0], r[1]);
        let low23 = _mm_unpacklo_ps(r[2], r[3]);
        let high01 = _mm_unpackhi_ps(r[0], r[1]);
        let high23 = _mm_unpackhi_ps(r[2], r[3]);
        let columns = [
            _mm_movelh_ps(low01, low23),
            _mm_movehl_ps(low23, low01),
            _mm_movelh_ps(high01, high23),
            _mm_movehl_ps(high23, high01),
        ];
        let l = [
            _mm_shuffle_ps::<0x00>(l, l),
            _mm_shuffle_ps::<0x55>(l, l),
            _mm_shuffle_ps::<0xaa>(l, l),
            _mm_shuffle_ps::<0xff>(l, l),
        ];
        // No FMA or reduction: one addition per lane and coordinate, in order.
        for (l, column) in l.into_iter().zip(columns) {
            let (l, r) = (_mm256_cvtps_pd(l), _mm256_cvtps_pd(column));
            if EUCLIDEAN {
                let delta = _mm256_sub_pd(l, r);
                first_sum = _mm256_add_pd(first_sum, _mm256_mul_pd(delta, delta));
            } else {
                first_sum = _mm256_add_pd(first_sum, _mm256_mul_pd(l, r));
                second_sum = _mm256_add_pd(second_sum, _mm256_mul_pd(r, r));
            }
        }
        index += 4;
    }
    // SAFETY: each output array holds exactly one full SIMD register.
    unsafe {
        _mm256_storeu_pd(first.as_mut_ptr(), first_sum);
        _mm256_storeu_pd(second.as_mut_ptr(), second_sum);
    }
    let mode = if EUCLIDEAN {
        Mode::Euclidean
    } else {
        Mode::PreparedCosine
    };
    for lane in 0..4 {
        if EUCLIDEAN {
            sums[lane].squared = first[lane];
        } else {
            sums[lane].dot = first[lane];
            sums[lane].right = second[lane];
        }
        for index in index..left.len() {
            if !scalar_coordinate(
                left.get(index),
                right[lane].get(index),
                mode,
                &mut sums[lane],
            ) {
                return false;
            }
        }
    }
    true
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
