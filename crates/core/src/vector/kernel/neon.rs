// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::{scalar_coordinate, Input, Mode, Sums};
use std::arch::aarch64::*;

#[target_feature(enable = "neon")]
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

#[target_feature(enable = "neon")]
unsafe fn run_mode<const MODE: u8>(left: Input<'_>, right: Input<'_>, sums: &mut Sums) -> bool {
    let mut index = 0;
    // SIMD lanes carry independent accumulators, preserving each coordinate's
    // addition order without spilling a product array on every iteration.
    let initial = [sums.dot, sums.right];
    // SAFETY: initial has exactly two initialized f64 lanes.
    let mut pair = unsafe { vld1q_f64(initial.as_ptr()) };
    let mut left_norm = sums.left;
    let mut squared = sums.squared;
    while index + 2 <= left.len() {
        // SAFETY: complete two-coordinate unaligned loads are inside both inputs.
        let (l, r) = unsafe {
            (
                vld1_f32(left.as_ptr().add(index)),
                vld1_f32(right.as_ptr().add(index)),
            )
        };
        let exponent = vdup_n_u32(0x7f80_0000);
        let invalid = vorr_u32(
            vceq_u32(vand_u32(vreinterpret_u32_f32(l), exponent), exponent),
            vceq_u32(vand_u32(vreinterpret_u32_f32(r), exponent), exponent),
        );
        if vget_lane_u64::<0>(vreinterpret_u64_u32(invalid)) != 0 {
            return false;
        }
        let (l, r) = (vcvt_f64_f32(l), vcvt_f64_f32(r));
        if MODE == 0 || MODE == 1 {
            let dot = vmulq_f64(l, r);
            let norm = vmulq_f64(r, r);
            pair = vaddq_f64(pair, vzip1q_f64(dot, norm));
            pair = vaddq_f64(pair, vzip2q_f64(dot, norm));
        }
        if MODE == 0 || MODE == 3 {
            let norm = vmulq_f64(l, l);
            left_norm += vgetq_lane_f64::<0>(norm);
            left_norm += vgetq_lane_f64::<1>(norm);
        }
        if MODE == 2 {
            let delta = vsubq_f64(l, r);
            let value = vmulq_f64(delta, delta);
            squared += vgetq_lane_f64::<0>(value);
            squared += vgetq_lane_f64::<1>(value);
        }
        index += 2;
    }
    if MODE == 0 || MODE == 1 {
        sums.dot = vgetq_lane_f64::<0>(pair);
        sums.right = vgetq_lane_f64::<1>(pair);
    }
    if MODE == 0 || MODE == 3 {
        sums.left = left_norm;
    }
    if MODE == 2 {
        sums.squared = squared;
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
