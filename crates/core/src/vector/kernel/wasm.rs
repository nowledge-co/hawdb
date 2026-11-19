// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::{scalar_coordinate, Input, Mode, Sums};
use std::arch::wasm32::*;

#[target_feature(enable = "simd128")]
pub(super) unsafe fn run(left: Input<'_>, right: Input<'_>, mode: Mode, sums: &mut Sums) -> bool {
    // SAFETY: the WASM module requires SIMD; inputs have equal initialized lengths.
    unsafe {
        match mode {
            Mode::Cosine => run_mode::<0>(left, right, sums),
            Mode::PreparedCosine => run_mode::<1>(left, right, sums),
            Mode::Euclidean => run_mode::<2>(left, right, sums),
            Mode::Norm => run_mode::<3>(left, right, sums),
        }
    }
}

#[target_feature(enable = "simd128")]
unsafe fn run_mode<const MODE: u8>(left: Input<'_>, right: Input<'_>, sums: &mut Sums) -> bool {
    let mut index = 0;
    // Each lane accumulates a different sum in coordinate order, avoiding a
    // horizontal reduction that would change rounding and distance ties.
    let mut pair = f64x2(sums.dot, sums.right);
    let mut left_norm = sums.left;
    let mut squared = sums.squared;
    while index + 2 <= left.len() {
        // SAFETY: each unaligned load reads exactly two in-bounds f32 values.
        let (left_values, right_values) = unsafe {
            (
                v128_load64_zero(left.as_ptr().add(index).cast()),
                v128_load64_zero(right.as_ptr().add(index).cast()),
            )
        };
        let exponent = i32x4_splat(0x7f80_0000);
        let invalid = v128_or(
            i32x4_eq(v128_and(left_values, exponent), exponent),
            i32x4_eq(v128_and(right_values, exponent), exponent),
        );
        if v128_any_true(invalid) {
            return false;
        }
        let (left_values, right_values) = (
            f64x2_promote_low_f32x4(left_values),
            f64x2_promote_low_f32x4(right_values),
        );
        if MODE == 0 || MODE == 1 {
            let dot = f64x2_mul(left_values, right_values);
            let norm = f64x2_mul(right_values, right_values);
            pair = f64x2_add(pair, i64x2_shuffle::<0, 2>(dot, norm));
            pair = f64x2_add(pair, i64x2_shuffle::<1, 3>(dot, norm));
        }
        if MODE == 0 || MODE == 3 {
            let norm = f64x2_mul(left_values, left_values);
            left_norm += f64x2_extract_lane::<0>(norm);
            left_norm += f64x2_extract_lane::<1>(norm);
        }
        if MODE == 2 {
            let delta = f64x2_sub(left_values, right_values);
            let value = f64x2_mul(delta, delta);
            squared += f64x2_extract_lane::<0>(value);
            squared += f64x2_extract_lane::<1>(value);
        }
        index += 2;
    }
    if MODE == 0 || MODE == 1 {
        sums.dot = f64x2_extract_lane::<0>(pair);
        sums.right = f64x2_extract_lane::<1>(pair);
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
