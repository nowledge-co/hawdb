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

// Copyright (c) Meta Platforms, Inc. and affiliates.
//
// Adapted from Faiss multibit::ip_multibit_neon at revision
// 71c7f482d2da0aa1fbed179485818184a5d7df42 (faiss/utils/simd_impl/rabitq_neon.cpp).
//
// MIT License
//
// Copyright (c) Facebook, Inc. and its affiliates.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use crate::model::{encoded_vector_bytes, RaBitQBitWidth};
use std::arch::aarch64::*;

pub(super) fn score_codes(codes: &[u8], query: &[f32], bit_width: RaBitQBitWidth) -> f32 {
    assert!(std::arch::is_aarch64_feature_detected!("neon"));
    assert!(codes.len() >= encoded_vector_bytes(query.len(), bit_width));
    // SAFETY: NEON is available; score bounds every intrinsic load/store.
    unsafe {
        match bit_width {
            RaBitQBitWidth::One => score::<false>(codes, query),
            RaBitQBitWidth::Four => score::<true>(codes, query),
        }
    }
}

#[target_feature(enable = "neon")]
unsafe fn score<const FOUR_BIT: bool>(codes: &[u8], query: &[f32]) -> f32 {
    let (signs, refinement) = codes.split_at(query.len().div_ceil(8));
    // SAFETY: each constant array contains exactly four lanes.
    let (low_positions, high_positions, low_shifts, high_shifts) = unsafe {
        (
            vld1q_u32([1, 2, 4, 8].as_ptr()),
            vld1q_u32([16, 32, 64, 128].as_ptr()),
            vld1q_s32([0, -3, -6, -9].as_ptr()),
            vld1q_s32([-12, -15, -18, -21].as_ptr()),
        )
    };
    let weight = vdupq_n_u32(if FOUR_BIT { 8 } else { 1 });
    let mut sum = -0.0f32;
    let (chunks, tail) = query.as_chunks::<8>();
    for (block, chunk) in chunks.iter().enumerate() {
        let sign = vdupq_n_u32(u32::from(signs[block]));
        let mut low = vandq_u32(vtstq_u32(sign, low_positions), weight);
        let mut high = vandq_u32(vtstq_u32(sign, high_positions), weight);
        if FOUR_BIT {
            let offset = block * 3;
            let packed = vdupq_n_u32(u32::from_le_bytes([
                refinement[offset],
                refinement[offset + 1],
                refinement[offset + 2],
                0,
            ]));
            // Three-bit codes fit in 24 bits, so Faiss's 64-bit shifts and
            // narrowing can be specialized to two four-lane 32-bit shifts.
            let mask = vdupq_n_u32(7);
            low = vaddq_u32(low, vandq_u32(vshlq_u32(packed, low_shifts), mask));
            high = vaddq_u32(high, vandq_u32(vshlq_u32(packed, high_shifts), mask));
        }
        let mut products = [0.0f32; 8];
        // SAFETY: chunk and products each contain eight f32 values; both
        // four-lane halves are in bounds. NEON loads/stores allow unaligned data.
        unsafe {
            vst1q_f32(
                products.as_mut_ptr(),
                vmulq_f32(vld1q_f32(chunk.as_ptr()), vcvtq_f32_u32(low)),
            );
            vst1q_f32(
                products.as_mut_ptr().add(4),
                vmulq_f32(vld1q_f32(chunk.as_ptr().add(4)), vcvtq_f32_u32(high)),
            );
        }
        // Match scalar rounding and ties; do not use Faiss's FMA/reduction.
        for product in products {
            sum += product;
        }
    }
    let bit_width = if FOUR_BIT {
        RaBitQBitWidth::Four
    } else {
        RaBitQBitWidth::One
    };
    for (offset, value) in tail.iter().enumerate() {
        sum += *value
            * f32::from(crate::codec::code_at(
                codes,
                query.len(),
                query.len() - tail.len() + offset,
                bit_width,
            ));
    }
    sum
}
