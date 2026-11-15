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
// Adapted from Faiss multibit::ip_bitplane_avx2 at revision
// 71c7f482d2da0aa1fbed179485818184a5d7df42 (faiss/utils/simd_impl/rabitq_avx2.cpp).
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

#[cfg(target_arch = "x86")]
use std::arch::x86::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

pub(super) fn score_codes(codes: &[u8], query: &[f32], bit_width: RaBitQBitWidth) -> f32 {
    // Keep this function pointer safe even if called without select_kernel.
    assert!(super::avx2_available());
    assert!(codes.len() >= encoded_vector_bytes(query.len(), bit_width));
    // SAFETY: AVX2 was detected above. All loads in score use checked slices.
    unsafe {
        match bit_width {
            RaBitQBitWidth::One => score::<false>(codes, query),
            RaBitQBitWidth::Four => score::<true>(codes, query),
        }
    }
}

#[target_feature(enable = "avx2")]
unsafe fn score<const FOUR_BIT: bool>(codes: &[u8], query: &[f32]) -> f32 {
    let (signs, refinement) = codes.split_at(query.len().div_ceil(8));
    let positions = _mm256_setr_epi32(1, 2, 4, 8, 16, 32, 64, 128);
    let shifts = _mm256_setr_epi32(0, 3, 6, 9, 12, 15, 18, 21);
    let weight = _mm256_set1_ps(if FOUR_BIT { 8.0 } else { 1.0 });
    // Iterator::sum uses negative zero as the floating-point additive identity.
    let mut sum = -0.0f32;
    let (chunks, tail) = query.as_chunks::<8>();
    for (block, chunk) in chunks.iter().enumerate() {
        let sign_mask = _mm256_cmpgt_epi32(
            _mm256_and_si256(_mm256_set1_epi32(i32::from(signs[block])), positions),
            _mm256_setzero_si256(),
        );
        let mut values = _mm256_and_ps(_mm256_castsi256_ps(sign_mask), weight);
        if FOUR_BIT {
            let offset = block * 3;
            // Eight 3-bit codes occupy exactly three bytes. Unlike Faiss's
            // wide window read, this needs no padding after the final code.
            let packed = i32::from_le_bytes([
                refinement[offset],
                refinement[offset + 1],
                refinement[offset + 2],
                0,
            ]);
            // Specializing Faiss's bit-plane expansion to three refinement
            // bits lets AVX2 extract lanes directly, without BMI2 PEXT.
            let extra = _mm256_and_si256(
                _mm256_srlv_epi32(_mm256_set1_epi32(packed), shifts),
                _mm256_set1_epi32(7),
            );
            values = _mm256_add_ps(values, _mm256_cvtepi32_ps(extra));
        }
        // SAFETY: as_chunks guarantees eight readable f32 values; loadu
        // does not require alignment. The output array has eight writable lanes.
        let mut products = [0.0f32; 8];
        unsafe {
            _mm256_storeu_ps(
                products.as_mut_ptr(),
                _mm256_mul_ps(_mm256_loadu_ps(chunk.as_ptr()), values),
            );
        }
        // Preserve the scalar accumulation order, including rounding and ties.
        // Faiss's FMA and horizontal reduction would change candidate scores.
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
        let dimension = query.len() - tail.len() + offset;
        sum += *value
            * f32::from(crate::codec::code_at(
                codes,
                query.len(),
                dimension,
                bit_width,
            ));
    }
    sum
}
