// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual comparison probe, not selected by the adapter's default dependency.
//! All loads cover immutable key slices, never Bf-Tree metadata/ref bits.

use std::cmp::Ordering;

#[inline]
pub fn word(left: &[u8], right: &[u8]) -> Ordering {
    let common = left.len().min(right.len());
    let mut offset = 0;
    while common - offset >= 8 {
        let a = u64::from_be_bytes(left[offset..offset + 8].try_into().expect("eight bytes"));
        let b = u64::from_be_bytes(right[offset..offset + 8].try_into().expect("eight bytes"));
        if a != b {
            return a.cmp(&b);
        }
        offset += 8;
    }
    left[offset..].cmp(&right[offset..])
}

#[inline]
pub fn neon(left: &[u8], right: &[u8]) -> Ordering {
    #[cfg(all(
        target_arch = "aarch64",
        target_endian = "little",
        target_feature = "neon"
    ))]
    {
        use std::arch::aarch64::{vceqq_u64, vgetq_lane_u64, vld1q_u8, vreinterpretq_u64_u8};
        let common = left.len().min(right.len());
        let mut offset = 0;
        while common - offset >= 16 {
            // SAFETY: offset + 16 <= both slice lengths. NEON is enabled for
            // this compilation target. Unaligned loads are supported. These
            // are immutable key bytes, not concurrently mutable ref metadata.
            unsafe {
                let a = vreinterpretq_u64_u8(vld1q_u8(left.as_ptr().add(offset)));
                let b = vreinterpretq_u64_u8(vld1q_u8(right.as_ptr().add(offset)));
                let equal = vceqq_u64(a, b);
                // Compare the first unequal 64-bit lane in byte order. Reuse
                // loaded registers: no second key load or byte-index search.
                if vgetq_lane_u64::<0>(equal) != u64::MAX {
                    return vgetq_lane_u64::<0>(a)
                        .to_be()
                        .cmp(&vgetq_lane_u64::<0>(b).to_be());
                }
                if vgetq_lane_u64::<1>(equal) != u64::MAX {
                    return vgetq_lane_u64::<1>(a)
                        .to_be()
                        .cmp(&vgetq_lane_u64::<1>(b).to_be());
                }
            }
            offset += 16;
        }
        word(&left[offset..], &right[offset..])
    }
    #[cfg(not(all(
        target_arch = "aarch64",
        target_endian = "little",
        target_feature = "neon"
    )))]
    left.cmp(right)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(left: &[u8], right: &[u8]) {
        let expected = left.cmp(right);
        assert_eq!(word(left, right), expected, "word {left:?} {right:?}");
        assert_eq!(neon(left, right), expected, "neon {left:?} {right:?}");
        assert_eq!(word(right, left), expected.reverse());
        assert_eq!(neon(right, left), expected.reverse());
    }

    #[test]
    fn all_lengths_alignments_mismatch_positions_and_prefixes() {
        // The adapter's longest escaped key is 138 bytes. Include larger
        // lengths to exercise additional blocks and every possible tail.
        for len in 0..=160 {
            for alignment in 0..16 {
                let mut left = vec![0; alignment + len];
                for (n, byte) in left[alignment..].iter_mut().enumerate() {
                    *byte = n.wrapping_mul(73) as u8;
                }
                let mut right = left.clone();
                check(&left[alignment..], &right[alignment..]);
                for position in 0..len {
                    right[alignment + position] ^= 0xff;
                    check(&left[alignment..], &right[alignment..]);
                    right[alignment + position] ^= 0xff;
                    check(&left[alignment..position + alignment], &right[alignment..]);
                }
            }
        }
    }

    #[test]
    fn independently_sized_binary_keys_match_slice_order() {
        let mut state = 0xd1b5_4a32_d192_ed03_u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..20_000 {
            let left_len = (next() % 161) as usize;
            let right_len = (next() % 161) as usize;
            let left: Vec<_> = (0..left_len).map(|_| next() as u8).collect();
            let mut right: Vec<_> = (0..right_len).map(|_| next() as u8).collect();
            let prefix = (next() as usize) % (left_len.min(right_len) + 1);
            right[..prefix].copy_from_slice(&left[..prefix]);
            check(&left, &right);
        }
        for byte in 0..=255_u8 {
            check(&[byte; 32], &[0; 32]);
            check(&[byte; 32], &[255; 32]);
        }
    }
}
