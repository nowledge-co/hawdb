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

#![forbid(unsafe_code)]

use bitpacking::{BitPacker, BitPacker4x};

pub(super) const BLOCK_LEN: usize = 128;
const HEADER_LEN: usize = 32;
pub(super) const MAX_BLOCK_BYTES: usize = HEADER_LEN + BLOCK_LEN * 15;
type Result<T> = std::result::Result<T, &'static str>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Posting {
    pub(super) ordinal: u64,
    pub(super) tf: u32,
}

fn put_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 128 {
        output.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    output.push(value as u8);
}

fn get_varint(input: &[u8], position: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    for group in 0..10 {
        let byte = *input.get(*position).ok_or("truncated varint")?;
        *position += 1;
        if group == 9 && byte > 1 {
            return Err("varint overflows u64");
        }
        value |= u64::from(byte & 127) << (group * 7);
        if byte & 128 == 0 {
            if group != 0 && byte == 0 {
                return Err("noncanonical varint");
            }
            return Ok(value);
        }
    }
    Err("unterminated varint")
}

fn pack(values: &[u32; BLOCK_LEN], output: &mut Vec<u8>) -> u8 {
    let packer = BitPacker4x::new();
    let width = packer.num_bits(values);
    let mut bytes = [0u8; BLOCK_LEN * 4];
    let size = packer.compress(values, &mut bytes, width);
    for word in bytes[..size].chunks_exact(4) {
        output.extend_from_slice(&u32::from_ne_bytes(word.try_into().unwrap()).to_le_bytes());
    }
    width
}

fn unpack(input: &[u8], width: u8, output: &mut [u32; BLOCK_LEN]) -> Result<()> {
    if width > 32 || input.len() != usize::from(width) * 16 {
        return Err("invalid bitpack extent");
    }
    let mut native = [0u8; BLOCK_LEN * 4];
    for (source, target) in input.chunks_exact(4).zip(native.chunks_exact_mut(4)) {
        target.copy_from_slice(&u32::from_le_bytes(source.try_into().unwrap()).to_ne_bytes());
    }
    BitPacker4x::new().decompress(&native[..input.len()], output, width);
    Ok(())
}

#[cfg(test)]
pub(super) fn encode(postings: &[Posting]) -> Result<Vec<u8>> {
    encode_by(postings.len(), |index| postings[index])
}

pub(super) fn encode_by(count: usize, posting: impl Fn(usize) -> Posting) -> Result<Vec<u8>> {
    if count == 0 || count > BLOCK_LEN {
        return Err("invalid posting count");
    }
    let mut deltas = [0u32; BLOCK_LEN];
    let mut frequencies = [0u32; BLOCK_LEN];
    let mut wide = false;
    let first = posting(0);
    let mut previous = first.ordinal;
    let mut max_tf = 0u32;
    for index in 0..count {
        let posting = posting(index);
        if posting.tf == 0 || (index > 0 && posting.ordinal <= previous) {
            return Err("invalid posting order or frequency");
        }
        let delta = posting.ordinal - previous;
        match u32::try_from(delta) {
            Ok(value) => deltas[index] = value,
            Err(_) => wide = true,
        }
        frequencies[index] = posting.tf;
        previous = posting.ordinal;
        max_tf = max_tf.max(posting.tf);
    }
    let mode = if count < BLOCK_LEN {
        1
    } else if wide {
        2
    } else {
        0
    };
    let mut bytes = vec![0u8; HEADER_LEN];
    bytes[..4].copy_from_slice(b"LXP1");
    bytes[4..6].copy_from_slice(&(count as u16).to_le_bytes());
    bytes[6..8].copy_from_slice(&(max_tf.min(u16::MAX as u32) as u16).to_le_bytes());
    bytes[8] = mode;
    bytes[12..20].copy_from_slice(&first.ordinal.to_le_bytes());
    bytes[20..28].copy_from_slice(&previous.to_le_bytes());
    if mode == 0 {
        let doc_bits = pack(&deltas, &mut bytes);
        let tf_bits = pack(&frequencies, &mut bytes);
        bytes[9] = doc_bits;
        bytes[10] = tf_bits;
    } else {
        previous = first.ordinal;
        for index in 0..count {
            let posting = posting(index);
            put_varint(posting.ordinal - previous, &mut bytes);
            put_varint(u64::from(posting.tf), &mut bytes);
            previous = posting.ordinal;
        }
    }
    let size = (bytes.len() - HEADER_LEN) as u32;
    bytes[28..32].copy_from_slice(&size.to_le_bytes());
    Ok(bytes)
}

pub(super) fn decode(bytes: &[u8]) -> Result<Vec<Posting>> {
    if !(HEADER_LEN..=MAX_BLOCK_BYTES).contains(&bytes.len()) || &bytes[..4] != b"LXP1" {
        return Err("invalid block envelope");
    }
    let count = usize::from(u16::from_le_bytes(bytes[4..6].try_into().unwrap()));
    let max_tf = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
    let mode = bytes[8];
    let doc_bits = bytes[9];
    let tf_bits = bytes[10];
    let base = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
    let last = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
    let size = u32::from_le_bytes(bytes[28..32].try_into().unwrap()) as usize;
    if !(1..=BLOCK_LEN).contains(&count)
        || size != bytes.len() - HEADER_LEN
        || bytes[11] != 0
        || last < base
        || max_tf == 0
    {
        return Err("invalid block header");
    }
    let payload = &bytes[HEADER_LEN..];
    let mut deltas = [0u32; BLOCK_LEN];
    let mut frequencies = [0u32; BLOCK_LEN];
    match mode {
        0 if count == BLOCK_LEN && doc_bits <= 32 && tf_bits <= 32 => {
            let split = usize::from(doc_bits) * 16;
            if payload.len() != split + usize::from(tf_bits) * 16 {
                return Err("invalid bitpack payload length");
            }
            unpack(&payload[..split], doc_bits, &mut deltas)?;
            unpack(&payload[split..], tf_bits, &mut frequencies)?;
        }
        1 if count < BLOCK_LEN && doc_bits == 0 && tf_bits == 0 => {}
        2 if count == BLOCK_LEN && doc_bits == 0 && tf_bits == 0 => {}
        _ => return Err("invalid posting encoding"),
    }
    let mut output = Vec::with_capacity(count);
    let mut position = 0;
    let mut ordinal = base;
    let mut actual_max_tf = 0u32;
    let mut wide = false;
    for index in 0..count {
        let (delta, tf) = if mode == 0 {
            (u64::from(deltas[index]), frequencies[index])
        } else {
            let delta = get_varint(payload, &mut position)?;
            let tf =
                u32::try_from(get_varint(payload, &mut position)?).map_err(|_| "tf overflow")?;
            (delta, tf)
        };
        if tf == 0 || (index == 0 && delta != 0) || (index > 0 && delta == 0) {
            return Err("invalid decoded posting");
        }
        wide |= delta > u64::from(u32::MAX);
        ordinal = ordinal.checked_add(delta).ok_or("ordinal overflow")?;
        actual_max_tf = actual_max_tf.max(tf);
        output.push(Posting { ordinal, tf });
    }
    if ordinal != last
        || (mode != 0 && position != payload.len())
        || (mode == 2 && !wide)
        || actual_max_tf.min(u32::from(u16::MAX)) as u16 != max_tf
    {
        return Err("inconsistent posting summary");
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn next(seed: &mut u64) -> u64 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed
    }

    fn scalar_wire(values: &[u32; BLOCK_LEN], width: u8) -> Vec<u8> {
        let mut bytes = vec![0u8; usize::from(width) * 16];
        for (index, value) in values.iter().enumerate() {
            for bit in 0..usize::from(width) {
                let lane_bit = (index / 4) * usize::from(width) + bit;
                let word = (lane_bit / 32) * 4 + index % 4;
                let byte = word * 4 + (lane_bit % 32) / 8;
                bytes[byte] |= (((value >> bit) & 1) as u8) << (lane_bit % 8);
            }
        }
        bytes
    }

    #[test]
    fn native_bitpacking_matches_independent_scalar_wire_for_every_width() {
        let mut seed = 206;
        for width in 0..=32 {
            let mask = if width == 32 {
                u32::MAX
            } else {
                (1u32 << width) - 1
            };
            for _ in 0..32 {
                let mut values = std::array::from_fn(|_| next(&mut seed) as u32 & mask);
                values[0] = mask;
                let expected = scalar_wire(&values, width);
                let mut actual = Vec::new();
                assert_eq!(pack(&values, &mut actual), width);
                assert_eq!(actual, expected);
                let mut output = [0u32; BLOCK_LEN];
                unpack(&expected, width, &mut output).unwrap();
                assert_eq!(output, values);
            }
        }
    }

    #[test]
    fn every_tail_size_and_full_block_roundtrip_with_u64_identity() {
        let mut seed = 206;
        for count in 1..=BLOCK_LEN {
            for _ in 0..64 {
                let mut ordinal = u64::from(u32::MAX) * 2 + next(&mut seed) % 10000;
                let input: Vec<_> = (0..count)
                    .map(|_| {
                        ordinal += 1 + next(&mut seed) % 4096;
                        Posting {
                            ordinal,
                            tf: 1 + (next(&mut seed) % 100000) as u32,
                        }
                    })
                    .collect();
                let bytes = encode(&input).unwrap();
                assert!(bytes.len() <= MAX_BLOCK_BYTES);
                assert_eq!(decode(&bytes).unwrap(), input);
            }
        }
    }

    #[test]
    fn wide_gaps_and_maximum_ordinals_are_not_truncated() {
        let input: Vec<_> = (0..128)
            .map(|index| Posting {
                ordinal: index * (u64::from(u32::MAX) + 9),
                tf: u32::MAX,
            })
            .collect();
        let bytes = encode(&input).unwrap();
        assert_eq!(bytes[8], 2);
        assert_eq!(decode(&bytes).unwrap(), input);
        let top: Vec<_> = (0..128)
            .map(|index| Posting {
                ordinal: u64::MAX - 127 + index,
                tf: 1,
            })
            .collect();
        let bytes = encode(&top).unwrap();
        assert_eq!(bytes[8], 0);
        assert_eq!(decode(&bytes).unwrap(), top);
    }

    #[test]
    fn max_tf_header_never_underestimates_and_zero_tf_is_rejected() {
        for tf in [1, 65534, 65535, 65536, u32::MAX] {
            let input = [Posting { ordinal: 0, tf }];
            let bytes = encode(&input).unwrap();
            let bound = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
            assert!(bound == u16::MAX || u32::from(bound) >= tf);
            assert_eq!(decode(&bytes).unwrap(), input);
        }
        assert!(encode(&[Posting { ordinal: 0, tf: 0 }]).is_err());
        assert!(encode(&[Posting { ordinal: 3, tf: 1 }, Posting { ordinal: 3, tf: 2 },]).is_err());
    }

    #[test]
    fn malformed_inputs_are_bounded_and_never_panic() {
        let input: Vec<_> = (0..128).map(|ordinal| Posting { ordinal, tf: 1 }).collect();
        let bytes = encode(&input).unwrap();
        for length in 0..bytes.len() {
            assert!(decode(&bytes[..length]).is_err());
        }
        for index in 0..bytes.len() {
            for mask in [1, 16, 128, 255] {
                let mut mutant = bytes.clone();
                mutant[index] ^= mask;
                let _ = decode(&mutant);
            }
        }
        let mut seed = 206;
        for iteration in 0..20000 {
            let length = iteration % (MAX_BLOCK_BYTES + 32);
            let input: Vec<_> = (0..length).map(|_| next(&mut seed) as u8).collect();
            let _ = decode(&input);
        }
        let mut overflow = encode(&[
            Posting {
                ordinal: u64::MAX - 1,
                tf: 1,
            },
            Posting {
                ordinal: u64::MAX,
                tf: 1,
            },
        ])
        .unwrap();
        overflow[HEADER_LEN + 2] = 2;
        assert_eq!(decode(&overflow), Err("ordinal overflow"));
        for input in [&[128, 0][..], &[255; 10][..], &[128; 11][..]] {
            assert!(get_varint(input, &mut 0).is_err());
        }
    }
}
