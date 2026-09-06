//! Bounded preflight for the pinned fst 0.4.7 v3 wire format.
//!
//! Raw nodes are exposed to fst only after their ranges and every encoded
//! backward address have been checked without calling its node decoder.

use std::mem::size_of;

type Result<T> = std::result::Result<T, &'static str>;

#[derive(Clone, Copy)]
pub(super) struct Limits {
    pub max_bytes: usize,
    pub max_scratch_bytes: usize,
    pub max_nodes: usize,
    pub max_keys: u64,
    pub max_key_bytes: u32,
    pub max_value: u64,
}

#[derive(Clone, Copy, Default)]
struct Summary {
    max_output: u64,
    keys: u64,
    depth_plus_one: u32,
}

#[derive(Clone, Copy)]
struct Layout {
    start: usize,
    count: usize,
    delta_end: usize,
    delta_width: usize,
    index_start: Option<usize>,
    next: bool,
}

fn uint(bytes: &[u8], offset: usize, width: usize) -> Result<u64> {
    if !(1..=8).contains(&width) {
        return Err("invalid integer width");
    }
    let end = offset.checked_add(width).ok_or("integer range overflow")?;
    let input = bytes.get(offset..end).ok_or("truncated integer")?;
    let mut buffer = [0u8; 8];
    buffer[..width].copy_from_slice(input);
    Ok(u64::from_le_bytes(buffer))
}

fn back(position: usize, length: usize) -> Result<usize> {
    position
        .checked_sub(length)
        .filter(|&at| at >= 16)
        .ok_or("node overlaps header")
}

fn layout(bytes: &[u8], address: usize) -> Result<Layout> {
    let state = *bytes.get(address).ok_or("invalid node address")?;
    let kind = state >> 6;
    if kind >= 2 {
        let input_length = usize::from(state & 63 == 0);
        let after_input = back(address, input_length)?;
        if kind == 3 {
            return Ok(Layout {
                start: after_input,
                count: 1,
                delta_end: after_input,
                delta_width: 0,
                index_start: None,
                next: true,
            });
        }
        let sizes_at = back(after_input, 1)?;
        let sizes = bytes[sizes_at];
        let delta_width = usize::from(sizes >> 4);
        let output_width = usize::from(sizes & 15);
        if !(1..=8).contains(&delta_width) || output_width > 8 {
            return Err("invalid single-transition widths");
        }
        let start = back(sizes_at, delta_width + output_width)?;
        return Ok(Layout {
            start,
            count: 1,
            delta_end: sizes_at,
            delta_width,
            index_start: None,
            next: false,
        });
    }
    let inline_count = usize::from(state & 63);
    let count_length = usize::from(inline_count == 0);
    let count = if inline_count == 0 {
        let value = usize::from(bytes[back(address, 1)?]);
        if value == 1 {
            256
        } else {
            value
        }
    } else {
        inline_count
    };
    let sizes_at = back(address, count_length + 1)?;
    let sizes = bytes[sizes_at];
    let delta_width = usize::from(sizes >> 4);
    let output_width = usize::from(sizes & 15);
    if delta_width > 8 || output_width > 8 || (count != 0 && delta_width == 0) {
        return Err("invalid transition widths");
    }
    let index_length = if count > 32 { 256 } else { 0 };
    let index_at = back(sizes_at, index_length)?;
    let delta_end = back(index_at, count)?;
    let final_width = if state & 64 != 0 { output_width } else { 0 };
    let start = back(
        delta_end,
        count * (delta_width + output_width) + final_width,
    )?;
    Ok(Layout {
        start,
        count,
        delta_end,
        delta_width,
        index_start: (index_length != 0).then_some(index_at),
        next: false,
    })
}

impl Layout {
    fn child(self, bytes: &[u8], index: usize) -> Result<usize> {
        if index >= self.count {
            return Err("transition index out of bounds");
        }
        if self.next {
            return back(self.start, 1);
        }
        let offset = back(self.delta_end, (index + 1) * self.delta_width)?;
        let delta = usize::try_from(uint(bytes, offset, self.delta_width)?)
            .map_err(|_| "address overflow")?;
        if delta == 0 {
            return Ok(0);
        }
        back(self.start, delta)
    }
}

pub(super) fn scratch_bytes(bytes: usize, max_nodes: usize) -> Result<usize> {
    bytes
        .checked_mul(size_of::<Summary>())
        .and_then(|size| {
            bytes
                .min(max_nodes)
                .checked_mul(size_of::<u32>())
                .and_then(|addresses| size.checked_add(addresses))
        })
        .ok_or("FST scratch bound overflows")
}

pub(super) fn open_checked<'a>(
    bytes: &'a [u8],
    limits: Limits,
    checkpoint: &mut impl FnMut() -> Result<()>,
) -> Result<fst::Map<&'a [u8]>> {
    checkpoint()?;
    if bytes.len() < 36
        || bytes.len() > limits.max_bytes
        || bytes.len() > u32::MAX as usize
        || uint(bytes, 0, 8)? != 3
        || uint(bytes, 8, 8)? != 0
    {
        return Err("invalid FST envelope");
    }
    let keys = uint(bytes, bytes.len() - 20, 8)?;
    let root =
        usize::try_from(uint(bytes, bytes.len() - 12, 8)?).map_err(|_| "root address overflow")?;
    if keys > limits.max_keys
        || usize::try_from(keys).is_err()
        || (root == 0 && (bytes.len() != 36 || keys != 1))
        || (root != 0 && (root < 16 || root.checked_add(21) != Some(bytes.len())))
    {
        return Err("invalid FST footer");
    }
    // The constructor's limited header checks cannot panic after this preflight.
    let map = fst::Map::new(bytes).map_err(|_| "invalid FST header")?;
    map.as_fst().verify().map_err(|_| "invalid FST checksum")?;
    if root == 0 {
        return Ok(map);
    }
    let node_limit = limits.max_nodes.min(bytes.len());
    let scratch = scratch_bytes(bytes.len(), node_limit)?;
    if scratch > limits.max_scratch_bytes {
        return Err("FST scratch budget exceeded");
    }
    let mut summaries = Vec::new();
    summaries
        .try_reserve_exact(bytes.len())
        .map_err(|_| "FST scratch allocation failed")?;
    summaries.resize(bytes.len(), Summary::default());
    summaries[0] = Summary {
        max_output: 0,
        keys: 1,
        depth_plus_one: 1,
    };
    let mut addresses = Vec::new();
    addresses
        .try_reserve_exact(node_limit)
        .map_err(|_| "FST node allocation failed")?;
    let mut address = root;
    loop {
        checkpoint()?;
        if addresses.len() == node_limit {
            return Err("FST node budget exceeded");
        }
        let node = layout(bytes, address)?;
        addresses.push(address as u32);
        // This tag records a boundary; children are computed before parents.
        summaries[address].depth_plus_one = 1;
        if node.start == 16 {
            break;
        }
        address = node.start - 1;
    }
    let max_depth = limits
        .max_key_bytes
        .checked_add(1)
        .ok_or("FST depth bound overflows")?;
    for &address in addresses.iter().rev() {
        checkpoint()?;
        let address = address as usize;
        let parsed = layout(bytes, address)?;
        // Validate every backward address before fst performs unchecked subtraction.
        for index in 0..parsed.count {
            let child = parsed.child(bytes, index)?;
            if child >= parsed.start || summaries[child].depth_plus_one == 0 {
                return Err("transition does not reference an earlier node");
            }
        }
        let node = map.as_fst().node(address);
        let mut summary = Summary {
            max_output: node.final_output().value(),
            keys: u64::from(node.is_final()),
            depth_plus_one: 1,
        };
        let mut previous = None;
        let mut expected_index = [255u8; 256];
        for index in 0..parsed.count {
            let transition = node.transition(index);
            if transition.addr != parsed.child(bytes, index)? {
                return Err("FST decoder disagrees with validated address");
            }
            if previous.is_some_and(|old| old >= transition.inp) {
                return Err("FST inputs are not strictly ordered");
            }
            previous = Some(transition.inp);
            expected_index[usize::from(transition.inp)] = index as u8;
            let child = summaries[transition.addr];
            summary.keys = summary
                .keys
                .checked_add(child.keys)
                .ok_or("FST key count overflow")?;
            summary.max_output = summary.max_output.max(
                transition
                    .out
                    .value()
                    .checked_add(child.max_output)
                    .ok_or("FST output overflow")?,
            );
            summary.depth_plus_one = summary.depth_plus_one.max(
                child
                    .depth_plus_one
                    .checked_add(1)
                    .ok_or("FST path length overflow")?,
            );
        }
        if let Some(start) = parsed.index_start
            && bytes.get(start..start + 256) != Some(expected_index.as_slice())
        {
            return Err("FST transition index disagrees with inputs");
        }
        if summary.keys > limits.max_keys
            || summary.max_output > limits.max_value
            || summary.depth_plus_one > max_depth
        {
            return Err("FST semantic budget exceeded");
        }
        summaries[address] = summary;
    }
    if summaries[root].keys != keys {
        return Err("FST declared key count disagrees with graph");
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fst::Streamer;
    use std::collections::{BTreeMap, BTreeSet};

    fn limits() -> Limits {
        Limits {
            max_bytes: 65536,
            max_scratch_bytes: 2 * 1024 * 1024,
            max_nodes: 65536,
            max_keys: 10000,
            max_key_bytes: 256,
            max_value: u64::MAX,
        }
    }

    fn build(entries: &BTreeMap<Vec<u8>, u64>) -> Vec<u8> {
        let mut builder = fst::MapBuilder::memory();
        for (key, value) in entries {
            builder.insert(key, *value).unwrap();
        }
        builder.into_inner().unwrap()
    }

    fn fixture() -> Vec<u8> {
        let mut entries = BTreeMap::new();
        entries.insert(Vec::new(), 3);
        for input in 0..=255 {
            entries.insert(vec![input], u64::from(input) * 1001);
            entries.insert(vec![input, b'a', b'b'], u64::MAX - u64::from(input));
        }
        build(&entries)
    }

    fn resign(bytes: &mut [u8]) {
        let end = bytes.len() - 4;
        let masked = skein_integrity::crc32c(&bytes[..end])
            .get()
            .rotate_right(15)
            .wrapping_add(0xa282_ead8);
        bytes[end..].copy_from_slice(&masked.to_le_bytes());
    }

    fn nodes(bytes: &[u8]) -> Vec<(usize, Layout)> {
        let mut address = uint(bytes, bytes.len() - 12, 8).unwrap() as usize;
        let mut output = Vec::new();
        while address != 0 {
            let parsed = layout(bytes, address).unwrap();
            output.push((address, parsed));
            if parsed.start == 16 {
                break;
            }
            address = parsed.start - 1;
        }
        output
    }

    fn checked_error(bytes: &[u8], limits: Limits) -> &'static str {
        open_checked(bytes, limits, &mut || Ok(())).unwrap_err()
    }

    fn inspect_accepted(map: &fst::Map<&[u8]>) {
        let mut stream = map.stream();
        let mut previous: Option<Vec<u8>> = None;
        let mut count = 0u64;
        while let Some((key, value)) = stream.next() {
            assert!(previous.as_ref().is_none_or(|old| old.as_slice() < key));
            assert_eq!(map.get(key), Some(value));
            assert!(key.len() <= 256);
            previous = Some(key.to_vec());
            count += 1;
            assert!(count <= 10000);
        }
        assert_eq!(count, map.len() as u64);
        for input in 0..=255 {
            let _ = map.get([input]);
            let _ = map.get([input, input, input]);
        }
    }

    #[test]
    fn valid_maps_cover_empty_prefixes_binary_inputs_and_all_node_forms() {
        let mut cases = vec![BTreeMap::new()];
        for value in [0, 1, 255, 256, 1 << 32, u64::MAX] {
            cases.push(BTreeMap::from([(Vec::new(), value)]));
            cases.push(BTreeMap::from([(b"abcdef".to_vec(), value)]));
            cases.push(BTreeMap::from([
                (b"a".to_vec(), value),
                (b"ab".to_vec(), 1),
                (b"abc".to_vec(), 300),
                (b"zbc".to_vec(), 700),
            ]));
        }
        for count in [2, 31, 32, 33, 63, 64, 255, 256] {
            cases.push(
                (0..count)
                    .map(|key| (vec![key as u8], key as u64))
                    .collect(),
            );
        }
        let mut states = BTreeSet::new();
        for entries in cases {
            let bytes = build(&entries);
            let map = open_checked(&bytes, limits(), &mut || Ok(())).unwrap();
            for (address, parsed) in nodes(&bytes) {
                let node = map.as_fst().node(address);
                states.insert(node.state());
                assert_eq!(node.len(), parsed.count);
                assert_eq!(node.as_slice().len(), address - parsed.start + 1);
                for index in 0..parsed.count {
                    assert_eq!(
                        node.transition_addr(index),
                        parsed.child(&bytes, index).unwrap()
                    );
                }
            }
            for (key, value) in entries {
                assert_eq!(map.get(key), Some(value));
            }
            inspect_accepted(&map);
        }
        assert_eq!(states.len(), 3, "serialized node variants: {states:?}");
    }

    #[test]
    fn valid_random_maps_match_independent_ordered_dictionary() {
        let mut seed = 206u64;
        for _ in 0..128 {
            let mut entries = BTreeMap::new();
            for _ in 0..128 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key = seed.to_le_bytes()[..1 + seed as usize % 8].to_vec();
                entries.insert(key, seed);
            }
            let bytes = build(&entries);
            let map = open_checked(&bytes, limits(), &mut || Ok(())).unwrap();
            assert_eq!(map.len(), entries.len());
            for (key, value) in entries {
                assert_eq!(map.get(key), Some(value));
            }
            inspect_accepted(&map);
        }
    }

    #[test]
    fn rejects_bad_envelopes_checksums_and_false_key_counts() {
        let bytes = fixture();
        for length in 0..bytes.len() {
            assert!(open_checked(&bytes[..length], limits(), &mut || Ok(())).is_err());
        }
        for (offset, value) in [(0, 4), (8, 1), (bytes.len() - 12, u64::MAX)] {
            let mut damaged = bytes.clone();
            damaged[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            resign(&mut damaged);
            assert!(open_checked(&damaged, limits(), &mut || Ok(())).is_err());
        }
        let mut damaged = bytes.clone();
        damaged[16] ^= 1;
        assert_eq!(checked_error(&damaged, limits()), "invalid FST checksum");
        let offset = damaged.len() - 20;
        damaged = bytes.clone();
        damaged[offset..offset + 8].copy_from_slice(&514u64.to_le_bytes());
        resign(&mut damaged);
        assert_eq!(
            checked_error(&damaged, limits()),
            "FST declared key count disagrees with graph"
        );
        let mut singleton = build(&BTreeMap::from([(Vec::new(), 0)]));
        singleton[16..24].copy_from_slice(&0u64.to_le_bytes());
        resign(&mut singleton);
        assert_eq!(checked_error(&singleton, limits()), "invalid FST footer");
    }

    #[test]
    fn recomputed_checksums_do_not_hide_bad_widths_or_backward_addresses() {
        let bytes = fixture();
        let mut width_cases = 0;
        let mut address_cases = 0;
        for (address, parsed) in nodes(&bytes) {
            if parsed.next {
                continue;
            }
            let sizes_at = address - usize::from(bytes[address] & 63 == 0) - 1;
            for width in [0xf0, 0x0f, 0xff] {
                let mut damaged = bytes.clone();
                damaged[sizes_at] = width;
                resign(&mut damaged);
                fst::Map::new(damaged.as_slice())
                    .unwrap()
                    .as_fst()
                    .verify()
                    .unwrap();
                assert!(open_checked(&damaged, limits(), &mut || Ok(())).is_err());
                width_cases += 1;
            }
            if parsed.count != 0 {
                let mut damaged = bytes.clone();
                damaged[parsed.delta_end - parsed.delta_width..parsed.delta_end].fill(255);
                resign(&mut damaged);
                assert!(open_checked(&damaged, limits(), &mut || Ok(())).is_err());
                address_cases += 1;
            }
        }
        assert!(width_cases > 100);
        assert!(address_cases > 100);
    }

    #[test]
    fn rejects_duplicate_inputs_and_corrupt_acceleration_table() {
        let bytes = fixture();
        let (_, parsed) = nodes(&bytes)
            .into_iter()
            .find(|(_, node)| node.count == 256)
            .unwrap();
        let mut damaged = bytes.clone();
        damaged[parsed.delta_end] = damaged[parsed.delta_end + 1];
        resign(&mut damaged);
        assert_eq!(
            checked_error(&damaged, limits()),
            "FST inputs are not strictly ordered"
        );
        for index in 0..256 {
            let mut damaged = bytes.clone();
            damaged[parsed.index_start.unwrap() + index] ^= 1;
            resign(&mut damaged);
            assert_eq!(
                checked_error(&damaged, limits()),
                "FST transition index disagrees with inputs"
            );
        }
    }

    fn finish_raw(mut bytes: Vec<u8>, keys: u64) -> Vec<u8> {
        let root = bytes.len() as u64 - 1;
        bytes.extend_from_slice(&keys.to_le_bytes());
        bytes.extend_from_slice(&root.to_le_bytes());
        bytes.extend_from_slice(&[0; 4]);
        resign(&mut bytes);
        bytes
    }

    #[test]
    fn rejects_exponential_key_graph_before_enumeration() {
        let mut bytes = [3u64.to_le_bytes(), 0u64.to_le_bytes()].concat();
        for depth in 0..64 {
            // Every node branches twice to the preceding suffix. Six bytes of
            // storage double the key count without storing any individual key.
            let delta = u8::from(depth != 0);
            bytes.extend_from_slice(&[delta, delta, 1, 0, 0x10, 2]);
        }
        let bytes = finish_raw(bytes, 0);
        let error = checked_error(
            &bytes,
            Limits {
                max_keys: u64::MAX,
                ..limits()
            },
        );
        assert_eq!(error, "FST key count overflow");
        assert_eq!(
            checked_error(&bytes, limits()),
            "FST semantic budget exceeded"
        );
    }

    #[test]
    fn rejects_path_output_overflow_before_lookup() {
        let mut bytes = [3u64.to_le_bytes(), 0u64.to_le_bytes()].concat();
        bytes.extend_from_slice(&u64::MAX.to_le_bytes());
        bytes.extend_from_slice(&[8, 0, 0x40]);
        // An edge output of one plus the final suffix output overflows u64.
        bytes.extend_from_slice(&[1, 1, 0x11, b'x', 0x80]);
        let bytes = finish_raw(bytes, 1);
        assert_eq!(checked_error(&bytes, limits()), "FST output overflow");
    }

    #[test]
    fn every_valid_encoded_integer_width_is_checked_before_library_decoding() {
        for delta_width in 1..=8 {
            for output_width in 0..=8 {
                let value = if output_width == 0 {
                    0
                } else {
                    1u64 << ((output_width - 1) * 8)
                };
                let packed = value.to_le_bytes();
                let sizes = ((delta_width << 4) | output_width) as u8;
                let mut single = [3u64.to_le_bytes(), 0u64.to_le_bytes()].concat();
                single.extend_from_slice(&packed[..output_width]);
                single.extend_from_slice(&[0; 8][..delta_width]);
                single.extend_from_slice(&[sizes, b'x', 0x80]);
                let single = finish_raw(single, 1);
                assert_eq!(
                    open_checked(&single, limits(), &mut || Ok(()))
                        .unwrap()
                        .get("x"),
                    Some(value)
                );

                let mut multiple = [3u64.to_le_bytes(), 0u64.to_le_bytes()].concat();
                for _ in 0..2 {
                    multiple.extend_from_slice(&packed[..output_width]);
                }
                for _ in 0..2 {
                    multiple.extend_from_slice(&[0; 8][..delta_width]);
                }
                multiple.extend_from_slice(&[b'b', b'a', sizes, 2]);
                let multiple = finish_raw(multiple, 2);
                let map = open_checked(&multiple, limits(), &mut || Ok(())).unwrap();
                assert_eq!(map.get("a"), Some(value));
                assert_eq!(map.get("b"), Some(value));
            }
        }
    }

    #[test]
    fn byte_node_key_depth_output_scratch_and_cancellation_limits_fail_closed() {
        let bytes = fixture();
        for constrained in [
            Limits {
                max_bytes: bytes.len() - 1,
                ..limits()
            },
            Limits {
                max_nodes: 0,
                ..limits()
            },
            Limits {
                max_keys: 512,
                ..limits()
            },
            Limits {
                max_key_bytes: 2,
                ..limits()
            },
            Limits {
                max_value: u64::MAX - 1,
                ..limits()
            },
            Limits {
                max_scratch_bytes: 0,
                ..limits()
            },
        ] {
            assert!(open_checked(&bytes, constrained, &mut || Ok(())).is_err());
        }
        for stop in [0, 1, 2, 5, 100] {
            let mut calls = 0;
            let error = open_checked(&bytes, limits(), &mut || {
                calls += 1;
                if calls > stop {
                    Err("cancelled")
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
            assert_eq!(error, "cancelled");
            assert_eq!(calls, stop + 1);
        }
    }

    #[test]
    fn recomputed_checksum_mutations_never_expose_panicking_or_unbounded_traversal() {
        let bytes = fixture();
        let mut accepted = 0;
        let mut rejected = 0;
        for index in 16..bytes.len() - 4 {
            for mask in [1, 16, 128, 255] {
                let mut damaged = bytes.clone();
                damaged[index] ^= mask;
                resign(&mut damaged);
                match open_checked(&damaged, limits(), &mut || Ok(())) {
                    Ok(map) => {
                        inspect_accepted(&map);
                        accepted += 1;
                    }
                    Err(_) => rejected += 1,
                }
            }
        }
        assert!(accepted > 100);
        assert!(rejected > 100);
    }
}
