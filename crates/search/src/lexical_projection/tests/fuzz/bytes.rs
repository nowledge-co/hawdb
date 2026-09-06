use super::*;
use dictionary::{Dictionary, Limits, Metadata};
use posting_codec::{Posting as NumericPosting, BLOCK_LEN, MAX_BLOCK_BYTES};

fn postings(random: &mut Random, count: usize, wide: bool) -> Vec<NumericPosting> {
    let mut ordinal = random.next() % (u64::MAX / 2);
    (0..count)
        .map(|_| {
            ordinal += if wide {
                u64::from(u32::MAX) + 1 + random.next() % 4096
            } else {
                1 + random.next() % 4096
            };
            let tf = match random.index(5) {
                0 => 1,
                1 => u16::MAX as u32,
                2 => u32::MAX,
                _ => 1 + random.index(100_000) as u32,
            };
            NumericPosting { ordinal, tf }
        })
        .collect()
}

fn validate_postings(decoded: &[NumericPosting]) {
    assert!(!decoded.is_empty());
    assert!(decoded.iter().all(|posting| posting.tf > 0));
    assert!(decoded
        .windows(2)
        .all(|pair| pair[0].ordinal < pair[1].ordinal));
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn validate_frame_summary(bytes: &[u8], decoded: &[NumericPosting]) {
    assert_eq!(&bytes[..4], b"LXP1");
    assert_eq!(
        u16::from_le_bytes(bytes[4..6].try_into().unwrap()) as usize,
        decoded.len()
    );
    assert_eq!(u64_at(bytes, 12), decoded[0].ordinal);
    assert_eq!(u64_at(bytes, 20), decoded.last().unwrap().ordinal);
    let max_tf = decoded
        .iter()
        .map(|posting| posting.tf)
        .max()
        .unwrap()
        .min(u16::MAX as u32) as u16;
    assert_eq!(u16::from_le_bytes(bytes[6..8].try_into().unwrap()), max_tf);
    assert_eq!(
        u32::from_le_bytes(bytes[28..32].try_into().unwrap()) as usize,
        bytes.len() - 32
    );
}

#[test]
#[ignore = "local production-codec campaign; run the explicit Bazel fuzz suite"]
fn posting_bytes_campaign() {
    let mut random = Random(206);
    let mut outcomes = Outcomes::default();
    let mut modes = [0; 3];
    for case in 0..100_000 {
        let count = if case % 3 == 0 {
            BLOCK_LEN
        } else {
            1 + random.index(BLOCK_LEN)
        };
        let expected = postings(&mut random, count, case % 5 == 0);
        let original = posting_codec::encode(&expected).unwrap();
        modes[original[8] as usize] += 1;
        assert_eq!(
            posting_codec::decode(&original).unwrap(),
            expected,
            "case={case}"
        );
        let bytes = random.mutate(&original, case);
        match posting_codec::decode(&bytes) {
            Ok(decoded) => {
                assert!(decoded.len() <= BLOCK_LEN);
                validate_postings(&decoded);
                validate_frame_summary(&bytes, &decoded);
                let encoded = posting_codec::encode(&decoded).unwrap();
                assert!(encoded.len() <= MAX_BLOCK_BYTES);
                assert_eq!(
                    posting_codec::decode(&encoded).unwrap(),
                    decoded,
                    "case={case}"
                );
                outcomes.record(true);
            }
            Err(_) => outcomes.record(false),
        }
    }
    assert!(modes.iter().all(|&count| count > 100));
    outcomes.finish("posting seed=206", 100_000);
    eprintln!("posting valid mode counts={modes:?}");
}

fn dictionary_limits() -> Limits {
    Limits {
        max_bytes: 16 * 1024,
        max_terms: 128,
        max_key_bytes: 128,
        max_builder_bytes: 8 * 1024 * 1024,
        max_validation_bytes: 512 * 1024,
    }
}

fn resign_dictionary(bytes: &mut [u8]) {
    if bytes.len() < 24 + 36 + 4 {
        return;
    }
    let end = bytes.len() - 4;
    let records = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as u64;
    let prefix = 24 + records * 32;
    let fst_bytes = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as u64;
    if fst_bytes >= 36 && prefix + fst_bytes == end as u64 {
        let crc = skein_integrity::crc32c(&bytes[prefix as usize..end - 4])
            .get()
            .rotate_right(15)
            .wrapping_add(0xa282_ead8);
        bytes[end - 4..end].copy_from_slice(&crc.to_le_bytes());
    }
    let crc = skein_integrity::crc32c(&bytes[..end]).get();
    bytes[end..].copy_from_slice(&crc.to_le_bytes());
}

fn inspect_dictionary(bytes: &[u8], limits: Limits) -> bool {
    let mut checkpoints = 0;
    let dictionary = Dictionary::open(bytes, limits, &mut || {
        checkpoints += 1;
        // A valid block is bounded by bytes, keys and key depth. This guard
        // asserts termination instead of turning excessive work into success.
        assert!(checkpoints <= 1_000_000);
        Ok(())
    });
    let Ok(dictionary) = dictionary else {
        return false;
    };
    let mut entries = Vec::new();
    dictionary
        .visit(|term, metadata| {
            assert!(!term.is_empty() && term.len() <= limits.max_key_bytes as usize);
            assert!(entries
                .last()
                .is_none_or(|(previous, _): &(String, Metadata)| previous.as_str() < term));
            assert_eq!(dictionary.get(term).unwrap(), Some(metadata));
            assert!(metadata.df > 0 && metadata.posting_bytes > 0);
            assert!(metadata
                .posting_offset
                .checked_add(metadata.posting_bytes)
                .is_some());
            assert!(metadata.skip_offset < metadata.posting_bytes);
            let start = 24 + entries.len() * 32;
            assert_eq!(
                metadata,
                Metadata {
                    df: u64_at(bytes, start),
                    posting_offset: u64_at(bytes, start + 8),
                    posting_bytes: u64_at(bytes, start + 16),
                    skip_offset: u64_at(bytes, start + 24),
                }
            );
            entries.push((term.to_owned(), metadata));
            assert!(entries.len() <= limits.max_terms as usize);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        entries.len(),
        u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize
    );
    // Mutated but valid dictionaries may describe different keys and metadata.
    // Compare against their own decoded mapping, never the unmutated corpus.
    let rebuilt = dictionary::build(&entries, limits, &mut || Ok(())).unwrap();
    let reopened = Dictionary::open(&rebuilt, limits, &mut || Ok(())).unwrap();
    for (term, metadata) in entries {
        assert_eq!(reopened.get(&term).unwrap(), Some(metadata));
    }
    true
}

#[test]
#[ignore = "local production-dictionary campaign; run the explicit Bazel fuzz suite"]
fn dictionary_bytes_campaign() {
    let mut random = Random(0x206_f57);
    let limits = dictionary_limits();
    let mut outcomes = Outcomes::default();
    for fixture in 0..32 {
        let count = 1 + random.index(96);
        let entries = (0..count)
            .map(|index| {
                let suffix =
                    ["ascii", "\u{4e2d}\u{6587}", "\u{1f600}", "\0"][(fixture + index) % 4];
                let term = format!(
                    "prefix{fixture:02}-{index:03}-{:016x}-{suffix}",
                    random.next()
                );
                let metadata = Metadata {
                    df: 1 + random.next() % (u64::MAX - 1),
                    posting_offset: random.next() % (u64::MAX / 2),
                    posting_bytes: 256,
                    skip_offset: if index % 2 == 0 { 0 } else { 128 },
                };
                (term, metadata)
            })
            .collect::<Vec<_>>();
        let original = dictionary::build(&entries, limits, &mut || Ok(())).unwrap();
        let valid = Dictionary::open(&original, limits, &mut || Ok(())).unwrap();
        for (term, metadata) in &entries {
            assert_eq!(valid.get(term).unwrap(), Some(*metadata));
        }
        for case in 0..512 {
            let mut bytes = random.mutate(&original, case);
            if case % 2 == 0 {
                resign_dictionary(&mut bytes);
            }
            outcomes.record(inspect_dictionary(&bytes, limits));
        }
    }
    outcomes.finish("dictionary seed=0x206f57", 32 * 512);
}

fn decode_doclist(bytes: &[u8], metadata: Metadata) -> Result<Vec<NumericPosting>> {
    let mut cursor = doclist::Cursor::new(metadata)?;
    let mut output = Vec::new();
    let mut reads = 0;
    while let Some(frame) = cursor.next_frame(&mut |offset, length, _digest| {
        reads += 1;
        assert!(reads <= bytes.len().saturating_mul(4) + 8);
        assert!(length <= MAX_BLOCK_BYTES);
        let start = offset
            .checked_sub(metadata.posting_offset)
            .and_then(|offset| usize::try_from(offset).ok());
        start
            .and_then(|start| {
                start
                    .checked_add(length)
                    .and_then(|end| bytes.get(start..end))
            })
            .map(Arc::<[u8]>::from)
            .ok_or_else(|| SkeinError::Storage("fuzz input range is unavailable".to_string()))
    })? {
        validate_postings(&frame);
        output.extend(frame);
        assert!(output.len() <= bytes.len().saturating_mul(BLOCK_LEN));
    }
    assert_eq!(output.len() as u64, metadata.df);
    validate_postings(&output);
    // Independently inspect the physical frame summaries and skip table. A
    // decoder that silently skips integrity checks must not pass by roundtrip.
    let mut position = 0;
    for (index, frame) in output.chunks(BLOCK_LEN).enumerate() {
        let length = u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap()) as usize;
        let payload = &bytes[position + 12..position + 12 + length];
        assert_eq!(u64_at(bytes, position + 4), checksum(payload));
        validate_frame_summary(payload, frame);
        if metadata.skip_offset > 0 {
            let skip = metadata.skip_offset as usize + 12 + index * 16;
            assert_eq!(u64_at(bytes, skip), frame.last().unwrap().ordinal);
            assert_eq!(u64_at(bytes, skip + 8), position as u64);
        }
        position += 12 + length;
    }
    if metadata.skip_offset == 0 {
        assert_eq!(position as u64, metadata.posting_bytes);
        assert!(output.len() <= BLOCK_LEN);
    } else {
        assert_eq!(position as u64, metadata.skip_offset);
        assert_eq!(&bytes[position..position + 4], b"LXS1");
        let frames = output.len().div_ceil(BLOCK_LEN);
        assert_eq!(u64_at(bytes, position + 4), frames as u64);
        assert_eq!(
            metadata.posting_bytes,
            metadata.skip_offset + 16 + frames as u64 * 16
        );
        let last = metadata.posting_bytes as usize - 4;
        let crc = u32::from_le_bytes(bytes[last..last + 4].try_into().unwrap());
        assert_eq!(crc, skein_integrity::crc32c(&bytes[position..last]).get());
    }
    assert!(cursor
        .next_frame(&mut |_, _, _| panic!("exhausted cursor must not read"))
        .unwrap()
        .is_none());
    Ok(output)
}

fn resign_doclist(bytes: &mut [u8], metadata: Metadata) {
    let data_end = if metadata.skip_offset == 0 {
        metadata.posting_bytes
    } else {
        metadata.skip_offset
    };
    let Ok(end) = usize::try_from(data_end) else {
        return;
    };
    if end > bytes.len() {
        return;
    }
    let mut position = 0;
    while end - position >= 12 {
        let length = u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap()) as usize;
        let Some(next) = (position + 12)
            .checked_add(length)
            .filter(|&next| next <= end)
        else {
            break;
        };
        let crc = checksum(&bytes[position + 12..next]);
        bytes[position + 4..position + 12].copy_from_slice(&crc.to_le_bytes());
        position = next;
    }
    if metadata.skip_offset > 0 && bytes.len() >= end + 4 {
        let last = bytes.len() - 4;
        let crc = skein_integrity::crc32c(&bytes[end..last]).get();
        bytes[last..].copy_from_slice(&crc.to_le_bytes());
    }
}

#[test]
#[ignore = "local production-doclist campaign; run the explicit Bazel fuzz suite"]
fn doclist_bytes_campaign() {
    let root = temporary_root("doclist");
    let mut random = Random(0x206_d0c);
    let mut outcomes = Outcomes::default();
    let mut writer = doclist::Writer::new(
        &root.join("skip.tmp"),
        dictionary_store::SpillBudget::new(0, 1024 * 1024),
        MAX_BLOCK_BYTES as u64,
    )
    .unwrap();
    for count in [1, 127, 128, 129, 256, 257, 513] {
        let expected = postings(&mut random, count, count % 2 == 0);
        let mut original = Vec::new();
        // Exercise physical offsets above 32 bits without allocating a sparse file.
        let mut offset = u64::from(u32::MAX) + 99;
        for frame in expected.chunks(BLOCK_LEN) {
            writer
                .push_frame(&mut original, &mut offset, frame)
                .unwrap();
        }
        let metadata = writer.finish(&mut original, &mut offset).unwrap();
        assert_eq!(decode_doclist(&original, metadata).unwrap(), expected);
        for case in 0..2048 {
            let mut bytes = random.mutate(&original, case);
            let mut input_metadata = metadata;
            if case % 2 == 0 {
                resign_doclist(&mut bytes, metadata);
            }
            if case % 11 == 0 {
                let value = [0, 1, 128, 129, u64::MAX][random.index(5)];
                match random.index(4) {
                    0 => input_metadata.df = value,
                    1 => input_metadata.posting_offset = value,
                    2 => input_metadata.posting_bytes = value,
                    _ => input_metadata.skip_offset = value,
                }
            }
            match decode_doclist(&bytes, input_metadata) {
                Ok(decoded) => {
                    let mut encoded = Vec::new();
                    let mut offset = 0;
                    for frame in decoded.chunks(BLOCK_LEN) {
                        writer.push_frame(&mut encoded, &mut offset, frame).unwrap();
                    }
                    let rebuilt = writer.finish(&mut encoded, &mut offset).unwrap();
                    assert_eq!(decode_doclist(&encoded, rebuilt).unwrap(), decoded);
                    outcomes.record(true);
                }
                Err(_) => outcomes.record(false),
            }
        }
    }
    outcomes.finish("doclist seed=0x206d0c", 7 * 2048);
    drop(writer);
    assert!(!root.join("skip.tmp").exists());
    fs::remove_dir(root).unwrap();
}
