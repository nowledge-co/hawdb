// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::SearchDocument;
use std::collections::{BTreeMap, BTreeSet};

type Bits = Vec<(String, Vec<u32>)>;

fn segment(first: &str, last: &str, count: usize) -> SearchSegmentDescriptorEntry {
    let document = |id: &str| SearchDocument {
        id: id.into(),
        title: String::new(),
        content: String::new(),
        embedding: None,
        metadata: BTreeMap::new(),
    };
    let (first, last) = (document(first), document(last));
    let mut segment =
        SearchSegmentDescriptorEntry::from_documents(3, &[&first, &last], &BTreeSet::new());
    segment.document_count = count;
    segment
}

fn rows(count: usize, dimension: usize) -> Vec<(String, Vec<f32>)> {
    let specials = [
        0.0,
        -0.0,
        f32::MIN,
        f32::MAX,
        f32::MIN_POSITIVE,
        f32::from_bits(1),
        -f32::from_bits(1),
        0.1,
        1.0 / 3.0,
        -1e-30,
    ];
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    (0..count)
        .map(|row| {
            let embedding = (0..dimension)
                .map(|column| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    if (row + column).is_multiple_of(3) {
                        specials[(row * 7 + column) % specials.len()]
                    } else {
                        (state >> 40) as f32 / (1u64 << 23) as f32 - 1.0
                    }
                })
                .collect();
            // Multi-byte IDs exercise the offset table and UTF-8 validation.
            (format!("doc:{row:04}:é"), embedding)
        })
        .collect()
}

fn encode(rows: &[(String, Vec<f32>)]) -> Vec<u8> {
    let borrowed = rows
        .iter()
        .map(|(id, embedding)| (id.as_str(), embedding.as_slice()))
        .collect::<Vec<_>>();
    let mut bytes = Vec::new();
    encode_into(&borrowed, &mut bytes, || Ok(())).unwrap();
    let id_bytes = rows.iter().map(|(id, _)| id.len()).sum();
    let dimension = rows.first().map_or(0, |(_, embedding)| embedding.len());
    assert_eq!(
        bytes.len(),
        encoded_len(rows.len(), id_bytes, dimension).unwrap()
    );
    bytes
}

fn expected<'a>(
    segment: &'a SearchSegmentDescriptorEntry,
    bytes: &[u8],
    count: usize,
    dimension: usize,
) -> Expected<'a> {
    Expected {
        segment,
        payload_len: bytes.len() as u64,
        count,
        vector_ordinal_base: 40,
        dimension: Some(dimension),
    }
}

fn visit(bytes: &[u8], expected: Expected<'_>, selected: impl Fn(u64) -> bool) -> Result<Bits> {
    let mut output = Vec::new();
    visit_selected(&mut SliceSource(bytes), expected, selected, |document| {
        output.push((
            document.id.clone(),
            document
                .embedding
                .iter()
                .map(|value| value.to_bits())
                .collect(),
        ));
        Ok(())
    })?;
    Ok(output)
}

fn reference(rows: &[(String, Vec<f32>)], selected: impl Fn(u64) -> bool) -> Bits {
    rows.iter()
        .enumerate()
        .filter(|(index, _)| selected(40 + *index as u64))
        .map(|(_, (id, embedding))| {
            (
                id.clone(),
                embedding.iter().map(|value| value.to_bits()).collect(),
            )
        })
        .collect()
}

#[test]
fn every_subset_round_trips_exact_bits_and_ids() {
    for (count, dimension) in [(1, 1), (3, 2), (6, 5), (9, 3)] {
        let rows = rows(count, dimension);
        let bytes = encode(&rows);
        let segment = segment(&rows[0].0, &rows[count - 1].0, count);
        for mask in 0u32..(1 << count) {
            let selected = |ordinal: u64| mask & (1 << (ordinal - 40)) != 0;
            assert_eq!(
                visit(
                    &bytes,
                    expected(&segment, &bytes, count, dimension),
                    selected
                )
                .unwrap(),
                reference(&rows, selected),
                "{count}x{dimension} mask {mask:b}"
            );
        }
    }
}

#[test]
fn wide_segments_read_only_the_head_and_selected_rows() {
    let rows = rows(512, 384);
    let bytes = encode(&rows);
    let segment = segment(&rows[0].0, &rows[511].0, 512);
    let selected = |ordinal: u64| [41, 42, 300, 551].contains(&ordinal);
    let mut source = SliceSource(&bytes);
    let mut seen = Vec::new();
    let stats = visit_selected(
        &mut source,
        expected(&segment, &bytes, 512, 384),
        selected,
        |document| {
            seen.push(document.id.clone());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        seen,
        ["doc:0001:é", "doc:0002:é", "doc:0260:é", "doc:0511:é"]
    );
    // Head (two reads), one coalesced run for 41..=42, and two single rows.
    assert_eq!(stats.reads, 5);
    assert!(stats.bytes < (bytes.len() / 20) as u64, "{stats:?}");
    // A full scan reads every byte once, in bounded runs.
    let all = visit(&bytes, expected(&segment, &bytes, 512, 384), |_| true).unwrap();
    assert_eq!(all, reference(&rows, |_| true));
}

#[test]
fn empty_segments_round_trip() {
    let bytes = encode(&[]);
    let segment = segment("a", "z", 4);
    let expected = Expected {
        segment: &segment,
        payload_len: bytes.len() as u64,
        count: 0,
        vector_ordinal_base: 0,
        dimension: Some(7),
    };
    assert!(visit(&bytes, expected, |_| true).unwrap().is_empty());
    #[cfg(feature = "vector-search")]
    visit_ids(&mut SliceSource(&bytes), expected, |_, _| {
        panic!("an empty segment has no IDs")
    })
    .unwrap();
}

#[test]
fn every_corrupt_head_byte_fails_closed_and_rows_fail_only_when_read() {
    let rows = rows(5, 3);
    let bytes = encode(&rows);
    let segment = segment(&rows[0].0, &rows[4].0, 5);
    let head = u32_at(&bytes, 16) as usize;
    for position in 0..head {
        let mut corrupt = bytes.clone();
        corrupt[position] ^= 0x01;
        let error = visit(&corrupt, expected(&segment, &corrupt, 5, 3), |_| false).unwrap_err();
        assert!(
            matches!(&error, HawDBError::Storage(message) if message.contains("vector sidecar")),
            "byte {position}: {error:?}"
        );
    }
    for row in 0..5 {
        for column in [0, 11] {
            let mut corrupt = bytes.clone();
            corrupt[head + row * 12 + column] ^= 0x80;
            let target = 40 + row as u64;
            let error = visit(&corrupt, expected(&segment, &corrupt, 5, 3), |ordinal| {
                ordinal == target
            })
            .unwrap_err();
            assert!(
                error.to_string().contains("row checksum mismatch"),
                "{error}"
            );
            // Unselected corrupt rows are never read.
            assert_eq!(
                visit(&corrupt, expected(&segment, &corrupt, 5, 3), |ordinal| {
                    ordinal != target
                })
                .unwrap(),
                reference(&rows, |ordinal| ordinal != target)
            );
        }
    }
}

#[test]
fn layout_disagreements_and_superseded_text_payloads_fail_closed() {
    let rows = rows(4, 2);
    let bytes = encode(&rows);
    let segment = segment(&rows[0].0, &rows[3].0, 4);
    let base = expected(&segment, &bytes, 4, 2);
    let narrow = self::segment(&rows[1].0, &rows[3].0, 4);
    let cases = [
        Expected { count: 3, ..base },
        Expected {
            dimension: Some(3),
            ..base
        },
        Expected {
            dimension: None,
            ..base
        },
        Expected {
            payload_len: bytes.len() as u64 - 1,
            ..base
        },
        Expected {
            segment: &narrow,
            ..base
        },
    ];
    for case in cases {
        let truncated = &bytes[..usize::try_from(case.payload_len).unwrap()];
        assert!(
            matches!(
                visit(truncated, case, |_| true),
                Err(HawDBError::Storage(_))
            ),
            "{:?}",
            case.count
        );
    }
    let legacy = crate::encode_search_snapshot_text(
        "HAWDB_SEARCH_VECTOR_SEGMENT_V1\nvector\t0\t646f633a30\t1,2\n",
    )
    .unwrap();
    let error = visit(&legacy, expected(&segment, &legacy, 4, 2), |_| true).unwrap_err();
    assert!(
        error.to_string().contains("rebuild the search projection"),
        "{error}"
    );
}

#[test]
fn encoder_rejects_invalid_rows() {
    let finite = [1.0f32, 2.0];
    let short = [1.0f32];
    let nan = [f32::NAN, 1.0];
    let infinite = [1.0, f32::INFINITY];
    for rows in [
        vec![("a", &finite[..]), ("b", &short[..])],
        vec![("a", &finite[..]), ("a", &finite[..])],
        vec![("b", &finite[..]), ("a", &finite[..])],
        vec![("a", &nan[..])],
        vec![("a", &infinite[..])],
        vec![("a", &[][..])],
    ] {
        assert!(encode_into(&rows, &mut Vec::new(), || Ok(())).is_err());
    }
}

#[test]
fn cancellation_is_observed_while_encoding() {
    let rows = rows(4, 2);
    let borrowed = rows
        .iter()
        .map(|(id, embedding)| (id.as_str(), embedding.as_slice()))
        .collect::<Vec<_>>();
    let error = encode_into(&borrowed, &mut Vec::new(), || {
        Err(HawDBError::Execution("cancelled".into()))
    })
    .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
}

#[cfg(feature = "vector-search")]
#[test]
fn ids_are_visited_without_reading_rows() {
    let rows = rows(130, 4);
    let bytes = encode(&rows);
    let segment = segment(&rows[0].0, &rows[129].0, 130);
    let mut ids = Vec::new();
    let stats = visit_ids(
        &mut SliceSource(&bytes),
        expected(&segment, &bytes, 130, 4),
        |ordinal, id| {
            ids.push((ordinal, id.to_owned()));
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(stats.reads, 1);
    assert_eq!(
        ids,
        rows.iter()
            .enumerate()
            .map(|(index, (id, _))| (40 + index as u64, id.clone()))
            .collect::<Vec<_>>()
    );
}
