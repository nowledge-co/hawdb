// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Binary records decode, re-encode and walk exactly like text records.

use super::*;
use crate::out_of_core::hydration::Bounds;
use std::io::BufReader;

fn documents() -> Vec<SearchDocument> {
    let metadata = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<BTreeMap<_, _>>()
    };
    vec![
        SearchDocument {
            id: "a".into(),
            title: String::new(),
            content: String::new(),
            embedding: None,
            metadata: BTreeMap::new(),
        },
        SearchDocument {
            id: "b\tid".into(),
            title: "tab\there\nnewline\r\\ 中文 ünï".into(),
            content: "x;y=z\n\n\t".repeat(700),
            embedding: Some(vec![
                0.0,
                -0.0,
                1.5,
                -3.25e-7,
                f32::MIN_POSITIVE / 8.0,
                f32::MAX,
                f32::MIN,
                0.1,
            ]),
            metadata: metadata(&[("kind", "note;="), ("", ""), ("space_id", "a=b;c")]),
        },
        SearchDocument {
            id: "c".into(),
            title: "t".into(),
            content: "c".into(),
            // An empty vector writes no components, like no vector.
            embedding: Some(Vec::new()),
            metadata: metadata(&[("k", "v")]),
        },
        SearchDocument {
            id: "d".into(),
            title: "🦀".repeat(3000),
            content: "y".into(),
            embedding: Some((0..1024).map(|value| value as f32 / 7.0).collect()),
            metadata: BTreeMap::new(),
        },
    ]
}

fn encode(documents: &[SearchDocument]) -> Vec<u8> {
    let mut body = Vec::new();
    for document in documents {
        let content = content_len(document, None).unwrap();
        assert_eq!(content, document.content.len() as u64);
        let before = body.len();
        write_record(&mut body, document, content).unwrap();
        assert_eq!(
            (body.len() - before) as u64,
            record_len(
                crate::document_encoding::HeaderSource::header(document),
                content
            )
            .unwrap()
        );
    }
    body
}

fn canonical(document: &SearchDocument) -> SearchDocument {
    let mut document = document.clone();
    if document.embedding.as_ref().is_some_and(Vec::is_empty) {
        document.embedding = None;
    }
    document
}

#[test]
fn binary_records_decode_and_reencode_as_their_text_lines() {
    let documents = documents();
    let body = encode(&documents);
    let text = documents
        .iter()
        .map(crate::encode_search_document_line)
        .collect::<String>();
    // Parsing in place: exact documents, byte accounting and skipping.
    let mut at = 0;
    for document in &documents {
        let (record, length) = Record::parse(&body[at..]).unwrap();
        let expected = canonical(document);
        assert_eq!(record.id, expected.id);
        assert_eq!(record.decode(true).unwrap(), expected);
        let mut without = expected.clone();
        without.embedding = None;
        assert_eq!(record.decode(false).unwrap(), without);
        assert_eq!(
            record.document_bytes(),
            crate::out_of_core::search_document_bytes(&expected)
        );
        at += length;
    }
    assert_eq!(at, body.len());
    // Re-encoding yields the text lines byte for byte, whatever the reads.
    for capacity in [1, 3, 7, 8192] {
        let mut output = Vec::new();
        TextRecords::new(BufReader::with_capacity(capacity, &body[..]))
            .read_to_end(&mut output)
            .unwrap();
        assert_eq!(output, text.as_bytes(), "capacity {capacity}");
        // Line consumers read the same lines through BufRead.
        let mut lines = TextRecords::new(BufReader::with_capacity(capacity, &body[..]));
        let mut line = String::new();
        let mut count = 0;
        while lines.read_line(&mut line).unwrap() != 0 {
            count += 1;
        }
        assert_eq!(line, text);
        assert_eq!(count, documents.len());
    }
}

#[test]
fn record_walk_and_scan_check_counts_bounds_and_truncation() {
    let documents = documents();
    let body = encode(&documents);
    let (first, last) = (documents[0].id.as_str(), documents[3].id.as_str());
    let entry = |count: u32| super::super::Entry {
        offset: 0,
        compressed_len: 1,
        uncompressed_len: body.len() as u32,
        compressed_crc: 0,
        uncompressed_crc: 0,
        document_count: count,
        first: (0, 0),
        last: (0, 0),
    };
    for step in [1, 2, 5, 19, body.len()] {
        let mut walk = RecordWalk::default();
        walk.start(16);
        for chunk in body.chunks(step) {
            walk.feed(chunk, first);
        }
        assert!(walk.finish(&entry(4), last), "step {step}");
        assert!(!walk.finish(&entry(3), last));
        assert!(!walk.finish(&entry(4), first));
        let mut wrong_first = RecordWalk::default();
        wrong_first.start(16);
        wrong_first.feed(&body, "z");
        assert!(!wrong_first.finish(&entry(4), last));
    }
    // A truncated block ends inside a record.
    for cut in [1, PREFIX_BYTES, body.len() - 1] {
        let mut walk = RecordWalk::default();
        walk.start(16);
        walk.feed(&body[..cut], first);
        assert!(!walk.finish(&entry(4), last), "cut {cut}");
        let mut output = Vec::new();
        assert!(TextRecords::new(&body[..cut])
            .read_to_end(&mut output)
            .is_err());
    }
    // An id longer than the walk's limit cannot match the bounds.
    let mut walk = RecordWalk::default();
    walk.start(0);
    walk.feed(&body, first);
    assert!(!walk.finish(&entry(4), last));

    let bounds = |first, last, count| Bounds { first, last, count };
    // A last id longer than the limit cannot match an indexed last id that is
    // its prefix: the walk and the sparse scan accept the same indexes.
    let mut long_last = documents[..2].to_vec();
    long_last[1].id = "abcdef".into();
    let long_body = encode(&long_last);
    let long_entry = super::super::Entry {
        uncompressed_len: long_body.len() as u32,
        ..entry(2)
    };
    for (indexed_last, accepted) in [("abc", false), ("abcdef", true)] {
        let mut walk = RecordWalk::default();
        walk.start(first.len().max(indexed_last.len()));
        walk.feed(&long_body, first);
        assert_eq!(walk.finish(&long_entry, indexed_last), accepted);
        assert_eq!(
            scan(&long_body, bounds(first, indexed_last, 2), |_| Ok(())).is_ok(),
            accepted
        );
    }
    let mut seen = Vec::new();
    scan(&body, bounds(first, last, 4), |record| {
        seen.push(record.id.to_string());
        Ok(())
    })
    .unwrap();
    assert_eq!(
        seen,
        documents
            .iter()
            .map(|document| document.id.clone())
            .collect::<Vec<_>>()
    );
    for (first, last, count) in [("a", "d", 3), ("a", "c", 4), ("b\tid", "d", 4)] {
        assert!(scan(&body, bounds(first, last, count), |_| Ok(())).is_err());
    }
    assert!(scan(&body[..body.len() - 1], bounds(first, last, 4), |_| Ok(())).is_err());
    // Records out of order are refused.
    let reversed = encode(&[documents[1].clone(), documents[0].clone()]);
    assert!(scan(&reversed, bounds("b\tid", "a", 2), |_| Ok(())).is_err());
}
