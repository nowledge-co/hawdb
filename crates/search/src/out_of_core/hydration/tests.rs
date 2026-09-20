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

use super::super::*;
use super::*;
use std::io::Cursor;

thread_local! {
    static INFLATED_BYTES: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

pub(super) fn record_inflated_bytes(bytes: u64) {
    INFLATED_BYTES.set(Some(bytes));
}

fn test_dir() -> PathBuf {
    let sequence = CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "hawdb-streamed-hydration-{}-{sequence}",
        std::process::id()
    ));
    fs::create_dir(&root).unwrap();
    root
}

fn document(number: usize) -> SearchDocument {
    SearchDocument {
        id: format!("document:{number:04}"),
        title: "title".to_string(),
        content: " ".repeat(16 * 1024),
        embedding: Some(vec![1.0, 2.0]),
        metadata: BTreeMap::from([("key".to_string(), "value".to_string())]),
    }
}

#[test]
fn hydration_retains_one_decoded_document_instead_of_the_segment() {
    let root = test_dir();
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    for number in 0..16 {
        writer.push(document(number)).unwrap();
    }
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let expected = document(7);
    assert!(
        reader
            .segment_for_document(&expected.id)
            .unwrap()
            .unwrap()
            .segment
            .document_count
            > 1
    );
    let output = reader
        .hydrate_documents(std::slice::from_ref(&expected.id))
        .unwrap();
    assert_eq!(output.documents, vec![expected.clone()]);
    assert_eq!(
        output.metrics.hydrated_bytes,
        search_document_bytes(&expected)
    );
    assert_eq!(
        output.metrics.peak_segment_document_bytes,
        search_document_bytes(&expected)
    );
    assert_eq!(output.metrics.lexical_document_block_reads, 1);
    assert!(output.metrics.lexical_document_bytes_read > 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

fn fixture(documents: &[SearchDocument]) -> (String, SearchSegmentDescriptorEntry) {
    let mut text = String::from("HAWDB_SEARCH_SEGMENT_V1\n");
    for document in documents {
        text.push_str(&crate::encode_search_document_line(document));
    }
    let descriptor = SearchSegmentDescriptorEntry::from_documents(
        0,
        &documents.iter().collect::<Vec<_>>(),
        &BTreeSet::new(),
    );
    (text, descriptor)
}

fn envelope(payload: &[u8], declared_len: usize, checksum: u64) -> Vec<u8> {
    let mut bytes = format!(
        "{}\ncodec\tzstd\nuncompressed_checksum\t{checksum}\ncompressed_checksum\t{}\nuncompressed_len\t{declared_len}\ncompressed_len\t{}\n\n",
        crate::SEARCH_COMPRESSION_HEADER, checksum_bytes(payload), payload.len(),
    ).into_bytes();
    bytes.extend_from_slice(payload);
    bytes
}

fn compressed(text: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(text, 1).unwrap()
}

struct FussyReader<'a> {
    bytes: &'a [u8],
    position: usize,
    chunk: usize,
    reads: usize,
    fail_at: Option<usize>,
}

impl Read for FussyReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        assert!(output.len() <= INPUT_BYTES, "unbounded range read");
        self.reads += 1;
        if self.reads.is_multiple_of(5) {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.fail_at == Some(self.position) {
            return Err(io::Error::other("injected hydration read failure"));
        }
        let count = output
            .len()
            .min(self.chunk)
            .min(self.bytes.len() - self.position)
            .min(self.fail_at.unwrap_or(usize::MAX) - self.position);
        output[..count].copy_from_slice(&self.bytes[self.position..self.position + count]);
        self.position += count;
        Ok(count)
    }
}

fn streamed(
    bytes: &[u8],
    segment: &SearchSegmentDescriptorEntry,
    ids: &BTreeSet<String>,
    max_bytes: u64,
    output_bytes: u64,
    chunk: usize,
) -> Result<Selection> {
    let mut reader = FussyReader {
        bytes,
        position: 0,
        chunk,
        reads: 0,
        fail_at: None,
    };
    let result = read_selected(
        &mut reader,
        bytes.len() as u64,
        checksum_bytes(bytes),
        segment,
        ids,
        max_bytes,
        output_bytes,
    );
    assert_eq!(
        reader.position,
        bytes.len(),
        "the whole range must be consumed exactly once"
    );
    result
}

#[test]
fn streamed_hydration_preserves_legacy_grammar_and_admission() {
    let sources = (0..4)
        .map(|number| {
            let mut source = document(number);
            source.content = "text\t\n\0\u{e9}\u{4e2d}\u{1f980}".repeat(32);
            source
        })
        .collect::<Vec<_>>();
    let (text, segment) = fixture(&sources);
    let ids = [sources[0].id.clone(), sources[3].id.clone()]
        .into_iter()
        .collect::<BTreeSet<_>>();
    for text in [
        text.clone(),
        text.replace('\n', "\r\n"),
        text.trim_end_matches('\n').to_string(),
        format!("\n{text}\nHAWDB_SEARCH_SEGMENT_V1\n"),
    ] {
        let bytes = encode_search_snapshot_text(&text).unwrap();
        let reference = decode_search_segment_documents_bounded(&bytes, text.len() as u64).unwrap();
        validate_search_segment_documents(&segment, &reference).unwrap();
        let expected = reference
            .into_iter()
            .filter(|doc| ids.contains(&doc.id))
            .collect::<Vec<_>>();
        let required = expected.iter().map(search_document_bytes).sum::<u64>();
        for chunk in [1, 3, 17, INPUT_BYTES] {
            let selected =
                streamed(&bytes, &segment, &ids, text.len() as u64, required, chunk).unwrap();
            assert_eq!(selected.documents, expected);
            assert_eq!(selected.bytes, required);
            assert!(selected.peak_document_bytes <= required + search_document_bytes(&sources[0]));
            assert!(streamed(
                &bytes,
                &segment,
                &ids,
                text.len() as u64,
                required - 1,
                chunk
            )
            .is_err());
            assert!(streamed(
                &bytes,
                &segment,
                &ids,
                text.len() as u64 - 1,
                required,
                chunk
            )
            .is_err());
        }
    }
}

#[test]
fn unrequested_tail_corruption_and_descriptor_damage_fail_closed() {
    let sources = (0..3).map(document).collect::<Vec<_>>();
    let (text, segment) = fixture(&sources);
    let ids = BTreeSet::from([sources[0].id.clone()]);
    let mut invalid_texts = vec![format!("{text}not a document\n").into_bytes()];
    let mut bad_utf8 = text.as_bytes().to_vec();
    bad_utf8.extend_from_slice(&[0xff, b'\n']);
    invalid_texts.push(bad_utf8);
    let mut reversed = sources.clone();
    reversed.swap(1, 2);
    invalid_texts.push(fixture(&reversed).0.into_bytes());
    let mut duplicate = sources.clone();
    duplicate[2] = sources[1].clone();
    invalid_texts.push(fixture(&duplicate).0.into_bytes());
    let mut bad_last = crate::encode_search_document_line(&sources[2]);
    bad_last = bad_last.replacen("doc\t", "doc\tgg", 1);
    invalid_texts.push(format!("{}{}", fixture(&sources[..2]).0, bad_last).into_bytes());
    for text in invalid_texts {
        let bytes = envelope(&compressed(&text), text.len(), checksum_bytes(&text));
        for requested in [&ids, &BTreeSet::new()] {
            assert!(streamed(&bytes, &segment, requested, u64::MAX, u64::MAX, 13).is_err());
        }
    }
    let bytes = encode_search_snapshot_text(&text).unwrap();
    for case in 0..4 {
        let mut wrong = segment.clone();
        match case {
            0 => wrong.document_count += 1,
            1 => wrong.document_count -= 1,
            2 => wrong.first_document_id.push('x'),
            _ => wrong.last_document_id.push('x'),
        }
        assert!(streamed(&bytes, &wrong, &ids, u64::MAX, u64::MAX, 7).is_err());
    }
}

#[test]
fn complete_integrity_covers_frames_lengths_and_every_fault_prefix() {
    let mut source = document(0);
    source.content = "small".to_string();
    let (text, segment) = fixture(&[source.clone()]);
    let ids = BTreeSet::from([source.id]);
    let payload = compressed(text.as_bytes());
    let bytes = envelope(&payload, text.len(), checksum_bytes(text.as_bytes()));
    for offset in 0..bytes.len() {
        let mut input = FussyReader {
            bytes: &bytes,
            position: 0,
            chunk: 5,
            reads: 0,
            fail_at: Some(offset),
        };
        let result = read_selected(
            &mut input,
            bytes.len() as u64,
            checksum_bytes(&bytes),
            &segment,
            &ids,
            u64::MAX,
            u64::MAX,
        );
        assert!(result.is_err(), "read failure at {offset}");
        assert!(read_selected(
            Cursor::new(&bytes[..offset]),
            bytes.len() as u64,
            checksum_bytes(&bytes),
            &segment,
            &ids,
            u64::MAX,
            u64::MAX
        )
        .is_err());
    }
    for declared in [0, 1, text.len() - 1, text.len() + 1] {
        let bytes = envelope(&payload, declared, checksum_bytes(text.as_bytes()));
        assert!(streamed(&bytes, &segment, &ids, u64::MAX, u64::MAX, 11).is_err());
    }
    let bad = envelope(&payload, text.len(), checksum_bytes(text.as_bytes()) ^ 1);
    assert!(streamed(&bad, &segment, &ids, u64::MAX, u64::MAX, 11).is_err());
    assert!(read_selected(
        Cursor::new(&bytes),
        bytes.len() as u64,
        checksum_bytes(&bytes) ^ 1,
        &segment,
        &ids,
        u64::MAX,
        u64::MAX
    )
    .is_err());
    let mut bad = bytes.clone();
    *bad.last_mut().unwrap() ^= 1;
    assert!(streamed(&bad, &segment, &ids, u64::MAX, u64::MAX, 11).is_err());

    let split = text.len() / 2;
    let mut frames = compressed(&text.as_bytes()[..split]);
    frames.extend_from_slice(&compressed(&text.as_bytes()[split..]));
    let bytes = envelope(&frames, text.len(), checksum_bytes(text.as_bytes()));
    assert!(streamed(&bytes, &segment, &ids, u64::MAX, u64::MAX, 7).is_ok());
    for tail in [vec![0xff], compressed(b"garbage")] {
        let mut damaged = frames.clone();
        damaged.extend_from_slice(&tail);
        let bytes = envelope(&damaged, text.len(), checksum_bytes(text.as_bytes()));
        assert!(streamed(&bytes, &segment, &ids, u64::MAX, u64::MAX, 7).is_err());
    }
}

#[test]
fn public_hydration_preserves_order_budgets_and_old_reader() {
    let root = test_dir();
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    for number in 0..8 {
        writer.push(document(number)).unwrap();
    }
    writer.finish().unwrap();
    let expected = vec![document(7), document(0), document(3)];
    let ids = expected
        .iter()
        .map(|doc| doc.id.clone())
        .collect::<Vec<_>>();
    let required = expected.iter().map(search_document_bytes).sum::<u64>();
    let config = SearchOutOfCoreConfig {
        max_hydrated_bytes: NonZeroU64::new(required).unwrap(),
        ..Default::default()
    };
    let reader = SearchOutOfCoreReader::open_with_config(&root, config.clone()).unwrap();
    let output = reader.hydrate_documents(&ids).unwrap();
    assert_eq!(output.documents, expected);
    assert_eq!(output.metrics.hydrated_bytes, required);
    assert_eq!(output.metrics.hydrated_documents, 3);
    assert_eq!(output.metrics.segment_range_reads, 3);
    assert_eq!(
        output.metrics.segment_bytes_read,
        output.metrics.hydration_segment_bytes_read
    );
    assert!(reader
        .hydrate_documents(&[ids[0].clone(), ids[0].clone()])
        .is_err());
    assert!(reader.hydrate_documents(&["missing".to_string()]).is_err());
    assert!(reader.hydrate_documents(&[]).unwrap().documents.is_empty());
    let short = SearchOutOfCoreReader::open_with_config(
        &root,
        SearchOutOfCoreConfig {
            max_hydrated_bytes: NonZeroU64::new(required - 1).unwrap(),
            ..config
        },
    )
    .unwrap();
    assert!(short.hydrate_documents(&ids).is_err());
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    writer.push(document(9)).unwrap();
    writer.finish().unwrap();
    assert_eq!(reader.hydrate_documents(&ids).unwrap().documents, expected);
    drop(reader);
    drop(short);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn understated_output_never_inflates_to_the_larger_reader_limit() {
    let mut source = document(0);
    source.content = " ".repeat(2 * 1024 * 1024);
    let (text, segment) = fixture(&[source]);
    let payload = compressed(text.as_bytes());
    for declared in [0, 1, 17, INPUT_BYTES, INPUT_BYTES + 1] {
        let bytes = envelope(&payload, declared, checksum_bytes(text.as_bytes()));
        INFLATED_BYTES.set(None);
        assert!(streamed(&bytes, &segment, &BTreeSet::new(), u64::MAX, u64::MAX, 31).is_err());
        assert_eq!(INFLATED_BYTES.get(), Some(declared as u64 + 1));
    }
    let bytes = envelope(&payload, text.len(), checksum_bytes(text.as_bytes()));
    INFLATED_BYTES.set(None);
    assert!(streamed(
        &bytes,
        &segment,
        &BTreeSet::new(),
        text.len() as u64 - 1,
        u64::MAX,
        31
    )
    .is_err());
    assert_eq!(INFLATED_BYTES.get(), None);
}

#[test]
fn public_hydration_rejects_damaged_tail_without_writing_artifacts() {
    let root = test_dir();
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    writer.push(document(0)).unwrap();
    writer.push(document(1)).unwrap();
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reader.primary_segment().descriptor.segments.len(), 1);
    let path = root.join(
        &reader
            .manifest
            .segments
            .first()
            .expect("manifest has a segment")
            .payload_file,
    );
    let valid = fs::read(&path).unwrap();
    let mut damaged = valid.clone();
    *damaged.last_mut().unwrap() ^= 1;
    fs::write(&path, &damaged).unwrap();
    let error = reader.hydrate_documents(&[document(0).id]).unwrap_err();
    assert!(error.to_string().contains("payload checksum mismatch"));
    assert_eq!(fs::read(&path).unwrap(), damaged);
    fs::write(&path, &valid).unwrap();
    assert_eq!(
        reader
            .hydrate_documents(&[document(0).id])
            .unwrap()
            .documents,
        vec![document(0)]
    );
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

fn differential_cases(cases: usize) {
    let mut state = 0x392_409_u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for case in 0..cases {
        let count = (next() % 12 + 1) as usize;
        let documents = (0..count)
            .map(|number| {
                let mut source = document(number);
                source.content = format!("{}\t\n\0\u{e9}\u{4e2d}\u{1f980}", next())
                    .repeat((next() % 24) as usize);
                source.title = format!("title:{}", next());
                if next() & 1 == 0 {
                    source.embedding = None;
                }
                source.metadata.insert(
                    format!("key:{}", next()),
                    "\n\t=;\u{4e2d}".repeat((next() % 4) as usize),
                );
                source
            })
            .collect::<Vec<_>>();
        let (text, segment) = fixture(&documents);
        let ids = documents
            .iter()
            .filter(|_| next() & 1 == 0)
            .map(|doc| doc.id.clone())
            .collect::<BTreeSet<_>>();
        let bytes = encode_search_snapshot_text(&text).unwrap();
        let legacy = decode_search_segment_documents_bounded(&bytes, text.len() as u64).unwrap();
        validate_search_segment_documents(&segment, &legacy).unwrap();
        let expected = legacy
            .into_iter()
            .filter(|doc| ids.contains(&doc.id))
            .collect::<Vec<_>>();
        let required = expected.iter().map(search_document_bytes).sum::<u64>();
        let selected = streamed(
            &bytes,
            &segment,
            &ids,
            text.len() as u64,
            required,
            (next() % 127 + 1) as usize,
        )
        .unwrap();
        assert_eq!(selected.documents, expected, "case {case}");
        assert_eq!(selected.bytes, required);
        if required != 0 {
            assert!(streamed(&bytes, &segment, &ids, text.len() as u64, required - 1, 31).is_err());
        }
        let mut damaged = text.into_bytes();
        damaged.push(0xff);
        let bytes = envelope(
            &compressed(&damaged),
            damaged.len(),
            checksum_bytes(&damaged),
        );
        assert!(streamed(&bytes, &segment, &ids, u64::MAX, u64::MAX, 29).is_err());
    }
}

#[test]
fn hydration_differential_smoke() {
    differential_cases(32);
}

#[test]
#[ignore = "explicit local hydration differential campaign"]
fn hydration_differential_campaign() {
    differential_cases(512);
}
