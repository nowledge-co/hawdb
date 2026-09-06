use super::*;

#[test]
fn decoded_metadata_retains_its_charge_after_reader_drop() {
    let (root, reader) = fixture("candidate-metadata-owner");
    let memory = memory(128 * 1024 * 1024);
    let ledger = memory.ledger.clone();
    let documents = reader
        .read_metadata_segment(
            &reader.descriptor.segments[0],
            &mut SearchOutOfCoreMetrics::default(),
            &memory,
            &RuntimeTaskContext::default(),
        )
        .unwrap();
    let retained = documents.capacity() * std::mem::size_of::<SearchMetadataDocument>()
        + documents
            .iter()
            .map(|entry| {
                entry.document.id.capacity()
                    + entry
                        .document
                        .metadata
                        .iter()
                        .map(|(key, value)| 2048 + key.capacity() + value.capacity())
                        .sum::<usize>()
            })
            .sum::<usize>();
    assert!(retained > 0);
    assert_eq!(ledger.snapshot().used_bytes, retained);
    drop(memory);
    drop(reader);
    assert_eq!(ledger.snapshot().used_bytes, retained);
    assert_eq!(documents[0].document.id, "memory:000");
    drop(documents);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

pub(super) fn envelope(payload: &[u8], decoded: &[u8], advertised_len: u64) -> Vec<u8> {
    // Construct the independent v1 wire, not the production envelope parser.
    let mut bytes = format!(
        "{}\ncodec\tzstd\ncompressed_len\t{}\ncompressed_checksum\t{}\nuncompressed_len\t{advertised_len}\nuncompressed_checksum\t{}\n\n",
        crate::SEARCH_COMPRESSION_HEADER, payload.len(), checksum_bytes(payload), checksum_bytes(decoded),
    ).into_bytes();
    bytes.extend_from_slice(payload);
    bytes
}

#[test]
fn concatenated_modern_frames_match_the_independent_body() {
    let body = "a\0雪".repeat(1024);
    let split = body.len() / 2;
    let mut frames = zstd::stream::encode_all(&body.as_bytes()[..split], 0).unwrap();
    frames.extend(zstd::stream::encode_all(&body.as_bytes()[split..], 0).unwrap());
    let bytes = envelope(&frames, body.as_bytes(), body.len() as u64);
    let memory = memory(query_io::DECODE_WORKSPACE_BYTES + body.len());
    let decoded = query_io::decode(
        &bytes,
        body.len() as u64,
        &memory.working,
        &RuntimeTaskContext::default(),
    )
    .unwrap();
    assert_eq!(**decoded, body);
    assert_eq!(
        crate::decode_search_snapshot_text_bounded(&bytes, body.len() as u64).unwrap(),
        body
    );
    assert_eq!(memory.ledger.snapshot().used_bytes, body.len());
    drop(decoded);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn forged_envelopes_and_unqualified_frames_fail_without_unbounded_allocation() {
    let body = "x".repeat(4096);
    let compressed = zstd::stream::encode_all(body.as_bytes(), 0).unwrap();
    let memory = memory(query_io::DECODE_WORKSPACE_BYTES + body.len());
    let task = RuntimeTaskContext::default();
    let good = envelope(&compressed, body.as_bytes(), body.len() as u64);
    let header_end = good.windows(2).position(|value| value == b"\n\n").unwrap();
    let mut duplicate = good[..header_end].to_vec();
    duplicate.extend_from_slice(b"\ncodec\tzstd");
    duplicate.extend_from_slice(&good[header_end..]);
    let skippable = [0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0];
    for bytes in [
        duplicate,
        envelope(&compressed, body.as_bytes(), u64::MAX),
        envelope(&skippable, b"", 0),
        envelope(&[0x27, 0xb5, 0x2f, 0xfd], b"", 0),
        envelope(
            &compressed[..compressed.len() - 1],
            body.as_bytes(),
            body.len() as u64,
        ),
    ] {
        query_io::evidence::take();
        assert!(query_io::decode(&bytes, body.len() as u64, &memory.working, &task).is_err());
        assert_eq!(query_io::evidence::take().1, 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    for advertised in [0, body.len() - 1] {
        let bytes = envelope(&compressed, body.as_bytes(), advertised as u64);
        query_io::evidence::take();
        assert!(query_io::decode(&bytes, body.len() as u64, &memory.working, &task).is_err());
        assert_eq!(query_io::evidence::take().1, 1);
        assert!(
            memory.ledger.snapshot().peak_bytes
                <= query_io::DECODE_WORKSPACE_BYTES + advertised.max(body.len() - 1)
        );
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    let wrong_checksum = envelope(&compressed, b"wrong checksum", body.len() as u64);
    assert!(query_io::decode(&wrong_checksum, body.len() as u64, &memory.working, &task).is_err());
    let non_utf8 = [0xff, 0xfe];
    let bytes = envelope(
        &zstd::stream::encode_all(non_utf8.as_slice(), 0).unwrap(),
        &non_utf8,
        2,
    );
    assert!(query_io::decode(&bytes, 2, &memory.working, &task).is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn metadata_count_and_shape_preflight_cannot_drive_capacity() {
    let task = RuntimeTaskContext::default();
    for (line, count) in [
        ("meta\t61\t-\t\n", 0),
        ("meta\t61\t-\t\textra\n", 1),
        ("meta\t61\t-\tinvalid\n", 1),
        ("meta\t61\t-\t\n", 2),
    ] {
        let text = format!("SKEIN_SEARCH_METADATA_SEGMENT_V1\n{line}");
        assert!(query_io::metadata_bytes(&text, count, &task).is_err());
    }
}

#[test]
fn candidate_predicate_scratch_preserves_json_csv_and_unicode_matching() {
    for (raw, expected, matches) in [
        (r#"[" Snow ","İ","comma,value"]"#, "i\u{307}", true),
        (" Snow , İ , second ", "snow", true),
        (r#"["comma,value"]"#, "comma,value", true),
        (r#"["comma,value"]"#, "comma", false),
        (r#"["valid",null]"#, "valid", false),
    ] {
        let mut doc = document(0, "team");
        doc.metadata.insert("labels".to_owned(), raw.to_owned());
        let documents = [SearchMetadataDocument {
            document: doc,
            vector_ordinal: None,
        }];
        let predicates =
            crate::SearchPredicateSet::new(vec![crate::SearchPredicate::eq("labels", expected)]);
        let baseline = memory(128 * 1024 * 1024);
        let task = RuntimeTaskContext::default();
        let (bytes, count) = candidate_memory::encode(
            &documents,
            &predicates,
            8192,
            8192,
            &baseline.working,
            &task,
        )
        .unwrap();
        assert_eq!(count, usize::from(matches));
        assert_eq!(
            bytes.as_slice(),
            if matches {
                wire(&[("memory:000", None)])
            } else {
                Vec::new()
            }
        );
        let peak = baseline.ledger.snapshot().peak_bytes;
        drop(bytes);
        assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
        for limit in [peak, peak - 1] {
            let bounded = memory(limit);
            let result = candidate_memory::encode(
                &documents,
                &predicates,
                8192,
                8192,
                &bounded.working,
                &task,
            );
            assert_eq!(result.is_ok(), limit == peak);
            drop(result);
            assert_eq!(bounded.ledger.snapshot().used_bytes, 0);
        }
    }
}
