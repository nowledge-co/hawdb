use super::*;

#[test]
fn artifact_counters_match_wire_categories_and_uncompressed_record_encoding() {
    let root = projection_root("compact-artifact-bytes");
    fs::create_dir_all(&root).unwrap();
    let analyzer = SearchAnalyzerLexicon::default();
    let documents = (0..257)
        .map(|index| document(&format!("memory:{index:08}"), "", "graph"))
        .collect::<Vec<_>>();
    let reader = LexicalProjectionWriter::new(LexicalProjectionConfig::default())
        .write(&root, 1, None, 11, 13, documents.iter(), &analyzer)
        .unwrap();
    let mut expected = SearchLexicalArtifactBytes {
        header_bytes: 24,
        document_mapping_bytes: reader
            .manifest
            .blocks
            .iter()
            .map(|block| block.length)
            .sum(),
        dictionary_bytes: reader
            .manifest
            .dictionaries
            .iter()
            .map(|block| block.length)
            .sum(),
        ..SearchLexicalArtifactBytes::default()
    };
    for descriptor in &reader.manifest.dictionaries {
        let bytes = reader
            .read_range(descriptor.offset, descriptor.length as usize)
            .unwrap();
        let dictionary = dictionary::Dictionary::open(
            &bytes,
            dictionary_store::limits(reader.config).unwrap(),
            &mut || Ok(()),
        )
        .unwrap();
        dictionary
            .visit(|_, metadata| {
                let end = if metadata.skip_offset == 0 {
                    metadata.posting_bytes
                } else {
                    metadata.skip_offset
                };
                let bytes = reader
                    .read_range(metadata.posting_offset, end as usize)
                    .unwrap();
                let mut cursor = SliceCursor::new(&bytes);
                while !cursor.is_empty() {
                    let length = cursor.u32().unwrap() as usize;
                    let checksum = cursor.u64().unwrap();
                    let payload = cursor.bytes(length).unwrap();
                    assert_eq!(super::super::checksum(payload), checksum);
                    posting_codec::decode(payload).unwrap();
                    expected.posting_frame_bytes += 12 + length as u64;
                }
                expected.posting_skip_bytes += metadata.posting_bytes - end;
                Ok(())
            })
            .unwrap();
    }
    let mut legacy_records = Vec::new();
    for document in &documents {
        let tokens = document_tokens(document, &analyzer);
        let mut frequencies = BTreeMap::new();
        for term in &tokens {
            *frequencies.entry(term).or_insert(0u32) += 1;
        }
        for (term, frequency) in frequencies {
            write_string(&mut legacy_records, term).unwrap();
            write_string(&mut legacy_records, &document.id).unwrap();
            legacy_records.extend_from_slice(&frequency.to_le_bytes());
            legacy_records.extend_from_slice(&(tokens.len() as u32).to_le_bytes());
        }
    }
    expected.uncompressed_posting_payload_bytes = legacy_records.len() as u64;
    assert_eq!(reader.artifact_bytes(), expected);
    assert!(expected.posting_skip_bytes > 0);
    assert_eq!(
        expected.header_bytes
            + expected.document_mapping_bytes
            + expected.posting_frame_bytes
            + expected.posting_skip_bytes
            + expected.dictionary_bytes,
        reader.manifest.artifact_len
    );
    let reopened = LexicalProjectionReader::load(&root, None, 11, 13, reader.config)
        .unwrap()
        .unwrap();
    assert_eq!(reopened.artifact_bytes(), expected);
    let mut invalid = reader.manifest.clone();
    invalid.byte_counters.posting_frame_bytes += 1;
    let checksum = checksum(&serde_json::to_vec(&invalid).unwrap());
    let forged = serde_json::to_vec(&ManifestEnvelope {
        body: invalid,
        checksum,
    })
    .unwrap();
    assert!(ManifestBody::decode(&forged).is_err());
    drop(reader);
    drop(reopened);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn bounded_manifest_io_preserves_wire_bytes_without_directory_clones() {
    let root = projection_root("compact-manifest-budget");
    fs::create_dir_all(&root).unwrap();
    let documents = [document("a", "", "graph")];
    let reader = LexicalProjectionWriter::new(LexicalProjectionConfig::default())
        .write(
            &root,
            1,
            None,
            11,
            13,
            documents.iter(),
            &SearchAnalyzerLexicon::default(),
        )
        .unwrap();
    let manifest = &reader.manifest;
    let checksum = checksum(&serde_json::to_vec(manifest).unwrap());
    let expected = serde_json::to_vec(&ManifestEnvelope {
        body: manifest.clone(),
        checksum,
    })
    .unwrap();
    let limit = (expected.len() as u64).max(manifest.directory_resident_bytes());
    let encoded = manifest.encode_bounded(limit).unwrap();
    assert_eq!(encoded, expected);
    assert!(encoded.capacity() as u64 <= limit);
    assert!(manifest.encode_bounded(limit - 1).is_err());
    assert_eq!(
        ManifestBody::decode_bounded(&encoded, limit).unwrap(),
        *manifest
    );
    assert!(ManifestBody::decode_bounded(&encoded, encoded.len() as u64 - 1).is_err());
    assert_eq!(
        manifest_io::read(&root.join(MANIFEST_FILE), encoded.len() as u64).unwrap(),
        encoded
    );
    assert!(manifest_io::read(&root.join(MANIFEST_FILE), encoded.len() as u64 - 1).is_err());
    let mut inflated = manifest.clone();
    inflated.blocks.reserve_exact(limit as usize);
    assert!(inflated.directory_resident_bytes() > limit);
    assert!(inflated
        .encode_bounded(limit)
        .unwrap_err()
        .to_string()
        .contains("directory budget"));
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}
