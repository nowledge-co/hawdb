use super::*;

fn fixture_documents(count: usize) -> BTreeMap<String, SearchDocument> {
    (0..count)
        .map(|index| {
            let id = format!("memory:{index:08}-0000-0000-0000-000000000000");
            let content = if index == count / 2 {
                "graph needle".to_string()
            } else {
                "graph".to_string()
            };
            (id.clone(), document(&id, "", &content))
        })
        .collect()
}

#[test]
fn sparse_ordinal_lookup_reads_only_the_selected_mapping_block() {
    let root = projection_root("sparse-mapping");
    fs::create_dir_all(&root).unwrap();
    let documents = fixture_documents(257);
    let config = LexicalProjectionConfig {
        target_block_bytes: NonZeroU64::new(128).unwrap(),
        max_block_bytes: NonZeroU64::new(1024).unwrap(),
        build_memory_bytes: NonZeroU64::new(512).unwrap(),
        max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
        ..LexicalProjectionConfig::default()
    };
    let analyzer = SearchAnalyzerLexicon::default();
    let reader = LexicalProjectionWriter::new(config)
        .write(&root, 1, None, 11, 13, documents.values(), &analyzer)
        .unwrap();
    let report = reader
        .score(
            &BTreeSet::from(["needle".to_string()]),
            &LexicalMiniDelta::default(),
            None,
            |_| Ok(true),
        )
        .unwrap();
    let expected = documents.keys().nth(128).unwrap();
    assert_eq!(report.scores.keys().collect::<Vec<_>>(), vec![expected]);
    let mapping = reader
        .manifest
        .blocks
        .iter()
        .find(|block| {
            block.kind == BlockKind::Documents
                && block.ordinal_start <= 128
                && 128 < block.ordinal_start + u64::from(block.entry_count)
        })
        .unwrap();
    assert_eq!(report.document_bytes_read, mapping.length);
    assert!(report.document_bytes_read * 100 < document_mapping_bytes(&reader));

    let mut lookup = DocumentLookup::new(&reader);
    for ordinal in [0, 128, 256] {
        let (id, length) = lookup.get(ordinal).unwrap();
        assert_eq!(&id, documents.keys().nth(ordinal as usize).unwrap());
        assert_eq!(
            length as usize,
            document_tokens(&documents[&id], &analyzer).len()
        );
    }
    assert!(lookup.get(255).is_err());
    assert!(lookup.get(257).is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn persisted_postings_use_full_simd_frames_and_a_varint_tail() {
    let root = projection_root("persisted-frames");
    fs::create_dir_all(&root).unwrap();
    let mut documents = fixture_documents(257);
    for document in documents.values_mut() {
        document.content = "graph".to_string();
    }
    let analyzer = SearchAnalyzerLexicon::default();
    let config = LexicalProjectionConfig {
        build_memory_bytes: NonZeroU64::new(512).unwrap(),
        target_block_bytes: NonZeroU64::new(4096).unwrap(),
        max_block_bytes: NonZeroU64::new(8192).unwrap(),
        max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
        ..LexicalProjectionConfig::default()
    };
    let reader = LexicalProjectionWriter::new(config)
        .write(&root, 1, None, 11, 13, documents.values(), &analyzer)
        .unwrap();
    let mut frames = Vec::new();
    for block in reader
        .manifest
        .blocks
        .iter()
        .filter(|block| block.kind == BlockKind::Postings)
    {
        let bytes = reader.read_block(block).unwrap();
        let mut cursor = SliceCursor::new(&bytes);
        decode_block_header(&mut cursor, 1, block, BlockKind::Postings).unwrap();
        while !cursor.is_empty() {
            assert_eq!(cursor.str(4096).unwrap(), "graph");
            let length = cursor.u32().unwrap() as usize;
            let bytes = cursor.bytes(length).unwrap();
            let decoded = posting_codec::decode(bytes).unwrap();
            frames.push((decoded.len(), bytes[8]));
        }
    }
    assert_eq!(frames, vec![(128, 0), (128, 0), (1, 1)]);
    // This synthetic wire-size check is not the representative-corpus benchmark.
    let old_record_bytes: u64 = documents
        .values()
        .map(|doc| 4 + "graph".len() as u64 + 4 + doc.id.len() as u64 + 8)
        .sum();
    assert!(term_posting_bytes(&reader, "graph") * 10 < old_record_bytes);
    let mut stream = TermPostingStream::new(&reader, "graph");
    let mut ordinals = Vec::new();
    while let Some(posting) = stream.next().unwrap() {
        assert!(stream.current.len() < 128);
        ordinals.push(posting.ordinal);
    }
    assert_eq!(ordinals, (0..257).collect::<Vec<_>>());
    let terms = BTreeSet::from(["graph".to_string()]);
    let report = reader
        .score(&terms, &LexicalMiniDelta::default(), None, |_| Ok(true))
        .unwrap();
    let corpus =
        super::super::super::TextCorpusStats::from_documents(documents.values(), &analyzer);
    for document in documents.values() {
        assert_eq!(
            report.scores[&document.id],
            super::super::super::bm25_score(&terms, document, &corpus, &analyzer)
        );
    }
    assert!(fs::read_dir(&root).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with("tmp")
    }));
    let reopened = LexicalProjectionReader::load(&root, None, 11, 13, config)
        .unwrap()
        .unwrap();
    assert_eq!(
        reopened
            .score(&terms, &LexicalMiniDelta::default(), None, |_| Ok(true))
            .unwrap(),
        report
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn document_mapping_rejects_truncation_ranges_and_old_layouts() {
    let root = projection_root("mapping-corruption");
    fs::create_dir_all(&root).unwrap();
    let documents = fixture_documents(3);
    let reader = LexicalProjectionWriter::new(LexicalProjectionConfig::default())
        .write(
            &root,
            1,
            None,
            11,
            13,
            documents.values(),
            &SearchAnalyzerLexicon::default(),
        )
        .unwrap();
    let block = reader
        .manifest
        .blocks
        .iter()
        .find(|block| block.kind == BlockKind::Documents)
        .unwrap();
    let bytes = reader.read_block(block).unwrap();
    for length in 0..bytes.len() {
        assert!(documents::validate_document_block(&bytes[..length], 1, block).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(documents::validate_document_block(&trailing, 1, block).is_err());
    let mut invalid = reader.manifest.clone();
    invalid.blocks[0].ordinal_start = u64::MAX;
    assert!(invalid.validate().is_err());
    let mut old = serde_json::to_value(&reader.manifest).unwrap();
    old.as_object_mut().unwrap().remove("layout");
    assert!(serde_json::from_value::<ManifestBody>(old).is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn duplicate_document_ids_fail_before_publishing_a_mapping() {
    let root = projection_root("duplicate-mapping-id");
    fs::create_dir_all(&root).unwrap();
    let document = document("a", "graph", "");
    let result = LexicalProjectionWriter::new(LexicalProjectionConfig::default()).write(
        &root,
        1,
        None,
        11,
        13,
        [&document, &document].into_iter(),
        &SearchAnalyzerLexicon::default(),
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("strictly increasing IDs"));
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    fs::remove_dir_all(root).unwrap();
}
