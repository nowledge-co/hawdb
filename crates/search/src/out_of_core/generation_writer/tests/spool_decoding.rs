use super::*;

fn append_frame(bytes: &mut Vec<u8>, record: &[u8]) {
    bytes.extend_from_slice(&(record.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&checksum_bytes(record).to_le_bytes());
    bytes.extend_from_slice(record);
}

#[test]
fn streamed_spool_scan_rejects_unadmitted_lengths_before_decoding() {
    let root = test_dir("streamed_spool_reader_admission");
    fs::create_dir(&root).unwrap();
    let source = SpoolSource {
        path: root.join("test.spool"),
        document_count: 1,
        max_record_bytes: 128,
    };
    for length in [0u64, 129, u64::MAX] {
        let mut bytes = SPOOL_HEADER.to_vec();
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        fs::write(&source.path, bytes).unwrap();
        let error = source
            .scan(&mut |_| panic!("unadmitted record reached a consumer"))
            .unwrap_err();
        assert!(
            error.to_string().contains("outside its admission"),
            "{error}"
        );
    }
    let record = b"doc\t61\t\t\t\t\n";
    let mut bytes = SPOOL_HEADER.to_vec();
    append_frame(&mut bytes, record);
    fs::write(&source.path, bytes).unwrap();
    for limit in [record.len() - 1, record.len()] {
        let source = SpoolSource {
            path: source.path.clone(),
            document_count: source.document_count,
            max_record_bytes: limit as u64,
        };
        let mut documents = Vec::new();
        let result = source.scan(&mut |document| {
            documents.push(document);
            Ok(())
        });
        if limit == record.len() {
            result.unwrap();
            assert_eq!(documents.len(), 1);
            assert_eq!(documents[0].id, "a");
        } else {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("outside its admission"));
            assert!(documents.is_empty());
        }
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn streamed_spool_syntax_failures_never_emit_bad_rows_or_publish() {
    let root = test_dir("streamed_spool_invalid_fields");
    let mut initial = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    initial.push(document(0)).unwrap();
    let generation = initial.finish().unwrap().generation;
    let before = published_files(&root);
    for field in 0..7 {
        let mut replacement =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        replacement.push(document(1)).unwrap();
        replacement.push(document(2)).unwrap();
        let error = replacement
            .finish_with_artifacts(|input, source, generation| {
                let first = crate::document_encoding::legacy_encode(&document(1));
                let second = crate::document_encoding::legacy_encode(&document(2));
                let mut fields = second
                    .trim_end_matches('\n')
                    .split('\t')
                    .map(str::to_string)
                    .collect::<Vec<_>>();
                match field {
                    0..=2 => fields[field + 1] = "0\u{20ac}".into(),
                    3 => fields[4] = "1,,2".into(),
                    4 => fields[5] = "0\u{20ac}=61".into(),
                    5 => fields[5] = "61=0\u{20ac}".into(),
                    6 => fields[1] = crate::encode_string(&document(1).id),
                    _ => unreachable!(),
                }
                let second = format!("{}\n", fields.join("\t"));
                let mut bytes = SPOOL_HEADER.to_vec();
                append_frame(&mut bytes, first.as_bytes());
                append_frame(&mut bytes, second.as_bytes());
                fs::write(&source.path, bytes)?;
                let mut consumed = Vec::new();
                assert!(source
                    .scan(&mut |document| {
                        consumed.push(document);
                        Ok(())
                    })
                    .is_err());
                assert_eq!(consumed, vec![document(1)]);
                input.build_artifacts(source, generation)
            })
            .unwrap_err();
        let expected = match field {
            3 => "embedding",
            6 => "strictly ordered",
            _ => "hex field",
        };
        assert!(
            error.to_string().contains(expected),
            "field {field}: {error}"
        );
        assert_eq!(stage_directories(&root), 0);
        assert_eq!(published_files(&root), before);
        let reader = crate::SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reader.generation(), generation);
        assert_eq!(
            reader
                .hydrate_documents(&[document(0).id])
                .unwrap()
                .documents,
            vec![document(0)]
        );
    }
    fs::remove_dir_all(root).unwrap();
}
