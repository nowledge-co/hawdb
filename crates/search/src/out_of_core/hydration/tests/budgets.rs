// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn hydration_byte_refusal_preserves_the_complete_operation_budget() {
    let root = super::super::super::tests::test_dir("hydration-byte-budget");
    let mut documents = (0..4).map(document).collect::<Vec<_>>();
    for (number, source) in documents.iter_mut().enumerate() {
        source.content = "body ".repeat(10 * (number + 1));
    }
    documents[1].content = "unselected ".repeat(4096);
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    for source in &documents {
        writer.push(source.clone()).unwrap();
    }
    writer.finish().unwrap();

    // The requested order differs from physical traversal. The last refusal
    // must include both an earlier segment and an earlier selected row here.
    let expected = vec![
        documents[3].clone(),
        documents[0].clone(),
        documents[2].clone(),
    ];
    let ids = expected
        .iter()
        .map(|source| source.id.clone())
        .collect::<Vec<_>>();
    let required = expected.iter().map(search_document_bytes).sum::<u64>();
    assert!(search_document_bytes(&documents[1]) > required);
    let reference = SearchOutOfCoreReader::open(&root).unwrap();
    let segment_ids = documents
        .iter()
        .map(|source| {
            reference
                .segment_for_document(&source.id)
                .unwrap()
                .unwrap()
                .segment
                .segment_id
        })
        .collect::<Vec<_>>();
    assert_eq!(segment_ids, vec![0, 0, 1, 1]);
    assert_eq!(
        reference.hydrate_documents(&ids).unwrap().documents,
        expected
    );

    let config = SearchOutOfCoreConfig {
        max_hydrated_bytes: NonZeroU64::new(required - 1).unwrap(),
        ..Default::default()
    };
    let reader = SearchOutOfCoreReader::open_with_config(&root, config).unwrap();
    let previous_segment = search_document_bytes(&documents[0]);
    let previous_document = search_document_bytes(&documents[2]);
    let requested = search_document_bytes(&documents[3]);
    let remaining = required - 1 - previous_segment;
    let expected_error = HawDBError::from(ResourceBudgetError {
        resource: ResourceBudgetKind::MemoryBytes,
        scope: ResourceBudgetScope::Operator,
        owner: "search hydration".to_string(),
        memory_class: None,
        requested,
        used: previous_segment + previous_document,
        budget: required - 1,
        budget_option: "SearchOutOfCoreConfig.max_hydrated_bytes",
        message: format!(
            "search hydration requires {} bytes, exceeding {remaining}",
            previous_document + requested
        ),
    });
    for _ in 0..2 {
        assert_eq!(reader.hydrate_documents(&ids).unwrap_err(), expected_error);
        let output = reader
            .hydrate_documents(std::slice::from_ref(&documents[0].id))
            .unwrap();
        assert_eq!(output.documents, vec![documents[0].clone()]);
        assert_eq!(output.metrics.hydrated_bytes, previous_segment);
    }

    let exact = SearchOutOfCoreReader::open_with_config(
        &root,
        SearchOutOfCoreConfig {
            max_hydrated_bytes: NonZeroU64::new(required).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    let output = exact.hydrate_documents(&ids).unwrap();
    assert_eq!(output.documents, expected);
    assert_eq!(output.metrics.hydrated_documents, ids.len());
    assert_eq!(output.metrics.hydrated_bytes, required);
    assert_eq!(output.metrics.segment_range_reads, 2);

    let count_limited = SearchOutOfCoreReader::open_with_config(
        &root,
        SearchOutOfCoreConfig {
            max_hydrated_documents: NonZeroUsize::new(2).unwrap(),
            max_hydrated_bytes: NonZeroU64::new(1).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    // Too many requested ids is a typed budget refusal, not a storage fault,
    // for hydration by id and for mutation-target loading (#1754).
    let assert_count_refusal = |error: HawDBError, owner: &str| match error {
        HawDBError::ResourceBudgetExceeded(details) => {
            assert_eq!(details.resource, ResourceBudgetKind::MetadataEntries);
            assert_eq!(details.scope, ResourceBudgetScope::Operator);
            assert_eq!(details.owner, owner, "{details:?}");
            assert_eq!(details.requested, 3, "{details:?}");
            assert_eq!(details.budget, 2, "{details:?}");
            assert_eq!(
                details.budget_option, "SearchOutOfCoreConfig.max_hydrated_documents",
                "{details:?}"
            );
        }
        other => panic!("{other:?}"),
    };
    assert_count_refusal(
        count_limited.hydrate_documents(&ids).unwrap_err(),
        "search mutation target resolution",
    );
    assert_count_refusal(
        count_limited
            .resolve_mutation_targets(&ids.iter().cloned().collect())
            .unwrap_err(),
        "search mutation target resolution",
    );
    drop((reference, reader, exact, count_limited));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn hydration_integrity_checks_precede_byte_refusals_after_draining() {
    let mut sources = vec![document(0), document(1)];
    sources[0].content = "first".to_string();
    let (text, segment) = fixture(&sources);
    assert!(text.len() > INPUT_BYTES);
    let ids = BTreeSet::from([sources[0].id.clone()]);
    let payload = compressed(text.as_bytes());
    let valid = envelope(&payload, text.len(), checksum_bytes(text.as_bytes()));
    let invalid = envelope(&payload, text.len(), checksum_bytes(text.as_bytes()) ^ 1);
    for (bytes, checksum, expected_integrity_error) in [
        (&valid, checksum_bytes(&valid), None),
        (
            &invalid,
            checksum_bytes(&invalid),
            Some("uncompressed checksum mismatch"),
        ),
        (
            &valid,
            checksum_bytes(&valid) ^ 1,
            Some("payload checksum mismatch"),
        ),
    ] {
        let mut input = Cursor::new(bytes);
        INFLATED_BYTES.set(None);
        let error = read_selected(
            &mut input,
            bytes.len() as u64,
            checksum,
            &segment,
            &ids,
            u64::MAX,
            HydrationBudget {
                limit: 20,
                used: 19,
            },
        )
        .unwrap_err();
        assert_eq!(input.position(), bytes.len() as u64);
        assert_eq!(INFLATED_BYTES.get(), Some(text.len() as u64));
        match expected_integrity_error {
            Some(message) => assert_eq!(error, super::super::invalid(message)),
            None => assert!(matches!(
                error,
                HawDBError::ResourceBudgetExceeded(details)
                    if details.requested == search_document_bytes(&sources[0])
                        && details.used == 19 && details.budget == 20
            )),
        }
    }
}
