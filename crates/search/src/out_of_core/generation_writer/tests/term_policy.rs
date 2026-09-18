use super::*;
use crate::{SearchLexicalTermPolicy, SearchOutOfCoreReader};
#[cfg(feature = "full-text-search")]
use crate::{SearchMode, SearchQueryOptions};

fn policy(bytes: u64) -> SearchLexicalTermPolicy {
    SearchLexicalTermPolicy::new(NonZeroU64::new(bytes).unwrap()).unwrap()
}

fn term_document(number: usize, term: &str) -> SearchDocument {
    SearchDocument {
        id: format!("memory:{number:06}"),
        title: String::new(),
        content: term.into(),
        embedding: None,
        metadata: BTreeMap::new(),
    }
}

fn open(root: &Path, limit: u64) -> crate::error::Result<SearchOutOfCoreReader> {
    SearchOutOfCoreReader::open_with_term_policy(
        root,
        Default::default(),
        Default::default(),
        policy(limit),
    )
}

#[cfg(feature = "full-text-search")]
fn search(
    reader: &SearchOutOfCoreReader,
    term: &str,
) -> crate::error::Result<crate::SearchOutOfCoreOutput> {
    reader.search_with_options(term, None, SearchMode::Text, query_options(10))
}

#[cfg(feature = "full-text-search")]
fn query_options(limit: usize) -> SearchQueryOptions {
    SearchQueryOptions {
        limit,
        offset: 0,
        rank_window: None,
        fusion_weights: Default::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    }
}

#[test]
fn term_policy_has_checked_v1_bounds_and_unchanged_default() {
    assert_eq!(SearchLexicalTermPolicy::default(), policy(4096));
    assert_eq!(policy(1).max_term_bytes().get(), 1);
    assert_eq!(
        policy(u64::from(u32::MAX)).max_term_bytes().get(),
        u64::from(u32::MAX)
    );
    for bytes in [u64::from(u32::MAX) + 1, u64::MAX] {
        assert!(SearchLexicalTermPolicy::new(NonZeroU64::new(bytes).unwrap()).is_err());
    }
}

#[test]
fn term_policy_build_open_and_utf8_boundaries() {
    for limit in [4096, 5202] {
        for bytes in [4095, 4096, 4097, 5201, 5202, 5203] {
            for unicode in [false, true] {
                let root = test_dir("term_policy_boundary");
                let term = if unicode {
                    format!("{}{}", "é".repeat(bytes / 2), "x".repeat(bytes % 2))
                } else {
                    "x".repeat(bytes)
                };
                let mut writer = SearchOutOfCoreGenerationWriter::create_with_term_policy(
                    &root,
                    Default::default(),
                    policy(limit),
                )
                .unwrap();
                writer.push(term_document(0, &term)).unwrap();
                let result = writer.finish();
                if bytes as u64 > limit {
                    assert!(result
                        .unwrap_err()
                        .to_string()
                        .contains("lexical term uses"));
                    assert!(!root.join(OUT_OF_CORE_MANIFEST_FILE).exists());
                } else {
                    result.unwrap();
                    let reader = open(&root, limit).unwrap();
                    assert_eq!(reader.document_count(), 1);
                    assert_eq!(
                        reader
                            .hydrate_documents(&["memory:000000".into()])
                            .unwrap()
                            .documents[0]
                            .content,
                        term
                    );
                    assert_eq!(SearchOutOfCoreReader::open(&root).is_ok(), bytes <= 4096);
                    assert!(open(&root, bytes as u64 - 1).is_err());
                    #[cfg(feature = "full-text-search")]
                    assert_eq!(search(&reader, &term).unwrap().result.hits.len(), 1);
                }
                assert_eq!(stage_directories(&root), 0);
                fs::remove_dir_all(root).unwrap();
            }
        }
    }
}

#[test]
fn term_policy_writer_reconfiguration_validates_the_complete_stage() {
    let root = test_dir("term_policy_writer_dynamic");
    let term = "x".repeat(5202);
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    writer.push(term_document(0, &term)).unwrap();
    writer.set_lexical_term_policy(policy(5202));
    assert_eq!(writer.lexical_term_policy(), policy(5202));
    writer.finish().unwrap();
    let active = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();

    let mut writer = SearchOutOfCoreGenerationWriter::create_with_term_policy(
        &root,
        Default::default(),
        policy(5202),
    )
    .unwrap();
    writer.push(term_document(0, &term)).unwrap();
    writer.set_lexical_term_policy(policy(4096));
    assert!(writer
        .finish()
        .unwrap_err()
        .to_string()
        .contains("exceeding 4096"));
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        active
    );
    assert_eq!(stage_directories(&root), 0);
    assert_eq!(open(&root, 5202).unwrap().document_count(), 1);
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn term_policy_reader_changes_are_atomic_and_do_not_require_writer_cap() {
    let root = test_dir("term_policy_reader_dynamic");
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_term_policy(
        &root,
        Default::default(),
        policy(8192),
    )
    .unwrap();
    writer.push(term_document(0, "short")).unwrap();
    writer.finish().unwrap();
    let mut reader = SearchOutOfCoreReader::open(&root).unwrap();
    let long_query = "x".repeat(5202);
    assert!(search(&reader, &long_query)
        .unwrap_err()
        .to_string()
        .contains("exceeding 4096"));
    reader.set_lexical_term_policy(policy(5202)).unwrap();
    assert!(search(&reader, &long_query).unwrap().result.hits.is_empty());
    reader.set_lexical_term_policy(policy(5)).unwrap();
    assert_eq!(search(&reader, "short").unwrap().result.hits.len(), 1);
    assert!(reader.set_lexical_term_policy(policy(4)).is_err());
    assert_eq!(reader.lexical_term_policy(), policy(5));
    assert_eq!(search(&reader, "short").unwrap().result.hits.len(), 1);
    assert!(search(&reader, "longer").is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn term_policy_delta_uses_prepare_snapshot_and_preserves_failed_generation() {
    let root = test_dir("term_policy_delta_snapshot");
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    writer.push(term_document(0, "short")).unwrap();
    writer.finish().unwrap();
    let mut reader = SearchOutOfCoreReader::open(&root).unwrap();
    reader.set_lexical_term_policy(policy(5202)).unwrap();
    let delta = |bytes| SearchProjectionDelta {
        upserts: vec![SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "000001".into(),
            title: String::new(),
            body: "x".repeat(bytes),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::new(),
        }],
        deletes: vec!["memory:000000".into()],
        max_operations: None,
        source_graph_commit_epoch: None,
    };
    let update =
        SearchOutOfCoreGenerationWriter::prepare_delta(&reader, delta(5202), Default::default())
            .unwrap();
    reader.set_lexical_term_policy(policy(4096)).unwrap();
    let (_, report, _) = update.finish().unwrap();
    let reader = open(&root, 5202).unwrap();
    assert_eq!(reader.generation(), report.generation);
    assert_eq!(reader.document_count(), 1);
    #[cfg(feature = "full-text-search")]
    assert_eq!(
        search(&reader, &"x".repeat(5202))
            .unwrap()
            .result
            .hits
            .len(),
        1
    );
    let active = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let bad_delta = SearchProjectionDelta {
        deletes: Vec::new(),
        ..delta(5203)
    };
    let failed =
        SearchOutOfCoreGenerationWriter::prepare_delta(&reader, bad_delta, Default::default())
            .unwrap();
    assert!(failed
        .finish()
        .unwrap_err()
        .to_string()
        .contains("exceeding 5202"));
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        active
    );
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn term_policy_long_terms_cross_multiple_spill_runs() {
    let root = test_dir("term_policy_multi_run");
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_term_policy(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            lexical_build_memory_bytes: NonZeroU64::new(64 * 1024).unwrap(),
            lexical_max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
            ..Default::default()
        },
        policy(5202),
    )
    .unwrap();
    let term = "x".repeat(5202);
    for index in 0..64 {
        writer.push(term_document(index, &term)).unwrap();
    }
    writer.finish().unwrap();
    let reader = open(&root, 5202).unwrap();
    assert_eq!(reader.document_count(), 64);
    #[cfg(feature = "full-text-search")]
    {
        let result = reader
            .search_with_options(&term, None, SearchMode::Text, query_options(64))
            .unwrap();
        assert_eq!(result.result.hits.len(), 64);
        assert!(result
            .result
            .hits
            .windows(2)
            .all(|hits| hits[0].score == hits[1].score));
    }
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn term_policy_spilled_document_frequencies_preserve_exact_artifact() {
    let roots = [
        test_dir("long_frequency_resident"),
        test_dir("long_frequency_spilled"),
    ];
    let terms = ('a'..='x')
        .map(|ch| ch.to_string().repeat(5202))
        .collect::<Vec<_>>();
    let source = terms.join(" ");
    for (root, memory) in roots.iter().zip([32 * 1024 * 1024, 128 * 1024]) {
        let mut writer = SearchOutOfCoreGenerationWriter::create_with_term_policy(
            root,
            SearchOutOfCoreGenerationBuildOptions {
                lexical_build_memory_bytes: NonZeroU64::new(memory).unwrap(),
                ..Default::default()
            },
            policy(11000),
        )
        .unwrap();
        writer.push(term_document(0, &source)).unwrap();
        writer.finish().unwrap();
        let reader = open(root, 11000).unwrap();
        for term in &terms {
            assert_eq!(search(&reader, term).unwrap().result.hits.len(), 1);
        }
        assert_eq!(stage_directories(root), 0);
    }
    let artifact = crate::lexical_projection::artifact_file(1);
    assert_eq!(
        fs::read(roots[0].join(&artifact)).unwrap(),
        fs::read(roots[1].join(&artifact)).unwrap()
    );
    for root in roots {
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
#[ignore = "explicit local seeded generation-lifecycle campaign"]
fn term_policy_lifecycle_differential_campaign() {
    let mut seed = 0x3250_5202_4096_0001u64;
    for case in 0..128 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let limit = [4096, 5202, 8192][case % 3];
        let bytes = match case % 4 {
            0 => limit - 1,
            1 => limit,
            2 => limit + 1,
            _ => 1 + seed % 8500,
        } as usize;
        let root = test_dir("term_policy_lifecycle_fuzz");
        let term = if seed & 1 == 0 {
            "x".repeat(bytes)
        } else {
            format!("{}{}", "é".repeat(bytes / 2), "x".repeat(bytes % 2))
        };
        let count = 1 + (seed as usize % 4);
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        for id in 0..count {
            writer.push(term_document(id, &term)).unwrap();
        }
        writer.set_lexical_term_policy(policy(limit));
        let result = writer.finish();
        if bytes as u64 > limit {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("lexical term uses"),
                "case {case}"
            );
            assert!(!root.join(OUT_OF_CORE_MANIFEST_FILE).exists());
        } else {
            result.unwrap();
            let mut reader = open(&root, limit).unwrap();
            assert_eq!(reader.document_count(), count);
            assert_eq!(SearchOutOfCoreReader::open(&root).is_ok(), bytes <= 4096);
            if bytes > 1 {
                assert!(reader
                    .set_lexical_term_policy(policy(bytes as u64 - 1))
                    .is_err());
                assert_eq!(reader.lexical_term_policy(), policy(limit));
            }
            reader
                .set_lexical_term_policy(policy(bytes as u64))
                .unwrap();
            #[cfg(feature = "full-text-search")]
            {
                let result = search(&reader, &term).unwrap();
                assert_eq!(result.result.hits.len(), count, "case {case}");
                assert!(search(&reader, &"z".repeat(bytes + 1)).is_err());
            }
            let ids = (0..count)
                .map(|id| format!("memory:{id:06}"))
                .collect::<Vec<_>>();
            let documents = reader.hydrate_documents(&ids).unwrap().documents;
            assert_eq!(documents.len(), count);
            assert!(documents.iter().all(|document| document.content == term));
            drop(reader);
        }
        assert_eq!(stage_directories(&root), 0);
        fs::remove_dir_all(root).unwrap();
    }
}
