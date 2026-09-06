use super::*;

fn generated(mut seed: u64) -> SearchOutOfCoreGenerationBuildOptions {
    let mut next = || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        seed
    };
    let count = next() as usize % 7;
    let mut rules = Vec::with_capacity(count + next() as usize % 17);
    for _ in 0..count {
        let mut inputs = Vec::with_capacity(1 + next() as usize % 9);
        let mut aliases = Vec::with_capacity(1 + next() as usize % 9);
        for words in [&mut inputs, &mut aliases] {
            let value =
                ["word", "\u{130}", "a\0B", ""][next() as usize % 4].repeat(next() as usize % 32);
            words.push(spare(&value, value.len() + next() as usize % 4096));
        }
        rules.push(SearchAnalyzerAliasRule { inputs, aliases });
    }
    let mut stopwords = BTreeSet::new();
    for index in 0..next() as usize % 9 {
        stopwords.insert(spare(
            &format!("stop-{index}"),
            128 + next() as usize % 4096,
        ));
    }
    let identity = (next() % 3 != 0).then(|| SearchEmbeddingManifest {
        model: spare("model-\u{130}", 128 + next() as usize % 4096),
        version: (next() % 2 == 0).then(|| spare("v1\0", 128 + next() as usize % 4096)),
        dimension: 2,
    });
    SearchOutOfCoreGenerationBuildOptions {
        analyzer_lexicon: SearchAnalyzerLexicon {
            alias_rules: rules,
            stopwords,
        },
        embedding_manifest: identity,
        ..Default::default()
    }
}

#[test]
#[ignore = "local build-context ownership and identity publication campaign"]
fn context_admission_campaign() {
    let seed = 0x206c017eu64;
    let mut accepted = 0;
    let mut paths = 0;
    for case in 0..128 {
        let recipe = seed + case;
        let baseline = memory(8 * 1024 * 1024);
        let other = baseline.input.reserve(137).unwrap();
        let owned =
            Options::new(generated(recipe), &baseline, &RuntimeTaskContext::default()).unwrap();
        let peak = baseline.ledger.snapshot().peak_bytes;
        assert_eq!(*owned, generated(recipe));
        drop(owned);
        drop(other);
        assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
        for limit in [peak, peak - 1] {
            let bounded = memory(limit);
            let other = bounded.input.reserve(137).unwrap();
            let result = Options::new(generated(recipe), &bounded, &RuntimeTaskContext::default());
            assert_eq!(result.is_ok(), limit == peak, "case={case}");
            accepted += usize::from(result.is_ok());
            drop(result);
            drop(other);
            assert_eq!(bounded.ledger.snapshot().used_bytes, 0);
            assert_eq!(bounded.ledger.snapshot().account_count, 3);
        }
        let parent = PathBuf::from(format!(
            "parent/{}/./child",
            "\u{130}".repeat(case as usize % 64)
        ));
        let child = PathBuf::from(format!("file-{case}.skein"));
        let baseline = memory(1024 * 1024);
        let other = baseline.input.reserve(137).unwrap();
        let owned =
            OwnedPath::join(&parent, &child, &baseline, &RuntimeTaskContext::default()).unwrap();
        let peak = baseline.ledger.snapshot().peak_bytes;
        assert_eq!(owned.as_ref(), parent.join(&child));
        drop(owned);
        drop(other);
        assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
        for limit in [peak, peak - 1] {
            let bounded = memory(limit);
            let other = bounded.input.reserve(137).unwrap();
            let result = OwnedPath::join(&parent, &child, &bounded, &RuntimeTaskContext::default());
            assert_eq!(result.is_ok(), limit == peak);
            paths += usize::from(result.is_ok());
            drop(result);
            drop(other);
            assert_eq!(bounded.ledger.snapshot().used_bytes, 0);
        }
    }
    let mut published = 0;
    for case in 0..16 {
        let (path, reader) = fixture();
        let original = reader.embedding_manifest();
        let before =
            std::fs::read(path.join(crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE)).unwrap();
        let mut value = SearchOutOfCoreGenerationBuildOptions::default();
        if case % 2 == 0 {
            value.embedding_manifest = original.clone();
            value
                .embedding_manifest
                .as_mut()
                .unwrap()
                .model
                .reserve_exact(case * 128);
        }
        let update =
            super::super::super::SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
                &reader,
                crate::SearchProjectionDelta {
                    source_graph_commit_epoch: Some(case as u64 + 1),
                    ..Default::default()
                },
                value,
                task(16 * 1024 * 1024),
            )
            .unwrap();
        assert_eq!(
            std::fs::read(path.join(crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
            before
        );
        let ledger = update.writer_for_test().memory.ledger.clone();
        assert_eq!(
            update.writer_for_test().options.embedding_manifest,
            original
        );
        assert!(ledger.snapshot().used_bytes > 0);
        update.finish().unwrap();
        assert_eq!(ledger.snapshot().used_bytes, 0);
        drop(reader);
        let reopened = SearchOutOfCoreReader::open(&path).unwrap();
        assert_eq!(reopened.embedding_manifest(), original);
        assert_eq!(reopened.source_graph_commit_epoch(), Some(case as u64 + 1));
        assert_eq!(reopened.document_count(), 1);
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
        published += 1;
    }
    assert_eq!((accepted, paths, published), (128, 128, 16));
    eprintln!("context seed=0x206c017e options=128 option_exact=128 option_short=128 paths=128 path_exact=128 path_short=128 published={published}");
}
