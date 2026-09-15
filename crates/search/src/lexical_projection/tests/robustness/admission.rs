use super::*;

#[test]
fn rejected_upserts_preserve_the_shared_delta_allocation() {
    let fixture = Fixture::new("upsert-cow-admission");
    let config = LexicalProjectionConfig::default();
    let mut active = Arc::new(LexicalMiniDelta::default());
    let inserted = document("c", "graph", "index");
    active
        .upsert(&inserted, None, &fixture.analyzer, config)
        .unwrap();
    let mut documents = fixture.documents.clone();
    documents.insert(inserted.id.clone(), inserted.clone());
    let snapshot = Arc::clone(&active);
    let short = LexicalProjectionConfig {
        mini_delta_bytes: NonZeroU64::new(active.resident_bytes - 1).unwrap(),
        ..config
    };
    let source_limit = LexicalProjectionConfig {
        max_document_source_bytes: NonZeroU64::new(1).unwrap(),
        ..config
    };
    for (next, limits) in [
        (inserted.clone(), short),
        (document("d", "graph", "new"), short),
        (document("c", "graph", &"x".repeat(4097)), config),
        (inserted, source_limit),
    ] {
        assert!(active
            .upsert(&next, None, &fixture.analyzer, limits)
            .is_err());
        assert!(
            Arc::ptr_eq(&active, &snapshot),
            "rejected upsert detached the delta"
        );
        assert_delta_snapshot(&fixture, &active, &documents);
        assert_delta_snapshot(&fixture, &snapshot, &documents);
    }
}

#[test]
fn rejected_deletes_preserve_the_shared_delta_allocation() {
    let fixture = Fixture::new("delete-cow-admission");
    let config = LexicalProjectionConfig::default();
    let mut active = Arc::new(LexicalMiniDelta::default());
    let inserted = document("c", "graph", "index");
    active
        .upsert(&inserted, None, &fixture.analyzer, config)
        .unwrap();
    let mut documents = fixture.documents.clone();
    documents.insert(inserted.id.clone(), inserted);
    let snapshot = Arc::clone(&active);
    let short = LexicalProjectionConfig {
        mini_delta_bytes: NonZeroU64::new(active.resident_bytes).unwrap(),
        ..config
    };
    let invalid = document("a", "graph", &"x".repeat(4097));
    for (previous, limits, expected_error) in [
        (&fixture.documents["a"], short, false),
        (&invalid, config, true),
        (
            &fixture.documents["a"],
            LexicalProjectionConfig {
                max_document_source_bytes: NonZeroU64::new(1).unwrap(),
                ..config
            },
            true,
        ),
    ] {
        let result = active.delete("a", Some(previous), &fixture.analyzer, limits);
        if expected_error {
            assert!(result.is_err());
        } else {
            assert!(!result.unwrap());
        }
        assert!(
            Arc::ptr_eq(&active, &snapshot),
            "rejected delete detached the delta"
        );
        assert_delta_snapshot(&fixture, &active, &documents);
        assert_delta_snapshot(&fixture, &snapshot, &documents);
    }
}

#[test]
fn no_op_deletes_preserve_the_shared_delta_allocation() {
    let fixture = Fixture::new("no-op-cow-admission");
    let config = LexicalProjectionConfig::default();
    let mut active = Arc::new(LexicalMiniDelta::default());
    let empty = Arc::clone(&active);
    assert!(active
        .delete("missing", None, &fixture.analyzer, config)
        .unwrap());
    assert!(Arc::ptr_eq(&active, &empty));
    assert!(active
        .delete(
            "a",
            Some(&fixture.documents["a"]),
            &fixture.analyzer,
            config
        )
        .unwrap());
    assert!(!Arc::ptr_eq(&active, &empty));
    let snapshot = Arc::clone(&active);
    let short = LexicalProjectionConfig {
        mini_delta_bytes: NonZeroU64::new(1).unwrap(),
        ..config
    };
    assert!(active.delete("a", None, &fixture.analyzer, short).unwrap());
    assert!(Arc::ptr_eq(&active, &snapshot));
    let mut documents = fixture.documents.clone();
    documents.remove("a");
    assert_delta_snapshot(&fixture, &active, &documents);
    assert_delta_snapshot(&fixture, &empty, &fixture.documents);
}

#[test]
fn deleting_an_inserted_document_detaches_only_the_active_snapshot() {
    let fixture = Fixture::new("insert-delete-cow");
    let config = LexicalProjectionConfig::default();
    let mut active = Arc::new(LexicalMiniDelta::default());
    let inserted = document("c", "graph", "index");
    active
        .upsert(&inserted, None, &fixture.analyzer, config)
        .unwrap();
    let mut documents = fixture.documents.clone();
    documents.insert(inserted.id.clone(), inserted);
    let snapshot = Arc::clone(&active);
    assert!(active.delete("c", None, &fixture.analyzer, config).unwrap());
    assert!(!Arc::ptr_eq(&active, &snapshot));
    assert_delta_snapshot(&fixture, &active, &fixture.documents);
    assert_delta_snapshot(&fixture, &snapshot, &documents);
}

#[cfg(feature = "full-text-search")]
#[test]
fn rejected_index_writes_invalidate_current_projection_but_preserve_old_queries() {
    use crate::{SearchIndex, SearchMode, SearchQueryOptions};

    let search = |index: &SearchIndex| {
        index
            .try_search_with_options(
                "graph",
                None,
                SearchMode::Text,
                SearchQueryOptions {
                    limit: 10,
                    offset: 0,
                    rank_window: None,
                    fusion_weights: Default::default(),
                    metadata_filters: BTreeMap::new(),
                    policy_epoch: None,
                },
            )
            .unwrap()
    };

    for delete in [false, true] {
        let root = projection_root(if delete {
            "index-delete-admission"
        } else {
            "index-upsert-admission"
        });
        let mut index = SearchIndex::open(&root).unwrap();
        index.upsert(document("a", "graph", "storage")).unwrap();
        index.upsert(document("b", "graph", "memory")).unwrap();
        index.checkpoint().unwrap();
        index.upsert(document("c", "graph", "index")).unwrap();
        assert!(search(&index)
            .retrievers
            .iter()
            .any(|retriever| retriever.segmented_lexical_projection_used));
        let (reader, snapshot) = index.lexical_snapshot().unwrap();
        let terms = BTreeSet::from(["graph".to_string()]);
        let before = reader.score(&terms, &snapshot, None, |_| Ok(true)).unwrap();
        assert_eq!(before.matching_document_count, 3);
        index.lexical_config.mini_delta_bytes = NonZeroU64::new(1).unwrap();
        if delete {
            index.delete("a");
        } else {
            index.upsert(document("d", "graph", "new")).unwrap();
        }
        assert!(index.lexical_snapshot().is_none());
        assert_eq!(
            reader.score(&terms, &snapshot, None, |_| Ok(true)).unwrap(),
            before
        );
        let report = search(&index);
        assert_eq!(report.total_hits, if delete { 2 } else { 4 });
        assert!(!report
            .retrievers
            .iter()
            .any(|retriever| retriever.segmented_lexical_projection_used));
        let expected = index.documents.keys().cloned().collect::<BTreeSet<_>>();
        assert_eq!(
            report
                .hits
                .iter()
                .map(|hit| hit.id.clone())
                .collect::<BTreeSet<_>>(),
            expected
        );
        index.checkpoint().unwrap();
        drop(index);
        let reopened = SearchIndex::open(&root).unwrap();
        let actual = search(&reopened);
        assert_eq!(actual.hits, report.hits);
        assert_eq!(actual.total_hits, report.total_hits);
        assert!(actual
            .retrievers
            .iter()
            .any(|retriever| retriever.segmented_lexical_projection_used));
        drop(reopened);
        drop(reader);
        drop(snapshot);
        fs::remove_dir_all(root).unwrap();
    }
}
