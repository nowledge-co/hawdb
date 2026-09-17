//! External embedded-library contract: imports only the facade and std.

use skein::{
    Database, RuntimeCancellationToken, RuntimeMemoryReservation, RuntimeTaskContext,
    SearchAnalyzerLexicon, SearchDocument, SearchEmbeddingManifest, SearchLexicalTermPolicy,
    SearchMode, SearchOutOfCoreConfig, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationBuildReport, SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader,
    SearchProjectionConsumerId, SearchProjectionConsumerOptions, SearchProjectionDelta,
    SearchProjectionKind, SearchProjectionRow, SearchQueryOptions, SearchRebuildOptions,
};
use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        Self(std::env::temp_dir().join(format!(
            "skein-facade-generation-{}-{:06}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        match std::fs::remove_dir_all(&self.0) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("cannot remove generation fixture: {error}"),
        }
    }
}

fn context(bytes: u64) -> RuntimeTaskContext {
    RuntimeTaskContext::default().with_memory_reservation(RuntimeMemoryReservation::new(bytes, 0))
}

fn admitted() -> RuntimeTaskContext {
    context(128 * 1024 * 1024)
}

fn identity() -> SearchEmbeddingManifest {
    SearchEmbeddingManifest {
        model: "facade-model".into(),
        version: Some("v1".into()),
        dimension: 4,
    }
}

fn options() -> SearchOutOfCoreGenerationBuildOptions {
    SearchOutOfCoreGenerationBuildOptions {
        source_graph_commit_epoch: Some(101),
        import_source_graph_commit_epoch: Some(77),
        embedding_manifest: Some(identity()),
        lexical_build_memory_bytes: NonZeroU64::new(128 * 1024).unwrap(),
        ..Default::default()
    }
}

fn update_options() -> SearchOutOfCoreGenerationBuildOptions {
    SearchOutOfCoreGenerationBuildOptions {
        source_graph_commit_epoch: None,
        import_source_graph_commit_epoch: None,
        embedding_manifest: None,
        ..options()
    }
}

fn row(id: &str, epoch: u64) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: id.into(),
        title: format!("Graph \u{39f}\u{3a3} \u{77e5}\u{8b58} {id}"),
        body: format!("moving HTTPServer42 studies epoch{epoch}"),
        embedding: Some(vec![1.0, 0.25, epoch as f32 * 0.01, -0.5]),
        source_id: Some("source-owner".into()),
        metadata: BTreeMap::from([("kind".into(), "overwritten".into())]),
    }
}

fn delta(id: &str, epoch: u64) -> SearchProjectionDelta {
    SearchProjectionDelta {
        upserts: vec![row(id, epoch)],
        source_graph_commit_epoch: Some(epoch),
        ..Default::default()
    }
}

fn seed(root: &Path) -> SearchOutOfCoreGenerationBuildReport {
    let mut writer = SearchOutOfCoreGenerationWriter::create(root, options()).unwrap();
    writer.push(row("a", 101).into_document()).unwrap();
    writer.finish().unwrap()
}

fn files(root: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            assert!(
                entry.file_type().unwrap().is_file(),
                "unreleased private stage"
            );
            (
                entry.file_name().into_string().unwrap(),
                std::fs::read(entry.path()).unwrap(),
            )
        })
        .collect()
}

fn verify(root: &Path, expected: &BTreeMap<String, SearchDocument>, epoch: u64) -> String {
    let reader = SearchOutOfCoreReader::open(root).unwrap();
    assert_eq!(reader.document_count(), expected.len());
    assert_eq!(reader.source_graph_commit_epoch(), Some(epoch));
    assert_eq!(reader.import_source_graph_commit_epoch(), Some(77));
    assert_eq!(reader.embedding_manifest(), Some(identity()));
    let ids = expected.keys().cloned().collect::<Vec<_>>();
    assert_eq!(
        reader.hydrate_documents(&ids).unwrap().documents,
        expected.values().cloned().collect::<Vec<_>>()
    );
    let result = reader.search_with_options(
        "moving",
        None,
        SearchMode::Text,
        SearchQueryOptions {
            limit: 100,
            offset: 0,
            rank_window: None,
            fusion_weights: Default::default(),
            metadata_filters: BTreeMap::new(),
            policy_epoch: None,
        },
    );
    if cfg!(feature = "full-text-search") {
        let result = result.unwrap();
        assert_eq!(result.result.hits.len(), expected.len());
        format!("{:?}", result.result)
    } else {
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            skein::SkeinError::CapabilityUnavailable {
                capability: skein::RuntimeCapability::FullTextSearch
            }
        ));
        error.to_string()
    }
}

#[test]
fn governed_facade_matches_default_build_update_delete_and_complete_results() {
    let ordinary = Directory::new();
    let governed = Directory::new();
    let mut baseline = SearchOutOfCoreGenerationWriter::create(&ordinary.0, options()).unwrap();
    let mut writer =
        SearchOutOfCoreGenerationWriter::create_with_context(&governed.0, options(), admitted())
            .unwrap();
    assert_eq!(writer.max_lexical_manifest_bytes().get(), 256 * 1024 * 1024);
    assert_eq!(
        writer.lexical_term_policy(),
        SearchLexicalTermPolicy::default()
    );
    let mut expected = BTreeMap::new();
    for id in ["a", "c", "e"] {
        let document = row(id, 101).into_document();
        baseline.push(document.clone()).unwrap();
        writer.push(document.clone()).unwrap();
        expected.insert(document.id.clone(), document);
    }
    assert_eq!(writer.finish().unwrap(), baseline.finish().unwrap());
    assert_eq!(files(&governed.0), files(&ordinary.0));
    assert_eq!(
        verify(&governed.0, &expected, 101),
        verify(&ordinary.0, &expected, 101)
    );
    for epoch in 102..=104 {
        let added = format!("new-{epoch}");
        let mut input = delta(&added, epoch);
        input.upserts.push(row("c", epoch));
        input.deletes = vec!["memory:e".into(), "memory:missing".into()];
        for row in &input.upserts {
            let document = row.clone().into_document();
            expected.insert(document.id.clone(), document);
        }
        expected.remove("memory:e");
        let reader = SearchOutOfCoreReader::open(&ordinary.0).unwrap();
        let baseline = SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            input.clone(),
            update_options(),
        )
        .unwrap();
        let reader = SearchOutOfCoreReader::open(&governed.0).unwrap();
        let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
            &reader,
            input,
            update_options(),
            admitted(),
        )
        .unwrap();
        assert_eq!(
            format!("{:?}", update.delta_report()),
            format!("{:?}", baseline.delta_report())
        );
        drop(reader);
        assert_eq!(
            format!("{:?}", update.finish().unwrap()),
            format!("{:?}", baseline.finish().unwrap())
        );
        assert_eq!(files(&governed.0), files(&ordinary.0));
        assert_eq!(
            verify(&governed.0, &expected, epoch),
            verify(&ordinary.0, &expected, epoch)
        );
    }
}

#[test]
fn stopped_or_unadmitted_context_rejects_both_facade_entrypoints() {
    let cancellation = RuntimeCancellationToken::new();
    cancellation.cancel();
    let base = Directory::new();
    seed(&base.0);
    let reader = SearchOutOfCoreReader::open(&base.0).unwrap();
    let before = files(&base.0);
    for task in [
        RuntimeTaskContext::without_deadline(cancellation),
        RuntimeTaskContext::new(Default::default(), Some(Instant::now())),
        context(0),
        context(1),
    ] {
        let root = Directory::new();
        assert!(SearchOutOfCoreGenerationWriter::create_with_context(
            &root.0,
            options(),
            task.clone()
        )
        .is_err());
        assert!(!root.0.exists());
        assert!(SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
            &reader,
            delta("b", 102),
            update_options(),
            task
        )
        .is_err());
        assert_eq!(files(&base.0), before);
    }
}

#[test]
fn cancellation_and_input_poison_keep_the_active_generation() {
    let root = Directory::new();
    seed(&root.0);
    let before = files(&root.0);
    let expected = BTreeMap::from([("memory:a".into(), row("a", 101).into_document())]);
    let task = admitted();
    let mut writer =
        SearchOutOfCoreGenerationWriter::create_with_context(&root.0, options(), task.child())
            .unwrap();
    writer.push(row("b", 102).into_document()).unwrap();
    task.cancellation().cancel();
    assert!(writer.finish().unwrap_err().to_string().contains("cancel"));
    assert_eq!(files(&root.0), before);

    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let task = admitted();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        delta("b", 102),
        update_options(),
        task.child(),
    )
    .unwrap();
    task.cancellation().cancel();
    assert!(update.finish().unwrap_err().to_string().contains("cancel"));
    assert_eq!(files(&root.0), before);

    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root.0,
        options(),
        context(2 * 1024 * 1024),
    )
    .unwrap();
    let mut document = row("b", 102).into_document();
    document.content.reserve_exact(4 * 1024 * 1024);
    assert!(writer.push(document).is_err());
    assert!(writer
        .push(row("c", 102).into_document())
        .unwrap_err()
        .to_string()
        .contains("poisoned"));
    assert!(writer
        .finish()
        .unwrap_err()
        .to_string()
        .contains("poisoned"));
    assert_eq!(files(&root.0), before);
    verify(&root.0, &expected, 101);
}

#[test]
fn abandoned_and_unwinding_facade_owners_discard_the_private_stage() {
    let root = Directory::new();
    seed(&root.0);
    let before = files(&root.0);
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    for unwind in [false, true] {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
                &root.0,
                options(),
                admitted(),
            )
            .unwrap();
            writer.push(row("b", 102).into_document()).unwrap();
            let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
                &reader,
                delta("b", 102),
                update_options(),
                admitted(),
            )
            .unwrap();
            if unwind {
                panic!("external caller unwind");
            }
            drop(update);
            drop(writer);
        }));
        assert_eq!(result.is_err(), unwind);
        assert_eq!(files(&root.0), before);
    }
}

#[test]
fn stale_prepared_update_cannot_replace_a_newer_generation() {
    let root = Directory::new();
    seed(&root.0);
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let stale = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        delta("stale", 103),
        update_options(),
        admitted(),
    )
    .unwrap();
    let fresh = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        delta("fresh", 102),
        update_options(),
        admitted(),
    )
    .unwrap();
    fresh.finish().unwrap();
    // The stale update legitimately still owns its private stage here.
    let active =
        std::fs::read(root.0.join("search_projection.out_of_core.manifest.skein")).unwrap();
    assert!(stale
        .finish()
        .unwrap_err()
        .to_string()
        .contains("base changed"));
    assert_eq!(
        std::fs::read(root.0.join("search_projection.out_of_core.manifest.skein")).unwrap(),
        active
    );
    files(&root.0);
    let expected = [row("a", 101), row("fresh", 102)]
        .into_iter()
        .map(|row| {
            let document = row.into_document();
            (document.id.clone(), document)
        })
        .collect();
    verify(&root.0, &expected, 102);
}

#[test]
fn facade_rejects_incompatible_identity_without_publishing() {
    let root = Directory::new();
    seed(&root.0);
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let before = files(&root.0);
    for variant in 0..4 {
        let mut options = update_options();
        match variant {
            0 => options.source_graph_commit_epoch = Some(999),
            1 => options.import_source_graph_commit_epoch = Some(999),
            2 => {
                options.embedding_manifest = Some(SearchEmbeddingManifest {
                    model: "different".into(),
                    ..identity()
                })
            }
            3 => options.analyzer_lexicon = SearchAnalyzerLexicon::empty(),
            _ => unreachable!(),
        }
        assert!(SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
            &reader,
            delta("b", 102),
            options,
            admitted()
        )
        .is_err());
        assert_eq!(files(&root.0), before);
    }
}

#[test]
fn facade_captures_reader_term_and_manifest_admission() {
    let root = Directory::new();
    let initial = seed(&root.0);
    let before = files(&root.0);
    let reader = SearchOutOfCoreReader::open_with_config(
        &root.0,
        SearchOutOfCoreConfig {
            max_lexical_manifest_bytes: NonZeroU64::new(initial.lexical_manifest_bytes).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    let mut input = delta("a", 102);
    input.upserts[0].body = (0..100).map(|i| format!("term{i:04} ")).collect();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        input,
        update_options(),
        admitted(),
    )
    .unwrap();
    assert!(update
        .finish()
        .unwrap_err()
        .to_string()
        .contains("manifest requires"));
    assert_eq!(files(&root.0), before);

    let policy = SearchLexicalTermPolicy::new(NonZeroU64::new(8192).unwrap()).unwrap();
    let mut reader = SearchOutOfCoreReader::open_with_term_policy(
        &root.0,
        SearchOutOfCoreConfig::default(),
        SearchAnalyzerLexicon::default(),
        policy,
    )
    .unwrap();
    let mut input = delta("a", 102);
    input.upserts[0].body = "x".repeat(5202);
    let expected = input.upserts[0].clone().into_document();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        input,
        update_options(),
        admitted(),
    )
    .unwrap();
    reader
        .set_lexical_term_policy(SearchLexicalTermPolicy::default())
        .unwrap();
    drop(reader);
    let (report, built, _) = update.finish().unwrap();
    assert_eq!(report.source_graph_commit_epoch_after, Some(102));
    assert_eq!(built.document_count, 1);
    assert!(SearchOutOfCoreReader::open(&root.0).is_err());
    let reader = SearchOutOfCoreReader::open_with_term_policy(
        &root.0,
        SearchOutOfCoreConfig::default(),
        SearchAnalyzerLexicon::default(),
        policy,
    )
    .unwrap();
    assert_eq!(
        reader
            .hydrate_documents(std::slice::from_ref(&expected.id))
            .unwrap()
            .documents,
        vec![expected]
    );
    files(&root.0);
}

#[test]
fn governed_writer_preserves_registered_consumer_and_releases_publication() {
    let root = Directory::new();
    std::fs::create_dir(&root.0).unwrap();
    let projection = root.0.join("projection");
    let mut db = Database::open(root.0.join("database")).unwrap();
    db.query("CREATE (:Memory {id: 'owned', title: 'consumer document'})")
        .unwrap();
    let id = SearchProjectionConsumerId::new("generation-owner").unwrap();
    let consumer = db
        .create_search_projection_consumer(
            id.clone(),
            &projection,
            SearchProjectionConsumerOptions::new(NonZeroU64::new(100).unwrap()),
            |snapshot, index| {
                snapshot
                    .rebuild_search_projection(index, SearchRebuildOptions::default())
                    .map(|_| ())
            },
        )
        .unwrap();
    let before = files(&projection);
    let rejected_publication = |task| {
        let mut writer =
            SearchOutOfCoreGenerationWriter::create_with_context(&projection, options(), task)
                .unwrap();
        writer.push(row("outside", 101).into_document()).unwrap();
        writer.finish().unwrap_err()
    };
    let error = rejected_publication(admitted());
    assert!(
        error
            .to_string()
            .contains("another search projection publication is active"),
        "unexpected live-owner failure: {error}"
    );
    assert_eq!(files(&projection), before);
    drop(consumer);

    for task in [RuntimeTaskContext::default(), admitted()] {
        let error = rejected_publication(task);
        assert!(
            error
                .to_string()
                .contains("registered projection requires its consumer owner"),
            "unexpected publication failure: {error}"
        );
        assert_eq!(files(&projection), before);
    }

    // A real consumer checkpoint must still acquire publication after rejection.
    let mut consumer = db
        .open_search_projection_consumer(&id, &projection)
        .unwrap();
    db.query("CREATE (:Memory {id: 'next', title: 'next consumer document'})")
        .unwrap();
    let report = db
        .catch_up_search_projection_consumer(&mut consumer, 16, 16, 1, |_, batch| {
            assert!(batch.relational_primary_key_changes().is_empty());
            Ok(Default::default())
        })
        .unwrap();
    assert!(report.catch_up.complete);
    assert_eq!(report.catch_up.applied_batch_count, 1);
    assert!(consumer.search_index().document("memory:owned").is_some());
    assert!(consumer.search_index().document("memory:next").is_some());
    assert!(consumer.search_index().document("memory:outside").is_none());
    files(&projection);
}
