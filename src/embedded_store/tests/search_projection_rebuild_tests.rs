// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded rebuild of an incremental search projection from the graph (#1738).

use super::*;
use crate::search::{
    SearchDocumentSetDigest, SearchEmbeddingManifest, SearchOutOfCoreGenerationWriter,
    SearchProjectionKind, SearchProjectionRow, SearchProjectionScanBudget, SearchRebuildOptions,
};
use crate::{
    DatabaseReadTransaction, GraphOnlySearchProjectionRebuild, IncrementalSearchMaintenanceOptions,
    Result, RuntimeCancellationToken, RuntimeTaskContext, SearchProjectionCatchUpReport,
    SearchProjectionRebuildOptions, SearchProjectionRebuildPhase, SearchProjectionRebuildProgress,
    SearchProjectionRebuildReport, SearchProjectionRebuildSource,
};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};

const DIMENSION: usize = 8;
const MANIFEST: &str = "search_projection.out_of_core.manifest.hawdb";

fn embedding_for(id: &str) -> Vec<f32> {
    let mut state = id
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        })
        .max(1);
    (0..DIMENSION)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 40) as f32 / (1u64 << 23) as f32 - 0.6
        })
        .collect()
}

fn manifest() -> SearchEmbeddingManifest {
    SearchEmbeddingManifest {
        model: "rebuild-test".into(),
        version: None,
        dimension: DIMENSION,
    }
}

fn maintenance() -> IncrementalSearchMaintenanceOptions {
    let mut options = IncrementalSearchMaintenanceOptions::default();
    options.build_options.embedding_manifest = Some(manifest());
    options
}

fn rebuild_options() -> SearchProjectionRebuildOptions {
    SearchProjectionRebuildOptions {
        maintenance: maintenance(),
        hydration_batch_rows: NonZeroUsize::new(16).unwrap(),
        ..Default::default()
    }
}

fn host_row() -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Source,
        external_id: "host-1".into(),
        title: "host relational source".into(),
        body: "rebuild storage evidence from the host".into(),
        embedding: Some(embedding_for("source:host-1")),
        source_id: None,
        metadata: BTreeMap::from([("space_id".into(), "default".into())]),
    }
}

type ProgressObserver = Box<dyn FnMut(&SearchProjectionRebuildProgress)>;

/// Attaches deterministic external embeddings and one host-owned row, the
/// way a host's catch-up hydrator would. Optional actions observe phases.
#[derive(Default)]
struct HostSource {
    phases: Vec<SearchProjectionRebuildPhase>,
    hydrated_batches: usize,
    on_first_batch: Option<Box<dyn FnOnce()>>,
    on_progress: Option<ProgressObserver>,
}

impl SearchProjectionRebuildSource for HostSource {
    fn hydrate_graph_rows(
        &mut self,
        _snapshot: &mut DatabaseReadTransaction,
        rows: &mut Vec<SearchProjectionRow>,
    ) -> Result<()> {
        self.hydrated_batches += 1;
        if let Some(action) = self.on_first_batch.take() {
            action();
        }
        for row in rows {
            let id = format!("{}:{}", row.kind.as_str(), row.external_id);
            row.embedding = Some(embedding_for(&id));
        }
        Ok(())
    }

    fn visit_host_rows(
        &mut self,
        _snapshot: &mut DatabaseReadTransaction,
        emit: &mut dyn FnMut(SearchProjectionRow) -> Result<()>,
    ) -> Result<()> {
        emit(host_row())
    }

    fn progress(&mut self, progress: &SearchProjectionRebuildProgress) {
        if self.phases.last() != Some(&progress.phase) {
            self.phases.push(progress.phase);
        }
        if let Some(observe) = self.on_progress.as_mut() {
            observe(progress);
        }
    }
}

fn open_options(root: &Path, search: &str, config: &DatabaseConfig) -> EmbeddedStoreOpenOptions {
    EmbeddedStoreOpenOptions::with_search_projection(
        root.join("graph"),
        root.join(search),
        GraphMode::WritableCutover,
    )
    .with_database_config(config.clone())
    .with_incremental_search_maintenance(maintenance())
}

fn small_change_log_config() -> DatabaseConfig {
    DatabaseConfig {
        max_search_projection_change_log_entries: Some(2),
        ..DatabaseConfig::default()
    }
}

/// An old library: a bootstrapped incremental projection that never caught
/// up, followed by more graph commits than the change log retains.
fn old_library(name: &str, memories: usize) -> (PathBuf, EmbeddedStoreHandle, DatabaseConfig) {
    let root = unique_embedded_store_test_dir(name);
    let search_path = root.join("search");
    let mut index = SearchIndex::open(&search_path).unwrap();
    index
        .upsert_projection_row(SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "stale".into(),
            title: "stale".into(),
            body: "stale bootstrap".into(),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();
    index.checkpoint().unwrap();
    drop(index);
    let config = small_change_log_config();
    let (handle, _) =
        EmbeddedStoreHandle::open_with_options(open_options(&root, "search", &config)).unwrap();
    handle
        .query_with_report("CREATE (:Label {name: 'alpha'})")
        .unwrap();
    handle
        .query_with_report("CREATE (:Label {name: 'beta'})")
        .unwrap();
    for chunk in 0..memories.div_ceil(16) {
        handle
            .with_transaction(|transaction| {
                for number in chunk * 16..((chunk + 1) * 16).min(memories) {
                    let topic = if number % 3 == 0 {
                        "storage rebuild"
                    } else {
                        "garden notes"
                    };
                    transaction.query(&format!(
                        "CREATE (:Memory {{id: 'm{number:04}', title: 'memory {number}', \
                         content: '{topic} entry {number}', space_id: 'default'}})"
                    ))?;
                }
                Ok(())
            })
            .unwrap();
    }
    handle
        .with_transaction(|transaction| {
            for number in (0..memories).filter(|number| number % 10 == 0) {
                transaction.query(&format!(
                    "MATCH (m:Memory {{id: 'm{number:04}'}}), (l:Label {{name: 'alpha'}}) \
                     CREATE (m)-[:HAS_LABEL]->(l)"
                ))?;
            }
            for number in (0..memories).filter(|number| number % 10 == 5) {
                transaction.query(&format!(
                    "MATCH (l:Label {{name: 'beta'}}), (m:Memory {{id: 'm{number:04}'}}) \
                     CREATE (l)-[:HAS_LABEL]->(m)"
                ))?;
            }
            Ok(())
        })
        .unwrap();
    handle
        .query_with_report("CREATE (:Entity {id: 'e1', name: 'storage engine'})")
        .unwrap();
    (root, handle, config)
}

fn digest(path: &Path) -> SearchDocumentSetDigest {
    let reader = crate::SearchOutOfCoreReader::open(path).unwrap();
    let mut digest = SearchDocumentSetDigest::default();
    reader
        .visit_projection_documents(
            SearchProjectionScanBudget {
                max_rows: 1 << 20,
                max_payload_bytes: 1 << 30,
                max_encoded_record_bytes: 1 << 24,
            },
            &RuntimeTaskContext::default(),
            &mut |document| {
                digest.add(document);
                Ok(())
            },
        )
        .unwrap();
    digest
}

/// An independent fresh build of the current graph: the resident full
/// rebuild's documents, given the host's embeddings except `unhydrated`
/// (written after the rebuild epoch and maintained only by graph catch-up),
/// pushed in ID order through the ordered out-of-core writer.
fn fresh_build(handle: &EmbeddedStoreHandle, path: &Path, unhydrated: &[&str]) {
    let resident_path = path.with_extension("resident");
    let mut index = SearchIndex::open(&resident_path).unwrap();
    let epoch = {
        let store = handle.read_store().unwrap();
        let database = store.graph.database();
        database
            .rebuild_search_projection(&mut index, SearchRebuildOptions::default())
            .unwrap();
        database.commit_epoch().unwrap()
    };
    let mut documents = BTreeMap::new();
    index
        .visit_projection_documents(
            SearchProjectionScanBudget {
                max_rows: 1 << 20,
                max_payload_bytes: 1 << 30,
                max_encoded_record_bytes: 1 << 24,
            },
            &RuntimeTaskContext::default(),
            &mut |document| {
                let mut document = document.clone();
                if !unhydrated.contains(&document.id.as_str()) {
                    document.embedding = Some(embedding_for(&document.id));
                }
                documents.insert(document.id.clone(), document);
                Ok(())
            },
        )
        .unwrap();
    let host = host_row().into_document();
    documents.insert(host.id.clone(), host);
    let mut options = maintenance().build_options;
    options.source_graph_commit_epoch = Some(epoch);
    let mut writer = SearchOutOfCoreGenerationWriter::create(path, options).unwrap();
    for document in documents.into_values() {
        writer.push(document).unwrap();
    }
    writer.finish().unwrap();
    drop(index);
    std::fs::remove_dir_all(resident_path).unwrap();
}

type Ranked = Vec<(String, u64, u64)>;

fn query_results(handle: &EmbeddedStoreHandle) -> Vec<(&'static str, Ranked)> {
    let query = embedding_for("memory:m0003");
    [
        ("text", SearchCandidateRequest::text("storage rebuild", 20)),
        (
            "vector",
            SearchCandidateRequest::vector(query.clone(), 20).with_exact_vector_search(),
        ),
        (
            "hybrid",
            SearchCandidateRequest::hybrid("storage rebuild", query, 20).with_exact_vector_search(),
        ),
    ]
    .into_iter()
    .map(|(name, request)| {
        let hits = handle
            .search_candidates(&request)
            .unwrap_or_else(|error| panic!("{name}: {error}"))
            .hits
            .into_iter()
            .map(|hit| (hit.id, hit.score.to_bits(), hit.text_score.to_bits()))
            .collect::<Vec<_>>();
        assert!(!hits.is_empty(), "{name}");
        (name, hits)
    })
    .collect()
}

fn stage_names(root: &Path) -> Vec<String> {
    std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".hawdb-search-rebuild-"))
        .collect()
}

fn catch_up(handle: &EmbeddedStoreHandle) -> Result<SearchProjectionCatchUpReport> {
    handle.catch_up_search_projection_with_batch_hydrator(64, 64, 64, |_, _| {
        Ok(SearchProjectionRelationalDelta::default())
    })
}

fn rebuild(
    handle: &EmbeddedStoreHandle,
    target: &Path,
    source: &mut dyn SearchProjectionRebuildSource,
    task: RuntimeTaskContext,
) -> Result<SearchProjectionRebuildReport> {
    handle.rebuild_out_of_core_search_projection(target, rebuild_options(), source, task)
}

#[test]
#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
fn rebuild_recovers_a_trimmed_change_log_and_matches_a_fresh_build() {
    let (root, handle, config) = old_library("projection_rebuild_trimmed", 120);
    let log_start = handle
        .read_store()
        .unwrap()
        .graph
        .database()
        .search_projection_changefeed_status()
        .unwrap()
        .resume_floor_commit_epoch;
    assert!(log_start > 0);
    let error = catch_up(&handle).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("full search projection rebuild required"),
        "{error}"
    );
    let old_manifest = std::fs::read(root.join("search").join(MANIFEST)).unwrap();

    // A writer commits while the rebuild scans its pinned snapshot.
    let writer = handle.clone();
    let mut source = HostSource {
        on_first_batch: Some(Box::new(move || {
            writer
                .query_with_report(
                    "CREATE (:Memory {id: 'late', title: 'late', \
                     content: 'storage rebuild written during the scan'})",
                )
                .unwrap();
        })),
        ..Default::default()
    };
    let target = root.join("search-rebuilt");
    let report = rebuild(&handle, &target, &mut source, RuntimeTaskContext::default()).unwrap();
    assert_eq!(report.target_path, target);
    assert_eq!(report.graph_documents, 121, "{report:?}");
    assert_eq!(report.host_documents, 1);
    assert_eq!(report.build.document_count, 122);
    assert_eq!(report.build.vector_document_count, 122);
    assert_eq!(
        report.build.source_graph_commit_epoch,
        Some(report.source_graph_commit_epoch)
    );
    assert_eq!(report.verification.graph, report.verification.generation);
    assert_eq!(report.verification.graph.document_count(), 122);
    assert!(source.hydrated_batches >= 2 * 121usize.div_ceil(16));
    assert_eq!(
        source.phases,
        vec![
            SearchProjectionRebuildPhase::Scan,
            SearchProjectionRebuildPhase::Build,
            SearchProjectionRebuildPhase::Verify,
            SearchProjectionRebuildPhase::Install,
        ]
    );
    assert!(stage_names(&root).is_empty());
    // The previous directory is untouched evidence.
    assert_eq!(
        std::fs::read(root.join("search").join(MANIFEST)).unwrap(),
        old_manifest
    );
    let graph_epoch = handle
        .read_store()
        .unwrap()
        .graph
        .database()
        .commit_epoch()
        .unwrap();
    assert!(graph_epoch > report.source_graph_commit_epoch);
    let governor = handle.runtime_governor_snapshot().unwrap();
    assert_eq!(governor.admissions, governor.completions);
    drop(handle);

    // Switch: reopen on the rebuilt directory and catch up from its epoch.
    let (switched, _) =
        EmbeddedStoreHandle::open_with_options(open_options(&root, "search-rebuilt", &config))
            .unwrap();
    let freshness = switched
        .runtime_status()
        .unwrap()
        .projection_freshness
        .unwrap();
    assert_eq!(
        freshness.durable_source_graph_commit_epoch,
        Some(report.source_graph_commit_epoch)
    );
    assert!(switched
        .search_projection_documents(&["memory:late".into()], 1)
        .is_err());
    let caught_up = catch_up(&switched).unwrap();
    assert!(caught_up.complete);
    assert_eq!(caught_up.end_durable_epoch, Some(graph_epoch));
    let late = switched
        .search_projection_documents(&["memory:late".into()], 1)
        .unwrap();
    assert_eq!(late[0].content, "storage rebuild written during the scan");
    let labelled = switched
        .search_projection_documents(&["memory:m0010".into(), "memory:m0015".into()], 2)
        .unwrap();
    assert_eq!(labelled[0].metadata["labels"], "[\"alpha\"]");
    assert_eq!(labelled[1].metadata["labels"], "[\"beta\"]");
    let rebuilt_results = query_results(&switched);

    let fresh = root.join("search-fresh");
    fresh_build(&switched, &fresh, &["memory:late"]);
    drop(switched);
    assert_eq!(digest(&target), digest(&fresh));
    let (fresh_handle, _) =
        EmbeddedStoreHandle::open_with_options(open_options(&root, "search-fresh", &config))
            .unwrap();
    assert!(catch_up(&fresh_handle).unwrap().complete);
    assert_eq!(query_results(&fresh_handle), rebuilt_results);
    drop(fresh_handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn rebuild_cancellation_and_interruption_keep_the_old_projection_and_publish_nothing() {
    let (root, handle, config) = old_library("projection_rebuild_interrupted", 40);
    let old = root.join("search");
    let old_manifest = std::fs::read(old.join(MANIFEST)).unwrap();
    let target = root.join("search-rebuilt");

    // Cancellation mid-scan: the private stage is removed, the target never appears.
    let token = RuntimeCancellationToken::new();
    let cancel = token.clone();
    let mut source = HostSource {
        on_first_batch: Some(Box::new(move || {
            cancel.cancel();
        })),
        ..Default::default()
    };
    let error = rebuild(
        &handle,
        &target,
        &mut source,
        RuntimeTaskContext::without_deadline(token),
    )
    .unwrap_err();
    assert!(error.to_string().contains("cancel"), "{error}");
    assert!(!target.exists());
    assert!(stage_names(&root).is_empty());

    // A crash during the build leaves only an unselected stage: copy the stage
    // as the process would leave it mid-scan, and prove it cannot be opened.
    let crash_image = root.join("crash-during-build");
    let image = crash_image.clone();
    let parent = root.clone();
    let mut copied = false;
    let mut source = HostSource {
        on_progress: Some(Box::new(move |progress| {
            if !copied && progress.phase == SearchProjectionRebuildPhase::Scan {
                let stage = stage_names(&parent)
                    .pop()
                    .expect("stage exists during scan");
                copy_dir(&parent.join(stage), &image);
                copied = true;
            }
        })),
        ..Default::default()
    };
    // Then refuse the install, proving the complete stage is cleaned up too.
    crate::embedded_store::search_projection_rebuild::hooks::set_before_install(|stage, target| {
        assert!(stage.join(MANIFEST).exists());
        assert!(!target.exists());
        // A crash here leaves a complete but unselected stage.
        std::fs::create_dir(target).unwrap();
    });
    let error = rebuild(&handle, &target, &mut source, RuntimeTaskContext::default()).unwrap_err();
    assert!(
        error.to_string().contains("nothing was installed"),
        "{error}"
    );
    assert!(!crash_image.join(MANIFEST).exists());
    assert!(
        crate::SearchOutOfCoreReader::open(&crash_image).is_err(),
        "a mid-scan stage must not be an openable projection"
    );
    assert!(stage_names(&root).is_empty());
    // The directory the hook created is not a projection and was not replaced.
    assert_eq!(std::fs::read_dir(&target).unwrap().count(), 0);
    std::fs::remove_dir(&target).unwrap();

    // A leftover stage from a crash does not block a later rebuild.
    let leftover = root.join(".search-rebuilt.hawdb-search-rebuild-crashed");
    copy_dir(&crash_image, &leftover);
    let report = rebuild(
        &handle,
        &target,
        &mut HostSource::default(),
        RuntimeTaskContext::default(),
    )
    .unwrap();
    assert_eq!(report.build.document_count, 42);
    assert!(leftover.exists());
    assert_eq!(std::fs::read(old.join(MANIFEST)).unwrap(), old_manifest);
    // An existing target is refused before any work.
    let error = rebuild(
        &handle,
        &target,
        &mut HostSource::default(),
        RuntimeTaskContext::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("already exists"), "{error}");
    let governor = handle.runtime_governor_snapshot().unwrap();
    assert_eq!(governor.admissions, governor.completions);
    drop(handle);

    // The old projection still opens and serves exactly its old state.
    let (reopened, _) =
        EmbeddedStoreHandle::open_with_options(open_options(&root, "search", &config)).unwrap();
    assert_eq!(
        reopened
            .search_projection_documents(&["memory:stale".into()], 1)
            .unwrap()[0]
            .content,
        "stale bootstrap"
    );
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}

/// A source whose second pass disagrees with the first must not install.
#[test]
fn rebuild_refuses_to_install_a_generation_that_differs_from_the_graph() {
    struct Drifting(usize);
    impl SearchProjectionRebuildSource for Drifting {
        fn hydrate_graph_rows(
            &mut self,
            _snapshot: &mut DatabaseReadTransaction,
            rows: &mut Vec<SearchProjectionRow>,
        ) -> Result<()> {
            self.0 += 1;
            if self.0 > 1 {
                rows[0].body.push_str(" drifted");
            }
            Ok(())
        }
    }
    let (root, handle, _) = old_library("projection_rebuild_drift", 8);
    let target = root.join("search-rebuilt");
    let error = handle
        .rebuild_out_of_core_search_projection(
            &target,
            SearchProjectionRebuildOptions {
                maintenance: IncrementalSearchMaintenanceOptions::default(),
                ..Default::default()
            },
            &mut Drifting(0),
            RuntimeTaskContext::default(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("differs from graph"), "{error}");
    assert!(!target.exists());
    assert!(stage_names(&root).is_empty());
    // Host-supplied epochs are refused: the rebuild owns the source epoch.
    let mut options = SearchProjectionRebuildOptions::default();
    options.maintenance.build_options.source_graph_commit_epoch = Some(1);
    assert!(handle
        .rebuild_out_of_core_search_projection(
            &target,
            options,
            &mut GraphOnlySearchProjectionRebuild,
            RuntimeTaskContext::default(),
        )
        .is_err());
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

/// The scan, external sort and generation build never hold the corpus: live
/// allocations stay within the working reservation while the documents are
/// several times larger. Verification then opens the generation as a serving
/// reader, whose resident range index is bounded by the reader configuration.
#[test]
fn rebuild_working_memory_is_bounded_below_the_corpus() {
    const DOCUMENTS: usize = 2_500;
    const BODY_BYTES: usize = 8 * 1024;
    struct Peaks<'a> {
        window: &'a crate::api::test_allocation::Window,
        before_verify: Option<usize>,
    }
    impl SearchProjectionRebuildSource for Peaks<'_> {
        fn progress(&mut self, progress: &SearchProjectionRebuildProgress) {
            if progress.phase == SearchProjectionRebuildPhase::Verify
                && self.before_verify.is_none()
            {
                self.before_verify = Some(self.window.peak());
            }
        }
    }
    let root = unique_embedded_store_test_dir("projection_rebuild_bounded");
    let (handle, _) = EmbeddedStoreHandle::open_with_options(EmbeddedStoreOpenOptions::graph_only(
        root.join("graph"),
        GraphMode::WritableCutover,
    ))
    .unwrap();
    let filler = "lorem ".repeat(BODY_BYTES / 6);
    for chunk in 0..DOCUMENTS / 100 {
        handle
            .with_transaction(|transaction| {
                for number in chunk * 100..(chunk + 1) * 100 {
                    transaction.query_with_params(
                        "CREATE (:Memory {id: $id, title: $id, content: $content})",
                        &BTreeMap::from([
                            ("id".into(), Value::String(format!("bulk-{number:05}"))),
                            (
                                "content".into(),
                                Value::String(format!("{filler} token{number}")),
                            ),
                        ]),
                    )?;
                }
                Ok(())
            })
            .unwrap();
    }
    let corpus = DOCUMENTS * BODY_BYTES;
    let working = 24 << 20;
    let mut options = SearchProjectionRebuildOptions {
        hydration_batch_rows: NonZeroUsize::new(32).unwrap(),
        ..Default::default()
    };
    options.maintenance.working_memory_bytes = NonZeroU64::new(working).unwrap();
    options.sorting.max_buffer_records = NonZeroUsize::new(256).unwrap();
    options.sorting.max_buffer_payload_bytes = NonZeroUsize::new(2 << 20).unwrap();
    let build = &mut options.maintenance.build_options;
    build.max_content_documents = NonZeroUsize::new(512).unwrap();
    build.max_content_artifact_bytes = NonZeroU64::new(4 << 20).unwrap();
    build.lexical_build_memory_bytes = NonZeroU64::new(4 << 20).unwrap();
    build.rabitq_build_memory_bytes = NonZeroUsize::new(2 << 20).unwrap();
    let target = root.join("search-rebuilt");
    let window = crate::api::test_allocation::Window::new();
    let mut peaks = Peaks {
        window: &window,
        before_verify: None,
    };
    let report = handle
        .rebuild_out_of_core_search_projection(
            &target,
            options,
            &mut peaks,
            RuntimeTaskContext::default(),
        )
        .unwrap();
    let build_peak = peaks.before_verify.unwrap();
    drop(window);
    assert_eq!(report.build.document_count, DOCUMENTS);
    assert_eq!(report.working_memory_bytes, working);
    assert!(report.sorting.runs_created > 1, "{:?}", report.sorting);
    assert!(
        report.sorting.peak_buffer_payload_bytes <= 2 << 20,
        "{:?}",
        report.sorting
    );
    assert!(report.build.logical_document_bytes as usize >= corpus);
    assert!(
        build_peak < working as usize && build_peak < corpus / 2,
        "scan/sort/build peak {build_peak} bytes, working {working} bytes, corpus {corpus} bytes"
    );
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

fn copy_dir(source: &Path, destination: &Path) {
    std::fs::create_dir_all(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&path, &target);
        } else if std::fs::copy(&path, &target).is_err() {
            // Files can disappear while a live stage is copied.
        }
    }
}
