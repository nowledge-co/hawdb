// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use crate::{
    SearchDocument, SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationWriter,
    SearchOutOfCoreReader, SearchOutOfCoreSegmentCompactionPolicy, SearchProjectionDelta,
    SearchProjectionKind, SearchProjectionRow,
};
use hawdb_search::{SearchMode, SearchQueryOptions, SearchResultSet};
use std::num::{NonZeroU64, NonZeroUsize};

const MANIFEST: &str = "search/search_projection.out_of_core.manifest.hawdb";

fn row(id: &str, body: &str) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: id.into(),
        title: id.into(),
        body: body.into(),
        embedding: Some(vec![0.25, 0.75]),
        source_id: None,
        metadata: BTreeMap::new(),
    }
}

fn documents(reader: &SearchOutOfCoreReader, ids: &[&str]) -> Vec<SearchDocument> {
    reader
        .hydrate_documents(
            &ids.iter()
                .map(|id| format!("memory:{id}"))
                .collect::<Vec<_>>(),
        )
        .unwrap()
        .documents
}

fn bootstrap(path: &Path) -> SearchOutOfCoreReader {
    let mut writer = SearchOutOfCoreGenerationWriter::create(path, Default::default()).unwrap();
    writer.push(row("a", "old a").into_document()).unwrap();
    writer.push(row("z", "old z").into_document()).unwrap();
    writer.finish().unwrap();
    SearchOutOfCoreReader::open(path).unwrap()
}

fn mutate(reader: &SearchOutOfCoreReader) {
    SearchOutOfCoreGenerationWriter::prepare_delta(
        reader,
        SearchProjectionDelta {
            upserts: vec![row("m", "inserted m"), row("z", "replaced z")],
            deletes: vec!["memory:a".into()],
            max_operations: Some(3),
            source_graph_commit_epoch: Some(11),
        },
        Default::default(),
    )
    .unwrap()
    .finish()
    .unwrap();
}

fn plans(snapshot: &PowerLossSnapshot) -> (Vec<CrashPlan>, usize) {
    let mut plans = publication_fault_plans(snapshot);
    let complete = snapshot.persist_all_plan();
    let mut torn = 0;
    if let Some(path) = snapshot.observed_path() {
        for write in snapshot.uncovered_writes(path).unwrap() {
            if write.length < 2 {
                continue;
            }
            for bytes in [0..1, write.length / 2..write.length] {
                let mut plan = complete.clone();
                plan.persistence
                    .retain(|operation| *operation != PersistOperation::Whole(write.operation));
                plan.persistence.push(PersistOperation::TornWrite {
                    operation: write.operation,
                    bytes,
                });
                plans.push(plan);
                torn += 1;
            }
        }
    }
    (plans, torn)
}

fn qualify(compaction: bool) {
    for (event, boundary, relative_path) in [
        (IoEvent::Write, ObservationBoundary::After, "search"),
        (IoEvent::Rename, ObservationBoundary::Before, MANIFEST),
        (IoEvent::Rename, ObservationBoundary::After, MANIFEST),
    ] {
        let mut fixture = Fixture::new();
        let path = fixture.root.join("search");
        let mut reader = bootstrap(&path);
        let bootstrap_image = fixture
            .model
            .capture()
            .unwrap()
            .crash(&CrashPlan::default())
            .unwrap();
        assert!(
            bootstrap_image.bytes(Path::new(MANIFEST)).is_some(),
            "acknowledged bootstrap manifest must survive: directories={:?}",
            bootstrap_image.directory_paths().collect::<Vec<_>>()
        );
        if compaction {
            mutate(&reader);
            reader = SearchOutOfCoreReader::open(&path).unwrap();
        }
        let old_generation = reader.generation();
        let old_epoch = reader.source_graph_commit_epoch();
        let old_documents = documents(&reader, if compaction { &["m", "z"] } else { &["a", "z"] });
        fixture
            .model
            .observe(ObservationPoint {
                event,
                relative_path: relative_path.into(),
                boundary,
                skip_matches: 0,
                include_descendants: event == IoEvent::Write,
                keep_last: false,
            })
            .unwrap();
        if compaction {
            let policy = SearchOutOfCoreSegmentCompactionPolicy::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroU64::new(256 * 1024 * 1024).unwrap(),
            )
            .unwrap();
            assert!(SearchOutOfCoreGenerationWriter::compact_segments(
                &reader,
                policy,
                Default::default(),
            )
            .unwrap()
            .is_some());
        } else {
            mutate(&reader);
        }
        let published = SearchOutOfCoreReader::open(&path).unwrap();
        let new_generation = published.generation();
        let new_epoch = published.source_graph_commit_epoch();
        let new_documents = documents(&published, &["m", "z"]);
        assert!(new_generation > old_generation);
        assert_eq!(new_epoch, Some(11));
        let acknowledged = fixture.model.capture().unwrap();
        let snapshot = fixture
            .model
            .take_observation()
            .unwrap()
            .expect("real publication IO must be recorded");
        drop(published);
        drop(reader);
        let (plans, torn) = plans(&snapshot);
        eprintln!(
            "search-power-publication-v1 compaction={compaction} event={event:?} boundary={boundary:?} plans={} torn={torn}",
            plans.len()
        );
        if event == IoEvent::Write {
            assert!(
                torn > 0,
                "the actual unsynchronized write must exercise torn images"
            );
        }
        for plan in plans {
            let image = fixture.image(&snapshot, &plan);
            let recovered =
                SearchOutOfCoreReader::open(image.join("search")).unwrap_or_else(|error| {
                    panic!("complete publication closure required: {error}; plan={plan:?}")
                });
            if event == IoEvent::Write {
                assert_eq!(recovered.generation(), old_generation);
            }
            if recovered.generation() == old_generation {
                assert_eq!(recovered.source_graph_commit_epoch(), old_epoch);
                assert_eq!(
                    documents(
                        &recovered,
                        if compaction { &["m", "z"] } else { &["a", "z"] }
                    ),
                    old_documents
                );
            } else {
                assert_eq!(recovered.generation(), new_generation);
                assert_eq!(recovered.source_graph_commit_epoch(), new_epoch);
                assert_eq!(documents(&recovered, &["m", "z"]), new_documents);
            }
            assert_eq!(recovered.document_count(), 2);
        }
        // A response can be lost after the durable publication has completed.
        // Losing all uncovered writes must still retain its entire closure.
        let image = fixture.image(&acknowledged, &CrashPlan::default());
        let recovered = SearchOutOfCoreReader::open(image.join("search")).unwrap();
        assert_eq!(recovered.generation(), new_generation);
        assert_eq!(recovered.source_graph_commit_epoch(), Some(11));
        assert_eq!(documents(&recovered, &["m", "z"]), new_documents);
    }
}

#[test]
fn incremental_publication_survives_lost_torn_and_reordered_writes() {
    qualify(false);
}

#[test]
fn overlapping_compaction_survives_lost_torn_and_reordered_writes() {
    qualify(true);
}

#[cfg(feature = "full-text-search")]
fn assert_same_queries(actual: &SearchOutOfCoreReader, expected: &SearchOutOfCoreReader) {
    use crate::{SearchMode, SearchQueryOptions};
    for mode in [
        SearchMode::Text,
        #[cfg(feature = "vector-search")]
        SearchMode::Vector,
        #[cfg(feature = "vector-search")]
        SearchMode::Hybrid,
    ] {
        let options = SearchQueryOptions {
            limit: 10,
            offset: 0,
            rank_window: None,
            fusion_weights: Default::default(),
            metadata_filters: Default::default(),
            policy_epoch: None,
        };
        let reference = expected
            .search_with_options(
                "complete import",
                Some(&[0.25, 0.75]),
                mode,
                options.clone(),
            )
            .unwrap();
        let result = actual
            .search_with_options(
                "complete import",
                Some(&[0.25, 0.75]),
                mode,
                options.clone(),
            )
            .unwrap();
        assert_eq!(result.result.hits, reference.result.hits);
        assert_eq!(result.result.total_hits, reference.result.total_hits);
        #[cfg(feature = "vector-search")]
        if mode != SearchMode::Text {
            let compressed = actual
                .search_with_options_compressed_vector_projection_mode(
                    "complete import",
                    Some(&[0.25, 0.75]),
                    mode,
                    options,
                    crate::CompressedVectorSearchMode::Required,
                )
                .unwrap();
            assert!(compressed.metrics.rabitq_payload_bytes_read > 0);
            assert_eq!(compressed.result.hits, reference.result.hits);
            assert_eq!(compressed.result.total_hits, reference.result.total_hits);
        }
    }
}

#[test]
fn partitioned_initial_publication_survives_lost_torn_and_reordered_writes() {
    for replaces_existing in [false, true] {
        // Prefix selectors are private. Only the final real selector can make
        // any part of this import visible, including after a lost response.
        for cut in 0..5 {
            let mut fixture = Fixture::new();
            let path = fixture.root.join("search");
            let old = replaces_existing.then(|| bootstrap(&path));
            let old_documents = old.as_ref().map(|reader| documents(reader, &["a", "z"]));
            let mut writer = SearchOutOfCoreGenerationWriter::create(
                &path,
                SearchOutOfCoreGenerationBuildOptions {
                    max_content_documents: NonZeroUsize::new(2).unwrap(),
                    source_graph_commit_epoch: Some(21),
                    ..Default::default()
                },
            )
            .unwrap();
            let ids = ["n0", "n1", "n2", "n3", "n4", "n5", "n6"];
            for id in ids {
                writer
                    .push(row(id, "complete import").into_document())
                    .unwrap();
            }
            let stage = std::fs::read_dir(&path)
                .unwrap()
                .map(|entry| entry.unwrap())
                .find(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".search-generation.")
                })
                .expect("the one active initial writer stage");
            let private_manifest = Path::new("search")
                .join(stage.file_name())
                .join("search_projection.out_of_core.manifest.hawdb");
            let (event, boundary, relative_path) = match cut {
                0 => (
                    IoEvent::Write,
                    ObservationBoundary::After,
                    PathBuf::from("search"),
                ),
                1 => (
                    IoEvent::Rename,
                    ObservationBoundary::Before,
                    private_manifest,
                ),
                2 => (
                    IoEvent::Rename,
                    ObservationBoundary::After,
                    private_manifest,
                ),
                3 => (
                    IoEvent::Rename,
                    ObservationBoundary::Before,
                    PathBuf::from(MANIFEST),
                ),
                _ => (
                    IoEvent::Rename,
                    ObservationBoundary::After,
                    PathBuf::from(MANIFEST),
                ),
            };
            fixture
                .model
                .observe(ObservationPoint {
                    event,
                    relative_path,
                    boundary,
                    skip_matches: 0,
                    include_descendants: event == IoEvent::Write,
                    keep_last: false,
                })
                .unwrap();
            let report = writer.finish().unwrap();
            assert_eq!(report.published_content_segments, 4);
            let published = SearchOutOfCoreReader::open(&path).unwrap();
            assert_eq!(published.document_count(), ids.len());
            let new_documents = documents(&published, &ids);
            let acknowledged = fixture.model.capture().unwrap();
            let snapshot = fixture
                .model
                .take_observation()
                .unwrap()
                .expect("the actual initial publication boundary must be observed");
            // Before the first rename its destination has no inode yet. The
            // namespace plans still cover loss and reordering; the write cut
            // supplies the actual uncovered inode for torn-write plans.
            let (plans, torn) = if cut == 1 || (cut == 3 && !replaces_existing) {
                (publication_fault_plans(&snapshot), 0)
            } else {
                plans(&snapshot)
            };
            eprintln!("search-power-initial-v1 replaces_existing={replaces_existing} cut={cut} plans={} torn={torn}", plans.len());
            if cut == 0 {
                assert!(torn > 0);
            }
            for plan in plans {
                let image = fixture.image(&snapshot, &plan);
                if !image.join(MANIFEST).exists() {
                    assert!(
                        !replaces_existing,
                        "an acknowledged previous selector must survive"
                    );
                    assert!(SearchOutOfCoreReader::open(image.join("search")).is_err());
                    continue;
                }
                let recovered =
                    SearchOutOfCoreReader::open(image.join("search")).unwrap_or_else(|error| {
                        panic!("complete initial closure required: {error}; plan={plan:?}")
                    });
                if old
                    .as_ref()
                    .is_some_and(|old| recovered.generation() == old.generation())
                {
                    assert_eq!(recovered.document_count(), 2);
                    assert_eq!(recovered.source_graph_commit_epoch(), None);
                    assert_eq!(
                        documents(&recovered, &["a", "z"]),
                        *old_documents.as_ref().unwrap()
                    );
                } else {
                    assert_eq!(cut, 4, "a private prefix cannot publish the real selector");
                    assert_eq!(recovered.generation(), report.generation);
                    assert_eq!(recovered.source_graph_commit_epoch(), Some(21));
                    assert_eq!(recovered.document_count(), ids.len());
                    assert_eq!(documents(&recovered, &ids), new_documents);
                    #[cfg(feature = "full-text-search")]
                    assert_same_queries(&recovered, &published);
                }
            }
            let image = fixture.image(&acknowledged, &CrashPlan::default());
            let recovered = SearchOutOfCoreReader::open(image.join("search")).unwrap();
            assert_eq!(recovered.generation(), report.generation);
            assert_eq!(recovered.source_graph_commit_epoch(), Some(21));
            assert_eq!(recovered.document_count(), ids.len());
            assert_eq!(documents(&recovered, &ids), new_documents);
            #[cfg(feature = "full-text-search")]
            assert_same_queries(&recovered, &published);
        }
    }
}

const COMPONENT_MANIFEST: &str = "search_projection.out_of_core.manifest.hawdb";

fn component_row(number: usize) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: format!("{number:06}"),
        title: format!("Graph storage {number}"),
        body: "Graph storage preserves complete search generations".into(),
        embedding: Some(vec![1.0, number as f32 / 10.0]),
        source_id: None,
        metadata: BTreeMap::from([("space_id".into(), "default".into())]),
    }
}

fn document(number: usize) -> SearchDocument {
    component_row(number).into_document()
}

#[test]
fn acknowledged_generation_survives_loss_of_uncovered_operations() {
    for relative_root in [Path::new("search"), Path::new("projections/search")] {
        let mut fixture = Fixture::new();
        let root = fixture.root.join(relative_root);
        let expected = document(0);
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        writer.push(expected.clone()).unwrap();
        let report = writer.finish().unwrap();
        // Coverage validation compares the modeled complete image with native IO.
        let snapshot = fixture.model.capture().unwrap();
        for plan in [snapshot.persist_all_plan(), CrashPlan::default()] {
            let crash = snapshot.crash(&plan).unwrap();
            assert!(
                crash.directory_paths().any(|path| path == relative_root),
                "acknowledged search root {relative_root:?} is missing; plan={plan:?}"
            );
            let image = fixture.image(&snapshot, &plan);
            let reader = SearchOutOfCoreReader::open(image.join(relative_root)).unwrap();
            assert_eq!(reader.generation(), report.generation);
            assert_eq!(reader.document_count(), 1);
            assert_eq!(
                reader
                    .hydrate_documents(std::slice::from_ref(&expected.id))
                    .unwrap()
                    .documents,
                vec![expected.clone()]
            );
        }
    }
}

#[test]
fn retry_covers_existing_unsynchronized_component_ancestry() {
    let mut fixture = Fixture::new();
    let root = fixture.root.join("interrupted/search");
    // An earlier interrupted attempt may leave these names visible but unsynced.
    hawdb_storage::file_io::create_dir_all(&root).unwrap();
    let project = fixture.model.project().clone();
    with_reserved_descriptors(&project, project.metrics().limit, || {
        assert!(SearchOutOfCoreGenerationWriter::create(&root, Default::default()).is_err());
    });

    // Root barriers precede the spool and use just one transient descriptor.
    with_reserved_descriptors(&project, project.metrics().limit - 1, || {
        let writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        assert_eq!(project.metrics().open, 1);
        drop(writer);
    });

    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    writer.push(document(0)).unwrap();
    let generation = writer.finish().unwrap().generation;
    let snapshot = fixture.model.capture().unwrap();
    let image = fixture.image(&snapshot, &CrashPlan::default());
    let reader = SearchOutOfCoreReader::open(image.join("interrupted/search")).unwrap();
    assert_eq!(reader.generation(), generation);
    assert_eq!(reader.document_count(), 1);
    assert_eq!(project.metrics().open, 0);
    assert_eq!(project.metrics().reserved, 0);
    assert!(project.metrics().high_water <= project.metrics().limit);
}

#[test]
fn external_symlink_root_preserves_writer_and_reader_behavior() {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let unique = format!(
        "{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let project_root = MaterializedImage(
        std::env::temp_dir().join(format!("hawdb-search-symlink-project-{unique}")),
    );
    let target_root = MaterializedImage(
        std::env::temp_dir().join(format!("hawdb-search-symlink-target-{unique}")),
    );
    std::fs::create_dir_all(&project_root).unwrap();
    std::fs::create_dir_all(&target_root).unwrap();
    let _project = ProjectFileDescriptors::acquire(&project_root, 32).unwrap();
    let root = project_root.join("search");
    std::os::unix::fs::symlink(&target_root, &root).unwrap();
    assert!(!std::fs::canonicalize(&root)
        .unwrap()
        .starts_with(std::fs::canonicalize(&project_root).unwrap()));

    let expected = document(0);
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    writer.push(expected.clone()).unwrap();
    let generation = writer.finish().unwrap().generation;
    for path in [&root, &target_root.0] {
        let reader = SearchOutOfCoreReader::open(path).unwrap();
        assert_eq!(reader.generation(), generation);
        assert_complete(&reader, std::slice::from_ref(&expected));
    }
}

fn with_reserved_descriptors(project: &ProjectFileDescriptors, count: usize, work: impl FnOnce()) {
    // Reservations are thread-local operation inventories. Hold unrelated host
    // capacity on another thread so the writer cannot borrow that inventory.
    let project = project.clone();
    let (ready, started) = std::sync::mpsc::sync_channel(0);
    let (release, released) = std::sync::mpsc::sync_channel(0);
    let owner = std::thread::spawn(move || {
        let _reservation = project.reserve(count).unwrap();
        ready.send(()).unwrap();
        let _ = released.recv();
    });
    started.recv().unwrap();
    work();
    release.send(()).unwrap();
    owner.join().unwrap();
}

#[derive(Clone, Copy, Debug)]
enum Publication {
    Append,
    Mutation,
    Compaction,
    PartitionedAppend,
    PartitionedMutation,
}

fn append(root: &Path) {
    let reader = SearchOutOfCoreReader::open(root).unwrap();
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![component_row(2)],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap()
    .finish()
    .unwrap();
}

fn replacement() -> SearchProjectionRow {
    let mut replacement = component_row(0);
    replacement.title = "Graph replacement".into();
    replacement.body = "Graph current replacement generation".into();
    replacement.embedding = Some(vec![0.1, 1.0]);
    replacement
        .metadata
        .insert("space_id".into(), "current".into());
    replacement
}

fn mutate_component(root: &Path) {
    let reader = SearchOutOfCoreReader::open(root).unwrap();
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![replacement()],
            deletes: vec![document(1).id],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap()
    .finish()
    .unwrap();
}

fn expected_documents(publication: Publication, after: bool) -> Vec<SearchDocument> {
    match (publication, after) {
        (Publication::Append, false) => vec![document(0), document(1)],
        (Publication::Append, true) | (Publication::Mutation, false) => {
            vec![document(0), document(1), document(2)]
        }
        (Publication::Mutation, true) | (Publication::Compaction, _) => {
            vec![replacement().into_document(), document(2)]
        }
        (Publication::PartitionedAppend, false) => vec![document(0), document(1)],
        (Publication::PartitionedAppend, true) => (0..6).map(document).collect(),
        (Publication::PartitionedMutation, false) => {
            vec![replacement().into_document(), document(2)]
        }
        (Publication::PartitionedMutation, true) => {
            vec![replacement().into_document(), document(3), document(4)]
        }
    }
}

fn search(
    reader: &SearchOutOfCoreReader,
    mode: SearchMode,
    space: Option<&str>,
) -> SearchResultSet {
    reader
        .search_with_options(
            "graph",
            Some(&[1.0, 0.2]),
            mode,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: Default::default(),
                metadata_filters: space
                    .map(|space| BTreeMap::from([("space_id".into(), space.into())]))
                    .unwrap_or_default(),
                policy_epoch: None,
            },
        )
        .unwrap()
        .result
}

fn assert_complete(reader: &SearchOutOfCoreReader, expected: &[SearchDocument]) {
    assert_eq!(reader.document_count(), expected.len());
    let ids: Vec<_> = expected
        .iter()
        .map(|document| document.id.clone())
        .collect();
    assert_eq!(reader.hydrate_documents(&ids).unwrap().documents, expected);
    for number in 0..6 {
        let id = document(number).id;
        if !ids.contains(&id) {
            assert!(reader.hydrate_documents(&[id]).is_err());
        }
    }
}

fn assert_search_parity(expected: &SearchResultSet, actual: &SearchResultSet) {
    assert_eq!(actual.total_hits, expected.total_hits);
    assert_eq!(actual.hits.len(), expected.hits.len());
    for (actual, expected) in actual.hits.iter().zip(&expected.hits) {
        assert_eq!(actual.id, expected.id);
        for (actual, expected) in [
            (actual.score, expected.score),
            (actual.text_score, expected.text_score),
            (actual.vector_score, expected.vector_score),
        ] {
            assert!((actual - expected).abs() < 1e-10, "{actual} != {expected}");
        }
    }
}

fn fault_plans(snapshot: &PowerLossSnapshot, event: IoEvent) -> Vec<CrashPlan> {
    let mut plans = publication_fault_plans(snapshot);
    if event == IoEvent::Write {
        let path = snapshot.observed_path().unwrap();
        assert!(path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("search_projection.out_of_core.manifest.tmp."));
        let writes = snapshot.uncovered_writes(path).unwrap();
        assert!(
            !writes.is_empty(),
            "manifest write must not yet be synchronized"
        );
        for write in writes {
            assert!(write.length > 1);
            for bytes in [0..write.length / 2, write.length / 2..write.length] {
                let mut plan = snapshot.persist_all_plan();
                let operation = plan
                    .persistence
                    .iter_mut()
                    .find(|operation| **operation == PersistOperation::Whole(write.operation))
                    .unwrap();
                *operation = PersistOperation::TornWrite {
                    operation: write.operation,
                    bytes,
                };
                plans.push(plan.clone());
                plan.persistence.reverse();
                plans.push(plan);
            }
        }
    }
    plans
}

fn same_physical_image(
    left: &hawdb_storage::power_loss::image::CrashImage,
    right: &hawdb_storage::power_loss::image::CrashImage,
) -> bool {
    left.directory_paths().eq(right.directory_paths())
        && left.paths().eq(right.paths())
        && left.paths().all(|path| {
            left.file_inode(path) == right.file_inode(path) && left.bytes(path) == right.bytes(path)
        })
}

fn qualify_publication(publication: Publication) {
    for (event, boundary) in [
        (IoEvent::Write, ObservationBoundary::After),
        (IoEvent::Rename, ObservationBoundary::Before),
        (IoEvent::Rename, ObservationBoundary::After),
    ] {
        // Mutation's post-commit cleanup discovery writes validation scratch.
        // Its exact rename points cover selector publication; append/compaction
        // also cover torn temporary writes through the shared GenerationIo.
        if matches!(publication, Publication::Mutation) && event == IoEvent::Write {
            continue;
        }
        let mut fixture = Fixture::new();
        let root = fixture.root.join("search");
        let selector = Path::new("search").join(COMPONENT_MANIFEST);
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        writer.push(document(0)).unwrap();
        writer.push(document(1)).unwrap();
        writer.finish().unwrap();
        if !matches!(
            publication,
            Publication::Append | Publication::PartitionedAppend
        ) {
            append(&root);
        }
        if matches!(
            publication,
            Publication::Compaction | Publication::PartitionedMutation
        ) {
            mutate_component(&root);
        }
        let before = std::fs::read(root.join(COMPONENT_MANIFEST)).unwrap();
        let old = SearchOutOfCoreReader::open(&root).unwrap();
        assert_complete(&old, &expected_documents(publication, false));
        let old_generation = old.generation();
        let mut modes = vec![SearchMode::Text];
        #[cfg(feature = "vector-search")]
        modes.extend([SearchMode::Vector, SearchMode::Hybrid]);
        let before_queries: Vec<_> = modes
            .drain(..)
            .flat_map(|mode| {
                [None, Some("default"), Some("current")]
                    .map(|space| (mode, space, search(&old, mode, space)))
            })
            .collect();
        fixture
            .model
            .observe(ObservationPoint {
                event,
                relative_path: if event == IoEvent::Write {
                    "search".into()
                } else {
                    selector.clone()
                },
                boundary,
                skip_matches: 0,
                include_descendants: event == IoEvent::Write,
                keep_last: event == IoEvent::Write,
            })
            .unwrap();
        match publication {
            Publication::Append => append(&root),
            Publication::Mutation => mutate_component(&root),
            Publication::Compaction => {
                let report = SearchOutOfCoreGenerationWriter::compact_segments(
                    &old,
                    SearchOutOfCoreSegmentCompactionPolicy::new(
                        NonZeroUsize::new(2).unwrap(),
                        NonZeroU64::new(256 * 1024 * 1024).unwrap(),
                    )
                    .unwrap(),
                    Default::default(),
                )
                .unwrap()
                .unwrap();
                assert_eq!(report.source_segment_count(), 2);
            }
            Publication::PartitionedAppend | Publication::PartitionedMutation => {
                let delta = if matches!(publication, Publication::PartitionedAppend) {
                    SearchProjectionDelta {
                        upserts: (2..6).map(component_row).collect(),
                        ..Default::default()
                    }
                } else {
                    SearchProjectionDelta {
                        upserts: vec![replacement(), component_row(3), component_row(4)],
                        deletes: vec![document(2).id],
                        ..Default::default()
                    }
                };
                let expected_segments = delta.upserts.len();
                let (_, report, _) = SearchOutOfCoreGenerationWriter::prepare_delta(
                    &old,
                    delta,
                    SearchOutOfCoreGenerationBuildOptions {
                        max_content_documents: NonZeroUsize::new(1).unwrap(),
                        ..Default::default()
                    },
                )
                .unwrap()
                .finish()
                .unwrap();
                assert_eq!(report.published_content_segments, expected_segments);
            }
        }
        let cut = fixture.model.take_observation().unwrap().unwrap();
        let acknowledged = fixture.model.capture().unwrap();
        let after = std::fs::read(root.join(COMPONENT_MANIFEST)).unwrap();
        assert_ne!(
            before, after,
            "publication must replace the active selector"
        );
        // Verify the pinned old closure after publication, then close it before
        // admitting a second full reader under the unchanged FD32 project budget.
        assert_complete(&old, &expected_documents(publication, false));
        for (mode, space, expected) in &before_queries {
            assert_search_parity(expected, &search(&old, *mode, *space));
        }
        drop(old);
        let new = SearchOutOfCoreReader::open(&root).unwrap();
        assert_ne!(old_generation, new.generation());
        assert_complete(&new, &expected_documents(publication, true));
        let queries: Vec<_> = before_queries
            .into_iter()
            .map(|(mode, space, before)| (mode, space, before, search(&new, mode, space)))
            .collect();
        let plans = fault_plans(&cut, event);
        eprintln!(
            "search-power-publication-v1 operation={publication:?} event={event:?} boundary={boundary:?} plans={}",
            plans.len()
        );
        let mut images = Vec::new();
        for plan in plans {
            let crash = cut.crash(&plan).unwrap();
            let manifest = crash.bytes(&selector).unwrap();
            let is_new = manifest == after;
            assert!(is_new || manifest == before, "partial selector: {plan:?}");
            if event == IoEvent::Write || boundary == ObservationBoundary::Before {
                assert!(!is_new, "the active selector has not been replaced");
            }
            // Pending cleanup of unreachable stage inodes can produce the exact
            // same physical tree. Evaluate every plan, reopening each distinct
            // namespace/byte/hard-link image once without dropping fault cases.
            if images
                .iter()
                .any(|image| same_physical_image(image, &crash))
            {
                continue;
            }
            images.push(crash);
            let image = fixture.image(&cut, &plan);
            let recovered = SearchOutOfCoreReader::open(image.join("search")).unwrap();
            assert_eq!(
                recovered.generation(),
                if is_new {
                    new.generation()
                } else {
                    old_generation
                }
            );
            assert_complete(&recovered, &expected_documents(publication, is_new));
            for (mode, space, before, after) in &queries {
                assert_search_parity(
                    if is_new { after } else { before },
                    &search(&recovered, *mode, *space),
                );
            }
        }
        eprintln!(
            "search-power-publication-v1 distinct_images={}",
            images.len()
        );
        let image = fixture.image(&acknowledged, &CrashPlan::default());
        let recovered = SearchOutOfCoreReader::open(image.join("search")).unwrap();
        assert_eq!(recovered.generation(), new.generation());
        assert_complete(&recovered, &expected_documents(publication, true));
    }
}

#[test]
fn append_publication_cuts_select_a_complete_generation() {
    qualify_publication(Publication::Append);
}

#[test]
fn mutation_publication_cuts_preserve_complete_replacements_and_deletes() {
    qualify_publication(Publication::Mutation);
}

#[test]
fn compaction_publication_cuts_preserve_the_complete_selected_closure() {
    qualify_publication(Publication::Compaction);
}

#[test]
fn partitioned_append_publication_cuts_select_the_complete_batch() {
    qualify_publication(Publication::PartitionedAppend);
}

#[test]
fn partitioned_mutation_publication_cuts_preserve_prior_runs_and_the_complete_batch() {
    qualify_publication(Publication::PartitionedMutation);
}
