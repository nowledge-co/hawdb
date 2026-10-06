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
use hawdb_search::{
    SearchDocument, SearchMode, SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader,
    SearchOutOfCoreSegmentCompactionPolicy, SearchProjectionDelta, SearchProjectionKind,
    SearchProjectionRow, SearchQueryOptions, SearchResultSet,
};
use std::num::{NonZeroU64, NonZeroUsize};

const MANIFEST: &str = "search_projection.out_of_core.manifest.hawdb";

fn row(number: usize) -> SearchProjectionRow {
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
    row(number).into_document()
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
}

fn append(root: &Path) {
    let reader = SearchOutOfCoreReader::open(root).unwrap();
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![row(2)],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap()
    .finish()
    .unwrap();
}

fn replacement() -> SearchProjectionRow {
    let mut replacement = row(0);
    replacement.title = "Graph replacement".into();
    replacement.body = "Graph current replacement generation".into();
    replacement.embedding = Some(vec![0.1, 1.0]);
    replacement
        .metadata
        .insert("space_id".into(), "current".into());
    replacement
}

fn mutate(root: &Path) {
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
    for number in 0..3 {
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
        let selector = Path::new("search").join(MANIFEST);
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        writer.push(document(0)).unwrap();
        writer.push(document(1)).unwrap();
        writer.finish().unwrap();
        if !matches!(publication, Publication::Append) {
            append(&root);
        }
        if matches!(publication, Publication::Compaction) {
            mutate(&root);
        }
        let before = std::fs::read(root.join(MANIFEST)).unwrap();
        let old = SearchOutOfCoreReader::open(&root).unwrap();
        assert_complete(&old, &expected_documents(publication, false));
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
            Publication::Mutation => mutate(&root),
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
        }
        let cut = fixture.model.take_observation().unwrap().unwrap();
        let acknowledged = fixture.model.capture().unwrap();
        let after = std::fs::read(root.join(MANIFEST)).unwrap();
        assert_ne!(
            before, after,
            "publication must replace the active selector"
        );
        let new = SearchOutOfCoreReader::open(&root).unwrap();
        assert_ne!(old.generation(), new.generation());
        assert_complete(&new, &expected_documents(publication, true));
        let mut modes = vec![SearchMode::Text];
        #[cfg(feature = "vector-search")]
        modes.extend([SearchMode::Vector, SearchMode::Hybrid]);
        let queries: Vec<_> = modes
            .drain(..)
            .flat_map(|mode| {
                [None, Some("default"), Some("current")].map(|space| {
                    (
                        mode,
                        space,
                        search(&old, mode, space),
                        search(&new, mode, space),
                    )
                })
            })
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
                    old.generation()
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
