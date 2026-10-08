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
