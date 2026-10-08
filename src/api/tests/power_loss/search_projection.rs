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
    SearchDocument, SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader,
    SearchOutOfCoreSegmentCompactionPolicy, SearchProjectionDelta, SearchProjectionKind,
    SearchProjectionRow,
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
