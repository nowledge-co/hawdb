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

fn artifact_names(generation: u64) -> [String; 5] {
    [
        relational_row_page_artifact_file(generation),
        relational_row_page_root_descriptor_file(generation),
        relational_row_page_root_key_file(generation),
        relational_row_page_manifest_generation_file(generation),
        RELATIONAL_ROW_PAGE_MANIFEST_FILE.into(),
    ]
}

fn authority(directory: &std::path::Path) -> Vec<Vec<u8>> {
    artifact_names(1)
        .iter()
        .map(|name| fs::read(directory.join(name)).unwrap())
        .collect()
}

fn setup(directory: &std::path::Path) -> RelationalRowPageRootReader {
    let config = RelationalRowPagePublicationConfig::default();
    RelationalRowPagePublisher::new(config)
        .publish(
            directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap();
    RelationalRowPageRootReader::open_latest(directory, config)
        .unwrap()
        .unwrap()
}

fn changed_page(generation: u64) -> ImmutableRelationalRowPage {
    let mut changed = page(1, generation, 11, 1, 2);
    for entry in &mut changed.rows {
        entry.row = RelationalRow::new(vec![
            entry.row.values()[0].clone(),
            RelationalValue::Text(format!("changed-{:?}", entry.primary_key)),
        ]);
    }
    changed
}

fn deltas(generation: u64) -> Vec<RelationalRowPageTableDelta> {
    let mut delta = table_delta("documents", vec![changed_page(generation)]);
    delta.next_page_id = NonZeroU64::new(3).unwrap();
    vec![delta]
}

fn candidate(
    directory: &std::path::Path,
    base: &RelationalRowPageRootReader,
    mode: usize,
    generation: u64,
) -> Result<RelationalRowPagePublicationReport, RelationalRowPagePublicationError> {
    let publisher = RelationalRowPagePublisher::new(RelationalRowPagePublicationConfig::default());
    let request = RelationalRowPageGenerationRequest {
        directory,
        generation,
        source_commit_epoch: 11,
        base: Some(base),
        expected_previous_generation: Some(1),
        overflow_root: None,
    };
    match mode {
        0 => publisher.persist_generation(request, deltas(generation)),
        1 => publisher.persist_generation_compacting(
            request,
            deltas(generation),
            RelationalRowPageRewriteConfig {
                max_live_ratio_percent: 100,
                ..RelationalRowPageRewriteConfig::default()
            },
            &hawdb_core::RuntimeTaskContext::default(),
        ),
        2 => publisher.publish(directory, generation, 11, Some(1), deltas(generation)),
        _ => unreachable!(),
    }
}

fn verify_generation(directory: &std::path::Path, generation: u64, compacted: bool) {
    let reader = RelationalRowPageRootReader::open_generation(
        directory,
        generation,
        RelationalRowPagePublicationConfig::default(),
    )
    .unwrap();
    reader.scrub_physical_pages().unwrap();
    assert_eq!(reader.manifest().source_commit_epoch, 11);
    assert_eq!(reader.manifest().root_page_count, 2);
    let descriptors = collect_descriptors(&reader, "documents");
    assert_eq!(page_id_values(&descriptors), vec![1, 2]);
    assert_eq!(
        physical_generations(&descriptors),
        vec![generation, if compacted { generation } else { 1 }]
    );
    let actual = descriptors
        .iter()
        .flat_map(|descriptor| reader.read_page(descriptor).unwrap().rows)
        .collect::<Vec<_>>();
    let expected = changed_page(generation)
        .rows
        .into_iter()
        .chain(page(2, 1, 10, 3, 4).rows)
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

fn assert_only_evidence(directory: &std::path::Path, evidence: &[PathBuf]) {
    let mut actual = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension() == Some(std::ffi::OsStr::new("tmp")))
        .collect::<Vec<_>>();
    actual.sort();
    let mut expected = evidence.to_vec();
    expected.sort();
    assert_eq!(actual, expected);
}

#[test]
fn checkpoint_units_row_publication_preserves_each_unowned_temporary_and_recovers_complete_candidate(
) {
    for mode in 0..3 {
        for stage in 0..5 {
            let directory = unique_test_dir("owned-temporary");
            let base = setup(&directory);
            let before = authority(&directory);
            let names = artifact_names(2);
            let evidence = directory.join(&names[stage]).with_extension("hawdb.tmp");
            let evidence_bytes = b"interrupted row candidate: every byte must survive";
            fs::write(&evidence, evidence_bytes).unwrap();
            let result = candidate(&directory, &base, mode, 2);
            if stage == 4 && mode != 2 {
                result.unwrap();
            } else {
                assert!(
                    matches!(
                        result,
                        Err(RelationalRowPagePublicationError::Durability(_))
                    ),
                    "{result:?}"
                );
            }
            assert_eq!(authority(&directory), before);
            assert_eq!(fs::read(&evidence).unwrap(), evidence_bytes);
            assert_only_evidence(&directory, std::slice::from_ref(&evidence));
            for name in names.iter().take(4) {
                assert_eq!(directory.join(name).exists(), stage == 4);
            }
            // Reopening still sees every selected base row, including after a
            // failed selector creation left a fully durable immutable candidate.
            let selected = RelationalRowPageRootReader::open_latest(
                &directory,
                RelationalRowPagePublicationConfig::default(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(selected.manifest().generation, 1);
            selected.scrub_physical_pages().unwrap();
            assert_eq!(
                collect_descriptors(&selected, "documents"),
                collect_descriptors(&base, "documents")
            );
            for descriptor in collect_descriptors(&base, "documents") {
                assert_eq!(
                    selected.read_page(&descriptor).unwrap(),
                    base.read_page(&descriptor).unwrap()
                );
            }
            // Only the fixture owns this preexisting file. Required-path errors
            // clean every successfully created temporary, permitting full retry.
            fs::remove_file(&evidence).unwrap();
            if stage < 4 {
                candidate(&directory, &base, mode, 2).unwrap();
            }
            verify_generation(&directory, 2, mode == 1);
            assert_no_temporary_files(&directory);
            fs::remove_dir_all(directory).unwrap();
        }
    }
}

#[test]
fn checkpoint_units_row_publication_fault_cleanup_preserves_evidence_and_published_generations() {
    for phase in [
        RelationalRowPagePublicationPhase::CandidateStarted,
        RelationalRowPagePublicationPhase::CandidatePagesDurable,
        RelationalRowPagePublicationPhase::CandidateRootDurable,
        RelationalRowPagePublicationPhase::CandidateManifestDurable,
        RelationalRowPagePublicationPhase::BaseRevalidated,
    ] {
        let directory = unique_test_dir("fault-evidence");
        let base = setup(&directory);
        let before = authority(&directory);
        let evidence = artifact_names(99)
            .iter()
            .map(|name| directory.join(name).with_extension("hawdb.tmp"))
            .collect::<Vec<_>>();
        for (index, path) in evidence.iter().enumerate() {
            fs::write(path, [index as u8; 31]).unwrap();
        }
        let publisher =
            RelationalRowPagePublisher::new(RelationalRowPagePublicationConfig::default());
        let error = publisher
            .publish_inner(
                &directory,
                2,
                11,
                Some(1),
                deltas(2),
                publisher::PublicationControls {
                    overflow_root: None,
                    stop_after: Some(phase),
                },
            )
            .unwrap_err();
        assert!(
            matches!(error, RelationalRowPagePublicationError::Durability(message) if message.contains("injected stop"))
        );
        assert_eq!(authority(&directory), before);
        assert_only_evidence(&directory, &evidence);
        for (index, path) in evidence.iter().enumerate() {
            assert_eq!(fs::read(path).unwrap(), [index as u8; 31]);
        }
        let names = artifact_names(2);
        let published = match phase {
            RelationalRowPagePublicationPhase::CandidateStarted => 0,
            RelationalRowPagePublicationPhase::CandidatePagesDurable => 1,
            RelationalRowPagePublicationPhase::CandidateRootDurable => 3,
            _ => 4,
        };
        for (index, name) in names.iter().take(4).enumerate() {
            assert_eq!(directory.join(name).exists(), index < published);
        }
        if published == 4 {
            verify_generation(&directory, 2, false);
        }
        candidate(&directory, &base, 0, 3).unwrap();
        verify_generation(&directory, 3, false);
        assert_eq!(authority(&directory), before);
        assert_only_evidence(&directory, &evidence);
        fs::remove_dir_all(directory).unwrap();
    }
}
