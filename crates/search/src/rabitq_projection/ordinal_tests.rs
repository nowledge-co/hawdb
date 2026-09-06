use super::*;
use crate::{
    SearchEmbeddingManifest, SearchIndex, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationWriter,
};
use std::fs;

#[test]
fn resident_projection_numbers_only_vector_documents_in_document_id_order() {
    let documents = [
        ("z-vector", Some(vec![0.0, 1.0])),
        ("a-no-vector", None),
        ("b-vector", Some(vec![1.0, 0.0])),
        ("c-no-vector", None),
    ]
    .into_iter()
    .map(|(id, embedding)| {
        (
            id.to_owned(),
            SearchDocument {
                id: id.to_owned(),
                title: String::new(),
                content: String::new(),
                embedding,
                metadata: BTreeMap::new(),
            },
        )
    })
    .collect();
    let projection = RaBitQCandidateProjection::build_from_documents(
        &documents,
        ProjectionIdentity::new(1),
        RaBitQCandidateProjectionBuildOptions::default(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(projection.manifest().document_count, 2);
    assert_eq!(
        projection.manifest().source_digest,
        skein_vector_projection::source_digest([
            (0, [1.0, 0.0].as_slice()),
            (1, [0.0, 1.0].as_slice()),
        ])
    );
}

#[test]
fn seeded_resident_and_file_candidates_match_independently_numbered_vectors() {
    let root = super::tests::unique_test_dir("ordinal-oracle");
    fs::create_dir_all(&root).unwrap();
    let mut comparisons = 0;
    let mut tied_outputs = 0;
    let mut negative_outputs = 0;
    for seed in 0..24 {
        let documents = seeded_documents(seed, [2, 3, 17, 33, 65, 129][seed as usize % 6]);
        let vectors = documents
            .values()
            .filter_map(|document| {
                document
                    .embedding
                    .as_deref()
                    .map(|vector| (document.id.as_str(), vector))
            })
            .collect::<Vec<_>>();
        let subset = vectors
            .iter()
            .step_by(3)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        let duplicated = [vectors[0].0, "missing", vectors[0].0, "a-vectorless"];
        for bit_width in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
            let options = RaBitQCandidateProjectionBuildOptions {
                bit_width,
                segment_rows: [1, 7, 32][seed as usize % 3],
                transform_seed: seed,
                ..RaBitQCandidateProjectionBuildOptions::default()
            };
            let identity = ProjectionIdentity::new(seed + 1);
            // Construct the oracle through the raw crate, without the facade's
            // ID mapper or configuration helper.
            let mut builder = ProjectionBuilder::new(
                ProjectionBuildConfig::new(4, identity.clone())
                    .with_bit_width(bit_width)
                    .with_segment_rows(options.segment_rows)
                    .with_transform_seed(seed)
                    .with_max_working_bytes(options.max_working_bytes),
            )
            .unwrap();
            for (ordinal, (_, vector)) in vectors.iter().enumerate() {
                builder.push(ordinal as u64, vector).unwrap();
            }
            let reference = builder.finish().unwrap();
            let resident = RaBitQCandidateProjection::build_from_documents(
                &documents,
                identity.clone(),
                options,
            )
            .unwrap()
            .unwrap();
            let path = root.join(format!("{seed}-{bit_width:?}.skein"));
            let written = RaBitQCandidateProjection::write_from_documents(
                &path,
                &documents,
                identity.clone(),
                options,
            )
            .unwrap()
            .unwrap();
            drop(written);
            let loaded =
                RaBitQCandidateProjection::load_from_path(&path, &documents, &identity).unwrap();
            assert_eq!(
                resident.manifest().source_digest,
                reference.manifest().source_digest
            );
            assert_eq!(
                loaded.manifest().source_digest,
                reference.manifest().source_digest
            );

            for query in [[0.0; 4], [1.0, -1.0, 0.5, 0.0], [-1.0, 0.0, 0.0, 0.0]] {
                for limit in [0, 1, 5, vectors.len() + 3] {
                    for allowed in [
                        None,
                        Some([].as_slice()),
                        Some(subset.as_slice()),
                        Some(duplicated.as_slice()),
                    ] {
                        let allowed_ordinals = allowed.map(|ids| {
                            vectors
                                .iter()
                                .enumerate()
                                .filter(|(_, (id, _))| ids.contains(id))
                                .map(|(ordinal, _)| ordinal as u64)
                                .collect::<Vec<_>>()
                        });
                        let mut scan = ProjectionSearchOptions::new()
                            .with_kernel(KernelPreference::Scalar)
                            .with_max_parallelism(NonZeroUsize::new(2).unwrap());
                        if let Some(ids) = &allowed_ordinals {
                            scan = scan.with_allowed_ids(ids);
                        }
                        let expected = reference
                            .search(&query, limit, scan)
                            .unwrap()
                            .hits
                            .into_iter()
                            .map(|hit| RaBitQCandidate {
                                id: vectors[hit.id as usize].0.to_owned(),
                                score: f64::from(hit.score),
                            })
                            .collect::<Vec<_>>();
                        tied_outputs += usize::from(
                            expected
                                .windows(2)
                                .any(|pair| pair[0].score == pair[1].score),
                        );
                        negative_outputs += usize::from(expected.iter().any(|hit| hit.score < 0.0));
                        for projection in [&resident, &loaded] {
                            let scan = RaBitQCandidateScanOptions {
                                kernel: KernelPreference::Scalar,
                                max_parallelism: NonZeroUsize::new(2).unwrap(),
                                ..RaBitQCandidateScanOptions::default()
                            };
                            let actual = projection
                                .search_with_options(&query, limit, allowed, scan)
                                .unwrap();
                            assert_eq!(
                                actual.candidates, expected,
                                "seed={seed} bits={bit_width:?} limit={limit} allowed={allowed:?}"
                            );
                            comparisons += 1;
                            if let Some(ids) = allowed {
                                let allowed_documents = ids
                                    .iter()
                                    .filter_map(|id| documents.get(*id))
                                    .collect::<Vec<_>>();
                                let actual = projection
                                    .search_candidates_for_documents_with_options(
                                        &query,
                                        limit,
                                        Some(&allowed_documents),
                                        scan,
                                    )
                                    .unwrap();
                                assert_eq!(actual.candidates, expected);
                                comparisons += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    assert_eq!(comparisons, 8_064);
    assert!(tied_outputs > 0);
    assert!(negative_outputs > 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn resident_and_out_of_core_writers_emit_identical_vector_artifacts() {
    let root = super::tests::unique_test_dir("ordinal-out-of-core");
    fs::create_dir_all(&root).unwrap();
    for bit_width in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
        for count in [0, 1, 2, 33, 129] {
            let documents = seeded_documents(count as u64, count);
            let directory = root.join(format!("{count}-{bit_width:?}"));
            let options = SearchOutOfCoreGenerationBuildOptions {
                rabitq_bit_width: bit_width,
                rabitq_segment_rows: NonZeroUsize::new(7).unwrap(),
                rabitq_transform_seed: 17,
                source_graph_commit_epoch: Some(19),
                embedding_manifest: Some(SearchEmbeddingManifest {
                    model: "ordinal-test-model".to_owned(),
                    version: Some("v1".to_owned()),
                    dimension: 4,
                }),
                ..SearchOutOfCoreGenerationBuildOptions::default()
            };
            let mut writer =
                SearchOutOfCoreGenerationWriter::create(&directory, options.clone()).unwrap();
            for document in documents.values() {
                writer.push(document.clone()).unwrap();
            }
            let report = writer.finish().unwrap();
            let identity = ProjectionIdentity {
                generation: report.generation,
                source_epoch: Some(19),
                embedding_model: Some("ordinal-test-model".to_owned()),
                embedding_version: Some("v1".to_owned()),
            };
            let path = directory.join("resident.skein");
            let resident = RaBitQCandidateProjection::write_from_documents(
                &path,
                &documents,
                identity.clone(),
                RaBitQCandidateProjectionBuildOptions {
                    bit_width,
                    segment_rows: options.rabitq_segment_rows.get(),
                    max_working_bytes: options.rabitq_build_memory_bytes.get(),
                    transform_seed: options.rabitq_transform_seed,
                },
            )
            .unwrap();
            let out_of_core_path = directory.join(crate::rabitq_artifact_file(report.generation));
            let Some(resident) = resident else {
                assert_eq!(count, 0);
                assert_eq!(report.vector_document_count, 0);
                assert_eq!(report.rabitq_source_digest, None);
                assert!(!path.exists());
                assert!(!out_of_core_path.exists());
                continue;
            };
            assert_eq!(
                report.vector_document_count,
                resident.manifest().document_count
            );
            assert_eq!(
                report.rabitq_source_digest,
                Some(resident.manifest().source_digest)
            );
            assert_eq!(
                fs::read(&path).unwrap(),
                fs::read(&out_of_core_path).unwrap()
            );
            let loaded =
                RaBitQCandidateProjection::load_from_path(&out_of_core_path, &documents, &identity)
                    .unwrap();
            assert_eq!(
                loaded
                    .search(&[0.0; 4], report.vector_document_count + 1, None)
                    .unwrap()
                    .candidates,
                resident
                    .search(&[0.0; 4], report.vector_document_count + 1, None)
                    .unwrap()
                    .candidates,
            );
        }
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn checkpoint_reopen_restores_generation_local_mapping_after_mutations() {
    let root = super::tests::unique_test_dir("ordinal-reopen");
    let mut index = SearchIndex::open(&root).unwrap();
    for (id, vector) in [
        ("b", Some(vec![1.0, 0.0])),
        ("c", None),
        ("d", Some(vec![0.0, 1.0])),
    ] {
        index.upsert(document(id, vector)).unwrap();
    }
    index.checkpoint().unwrap();
    drop(index);
    let mut index = SearchIndex::open(&root).unwrap();
    let previous = index.rabitq_projection().unwrap();
    assert!(previous.is_file_backed());
    assert_eq!(previous.ordinal_to_document_id, ["b", "d"]);
    let previous_generation = previous.manifest().identity.generation;
    assert_eq!(index.document_count(), 3);
    assert_eq!(index.document("c").unwrap(), &document("c", None));

    index.delete("b");
    index.upsert(document("a", Some(vec![1.0, 1.0]))).unwrap();
    index.upsert(document("c", Some(vec![1.0, -1.0]))).unwrap();
    index.upsert(document("d", None)).unwrap();
    index.upsert(document("z", Some(vec![0.0, 1.0]))).unwrap();
    assert!(index.rabitq_projection().is_none());
    let rebuilt = index.build_in_memory_rabitq_projection().unwrap().unwrap();
    assert_eq!(rebuilt.ordinal_to_document_id, ["a", "c", "z"]);
    let expected_documents = index.documents.clone();
    index.checkpoint().unwrap();
    drop(rebuilt);
    drop(index);

    let reopened = SearchIndex::open(&root).unwrap();
    assert_eq!(reopened.documents, expected_documents);
    let current = reopened.rabitq_projection().unwrap();
    assert!(current.is_file_backed());
    assert!(current.manifest().identity.generation > previous_generation);
    assert_eq!(current.ordinal_to_document_id, ["a", "c", "z"]);
    assert_eq!(
        current
            .search(&[0.0; 2], 10, None)
            .unwrap()
            .candidates
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>(),
        ["a", "c", "z"]
    );
    assert_eq!(
        previous
            .search(&[0.0; 2], 10, None)
            .unwrap()
            .candidates
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>(),
        ["b", "d"]
    );
    let allowed = ["b", "d", "z", "z", "missing"];
    assert_eq!(
        current
            .search(&[0.0; 2], 10, Some(&allowed))
            .unwrap()
            .candidates
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>(),
        ["z"]
    );
    assert_eq!(
        previous
            .search(&[0.0; 2], 10, Some(&allowed))
            .unwrap()
            .candidates
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>(),
        ["b", "d"]
    );
    drop(current);
    drop(previous);
    drop(reopened);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn load_binds_ordinals_to_the_ordered_vector_stream_without_quarantining_stale_artifacts() {
    let root = super::tests::unique_test_dir("ordinal-applicability");
    fs::create_dir_all(&root).unwrap();
    let path = root.join("vectors.skein");
    let identity = ProjectionIdentity::new(9);
    let documents = [
        document("b", Some(vec![1.0, 0.0])),
        document("d", Some(vec![0.0, 1.0])),
    ]
    .into_iter()
    .map(|document| (document.id.clone(), document))
    .collect::<BTreeMap<_, _>>();
    drop(
        RaBitQCandidateProjection::write_from_documents(
            &path,
            &documents,
            identity.clone(),
            RaBitQCandidateProjectionBuildOptions::default(),
        )
        .unwrap()
        .unwrap(),
    );
    for change in 0..5 {
        let mut changed = documents.clone();
        let mut expected_identity = identity.clone();
        match change {
            0 => {
                changed.remove("b");
            }
            1 => {
                changed.get_mut("b").unwrap().embedding = Some(vec![0.5, 0.5]);
            }
            2 => {
                expected_identity.source_epoch = Some(1);
            }
            3 => {
                changed.clear();
            }
            4 => {
                changed.get_mut("b").unwrap().embedding = Some(vec![0.0, 1.0]);
                changed.get_mut("d").unwrap().embedding = Some(vec![1.0, 0.0]);
            }
            _ => unreachable!(),
        }
        let error = RaBitQCandidateProjection::load_from_path_classified(
            &path,
            &changed,
            &expected_identity,
        )
        .unwrap_err();
        assert!(!error.should_quarantine(), "change={change}: {error:?}");
        assert!(matches!(
            error,
            RaBitQCandidateProjectionLoadError::NotApplicable(_)
        ));
        assert!(path.exists());
    }
    // IDs are persisted in the canonical snapshot. A rename that preserves
    // vector order can reuse the numeric artifact and resolves the new ID.
    let mut renamed = documents.clone();
    let mut first = renamed.remove("b").unwrap();
    first.id = "a".to_owned();
    renamed.insert(first.id.clone(), first);
    renamed.insert("c".to_owned(), document("c", None));
    let loaded = RaBitQCandidateProjection::load_from_path(&path, &renamed, &identity).unwrap();
    assert_eq!(loaded.ordinal_to_document_id, ["a", "d"]);
    assert_eq!(
        loaded
            .search(&[0.0; 2], 10, None)
            .unwrap()
            .candidates
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>(),
        ["a", "d"]
    );
    drop(loaded);

    // Sparse numeric IDs from an earlier derived layout are not corrupt, but
    // must not be interpreted through a new dense mapping.
    let sparse_path = root.join("sparse.skein");
    let mut writer = ProjectionWriter::create(
        &sparse_path,
        ProjectionBuildConfig::new(2, identity.clone()),
    )
    .unwrap();
    writer.push(7, &[1.0, 0.0]).unwrap();
    writer.push(19, &[0.0, 1.0]).unwrap();
    drop(writer.finish().unwrap());
    let error =
        RaBitQCandidateProjection::load_from_path_classified(&sparse_path, &documents, &identity)
            .unwrap_err();
    assert!(!error.should_quarantine());
    assert!(matches!(
        error,
        RaBitQCandidateProjectionLoadError::NotApplicable(_)
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn ordinal_validation_rejects_invalid_documents_before_creating_artifacts() {
    let root = super::tests::unique_test_dir("ordinal-validation");
    fs::create_dir_all(&root).unwrap();
    let path = root.join("must-not-exist.skein");
    let valid = document("a", Some(vec![1.0, 0.0]));
    for invalid in [
        BTreeMap::from([("different-key".to_owned(), valid.clone())]),
        BTreeMap::from([
            ("a".to_owned(), valid.clone()),
            ("b".to_owned(), valid.clone()),
        ]),
        BTreeMap::from([("different-key".to_owned(), document("a", None))]),
        BTreeMap::from([("a".to_owned(), document("a", Some(vec![])))]),
        BTreeMap::from([("a".to_owned(), document("a", Some(vec![f32::NAN, 0.0])))]),
        BTreeMap::from([(
            "a".to_owned(),
            document("a", Some(vec![f32::INFINITY, 0.0])),
        )]),
        BTreeMap::from([
            ("a".to_owned(), valid),
            ("b".to_owned(), document("b", Some(vec![1.0]))),
        ]),
    ] {
        let result = RaBitQCandidateProjection::write_from_documents(
            &path,
            &invalid,
            ProjectionIdentity::new(1),
            RaBitQCandidateProjectionBuildOptions::default(),
        );
        assert!(matches!(result, Err(SkeinError::Storage(_))));
        assert!(!path.exists());
        assert!(RaBitQCandidateProjection::build_from_documents(
            &invalid,
            ProjectionIdentity::new(1),
            RaBitQCandidateProjectionBuildOptions::default(),
        )
        .is_err());
    }
    for documents in [
        BTreeMap::new(),
        BTreeMap::from([("a".to_owned(), document("a", None))]),
    ] {
        assert!(RaBitQCandidateProjection::write_from_documents(
            &path,
            &documents,
            ProjectionIdentity::new(1),
            RaBitQCandidateProjectionBuildOptions::default(),
        )
        .unwrap()
        .is_none());
        assert!(!path.exists());
    }
    fs::remove_dir_all(root).unwrap();
}

fn document(id: &str, embedding: Option<Vec<f32>>) -> SearchDocument {
    SearchDocument {
        id: id.to_owned(),
        title: "ordinal fixture".to_owned(),
        content: String::new(),
        embedding,
        metadata: BTreeMap::new(),
    }
}

fn seeded_documents(seed: u64, count: usize) -> BTreeMap<String, SearchDocument> {
    let mut state = seed + 1;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut documents = vec![document("a-vectorless", None)];
    for index in 0..count {
        let vector = match index % 5 {
            0 => None,
            1 | 2 => Some(vec![1.0, 0.0, 0.0, 0.0]),
            _ => Some((0..4).map(|_| (next() % 5) as f32 - 2.0).collect()),
        };
        documents.push(document(&format!("doc-{index:04}-\u{e9}"), vector));
    }
    if count > 0 {
        documents.push(document("z-vector", Some(vec![0.0, 1.0, 0.0, 0.0])));
    }
    for index in (1..documents.len()).rev() {
        documents.swap(index, next() as usize % (index + 1));
    }
    documents
        .into_iter()
        .map(|document| (document.id.clone(), document))
        .collect()
}
