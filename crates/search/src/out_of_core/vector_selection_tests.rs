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

type DocumentBits = (String, Vec<u32>);

struct ReferenceVectorDocument {
    vector_ordinal: u64,
    id: String,
    embedding: Vec<f32>,
}

fn bits(documents: Vec<SearchVectorDocument>) -> Vec<DocumentBits> {
    documents
        .into_iter()
        .map(|document| {
            (
                document.id,
                document.embedding.into_iter().map(f32::to_bits).collect(),
            )
        })
        .collect()
}

fn reference_bits(documents: Vec<ReferenceVectorDocument>) -> Vec<DocumentBits> {
    documents
        .into_iter()
        .map(|document| {
            (
                document.id,
                document.embedding.into_iter().map(f32::to_bits).collect(),
            )
        })
        .collect()
}

fn fixture(embeddings: &[&str], base: u64) -> (String, SearchSegmentDescriptorEntry) {
    let mut text = "HAWDB_SEARCH_VECTOR_SEGMENT_V1\n".to_owned();
    for (index, embedding) in embeddings.iter().enumerate() {
        text.push_str(&format!(
            "vector\t{}\t{}\t{embedding}\n",
            base.saturating_add(index as u64),
            encode_string(&format!("memory:{index:08}")),
        ));
    }
    let segment = SearchSegmentDescriptorEntry {
        segment_id: 3,
        first_document_id: "memory:00000000".into(),
        last_document_id: format!("memory:{:08}", embeddings.len().saturating_sub(1)),
        document_count: embeddings.len(),
        payload_range: None,
        metadata: BTreeMap::new(),
    };
    (text, segment)
}

fn assert_selection(
    text: &str,
    segment: &SearchSegmentDescriptorEntry,
    count: usize,
    base: u64,
    dimension: Option<usize>,
    selected: &[u64],
) {
    let reference = reference_decode_vector_segment(text, segment, count, base, dimension)
        .map(|documents| {
            reference_bits(
                documents
                    .into_iter()
                    .filter(|document| selected.binary_search(&document.vector_ordinal).is_ok())
                    .collect(),
            )
        })
        .map_err(|error| error.to_string());
    let actual = decode_vector_segment(
        text,
        segment,
        count,
        base,
        dimension,
        selected.len(),
        |ordinal| selected.binary_search(&ordinal).is_ok(),
    )
    .map(bits)
    .map_err(|error| error.to_string());
    assert_eq!(actual, reference, "selected={selected:?}, text={text:?}");
}

#[test]
fn every_subset_preserves_embedding_bits_and_ids() {
    let embeddings = [
        "-0,0.1,1e-30,3.4028235e38",
        "0,-0.25,-1e-40,-3.4028235e38",
        "1,-1,0.9999999999,0.0000000000000000001",
        "0.2,+0.1,.5,1.",
        "00.1,-0.,1e-45,0e9999999",
        "0.3,0.4,0.5,0.6",
    ];
    let base = 17;
    let (text, segment) = fixture(&embeddings, base);
    assert_eq!(
        reference_decode_vector_segment(&text, &segment, embeddings.len(), base, Some(4))
            .unwrap()
            .len(),
        embeddings.len()
    );
    for mask in 0..1 << embeddings.len() {
        let selected = (0..embeddings.len())
            .filter(|index| mask & (1 << index) != 0)
            .map(|index| base + index as u64)
            .collect::<Vec<_>>();
        assert_selection(&text, &segment, embeddings.len(), base, Some(4), &selected);
    }
    assert_selection(&text, &segment, embeddings.len(), base, Some(4), &[999]);
    let (text, segment) = fixture(&[], 0);
    assert_selection(&text, &segment, 0, 0, None, &[]);

    // Preserve the original decoder's saturating ordinal arithmetic at the boundary.
    let base = u64::MAX - 1;
    let (text, segment) = fixture(&["0.1", "0.2", "0.3"], base);
    assert_selection(&text, &segment, 3, base, Some(1), &[u64::MAX]);
}

#[test]
fn selected_and_unselected_malformed_rows_keep_the_same_errors() {
    let embeddings = ["0.1,0.2", "0.3,0.4", "0.5,0.6"];
    let base = 7;
    let (valid, segment) = fixture(&embeddings, base);
    let mut cases = vec![
        valid.replacen("HAWDB_SEARCH_VECTOR_SEGMENT_V1", "wrong", 1),
        format!("{valid}\n"),
        valid.replace("vector\t7", "unknown\t7"),
        valid.replace("vector\t7", "vector\t-1"),
        valid.replace("vector\t7", "vector\t18446744073709551616"),
        valid.replace("vector\t7", "vector\t8"),
        valid.replace("vector\t8", "vector\t99"),
        valid.replace(&encode_string("memory:00000001"), "zz"),
        valid.replace(&encode_string("memory:00000001"), "f"),
        valid.replace(&encode_string("memory:00000001"), "ff"),
        valid.replace(&encode_string("memory:00000001"), "\u{ff11}\u{ff11}"),
        valid.replace(
            &encode_string("memory:00000001"),
            &encode_string("memory:00000000"),
        ),
        valid.replace(
            &encode_string("memory:00000002"),
            &encode_string("memory:99999999"),
        ),
        valid.replace("0.3,0.4", "0.3,0.4\textra"),
        valid.lines().take(3).collect::<Vec<_>>().join("\n"),
        format!("{valid}{}\n", valid.lines().nth(3).unwrap()),
    ];
    for raw in [
        "",
        "NaN,0",
        "inf,0",
        "-inf,0",
        "3.4028236e38,0",
        "0.1,x",
        "NaN,x",
        "0.1,",
        "0.1",
        "0.1,0.2,0.3",
        " 0.1,0.2",
        "0.1\0,0.2",
        "0.\u{ff11},0.2",
    ] {
        for original in embeddings {
            cases.push(valid.replace(original, raw));
        }
    }
    // A later parse error still wins over an earlier ordinal/order mismatch.
    cases.push(
        valid
            .replace("vector\t7", "vector\t99")
            .replace("0.5,0.6", "NaN,x"),
    );
    for text in &cases {
        assert!(reference_decode_vector_segment(text, &segment, 3, base, Some(2)).is_err());
        for selected in [
            vec![],
            vec![base],
            vec![base + 1],
            vec![base, base + 1, base + 2],
        ] {
            assert_selection(text, &segment, 3, base, Some(2), &selected);
        }
    }
    for count in [2, 4] {
        assert_selection(&valid, &segment, count, base, Some(2), &[]);
        assert_selection(&valid, &segment, count, base, Some(2), &[base]);
    }
    assert_selection(&valid, &segment, 3, base, None, &[]);
    let mut narrowed = segment.clone();
    narrowed.first_document_id = "memory:00000001".into();
    assert_selection(&valid, &narrowed, 3, base, Some(2), &[base + 2]);
}

#[test]
fn unmaterialized_embedding_validation_matches_float_parsing() {
    let check = |input: &str| {
        let expected = decode_embedding(input)
            .map(|values| {
                values.map_or((0, true), |values| {
                    (values.len(), values.iter().all(|value| value.is_finite()))
                })
            })
            .map_err(|error| error.to_string());
        let actual = validate_unselected_embedding(input).map_err(|error| error.to_string());
        assert_eq!(actual, expected, "{input:?}");
    };
    for input in [
        "",
        "0",
        "-0",
        "1",
        "-1",
        "0.",
        "-0.",
        ".1",
        "+0.1",
        "00.1",
        "0.1e999",
        "0e9999999999999999999999",
        "1e-9999999999999999999",
        "3.4028235e38",
        "3.4028236e38",
        "-3.4028236e38",
        "NaN",
        "Infinity",
        " 0.1",
        "0.1 ",
        "0..1",
        "--0.1",
        "0.1,",
        "0.1\0",
        "0.\u{ff11}",
        "\u{2212}0.1",
        "NaN,bad",
        "0,0.,1.,-0.25,1e2",
        "0.125,NaN,-0.25,bad",
        "0.125,inf,-0.25,0.5",
        "0,0.5,,1",
        "-1,",
        "0.25,-0.5,0.\u{ff11}",
        "0.25,-0.5,\u{2212}0.1",
    ] {
        check(input);
    }
    for length in [1, 7, 64, 1024, 16384] {
        for digit in ['0', '1', '9'] {
            check(&format!("0.{}", digit.to_string().repeat(length)));
            check(&format!("-0.{}", digit.to_string().repeat(length)));
        }
        let common = format!("-0.{}", "1".repeat(length));
        for suffix in ["0.5", "+0.5", "1e39", "NaN,bad", "0.5,", "0.\u{ff11}"] {
            check(&format!("0.125,{common},{suffix}"));
        }
    }
    let mut state = 0x7d91_650e_1f04_9940u64;
    let alphabet = b"0123456789.eE+-NaInf \t,_";
    for iteration in 0..4096 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        check(&f32::from_bits((state >> 32) as u32).to_string());
        let mut input = String::new();
        for _ in 0..iteration % 48 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            input.push(alphabet[(state >> 32) as usize % alphabet.len()] as char);
        }
        check(&input);
    }
}

#[test]
#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
fn required_rerank_preserves_checksum_budget_and_cancellation_guards() {
    use std::io::{Seek, SeekFrom};

    let path = super::tests::test_dir("selected-vector-read-guards");
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &path,
        SearchOutOfCoreGenerationBuildOptions {
            embedding_manifest: Some(SearchEmbeddingManifest {
                model: "selected-vector-test".into(),
                version: Some("v1".into()),
                dimension: 2,
            }),
            vector_backend: None,
            ..Default::default()
        },
    )
    .unwrap();
    for number in 0..8 {
        writer
            .push(SearchDocument {
                id: format!("memory:{number:03}"),
                title: "Selected vector test".into(),
                content: "Synthetic content".into(),
                embedding: Some(vec![number as f32 + 1.0, 16.0 - number as f32]),
                metadata: BTreeMap::new(),
            })
            .unwrap();
    }
    writer.finish().unwrap();
    for candidate_limit in [1, 8] {
        let reader = SearchOutOfCoreReader::open_with_config(
            &path,
            SearchOutOfCoreConfig {
                max_vector_candidates: NonZeroUsize::new(candidate_limit).unwrap(),
                ..Default::default()
            },
        )
        .unwrap();
        let query = |execution| {
            reader.search_with_options_compressed_vector_projection_execution_options(
                "",
                Some(&[1.0, 0.5]),
                SearchMode::Vector,
                super::tests::options(1, None),
                CompressedVectorSearchMode::Required,
                execution,
            )
        };
        let initial = query(VectorSearchExecutionOptions::default()).unwrap();
        let report = initial
            .result
            .retrievers
            .iter()
            .find(|report| report.name == "vector")
            .unwrap();
        assert_eq!(
            report.backend,
            "hawdb_rabitq_out_of_core_candidate_projection"
        );
        assert_eq!(report.reranked_candidate_count, candidate_limit);
        assert_eq!(initial.result.hits.len(), 1);
        assert_eq!(
            initial.metrics.vector_bytes_read,
            8 * candidate_limit as u64
        );
        assert!(report.fallback_reason_codes.is_empty());

        if candidate_limit == 8 {
            let raw = reader
                .search_with_options_compressed_vector_projection_execution_options(
                    "",
                    Some(&[1.0, 0.5]),
                    SearchMode::Vector,
                    super::tests::options(1, None),
                    CompressedVectorSearchMode::Disabled,
                    VectorSearchExecutionOptions::default(),
                )
                .unwrap();
            assert_eq!(raw.result.hits[0].id, initial.result.hits[0].id);
            assert_eq!(
                raw.result.hits[0].vector_score.to_bits(),
                initial.result.hits[0].vector_score.to_bits()
            );
        }

        let error = query(VectorSearchExecutionOptions::bounded(
            NonZeroUsize::MIN,
            1,
            None,
        ))
        .unwrap_err();
        assert!(
            matches!(&error, HawDBError::ResourceBudgetExceeded(cause)
                if cause.resource == hawdb_core::ResourceBudgetKind::MemoryBytes
                    && cause.scope == hawdb_core::ResourceBudgetScope::Operator
                    && cause.owner == "RaBitQ candidate scan"
                    && cause.used == 0
                    && cause.budget == 1
                    && cause.requested > cause.budget
                    && cause.budget_option
                        == "SearchOutOfCoreConfig.max_vector_search_working_bytes"),
            "{error:?}"
        );

        let cancellation = crate::RuntimeCancellationToken::new();
        let task_context = crate::RuntimeTaskContext::without_deadline(cancellation.clone());
        assert!(cancellation.cancel());
        let error = query(VectorSearchExecutionOptions::bounded(
            NonZeroUsize::MIN,
            4 * 1024 * 1024,
            Some(&task_context),
        ))
        .unwrap_err();
        assert!(error.to_string().contains("cancelled"), "{error}");
        let repeated = query(VectorSearchExecutionOptions::default()).unwrap();
        assert_eq!(repeated.result.hits[0].id, initial.result.hits[0].id);
        assert_eq!(
            repeated.result.hits[0].vector_score.to_bits(),
            initial.result.hits[0].vector_score.to_bits()
        );

        assert_eq!(reader.segments.len(), 1);
        let artifact = &reader.segments[0];
        assert!(artifact.descriptor.segments.len() > 1);
        let hit_id = initial.result.hits[0].id.as_str();
        let segment = artifact
            .descriptor
            .segments
            .iter()
            .find(|segment| {
                segment.first_document_id.as_str() <= hit_id
                    && hit_id <= segment.last_document_id.as_str()
            })
            .unwrap();
        let offset = SearchOutOfCoreReader::artifact_layout_range(artifact, segment.segment_id)
            .unwrap()
            .vectors
            .offset;
        assert!(offset > 0);
        let payload = path.join(&reader.manifest.segments[0].vector_payload_file);
        let original_payload = fs::read(&payload).unwrap();
        let mut file = OpenOptions::new().write(true).open(&payload).unwrap();
        // Sparse reranking only reads frames containing selected candidates.
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&[original_payload[usize::try_from(offset).unwrap()] ^ 0xff])
            .unwrap();
        file.sync_all().unwrap();
        drop(file);
        let error = query(VectorSearchExecutionOptions::default()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("vector payload checksum mismatch"),
            "{error}"
        );
        drop(reader);
        fs::write(payload, original_payload).unwrap();
    }
    fs::remove_dir_all(path).unwrap();
}

#[test]
#[ignore = "manual release-mode differential campaign and decoder timing matrix"]
fn selected_vector_decode_matrix() -> Result<()> {
    use std::hint::black_box;
    use std::time::Instant;

    if cfg!(debug_assertions) {
        return Err(HawDBError::Storage("run the matrix with --release".into()));
    }
    enum Decoded {
        Selected(Vec<SearchVectorDocument>),
        Full(Vec<ReferenceVectorDocument>),
    }
    let mut state = 0x1067_2026_7281_956fu64;
    let alphabet = b"0123456789.eE+-NaInf ,\t_";
    for iteration in 0..20_000 {
        let mut value = String::new();
        for _ in 0..iteration % 80 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            value.push(alphabet[(state >> 32) as usize % alphabet.len()] as char);
        }
        if iteration % 8 == 0 {
            value = "-0,0.1,1e-30,3.4028235e38".into();
        }
        let base = 13;
        let (mut text, segment) =
            fixture(&["0.1,0.2,0.3,0.4", &value, "-0.1,-0.2,-0.3,-0.4"], base);
        if iteration % 7 == 0 {
            text = text.replace("vector\t13", "vector\t15");
        }
        let selected = (0..3)
            .filter(|index| state & (1 << index) != 0)
            .map(|index| base + index)
            .collect::<Vec<_>>();
        assert_selection(&text, &segment, 3, base, Some(4), &selected);
    }
    println!(
        "{}",
        serde_json::json!({"phase": "campaign", "cases": 20000})
    );

    for rows in [1, 32, 8192] {
        for dimension in [3, 384] {
            let embedding = (0..dimension)
                .map(|index| {
                    let value = (index + 1) as f32 / (dimension * 100) as f32;
                    if index % 2 == 0 { value } else { -value }.to_string()
                })
                .collect::<Vec<_>>()
                .join(",");
            let embeddings = vec![embedding.as_str(); rows];
            let (text, segment) = fixture(&embeddings, 0);
            for stride in [1, 2, 100] {
                let selected = (0..rows as u64).step_by(stride).collect::<Vec<_>>();
                let expected = reference_bits(
                    reference_decode_vector_segment(&text, &segment, rows, 0, Some(dimension))
                        .unwrap()
                        .into_iter()
                        .filter(|document| selected.binary_search(&document.vector_ordinal).is_ok())
                        .collect(),
                );
                for phase in ["selected_preparation", "full_decode"] {
                    if phase == "full_decode" && stride != 1 {
                        continue;
                    }
                    for sample in 0..=5 {
                        let order = if sample % 2 == 0 {
                            [false, true]
                        } else {
                            [true, false]
                        };
                        for retain_selected in order {
                            let started = Instant::now();
                            let decoded = if !retain_selected {
                                Decoded::Full(reference_decode_vector_segment(
                                    black_box(&text),
                                    &segment,
                                    rows,
                                    0,
                                    Some(dimension),
                                )?)
                            } else if phase == "full_decode" {
                                Decoded::Selected(decode_vector_segment(
                                    black_box(&text),
                                    &segment,
                                    rows,
                                    0,
                                    Some(dimension),
                                    rows,
                                    |_| true,
                                )?)
                            } else {
                                Decoded::Selected(decode_vector_segment(
                                    black_box(&text),
                                    &segment,
                                    rows,
                                    0,
                                    Some(dimension),
                                    selected.len(),
                                    |ordinal| selected.binary_search(&ordinal).is_ok(),
                                )?)
                            };
                            // Compare the same preparation boundary: the old path
                            // filters after decoding, while the new path already selected.
                            // Touch the same returned values without copying embeddings
                            // or adding rerank arithmetic that could hide decoder costs.
                            let mut visited = 0;
                            match &decoded {
                                Decoded::Selected(documents) => {
                                    for document in documents {
                                        black_box((&document.id, &document.embedding));
                                        visited += 1;
                                    }
                                }
                                Decoded::Full(documents) if phase == "full_decode" => {
                                    for document in documents {
                                        black_box((&document.id, &document.embedding));
                                        visited += 1;
                                    }
                                }
                                Decoded::Full(documents) => {
                                    for document in documents {
                                        if selected.binary_search(&document.vector_ordinal).is_ok()
                                        {
                                            black_box((&document.id, &document.embedding));
                                            visited += 1;
                                        }
                                    }
                                }
                            }
                            let elapsed = started.elapsed().as_nanos();
                            assert_eq!(visited, selected.len());
                            // Bit validation and destruction are outside both timers.
                            let actual = match decoded {
                                Decoded::Selected(documents) => bits(documents),
                                Decoded::Full(documents) => reference_bits(
                                    documents
                                        .into_iter()
                                        .filter(|document| {
                                            selected.binary_search(&document.vector_ordinal).is_ok()
                                        })
                                        .collect(),
                                ),
                            };
                            assert_eq!(actual, expected);
                            println!(
                                "{}",
                                serde_json::json!({
                                    "phase": phase, "rows": rows, "dimension": dimension,
                                    "stride": stride, "selected": selected.len(), "sample": sample,
                                    "warmup": sample == 0, "retain_selected": retain_selected,
                                    "elapsed_ns": elapsed, "text_bytes": text.len(),
                                })
                            );
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

// Frozen pre-optimization decoder: validate everything before filtering.
fn reference_decode_vector_segment(
    text: &str,
    segment: &SearchSegmentDescriptorEntry,
    expected_count: usize,
    expected_ordinal_base: u64,
    expected_dimension: Option<usize>,
) -> Result<Vec<ReferenceVectorDocument>> {
    let mut lines = text.lines();
    if lines.next() != Some("HAWDB_SEARCH_VECTOR_SEGMENT_V1") {
        return Err(HawDBError::Storage(format!(
            "search segment {} vector sidecar has an invalid header",
            segment.segment_id
        )));
    }
    let mut documents = Vec::with_capacity(expected_count);
    for line in lines {
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["vector", raw_ordinal, raw_id, raw_embedding] => {
                let vector_ordinal = raw_ordinal.parse::<u64>().map_err(|_| {
                    HawDBError::Storage(format!(
                        "search segment {} vector sidecar has an invalid ordinal",
                        segment.segment_id
                    ))
                })?;
                let embedding = decode_embedding(raw_embedding)?.ok_or_else(|| {
                    HawDBError::Storage(format!(
                        "search segment {} vector sidecar contains an empty embedding",
                        segment.segment_id
                    ))
                })?;
                if embedding.iter().any(|value| !value.is_finite())
                    || expected_dimension.is_none_or(|dimension| embedding.len() != dimension)
                {
                    return Err(HawDBError::Storage(format!(
                        "search segment {} vector sidecar has an invalid embedding",
                        segment.segment_id
                    )));
                }
                documents.push(ReferenceVectorDocument {
                    vector_ordinal,
                    id: decode_string(raw_id)?,
                    embedding,
                });
            }
            _ => {
                return Err(HawDBError::Storage(format!(
                    "search segment {} has an invalid vector sidecar line",
                    segment.segment_id
                )));
            }
        }
    }
    if documents.len() != expected_count
        || documents.iter().enumerate().any(|(offset, document)| {
            document.vector_ordinal != expected_ordinal_base.saturating_add(offset as u64)
        })
        || documents.windows(2).any(|pair| pair[0].id >= pair[1].id)
        || documents.iter().any(|document| {
            document.id < segment.first_document_id || document.id > segment.last_document_id
        })
    {
        return Err(HawDBError::Storage(format!(
            "search segment {} vector sidecar count, bounds, or ordering mismatch",
            segment.segment_id
        )));
    }
    Ok(documents)
}
