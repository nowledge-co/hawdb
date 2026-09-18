use super::*;

#[derive(Debug)]
struct TestSource {
    candidate_batch: VectorCandidateBatch,
    raw_scores: Vec<VectorRawScore>,
    observed_filter_fields: Vec<String>,
    residual_ids: BTreeSet<String>,
    residual_adds_candidate: bool,
    scan_limits: Vec<usize>,
    capture_candidate_ids: bool,
}

impl VectorExecutionSource for TestSource {
    type Error = &'static str;

    fn scan_candidates(
        &mut self,
        request: VectorCandidateScanRequest<'_>,
    ) -> Result<VectorCandidateBatch, Self::Error> {
        self.observed_filter_fields = request.filter_fields.to_vec();
        self.scan_limits.push(request.candidate_limit);
        Ok(self.candidate_batch.clone())
    }

    fn filter_residual(
        &mut self,
        request: VectorResidualFilterRequest<'_>,
    ) -> Result<Vec<VectorCandidate>, Self::Error> {
        if self.residual_ids.is_empty() {
            let mut candidates = request.candidates;
            if self.residual_adds_candidate {
                candidates.push(VectorCandidate {
                    id: "outside".to_string(),
                    score: 1.0,
                });
            }
            return Ok(candidates);
        }
        Ok(request
            .candidates
            .into_iter()
            .filter(|candidate| self.residual_ids.contains(&candidate.id))
            .collect())
    }

    fn rerank_raw(
        &mut self,
        _request: VectorRawRerankRequest<'_>,
    ) -> Result<Vec<VectorRawScore>, Self::Error> {
        Ok(self.raw_scores.clone())
    }

    fn capture_candidate_ids(&self) -> bool {
        self.capture_candidate_ids
    }
}

fn quantized_plan() -> VectorPhysicalPlan {
    VectorPhysicalPlan::TopK {
        limit: 2,
        input: Box::new(VectorPhysicalPlan::RawVectorRerank {
            embedding_dimension: 3,
            input: Box::new(VectorPhysicalPlan::VectorCandidateScan {
                source: VectorCandidateSource::Quantized,
                embedding_dimension: 3,
                candidate_limit: 3,
                input: Box::new(VectorPhysicalPlan::Filter {
                    fields: vec!["space_id".to_string()],
                }),
            }),
        }),
    }
}

#[test]
fn executes_filter_candidate_rerank_top_k_in_order() {
    let mut source = TestSource {
        candidate_batch: VectorCandidateBatch {
            score_source: VectorScoreSource::QuantizedApproximate,
            candidates: vec![
                VectorCandidate {
                    id: "a".to_string(),
                    score: 0.9,
                },
                VectorCandidate {
                    id: "b".to_string(),
                    score: 0.8,
                },
                VectorCandidate {
                    id: "c".to_string(),
                    score: 0.7,
                },
            ],
        },
        raw_scores: vec![
            VectorRawScore {
                id: "a".to_string(),
                score: 0.6,
            },
            VectorRawScore {
                id: "b".to_string(),
                score: 0.95,
            },
            VectorRawScore {
                id: "c".to_string(),
                score: 0.7,
            },
        ],
        observed_filter_fields: Vec::new(),
        residual_ids: BTreeSet::new(),
        residual_adds_candidate: false,
        scan_limits: Vec::new(),
        capture_candidate_ids: true,
    };

    let output = execute_vector_plan(&quantized_plan(), &mut source).unwrap();

    assert_eq!(source.observed_filter_fields, vec!["space_id"]);
    assert_eq!(
        output
            .scores
            .iter()
            .map(|score| score.id.as_str())
            .collect::<Vec<_>>(),
        vec!["b", "c"]
    );
    assert_eq!(
        output.report.candidate_score_source,
        VectorScoreSource::QuantizedApproximate
    );
    assert_eq!(
        output.report.final_score_source,
        VectorScoreSource::RawVector
    );
    assert_eq!(
        output.report.backend,
        VectorExecutionBackend::QuantizedProjection
    );
    assert_eq!(
        output.report.compression_mode,
        VectorCompressionMode::Unspecified
    );
    assert_eq!(output.report.generated_candidate_count, 3);
    assert_eq!(output.report.descriptor_pruned_count, 0);
    assert_eq!(output.report.scalar_filtered_count, 0);
    assert_eq!(output.report.index_coverage_complete, None);
    assert!(output.report.fallback_reason_codes.is_empty());
    assert_eq!(output.report.reranked_candidate_count, 3);
    assert_eq!(output.candidate_ids, vec!["a", "b", "c"]);
}

#[test]
fn vector_observability_codes_have_stable_strings() {
    assert_eq!(VectorExecutionBackend::ScalarFlat.as_str(), "scalar_flat");
    assert_eq!(
        VectorExecutionBackend::QuantizedProjection.as_str(),
        "quantized_projection"
    );
    assert_eq!(VectorCompressionMode::Required.as_str(), "required");
    assert_eq!(
        VectorFallbackReasonCode::CompressedVectorProjectionUnavailable.as_str(),
        "compressed_vector_projection_unavailable"
    );
}

#[test]
fn rejects_raw_scores_outside_candidate_set() {
    let mut source = TestSource {
        candidate_batch: VectorCandidateBatch {
            score_source: VectorScoreSource::QuantizedApproximate,
            candidates: vec![VectorCandidate {
                id: "a".to_string(),
                score: 0.9,
            }],
        },
        raw_scores: vec![VectorRawScore {
            id: "outside".to_string(),
            score: 1.0,
        }],
        observed_filter_fields: Vec::new(),
        residual_ids: BTreeSet::new(),
        residual_adds_candidate: false,
        scan_limits: Vec::new(),
        capture_candidate_ids: false,
    };

    assert_eq!(
        execute_vector_plan(&quantized_plan(), &mut source),
        Err(VectorExecutionError::RawScoreOutsideCandidateSet)
    );
}

#[test]
fn expands_candidate_window_for_residual_filters_within_budget() {
    let plan = VectorPhysicalPlan::TopK {
        limit: 2,
        input: Box::new(VectorPhysicalPlan::RawVectorRerank {
            embedding_dimension: 3,
            input: Box::new(VectorPhysicalPlan::ResidualFilter {
                fields: vec!["complex_visibility".to_string()],
                initial_candidate_limit: 2,
                input: Box::new(VectorPhysicalPlan::VectorCandidateScan {
                    source: VectorCandidateSource::Ann,
                    embedding_dimension: 3,
                    candidate_limit: 4,
                    input: Box::new(VectorPhysicalPlan::Filter { fields: Vec::new() }),
                }),
            }),
        }),
    };
    let mut source = TestSource {
        candidate_batch: VectorCandidateBatch {
            score_source: VectorScoreSource::AnnApproximate,
            candidates: vec![
                VectorCandidate {
                    id: "a".to_string(),
                    score: 0.9,
                },
                VectorCandidate {
                    id: "b".to_string(),
                    score: 0.8,
                },
                VectorCandidate {
                    id: "c".to_string(),
                    score: 0.7,
                },
                VectorCandidate {
                    id: "d".to_string(),
                    score: 0.6,
                },
            ],
        },
        raw_scores: vec![
            VectorRawScore {
                id: "b".to_string(),
                score: 0.8,
            },
            VectorRawScore {
                id: "d".to_string(),
                score: 0.9,
            },
        ],
        observed_filter_fields: Vec::new(),
        residual_ids: BTreeSet::from(["b".to_string(), "d".to_string()]),
        residual_adds_candidate: false,
        scan_limits: Vec::new(),
        capture_candidate_ids: true,
    };

    let output = execute_vector_plan(&plan, &mut source).unwrap();

    assert_eq!(source.scan_limits, vec![2, 4]);
    assert_eq!(output.report.candidate_scan_rounds, 2);
    assert_eq!(output.report.generated_candidate_count, 4);
    assert_eq!(output.report.residual_filtered_count, 2);
    assert_eq!(output.candidate_ids, vec!["b", "d"]);
    assert_eq!(
        output
            .scores
            .iter()
            .map(|score| score.id.as_str())
            .collect::<Vec<_>>(),
        vec!["d", "b"]
    );
}

#[test]
fn rejects_residual_filter_candidate_injection() {
    let plan = VectorPhysicalPlan::TopK {
        limit: 1,
        input: Box::new(VectorPhysicalPlan::RawVectorRerank {
            embedding_dimension: 3,
            input: Box::new(VectorPhysicalPlan::ResidualFilter {
                fields: vec!["complex_visibility".to_string()],
                initial_candidate_limit: 1,
                input: Box::new(VectorPhysicalPlan::VectorCandidateScan {
                    source: VectorCandidateSource::Ann,
                    embedding_dimension: 3,
                    candidate_limit: 1,
                    input: Box::new(VectorPhysicalPlan::Filter { fields: Vec::new() }),
                }),
            }),
        }),
    };
    let mut source = TestSource {
        candidate_batch: VectorCandidateBatch {
            score_source: VectorScoreSource::AnnApproximate,
            candidates: vec![VectorCandidate {
                id: "a".to_string(),
                score: 0.9,
            }],
        },
        raw_scores: Vec::new(),
        observed_filter_fields: Vec::new(),
        residual_ids: BTreeSet::new(),
        residual_adds_candidate: true,
        scan_limits: Vec::new(),
        capture_candidate_ids: false,
    };

    assert_eq!(
        execute_vector_plan(&plan, &mut source),
        Err(VectorExecutionError::ResidualFilterAddedCandidate)
    );
}
