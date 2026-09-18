use super::*;

#[test]
fn knowledge_truncation_reason_codes_have_stable_string_encodings() {
    let cases = [
        (
            KnowledgeTruncationReasonCode::RankWindowExceeded,
            "rank_window_exceeded",
        ),
        (
            KnowledgeTruncationReasonCode::SearchLimitExceeded,
            "search_limit_exceeded",
        ),
        (
            KnowledgeTruncationReasonCode::PartialCandidateReturn,
            "partial_candidate_return",
        ),
        (
            KnowledgeTruncationReasonCode::GraphSeedLimitExceeded,
            "graph_seed_limit_exceeded",
        ),
        (
            KnowledgeTruncationReasonCode::GraphContextLimitExceeded,
            "graph_context_limit_exceeded",
        ),
        (
            KnowledgeTruncationReasonCode::CandidateLimitExceeded,
            "candidate_limit_exceeded",
        ),
    ];

    for (code, name) in cases {
        assert_eq!(code.as_str(), name);
        assert_eq!(name.parse::<KnowledgeTruncationReasonCode>(), Ok(code));
    }
    assert!("limit".parse::<KnowledgeTruncationReasonCode>().is_err());
}

#[test]
fn knowledge_fallback_reason_codes_have_stable_string_encodings() {
    let cases = [
        (
            KnowledgeFallbackReasonCode::GraphSeedLimitZero,
            "graph_seed_limit_zero",
        ),
        (
            KnowledgeFallbackReasonCode::GraphContextLimitZero,
            "graph_context_limit_zero",
        ),
        (
            KnowledgeFallbackReasonCode::GraphContextMaxHopsZero,
            "graph_context_max_hops_zero",
        ),
    ];

    for (code, name) in cases {
        assert_eq!(code.as_str(), name);
        assert_eq!(name.parse::<KnowledgeFallbackReasonCode>(), Ok(code));
    }
    assert!("disabled".parse::<KnowledgeFallbackReasonCode>().is_err());
}

#[test]
fn knowledge_traversal_fallback_reason_codes_have_stable_string_encodings() {
    let cases = [
        (
            KnowledgeTraversalFallbackReasonCode::SeedNotFound,
            "seed_not_found",
        ),
        (
            KnowledgeTraversalFallbackReasonCode::TargetNotFound,
            "target_not_found",
        ),
        (
            KnowledgeTraversalFallbackReasonCode::MaxHopsZero,
            "max_hops_zero",
        ),
        (
            KnowledgeTraversalFallbackReasonCode::PathLimitZero,
            "path_limit_zero",
        ),
        (
            KnowledgeTraversalFallbackReasonCode::NodeLimitZero,
            "node_limit_zero",
        ),
        (
            KnowledgeTraversalFallbackReasonCode::RelationshipLimitZero,
            "relationship_limit_zero",
        ),
        (
            KnowledgeTraversalFallbackReasonCode::RelationshipTypeNotFound,
            "relationship_type_not_found",
        ),
        (
            KnowledgeTraversalFallbackReasonCode::QueryRuntimeFailed,
            "query_runtime_failed",
        ),
    ];

    for (code, name) in cases {
        assert_eq!(code.as_str(), name);
        assert_eq!(
            name.parse::<KnowledgeTraversalFallbackReasonCode>(),
            Ok(code)
        );
    }
    assert!("missing"
        .parse::<KnowledgeTraversalFallbackReasonCode>()
        .is_err());
}

#[test]
fn knowledge_fanout_reason_codes_have_stable_string_encodings() {
    let cases = [
        (KnowledgeFanoutReasonCode::DenseAdjacency, "dense_adjacency"),
        (
            KnowledgeFanoutReasonCode::GraphContextLimitReached,
            "graph_context_limit_reached",
        ),
        (
            KnowledgeFanoutReasonCode::GraphSeedLimitReached,
            "graph_seed_limit_reached",
        ),
        (
            KnowledgeFanoutReasonCode::CandidateLimitReached,
            "candidate_limit_reached",
        ),
        (
            KnowledgeFanoutReasonCode::PathLimitReached,
            "path_limit_reached",
        ),
        (
            KnowledgeFanoutReasonCode::NodeLimitReached,
            "node_limit_reached",
        ),
        (
            KnowledgeFanoutReasonCode::RelationshipLimitReached,
            "relationship_limit_reached",
        ),
    ];

    for (code, name) in cases {
        assert_eq!(code.as_str(), name);
        assert_eq!(name.parse::<KnowledgeFanoutReasonCode>(), Ok(code));
    }
    assert!("limit".parse::<KnowledgeFanoutReasonCode>().is_err());
}
