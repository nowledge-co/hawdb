// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::{
    HostScorer, HostScorerBatch, HostScorerDescriptor, HostScoringRequest, QueryRequest,
    QueryStreamOptions,
};
use std::num::{NonZeroU64, NonZeroUsize};

const QUERY: &str = "MATCH (m:Memory) RETURN m.id AS id, 1.0 AS score, 'direct' AS reason";

#[derive(Default)]
struct SnapshotScorer {
    calls: usize,
    rows: usize,
}

impl HostScorer for SnapshotScorer {
    fn descriptor(&self) -> HostScorerDescriptor<'_> {
        HostScorerDescriptor::new("snapshot", "v1", NonZeroU64::MIN).unwrap()
    }
    fn score_batch(&mut self, batch: HostScorerBatch<'_>, scores: &mut [f64]) -> crate::Result<()> {
        self.calls += 1;
        self.rows = batch.features.len();
        assert_eq!(batch.reference_time_millis, 1234);
        scores.fill(0.5);
        Ok(())
    }
}

#[test]
fn bounded_snapshot_host_scoring_shares_legacy_cumulative_accounting() {
    let handle = app_read_handle();
    let mut scorer = SnapshotScorer::default();
    let request = HostScoringRequest::new(&mut scorer, "score", NonZeroUsize::new(2).unwrap(), 1)
        .unwrap()
        .with_contiguous_evidence_ties("reason")
        .unwrap()
        .with_reference_time_millis(1234);
    handle
        .with_bounded_graph_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 1,
                max_payload_bytes: 16 * 1024,
            },
            |snapshot| {
                let epoch = snapshot.commit_epoch();
                let output = snapshot.query_request_profiled(
                    QueryRequest::new(QUERY).with_host_scoring(&request),
                    1,
                )?;
                assert_eq!(output.output.rows.len(), 1);
                assert_eq!(output.output.rows[0]["id"], Value::String("nearest".into()));
                assert_eq!(output.output.rows[0]["score"], Value::Float(1.0));
                assert_eq!(
                    output.output.rows[0]["reason"],
                    Value::String("direct".into())
                );
                assert_eq!(snapshot.commit_epoch(), epoch);
                let error = snapshot
                    .query_cypher(
                        "MATCH (t:Thread) RETURN t.thread_id AS id",
                        &BTreeMap::new(),
                        1,
                    )
                    .unwrap_err();
                assert!(error.to_string().contains("row"), "{error}");
                Ok(())
            },
        )
        .unwrap();
    drop(request);
    assert_eq!(scorer.calls, 1);
    assert_eq!(scorer.rows, 1);
}

#[test]
fn bounded_snapshot_host_scoring_keeps_stricter_caller_payload_limits() {
    let handle = app_read_handle();
    let mut scorer = SnapshotScorer::default();
    let request = HostScoringRequest::new(&mut scorer, "score", NonZeroUsize::new(2).unwrap(), 1)
        .unwrap()
        .with_contiguous_evidence_ties("reason")
        .unwrap()
        .with_reference_time_millis(1234);
    handle
        .with_bounded_graph_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 2,
                max_payload_bytes: 16 * 1024,
            },
            |snapshot| {
                let error = snapshot
                    .query_request(
                        QueryRequest::new(QUERY)
                            .with_host_scoring(&request)
                            .with_output_limits(QueryStreamOptions {
                                max_rows: Some(1),
                                max_payload_bytes: Some(1),
                            }),
                        2,
                    )
                    .unwrap_err();
                assert!(error.to_string().contains("payload"), "{error}");
                // A refused typed read must leave the same snapshot usable.
                let output = snapshot.query_cypher(
                    "MATCH (m:Memory) RETURN m.id AS id",
                    &BTreeMap::new(),
                    1,
                )?;
                assert_eq!(output.rows[0]["id"], Value::String("nearest".into()));
                Ok(())
            },
        )
        .unwrap();
}
