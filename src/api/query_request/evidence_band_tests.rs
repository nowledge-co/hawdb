// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::{Database, HostScorerBatch, HostScorerDescriptor, QueryRequest, Value};
use hawdb_plan_cypher::SCORING_RERANK_SCORE_COLUMN;
use std::collections::BTreeMap;
use std::num::NonZeroU64;

const QUERY: &str = "MATCH (m:Memory) RETURN m.id AS id, m.direct_score AS score, m.reason AS reason, m.other_reason AS other_reason, m.secondary AS secondary, m.ordinal AS ordinal ORDER BY ordinal";

#[derive(Default)]
struct SecondaryScorer {
    calls: usize,
    rows: usize,
}

impl HostScorer for SecondaryScorer {
    fn descriptor(&self) -> HostScorerDescriptor<'_> {
        HostScorerDescriptor::new("secondary", "v1", NonZeroU64::MIN).unwrap()
    }
    fn score_batch(&mut self, batch: HostScorerBatch<'_>, scores: &mut [f64]) -> Result<()> {
        self.calls += 1;
        self.rows = batch.features.len();
        for (row, score) in batch.features.iter().zip(scores) {
            batch.checkpoint()?;
            *score = row.numeric_property("secondary").unwrap();
        }
        Ok(())
    }
}

fn fixture() -> Database {
    let mut database = Database::new();
    for (ordinal, (id, score, reason, secondary)) in [
        ("a", 0.9, "direct", 0.1),
        ("b", 0.9, "direct", 0.9),
        ("c", 0.8, "direct", 900.0),
        ("d", 0.8, "other", 999.0),
        ("e", 0.9, "direct", 1000.0),
    ]
    .into_iter()
    .enumerate()
    {
        database.query_with_params(
            "CREATE (:Memory {id: $id, direct_score: $score, reason: $reason, other_reason: 'same', secondary: $secondary, ordinal: $ordinal})",
            &BTreeMap::from([
                ("id".into(), Value::String(id.into())),
                ("score".into(), Value::Float(score)),
                ("reason".into(), Value::String(reason.into())),
                ("secondary".into(), Value::Float(secondary)),
                ("ordinal".into(), Value::Int(ordinal as i64)),
            ]),
        ).unwrap();
    }
    database
}

#[test]
fn host_evidence_bands_query_preserves_direct_bits_and_separates_cache_policies() {
    let mut database = fixture();
    for (reason_column, expected, cached) in [
        (None, ["e", "d", "c"], false),
        (Some("reason"), ["b", "a", "c"], false),
        (Some("other_reason"), ["b", "a", "d"], false),
        (Some("reason"), ["b", "a", "c"], true),
        (None, ["e", "d", "c"], true),
    ] {
        let mut scorer = SecondaryScorer::default();
        let mut request =
            HostScoringRequest::new(&mut scorer, "score", NonZeroUsize::new(5).unwrap(), 3)
                .unwrap()
                .with_reference_time_millis(1234);
        if let Some(reason_column) = reason_column {
            request = request
                .with_contiguous_evidence_ties(reason_column)
                .unwrap();
        }
        let before = database.plan_cache_stats().unwrap();
        let result = database
            .query_request(QueryRequest::new(QUERY).with_host_scoring(&request))
            .unwrap();
        assert_eq!(
            result
                .rows
                .iter()
                .map(|row| row["id"].clone())
                .collect::<Vec<_>>(),
            expected.map(|id| Value::String(id.into()))
        );
        for row in result.rows {
            let Value::String(id) = &row["id"] else {
                unreachable!()
            };
            assert_eq!(
                row["score"],
                Value::Float(if id == "c" || id == "d" { 0.8 } else { 0.9 })
            );
            assert_eq!(
                row["reason"],
                Value::String(if id == "d" { "other" } else { "direct" }.into())
            );
            assert_eq!(row[SCORING_RERANK_SCORE_COLUMN], row["secondary"]);
            assert!(row.keys().all(|key| !key.contains('\0')));
        }
        let after = database.plan_cache_stats().unwrap();
        assert_eq!(after.misses, before.misses + usize::from(!cached) as u64);
        assert_eq!(after.hits, before.hits + usize::from(cached) as u64);
        drop(request);
        assert_eq!(scorer.calls, 1);
        assert_eq!(scorer.rows, 5);
    }
    let mut scorer = SecondaryScorer::default();
    let request = HostScoringRequest::new(&mut scorer, "score", NonZeroUsize::new(5).unwrap(), 3)
        .unwrap()
        .with_contiguous_evidence_ties("reason")
        .unwrap();
    let explained = database
        .query_request(QueryRequest::new(&format!("EXPLAIN {QUERY}")).with_host_scoring(&request))
        .unwrap();
    assert!(format!("{:?}", explained.rows).contains("ContiguousEvidenceTies"));
    drop(request);
    assert_eq!(scorer.calls, 0);
}
