// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};
use hawdb_qos::{IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeWorkRequest};

#[test]
fn retained_output_admission_is_explicit_and_refusal_preserves_existing_fields() {
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(64 << 20),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    let budget = NonZeroUsize::new(16 << 20).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "output", budget);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(16 << 20))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let query = QueryContext {
        memory: &account,
        task: &task,
        execution: Some(&permit),
        io_scope: None,
    };
    assert!(matches!(
        Frame::new(query, &permit),
        Err(SearchQueryError::Retention(
            RuntimeRetainedResultError::Capacity {
                retryable: false,
                ..
            }
        ))
    ));
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    drop(permit);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(16 << 20).with_result_bytes(16384))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let query = QueryContext {
        memory: &account,
        task: &task,
        execution: Some(&permit),
        io_scope: None,
    };
    let frame = Frame::new(query, &permit).unwrap();
    let memory = Memory(Some(&frame));
    let first = memory.text("kept").unwrap();
    let before = ledger.snapshot().used_bytes;
    let excessive = "x".repeat(16384);
    assert!(matches!(
        memory.text(&excessive),
        Err(HawDBError::ResourceBudgetExceeded(_))
    ));
    assert_eq!(first, "kept");
    assert_eq!(ledger.snapshot().used_bytes, before);
    let formatted = memory
        .format(format_args!("offset {} limit {}", 3, 7))
        .unwrap();
    assert_eq!(formatted, "offset 3 limit 7");
    let codes = memory
        .codes(&[SearchTruncationReasonCode::LimitExceeded])
        .unwrap();
    assert_eq!(codes.len(), 1);
    drop((first, formatted, codes));
    drop(frame);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
}

fn with_frame(work: impl FnOnce(Memory<'_>, QueryContext<'_>) -> Result<()>) -> Result<()> {
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(2 * 1024 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    let permit = governor
        .try_admit(
            RuntimeWorkRequest::background_maintenance(512 * 1024 * 1024)
                .with_result_bytes(1024 * 1024),
        )
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let limit = NonZeroUsize::new(512 * 1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(limit);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "output differential", limit);
    let query = QueryContext {
        memory: &account,
        task: &task,
        execution: Some(&permit),
        io_scope: None,
    };
    let frame = Frame::new(query, &permit).unwrap();
    let result = work(Memory(Some(&frame)), query);
    drop(frame);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    result
}

#[test]
fn retained_matches_preserve_aliases_metadata_cjk_offsets_and_config_limits() {
    with_frame(|memory, _query| {
        for (title, body, raw_query) in [
            (
                "GraphStream HTTPServerV2",
                "memory-retrieval graph_stream running Runs",
                "graph http run memory",
            ),
            (
                "Graph Graph",
                "graph_graph GRAPH_stream",
                "graph graph_stream",
            ),
            (
                "中文数据库",
                "北京海淀区 GraphStream 数据检索",
                "北京 graph 数据库",
            ),
            ("No body match", "", "memory team source"),
        ] {
            let analyzer = SearchAnalyzerLexicon::default();
            let document = SearchDocument {
                id: "doc".into(),
                title: title.into(),
                content: body.into(),
                embedding: None,
                metadata: BTreeMap::from([
                    ("kind".into(), "Memory".into()),
                    ("space_id".into(), "team".into()),
                    ("source_id".into(), "source".into()),
                ]),
            };
            let terms = crate::tokenize_list(raw_query, &analyzer)
                .into_iter()
                .collect();
            let expected_terms = crate::matched_query_terms(&terms, &document, &analyzer);
            let expected_spans = crate::matched_query_spans_bounded(
                &terms,
                &document,
                &analyzer,
                usize::MAX,
                u64::MAX,
                0,
            )?;
            let actual = memory.matches(
                &terms,
                &document,
                &analyzer,
                matches::Limits {
                    count: usize::MAX,
                    bytes: u64::MAX,
                    used: 0,
                },
            )?;
            assert_eq!(actual.terms, expected_terms, "{raw_query}");
            assert_eq!(actual.spans, expected_spans, "{raw_query}");
            drop(actual);
            if !expected_spans.is_empty() {
                assert!(matches!(
                    memory.matches(
                        &terms,
                        &document,
                        &analyzer,
                        matches::Limits {
                            count: 0,
                            bytes: u64::MAX,
                            used: 0
                        }
                    ),
                    Err(HawDBError::Storage(_))
                ));
                assert!(matches!(
                    memory.matches(
                        &terms,
                        &document,
                        &analyzer,
                        matches::Limits {
                            count: usize::MAX,
                            bytes: 1,
                            used: 0
                        }
                    ),
                    Err(HawDBError::ResourceBudgetExceeded(_))
                ));
            }
        }
        Ok(())
    })
    .unwrap();
}

#[test]
fn retained_report_ranking_and_pruning_match_ordinary_semantics() {
    with_frame(|memory, query| {
        let ranks = BTreeMap::from([
            ("a".into(), 2),
            ("b".into(), 1),
            ("c".into(), 1),
            ("missing".into(), 0),
        ]);
        let scores = BTreeMap::from([("a".into(), 3.0), ("b".into(), 2.0), ("c".into(), 7.0)]);
        for limit in [0, 1, 2, 9] {
            assert_eq!(
                memory.top_ids(&ranks, limit)?,
                top_ranked_ids(&ranks, limit)
            );
            assert_eq!(
                memory.top_candidates(&ranks, &scores, limit)?,
                top_ranked_candidates(&ranks, &scores, limit)
            );
        }
        let documents = [SearchDocument {
            id: "a".into(),
            title: "Graph".into(),
            content: "data".into(),
            embedding: None,
            metadata: BTreeMap::from([
                ("space_id".into(), "team".into()),
                ("kind".into(), "Memory".into()),
                ("confidence".into(), "0.9".into()),
                ("created_at".into(), "2026-10-20T00:00:00Z".into()),
            ]),
        }];
        let segment =
            SearchSegmentDescriptorEntry::from_documents(0, &[&documents[0]], &BTreeSet::new());
        for filters in [
            BTreeMap::from([
                ("space_id".into(), "team".into()),
                ("kind".into(), "Memory".into()),
            ]),
            BTreeMap::from([
                ("confidence__gt".into(), "0.7".into()),
                ("confidence__lt".into(), "1.0".into()),
                ("created_at__gte".into(), "2026-01-01T00:00:00Z".into()),
            ]),
            BTreeMap::from([
                ("space_id__exists".into(), "true".into()),
                ("space_id__in".into(), "[\"team\",\"other\"]".into()),
            ]),
        ] {
            let predicates =
                hawdb_optimizer_predicate::SearchPredicateSet::from_metadata_filters(&filters)
                    .unwrap();
            let mut expected = SearchFieldPruningAccumulator::new(&predicates);
            expected.observe_persisted_segment(&segment, &predicates);
            let expected = expected.into_reports();
            let mut actual = report::Fields::new(&predicates, Some(query))?;
            actual.observe(&segment, &predicates);
            assert_eq!(actual.into_reports(memory)?, expected);
        }
        Ok(())
    })
    .unwrap();
}

#[test]
fn retained_matching_quota_and_cancellation_refuse_without_leaking_results() {
    with_frame(|memory, query| {
        let terms = BTreeSet::from(["graph".into()]);
        let document = SearchDocument {
            id: "d".into(),
            title: "".into(),
            content: "graph ".repeat(20000),
            embedding: None,
            metadata: BTreeMap::new(),
        };
        let error = match memory.matches(
            &terms,
            &document,
            &SearchAnalyzerLexicon::empty(),
            matches::Limits {
                count: usize::MAX,
                bytes: u64::MAX,
                used: 0,
            },
        ) {
            Ok(_) => panic!("expected result quota refusal"),
            Err(error) => error,
        };
        let HawDBError::ResourceBudgetExceeded(error) = error else {
            panic!("unexpected refusal: {error}")
        };
        assert_eq!(error.owner, "search retained output");
        query.task.cancellation().cancel();
        assert!(memory
            .matches(
                &BTreeSet::new(),
                &document,
                &SearchAnalyzerLexicon::empty(),
                matches::Limits {
                    count: 0,
                    bytes: 0,
                    used: 0
                }
            )
            .is_err());
        Ok(())
    })
    .unwrap();
}
