// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn assert_memory_refusal(
    error: HawDBError,
    owner: &str,
    requested: u64,
    used: u64,
    budget: u64,
    message: &str,
) {
    let HawDBError::ResourceBudgetExceeded(cause) = error else {
        panic!("expected a typed lexical memory refusal, got {error:?}");
    };
    assert_eq!(
        *cause,
        ResourceBudgetError {
            resource: ResourceBudgetKind::MemoryBytes,
            scope: ResourceBudgetScope::Operator,
            owner: owner.into(),
            memory_class: None,
            requested,
            used,
            budget,
            budget_option: "LexicalProjectionConfig.query_memory_bytes",
            message: message.into(),
        }
    );
}

#[test]
fn lexical_token_memory_refusals_preserve_exact_boundaries_and_release_ownership() {
    let root = OwnedDirectory::new("private-token-memory");
    let mut reader = reader(&root, &documents());
    let analyzer = SearchAnalyzerLexicon::empty();
    let task = RuntimeTaskContext::default();
    let grant = NonZeroUsize::new(1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(grant);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "raw text", grant);
    let context = QueryContext {
        io_scope: None,
        memory: &account,
        task: &task,
        execution: None,
    };
    let competing = account.reserve(11).unwrap();

    // The analyzer emits the cross-word phrase before the second word.
    // A duplicate word must not consume a second retained-term allowance.
    for (text, expected, owner, requested, used, budget, message) in [
        (
            " ".repeat(100),
            BTreeSet::new(),
            "lexical query source",
            100,
            0,
            100,
            "lexical query source exceeds its memory budget",
        ),
        (
            "alpha beta".into(),
            BTreeSet::from(["alpha".into(), "alpha_beta".into(), "beta".into()]),
            "lexical query terms",
            36,
            79,
            115,
            "lexical query terms exceed their memory budget",
        ),
        (
            "alpha alpha".into(),
            BTreeSet::from(["alpha".into(), "alpha_alpha".into()]),
            "lexical query terms",
            43,
            37,
            80,
            "lexical query terms exceed their memory budget",
        ),
    ] {
        for accounted in [false, true] {
            for limit in [budget - 1, budget] {
                Arc::get_mut(&mut reader).unwrap().config.query_memory_bytes =
                    NonZeroU64::new(limit).unwrap();
                let result = if accounted {
                    reader
                        .tokenize_accounted(&text, &analyzer, reader.config.max_term_bytes, context)
                        .map(|output| {
                            output
                                .terms
                                .iter()
                                .map(|term| term.as_str().to_owned())
                                .collect::<BTreeSet<_>>()
                        })
                } else {
                    reader.tokenize_query(&text, &analyzer, reader.config.max_term_bytes)
                };
                if limit == budget {
                    assert_eq!(result.unwrap(), expected);
                } else {
                    assert_memory_refusal(
                        result.unwrap_err(),
                        owner,
                        requested,
                        used,
                        limit,
                        message,
                    );
                }
                assert_eq!(ledger.snapshot().used_bytes, 11);
            }
        }
    }
    drop(competing);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn lexical_statistics_memory_refusal_preserves_complete_retry_with_context() {
    let root = OwnedDirectory::new("private-statistics-memory");
    let mut reader = reader(&root, &documents());
    let terms = BTreeSet::from(["graph".into(), "storage".into()]);
    let expected = reader
        .query_statistics(&terms, reader.config.max_term_bytes)
        .unwrap();
    assert_eq!(expected.document_frequency("graph"), 3);
    assert_eq!(expected.document_frequency("storage"), 2);
    let bytes = reader
        .manifest
        .blocks
        .iter()
        .filter(|block| {
            block.kind == BlockKind::Postings
                && terms.iter().any(|term| {
                    term.as_str() >= block.min_key.as_str()
                        && term.as_str() <= block.max_key.as_str()
                })
        })
        .map(|block| block.length)
        .max()
        .unwrap();
    assert!(bytes > 1);
    // Streaming admits one block at a time, not the sum of all disk reads.
    assert!(expected.bytes_read() > bytes);
    let task = RuntimeTaskContext::default();
    let grant = NonZeroUsize::new(1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(grant);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "statistics", grant);
    for context in [
        None,
        Some(QueryContext {
            io_scope: None,
            memory: &account,
            task: &task,
            execution: None,
        }),
    ] {
        let statistics = |reader: &LexicalProjectionReader| match context {
            None => reader.query_statistics(&terms, reader.config.max_term_bytes),
            Some(context) => AccountedCorpusStatistics::aggregate(
                [reader],
                &terms,
                reader.config.max_term_bytes,
                context,
            )
            .map(|output| output.statistics),
        };
        Arc::get_mut(&mut reader).unwrap().config.query_memory_bytes =
            NonZeroU64::new(bytes - 1).unwrap();
        let error = statistics(&reader).unwrap_err();
        assert_memory_refusal(
            error,
            "lexical query statistics",
            bytes,
            0,
            bytes - 1,
            &format!(
                "lexical query statistics require {bytes} bytes, exceeding {}",
                bytes - 1
            ),
        );
        assert_eq!(ledger.snapshot().used_bytes, 0);
        Arc::get_mut(&mut reader).unwrap().config.query_memory_bytes =
            NonZeroU64::new(bytes).unwrap();
        assert_eq!(statistics(&reader).unwrap(), expected);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn lexical_private_memory_refusals_keep_cancellation_precedence() {
    let root = OwnedDirectory::new("private-memory-cancel");
    let mut reader = reader(&root, &documents());
    Arc::get_mut(&mut reader).unwrap().config.query_memory_bytes = NonZeroU64::new(1).unwrap();
    let task = RuntimeTaskContext::default();
    task.cancellation().cancel();
    let grant = NonZeroUsize::new(1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(grant);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "cancelled query", grant);
    let context = QueryContext {
        io_scope: None,
        memory: &account,
        task: &task,
        execution: None,
    };
    let terms = BTreeSet::from(["graph".into()]);
    let delta = LexicalMiniDelta::default();
    let errors = [
        reader
            .tokenize_accounted(
                "graph",
                &Default::default(),
                reader.config.max_term_bytes,
                context,
            )
            .err()
            .unwrap(),
        reader
            .query_statistics_with_context(&terms, reader.config.max_term_bytes, Some(context))
            .unwrap_err(),
        reader
            .score_accounted(&terms, inputs(&reader, &delta, context, None), |_| {
                panic!("cancelled scoring must not visit candidates")
            })
            .err()
            .unwrap(),
    ];
    for error in errors {
        assert!(
            matches!(error, HawDBError::Execution(message) if message == "lexical query cancelled")
        );
    }
    assert_eq!(ledger.snapshot().used_bytes, 0);
}
