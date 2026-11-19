// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn stable_filters_reuse_results_without_changing_scores_or_ownership() {
    let root = OwnedDirectory::new("stable-filter");
    let documents = documents();
    let reader = reader(&root, &documents);
    let terms = BTreeSet::from(["graph".to_string(), "storage".to_string()]);
    let delta = LexicalMiniDelta::default();
    let budget = NonZeroUsize::new(1024 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "stable filter", budget);
    let task = RuntimeTaskContext::default();
    let context = QueryContext {
        memory: &account,
        task: &task,
        execution: None,
        io_scope: None,
    };
    for limit in [None, Some(0), Some(1), Some(10)] {
        let mut repeated = 0;
        let expected = reader
            .score(&terms, &delta, limit, |id| {
                repeated += 1;
                Ok(id != "c")
            })
            .unwrap();
        let mut calls = 0;
        let actual = reader
            .score_terms_with_stable_filter(&terms, inputs(&reader, &delta, context, limit), |id| {
                calls += 1;
                Ok(id != "c")
            })
            .unwrap();
        assert_eq!(actual.report, expected);
        assert_eq!((repeated, calls), (5, 3));
        let mut ordinary_calls = 0;
        let ordinary = reader
            .score_with_stable_filter(&terms, &delta, limit, |id| {
                ordinary_calls += 1;
                Ok(id != "c")
            })
            .unwrap();
        assert_eq!(ordinary, expected);
        assert_eq!(ordinary_calls, 3);
        eprintln!("filter limit={limit:?}: repeated calls={repeated}, stable calls={calls}");
        drop(actual);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
    let cancellation = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(cancellation.clone());
    let context = QueryContext {
        task: &task,
        ..context
    };
    let result = reader.score_terms_with_stable_filter(
        &terms,
        inputs(&reader, &delta, context, None),
        |_| {
            cancellation.cancel();
            Ok(true)
        },
    );
    assert!(matches!(result, Err(HawDBError::RuntimeTaskStopped(_))));
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn arbitrary_filters_keep_per_posting_calls_and_errors() {
    let root = OwnedDirectory::new("mutable-filter");
    let documents = documents();
    let reader = reader(&root, &documents);
    let terms = BTreeSet::from(["graph".to_string(), "storage".to_string()]);
    let delta = LexicalMiniDelta::default();
    let mut calls = Vec::new();
    let report = reader
        .score(&terms, &delta, None, |id| {
            calls.push(id.to_owned());
            Ok(calls.len() % 2 == 1)
        })
        .unwrap();
    assert_eq!(calls, ["a", "a", "b", "c", "c"]);
    let corpus = crate::TextCorpusStats::from_documents(documents.values(), &Default::default());
    for (id, term) in [("a", "graph"), ("b", "graph"), ("c", "storage")] {
        let expected = crate::bm25_score(
            &BTreeSet::from([term.to_owned()]),
            &documents[id],
            &corpus,
            &Default::default(),
        );
        assert_eq!(report.scores[id].to_bits(), expected.to_bits());
    }
    let mut calls = 0;
    let result = reader.score(&terms, &delta, None, |_| {
        calls += 1;
        if calls == 2 {
            Err(HawDBError::Execution("second callback".into()))
        } else {
            Ok(true)
        }
    });
    assert!(matches!(result, Err(HawDBError::Execution(message)) if message == "second callback"));
    assert_eq!(calls, 2);
}

#[test]
fn denied_stable_filter_still_validates_every_posting() {
    let postings = [
        (0, QueryPosting { ordinal: 0, tf: 1 }),
        (1, QueryPosting { ordinal: 0, tf: 3 }),
    ];
    let mut collector = ScoreCollector::new(Some(1), 10, None).unwrap();
    let mut calls = 0;
    let result = score_document(
        "a",
        2,
        &postings,
        &[1.0, 1.0],
        &LexicalMiniDelta::default(),
        None,
        2.0,
        &mut |_| {
            calls += 1;
            Ok(false)
        },
        true,
        &mut collector,
    );
    assert!(
        matches!(result, Err(HawDBError::Storage(message)) if message.contains("document length"))
    );
    assert_eq!(calls, 1);
}
