use super::*;
use crate::query_memory::QueryMemory;
use skein_core::{RuntimeMemoryReservation, RuntimeTaskContext};

pub(super) fn fixture(name: &str) -> (PathBuf, Arc<LexicalProjectionReader>) {
    let root = projection_root(name);
    fs::create_dir_all(&root).unwrap();
    let documents = (0..17)
        .map(|index| {
            document(
                &format!("memory:{index:02}"),
                "graph",
                &"storage ".repeat(index % 4 + 1),
            )
        })
        .collect::<Vec<_>>();
    let reader = LexicalProjectionWriter::new(LexicalProjectionConfig::default())
        .write(
            &root,
            1,
            None,
            11,
            13,
            documents.iter(),
            &SearchAnalyzerLexicon::default(),
        )
        .unwrap();
    (root, reader)
}

pub(super) fn memory(bytes: u64, results: u64) -> QueryMemory {
    QueryMemory::new(
        NonZeroU64::new(bytes).unwrap(),
        Some(
            &RuntimeTaskContext::default()
                .with_memory_reservation(RuntimeMemoryReservation::new(bytes, results)),
        ),
    )
    .unwrap()
}

fn score(reader: &LexicalProjectionReader, memory: &QueryMemory) -> Result<LexicalQueryReport> {
    reader.score_with_memory(
        &BTreeSet::from(["graph".to_string()]),
        &LexicalMiniDelta::default(),
        Some(3),
        None,
        memory,
        |_| Ok(true),
    )
}

#[test]
fn returned_scores_own_capacity_after_report_and_query_owner_drop() {
    let (root, reader) = fixture("query-score-owner");
    let memory = reader.query_memory(None).unwrap();
    let ledger = memory.ledger.clone();
    let report = score(&reader, &memory).unwrap();
    let retained = ledger.snapshot().used_bytes;
    assert!(retained > 0);
    assert!(ledger.snapshot().peak_bytes > retained);
    assert_eq!(ledger.snapshot().account_count, 2);
    let scores = report.scores;
    drop(memory);
    drop(reader);
    assert_eq!(ledger.snapshot().used_bytes, retained);
    assert_eq!(scores.len(), 3);
    drop(scores);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn streams_and_retained_scores_share_the_exact_root_across_calls() {
    let (root, reader) = fixture("query-shared-root");
    let baseline = reader.query_memory(None).unwrap();
    let expected = score(&reader, &baseline).unwrap();
    let peak = baseline.ledger.snapshot().peak_bytes as u64;
    let exact = memory(peak, peak);
    let first = score(&reader, &exact).unwrap();
    assert_eq!(first.scores, expected.scores);
    let retained = exact.ledger.snapshot().used_bytes;
    assert!(retained > 0);
    assert!(score(&reader, &exact).is_err());
    assert_eq!(exact.ledger.snapshot().used_bytes, retained);
    drop(first);
    assert_eq!(exact.ledger.snapshot().used_bytes, 0);
    drop(score(&reader, &exact).unwrap());
    assert_eq!(exact.ledger.snapshot().used_bytes, 0);
    let short = memory(peak - 1, peak);
    assert!(score(&reader, &short).is_err());
    assert_eq!(short.ledger.snapshot().used_bytes, 0);
    drop(expected);
    assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn other_live_work_rejects_before_dictionary_io_and_releases_on_failure() {
    let (root, reader) = fixture("query-admission-order");
    let memory = reader.query_memory(None).unwrap();
    let guard = memory.scores.reserve(memory.limit as usize - 1).unwrap();
    let before = reader.cache.snapshot();
    let error = score(&reader, &memory).unwrap_err();
    assert!(error.to_string().contains("query memory ledger"));
    assert_eq!(reader.cache.snapshot(), before);
    assert_eq!(memory.ledger.snapshot().used_bytes, guard.bytes());
    drop(guard);
    drop(score(&reader, &memory).unwrap());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cancellation_and_consumer_failure_release_streams_and_scores() {
    let (root, reader) = fixture("query-cancel-owner");
    for cancel in [false, true] {
        let memory = reader.query_memory(None).unwrap();
        let task = RuntimeTaskContext::default();
        let mut calls = 0;
        let error = reader
            .score_with_memory(
                &BTreeSet::from(["graph".to_string()]),
                &LexicalMiniDelta::default(),
                Some(3),
                Some(&task),
                &memory,
                |_| {
                    calls += 1;
                    if calls == 4 {
                        assert!(memory.ledger.snapshot().used_bytes > 0);
                        if cancel {
                            task.cancellation().cancel();
                        } else {
                            return Err(SkeinError::Execution("consumer stopped".to_string()));
                        }
                    }
                    Ok(true)
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains(if cancel {
            "cancelled"
        } else {
            "consumer stopped"
        }));
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(reader.cache.snapshot().pinned_bytes, 0);
        assert_eq!(memory.ledger.snapshot().account_count, 2);
    }
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn result_cap_and_shared_root_rejection_leave_collector_unchanged() {
    let memory = memory(4096, 4096);
    let mut collector = ScoreCollector::new(Some(1), 10, 4096, &memory.scores).unwrap();
    let mut long = String::with_capacity(1000);
    long.push('a');
    collector.push(long, 1.0).unwrap();
    let before = memory.ledger.snapshot().used_bytes;
    let blocker = memory.working.reserve(4096 - before).unwrap();
    let mut larger = String::with_capacity(1001);
    larger.push('b');
    assert!(collector.push(larger, 2.0).is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 4096);
    drop(blocker);
    assert_eq!(memory.ledger.snapshot().used_bytes, before);
    collector.push("c".to_string(), 3.0).unwrap();
    assert_eq!(memory.ledger.snapshot().used_bytes, before - 999);
    let scores = collector.finish();
    assert_eq!(scores, BTreeMap::from([("c".to_string(), 3.0)]));
    drop(scores);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    let zero_result = super::query_admission::memory(4096, 0);
    assert!(
        ScoreCollector::new(Some(1), 1, zero_result.result_limit, &zero_result.scores).is_err()
    );
    assert_eq!(zero_result.ledger.snapshot().used_bytes, 0);
}

#[test]
fn absent_terms_and_zero_windows_release_all_query_charges() {
    let (root, reader) = fixture("query-empty-owner");
    let memory = reader.query_memory(None).unwrap();
    for terms in [
        BTreeSet::new(),
        BTreeSet::from(["absent".to_string()]),
        BTreeSet::from(["graph".to_string()]),
    ] {
        let report = reader
            .score_with_memory(
                &terms,
                &LexicalMiniDelta::default(),
                Some(0),
                None,
                &memory,
                |_| Ok(true),
            )
            .unwrap();
        assert!(report.scores.is_empty());
        drop(report);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    let zero =
        RuntimeTaskContext::default().with_memory_reservation(RuntimeMemoryReservation::new(0, 0));
    assert!(reader.query_memory(Some(&zero)).is_err());
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}
