use super::*;
use crate::lexical_projection::tests::query_admission::{fixture, memory};

#[test]
#[ignore = "local query ownership campaign; run the explicit Bazel fuzz suite"]
fn query_memory_campaign() {
    let (root, reader) = fixture("query-memory-campaign");
    let analyzer = SearchAnalyzerLexicon::default();
    let base = (0..17)
        .map(|index| {
            let value = document(
                &format!("memory:{index:02}"),
                "graph",
                &"storage ".repeat(index % 4 + 1),
            );
            (value.id.clone(), value)
        })
        .collect::<BTreeMap<_, _>>();
    let mut random = Random(0x2065c0e);
    let mut comparisons = 0;
    let mut cancellations = 0;
    for case in 0..512 {
        let mut documents = base.clone();
        let mut delta = LexicalMiniDelta::default();
        for _ in 0..random.index(5) {
            let id = format!("memory:{:02}", random.index(20));
            if random.index(3) == 0 {
                delta
                    .delete(&id, documents.get(&id), &analyzer, reader.config)
                    .unwrap();
                documents.remove(&id);
            } else {
                let value = document(&id, "storage", &"graph ".repeat(1 + random.index(7)));
                delta
                    .upsert(&value, documents.get(&id), &analyzer, reader.config)
                    .unwrap();
                documents.insert(id, value);
            }
        }
        let terms = [
            BTreeSet::from(["graph".to_string()]),
            BTreeSet::from(["storage".to_string()]),
            BTreeSet::from(["graph".to_string(), "storage".to_string()]),
            BTreeSet::from(["graph".to_string(), "absent".to_string()]),
        ][case % 4]
            .clone();
        let limit = [None, Some(0), Some(1), Some(3), Some(9), Some(20)][case % 6];
        let excluded = (0..random.index(7))
            .map(|_| format!("memory:{:02}", random.index(20)))
            .collect::<BTreeSet<_>>();
        let oracle =
            super::state_machine::reference_scores(&documents, &analyzer, &terms, &excluded);
        let expected = oracle
            .iter()
            .take(limit.unwrap_or(usize::MAX))
            .cloned()
            .collect::<BTreeMap<_, _>>();
        let competing = random.index(8192);
        let baseline = reader.query_memory(None).unwrap();
        let guard = baseline.working.reserve(competing).unwrap();
        let report = reader
            .score_with_memory(&terms, &delta, limit, None, &baseline, |id| {
                Ok(!excluded.contains(id))
            })
            .unwrap();
        assert_eq!(report.matching_document_count, oracle.len());
        assert_eq!(
            report.scores.keys().collect::<Vec<_>>(),
            expected.keys().collect::<Vec<_>>()
        );
        for (id, score) in &expected {
            assert!((report.scores[id] - score).abs() <= 1e-12 * score.max(1.0));
            comparisons += 1;
        }
        let peak = baseline.ledger.snapshot().peak_bytes as u64;
        let retained = baseline.ledger.snapshot().used_bytes - competing;
        for bytes in [peak, peak - 1] {
            let bounded = memory(bytes, bytes);
            let other = bounded.working.reserve(competing).unwrap();
            let actual = reader.score_with_memory(&terms, &delta, limit, None, &bounded, |id| {
                Ok(!excluded.contains(id))
            });
            if bytes == peak {
                let actual = actual.unwrap();
                assert_eq!(actual.scores, report.scores);
                assert_eq!(bounded.ledger.snapshot().used_bytes, retained + competing);
                // A second owner can only use the remaining root, not the full
                // configured limit again while these score IDs are still live.
                if retained > 0 {
                    assert!(bounded.working.reserve(bytes as usize - competing).is_err());
                }
                drop(actual);
            } else {
                assert!(actual.is_err(), "one-short query succeeded at case {case}");
            }
            assert_eq!(bounded.ledger.snapshot().used_bytes, competing);
            drop(other);
            assert_eq!(bounded.ledger.snapshot().used_bytes, 0);
            assert_eq!(bounded.ledger.snapshot().account_count, 2);
        }
        drop(report);
        assert_eq!(baseline.ledger.snapshot().used_bytes, competing);
        drop(guard);
        assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
        if case % 16 == 0 {
            let task = RuntimeTaskContext::default();
            let result =
                reader.score_with_memory(&terms, &delta, limit, Some(&task), &baseline, |_| {
                    task.cancellation().cancel();
                    Ok(true)
                });
            assert!(result.is_err());
            assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
            assert_eq!(reader.cache.snapshot().pinned_bytes, 0);
            cancellations += 1;
        }
    }
    drop(reader);
    fs::remove_dir_all(root).unwrap();
    eprintln!("query memory seed=0x2065c0e cases=512 exact=512 short=512 comparisons={comparisons} cancelled={cancellations}");
}
