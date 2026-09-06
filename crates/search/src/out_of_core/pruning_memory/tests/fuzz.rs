use super::*;

#[test]
#[ignore = "local pruning workspace and legacy parity campaign"]
fn pruning_admission_campaign() {
    let mut seed = 0x206f111eu64;
    let mut next = || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        seed
    };
    let mut kept = 0;
    let mut rejected = 0;
    for case in 0..512 {
        let count = 1 + next() as usize % 32;
        let docs = (0..count)
            .map(|index| {
                let mut doc = document(index, ["team", "private", "default"][next() as usize % 3]);
                doc.metadata.insert(
                    "unicode".to_owned(),
                    ["\u{130}", "a\0B", "Mixed", ""][next() as usize % 4]
                        .repeat(next() as usize % 64),
                );
                if next() % 3 == 0 {
                    doc.metadata.remove("score");
                }
                doc
            })
            .collect::<Vec<_>>();
        let segment = segment(&docs);
        let mut predicates = Vec::new();
        for _ in 0..1 + next() as usize % 9 {
            let predicate = match next() % 10 {
                0 => SearchPredicate::eq("space_id", "team"),
                1 => SearchPredicate::eq("unicode", "\u{130}".repeat(next() as usize % 64)),
                2 => SearchPredicate::in_list("space_id", ["team", "private"].map(str::to_owned)),
                3 => SearchPredicate::not_in_list("space_id", ["team".to_owned()]),
                4 => SearchPredicate::gte("score", (next() % 36).to_string()),
                5 => SearchPredicate::lt("score", (next() % 36).to_string()),
                6 => SearchPredicate::exists("score"),
                7 => SearchPredicate::is_missing("score"),
                8 => SearchPredicate::gte("created_at", format!("2026-09-{:02}", 1 + next() % 28)),
                _ => SearchPredicate::eq("kind", "memory"),
            };
            predicates.push(predicate);
        }
        let predicates = if case % 31 == 0 {
            SearchPredicateSet::empty()
        } else if case % 37 == 0 {
            SearchPredicateSet::unsatisfiable()
        } else {
            SearchPredicateSet::new(predicates)
        };
        let baseline = memory(64 * 1024 * 1024);
        let competing = 1 + next() as usize % 1024;
        let lease = baseline.scores.reserve(competing).unwrap();
        let result = compare(&segment, &predicates, &baseline).unwrap();
        kept += usize::from(result);
        rejected += usize::from(!result);
        let peak = baseline.ledger.snapshot().peak_bytes;
        assert_eq!(baseline.ledger.snapshot().used_bytes, competing);
        drop(lease);
        for limit in [peak, peak - 1] {
            let bounded = memory(limit);
            let result = bounded
                .scores
                .reserve(competing)
                .and_then(|_lease| compare(&segment, &predicates, &bounded));
            assert_eq!(result.is_ok(), limit == peak, "case={case}");
            assert_eq!(bounded.ledger.snapshot().used_bytes, 0);
            assert_eq!(bounded.ledger.snapshot().account_count, 2);
        }
    }
    assert!(kept > 0 && rejected > 0);
    eprintln!(
        "pruning seed=0x206f111e cases=512 exact=512 short=512 kept={kept} rejected={rejected}"
    );
}
