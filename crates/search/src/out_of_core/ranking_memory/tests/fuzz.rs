use super::*;

#[test]
#[ignore = "local borrowed-rank and bounded-fusion admission campaign"]
fn ranking_admission_campaign() {
    let mut seed = 0x206f0510u64;
    let mut next = || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        seed
    };
    let mut accepted = 0;
    let mut returned = 0;
    for case in 0..384 {
        let mut vector = BTreeMap::new();
        let mut text = BTreeMap::new();
        for id in 0..next() as usize % 49 {
            let id = format!(
                "{id:04}-{}",
                ["x", "\u{130}", "a\0B", ""][next() as usize % 4].repeat(next() as usize % 32)
            );
            if next() % 3 != 0 {
                vector.insert(id.clone(), (next() % 9 + 1) as f64 / 10.0);
            }
            if next() % 3 != 0 {
                text.insert(id, (next() % 7 + 1) as f64);
            }
        }
        let window = [None, Some(0), Some(1), Some(4), Some(64)][next() as usize % 5];
        let mut options = options(next() as usize % 60, next() as usize % 17, window);
        options.fusion_weights = SearchFusionWeights {
            vector_weight: [0.0, 0.5, 1.0, 2.0][next() as usize % 4],
            text_weight: [0.0, 0.5, 1.0, 2.0][next() as usize % 4],
        };
        let competing = 137 + next() as usize % 1024;
        for mode in [SearchMode::Vector, SearchMode::Text, SearchMode::Hybrid] {
            let baseline = memory(4 * 1024 * 1024);
            let other = baseline.scores.reserve(competing).unwrap();
            let page = check(&vector, &text, mode, &options, &baseline).unwrap();
            returned += page.candidates.len();
            let peak = baseline.ledger.snapshot().peak_bytes;
            drop(page);
            drop(other);
            assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
            for limit in [peak, peak - 1] {
                let bounded = memory(limit);
                let result = bounded
                    .scores
                    .reserve(competing)
                    .and_then(|_lease| check(&vector, &text, mode, &options, &bounded));
                assert_eq!(result.is_ok(), limit == peak, "case={case} mode={mode:?}");
                accepted += usize::from(result.is_ok());
                drop(result);
                assert_eq!(bounded.ledger.snapshot().used_bytes, 0);
                assert_eq!(bounded.ledger.snapshot().account_count, 2);
            }
        }
    }
    assert_eq!(accepted, 1152);
    assert!(returned > 0);
    eprintln!(
        "ranking seed=0x206f0510 cases=384 modes=1152 exact=1152 short=1152 returned={returned}"
    );
}
