// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn prepared_raw_scores_match_original_three_pass_score_bits() {
    for dimension in [1, 2, 3, 7, 32, 65, 384, 768, 1536, 8193] {
        let query = (0..dimension)
            .map(|i| ((i * 173 + 11) as f32).sin())
            .collect::<Vec<_>>();
        let scorer = CosineQuery::new(&query);
        for row in 0..17 {
            let mut vector = (0..dimension)
                .map(|i| ((i * 97 + row * 31) as f32).cos())
                .collect::<Vec<_>>();
            for boundary in [0.0, -0.0, f32::MAX, f32::MIN_POSITIVE, f32::from_bits(1)] {
                vector[dimension / 2] = boundary;
                assert_eq!(
                    scorer.score(&vector).map(f64::to_bits),
                    scalar_cosine_similarity(&query, &vector).map(f64::to_bits)
                );
            }
        }
    }
    for query in [
        vec![],
        vec![0.0, -0.0],
        vec![f32::NAN, 1.0],
        vec![f32::INFINITY, 1.0],
        vec![1.0, 0.0],
    ] {
        let scorer = CosineQuery::new(&query);
        for vector in [
            vec![],
            vec![0.0, 0.0],
            vec![-1.0, 0.0],
            vec![f32::NAN, 1.0],
            vec![f32::NEG_INFINITY, 1.0],
        ] {
            assert_eq!(
                scorer.score(&vector).map(f64::to_bits),
                scalar_cosine_similarity(&query, &vector).map(f64::to_bits)
            );
        }
    }
}

#[test]
fn query_preparation_preserves_work_failure_and_can_be_reused() {
    let query = vec![1.0; 20_000];
    let mut work = 0;
    let result = CosineQuery::with_work(&query, || {
        work += 1;
        if work == 2 {
            Err(HawDBError::ProjectionBusy)
        } else {
            Ok(())
        }
    });
    assert!(matches!(result, Err(HawDBError::ProjectionBusy)));
    assert_eq!(work, 2);
    let scorer = CosineQuery::with_work(&query, || {
        work += 1;
        Ok(())
    })
    .unwrap();
    let prepared_work = work;
    for _ in 0..5 {
        assert_eq!(scorer.score(&query), Some(1.0));
    }
    assert_eq!(work, prepared_work);
}
