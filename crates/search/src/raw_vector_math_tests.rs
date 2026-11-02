// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn prepared_raw_scores_match_original_three_pass_score_bits() {
    for dimension in [1, 2, 3, 7, 32, 65, 384, 768, 1536, 8193] {
        let query = (0..dimension)
            .map(|i| ((i * 173 + 11) as f32).sin())
            .collect::<Vec<_>>();
        let scorer = VectorRelevanceQuery::new(&query, hawdb_core::VectorDistance::Cosine);
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
        let scorer = VectorRelevanceQuery::new(&query, hawdb_core::VectorDistance::Cosine);
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
    let result =
        VectorRelevanceQuery::with_work(&query, hawdb_core::VectorDistance::Cosine, || {
            work += 1;
            if work == 2 {
                Err(HawDBError::ProjectionBusy)
            } else {
                Ok(())
            }
        });
    assert!(matches!(result, Err(HawDBError::ProjectionBusy)));
    assert_eq!(work, 2);
    let scorer =
        VectorRelevanceQuery::with_work(&query, hawdb_core::VectorDistance::Cosine, || {
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

#[test]
fn euclidean_relevance_is_defined_for_every_finite_pair_and_orders_by_distance() {
    for dimension in [2, 3, 16, 65, 384] {
        let query = (0..dimension)
            .map(|i| ((i * 173 + 11) as f32).sin())
            .collect::<Vec<_>>();
        let scorer = VectorRelevanceQuery::new(&query, hawdb_core::VectorDistance::Euclidean);
        let opposite = query.iter().map(|value| -value).collect::<Vec<_>>();
        let distance = hawdb_core::VectorDistance::Euclidean
            .distance_slices_with_work(&query, &opposite, || Ok(()))
            .unwrap();
        let score = scorer.score(&opposite).unwrap();
        assert!((score - 1.0 / (1.0 + distance)).abs() < 1e-12);
        assert!(score > 0.0);
        assert_eq!(scorer.score(&query), Some(1.0));
        let far = query.iter().map(|value| value * 3.0).collect::<Vec<_>>();
        assert!(scorer.score(&far).unwrap() < scorer.score(&query).unwrap());
        assert_eq!(scorer.score(&query[1..]), None);
        let zero = vec![0.0; dimension];
        let zero_scorer = VectorRelevanceQuery::new(&zero, hawdb_core::VectorDistance::Euclidean);
        assert!(zero_scorer.score(&query).is_some_and(|score| score > 0.0));
    }
}
