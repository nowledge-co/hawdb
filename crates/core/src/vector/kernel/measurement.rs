// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual release measurement against the original ordered scalar algorithms.

use super::{scalar, Input, Kernel, Mode};
use crate::vector::{PreparedVectorQuery, VectorDistance};
use std::hint::black_box;
use std::time::Instant;

fn original_distance(metric: VectorDistance, left: &[f32], right: &[f32]) -> f64 {
    let (mut dot, mut left_norm, mut right_norm, mut squared) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    for (&left, &right) in left.iter().zip(right) {
        assert!(left.is_finite() && right.is_finite());
        let (left, right) = (f64::from(left), f64::from(right));
        match metric {
            VectorDistance::Euclidean => squared += (left - right).powi(2),
            VectorDistance::Cosine => {
                dot += left * right;
                left_norm += left * left;
                right_norm += right * right;
            }
        }
    }
    match metric {
        VectorDistance::Euclidean => squared.sqrt(),
        VectorDistance::Cosine => {
            1.0 - (dot / (left_norm.sqrt() * right_norm.sqrt())).clamp(-1.0, 1.0)
        }
    }
}

fn original_positive(left: &[f32], right: &[f32]) -> f64 {
    let dot = |a: &[f32], b: &[f32]| {
        a.iter()
            .zip(b)
            .fold(0.0, |sum, (a, b)| sum + f64::from(*a) * f64::from(*b))
    };
    (dot(left, right) / (dot(left, left).sqrt() * dot(right, right).sqrt())).clamp(0.0, 1.0)
}

fn fixture(row: usize, dimension: usize) -> Vec<f32> {
    (0..dimension)
        .map(|column| {
            let mut state = (row as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15)
                ^ (column as u64 + 7).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            state ^= state >> 30;
            state = state.wrapping_mul(0x94d0_49bb_1331_11eb);
            ((state >> 40) as f32 / 8_388_608.0) - 1.0
        })
        .collect()
}

#[test]
#[ignore = "manual release distance comparison; not a CI performance gate"]
fn exact_distance_measurement() {
    if cfg!(debug_assertions) {
        panic!("use the repository-wide release profile");
    }
    for dimension in [3usize, 32, 384, 768, 1536, 8193] {
        let query = fixture(999, dimension);
        let rows = (0..128)
            .map(|row| fixture(row, dimension))
            .collect::<Vec<_>>();
        let iterations = if dimension < 64 { 32 } else { 4 };
        for metric in [VectorDistance::Cosine, VectorDistance::Euclidean] {
            let run = |arm: usize| {
                // Query preparation is inside timing, once per 128-candidate phase.
                let mut checksum = 0u64;
                for _ in 0..iterations {
                    let prepared =
                        (arm == 2).then(|| PreparedVectorQuery::new(black_box(&query)).unwrap());
                    for row in &rows {
                        let value = match arm {
                            0 => original_distance(
                                black_box(metric),
                                black_box(&query),
                                black_box(row),
                            ),
                            1 => metric
                                .distance_slices_with_work(
                                    black_box(&query),
                                    black_box(row),
                                    || Ok(()),
                                )
                                .unwrap(),
                            _ => prepared
                                .as_ref()
                                .unwrap()
                                .distance_slices_with_work(metric, black_box(row), || Ok(()))
                                .unwrap(),
                        };
                        checksum = checksum.wrapping_add(black_box(value.to_bits()));
                    }
                }
                checksum
            };
            let expected = run(0);
            assert_eq!(run(1), expected);
            assert_eq!(run(2), expected);
            for repetition in 0..9 {
                for offset in 0..3 {
                    let arm = (repetition + offset) % 3;
                    let start = Instant::now();
                    let checksum = run(arm);
                    let nanos = start.elapsed().as_nanos();
                    assert_eq!(checksum, expected);
                    println!("DISTANCE_RESULT {{\"scope\":\"distance\",\"dimension\":{dimension},\"rows\":128,\"iterations\":{iterations},\"metric\":\"{}\",\"arm\":{arm},\"repetition\":{repetition},\"elapsed_ns\":{nanos},\"checksum\":{checksum},\"kernel\":\"{}\"}}", metric.function_name(), crate::vector::distance_kernel_name(metric, dimension));
                }
            }
        }
        let run = |arm: usize| {
            let mut checksum = 0u64;
            for _ in 0..iterations {
                let scorer = (arm != 0).then(|| {
                    if arm == 2 {
                        PreparedVectorQuery::new(black_box(&query)).unwrap()
                    } else {
                        let kernel = Kernel(scalar, "scalar");
                        let norm = kernel
                            .calculate(
                                Input::Slice(&query),
                                Input::Slice(&query),
                                Mode::Norm,
                                || Ok(()),
                            )
                            .unwrap()
                            .left
                            .sqrt();
                        PreparedVectorQuery {
                            coordinates: &query,
                            norm,
                            kernel,
                        }
                    }
                });
                for row in &rows {
                    let value = match &scorer {
                        Some(scorer) => scorer
                            .positive_cosine_similarity(black_box(row))
                            .unwrap()
                            .unwrap(),
                        None => original_positive(black_box(&query), black_box(row)),
                    };
                    checksum = checksum.wrapping_add(black_box(value.to_bits()));
                }
            }
            checksum
        };
        let expected = run(0);
        assert_eq!(run(1), expected);
        assert_eq!(run(2), expected);
        for repetition in 0..9 {
            for offset in 0..3 {
                let arm = (repetition + offset) % 3;
                let start = Instant::now();
                let checksum = run(arm);
                let nanos = start.elapsed().as_nanos();
                assert_eq!(checksum, expected);
                println!("DISTANCE_RESULT {{\"scope\":\"positive\",\"dimension\":{dimension},\"rows\":128,\"iterations\":{iterations},\"arm\":{},\"repetition\":{repetition},\"elapsed_ns\":{nanos},\"checksum\":{checksum},\"kernel\":\"{}\"}}", arm, if arm == 2 { crate::vector::distance_kernel_name(VectorDistance::Cosine, dimension) } else { "scalar" });
            }
        }
    }
}
