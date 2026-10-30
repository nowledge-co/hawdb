// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::vector::{PreparedVectorQuery, Vector, VectorDistance, VectorRef};

fn kernels() -> Vec<Kernel> {
    let mut kernels = vec![Kernel(scalar, "scalar")];
    kernels.push(Kernel::get());
    #[cfg(all(
        any(target_arch = "x86", target_arch = "x86_64"),
        target_endian = "little"
    ))]
    if std::is_x86_feature_detected!("avx2") {
        kernels.push(Kernel(avx2::run, "avx2"));
    }
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    if std::arch::is_aarch64_feature_detected!("neon") {
        kernels.push(Kernel(neon::run, "neon"));
    }
    kernels
}

fn bits(sums: &Sums) -> [u64; 4] {
    [
        sums.dot.to_bits(),
        sums.left.to_bits(),
        sums.right.to_bits(),
        sums.squared.to_bits(),
    ]
}

#[test]
fn forced_native_blocks_cover_short_unaligned_loads_and_nonfinite_tails() {
    for dimension in 0..=17 {
        let left = (0..dimension).map(|i| i as f32 - 3.5).collect::<Vec<_>>();
        for bad in [false, true] {
            let mut right = left.clone();
            if bad && dimension != 0 {
                right[dimension - 1] = f32::NAN;
            }
            for offset in 0..4 {
                let mut bytes = vec![0; offset];
                bytes.extend(right.iter().flat_map(|value| value.to_le_bytes()));
                for mode in [
                    Mode::Cosine,
                    Mode::PreparedCosine,
                    Mode::Euclidean,
                    Mode::Norm,
                ] {
                    let mut expected = Sums::default();
                    // SAFETY: scalar has no ISA requirement; input dimensions match.
                    let valid = unsafe {
                        scalar(
                            Input::Slice(&left),
                            Input::Slice(&right),
                            mode,
                            &mut expected,
                        )
                    };
                    for kernel in kernels() {
                        let mut actual = Sums::default();
                        // SAFETY: kernels() checks CPU support; byte length is exactly
                        // dimension*4. This bypasses Auto's intentional tiny-vector fallback.
                        let observed = unsafe {
                            (kernel.0)(
                                Input::Slice(&left),
                                Input::Encoded(&bytes[offset..]),
                                mode,
                                &mut actual,
                            )
                        };
                        assert_eq!(observed, valid);
                        if valid {
                            assert_eq!(bits(&actual), bits(&expected));
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn native_kernels_match_ordered_scalar_for_tails_extremes_and_unaligned_bytes() {
    for dimension in [
        1, 2, 3, 4, 7, 8, 15, 31, 32, 65, 384, 768, 1536, 8191, 8192, 8193, 20000,
    ] {
        let mut left = (0..dimension)
            .map(|i| ((i * 173 + 19) as f32).sin())
            .collect::<Vec<_>>();
        let right = (0..dimension)
            .map(|i| ((i * 97 + 3) as f32).cos())
            .collect::<Vec<_>>();
        for extreme in [0.0, -0.0, f32::MAX, f32::MIN_POSITIVE, f32::from_bits(1)] {
            left[dimension / 2] = extreme;
            let mut encoded = vec![17u8];
            encoded.extend(left.iter().flat_map(|v| v.to_le_bytes()));
            for mode in [
                Mode::Cosine,
                Mode::PreparedCosine,
                Mode::Euclidean,
                Mode::Norm,
            ] {
                let expected = Kernel(scalar, "scalar")
                    .calculate(Input::Slice(&left), Input::Slice(&right), mode, || Ok(()))
                    .unwrap();
                for kernel in kernels() {
                    for input in [Input::Slice(&left), Input::Encoded(&encoded[1..])] {
                        let actual = kernel
                            .calculate(input, Input::Slice(&right), mode, || Ok(()))
                            .unwrap();
                        assert_eq!(bits(&actual), bits(&expected), "dimension={dimension}");
                    }
                }
            }
        }
    }
}

#[test]
fn prepared_distances_and_positive_scores_preserve_scalar_bits() {
    for dimension in [1, 3, 4, 7, 384, 768, 1536, 8193] {
        let left = (0..dimension)
            .map(|i| ((i * 19 + 7) as f32).sin())
            .collect::<Vec<_>>();
        let right = (0..dimension)
            .map(|i| ((i * 31 + 3) as f32).cos())
            .collect::<Vec<_>>();
        let prepared = PreparedVectorQuery::new(&left).unwrap();
        let encoded = Vector::new(&right).unwrap();
        let mut unaligned = vec![0];
        unaligned.extend_from_slice(encoded.encoded());
        let reference = Kernel(scalar, "scalar")
            .calculate(
                Input::Slice(&left),
                Input::Slice(&right),
                Mode::Cosine,
                || Ok(()),
            )
            .unwrap();
        let positive =
            (reference.dot / (reference.left.sqrt() * reference.right.sqrt())).clamp(0.0, 1.0);
        assert_eq!(
            prepared
                .positive_cosine_similarity(&right)
                .unwrap()
                .unwrap()
                .to_bits(),
            positive.to_bits()
        );
        for metric in [VectorDistance::Cosine, VectorDistance::Euclidean] {
            let sums = Kernel(scalar, "scalar")
                .calculate(
                    Input::Slice(&left),
                    Input::Slice(&right),
                    match metric {
                        VectorDistance::Cosine => Mode::Cosine,
                        VectorDistance::Euclidean => Mode::Euclidean,
                    },
                    || Ok(()),
                )
                .unwrap();
            let expected = super::super::finish_distance(metric, &sums, sums.left.sqrt()).unwrap();
            for actual in [
                metric
                    .distance_slices_with_work(&left, &right, || Ok(()))
                    .unwrap(),
                prepared
                    .distance_slices_with_work(metric, &right, || Ok(()))
                    .unwrap(),
                prepared
                    .distance_to_vector_with_work(
                        metric,
                        VectorRef::from_encoded(&unaligned[1..]).unwrap(),
                        || Ok(()),
                    )
                    .unwrap(),
            ] {
                assert_eq!(actual.to_bits(), expected.to_bits());
            }
        }
    }
}

#[test]
fn prepared_tiny_distances_preserve_scalar_errors_and_work() {
    for dimension in [1, 3, 15, 16] {
        let left = vec![1.0; dimension];
        let prepared = PreparedVectorQuery::new(&left).unwrap();
        for value in [0.0, 0.25, f32::MAX, f32::NAN] {
            let right = vec![value; dimension];
            for metric in [VectorDistance::Cosine, VectorDistance::Euclidean] {
                for cancel_at in [0, 1, usize::MAX] {
                    let run = |prepared_path: bool| {
                        let mut calls = 0;
                        let checkpoint = || {
                            let current = calls;
                            calls += 1;
                            if current == cancel_at {
                                Err(crate::HawDBError::ProjectionBusy)
                            } else {
                                Ok(())
                            }
                        };
                        let result = if prepared_path {
                            prepared.distance_slices_with_work(metric, &right, checkpoint)
                        } else {
                            metric.distance_slices_with_work(&left, &right, checkpoint)
                        };
                        (
                            result.map(f64::to_bits).map_err(|error| error.to_string()),
                            calls,
                        )
                    };
                    assert_eq!(run(true), run(false));
                }
            }
        }
    }
}

#[test]
fn errors_and_work_boundaries_preserve_scalar_precedence() {
    for kernel in kernels() {
        for dimension in [1, 7, 8192, 8193, 20000] {
            for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
                for position in [0, dimension / 2, dimension - 1] {
                    let left = vec![1.0; dimension];
                    let mut right = left.clone();
                    right[position] = bad;
                    for cancel_at in [0, 1, 2, 3, usize::MAX] {
                        let run = |kernel: Kernel| {
                            let mut work = 0;
                            let result = kernel.calculate(
                                Input::Slice(&left),
                                Input::Slice(&right),
                                Mode::Cosine,
                                || {
                                    let current = work;
                                    work += 1;
                                    if current == cancel_at {
                                        Err(crate::HawDBError::ProjectionBusy)
                                    } else {
                                        Ok(())
                                    }
                                },
                            );
                            (result.map(|s| bits(&s)).map_err(|e| e.to_string()), work)
                        };
                        assert_eq!(run(kernel), run(Kernel(scalar, "scalar")));
                    }
                }
            }
        }
    }
    assert!(PreparedVectorQuery::new(&[]).is_err());
    assert!(PreparedVectorQuery::new(&[f32::NAN]).is_err());
    let zero = [0.0, -0.0];
    let prepared = PreparedVectorQuery::new(&zero).unwrap();
    assert_eq!(prepared.positive_cosine_similarity(&zero).unwrap(), None);
    assert!(prepared
        .distance_slices_with_work(VectorDistance::Cosine, &zero, || Ok(()))
        .is_err());
    assert_eq!(
        prepared
            .distance_slices_with_work(VectorDistance::Euclidean, &zero, || Ok(()))
            .unwrap(),
        0.0
    );
}
