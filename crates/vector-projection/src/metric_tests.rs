// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Semantic, persistence and refusal controls for both candidate metrics.

use crate::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const DIMENSION: usize = 64;
static SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let root = std::env::var_os("TEST_TMPDIR")
            .map_or_else(std::env::temp_dir, PathBuf::from)
            .join(format!(
                "hawdb-rabitq-metrics-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&root).unwrap();
        Self(root)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn vector(first: f32) -> Vec<f32> {
    let mut vector = vec![0.0; DIMENSION];
    vector[0] = first;
    vector
}

fn config(width: RaBitQBitWidth, metric: ProjectionMetric) -> ProjectionBuildConfig {
    ProjectionBuildConfig::new(DIMENSION, ProjectionIdentity::new(1))
        .with_bit_width(width)
        .with_metric(metric)
        .with_segment_rows(2)
}

fn corpus() -> Vec<Vec<f32>> {
    [100.0, 1.25, 1.0, -0.125, 0.0, f32::MAX, f32::from_bits(1)]
        .into_iter()
        .map(vector)
        .collect()
}

fn build(width: RaBitQBitWidth, metric: ProjectionMetric) -> InMemoryProjection {
    let mut builder = ProjectionBuilder::new(config(width, metric)).unwrap();
    for (id, vector) in corpus().iter().enumerate() {
        builder.push(id as u64, vector).unwrap();
    }
    builder.finish().unwrap()
}

fn write(path: &Path, width: RaBitQBitWidth, metric: ProjectionMetric) -> FileProjection {
    let mut writer = ProjectionWriter::create(path, config(width, metric)).unwrap();
    for (id, vector) in corpus().iter().enumerate() {
        writer.push(id as u64, vector).unwrap();
    }
    writer.finish().unwrap()
}

fn options() -> ProjectionSearchOptions<'static> {
    ProjectionSearchOptions::default().with_kernel(KernelPreference::Scalar)
}

#[test]
fn both_widths_distinguish_nonunit_l2_from_cosine_and_preserve_extremes() {
    for width in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
        let l2 = build(width, ProjectionMetric::SquaredL2);
        let angular = build(width, ProjectionMetric::Cosine);
        let query = vector(1.0);
        let hits = l2.search(&query, corpus().len(), options()).unwrap();
        assert_eq!(hits.report.metric, ProjectionMetric::SquaredL2);
        assert_eq!(hits.hits[0].id, 2);
        assert!(hits.hits.iter().all(|hit| hit.score.is_finite()));
        assert!(hits.hits.iter().find(|hit| hit.id == 5).unwrap().score < -1e70);
        assert_eq!(angular.search(&query, 1, options()).unwrap().hits[0].id, 0);

        let zero = l2.search(&vector(0.0), corpus().len(), options()).unwrap();
        assert_eq!(zero.hits[0].id, 4);
        assert_eq!(zero.hits[0].score, 0.0);
        assert_eq!(zero.hits[1].id, 6);
        assert!(zero.hits[1].score < 0.0 && zero.hits[1].score > -1e-80);

        let allowed = [0, 1, 4];
        let filtered = l2
            .search(&query, 3, options().with_allowed_ids(&allowed))
            .unwrap();
        assert_eq!(filtered.hits[0].id, 1);
        assert_eq!(filtered.report.scored_document_count, allowed.len());
        assert!(filtered.hits.iter().all(|hit| allowed.contains(&hit.id)));
    }
}

#[test]
fn calibrated_self_similarity_and_l2_match_the_identical_vector_contract() {
    let source = (0..DIMENSION)
        .map(|index| ((index * 37 + 11) % 97) as f32 / 19.0 - 2.0)
        .collect::<Vec<_>>();
    for width in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
        for metric in [ProjectionMetric::Cosine, ProjectionMetric::SquaredL2] {
            let mut builder = ProjectionBuilder::new(config(width, metric)).unwrap();
            builder.push(7, &source).unwrap();
            let projection = builder.finish().unwrap();
            let result = projection.search(&source, 1, options()).unwrap();
            let expected = if metric == ProjectionMetric::Cosine {
                1.0
            } else {
                0.0
            };
            assert!(
                (result.hits[0].score - expected).abs() < 1e-4,
                "{width:?} {metric:?}: {:?}",
                result.hits
            );
        }
    }
}

#[test]
fn file_reopen_keeps_metric_norms_and_candidate_bits() {
    let root = Directory::new();
    for width in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
        for metric in [ProjectionMetric::Cosine, ProjectionMetric::SquaredL2] {
            let path = root.0.join(format!("{width:?}-{metric:?}.bin"));
            let mapped = write(&path, width, metric);
            assert_eq!(mapped.manifest().metric, metric);
            assert_eq!(mapped.manifest().format_version, 2);
            drop(mapped);
            let mapped = FileProjection::open(&path).unwrap();
            let memory = build(width, metric);
            for query in [vector(0.0), vector(1.0), vector(f32::from_bits(1))] {
                assert_eq!(
                    memory
                        .search(&query, corpus().len(), options())
                        .unwrap()
                        .hits,
                    mapped
                        .search(&query, corpus().len(), options())
                        .unwrap()
                        .hits
                );
            }
        }
    }
}

#[test]
fn delta_inherits_the_base_l2_metric_including_a_zero_query() {
    for width in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
        let base = build(width, ProjectionMetric::SquaredL2);
        let mut delta = DeltaBuffer::new(DIMENSION);
        delta.upsert(0, &vector(0.9)).unwrap();
        let allowed = [0, 1];
        for first in [0.0, 1.0] {
            let query = vector(first);
            let result = search_with_delta(
                &query,
                1,
                options().with_allowed_ids(&allowed),
                &delta,
                |query, count, controls| base.search(query, count, controls),
            )
            .unwrap();
            assert_eq!(result.hits[0].id, 0);
            let expected = -(f64::from(first) - f64::from(0.9_f32)).powi(2);
            assert_eq!(result.hits[0].score, expected);
        }
    }
}

#[test]
fn l2_budget_and_cancel_refusals_never_return_partial_candidates() {
    assert!(matches!(
        ProjectionBuilder::new(
            config(RaBitQBitWidth::Four, ProjectionMetric::SquaredL2).with_max_working_bytes(1)
        ),
        Err(ProjectionError::ResourceBudgetExceeded { .. })
    ));
    let base = build(RaBitQBitWidth::Four, ProjectionMetric::SquaredL2);
    assert!(matches!(
        base.search(&vector(1.0), 1, options().with_max_working_bytes(1)),
        Err(ProjectionError::ResourceBudgetExceeded { .. })
    ));
    let cancellation = hawdb_core::RuntimeCancellationToken::new();
    cancellation.cancel();
    let task = hawdb_core::RuntimeTaskContext::without_deadline(cancellation);
    assert!(matches!(
        base.search(&vector(1.0), 1, options().with_task_context(&task)),
        Err(ProjectionError::Cancelled(_))
    ));
    let empty = base
        .search(&vector(1.0), 0, options().with_max_working_bytes(0))
        .unwrap();
    assert!(empty.hits.is_empty());
    assert_eq!(empty.report.admitted_working_bytes, 0);
}

fn reseal(bytes: &mut Vec<u8>, manifest: &mut ProjectionManifest, start: usize) {
    for segment in &mut manifest.segments {
        let begin = segment.payload_offset as usize;
        segment.payload_checksum =
            crc32fast::hash(&bytes[begin..begin + segment.payload_bytes as usize]);
    }
    manifest.payload_checksum = crc32fast::hash(&bytes[..start]);
    let magic = bytes[bytes.len() - 8..].to_vec();
    let encoded = serde_json::to_vec(manifest).unwrap();
    bytes.truncate(start);
    bytes.extend_from_slice(&encoded);
    bytes.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&encoded).to_le_bytes());
    bytes.extend_from_slice(&magic);
}

#[test]
fn valid_checksums_do_not_hide_invalid_norms_or_obsolete_versions() {
    let root = Directory::new();
    for (number, invalid) in [f64::NAN, -1.0, f64::INFINITY].into_iter().enumerate() {
        let path = root.0.join(format!("bad-norm-{number}.bin"));
        drop(write(
            &path,
            RaBitQBitWidth::Four,
            ProjectionMetric::SquaredL2,
        ));
        let mut bytes = std::fs::read(&path).unwrap();
        let footer = bytes.len() - 20;
        let length = u64::from_le_bytes(bytes[footer..footer + 8].try_into().unwrap()) as usize;
        let start = footer - length;
        let mut manifest: ProjectionManifest =
            serde_json::from_slice(&bytes[start..footer]).unwrap();
        let norm = manifest.segments[0].row_count * 16;
        bytes[norm..norm + 8].copy_from_slice(&invalid.to_le_bytes());
        reseal(&mut bytes, &mut manifest, start);
        std::fs::write(&path, &bytes).unwrap();
        let error = FileProjection::open(&path).unwrap_err();
        assert!(error.to_string().contains("original norm"), "{error}");
    }
    let path = root.0.join("obsolete.bin");
    drop(write(&path, RaBitQBitWidth::One, ProjectionMetric::Cosine));
    let mut bytes = std::fs::read(&path).unwrap();
    let footer = bytes.len() - 20;
    let length = u64::from_le_bytes(bytes[footer..footer + 8].try_into().unwrap()) as usize;
    let start = footer - length;
    let mut manifest: ProjectionManifest = serde_json::from_slice(&bytes[start..footer]).unwrap();
    manifest.format_version = 1;
    reseal(&mut bytes, &mut manifest, start);
    std::fs::write(&path, bytes).unwrap();
    assert!(FileProjection::open(&path)
        .unwrap_err()
        .to_string()
        .contains("unsupported format version"));
}
