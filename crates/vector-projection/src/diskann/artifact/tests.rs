// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn identity() -> DiskAnnArtifactIdentity {
    DiskAnnArtifactIdentity {
        binding_digest: [17; 32],
        source_digest: [17; 32],
        source_epoch: 19,
        generation: 23,
    }
}

#[derive(Debug)]
struct Memory {
    bytes: usize,
    used: Arc<AtomicUsize>,
}
impl Drop for Memory {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}
struct Admission {
    used: Arc<AtomicUsize>,
    limit: usize,
    checkpoints: AtomicUsize,
    cancel_after: usize,
}
impl Default for Admission {
    fn default() -> Self {
        Self {
            used: Arc::default(),
            limit: 1 << 20,
            checkpoints: AtomicUsize::new(0),
            cancel_after: usize::MAX,
        }
    }
}
impl Admission {
    fn reserve(&self, bytes: usize) -> Result<Memory> {
        let used = self.used.load(Ordering::SeqCst);
        require_memory(used.saturating_add(bytes), self.limit)?;
        self.used.fetch_add(bytes, Ordering::SeqCst);
        Ok(Memory {
            bytes,
            used: self.used.clone(),
        })
    }
}
impl ProjectionReadAdmission for Admission {
    type Retained = Memory;
    type Scratch = Memory;
    type Io = ();
    fn reserve_retained(&self, bytes: usize) -> Result<Memory> {
        self.reserve(bytes)
    }
    fn reserve_scratch(&self, bytes: usize) -> Result<Memory> {
        self.reserve(bytes)
    }
    fn acquire_io(&self) -> Result<()> {
        Ok(())
    }
    fn checkpoint(&self) -> Result<()> {
        if self.checkpoints.fetch_add(1, Ordering::SeqCst) >= self.cancel_after {
            return Err(ProjectionError::Cancelled(
                hawdb_core::RuntimeCancellationReason::Cancelled,
            ));
        }
        Ok(())
    }
}

struct Source(PathBuf);
impl Source {
    fn new(bytes: &[u8]) -> Self {
        let path = std::env::temp_dir().join(format!(
            "hawdb-diskann-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::write(&path, bytes).unwrap();
        Self(path)
    }
    fn open(
        &self,
        expected: DiskAnnArtifactIdentity,
        admission: &Admission,
    ) -> Result<FileDiskAnn<Memory>> {
        FileDiskAnn::open_admitted_file(File::open(&self.0).unwrap(), expected, admission)
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).unwrap();
    }
}

fn build(metric: VectorDistance, lsg: bool) -> DiskAnnIndex {
    DiskAnnIndex::build(
        2,
        metric,
        (0..31)
            .map(|id| {
                (
                    id * 13 + 7,
                    vec![((id * 7) % 17) as f32 - 8.0, id as f32 - 15.5],
                )
            })
            .collect(),
        DiskAnnBuildConfig {
            max_degree: 4,
            build_beam: 16,
            lsg: lsg.then_some(LsgBuildConfig::default()),
            ..Default::default()
        },
        None,
    )
    .unwrap()
}

fn encoded() -> Vec<u8> {
    let mut bytes = Vec::new();
    build(VectorDistance::Cosine, true)
        .write_artifact(&mut bytes, identity(), None)
        .unwrap();
    bytes
}

fn resign(bytes: &mut [u8]) {
    let checksum_position = bytes.len() - FOOTER_BYTES;
    let checksum = crc32fast::hash(&bytes[..checksum_position]);
    bytes[checksum_position..checksum_position + 4].copy_from_slice(&checksum.to_le_bytes());
}

#[test]
fn diskann_file_preserves_parameters_links_raw_distances_and_filtered_navigation() {
    for metric in [VectorDistance::Cosine, VectorDistance::Euclidean] {
        for lsg in [false, true] {
            let graph = build(metric, lsg);
            let mut bytes = Vec::new();
            let metadata = graph.write_artifact(&mut bytes, identity(), None).unwrap();
            let source = Source::new(&bytes);
            let admission = Admission::default();
            let file = source.open(identity(), &admission).unwrap();
            assert_eq!(file.metadata(), &metadata);
            #[cfg(feature = "storage-io")]
            {
                let mut checksum = hawdb_integrity::Crc32cHasher::new();
                checksum.update(&bytes);
                assert_eq!(
                    file.artifact_fingerprint(),
                    (bytes.len() as u64, checksum.finish() as u32)
                );
            }
            assert_eq!(
                admission.used.load(Ordering::SeqCst),
                size_of::<FileDiskAnn<Memory>>()
            );
            for ordinal in 0..graph.len() as u32 {
                assert_eq!(file.id(ordinal).unwrap(), graph.id(ordinal).unwrap());
                assert_eq!(
                    file.neighbors(ordinal).unwrap().collect::<Vec<_>>(),
                    graph.neighbors(ordinal).unwrap().collect::<Vec<_>>()
                );
            }
            for eligible in [None, Some(CandidateSet::exact(&[7, 137, 397]))] {
                let options = DiskAnnSearchOptions {
                    beam_width: 31,
                    ..Default::default()
                };
                let memory = search_diskann(&graph, &[1.0, -2.0], 3, eligible, options).unwrap();
                let mapped = search_diskann(&file, &[1.0, -2.0], 3, eligible, options).unwrap();
                assert_eq!(mapped.hits, memory.hits);
                assert_eq!(mapped.report, memory.report);
            }
            drop(file);
            assert_eq!(admission.used.load(Ordering::SeqCst), 0);
        }
    }
}

/// The in-memory graph scores one vertex at a time and the mapped graph four.
#[test]
fn diskann_batched_mapped_scores_match_single_scores_and_reports() {
    const DIMENSION: usize = 35;
    let mut random = 0x5ca1_ab1e_0000_0001u64;
    let mut next = || {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        (random % 10_000) as f32 / 5000.0 - 1.0
    };
    // Repeated vectors make ties decide the order.
    let distinct: Vec<Vec<f32>> = (0..250)
        .map(|_| (0..DIMENSION).map(|_| next()).collect())
        .collect();
    let queries: Vec<Vec<f32>> = (0..6)
        .map(|_| (0..DIMENSION).map(|_| next()).collect())
        .collect();
    let every: Vec<u64> = (0..400).collect();
    let most: Vec<u64> = (0..400).filter(|id| id % 5 != 2).collect();
    let mut switched = [0; 2];
    for metric in [VectorDistance::Cosine, VectorDistance::Euclidean] {
        let graph = DiskAnnIndex::build(
            DIMENSION,
            metric,
            (0..400u64)
                .map(|id| (id, distinct[id as usize % 250].clone()))
                .collect(),
            DiskAnnBuildConfig {
                max_degree: 12,
                build_beam: 48,
                ..Default::default()
            },
            None,
        )
        .unwrap();
        let mut bytes = Vec::new();
        graph.write_artifact(&mut bytes, identity(), None).unwrap();
        let source = Source::new(&bytes);
        let admission = Admission::default();
        let mapped = source.open(identity(), &admission).unwrap();
        for query in &queries {
            for filter in [None, Some(&every), Some(&most)] {
                let filter = filter.map(|ids| CandidateSet::exact(ids));
                // Beam 40 stays above the cap; beam 128 is below it.
                for beam_width in [40, 128] {
                    let options = DiskAnnSearchOptions {
                        beam_width,
                        ..Default::default()
                    };
                    let single = search_diskann(&graph, query, 20, filter, options).unwrap();
                    let batched = search_diskann(&mapped, query, 20, filter, options).unwrap();
                    assert_eq!(batched.report, single.report);
                    assert_eq!(batched.hits.len(), single.hits.len());
                    assert!(batched.hits.iter().zip(&single.hits).all(|(left, right)| {
                        left.id == right.id && left.distance.to_bits() == right.distance.to_bits()
                    }));
                    switched[usize::from(single.report.adaptive_exact_scan)] += 1;
                }
            }
        }
    }
    assert!(switched.iter().all(|count| *count > 0), "{switched:?}");
}

#[test]
fn diskann_mapped_successor_matches_a_fresh_artifact_without_reconstruction() {
    for metric in [VectorDistance::Cosine, VectorDistance::Euclidean] {
        for lsg in [false, true] {
            let graph = build(metric, lsg);
            let mut original = Vec::new();
            graph
                .write_artifact(&mut original, identity(), None)
                .unwrap();
            let source = Source::new(&original);
            let admission = Admission::default();
            let mapped = source.open(identity(), &admission).unwrap();
            let next = DiskAnnArtifactIdentity {
                generation: identity().generation + 1,
                ..identity()
            };
            let mut expected = Vec::new();
            graph.write_artifact(&mut expected, next, None).unwrap();
            let mut actual = Vec::new();
            mapped.write_artifact(&mut actual, next, None).unwrap();
            assert_eq!(actual, expected);
            let held = source.open(identity(), &admission).unwrap();
            assert_eq!(held.metadata().identity, identity());
            let mut input = File::open(&source.0).unwrap();
            assert_eq!(
                FileDiskAnn::inspect_file(&mut input).unwrap(),
                *held.metadata()
            );
            let cancellation = hawdb_core::RuntimeCancellationToken::new();
            cancellation.cancel();
            let task = RuntimeTaskContext::without_deadline(cancellation);
            let mut refused = Vec::new();
            assert!(matches!(
                held.write_artifact(&mut refused, next, Some(&task)),
                Err(ProjectionError::Cancelled(_))
            ));
            assert!(refused.is_empty());
            // The source mapping remains usable after a refused successor.
            let expected =
                search_diskann(&graph, &[1.0, 0.0], 3, None, Default::default()).unwrap();
            let actual = search_diskann(&held, &[1.0, 0.0], 3, None, Default::default()).unwrap();
            assert_eq!(actual.hits, expected.hits);
        }
    }
}

#[test]
fn diskann_file_rejects_truncation_trailing_bytes_and_checksum_damage() {
    let bytes = encoded();
    for length in [
        0,
        7,
        HEADER_BYTES - 1,
        HEADER_BYTES,
        bytes.len() - FOOTER_BYTES,
        bytes.len() - 1,
    ] {
        let source = Source::new(&bytes[..length]);
        assert!(source.open(identity(), &Admission::default()).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(Source::new(&trailing)
        .open(identity(), &Admission::default())
        .is_err());
    let mut damaged = bytes;
    damaged[HEADER_BYTES + 24] ^= 1;
    assert!(matches!(
        Source::new(&damaged).open(identity(), &Admission::default()),
        Err(ProjectionError::CorruptArtifact(_))
    ));
}

#[test]
fn diskann_file_validates_semantics_even_with_a_matching_checksum() {
    let original = encoded();
    let stride = RECORD_PREFIX_BYTES + 2 * 4 + 4 * 4;
    for (offset, replacement) in [
        (12, vec![2]),
        (13, vec![1]),
        (16, 1u32.to_le_bytes().to_vec()),
        (24, f64::NAN.to_le_bytes().to_vec()),
        (40, 0.0f64.to_le_bytes().to_vec()),
        (48, u64::MAX.to_le_bytes().to_vec()),
        (56, u32::MAX.to_le_bytes().to_vec()),
        (HEADER_BYTES + 8, 0.0f64.to_le_bytes().to_vec()),
        (HEADER_BYTES + 16, 5u32.to_le_bytes().to_vec()),
        (HEADER_BYTES + 20, 3u32.to_le_bytes().to_vec()),
        (HEADER_BYTES + 24, f32::NAN.to_le_bytes().to_vec()),
        (HEADER_BYTES + 32, 0u32.to_le_bytes().to_vec()),
        (HEADER_BYTES + 32, 31u32.to_le_bytes().to_vec()),
        (HEADER_BYTES + stride, 7u64.to_le_bytes().to_vec()),
    ] {
        let mut bytes = original.clone();
        bytes[offset..offset + replacement.len()].copy_from_slice(&replacement);
        resign(&mut bytes);
        let admission = Admission::default();
        assert!(
            matches!(
                Source::new(&bytes).open(identity(), &admission),
                Err(ProjectionError::CorruptArtifact(_))
            ),
            "offset {offset}"
        );
        assert_eq!(admission.used.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn diskann_file_refuses_identity_drift_and_retains_no_memory_after_refusal() {
    let source = Source::new(&encoded());
    for expected in [
        DiskAnnArtifactIdentity {
            source_digest: [0; 32],
            ..identity()
        },
        DiskAnnArtifactIdentity {
            binding_digest: [0; 32],
            ..identity()
        },
        DiskAnnArtifactIdentity {
            source_epoch: 20,
            ..identity()
        },
        DiskAnnArtifactIdentity {
            generation: 24,
            ..identity()
        },
    ] {
        let admission = Admission::default();
        assert!(matches!(
            source.open(expected, &admission),
            Err(ProjectionError::CorruptArtifact(_))
        ));
        assert_eq!(admission.used.load(Ordering::SeqCst), 0);
    }
    for limit in [1, VERIFY_BYTES] {
        let admission = Admission {
            limit,
            ..Default::default()
        };
        assert!(matches!(
            source.open(identity(), &admission),
            Err(ProjectionError::ResourceBudgetExceeded { .. })
        ));
        assert_eq!(admission.used.load(Ordering::SeqCst), 0);
    }
    assert!(source.open(identity(), &Admission::default()).is_ok());
}

#[test]
fn diskann_file_cancellation_releases_all_opening_charges() {
    let source = Source::new(&encoded());
    for cancel_after in [0, 1, 3, 20] {
        let admission = Admission {
            cancel_after,
            ..Default::default()
        };
        assert!(matches!(
            source.open(identity(), &admission),
            Err(ProjectionError::Cancelled(_))
        ));
        assert_eq!(admission.used.load(Ordering::SeqCst), 0);
    }
    assert!(source.open(identity(), &Admission::default()).is_ok());
}

#[test]
fn diskann_empty_and_singleton_files_are_valid_graphs() {
    for entries in [Vec::new(), vec![(17, vec![1.0, 0.0])]] {
        let graph = DiskAnnIndex::build(
            2,
            VectorDistance::Cosine,
            entries,
            DiskAnnBuildConfig::default(),
            None,
        )
        .unwrap();
        let mut bytes = Vec::new();
        graph.write_artifact(&mut bytes, identity(), None).unwrap();
        let source = Source::new(&bytes);
        let mapped = source.open(identity(), &Admission::default()).unwrap();
        let result = search_diskann(&mapped, &[1.0, 0.0], 1, None, Default::default()).unwrap();
        assert_eq!(result.hits.len(), graph.len());
    }
}
