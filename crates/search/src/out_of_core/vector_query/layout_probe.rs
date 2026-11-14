// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Explicit release-only component measurements; never a CI performance gate.
//! The selected-vector arm compared V1 text decoders and was retired with them (#1469).

use super::*;
use hawdb_core::Value;
use hawdb_qos::ProcessMemorySnapshot;
use hawdb_storage::adjacency::AdjacencyDirection;
use hawdb_storage::cache::{ManifestGeneration, SegmentCache, StoreId};
use hawdb_storage::canonical::{
    CanonicalScanControl, CanonicalSegmentConfig, CanonicalSegmentReader, CanonicalSegmentWriter,
};
use hawdb_storage::canonical_adjacency::{
    CanonicalAdjacencyConfig, CanonicalAdjacencyEntry, CanonicalAdjacencyReader,
    CanonicalAdjacencyWriter,
};
use hawdb_storage::{NodeId, NodeRecord, RelId, RelRecord};
use serde_json::json;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

struct Directory(PathBuf);
impl Directory {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "hawdb-layout-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

struct AdjacencyRun {
    adjacency: CanonicalAdjacencyReader,
    canonical: CanonicalSegmentReader,
    cache: Arc<SegmentCache>,
    rows: usize,
    body_bytes: usize,
}
impl AdjacencyRun {
    fn scan(&self, properties: bool) -> (u64, u64, u64, u64) {
        let mut checksum = 0u64;
        let mut count = 0usize;
        let mut canonical_bytes = 0u64;
        let mut canonical_segments = 0u64;
        let report = if properties {
            self.adjacency
                .scan_endpoint_entries_control(
                    NodeId(1),
                    AdjacencyDirection::Outgoing,
                    Some(hawdb_core::RelTypeId(1)),
                    |entry| {
                        let row = match entry {
                            CanonicalAdjacencyEntry::Inline(row) => row,
                            CanonicalAdjacencyEntry::CanonicalReference { relationship_id } => {
                                let (row, report) = self
                                    .canonical
                                    .get_relationship_with_report(relationship_id)
                                    .unwrap();
                                canonical_bytes += report.bytes_read;
                                canonical_segments += report.segments_read;
                                row.unwrap()
                            }
                        };
                        let Value::String(body) = &row.properties["body"] else {
                            panic!("body")
                        };
                        assert_eq!(body.len(), self.body_bytes);
                        checksum =
                            checksum.wrapping_add(row.id.0 + row.target.0 + body.len() as u64);
                        count += 1;
                        Ok(CanonicalScanControl::Continue)
                    },
                )
                .unwrap()
                .0
        } else {
            self.adjacency
                .scan_endpoint_inputs_control(
                    NodeId(1),
                    AdjacencyDirection::Outgoing,
                    Some(hawdb_core::RelTypeId(1)),
                    |row| {
                        checksum = checksum.wrapping_add(row.relationship_id.0 + row.neighbor_id.0);
                        count += 1;
                        Ok(CanonicalScanControl::Continue)
                    },
                )
                .unwrap()
                .0
        };
        assert_eq!(count, self.rows);
        (
            checksum,
            report.bytes_read,
            canonical_bytes,
            canonical_segments,
        )
    }
}

#[test]
#[ignore = "manual release component measurement; run with --ignored --nocapture"]
fn adjacency_layout_measurement() {
    if cfg!(debug_assertions) {
        panic!("use the repository-wide release profile");
    }
    let directory = Directory::new("adjacency");
    let mut cases = Vec::new();
    for rows in [64usize, 1024] {
        for body_bytes in [0usize, 128, 8192] {
            let records = (0..rows)
                .map(|index| RelRecord {
                    id: RelId(index as u64 + 1),
                    source: NodeId(1),
                    target: NodeId(index as u64 + 100),
                    rel_type: hawdb_core::RelTypeId(1),
                    properties: BTreeMap::from([(
                        "body".into(),
                        Value::String("x".repeat(body_bytes)),
                    )]),
                })
                .collect::<Vec<_>>();
            let fixture = directory.0.join(format!("{rows}-{body_bytes}"));
            std::fs::create_dir(&fixture).unwrap();
            let canonical_path = fixture.join("canonical.hawdb");
            let canonical_manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig {
                target_segment_bytes: NonZeroU64::new(64 << 10).unwrap(),
                ..Default::default()
            })
            .write(
                &canonical_path,
                ManifestGeneration(1),
                std::iter::empty::<NodeRecord>(),
                &records,
            )
            .unwrap();
            let mut arms = Vec::new();
            for inline_limit in [16usize << 20, 256] {
                let path = fixture.join(format!("adjacency-{inline_limit}.hawdb"));
                let started = Instant::now();
                let output = CanonicalAdjacencyWriter::new(CanonicalAdjacencyConfig {
                    max_record_bytes: NonZeroU64::new(inline_limit as u64).unwrap(),
                    target_block_bytes: NonZeroU64::new(64 << 10).unwrap(),
                    ..Default::default()
                })
                .write_fallible(
                    &path,
                    ManifestGeneration(1),
                    records.iter().cloned().map(Ok),
                )
                .unwrap();
                arms.push((
                    path,
                    output.resident_manifest.unwrap(),
                    started.elapsed().as_nanos(),
                    output.report,
                ));
            }
            for cache_bytes in [0u64, 8 << 20] {
                let readers = arms
                    .iter()
                    .enumerate()
                    .map(|(arm, (path, manifest, _, _))| {
                        let cache = Arc::new(SegmentCache::new(cache_bytes));
                        AdjacencyRun {
                            adjacency: CanonicalAdjacencyReader::open(
                                path.clone(),
                                manifest.clone(),
                                Arc::clone(&cache),
                                StoreId(2 * arm as u128 + 1),
                                NonZeroU64::new(32 << 20).unwrap(),
                            )
                            .unwrap(),
                            canonical: CanonicalSegmentReader::open(
                                canonical_path.clone(),
                                canonical_manifest.clone(),
                                Arc::clone(&cache),
                                StoreId(2 * arm as u128 + 2),
                                NonZeroU64::new(32 << 20).unwrap(),
                            )
                            .unwrap(),
                            cache,
                            rows,
                            body_bytes,
                        }
                    })
                    .collect::<Vec<_>>();
                for properties in [false, true] {
                    let expected = readers[0].scan(properties).0;
                    assert_eq!(readers[1].scan(properties).0, expected);
                    for repetition in 0..7 {
                        for arm in if repetition % 2 == 0 { [0, 1] } else { [1, 0] } {
                            let before = readers[arm].cache.snapshot();
                            let started = Instant::now();
                            let output = black_box(readers[arm].scan(properties));
                            let elapsed = started.elapsed().as_nanos();
                            let after = readers[arm].cache.snapshot();
                            assert_eq!(output.0, expected);
                            cases.push(json!({"kind":"adjacency","candidate":arm==1,"repetition":repetition,
                                "rows":rows,"body_bytes":body_bytes,"properties":properties,"cache_budget_bytes":cache_bytes,
                                "elapsed_ns":elapsed,"adjacency_payload_bytes_visited":output.1,"canonical_payload_bytes_visited":output.2,
                                "canonical_segments_read":output.3,"cache_hits":after.hit_count-before.hit_count,
                                "cache_misses":after.miss_count-before.miss_count,"cache_resident_bytes":after.resident_bytes,
                                "adjacency_artifact_bytes":arms[arm].1.artifact_len,"build_ns":arms[arm].2,
                                "build_peak_resident_estimate_bytes":arms[arm].3.peak_resident_bytes,"result_checksum":expected}));
                        }
                    }
                }
            }
        }
    }
    let memory = ProcessMemorySnapshot::capture().unwrap();
    println!(
        "HAWDB_LAYOUT_RESULT {}",
        json!({"schema":"adjacency-layout-probe-v1","cases":cases,
        "process_resident_bytes":memory.resident_bytes,"process_peak_resident_bytes":memory.peak_resident_bytes})
    );
}
