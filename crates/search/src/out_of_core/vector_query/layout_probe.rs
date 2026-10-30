// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Explicit release-only component measurements; never a CI performance gate.

use super::*;
use hawdb_core::Value;
use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};
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
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

mod candidate;
use candidate::visit_records_selected;

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

fn vector_fixture(
    rows: usize,
    dimension: usize,
    exponent: bool,
) -> (
    String,
    SearchSegmentDescriptorEntry,
    SearchOutOfCoreSegmentLayout,
) {
    let mut text = String::from("HAWDB_SEARCH_VECTOR_SEGMENT_V1\n");
    for row in 0..rows {
        text.push_str(&format!(
            "vector\t{row}\t{}\t",
            crate::encode_string(&format!("doc:{row:08}"))
        ));
        for column in 0..dimension {
            if column != 0 {
                text.push(',');
            }
            let mut bits = (row as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15)
                ^ (column as u64 + 1).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            bits ^= bits >> 30;
            bits = bits.wrapping_mul(0x94d0_49bb_1331_11eb);
            let value = ((bits >> 40) as f32 / 8_388_608.0) - 1.0;
            if exponent {
                text.push_str(&format!("{value:e}"));
            } else {
                text.push_str(&value.to_string());
            }
        }
        text.push('\n');
    }
    let segment = SearchSegmentDescriptorEntry {
        segment_id: 0,
        first_document_id: "doc:00000000".into(),
        last_document_id: format!("doc:{:08}", rows - 1),
        document_count: rows,
        payload_range: None,
        metadata: BTreeMap::new(),
    };
    let range = SearchOutOfCoreRange {
        offset: 0,
        length: 0,
        checksum: 0,
        entry_count: rows,
    };
    let layout = SearchOutOfCoreSegmentLayout {
        segment_id: 0,
        vector_ordinal_base: 0,
        metadata: range,
        vectors: range,
    };
    (text, segment, layout)
}

struct VectorRun<'a> {
    path: &'a Path,
    encoded_len: usize,
    expanded_len: usize,
    checksum: u64,
    dimension: usize,
    segment: &'a SearchSegmentDescriptorEntry,
    layout: &'a SearchOutOfCoreSegmentLayout,
    selected: &'a [u64],
}

impl VectorRun<'_> {
    fn read(&self, candidate: bool) -> ((u64, usize), usize) {
        let budget = NonZeroUsize::new(32 << 20).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let memory = ledger.account(QueryMemoryClass::ExternalRead, "layout measurement", budget);
        let task = crate::RuntimeTaskContext::default();
        let mut checksum = 0u64;
        let mut count = 0usize;
        let score = |document: &SearchVectorDocument, sum: &mut u64, count: &mut usize| {
            assert_eq!(document.embedding.len(), self.dimension);
            let value = document
                .embedding
                .iter()
                .enumerate()
                .fold(0.0f64, |sum, (index, value)| {
                    sum + f64::from(*value) * ((index % 7 + 1) as f64)
                });
            *sum = sum.wrapping_add(value.to_bits().rotate_left((document.id.len() % 64) as u32));
            *count += 1;
        };
        let file = std::fs::File::open(self.path).unwrap();
        hydration::read_validated_with_account(
            file,
            self.encoded_len as u64,
            self.checksum,
            self.expanded_len as u64,
            Some(hydration::DecodeAdmission {
                memory: &memory,
                task: &task,
                max_header_bytes: 4096,
            }),
            |text| {
                let context = RecordContext {
                    segment: self.segment,
                    layout: self.layout,
                    dimension: Some(self.dimension),
                    limit: NonZeroU64::new(64 << 10).unwrap(),
                    query: QueryContext {
                        memory: &memory,
                        task: &task,
                        execution: None,
                        io_scope: None,
                    },
                };
                if candidate {
                    visit_records_selected(
                        text,
                        context,
                        |ordinal| self.selected.binary_search(&ordinal).is_ok(),
                        |document| {
                            score(document, &mut checksum, &mut count);
                            Ok(())
                        },
                    )
                } else {
                    let mut ordinal = 0u64;
                    visit_records(text, context, |document| {
                        if self.selected.binary_search(&ordinal).is_ok() {
                            score(document, &mut checksum, &mut count);
                        }
                        ordinal += 1;
                        Ok(())
                    })
                }
            },
        )
        .unwrap();
        assert_eq!(count, self.selected.len());
        assert_eq!(ledger.snapshot().used_bytes, 0);
        ((checksum, count), ledger.snapshot().peak_bytes)
    }
}

#[test]
#[ignore = "manual release component measurement; run with --ignored --nocapture"]
fn vector_materialization_measurement() {
    if cfg!(debug_assertions) {
        panic!("use the repository-wide release profile");
    }
    let directory = Directory::new("vector");
    let mut cases = Vec::new();
    for rows in [64usize, 1024] {
        for dimension in [32usize, 384, 1536] {
            for exponent in [false, true] {
                let (text, segment, layout) = vector_fixture(rows, dimension, exponent);
                let encoded = crate::encode_search_snapshot_text(&text).unwrap();
                let path = directory.0.join("vectors.hawdb");
                let mut file = std::fs::File::create(&path).unwrap();
                file.write_all(&encoded).unwrap();
                file.sync_all().unwrap();
                drop(file);
                for selected_count in [1usize, 32, rows] {
                    let selected = (0..selected_count)
                        .map(|index| (index * rows / selected_count) as u64)
                        .collect::<Vec<_>>();
                    let run = VectorRun {
                        path: &path,
                        encoded_len: encoded.len(),
                        expanded_len: text.len(),
                        checksum: checksum_bytes(&encoded),
                        dimension,
                        segment: &segment,
                        layout: &layout,
                        selected: &selected,
                    };
                    let expected = run.read(false).0;
                    assert_eq!(run.read(true).0, expected);
                    let iterations = if rows * dimension < 50_000 { 8 } else { 1 };
                    for repetition in 0..7 {
                        for candidate in if repetition % 2 == 0 {
                            [false, true]
                        } else {
                            [true, false]
                        } {
                            let start = Instant::now();
                            let mut memory_peak = 0;
                            for _ in 0..iterations {
                                let output = black_box(run.read(candidate));
                                assert_eq!(output.0, expected);
                                memory_peak = memory_peak.max(output.1);
                            }
                            cases.push(json!({"kind":"vector_stream", "candidate":candidate, "repetition":repetition,
                                "rows":rows,"dimension":dimension,"exponent":exponent,"selected":selected_count,
                                "iterations":iterations,"elapsed_ns":start.elapsed().as_nanos(),
                                "compressed_bytes_per_iteration":encoded.len(),"expanded_bytes_per_iteration":text.len(),
                                "score_checksum":expected.0,"query_memory_budget_bytes":32<<20,
                                "tracked_query_memory_peak_bytes":memory_peak}));
                        }
                    }
                }
            }
        }
    }
    let memory = ProcessMemorySnapshot::capture().unwrap();
    println!(
        "HAWDB_LAYOUT_RESULT {}",
        json!({"schema":"vector-materialization-probe-v1","cases":cases,
        "process_resident_bytes":memory.resident_bytes,"process_peak_resident_bytes":memory.peak_resident_bytes})
    );
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

fn decoded_bits(
    text: &str,
    segment: &SearchSegmentDescriptorEntry,
    layout: &SearchOutOfCoreSegmentLayout,
    dimension: usize,
    selected: &[u64],
    candidate: bool,
    cancelled: bool,
) -> Result<Vec<(String, Vec<u32>)>> {
    let budget = NonZeroUsize::new(1 << 20).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let memory = ledger.account(QueryMemoryClass::ExternalRead, "selection oracle", budget);
    let token = crate::RuntimeCancellationToken::new();
    let task = crate::RuntimeTaskContext::without_deadline(token.clone());
    if cancelled {
        token.cancel();
    }
    let context = RecordContext {
        segment,
        layout,
        dimension: Some(dimension),
        limit: NonZeroU64::new(64 << 10).unwrap(),
        query: QueryContext {
            memory: &memory,
            task: &task,
            execution: None,
            io_scope: None,
        },
    };
    let mut output = Vec::new();
    let mut ordinal = 0;
    let mut retain = |document: &SearchVectorDocument| {
        output.push((
            document.id.clone(),
            document
                .embedding
                .iter()
                .map(|value| value.to_bits())
                .collect(),
        ));
    };
    let result = if candidate {
        visit_records_selected(
            &mut Cursor::new(text.as_bytes()),
            context,
            |ordinal| selected.binary_search(&ordinal).is_ok(),
            |document| {
                retain(document);
                Ok(())
            },
        )
    } else {
        visit_records(&mut Cursor::new(text.as_bytes()), context, |document| {
            if selected.binary_search(&ordinal).is_ok() {
                retain(document);
            }
            ordinal += 1;
            Ok(())
        })
    };
    assert_eq!(ledger.snapshot().used_bytes, 0);
    result.map(|_| output)
}

#[test]
fn selected_stream_matches_native_decoder_for_every_subset_and_corrupt_rows() {
    let (_, segment, layout) = vector_fixture(3, 4, false);
    let valid = format!(
        "HAWDB_SEARCH_VECTOR_SEGMENT_V1\nvector\t0\t{}\t-0,0.1,1e-30,3.4028235e38\nvector\t1\t{}\t0,-0.25,-1e-40,-3.4028235e38\nvector\t2\t{}\t00.1,+0.1,.5,1.\n",
        crate::encode_string("doc:00000000"), crate::encode_string("doc:00000001"), crate::encode_string("doc:00000002"),
    );
    let mut inputs = vec![
        valid.clone(),
        valid.replace('\n', "\r\n"),
        valid.trim_end().into(),
    ];
    for raw in [
        "",
        "NaN,0,0,0",
        "inf,0,0,0",
        "-inf,0,0,0",
        "NaN,bad,0,0",
        "3.4028236e38,0,0,0",
        "0.1,0",
        "0.1,bad,0,0",
        "0.1,0,0,",
        " 0.1,0,0,0",
    ] {
        inputs.push(valid.replace("00.1,+0.1,.5,1.", raw));
    }
    for id in ["zz", "ff", "f", &crate::encode_string("doc:00000000")] {
        inputs.push(valid.replace(&crate::encode_string("doc:00000001"), id));
    }
    inputs.push(valid.replace("vector\t1", "vector\t18446744073709551616"));
    inputs.push(valid.replace("vector\t1", "vector\t4"));
    inputs.push(valid.lines().take(3).collect::<Vec<_>>().join("\n"));
    inputs.push(format!("{valid}{}\n", valid.lines().nth(3).unwrap()));
    for input in inputs {
        for mask in 0..8 {
            let selected = (0..3)
                .filter(|index| mask & (1 << index) != 0)
                .collect::<Vec<_>>();
            for cancelled in [false, true] {
                let expected =
                    decoded_bits(&input, &segment, &layout, 4, &selected, false, cancelled)
                        .map_err(|error| error.to_string());
                let actual = decoded_bits(&input, &segment, &layout, 4, &selected, true, cancelled)
                    .map_err(|error| error.to_string());
                assert_eq!(
                    actual, expected,
                    "mask={mask}, cancelled={cancelled}, input={input:?}"
                );
            }
        }
    }
}
