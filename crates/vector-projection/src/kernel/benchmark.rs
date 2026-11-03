// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod fixture;

use crate::{
    CandidateSet, KernelPreference, ProjectionError, ProjectionSearchOptions, RaBitQBitWidth,
    Result,
};
use fixture::{hit_bits, Case, Fixture, SEED};
use serde_json::json;
use std::fs::OpenOptions;
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Instant;

const SAMPLES: usize = 5;
const TOP_K: usize = 10;

fn matrix() -> Vec<Case> {
    let mut cases = Vec::new();
    for documents in [100_000, 1_000_000] {
        for dimension in [67, 384, 768, 1536] {
            for bit_width in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
                cases.push(Case {
                    documents,
                    dimension,
                    bit_width,
                });
            }
        }
    }
    cases
}

fn required_path(name: &str) -> Result<PathBuf> {
    std::env::var_os(name).map(PathBuf::from).ok_or_else(|| {
        ProjectionError::InvalidConfiguration(format!("set {name} for the manual benchmark"))
    })
}

fn record(output: &mut impl Write, value: serde_json::Value) -> Result<()> {
    serde_json::to_writer(&mut *output, &value)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}

fn measure_kernel(fixture: &Fixture, output: &mut impl Write) -> Result<()> {
    let width = fixture.case.bit_width;
    let bytes = crate::model::encoded_vector_bytes(fixture.case.dimension, width);
    let scorer = black_box(super::score_function(super::ScanKernel::Scalar));
    let mut expected = None;
    for sample in 0..=SAMPLES {
        let start = Instant::now();
        let mut checksum = 0u64;
        for segment in &fixture.memory.segments {
            for codes in segment.codes.chunks_exact(bytes) {
                let score = scorer(
                    black_box(codes),
                    black_box(&fixture.transformed_query),
                    width,
                );
                checksum = checksum.wrapping_add(u64::from(black_box(score).to_bits()));
            }
        }
        let elapsed = start.elapsed();
        assert_eq!(*expected.get_or_insert(checksum), checksum);
        record(
            output,
            json!({
                "phase": "score_codes", "case": fixture.case.name(),
                "sample": sample, "warmup": sample == 0, "kernel": "scalar",
                "elapsed_ns": elapsed.as_nanos(), "vectors": fixture.case.documents,
                "score_bit_sum": checksum,
            }),
        )?;
    }
    Ok(())
}

struct ScanCase<'a> {
    fixture: &'a Fixture,
    filter: &'static str,
    allowed: Option<&'a [u64]>,
    workers: usize,
    expected: &'a [(u64, u64)],
}

impl ScanCase<'_> {
    fn measure(&self, output: &mut impl Write, sample: usize, backend: &str) -> Result<()> {
        let mut options = ProjectionSearchOptions::new()
            .with_kernel(KernelPreference::Scalar)
            .with_max_parallelism(NonZeroUsize::new(self.workers).unwrap());
        if let Some(allowed) = self.allowed {
            options = options.with_candidate_set(CandidateSet::exact(allowed));
        }
        let start = Instant::now();
        let result = match backend {
            "memory" => self
                .fixture
                .memory
                .search(&self.fixture.query, TOP_K, options)?,
            "file" => self
                .fixture
                .file
                .search(&self.fixture.query, TOP_K, options)?,
            _ => unreachable!("benchmark backend is fixed"),
        };
        let elapsed = start.elapsed();
        let expected_count = self
            .allowed
            .map_or(self.fixture.case.documents, <[u64]>::len);
        assert_eq!(hit_bits(&result.hits), self.expected);
        assert_eq!(result.report.scored_document_count, expected_count);
        assert_eq!(
            result.report.filtered_document_count,
            self.fixture.case.documents - expected_count
        );
        assert_eq!(result.report.kernel, super::ScanKernel::Scalar);
        record(
            output,
            json!({
                "phase": "projection_search", "case": self.fixture.case.name(),
                "sample": sample, "warmup": sample == 0, "backend": backend,
                "kernel": result.report.kernel.as_str(),
                "filter": self.filter, "requested_workers": self.workers,
                "admitted_workers": result.report.worker_count,
                "elapsed_ns": elapsed.as_nanos(), "scored_vectors": expected_count,
                "admitted_working_bytes": result.report.admitted_working_bytes,
                "configured_working_bytes": options.max_working_bytes,
                "payload_bytes_read": result.report.payload_bytes_read,
                "scanned_blocks": result.report.scanned_block_count,
                "skipped_blocks": result.report.skipped_block_count,
                "hits": self.expected,
            }),
        )
    }
}

fn measure_fixture(fixture: &Fixture, output: &mut impl Write) -> Result<()> {
    let manifest = fixture.file.manifest();
    record(
        output,
        json!({
            "phase": "fixture", "case": fixture.case.name(),
            "documents": fixture.case.documents, "dimension": fixture.case.dimension,
            "bit_width": fixture.case.bit_width.bits(), "generator_seed": SEED,
            "source_digest": manifest.source_digest, "payload_crc32": manifest.payload_checksum,
            "payload_bytes": manifest.payload_bytes, "transform_seed": manifest.transform_seed,
            "artifact": fixture.file.path(), "query_bits": fixture.query.iter().map(|value| value.to_bits()).collect::<Vec<_>>(),
            "samples": SAMPLES, "warmups": 1, "top_k": TOP_K,
        }),
    )?;
    measure_kernel(fixture, output)?;
    for (filter, stride) in [
        ("unfiltered", None),
        ("sparse_1_percent", Some(100)),
        ("dense_50_percent", Some(2)),
    ] {
        let ids = stride.map(|stride| {
            (0..fixture.case.documents as u64)
                .step_by(stride)
                .collect::<Vec<_>>()
        });
        let mut options = ProjectionSearchOptions::new().with_kernel(KernelPreference::Scalar);
        if let Some(ids) = &ids {
            options = options.with_candidate_set(CandidateSet::exact(ids));
        }
        let expected = hit_bits(&fixture.file.search(&fixture.query, TOP_K, options)?.hits);
        for workers in [1, 4] {
            let case = ScanCase {
                fixture,
                filter,
                allowed: ids.as_deref(),
                workers,
                expected: &expected,
            };
            for sample in 0..=SAMPLES {
                let order = if sample % 2 == 0 {
                    ["memory", "file"]
                } else {
                    ["file", "memory"]
                };
                for backend in order {
                    case.measure(output, sample, backend)?;
                }
            }
        }
    }
    Ok(())
}

#[test]
#[ignore = "manual release-mode RaBitQ baseline for issue #994"]
fn candidate_scan_matrix() -> Result<()> {
    if cfg!(debug_assertions) {
        return Err(ProjectionError::InvalidConfiguration(
            "run the matrix with --release".into(),
        ));
    }
    let artifacts = required_path("HAWDB_RABITQ_BENCH_ARTIFACTS")?;
    let output = required_path("HAWDB_RABITQ_BENCH_OUTPUT")?;
    let build = match std::env::var("HAWDB_RABITQ_BENCH_BUILD").as_deref() {
        Ok("1") => true,
        Ok("0") | Err(std::env::VarError::NotPresent) => false,
        _ => {
            return Err(ProjectionError::InvalidConfiguration(
                "HAWDB_RABITQ_BENCH_BUILD must be 0 or 1".into(),
            ))
        }
    };
    let mut cases = matrix();
    if let Some(index) = std::env::var_os("HAWDB_RABITQ_BENCH_CASE") {
        let selected = index
            .to_str()
            .and_then(|value| value.parse::<usize>().ok())
            .and_then(|index| cases.get(index).copied())
            .ok_or_else(|| {
                ProjectionError::InvalidConfiguration(
                    "HAWDB_RABITQ_BENCH_CASE must be in 0..16".into(),
                )
            })?;
        cases = vec![selected];
    }
    if build {
        std::fs::create_dir_all(&artifacts)?;
    }
    let mut output = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output)?,
    );
    record(
        &mut output,
        json!({
            "phase": "run", "protocol": "hawdb.rabitq-kernel-baseline.v1", "build_artifacts": build,
            "cases": cases.iter().map(|case| case.name()).collect::<Vec<_>>(),
            "architecture": std::env::consts::ARCH, "os": std::env::consts::OS,
            "storage_io": cfg!(feature = "storage-io"),
        }),
    )?;
    for case in cases {
        let fixture = Fixture::open(&artifacts, case, build)?;
        measure_fixture(&fixture, &mut output)?;
    }
    record(&mut output, json!({"phase": "complete"}))
}

#[test]
fn artifact_fixture_preserves_codes_and_candidate_bits() -> Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let directory = loop {
        let nonce = NEXT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .expect("benchmark fixture counter overflow");
        let path = std::env::temp_dir().join(format!(
            "hawdb-rabitq-benchmark-{}-{nonce}",
            std::process::id()
        ));
        match std::fs::create_dir(&path) {
            Ok(()) => break path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    };
    for bit_width in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
        let case = Case {
            documents: 2057,
            dimension: 67,
            bit_width,
        };
        let fixture = Fixture::open(&directory, case, true)?;
        let reopened = Fixture::open(&directory, case, false)?;
        assert_eq!(fixture.file.manifest(), reopened.file.manifest());
        for (left, right) in fixture
            .memory
            .segments
            .iter()
            .zip(&reopened.memory.segments)
        {
            assert_eq!(left.ids, right.ids);
            assert_eq!(left.codes, right.codes);
        }
        for stride in [1, 2, 100] {
            let allowed = (0..case.documents as u64)
                .step_by(stride)
                .collect::<Vec<_>>();
            let options = ProjectionSearchOptions::new()
                .with_kernel(KernelPreference::Scalar)
                .with_candidate_set(CandidateSet::exact(&allowed));
            let expected = fixture
                .file
                .search(&fixture.query, case.documents, options)?;
            assert_eq!(expected.hits.len(), allowed.len());
            for workers in [1, 4] {
                let options = options.with_max_parallelism(NonZeroUsize::new(workers).unwrap());
                let memory = fixture
                    .memory
                    .search(&fixture.query, case.documents, options)?;
                let file = reopened
                    .file
                    .search(&fixture.query, case.documents, options)?;
                assert_eq!(hit_bits(&expected.hits), hit_bits(&memory.hits));
                assert_eq!(hit_bits(&expected.hits), hit_bits(&file.hits));
                assert_eq!(memory.report.scored_document_count, allowed.len());
                assert_eq!(file.report.scored_document_count, allowed.len());
            }
        }
    }
    std::fs::remove_dir_all(directory)?;
    Ok(())
}
