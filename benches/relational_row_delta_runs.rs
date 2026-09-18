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

//! Cold-open and scan evidence for immutable relational recovery runs.
//!
//! Release mode exercises the production admission boundary at 32, 256, and
//! 4096 runs. Debug mode remains a bounded smoke test for `cargo test --benches`.

use hawdb_core::RuntimeTaskContext;
use hawdb_integrity::Sha256Digest;
use hawdb_storage::{
    RelationalColumnSchema, RelationalInsertMode, RelationalOverflowPublicationConfig,
    RelationalOverflowPublisher, RelationalOverflowRootReader, RelationalRecoveryFence,
    RelationalRecoverySourceBuilder, RelationalRecoverySourceIdentity, RelationalRow,
    RelationalRowChange, RelationalRowChangeCapture, RelationalRowDeltaBuilder,
    RelationalRowDeltaConfig, RelationalRowDeltaReader, RelationalRowDeltaTableMetadata,
    RelationalRowPageProjectedRange, RelationalRowPagePublicationConfig,
    RelationalRowPagePublisher, RelationalRowPageReadView, RelationalRowPageRootReader,
    RelationalRowPageSnapshotReadLimits, RelationalRowPageSnapshotReader, RelationalScalarType,
    RelationalState, RelationalStore, RelationalTableSchema, RelationalTransaction,
    RelationalValue, RelationalWrite, SegmentCache, StoreId,
};
use serde_json::json;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

const SMOKE: bool = cfg!(debug_assertions);
const RELEASE_RUN_COUNTS: &[usize] = &[32, 256, 4096];
const SMOKE_RUN_COUNTS: &[usize] = &[8, 32];
const RELEASE_FILE_POOL_CAPACITIES: &[usize] = &[8, 32, 64];
const SMOKE_FILE_POOL_CAPACITIES: &[usize] = &[8, 32];
const RELEASE_SAMPLES: usize = 3;
const SMOKE_SAMPLES: usize = 1;
const CACHE_BYTES: u64 = 4 * 1024 * 1024;
const TABLE: &str = "documents";
const REQUESTED_FIELDS: &[usize] = &[0];

static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn main() {
    let run_counts = if SMOKE {
        SMOKE_RUN_COUNTS
    } else {
        RELEASE_RUN_COUNTS
    };
    let file_pool_capacities = if SMOKE {
        SMOKE_FILE_POOL_CAPACITIES
    } else {
        RELEASE_FILE_POOL_CAPACITIES
    };
    let samples = if SMOKE {
        SMOKE_SAMPLES
    } else {
        RELEASE_SAMPLES
    };
    let mut evidence = Vec::with_capacity(run_counts.len() * file_pool_capacities.len());
    for &run_count in run_counts {
        let fixture = Fixture::new(run_count);
        for &file_pool_capacity in file_pool_capacities {
            evidence.push(measure(&fixture, file_pool_capacity, samples));
        }
        fixture.remove();
    }
    println!(
        "relational_row_delta_runs {}",
        json!({
            "protocol": "hawdb-relational-row-delta-runs-v1",
            "release_run_counts": RELEASE_RUN_COUNTS,
            "release_file_pool_capacities": RELEASE_FILE_POOL_CAPACITIES,
            "smoke": SMOKE,
            "samples": samples,
            "evidence": evidence,
        })
    );
}

fn measure(fixture: &Fixture, file_pool_capacity: usize, samples: usize) -> serde_json::Value {
    let mut open_nanos = Vec::with_capacity(samples);
    let mut first_row_nanos = Vec::with_capacity(samples);
    let mut scan_nanos = Vec::with_capacity(samples);
    let mut representative = None;
    for _ in 0..samples {
        let (reader, elapsed) = fixture.open_reader(file_pool_capacity);
        open_nanos.push(elapsed);
        let started = Instant::now();
        let first = reader
            .visit_projected_range_unhydrated(
                projected_range(),
                snapshot_limits(fixture.run_count),
                &RuntimeTaskContext::default(),
                |_| false,
            )
            .expect("read first row from recovery runs");
        first_row_nanos.push(started.elapsed().as_nanos());
        assert_eq!(first.recovery.runs_read, fixture.run_count);
        assert!(first.recovery.stopped_early);

        let (reader, _) = fixture.open_reader(file_pool_capacity);
        let mut rows = 0usize;
        let started = Instant::now();
        let scan = reader
            .visit_projected_range_unhydrated(
                projected_range(),
                snapshot_limits(fixture.run_count),
                &RuntimeTaskContext::default(),
                |_| {
                    rows = rows.saturating_add(1);
                    true
                },
            )
            .expect("scan recovery runs");
        scan_nanos.push(started.elapsed().as_nanos());
        assert_eq!(rows, fixture.run_count.saturating_add(2));
        assert_eq!(scan.recovery.runs_read, fixture.run_count);
        assert_eq!(
            scan.recovery.entries_visited as usize,
            fixture.run_count.saturating_mul(2)
        );
        assert!(!scan.recovery.stopped_early);
        assert!(scan.recovery.peak_open_files <= file_pool_capacity);
        representative = Some(scan);
        black_box(rows);
    }
    let representative = representative.expect("benchmark has at least one sample");
    json!({
        "run_count": fixture.run_count,
        "file_pool_capacity": file_pool_capacity,
        "open_nanos_p50": median(&mut open_nanos),
        "first_row_nanos_p50": median(&mut first_row_nanos),
        "scan_nanos_p50": median(&mut scan_nanos),
        "peak_open_files": representative.recovery.peak_open_files,
        "range_file_opens": representative.recovery.range_file_opens,
        "range_file_pool_hits": representative.recovery.range_file_pool_hits,
        "range_file_pool_misses": representative.recovery.range_file_pool_misses,
        "overlay_peak_buffered_entries": representative.overlay_peak_buffered_entries,
        "overlay_resident_bytes": representative.overlay_resident_bytes,
    })
}

fn projected_range() -> RelationalRowPageProjectedRange<'static> {
    RelationalRowPageProjectedRange {
        table: TABLE,
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
        requested_fields: REQUESTED_FIELDS,
    }
}

fn snapshot_limits(run_count: usize) -> RelationalRowPageSnapshotReadLimits {
    let mut limits = RelationalRowPageSnapshotReadLimits::default();
    limits.demand.max_rows =
        NonZeroUsize::new(run_count.saturating_add(2)).expect("benchmark row limit is non-zero");
    limits
}

struct Fixture {
    directory: PathBuf,
    row_root: Arc<RelationalRowPageRootReader>,
    overflow_root: Arc<RelationalOverflowRootReader>,
    recovery_source: RelationalRecoverySourceIdentity,
    visible_epoch: u64,
    run_count: usize,
}

impl Fixture {
    fn new(run_count: usize) -> Self {
        let directory = unique_fixture_dir(run_count);
        let base_state = seeded_base_state();
        let overflow_config = RelationalOverflowPublicationConfig::default();
        RelationalOverflowPublisher::new(overflow_config)
            .publish(&directory, 1, 1, None, Vec::new())
            .expect("publish empty benchmark overflow root");
        let overflow_root = Arc::new(
            RelationalOverflowRootReader::open_latest(&directory, overflow_config)
                .expect("open benchmark overflow root")
                .expect("benchmark overflow root exists"),
        );
        let row_config = RelationalRowPagePublicationConfig::default();
        let deltas = base_state
            .row_page_snapshot_deltas(1, 1, row_config)
            .expect("pack benchmark base row pages");
        RelationalRowPagePublisher::new(row_config)
            .publish_with_overflow_root(&directory, 1, 1, None, deltas, &overflow_root)
            .expect("publish benchmark base row pages");
        let row_root = Arc::new(
            RelationalRowPageRootReader::open_latest(&directory, row_config)
                .expect("open benchmark row root")
                .expect("benchmark row root exists"),
        );
        let table = row_root
            .manifest()
            .tables
            .first()
            .expect("benchmark row root has one table");
        let delta_config = delta_config(32);
        let mut builder = RelationalRowDeltaBuilder::new(
            &directory,
            &row_root,
            1,
            None,
            vec![RelationalRowDeltaTableMetadata {
                table: TABLE.to_string(),
                schema_digest: table.schema_digest,
                column_count: table.column_count,
                row_count: table.row_count,
            }],
            delta_config,
        )
        .expect("create benchmark recovery delta");
        for offset in 0..run_count {
            let epoch = u64::try_from(offset.saturating_add(2)).expect("benchmark epoch fits u64");
            builder
                .record(epoch, captured_rows(offset))
                .expect("append benchmark recovery row");
        }
        let visible_epoch =
            u64::try_from(run_count.saturating_add(1)).expect("benchmark visible epoch fits u64");
        let recovery_source = recovery_source(run_count);
        let mut final_state = RelationalState::from_canonical_row_root(row_root.manifest())
            .expect("build benchmark metadata state");
        final_state
            .adopt_recovered_row_counts([(
                TABLE,
                u64::try_from(run_count.saturating_add(2)).expect("benchmark row count fits u64"),
            )])
            .expect("advance benchmark row count");
        let report = builder
            .finish_with_state(visible_epoch, recovery_source, None, &final_state)
            .expect("publish benchmark recovery delta");
        assert_eq!(report.runs, run_count);
        Self {
            directory,
            row_root,
            overflow_root,
            recovery_source,
            visible_epoch,
            run_count,
        }
    }

    fn open_reader(&self, file_pool_capacity: usize) -> (RelationalRowPageSnapshotReader, u128) {
        let started = Instant::now();
        let delta = Arc::new(
            RelationalRowDeltaReader::open_latest_with_recovery_fence(
                &self.directory,
                &self.row_root,
                RelationalRecoveryFence::new(self.visible_epoch, self.recovery_source),
                delta_config(file_pool_capacity),
            )
            .expect("open benchmark recovery delta")
            .expect("benchmark recovery delta exists"),
        );
        let view = Arc::new(
            RelationalRowPageReadView::from_recovery_delta(Arc::clone(&self.row_root), delta)
                .expect("bind benchmark recovery view"),
        );
        let reader = RelationalRowPageSnapshotReader::new(
            view,
            Arc::clone(&self.overflow_root),
            None,
            Arc::new(SegmentCache::new(CACHE_BYTES)),
            StoreId(0x5244_5255_4e53),
        )
        .expect("open benchmark snapshot reader");
        (reader, started.elapsed().as_nanos())
    }

    fn remove(self) {
        let directory = self.directory.clone();
        drop(self);
        std::fs::remove_dir_all(directory).expect("remove benchmark fixture");
    }
}

fn seeded_base_state() -> RelationalState {
    let store = RelationalStore::default();
    store
        .commit(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: TABLE.to_string(),
                        columns: vec![bigint_column("id"), text_column("body")],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: Vec::new(),
                    }),
                    RelationalWrite::Insert {
                        table: TABLE.to_string(),
                        rows: vec![row(0, 0)],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            |_, _| Ok(()),
        )
        .expect("seed benchmark base state");
    store
        .snapshot()
        .expect("read benchmark base state")
        .value()
        .clone()
}

fn captured_rows(version: usize) -> RelationalRowChangeCapture {
    let sentinel = i64::try_from(version.saturating_add(2)).expect("benchmark id fits i64");
    RelationalRowChangeCapture::Captured {
        changes: vec![
            captured_change(sentinel, version),
            captured_change(1, version),
        ],
        encoded_bytes: 0,
    }
}

fn captured_change(id: i64, version: usize) -> RelationalRowChange {
    RelationalRowChange {
        table: TABLE.to_string(),
        primary_key: hawdb_storage::RelationalKey(vec![RelationalValue::BigInt(id)]),
        row: Some(row(id, version)),
    }
}

fn row(id: i64, version: usize) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::BigInt(id),
        RelationalValue::Text(format!("body-{id:08}-v{version:08}")),
    ])
}

fn bigint_column(name: &str) -> RelationalColumnSchema {
    RelationalColumnSchema {
        name: name.to_string(),
        scalar_type: RelationalScalarType::BigInt,
        nullable: false,
        default: None,
    }
}

fn text_column(name: &str) -> RelationalColumnSchema {
    RelationalColumnSchema {
        name: name.to_string(),
        scalar_type: RelationalScalarType::Text,
        nullable: false,
        default: None,
    }
}

fn recovery_source(run_count: usize) -> RelationalRecoverySourceIdentity {
    let mut builder = RelationalRecoverySourceBuilder::new(1, 1);
    for offset in 0..run_count {
        let lsn = u64::try_from(offset.saturating_add(1)).expect("benchmark LSN fits u64");
        builder
            .record(lsn, 1, Sha256Digest::from_bytes([0x52; 32]))
            .expect("record benchmark recovery source");
    }
    builder.finish().expect("finish benchmark recovery source")
}

fn delta_config(file_pool_capacity: usize) -> RelationalRowDeltaConfig {
    RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::new(2).expect("two is non-zero"),
        max_range_open_files: NonZeroUsize::new(file_pool_capacity)
            .expect("benchmark file pool capacity is non-zero"),
        ..RelationalRowDeltaConfig::default()
    }
}

fn median(samples: &mut [u128]) -> u128 {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn unique_fixture_dir(run_count: usize) -> PathBuf {
    std::env::temp_dir().join(format!(
        "hawdb-relational-row-delta-runs-{}-{run_count}-{}",
        std::process::id(),
        FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}
