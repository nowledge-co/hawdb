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

use hawdb_storage::{
    ImmutableIndexPageLimits, RelationalColumnSchema, RelationalIndexReadLimits,
    RelationalIndexSchema, RelationalIndexShadowConfig, RelationalIndexShadowReader,
    RelationalIndexShadowWriter, RelationalInsertMode, RelationalKey, RelationalMutationLimits,
    RelationalOverflowConfig, RelationalRow, RelationalScalarType, RelationalState,
    RelationalTableSchema, RelationalTransaction, RelationalValue, RelationalWrite, SegmentCache,
    StoreId,
};
use serde_json::json;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const SMOKE: bool = cfg!(debug_assertions);
const ROWS: usize = if SMOKE { 2_000 } else { 50_000 };
const SAMPLES: usize = if SMOKE { 3 } else { 31 };
const OWNER_COUNT: usize = 1_024;
const TARGET_OWNER: usize = 37;
const PAGE_BYTES: usize = 16 * 1024;
const CACHE_BYTES: u64 = 8 * 1024 * 1024;

fn main() {
    let path = std::env::temp_dir().join(format!(
        "hawdb-relational-index-page-cache-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let state = seeded_state();
    let config = RelationalIndexShadowConfig {
        page_limits: ImmutableIndexPageLimits {
            max_page_bytes: NonZeroUsize::new(PAGE_BYTES).expect("page size is non-zero"),
            ..ImmutableIndexPageLimits::default()
        },
        ..RelationalIndexShadowConfig::default()
    };
    let publication = RelationalIndexShadowWriter::new(config)
        .publish(&path, &state, 1, 1, None)
        .expect("publish benchmark index pages");
    drop(state);

    let cache = Arc::new(SegmentCache::new(CACHE_BYTES));
    let reader = RelationalIndexShadowReader::open_latest_with_cache(
        &path,
        config,
        Arc::clone(&cache),
        StoreId(1),
    )
    .expect("open benchmark index reader");
    let open_snapshot = cache.snapshot();
    assert_eq!(open_snapshot.resident_bytes, 0);
    let key = RelationalKey(vec![RelationalValue::Text(format!(
        "owner-{TARGET_OWNER:04}"
    ))]);

    let cold_started = Instant::now();
    let (cold_rows, cold_report) = lookup(&reader, &key);
    let cold_nanos = cold_started.elapsed().as_nanos();
    let expected_rows = (TARGET_OWNER..ROWS).step_by(OWNER_COUNT).count();
    assert_eq!(cold_rows, expected_rows);

    let mut warm_nanos = Vec::with_capacity(SAMPLES);
    let mut warm_report = None;
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let (rows, report) = lookup(&reader, &key);
        warm_nanos.push(started.elapsed().as_nanos());
        assert_eq!(rows, expected_rows);
        warm_report = Some(report);
    }
    warm_nanos.sort_unstable();
    let warm_report = warm_report.expect("at least one warm sample");
    let cache_snapshot = cache.snapshot();
    assert_eq!(cache_snapshot.pinned_bytes, 0);
    assert!(cache_snapshot.resident_bytes <= cache_snapshot.capacity_bytes);
    assert_eq!(warm_report.file_pages_read, 0);

    println!(
        "relational_index_page_cache {}",
        json!({
            "rows": ROWS,
            "samples": SAMPLES,
            "matched_rows": expected_rows,
            "page_bytes": PAGE_BYTES,
            "cache_capacity_bytes": CACHE_BYTES,
            "published_pages": publication.pages_written,
            "build_sort_spill_runs": publication.sort_spill_run_count,
            "build_sort_spill_bytes": publication.sort_spill_bytes,
            "build_peak_sort_memory_bytes": publication.peak_sort_memory_bytes,
            "open_resident_bytes": open_snapshot.resident_bytes,
            "cold_nanos": cold_nanos,
            "cold_pages": cold_report.pages_read,
            "cold_file_pages": cold_report.file_pages_read,
            "cold_cache_misses": cold_report.cache_misses,
            "warm_nanos_p50": percentile(&warm_nanos, 50),
            "warm_nanos_p95": percentile(&warm_nanos, 95),
            "warm_nanos_p99": percentile(&warm_nanos, 99),
            "warm_file_pages": warm_report.file_pages_read,
            "warm_cache_hits": warm_report.cache_hits,
            "resident_bytes": cache_snapshot.resident_bytes,
            "pinned_bytes": cache_snapshot.pinned_bytes,
            "evictions": cache_snapshot.eviction_count,
        })
    );

    std::fs::remove_dir_all(path).expect("remove page-cache benchmark fixture");
}

fn lookup(
    reader: &RelationalIndexShadowReader,
    key: &RelationalKey,
) -> (usize, hawdb_storage::RelationalIndexReadReport) {
    let mut rows = 0usize;
    let report = reader
        .visit_exact_postings(
            "messages",
            "messages_owner_idx",
            key,
            RelationalIndexReadLimits::default(),
            |primary_key| {
                black_box(primary_key);
                rows = rows.saturating_add(1);
                true
            },
        )
        .expect("benchmark index lookup");
    black_box(rows);
    (rows, report)
}

fn seeded_state() -> RelationalState {
    RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "messages".to_string(),
                        columns: vec![text_column("id"), text_column("owner")],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: vec![RelationalIndexSchema {
                            name: "messages_owner_idx".to_string(),
                            columns: vec!["owner".to_string()],
                            unique: false,
                        }],
                    }),
                    RelationalWrite::Insert {
                        table: "messages".to_string(),
                        rows: (0..ROWS)
                            .map(|ordinal| {
                                RelationalRow::new(vec![
                                    RelationalValue::Text(format!("message-{ordinal:08}")),
                                    RelationalValue::Text(format!(
                                        "owner-{:04}",
                                        ordinal % OWNER_COUNT
                                    )),
                                ])
                            })
                            .collect(),
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("build benchmark relational state")
}

fn text_column(name: &str) -> RelationalColumnSchema {
    RelationalColumnSchema {
        name: name.to_string(),
        scalar_type: RelationalScalarType::Text,
        nullable: false,
        default: None,
    }
}

fn percentile(samples: &[u128], percentile: usize) -> u128 {
    let rank = samples
        .len()
        .saturating_mul(percentile.min(100))
        .saturating_add(99)
        / 100;
    samples
        .get(rank.saturating_sub(1))
        .copied()
        .unwrap_or_default()
}
