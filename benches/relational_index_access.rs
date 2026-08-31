use serde_json::json;
use skein_optimizer::{
    select_relational_access_path, RelationalAccessPathDescriptor, RelationalAccessPathKind,
};
use skein_qos::ProcessMemorySnapshot;
use skein_storage::{
    RelationalColumnSchema, RelationalIndexRangeScan, RelationalIndexScanDirection,
    RelationalIndexSchema, RelationalInsertMode, RelationalKey, RelationalRow,
    RelationalScalarType, RelationalStore, RelationalTableSchema, RelationalTransaction,
    RelationalValue, RelationalWrite,
};
use std::collections::BTreeSet;
use std::hint::black_box;
use std::time::Instant;

const DATASET_ROWS: [usize; 4] = [1_000, 10_000, 50_000, 100_000];
const SAMPLES: usize = 31;
const KEYSET_PAGE_ROWS: usize = 25;
const TARGET_SPACE: &str = "space-target";
const TARGET_THREAD: &str = "thread-target";
const DISTRACTOR_THREAD: &str = "thread-distractor";

fn main() {
    let results = DATASET_ROWS.into_iter().map(measure).collect::<Vec<_>>();
    println!(
        "relational_index_access {}",
        json!({
            "samples": SAMPLES,
            "results": results,
        })
    );
}

fn measure(target_prefix_rows: usize) -> serde_json::Value {
    let memory_before = ProcessMemorySnapshot::capture().ok();
    let store = seeded_store(target_prefix_rows);
    let memory_after = ProcessMemorySnapshot::capture().ok();
    let snapshot = store.snapshot().expect("benchmark snapshot");
    let state = snapshot.value();
    let space = text(TARGET_SPACE);
    let thread = text(TARGET_THREAD);
    let space_prefix = RelationalKey(vec![space.clone()]);
    let composite_prefix = RelationalKey(vec![space.clone(), thread.clone()]);
    let space_rows = state
        .index_prefix_cardinality("messages", "idx_messages_space", &space_prefix)
        .expect("space index cardinality");
    let matched_rows = state
        .index_prefix_cardinality(
            "messages",
            "idx_messages_space_thread_order",
            &composite_prefix,
        )
        .expect("composite index cardinality");
    let selected = select_relational_access_path([
        access_path(
            RelationalAccessPathKind::FullScan,
            "__full_scan",
            &[],
            0,
            false,
            false,
            target_prefix_rows.saturating_mul(2),
        ),
        access_path(
            RelationalAccessPathKind::Index,
            "idx_messages_space",
            &["space_id"],
            1,
            false,
            true,
            space_rows,
        ),
        access_path(
            RelationalAccessPathKind::Index,
            "idx_messages_space_thread_order",
            &["space_id", "thread_id", "order_index", "id"],
            2,
            false,
            true,
            matched_rows,
        ),
    ])
    .expect("valid benchmark access paths")
    .expect("benchmark has access paths");

    let mut full_scan_samples = Vec::with_capacity(SAMPLES);
    let mut streaming_prefix_samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let full_scan_matches = state
            .rows("messages")
            .filter(|(_, row)| row.values()[1] == space && row.values()[2] == thread)
            .count();
        full_scan_samples.push(started.elapsed().as_nanos());
        assert_eq!(full_scan_matches, matched_rows);
        black_box(full_scan_matches);

        let started = Instant::now();
        let mut indexed_matches = 0usize;
        state
            .visit_index_prefix_rows(
                "messages",
                "idx_messages_space_thread_order",
                &composite_prefix,
                |primary_key, row| {
                    black_box((primary_key, row));
                    indexed_matches = indexed_matches.saturating_add(1);
                    true
                },
            )
            .expect("streaming composite prefix lookup");
        streaming_prefix_samples.push(started.elapsed().as_nanos());
        assert_eq!(indexed_matches, matched_rows);
        black_box(indexed_matches);
    }
    full_scan_samples.sort_unstable();
    streaming_prefix_samples.sort_unstable();

    let cursor_positions = [
        ("first", KEYSET_PAGE_ROWS),
        ("middle", target_prefix_rows / 2),
        (
            "deep",
            target_prefix_rows
                .saturating_sub(KEYSET_PAGE_ROWS)
                .saturating_sub(1),
        ),
    ];
    let mut keyset_results = Vec::with_capacity(cursor_positions.len());
    for (position, cursor_order) in cursor_positions {
        let cursor = RelationalKey(vec![
            space.clone(),
            thread.clone(),
            RelationalValue::BigInt(i64::try_from(cursor_order).expect("cursor order fits i64")),
            text(&format!("message-target-{cursor_order:08}")),
        ]);
        let mut forward_samples = Vec::with_capacity(SAMPLES);
        let mut backward_samples = Vec::with_capacity(SAMPLES);
        let expected_forward_rows = KEYSET_PAGE_ROWS.min(
            target_prefix_rows
                .saturating_sub(cursor_order)
                .saturating_sub(1),
        );
        let expected_backward_rows = KEYSET_PAGE_ROWS.min(cursor_order);

        for (direction, samples) in [
            (RelationalIndexScanDirection::Forward, &mut forward_samples),
            (
                RelationalIndexScanDirection::Backward,
                &mut backward_samples,
            ),
        ] {
            for _ in 0..SAMPLES {
                let started = Instant::now();
                let mut visited_rows = 0usize;
                state
                    .visit_index_range_entries(
                        "messages",
                        "idx_messages_space_thread_order",
                        &RelationalIndexRangeScan {
                            prefix: composite_prefix.clone(),
                            exclusive_bound: Some(cursor.clone()),
                            direction,
                        },
                        |index_key, primary_key| {
                            black_box((index_key, primary_key));
                            visited_rows = visited_rows.saturating_add(1);
                            visited_rows < KEYSET_PAGE_ROWS
                        },
                    )
                    .expect("exclusive keyset index lookup");
                samples.push(started.elapsed().as_nanos());
                let expected_rows = match direction {
                    RelationalIndexScanDirection::Forward => expected_forward_rows,
                    RelationalIndexScanDirection::Backward => expected_backward_rows,
                };
                assert_eq!(visited_rows, expected_rows);
                assert!(visited_rows <= KEYSET_PAGE_ROWS);
                black_box(visited_rows);
            }
        }
        forward_samples.sort_unstable();
        backward_samples.sort_unstable();
        keyset_results.push(json!({
            "position": position,
            "cursor_order": cursor_order,
            "forward_rows_visited": expected_forward_rows,
            "backward_rows_visited": expected_backward_rows,
            "exclusive_forward_ns_p50": percentile(&forward_samples, 50),
            "exclusive_forward_ns_p95": percentile(&forward_samples, 95),
            "exclusive_forward_ns_p99": percentile(&forward_samples, 99),
            "exclusive_backward_ns_p50": percentile(&backward_samples, 50),
            "exclusive_backward_ns_p95": percentile(&backward_samples, 95),
            "exclusive_backward_ns_p99": percentile(&backward_samples, 99),
        }));
    }
    let full_scan_p50 = percentile(&full_scan_samples, 50);
    let streaming_prefix_p50 = percentile(&streaming_prefix_samples, 50);

    json!({
        "target_prefix_rows": target_prefix_rows,
        "total_rows": target_prefix_rows.saturating_mul(2),
        "space_prefix_rows": space_rows,
        "matched_rows": matched_rows,
        "selected_path": selected.name,
        "selected_equality_prefix_len": selected.equality_prefix_len,
        "full_scan_ns_p50": full_scan_p50,
        "full_scan_ns_p95": percentile(&full_scan_samples, 95),
        "full_scan_ns_p99": percentile(&full_scan_samples, 99),
        "streaming_composite_prefix_ns_p50": streaming_prefix_p50,
        "streaming_composite_prefix_ns_p95": percentile(&streaming_prefix_samples, 95),
        "streaming_composite_prefix_ns_p99": percentile(&streaming_prefix_samples, 99),
        "keyset_page_rows": KEYSET_PAGE_ROWS,
        "keyset_cursors": keyset_results,
        "p50_speedup": full_scan_p50 as f64 / streaming_prefix_p50.max(1) as f64,
        "resident_delta_bytes": memory_before.zip(memory_after).map(|(before, after)| {
            after.resident_bytes.saturating_sub(before.resident_bytes)
        }),
    })
}

fn seeded_store(target_prefix_rows: usize) -> RelationalStore {
    let store = RelationalStore::default();
    store
        .commit(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "messages".to_string(),
                        columns: vec![
                            text_column("id"),
                            text_column("space_id"),
                            text_column("thread_id"),
                            bigint_column("order_index"),
                        ],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: Vec::new(),
                    }),
                    RelationalWrite::CreateIndex {
                        table: "messages".to_string(),
                        index: RelationalIndexSchema {
                            name: "idx_messages_space".to_string(),
                            columns: vec!["space_id".to_string()],
                            unique: false,
                        },
                    },
                    RelationalWrite::CreateIndex {
                        table: "messages".to_string(),
                        index: RelationalIndexSchema {
                            name: "idx_messages_space_thread_order".to_string(),
                            columns: vec![
                                "space_id".to_string(),
                                "thread_id".to_string(),
                                "order_index".to_string(),
                                "id".to_string(),
                            ],
                            unique: false,
                        },
                    },
                ],
            },
            |_, _| Ok(()),
        )
        .expect("benchmark schema");
    for (thread, id_prefix) in [(TARGET_THREAD, "target"), (DISTRACTOR_THREAD, "distractor")] {
        let rows = (0..target_prefix_rows)
            .map(|order| {
                RelationalRow::new(vec![
                    text(&format!("message-{id_prefix}-{order:08}")),
                    text(TARGET_SPACE),
                    text(thread),
                    RelationalValue::BigInt(
                        i64::try_from(order).expect("benchmark order fits i64"),
                    ),
                ])
            })
            .collect::<Vec<_>>();
        store
            .commit(
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "messages".to_string(),
                        rows,
                        mode: RelationalInsertMode::Error,
                    }],
                },
                |_, _| Ok(()),
            )
            .expect("benchmark rows");
    }
    assert_eq!(
        store
            .snapshot()
            .expect("seeded snapshot")
            .value()
            .row_count("messages"),
        target_prefix_rows.saturating_mul(2)
    );
    store
}

fn access_path(
    kind: RelationalAccessPathKind,
    name: &str,
    columns: &[&str],
    equality_prefix_len: usize,
    unique_point: bool,
    requires_row_fetch: bool,
    estimated_rows: usize,
) -> RelationalAccessPathDescriptor {
    RelationalAccessPathDescriptor {
        kind,
        name: name.to_string(),
        index_columns: columns.iter().map(|column| (*column).to_string()).collect(),
        access_columns: columns[..equality_prefix_len]
            .iter()
            .map(|column| (*column).to_string())
            .collect::<BTreeSet<_>>(),
        equality_prefix_len,
        order_prefix_len: 0,
        exclusive_range: false,
        reverse_order: false,
        unique_point,
        covering: false,
        requires_row_fetch,
        estimated_rows,
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

fn bigint_column(name: &str) -> RelationalColumnSchema {
    RelationalColumnSchema {
        name: name.to_string(),
        scalar_type: RelationalScalarType::BigInt,
        nullable: false,
        default: None,
    }
}

fn text(value: &str) -> RelationalValue {
    RelationalValue::Text(value.to_string())
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
