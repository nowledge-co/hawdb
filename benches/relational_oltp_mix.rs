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

//! The TP shape of the Nowledge Mem content store, as one mixed loop.
//!
//! The row-page and demand-paged-index contract preserves point read/write
//! latency while bounding index residency and improving scans (see
//! ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md). This benchmark records that
//! baseline on whatever engine it is compiled against: a YCSB-B-style mix
//! over the real `thread_messages` schema — primary-key point reads, ordered
//! `LIMIT` pagination, single-message insert transactions, content updates —
//! followed by the scan/aggregate class measured separately. Every mutation
//! commits its own transaction because per-request durability is the
//! workload's truth, not an artifact of the harness.

use hawdb::{Database, DatabaseConfig, QueryStreamOptions, RelationalSqlReadProfile, Value};
use serde_json::json;
use std::hint::black_box;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

// Debug-assertion builds are the smoke execution CI drives through
// `cargo test --benches`; per-transaction fsync at opt-level 0 makes the full
// scale take the better part of an hour there. Numbers are only meaningful
// from `cargo bench`.
const SMOKE: bool = cfg!(debug_assertions);

const THREADS: usize = if SMOKE { 20 } else { 200 };
const MESSAGES_PER_THREAD: usize = if SMOKE { 25 } else { 250 };
const LOAD_BATCH: usize = 500;
const MIXED_OPS: usize = if SMOKE { 200 } else { 4_000 };
const PAGE_LIMIT: usize = 50;
const AGGREGATE_SAMPLES: usize = if SMOKE { 3 } else { 21 };
const BUSINESS_READ_SAMPLES: usize = if SMOKE { 3 } else { 31 };
const SPACES: usize = 8;
const MESSAGE_CONTENT_BYTES: usize = 8 * 1024;
const BUSINESS_THREAD: usize = THREADS / 2;

const POINT_READ_PERMILLE: u64 = 700;
const PAGE_READ_PERMILLE: u64 = 800;
const INSERT_PERMILLE: u64 = 950;

fn main() {
    let path = std::env::temp_dir().join(format!(
        "hawdb-relational-oltp-mix-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let config = DatabaseConfig::default();
    let mut db = Database::open_with_config(&path, config.clone()).expect("database opens");
    create_schema(&mut db);
    load(&mut db);
    db.checkpoint().expect("checkpoint succeeds");
    drop(db);
    // Measure against a reopened store so reads pay published-artifact costs
    // instead of a warm build-time cache.
    let mut db = Database::open_with_config(&path, config).expect("database reopens");
    let business_reads = benchmark_thread_read_suite(&db);

    let mut rng = Xorshift(0x243F_6A88_85A3_08D3);
    let mut point_reads = Vec::new();
    let mut page_reads = Vec::new();
    let mut inserts = Vec::new();
    let mut updates = Vec::new();
    let mut inserted = 0usize;
    for _ in 0..MIXED_OPS {
        let dice = rng.next() % 1000;
        let thread = (rng.next() as usize) % THREADS;
        let message = (rng.next() as usize) % MESSAGES_PER_THREAD;
        if dice < POINT_READ_PERMILLE {
            let start = Instant::now();
            let output = db
                .query_sql_with_params(
                    "SELECT message_id, role, content, token_count FROM thread_messages \
                     WHERE content_message_id = $1",
                    &[Value::String(message_key(thread, message))],
                )
                .expect("point read succeeds");
            point_reads.push(start.elapsed().as_nanos() as u64);
            black_box(output);
        } else if dice < PAGE_READ_PERMILLE {
            let start = Instant::now();
            let output = db
                .query_sql_with_params(
                    &format!(
                        "SELECT content_message_id, role, content FROM thread_messages \
                         WHERE thread_storage_id = $1 \
                         ORDER BY order_index, content_message_id LIMIT {PAGE_LIMIT}"
                    ),
                    &[Value::String(thread_key(thread))],
                )
                .expect("page read succeeds");
            page_reads.push(start.elapsed().as_nanos() as u64);
            black_box(output);
        } else if dice < INSERT_PERMILLE {
            let order = MESSAGES_PER_THREAD + inserted;
            inserted += 1;
            let start = Instant::now();
            let mut transaction = db.begin_transaction();
            transaction
                .query_sql_with_params(
                    "INSERT INTO thread_messages (content_message_id, message_id, \
                     thread_storage_id, thread_id, content_doc_id, space_id, order_index, \
                     role, content, token_count, content_hash, created_at, updated_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
                    &message_row(thread, order, "appended during the mixed phase"),
                )
                .expect("insert succeeds");
            transaction.commit().expect("insert commit succeeds");
            inserts.push(start.elapsed().as_nanos() as u64);
        } else {
            let start = Instant::now();
            let mut transaction = db.begin_transaction();
            transaction
                .query_sql_with_params(
                    "UPDATE thread_messages SET content = $2, updated_at = $3 \
                     WHERE content_message_id = $1",
                    &[
                        Value::String(message_key(thread, message)),
                        Value::String("rewritten during the mixed phase".repeat(4)),
                        Value::String(timestamp(order_of(thread, message))),
                    ],
                )
                .expect("update succeeds");
            transaction.commit().expect("update commit succeeds");
            updates.push(start.elapsed().as_nanos() as u64);
        }
    }

    let mut space_counts = Vec::new();
    let mut thread_rollups = Vec::new();
    for _ in 0..AGGREGATE_SAMPLES {
        let start = Instant::now();
        let output = db
            .query_sql_with_params(
                "SELECT COUNT(*) FROM thread_messages WHERE space_id = $1",
                &[Value::String("space-3".to_string())],
            )
            .expect("space count succeeds");
        space_counts.push(start.elapsed().as_nanos() as u64);
        black_box(output);

        let start = Instant::now();
        let output = db
            .query_sql(
                // The current aggregate subset rejects ORDER BY entirely, so the
                // "top threads" shape of this query (ORDER BY COUNT(*) DESC
                // LIMIT 10) cannot be expressed yet; the group-and-sum cost is
                // what this sample measures. Closing that subset gap is phase 4
                // scope.
                "SELECT thread_storage_id, COUNT(*), SUM(token_count) FROM thread_messages \
                 GROUP BY thread_storage_id",
            )
            .expect("thread rollup succeeds");
        thread_rollups.push(start.elapsed().as_nanos() as u64);
        black_box(output);
    }

    println!(
        "relational_oltp_mix {}",
        json!({
            "rows_loaded": THREADS * MESSAGES_PER_THREAD,
            "mixed_ops": MIXED_OPS,
            "thread_read_suite": business_reads,
            "point_read": summarize(&mut point_reads),
            "page_read": summarize(&mut page_reads),
            "insert": summarize(&mut inserts),
            "update": summarize(&mut updates),
            "space_count": summarize(&mut space_counts),
            "thread_rollup": summarize(&mut thread_rollups),
        })
    );
    std::fs::remove_dir_all(&path).expect("benchmark directory must be removable");
}

fn create_schema(db: &mut Database) {
    for statement in [
        "CREATE TABLE content_documents (content_doc_id TEXT PRIMARY KEY, owner_kind TEXT \
         NOT NULL, owner_id TEXT NOT NULL, space_id TEXT NOT NULL DEFAULT 'default', \
         media_type TEXT NOT NULL, blob_path TEXT, sha256 TEXT NOT NULL DEFAULT '', \
         size_bytes BIGINT NOT NULL DEFAULT 0, item_count BIGINT NOT NULL DEFAULT 0, \
         schema_version BIGINT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, \
         UNIQUE (owner_kind, owner_id))",
        "CREATE TABLE thread_messages (content_message_id TEXT PRIMARY KEY, message_id TEXT \
         NOT NULL, thread_storage_id TEXT NOT NULL, thread_id TEXT NOT NULL, content_doc_id \
         TEXT NOT NULL REFERENCES content_documents(content_doc_id), space_id TEXT NOT NULL \
         DEFAULT 'default', order_index BIGINT NOT NULL, role TEXT NOT NULL, content TEXT \
         NOT NULL, timestamp TEXT, token_count BIGINT NOT NULL DEFAULT 0, metadata_json TEXT \
         NOT NULL DEFAULT '{}', external_id TEXT NOT NULL DEFAULT '', \
         exclude_from_distillation BOOLEAN NOT NULL DEFAULT FALSE, content_hash TEXT NOT \
         NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
        "CREATE INDEX idx_thread_messages_order ON thread_messages (thread_storage_id, \
         order_index, content_message_id)",
        "CREATE INDEX idx_thread_messages_space ON thread_messages (space_id, \
         thread_storage_id)",
    ] {
        db.query_sql(statement).expect("schema statement succeeds");
    }
}

fn benchmark_thread_read_suite(db: &Database) -> serde_json::Value {
    let thread = BUSINESS_THREAD;
    let thread_id = Value::String(thread_key(thread));
    let mut lookup_samples = Vec::with_capacity(BUSINESS_READ_SAMPLES);
    let mut summary_samples = Vec::with_capacity(BUSINESS_READ_SAMPLES);
    let mut page_samples = Vec::with_capacity(BUSINESS_READ_SAMPLES);
    let mut lookup_profile = None;
    let mut summary_profile = None;
    let mut page_profile = None;
    let mut page_plan = None;

    for sample in 0..BUSINESS_READ_SAMPLES {
        let read = db.begin_read_transaction();

        let started = Instant::now();
        let lookup = read
            .query_sql_with_params_options_profiled(
                "SELECT content_doc_id FROM content_documents \
                 WHERE owner_kind = 'thread' AND owner_id = $1",
                std::slice::from_ref(&thread_id),
                QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(4 * 1024),
                },
            )
            .expect("thread exact lookup succeeds");
        lookup_samples.push(started.elapsed().as_nanos() as u64);
        assert_eq!(lookup.output.rows.len(), 1);
        assert_eq!(lookup.profile.intermediate_rows, 1);
        assert_eq!(lookup.profile.hydrated_rows, 0);

        let started = Instant::now();
        let summary = read
            .query_sql_with_params_options_profiled(
                "SELECT COUNT(*) AS message_count, \
                 COALESCE(SUM(OCTET_LENGTH(content)), 0) AS size_bytes, \
                 SUM(token_count) AS token_count \
                 FROM thread_messages WHERE thread_storage_id = $1",
                std::slice::from_ref(&thread_id),
                QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(4 * 1024),
                },
            )
            .expect("thread summary succeeds");
        summary_samples.push(started.elapsed().as_nanos() as u64);
        assert_eq!(
            summary.output.rows[0]["message_count"],
            Value::Int(MESSAGES_PER_THREAD as i64)
        );
        assert_eq!(
            summary.output.rows[0]["token_count"],
            Value::Int((MESSAGES_PER_THREAD * 48) as i64)
        );
        assert_eq!(
            summary.output.rows[0]["size_bytes"],
            Value::Int((MESSAGES_PER_THREAD * MESSAGE_CONTENT_BYTES) as i64)
        );
        assert_eq!(summary.profile.intermediate_rows, MESSAGES_PER_THREAD);
        assert_eq!(summary.profile.hydrated_rows, 0);
        assert_eq!(summary.profile.hydrated_decompressed_bytes, 0);

        let started = Instant::now();
        let page = read
            .query_sql_with_params_options_profiled(
                &format!(
                    "SELECT content_message_id, role, content FROM thread_messages \
                     WHERE thread_storage_id = $1 \
                     ORDER BY order_index, content_message_id LIMIT {PAGE_LIMIT}"
                ),
                std::slice::from_ref(&thread_id),
                QueryStreamOptions {
                    max_rows: Some(PAGE_LIMIT),
                    max_payload_bytes: Some(PAGE_LIMIT * (MESSAGE_CONTENT_BYTES + 1024)),
                },
            )
            .expect("thread ordered page succeeds");
        page_samples.push(started.elapsed().as_nanos() as u64);
        let expected_page_rows = PAGE_LIMIT.min(MESSAGES_PER_THREAD);
        assert_eq!(page.output.rows.len(), expected_page_rows);
        assert_eq!(page.profile.intermediate_rows, expected_page_rows);
        assert_eq!(page.profile.hydrated_rows, expected_page_rows);
        assert_eq!(
            page.profile.hydrated_decompressed_bytes,
            expected_page_rows * MESSAGE_CONTENT_BYTES
        );

        if sample == 0 {
            let explained = read
                .query_sql_with_params(
                    &format!(
                        "EXPLAIN SELECT content_message_id, role, content FROM thread_messages \
                         WHERE thread_storage_id = $1 \
                         ORDER BY order_index, content_message_id LIMIT {PAGE_LIMIT}"
                    ),
                    std::slice::from_ref(&thread_id),
                )
                .expect("thread ordered page explain succeeds");
            let operators = explained
                .rows
                .iter()
                .filter_map(|row| match row.get("id") {
                    Some(Value::String(id)) => Some(id.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert!(operators
                .iter()
                .all(|operator| !operator.contains("TopNExec")));
            assert!(operators
                .iter()
                .any(|operator| operator.contains("IndexRangeScanExec")));
            page_plan = Some(operators);
            lookup_profile = Some(profile_json(&lookup.profile));
            summary_profile = Some(profile_json(&summary.profile));
            page_profile = Some(profile_json(&page.profile));
        }
        black_box((lookup, summary, page));
    }

    json!({
        "samples": BUSINESS_READ_SAMPLES,
        "snapshot_scope": "one_pinned_read_transaction_per_sample",
        "exact_lookup": {
            "latency": summarize(&mut lookup_samples),
            "profile": lookup_profile.expect("lookup profile recorded"),
        },
        "count_sum_length": {
            "latency": summarize(&mut summary_samples),
            "profile": summary_profile.expect("summary profile recorded"),
        },
        "ordered_page": {
            "limit": PAGE_LIMIT,
            "content_bytes_per_row": MESSAGE_CONTENT_BYTES,
            "latency": summarize(&mut page_samples),
            "profile": page_profile.expect("page profile recorded"),
            "plan_operators": page_plan.expect("page plan recorded"),
            "top_n_eliminated": true,
        },
    })
}

fn profile_json(profile: &RelationalSqlReadProfile) -> serde_json::Value {
    json!({
        "intermediate_rows": profile.intermediate_rows,
        "hydrated_rows": profile.hydrated_rows,
        "hydrated_compressed_bytes": profile.hydrated_compressed_bytes,
        "hydrated_decompressed_bytes": profile.hydrated_decompressed_bytes,
        "index_reads": profile.index_reads.iter().map(|read| json!({
            "table": read.table,
            "index": read.index,
            "runtime_path": read.runtime_path,
            "logical_pages": read.logical_pages,
            "logical_bytes": read.logical_bytes,
            "physical_pages": read.physical_pages,
            "physical_bytes": read.physical_bytes,
            "cache_hits": read.cache_hits,
            "cache_misses": read.cache_misses,
            "cache_admission_rejections": read.cache_admission_rejections,
            "rows_visited": read.rows_visited,
        })).collect::<Vec<_>>(),
        "row_read": {
            "runtime_path": profile.row_read.runtime_path,
            "descriptor_reads": profile.row_read.descriptor_reads,
            "logical_pages": profile.row_read.logical_pages,
            "logical_bytes": profile.row_read.logical_bytes,
            "physical_pages": profile.row_read.physical_pages,
            "physical_bytes": profile.row_read.physical_bytes,
            "cache_hits": profile.row_read.cache_hits,
            "cache_misses": profile.row_read.cache_misses,
            "cache_admission_rejections": profile.row_read.cache_admission_rejections,
            "rows_visited": profile.row_read.rows_visited,
            "overlay_entries": profile.row_read.overlay_entries,
            "overlay_resident_bytes": profile.row_read.overlay_resident_bytes,
        },
    })
}

fn load(db: &mut Database) {
    for thread in 0..THREADS {
        let mut transaction = db.begin_transaction();
        transaction
            .query_sql_with_params(
                "INSERT INTO content_documents (content_doc_id, owner_kind, owner_id, \
                 space_id, media_type, schema_version, created_at, updated_at) \
                 VALUES ($1, 'thread', $2, $3, 'thread/messages', 1, $4, $4)",
                &[
                    Value::String(document_key(thread)),
                    Value::String(thread_key(thread)),
                    Value::String(space_key(thread)),
                    Value::String(timestamp(0)),
                ],
            )
            .expect("document insert succeeds");
        transaction.commit().expect("document commit succeeds");
    }
    let total = THREADS * MESSAGES_PER_THREAD;
    for batch in (0..total).step_by(LOAD_BATCH) {
        let mut transaction = db.begin_transaction();
        for index in batch..(batch + LOAD_BATCH).min(total) {
            let thread = index % THREADS;
            let order = index / THREADS;
            transaction
                .query_sql_with_params(
                    "INSERT INTO thread_messages (content_message_id, message_id, \
                     thread_storage_id, thread_id, content_doc_id, space_id, order_index, \
                     role, content, token_count, content_hash, created_at, updated_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
                    &message_row(thread, order, "loaded before the mixed phase"),
                )
                .expect("message insert succeeds");
        }
        transaction.commit().expect("message batch commit succeeds");
    }
}

fn message_row(thread: usize, order: usize, note: &str) -> Vec<Value> {
    let sequence = order_of(thread, order);
    let content = if thread == BUSINESS_THREAD {
        let mut content = format!("{note}: ");
        content.push_str(&"x".repeat(MESSAGE_CONTENT_BYTES.saturating_sub(content.len())));
        content
    } else {
        format!("{note}: {}", "message body ".repeat(12))
    };
    vec![
        Value::String(message_key(thread, order)),
        Value::String(format!("msg-{thread:04}-{order:05}")),
        Value::String(thread_key(thread)),
        Value::String(format!("thread-{thread:04}")),
        Value::String(document_key(thread)),
        Value::String(space_key(thread)),
        Value::Int(order as i64),
        Value::String(
            if order.is_multiple_of(2) {
                "user"
            } else {
                "assistant"
            }
            .to_string(),
        ),
        Value::String(content),
        Value::Int(48),
        Value::String(format!(
            "{:016x}",
            sequence.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        )),
        Value::String(timestamp(sequence)),
        Value::String(timestamp(sequence)),
    ]
}

fn order_of(thread: usize, order: usize) -> u64 {
    (thread * MESSAGES_PER_THREAD + order) as u64
}

fn message_key(thread: usize, order: usize) -> String {
    format!("cmsg-{thread:04}-{order:05}")
}

fn thread_key(thread: usize) -> String {
    format!("tstore-{thread:04}")
}

fn document_key(thread: usize) -> String {
    format!("cdoc-{thread:04}")
}

fn space_key(thread: usize) -> String {
    format!("space-{}", thread % SPACES)
}

fn timestamp(sequence: u64) -> String {
    format!("2026-08-12T00:00:00.{:06}Z", sequence % 1_000_000)
}

fn summarize(nanos: &mut [u64]) -> serde_json::Value {
    if nanos.is_empty() {
        return json!({ "ops": 0 });
    }
    nanos.sort_unstable();
    let at = |quantile: f64| nanos[((nanos.len() - 1) as f64 * quantile) as usize];
    json!({
        "ops": nanos.len(),
        "p50_micros": at(0.50) / 1_000,
        "p95_micros": at(0.95) / 1_000,
        "p99_micros": at(0.99) / 1_000,
        "max_micros": nanos[nanos.len() - 1] / 1_000,
    })
}

struct Xorshift(u64);

impl Xorshift {
    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }
}
