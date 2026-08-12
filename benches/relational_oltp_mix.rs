//! The TP shape of the Nowledge Mem content store, as one mixed loop.
//!
//! The columnar canonical contract promises point read/write p99 parity with
//! the current row storage and strictly better scans (§10 of
//! COLUMNAR_CANONICAL_AND_PROJECTION_SPEC.md). This benchmark records that
//! baseline on whatever engine it is compiled against: a YCSB-B-style mix
//! over the real `thread_messages` schema — primary-key point reads, ordered
//! `LIMIT` pagination, single-message insert transactions, content updates —
//! followed by the scan/aggregate class measured separately. Every mutation
//! commits its own transaction because per-request durability is the
//! workload's truth, not an artifact of the harness.

use serde_json::json;
use skein::{Database, DatabaseConfig, Value};
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
const SPACES: usize = 8;

const POINT_READ_PERMILLE: u64 = 700;
const PAGE_READ_PERMILLE: u64 = 800;
const INSERT_PERMILLE: u64 = 950;

fn main() {
    let path = std::env::temp_dir().join(format!(
        "skein-relational-oltp-mix-{}-{}",
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
                         WHERE thread_storage_id = $1 ORDER BY order_index LIMIT {PAGE_LIMIT}"
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
        Value::String(format!("{note}: {}", "message body ".repeat(12))),
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
