//! Fixed-work SQL reader/writer qualification through the embedded facade.
//! Request lifetime overlap does not establish parallel engine execution.

use serde_json::{json, Value as JsonValue};
use skein::{ConcurrentDatabase, Database, QueryOutput, Value};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const SMOKE: bool = cfg!(debug_assertions);
const THREADS: usize = if SMOKE { 4 } else { 128 };
const MESSAGES: usize = if SMOKE { 16 } else { 128 };
const CONTENT_BYTES: usize = 1024;
const POINT_REQUESTS: usize = if SMOKE { 8 } else { 512 };
const PAGE_REQUESTS: usize = if SMOKE { 4 } else { 256 };
const PAGE_ROWS: usize = if SMOKE { 16 } else { 32 };
const WRITES: usize = if SMOKE { 8 } else { 512 };
const INSERT: &str = "INSERT INTO thread_messages \
    (content_message_id, thread_storage_id, order_index, content, token_count) \
    VALUES ($1, $2, $3, $4, $5)";
const POINT: &str =
    "SELECT content_message_id, thread_storage_id, order_index, content, token_count \
    FROM thread_messages WHERE content_message_id = $1";
const PAGE: &str =
    "SELECT content_message_id, thread_storage_id, order_index, content, token_count \
    FROM thread_messages WHERE thread_storage_id = $1 \
    ORDER BY order_index, content_message_id LIMIT $2";
const COMPLETE_THREAD: &str =
    "SELECT content_message_id, thread_storage_id, order_index, content, token_count \
    FROM thread_messages WHERE thread_storage_id = $1 \
    ORDER BY order_index, content_message_id";

#[derive(Clone, Copy, Debug)]
enum ReadShape {
    Point,
    Page,
}

impl ReadShape {
    fn name(self) -> &'static str {
        match self {
            Self::Point => "point",
            Self::Page => "page",
        }
    }

    fn requests(self) -> usize {
        match self {
            Self::Point => POINT_REQUESTS,
            Self::Page => PAGE_REQUESTS,
        }
    }
}

struct WorkerMeasurement {
    kind: &'static str,
    worker: usize,
    started_nanos: u64,
    finished_nanos: u64,
    requests: Vec<RequestMeasurement>,
}

struct RequestMeasurement {
    started_nanos: u64,
    elapsed_nanos: u64,
}

impl WorkerMeasurement {
    fn report(&self) -> JsonValue {
        json!({
            "kind": self.kind,
            "worker": self.worker,
            "started_nanos": self.started_nanos,
            "finished_nanos": self.finished_nanos,
            "latency": summarize(&self.requests.iter().map(|r| r.elapsed_nanos).collect::<Vec<_>>()),
            "requests": self.requests.iter().map(|request| json!({
                "started_nanos": request.started_nanos,
                "elapsed_nanos": request.elapsed_nanos,
            })).collect::<Vec<_>>(),
        })
    }
}

fn main() {
    let root = std::env::temp_dir().join(format!(
        "skein-concurrent-snapshot-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).expect("create owned benchmark root");
    let fixture = root.join("fixture");
    create_fixture(&fixture);
    let mut reports = vec![measure_case(&root, &fixture, ReadShape::Point, 0, true)];
    for shape in [ReadShape::Point, ReadShape::Page] {
        for readers in [1, 4, 8] {
            for writer in [false, true] {
                reports.push(measure_case(&root, &fixture, shape, readers, writer));
            }
        }
    }
    println!(
        "concurrent_snapshot_reads {}",
        json!({
            "schema": "concurrent-snapshot-reads-v1",
            "smoke": SMOKE,
            "fixture_threads": THREADS,
            "messages_per_thread": MESSAGES,
            "content_bytes": CONTENT_BYTES,
            "page_rows": PAGE_ROWS,
            "durability": "SyncOnEveryWrite",
            "group_commit": "default_disabled",
            "read_warmup_requests_per_case": 2,
            "cases": reports,
        })
    );
    std::fs::remove_dir_all(&root).expect("remove owned benchmark fixtures");
}

fn create_fixture(path: &Path) {
    let mut db = Database::open(path).expect("fixture opens");
    db.query_sql(
        "CREATE TABLE thread_messages (content_message_id TEXT PRIMARY KEY, \
         thread_storage_id TEXT NOT NULL, order_index BIGINT NOT NULL, \
         content TEXT NOT NULL, token_count BIGINT NOT NULL)",
    )
    .unwrap();
    db.query_sql(
        "CREATE INDEX idx_thread_messages_order \
         ON thread_messages (thread_storage_id, order_index, content_message_id)",
    )
    .unwrap();
    for thread in 0..THREADS {
        let mut transaction = db.begin_transaction();
        for message in 0..MESSAGES {
            transaction
                .query_sql_with_params(INSERT, &message_parameters(thread, message))
                .unwrap();
        }
        transaction.commit().unwrap();
    }
    db.checkpoint().expect("fixture checkpoint succeeds");
}

fn measure_case(
    root: &Path,
    fixture: &Path,
    shape: ReadShape,
    readers: usize,
    writer: bool,
) -> JsonValue {
    let label = if readers == 0 {
        "writer-only".to_string()
    } else {
        format!("{}-{readers}-readers-writer-{writer}", shape.name())
    };
    eprintln!("concurrent-snapshot phase=prepare case={label}");
    let path = root.join(&label);
    copy_fixture(fixture, &path);
    let db = ConcurrentDatabase::open(&path).expect("case reopens checkpoint");
    // Initialize both read plans before the timed phase; retain every measured
    // request afterwards, including data-cache misses and the first write.
    let point = db
        .query_sql_with_params(POINT, &[Value::String(message_id(0, 0))])
        .unwrap();
    assert_rows(&point, 0, 0, 1);
    let page = db
        .query_sql_with_params(
            PAGE,
            &[Value::String(thread_id(0)), Value::Int(PAGE_ROWS as i64)],
        )
        .unwrap();
    assert_rows(&page, 0, 0, PAGE_ROWS);
    let original_epoch = db.commit_epoch().unwrap();
    let barrier = Arc::new(Barrier::new(readers + usize::from(writer) + 1));
    let origin = Instant::now();
    let measurements = std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for worker in 0..readers {
            let db = db.clone();
            let barrier = Arc::clone(&barrier);
            workers.push(scope.spawn(move || {
                let mut requests = Vec::with_capacity(shape.requests());
                barrier.wait();
                let started_nanos = nanos(origin.elapsed());
                for request in 0..shape.requests() {
                    let thread = (worker * 17 + request * 13) % THREADS;
                    let message = (worker * 11 + request * 7) % MESSAGES;
                    let (query, parameters) = match shape {
                        ReadShape::Point => {
                            (POINT, vec![Value::String(message_id(thread, message))])
                        }
                        ReadShape::Page => (
                            PAGE,
                            vec![
                                Value::String(thread_id(thread)),
                                Value::Int(PAGE_ROWS as i64),
                            ],
                        ),
                    };
                    let started = Instant::now();
                    let output = db.query_sql_with_params(query, &parameters).unwrap();
                    requests.push(RequestMeasurement {
                        started_nanos: nanos(started.duration_since(origin)),
                        elapsed_nanos: nanos(started.elapsed()),
                    });
                    match shape {
                        ReadShape::Point => assert_rows(&output, thread, message, 1),
                        ReadShape::Page => assert_rows(&output, thread, 0, PAGE_ROWS),
                    }
                }
                WorkerMeasurement {
                    kind: "read",
                    worker,
                    started_nanos,
                    finished_nanos: nanos(origin.elapsed()),
                    requests,
                }
            }));
        }
        if writer {
            let db = db.clone();
            let barrier = Arc::clone(&barrier);
            workers.push(scope.spawn(move || {
                let mut requests = Vec::with_capacity(WRITES);
                barrier.wait();
                let started_nanos = nanos(origin.elapsed());
                for message in 0..WRITES {
                    let parameters = message_parameters(THREADS, message);
                    let started = Instant::now();
                    db.query_sql_with_params(INSERT, &parameters).unwrap();
                    requests.push(RequestMeasurement {
                        started_nanos: nanos(started.duration_since(origin)),
                        elapsed_nanos: nanos(started.elapsed()),
                    });
                }
                WorkerMeasurement {
                    kind: "write",
                    worker: 0,
                    started_nanos,
                    finished_nanos: nanos(origin.elapsed()),
                    requests,
                }
            }));
        }
        barrier.wait();
        workers
            .into_iter()
            .map(|worker| worker.join().expect("benchmark worker succeeds"))
            .collect::<Vec<_>>()
    });
    let committed = if writer { WRITES } else { 0 };
    let final_epoch = db.commit_epoch().unwrap();
    assert_eq!(final_epoch, original_epoch + committed as u64);
    drop(db);
    eprintln!("concurrent-snapshot phase=recover case={label}");
    verify_recovery(&path, committed, final_epoch);
    std::fs::remove_dir_all(&path).expect("remove completed case fixture");
    let start = measurements.iter().map(|m| m.started_nanos).min().unwrap();
    let end = measurements.iter().map(|m| m.finished_nanos).max().unwrap();
    let elapsed = end - start;
    let samples = |kind| {
        measurements
            .iter()
            .filter(|m| m.kind == kind)
            .flat_map(|m| m.requests.iter().map(|request| request.elapsed_nanos))
            .collect::<Vec<_>>()
    };
    let reads = samples("read");
    let writes = samples("write");
    assert_eq!(reads.len(), readers * shape.requests());
    assert_eq!(writes.len(), committed);
    let writer_phase = measurements.iter().find(|worker| worker.kind == "write");
    let reads_within_writer_phase = writer_phase.map_or(0, |writer| {
        measurements
            .iter()
            .filter(|worker| worker.kind == "read")
            .flat_map(|worker| &worker.requests)
            .filter(|request| {
                request.started_nanos >= writer.started_nanos
                    && request.started_nanos + request.elapsed_nanos <= writer.finished_nanos
            })
            .count()
    });
    json!({
        "case": label,
        "read_shape": shape.name(),
        "readers": readers,
        "writers": usize::from(writer),
        "elapsed_nanos": elapsed,
        "requests_per_second": (reads.len() + writes.len()) as f64 * 1e9 / elapsed as f64,
        "read_latency": summarize(&reads),
        "write_latency": summarize(&writes),
        "read_requests_within_writer_phase": reads_within_writer_phase,
        "recovered_rows_verified": THREADS * MESSAGES + committed,
        "recovered_commit_epoch": final_epoch,
        "workers": measurements.iter().map(WorkerMeasurement::report).collect::<Vec<_>>(),
    })
}

fn verify_recovery(path: &Path, writes: usize, epoch: u64) {
    let mut db = Database::open(path).expect("complete WAL recovery succeeds");
    assert_eq!(db.commit_epoch(), epoch);
    for thread in 0..=THREADS {
        let count = if thread == THREADS { writes } else { MESSAGES };
        let output = db
            .query_sql_with_params(COMPLETE_THREAD, &[Value::String(thread_id(thread))])
            .unwrap();
        assert_rows(&output, thread, 0, count);
    }
    let total = db
        .query_sql("SELECT count(*) AS total FROM thread_messages")
        .unwrap();
    assert_eq!(
        total.rows[0].get("total"),
        Some(&Value::Int((THREADS * MESSAGES + writes) as i64))
    );
}

fn assert_rows(output: &QueryOutput, thread: usize, first: usize, count: usize) {
    assert_eq!(output.rows.len(), count);
    let expected_thread = Value::String(thread_id(thread));
    for (offset, row) in output.rows.iter().enumerate() {
        assert_eq!(
            row.get("content_message_id"),
            Some(&Value::String(message_id(thread, first + offset)))
        );
        assert!(matches!(row.get("content"), Some(Value::String(content))
            if content.len() == CONTENT_BYTES && content.bytes().all(|byte| byte == b'm')));
        assert_eq!(row.get("thread_storage_id"), Some(&expected_thread));
        assert_eq!(
            row.get("order_index"),
            Some(&Value::Int((first + offset) as i64))
        );
        assert_eq!(
            row.get("token_count"),
            Some(&Value::Int(CONTENT_BYTES as i64 / 4))
        );
    }
}

fn message_parameters(thread: usize, message: usize) -> Vec<Value> {
    vec![
        Value::String(message_id(thread, message)),
        Value::String(thread_id(thread)),
        Value::Int(message as i64),
        Value::String("m".repeat(CONTENT_BYTES)),
        Value::Int(CONTENT_BYTES as i64 / 4),
    ]
}

fn message_id(thread: usize, message: usize) -> String {
    format!("thread-{thread:04}:message-{message:04}")
}

fn thread_id(thread: usize) -> String {
    format!("thread-{thread:04}")
}

fn copy_fixture(source: &Path, destination: &Path) {
    std::fs::create_dir(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target: PathBuf = destination.join(entry.file_name());
        let kind = entry.file_type().unwrap();
        if kind.is_dir() {
            copy_fixture(&entry.path(), &target);
        } else {
            assert!(kind.is_file(), "fixture contains an unexpected symlink");
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn summarize(samples: &[u64]) -> JsonValue {
    if samples.is_empty() {
        return json!({"count": 0});
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let percentile = |percent: usize| sorted[(sorted.len() * percent).div_ceil(100) - 1];
    json!({
        "count": sorted.len(),
        "min_nanos": sorted[0],
        "p50_nanos": percentile(50),
        "p95_nanos": percentile(95),
        "p99_nanos": percentile(99),
        "max_nanos": sorted[sorted.len() - 1],
    })
}

fn nanos(duration: std::time::Duration) -> u64 {
    duration
        .as_nanos()
        .try_into()
        .expect("measurement fits u64")
}
