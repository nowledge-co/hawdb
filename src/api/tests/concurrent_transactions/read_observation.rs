use super::release_autocommit_reads;
use crate::telemetry::{QueryTelemetry, TelemetrySink};
use crate::{ConcurrentDatabase, Database, DatabaseConfig, QueryOutput, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;

#[derive(Debug)]
struct PausedWriterTelemetry {
    writer_kind: &'static str,
    writer_started: mpsc::Sender<()>,
    writer_release: Arc<(Mutex<bool>, Condvar)>,
    writer_timed_out: AtomicBool,
    read_digest: String,
    reads: Mutex<Vec<(bool, usize)>>,
}

impl TelemetrySink for PausedWriterTelemetry {
    fn record_query(&self, event: QueryTelemetry<'_>) {
        if event.query_digest == self.read_digest {
            self.reads
                .lock()
                .unwrap()
                .push((event.success, event.row_count));
        }
        if event.statement_kind == self.writer_kind {
            let _ = self.writer_started.send(());
            let (released, available) = &*self.writer_release;
            let (_released, timeout) = available
                .wait_timeout_while(
                    released.lock().unwrap(),
                    Duration::from_secs(30),
                    |released| !*released,
                )
                .unwrap();
            self.writer_timed_out
                .store(timeout.timed_out(), Ordering::SeqCst);
        }
    }
}

#[derive(Clone, Copy)]
enum ReadLanguage {
    Cypher,
    Sql,
}

impl ReadLanguage {
    fn name(self) -> &'static str {
        match self {
            Self::Cypher => "cypher",
            Self::Sql => "sql",
        }
    }

    fn execute(self, db: &ConcurrentDatabase, query: &str) -> crate::Result<QueryOutput> {
        match self {
            Self::Cypher => db.query(query),
            Self::Sql => db.query_sql(query),
        }
    }

    fn count_query(self) -> &'static str {
        match self {
            Self::Cypher => "MATCH (m:Memory) RETURN count(m) AS total",
            Self::Sql => "SELECT count(*) AS total FROM messages",
        }
    }
}

fn assert_read_completion_does_not_wait_for_writer(language: ReadLanguage, fails: bool) {
    let mut database = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: fails.then_some(1),
        slow_query_log_threshold_micros: 0,
        ..DatabaseConfig::default()
    });
    database.query("CREATE (:Memory {id: 1})").unwrap();
    database.query("CREATE (:Memory {id: 2})").unwrap();
    database
        .query_sql("CREATE TABLE messages (id BIGINT PRIMARY KEY)")
        .unwrap();
    database
        .query_sql("INSERT INTO messages (id) VALUES (1), (2)")
        .unwrap();
    let read_query = match (language, fails) {
        (ReadLanguage::Cypher, true) => "MATCH (m:Memory) RETURN m.id AS id",
        (ReadLanguage::Sql, true) => "SELECT id FROM messages",
        (_, false) => language.count_query(),
    };
    let (writer_kind, write_query) = match language {
        ReadLanguage::Cypher => ("create_node", "CREATE (:Memory {id: 3})"),
        ReadLanguage::Sql => ("insert", "INSERT INTO messages (id) VALUES (3)"),
    };
    let read_digest = skein_query::QueryIdentity::new(language.name(), read_query)
        .query_digest()
        .to_string();
    let (writer_started, writer_events) = mpsc::channel();
    let writer_release = Arc::new((Mutex::new(false), Condvar::new()));
    let sink = Arc::new(PausedWriterTelemetry {
        writer_kind,
        writer_started,
        writer_release: Arc::clone(&writer_release),
        writer_timed_out: AtomicBool::new(false),
        read_digest: read_digest.clone(),
        reads: Mutex::new(Vec::new()),
    });
    database.set_telemetry_sink(Some(sink.clone()));
    let db = database.into_concurrent();
    let (snapshot_acquired, snapshots) = mpsc::channel();
    let reader_release = Arc::new((Mutex::new(false), Condvar::new()));
    db.set_autocommit_read_gate(snapshot_acquired, Arc::clone(&reader_release))
        .unwrap();

    let (read_completed, read_results) = mpsc::channel();
    let reader_db = db.clone();
    let reader = std::thread::spawn(move || {
        let _ = read_completed.send(language.execute(&reader_db, read_query));
    });
    let snapshot_ready = snapshots.recv_timeout(Duration::from_secs(5));
    let writer_db = db.clone();
    let writer = std::thread::spawn(move || language.execute(&writer_db, write_query));
    // Pause the real public mutation inside its telemetry callback, while it
    // owns the commit sequencer. The read already owns an older pinned snapshot.
    let writer_ready = writer_events.recv_timeout(Duration::from_secs(5));
    let gate_cleared = db.clear_autocommit_read_gate();
    release_autocommit_reads(&reader_release);
    let completed_while_writer_paused = read_results.recv_timeout(Duration::from_secs(5));
    let recorded_while_writer_paused = sink.reads.lock().unwrap().clone();

    // Release every controlled wait before asserting or joining either worker.
    release_autocommit_reads(&writer_release);
    let reader_joined = reader.join();
    let writer_result = writer.join();
    snapshot_ready.expect("read snapshot must precede the writer");
    writer_ready.expect("the public writer must reach its recording callback");
    gate_cleared.unwrap();
    reader_joined.unwrap();
    writer_result.unwrap().unwrap();
    assert!(!sink.writer_timed_out.load(Ordering::SeqCst));
    let result = completed_while_writer_paused
        .expect("snapshot read completion waited for the active writer's commit mutex");
    assert_eq!(
        recorded_while_writer_paused,
        vec![(!fails, usize::from(!fails))]
    );
    if fails {
        let error = result.unwrap_err().to_string();
        let expected = match language {
            ReadLanguage::Cypher => "exceeding max_read_result_rows 1",
            ReadLanguage::Sql => "relational SQL exceeds max_intermediate_rows 1",
        };
        assert!(error.contains(expected), "{error}");
    } else {
        let output = result.unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
    }

    let summaries = db
        .query_sql_with_params(
            "SELECT execution_count, success_count, error_count FROM system.statement_summary \
             WHERE digest = $1",
            &[Value::String(read_digest.clone())],
        )
        .unwrap();
    assert_eq!(summaries.rows.len(), 1);
    assert_eq!(
        summaries.rows[0].get("execution_count"),
        Some(&Value::Int(1))
    );
    assert_eq!(
        summaries.rows[0].get("success_count"),
        Some(&Value::Int(i64::from(!fails)))
    );
    assert_eq!(
        summaries.rows[0].get("error_count"),
        Some(&Value::Int(i64::from(fails)))
    );
    let slow_queries = db
        .query_sql_with_params(
            "SELECT row_count FROM system.slow_queries WHERE query_digest = $1",
            &[Value::String(read_digest)],
        )
        .unwrap();
    assert_eq!(slow_queries.rows.len(), usize::from(!fails));
    if !fails {
        assert_eq!(slow_queries.rows[0].get("row_count"), Some(&Value::Int(1)));
    }
    let current_query = match language {
        ReadLanguage::Cypher => "MATCH (m:Memory {id: 3}) RETURN m.id AS id",
        ReadLanguage::Sql => "SELECT id FROM messages WHERE id = 3",
    };
    let current = language.execute(&db, current_query).unwrap();
    assert_eq!(current.rows.len(), 1);
    assert_eq!(current.rows[0].get("id"), Some(&Value::Int(3)));
}

#[test]
fn successful_cypher_snapshot_read_returns_while_writer_is_active() {
    assert_read_completion_does_not_wait_for_writer(ReadLanguage::Cypher, false);
}

#[test]
fn failed_cypher_snapshot_read_returns_while_writer_is_active() {
    assert_read_completion_does_not_wait_for_writer(ReadLanguage::Cypher, true);
}

#[test]
fn successful_sql_snapshot_read_returns_while_writer_is_active() {
    assert_read_completion_does_not_wait_for_writer(ReadLanguage::Sql, false);
}

#[test]
fn failed_sql_snapshot_read_returns_while_writer_is_active() {
    assert_read_completion_does_not_wait_for_writer(ReadLanguage::Sql, true);
}
