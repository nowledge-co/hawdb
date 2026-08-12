//! What a restart pays to rebuild the property index.
//!
//! Materialized open has to scan every canonical node regardless, because the
//! records themselves go back into memory. The question is how much of that
//! open is the index rebuild riding along, because that is the part a persisted
//! index could skip. Measured as the difference between opening the same graph
//! with and without index declarations.

use serde_json::json;
use skein::{Database, DatabaseConfig};
use std::hint::black_box;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

// Debug-assertion builds are the smoke execution CI drives through
// `cargo test --benches`; building four 60k-node stores at opt-level 0 with
// per-batch fsync dominates that job. Numbers are only meaningful from
// `cargo bench`.
const SMOKE: bool = cfg!(debug_assertions);

const NODE_COUNT: usize = if SMOKE { 2_000 } else { 60_000 };
const SAMPLES: usize = if SMOKE { 2 } else { 5 };
const BATCH: usize = 500;
const INDEXED_PROPERTIES: [&str; 4] = ["id", "space_id", "unit_type", "created_at"];

fn main() {
    let results = [0usize, 1, 2, 4]
        .into_iter()
        .map(measure)
        .collect::<Vec<_>>();
    println!(
        "index_restart_cost {}",
        json!({
            "node_count": NODE_COUNT,
            "samples": SAMPLES,
            "results": results,
        })
    );
}

fn measure(declared_indexes: usize) -> serde_json::Value {
    let path = std::env::temp_dir().join(format!(
        "skein-index-restart-{declared_indexes}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let config = DatabaseConfig {
        // Pin the residency mode so the comparison is not silently decided by
        // the artifact happening to cross the auto-materialize threshold.
        storage_residency_mode: skein_storage::StorageResidencyMode::Materialized,
        ..DatabaseConfig::default()
    };
    {
        let mut db = Database::open_with_config(&path, config.clone()).expect("database opens");
        for property in INDEXED_PROPERTIES.iter().take(declared_indexes) {
            db.query(&format!("CREATE INDEX ON :Memory({property})"))
                .expect("index declaration succeeds");
        }
        // Grouped into transactions, because one commit per node makes this a
        // WAL benchmark rather than an open benchmark.
        for batch in (0..NODE_COUNT).step_by(BATCH) {
            let mut transaction = db.begin_transaction();
            for index in batch..(batch + BATCH).min(NODE_COUNT) {
                transaction
                    .query(&format!(
                        "CREATE (:Memory {{id: 'mem-{index:08x}', space_id: 'space-{}', \
                         unit_type: 'note', created_at: {}, title: 'Memory {index}'}})",
                        index % 3,
                        1_754_000_000u64 + index as u64
                    ))
                    .expect("node creation succeeds");
            }
            transaction.commit().expect("commit succeeds");
        }
        db.checkpoint().expect("checkpoint succeeds");
    }

    let mut nanos = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let start = Instant::now();
        let db = Database::open_with_config(&path, config.clone()).expect("database reopens");
        nanos.push(start.elapsed().as_nanos() as u64);
        black_box(&db);
    }
    nanos.sort_unstable();
    let median = nanos[nanos.len() / 2];
    std::fs::remove_dir_all(&path).expect("benchmark directory must be removable");
    json!({
        "declared_indexes": declared_indexes,
        "median_open_nanos": median,
        "median_open_millis": median / 1_000_000,
    })
}
