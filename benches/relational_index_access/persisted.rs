//! Persisted SQL access evidence for cost-model calibration, not a cost policy.

use serde_json::json;
use skein::{
    Database, DatabaseConfig, DatabaseReadTransaction, ProfiledRelationalSqlQueryOutput,
    QueryStreamOptions, RelationalOperatorKind, RelationalSqlReadProfile, Value,
};
use skein_storage::{RelationalIndexMode, StorageResidencyMode};
use std::fmt::Write as _;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const SMOKE: bool = cfg!(debug_assertions);
const ROWS: usize = if SMOKE { 256 } else { 8_192 };
const SAMPLES: usize = if SMOKE { 3 } else { 11 };
const BODY_WIDTHS: [usize; 2] = [64, 1_024];
const BATCH_ROWS: usize = 128;
const CACHE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy)]
enum Layout {
    Clustered,
    Dispersed,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Self::Clustered => "clustered",
            Self::Dispersed => "dispersed",
        }
    }

    fn position(self, ordinal: usize) -> usize {
        match self {
            Self::Clustered => ordinal,
            // An odd multiplier permutes the power-of-two fixture cardinality.
            Self::Dispersed => ordinal * 157 % ROWS,
        }
    }
}

#[derive(Clone, Copy)]
enum Shape {
    Point,
    Bucket(i64),
    All,
}

impl Shape {
    fn predicate(self, indexed: bool) -> (&'static str, i64) {
        match (self, indexed) {
            (Self::Point, true) => ("id", (ROWS / 2) as i64),
            (Self::Point, false) => ("scan_id", (ROWS / 2) as i64),
            (Self::Bucket(value), true) => ("bucket", value),
            (Self::Bucket(value), false) => ("scan_bucket", value),
            (Self::All, true) => ("all_rows", 0),
            (Self::All, false) => ("scan_all_rows", 0),
        }
    }

    fn matches(self, ordinal: usize, layout: Layout) -> bool {
        match self {
            Self::Point => ordinal == ROWS / 2,
            Self::Bucket(value) => bucket(layout.position(ordinal)) == value,
            Self::All => true,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Point => "point",
            Self::Bucket(0) => "sparse_prefix",
            Self::Bucket(1) => "medium_prefix",
            Self::Bucket(_) => "broad_prefix",
            Self::All => "all_rows_prefix",
        }
    }
}

pub(super) fn measure() -> serde_json::Value {
    let mut results = Vec::new();
    for (width, layout) in BODY_WIDTHS
        .into_iter()
        .flat_map(|width| [Layout::Clustered, Layout::Dispersed].map(|layout| (width, layout)))
    {
        let fixture = Fixture::new(width, layout);
        for shape in [
            Shape::Point,
            Shape::Bucket(0),
            Shape::Bucket(1),
            Shape::Bucket(2),
            Shape::All,
        ] {
            let expected = (0..ROWS)
                .filter(|ordinal| shape.matches(*ordinal, layout))
                .collect::<Vec<_>>();
            let scan = measure_path(&fixture.path, width, shape, false, &expected);
            let selected = measure_path(&fixture.path, width, shape, true, &expected);
            assert_eq!(
                scan["first_profile"]["row_read"]["visible_commit_epoch"],
                selected["first_profile"]["row_read"]["visible_commit_epoch"],
                "comparison paths must read the same checkpoint",
            );
            results.push(json!({
                "shape": shape.name(),
                "layout": layout.name(),
                "body_bytes": width,
                "matching_rows": expected.len(),
                "selectivity": expected.len() as f64 / ROWS as f64,
                "scan": scan,
                "cost_selected": selected,
            }));
        }
        fixture.remove();
    }
    json!({
        "protocol": "skein-persisted-relational-access-v2",
        "os": std::env::consts::OS,
        "architecture": std::env::consts::ARCH,
        "smoke": SMOKE,
        "rows": ROWS,
        "warm_samples": SAMPLES,
        "cache_capacity_bytes": CACHE_BYTES,
        "read_mode": "out_of_core_authoritative",
        "snapshot_scope": "one_pinned_snapshot_per_path",
        "first_query_scope": "new_handle_after_checkpoint_and_reopen",
        "os_page_cache": "uncontrolled_not_cold_disk",
        "result_validation": "complete_ids_and_payloads_against_fixture_oracle",
        "results": results,
    })
}

fn measure_path(
    path: &Path,
    width: usize,
    shape: Shape,
    indexed: bool,
    expected: &[usize],
) -> serde_json::Value {
    let started = Instant::now();
    let database = Database::open_with_config(
        path,
        DatabaseConfig {
            read_only: true,
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            relational_index_mode: RelationalIndexMode::Authoritative,
            segment_cache_capacity_bytes: CACHE_BYTES,
            ..DatabaseConfig::default()
        },
    )
    .expect("reopen persisted access fixture");
    let open_nanos = started.elapsed().as_nanos();
    let read = database.begin_read_transaction();
    let (column, parameter) = shape.predicate(indexed);
    let sql = format!("SELECT id, body FROM access_rows WHERE {column} = $1");
    let operator = if !indexed {
        RelationalOperatorKind::TableFullScan
    } else if matches!(shape, Shape::Point) {
        RelationalOperatorKind::TablePointGet
    } else if match shape {
        Shape::Bucket(_) => ROWS.div_ceil(3),
        _ => ROWS,
    } * 6
        + 2
        < ROWS * 3 + 4
    {
        // Independent policy oracle over fresh prefix NDV, not actual result rows.
        RelationalOperatorKind::IndexRangeScan
    } else {
        RelationalOperatorKind::TableFullScan
    };
    let (first_nanos, first) = execute(&read, &sql, parameter, width, expected, operator);
    assert!(
        first.row_read.physical_pages > 0,
        "first query did not read row files"
    );
    let mut elapsed = Vec::with_capacity(SAMPLES);
    let mut execution = Vec::with_capacity(SAMPLES);
    let mut planning = Vec::with_capacity(SAMPLES);
    let mut warm_profiles = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let (nanos, profile) = execute(&read, &sql, parameter, width, expected, operator);
        assert_eq!(profile.stage_timings.parse_nanos, 0, "warm SQL cache miss");
        assert_eq!(profile.row_read.physical_pages, 0, "warm row-file read");
        assert!(profile
            .index_reads
            .iter()
            .all(|index| index.physical_pages == 0));
        assert_eq!(
            profile.row_read.visible_commit_epoch,
            first.row_read.visible_commit_epoch
        );
        elapsed.push(nanos);
        execution.push(u128::from(profile.stage_timings.execute_nanos));
        planning.push(u128::from(profile.stage_timings.plan_nanos));
        warm_profiles.push(profile_json(&profile));
    }
    json!({
        "open_ns": open_nanos,
        "first_query_ns": first_nanos,
        "first_profile": profile_json(&first),
        "warm_query_ns": percentiles(elapsed),
        "warm_execute_ns": percentiles(execution),
        "warm_plan_ns": percentiles(planning),
        "warm_profiles": warm_profiles,
    })
}

fn execute(
    read: &DatabaseReadTransaction,
    sql: &str,
    parameter: i64,
    width: usize,
    expected: &[usize],
    operator: RelationalOperatorKind,
) -> (u128, RelationalSqlReadProfile) {
    let started = Instant::now();
    let ProfiledRelationalSqlQueryOutput { output, profile } = read
        .query_sql_with_params_options_profiled(
            sql,
            &[Value::Int(parameter)],
            QueryStreamOptions {
                max_rows: Some(ROWS),
                max_payload_bytes: Some(ROWS * (width + 128)),
            },
        )
        .expect("execute persisted access query");
    let elapsed = started.elapsed().as_nanos();

    // Validate outside the timed interval, including payloads and multiplicity.
    // Count-only agreement would miss wrong row lookups and partial results.
    let mut ids = Vec::with_capacity(output.rows.len());
    for row in &output.rows {
        assert_eq!(row.len(), 2);
        let Value::Int(id) = row["id"] else {
            panic!("persisted access result must contain an integer id");
        };
        let id = usize::try_from(id).expect("fixture id is nonnegative");
        assert_eq!(row["body"], Value::String(body(id, width)));
        ids.push(id);
    }
    ids.sort_unstable();
    assert_eq!(ids, expected, "persisted access oracle mismatch");
    assert_eq!(profile.operator_cardinality_profiles.len(), 1);
    let access = &profile.operator_cardinality_profiles[0];
    assert_eq!(access.operator, operator, "comparison access path changed");
    assert!(
        access.fully_consumed,
        "access path returned partial evidence"
    );
    let accessed_rows = if operator == RelationalOperatorKind::TableFullScan {
        ROWS
    } else {
        expected.len()
    };
    assert_eq!(access.actual_rows, Some(accessed_rows));
    assert_eq!(profile.row_read.runtime_path, "snapshot_rows");
    assert!(profile.row_read.visible_commit_epoch.is_some());
    assert!(
        profile.row_read.logical_pages > 0,
        "row pages were not read"
    );
    assert_eq!(profile.row_read.overlay_entries, 0);
    // Primary-key point reads use the canonical row-page locator directly.
    // Only secondary-index probes traverse a separate index-page tree.
    if operator != RelationalOperatorKind::IndexRangeScan {
        assert!(profile.index_reads.is_empty());
    } else {
        assert!(!access.access_path.covering);
        assert!(access.access_path.requires_row_fetch);
        assert!(!profile.index_reads.is_empty());
        for index in &profile.index_reads {
            assert_eq!(index.runtime_path, "authoritative");
            assert!(index.logical_pages > 0, "index pages were not read");
        }
    }
    black_box(output);
    (elapsed, profile)
}

fn profile_json(profile: &RelationalSqlReadProfile) -> serde_json::Value {
    let access = &profile.operator_cardinality_profiles[0];
    let row = &profile.row_read;
    json!({
        "parse_ns": profile.stage_timings.parse_nanos,
        "bind_ns": profile.stage_timings.bind_nanos,
        "plan_ns": profile.stage_timings.plan_nanos,
        "execute_ns": profile.stage_timings.execute_nanos,
        "operator": access.operator.as_str(),
        "estimated_rows": access.estimated_rows,
        "actual_rows": access.actual_rows,
        "fully_consumed": access.fully_consumed,
        "requires_row_fetch": access.access_path.requires_row_fetch,
        "intermediate_rows": profile.intermediate_rows,
        "hydrated_rows": profile.hydrated_rows,
        "hydrated_compressed_bytes": profile.hydrated_compressed_bytes,
        "hydrated_decompressed_bytes": profile.hydrated_decompressed_bytes,
        "index_reads": profile.index_reads.iter().map(|index| json!({
            "index": index.index,
            "runtime_path": index.runtime_path,
            "logical_pages": index.logical_pages,
            "logical_bytes": index.logical_bytes,
            "file_pages": index.physical_pages,
            "file_bytes": index.physical_bytes,
            "cache_hits": index.cache_hits,
            "cache_misses": index.cache_misses,
            "cache_admission_rejections": index.cache_admission_rejections,
            "rows_visited": index.rows_visited,
        })).collect::<Vec<_>>(),
        "row_read": {
            "runtime_path": row.runtime_path,
            "base_commit_epoch": row.base_commit_epoch,
            "visible_commit_epoch": row.visible_commit_epoch,
            "descriptor_reads": row.descriptor_reads,
            "logical_pages": row.logical_pages,
            "logical_bytes": row.logical_bytes,
            "file_pages": row.physical_pages,
            "file_bytes": row.physical_bytes,
            "cache_hits": row.cache_hits,
            "cache_misses": row.cache_misses,
            "cache_admission_rejections": row.cache_admission_rejections,
            "rows_visited": row.rows_visited,
            "overlay_entries": row.overlay_entries,
        },
    })
}

fn percentiles(mut samples: Vec<u128>) -> serde_json::Value {
    samples.sort_unstable();
    json!({
        "p50": super::percentile(&samples, 50),
        "p95": super::percentile(&samples, 95),
        "p99": super::percentile(&samples, 99),
    })
}

fn bucket(ordinal: usize) -> i64 {
    if ordinal < ROWS / 100 {
        0
    } else if ordinal < ROWS / 10 {
        1
    } else {
        2
    }
}

fn body(ordinal: usize, width: usize) -> String {
    let mut state = (ordinal as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    (0..width)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + (state % 26) as u8)
        })
        .collect()
}

struct Fixture {
    path: PathBuf,
}

impl Fixture {
    fn new(width: usize, layout: Layout) -> Self {
        let path = std::env::temp_dir().join(format!(
            "skein-persisted-access-{}-{}-{width}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("fixture clock")
                .as_nanos(),
            layout.name(),
        ));
        std::fs::create_dir(&path).expect("create unique access fixture directory");
        let mut database = Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: StorageResidencyMode::Materialized,
                relational_index_mode: RelationalIndexMode::Shadow,
                ..DatabaseConfig::default()
            },
        )
        .expect("create persisted access fixture");
        for statement in [
            "CREATE TABLE access_rows (id BIGINT PRIMARY KEY, scan_id BIGINT NOT NULL, \
             bucket BIGINT NOT NULL, scan_bucket BIGINT NOT NULL, \
             all_rows BIGINT NOT NULL, scan_all_rows BIGINT NOT NULL, body TEXT NOT NULL)",
            "CREATE INDEX access_rows_bucket ON access_rows (bucket)",
            "CREATE INDEX access_rows_all ON access_rows (all_rows)",
        ] {
            database.query_sql(statement).expect("create access schema");
        }
        for start in (0..ROWS).step_by(BATCH_ROWS) {
            let mut sql = String::from(
                "INSERT INTO access_rows \
                 (id, scan_id, bucket, scan_bucket, all_rows, scan_all_rows, body) VALUES ",
            );
            let mut parameters = Vec::new();
            for ordinal in start..ROWS.min(start + BATCH_ROWS) {
                if ordinal != start {
                    sql.push_str(", ");
                }
                let offset = parameters.len();
                write!(
                    sql,
                    "(${}, ${}, ${}, ${}, 0, 0, ${})",
                    offset + 1,
                    offset + 1,
                    offset + 2,
                    offset + 2,
                    offset + 3,
                )
                .expect("write fixture SQL");
                parameters.extend([
                    Value::Int(ordinal as i64),
                    Value::Int(bucket(layout.position(ordinal))),
                    Value::String(body(ordinal, width)),
                ]);
            }
            database
                .query_sql_with_params(&sql, &parameters)
                .expect("insert access fixture batch");
        }
        database.checkpoint().expect("checkpoint access fixture");
        drop(database);
        Self { path }
    }

    fn remove(self) {
        std::fs::remove_dir_all(self.path).expect("remove closed access fixture");
    }
}
