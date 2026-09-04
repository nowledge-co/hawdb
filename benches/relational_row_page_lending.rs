//! Allocation and latency evidence for the relational row-page lending path.
//!
//! The owned path is the compatibility cursor: it materializes every projected
//! row before the predicate sees it. The lending path evaluates the same
//! predicate and limit against page-backed values, then owns only selected
//! output. Both paths use the same snapshot reader, cache, fields, row order,
//! and checksums.

use serde_json::json;
use skein::{Database, DatabaseConfig, Value};
use skein_core::RuntimeTaskContext;
use skein_storage::{
    RelationalHydrationBudget, RelationalOverflowPublicationConfig, RelationalOverflowRootReader,
    RelationalProjectedRow, RelationalRowPageProjectedFields, RelationalRowPageProjectedRange,
    RelationalRowPageProjectedRangeFields, RelationalRowPagePublicationConfig,
    RelationalRowPageReadView, RelationalRowPageRootReader, RelationalRowPageSnapshotReadLimits,
    RelationalRowPageSnapshotReader, RelationalValue, RelationalValueRef, SegmentCache, StoreId,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::fmt::Write as _;
use std::hint::black_box;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const SMOKE: bool = cfg!(debug_assertions);
const ROWS: usize = if SMOKE { 4_096 } else { 32_768 };
const BODY_BYTES: usize = 1_024;
const INSERT_BATCH_ROWS: usize = 256;
const INSERT_TRANSACTION_ROWS: usize = 4_096;
const OUTPUT_LIMIT: usize = 32;
const SAMPLES: usize = if SMOKE { 3 } else { 11 };
const POINT_PROBES: usize = if SMOKE { 128 } else { 2_048 };
const CACHE_BYTES: u64 = 64 * 1024 * 1024;
const REQUIRED_SPEEDUP: f64 = 1.15;
const REQUIRED_ALLOCATION_REDUCTION: f64 = 0.50;
const POINT_MAX_RELATIVE_REGRESSION: f64 = 0.05;
const POINT_ABSOLUTE_NOISE_BUDGET_NANOS: u128 = 100_000;
const POINT_LATENCY_GATE_REQUIRED: bool = !SMOKE;
const REQUESTED_FIELDS: &[usize] = &[0, 1, 2];
const POINT_FIELDS: &[usize] = &[0, 2];
const SHAPES: [usize; 4] = [1, 10, 50, 100];

static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: this allocator delegates every operation to the system
        // allocator with the exact layout supplied by the caller.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            add_allocated_bytes(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: see `alloc`; the matching system operation is used here.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            add_allocated_bytes(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: `pointer` and `layout` came from this allocator, which
        // delegates allocation to the same system allocator.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: `pointer` and `layout` came from the system allocator and the
        // requested replacement size is forwarded unchanged.
        let replacement = unsafe { System.realloc(pointer, layout, new_size) };
        if !replacement.is_null() {
            add_allocated_bytes(new_size);
        }
        replacement
    }
}

fn main() {
    let fixture = Fixture::new();
    let point_cache_evidence = measure_point_cache(&fixture);
    let scan_evidence = SHAPES
        .into_iter()
        .map(|selectivity_percent| measure_scan(&fixture.reader, selectivity_percent))
        .collect::<Vec<_>>();
    let point_evidence = measure_point_lookup(&fixture.reader);
    let selective_gate_admitted = scan_evidence
        .iter()
        .filter(|evidence| evidence.selectivity_percent < 100)
        .all(|evidence| evidence.admitted);
    let admitted =
        point_cache_evidence.admitted && selective_gate_admitted && point_evidence.admitted;

    println!(
        "relational_row_page_lending {}",
        json!({
            "protocol": "skein-relational-row-page-lending-evidence-v1",
            "rows": ROWS,
            "body_bytes": BODY_BYTES,
            "output_limit": OUTPUT_LIMIT,
            "samples": SAMPLES,
            "cache_bytes": CACHE_BYTES,
            "required_speedup": REQUIRED_SPEEDUP,
            "required_allocation_reduction": REQUIRED_ALLOCATION_REDUCTION,
            "scan_shapes": scan_evidence.iter().map(ScanEvidence::json).collect::<Vec<_>>(),
            "point_lookup": point_evidence.json(),
            "point_cache": point_cache_evidence.json(),
            "selective_gate_admitted": selective_gate_admitted,
            "evidence_admitted": admitted,
        })
    );
    assert!(admitted, "relational row-page lending evidence rejected");
    fixture.remove();
}

fn measure_point_cache(fixture: &Fixture) -> PointCacheEvidence {
    let (reader, cache) = fixture.fresh_reader();
    let key = skein_storage::RelationalKey(vec![RelationalValue::Text(row_id(0))]);
    let cold = run_point_cache_probe(&reader, &key);
    let warm = run_point_cache_probe(&reader, &key);
    let resident_bytes = cache.snapshot().resident_bytes;
    let admitted = cold.cache_hits == 0
        && cold.cache_misses == 1
        && cold.file_pages_read == 1
        && warm.cache_hits == 1
        && warm.cache_misses == 0
        && warm.file_pages_read == 0
        && resident_bytes > 0;
    PointCacheEvidence {
        cold,
        warm,
        resident_bytes,
        admitted,
    }
}

fn run_point_cache_probe(
    reader: &RelationalRowPageSnapshotReader,
    key: &skein_storage::RelationalKey,
) -> PointCacheProbe {
    let mut hydration = RelationalHydrationBudget::default();
    let (row, report) = reader
        .point_projected_fields(
            "messages",
            key,
            RelationalRowPageProjectedFields {
                requested_fields: POINT_FIELDS,
                hydration_fields: &[],
            },
            RelationalRowPageSnapshotReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
        )
        .expect("point cache evidence read");
    assert!(row.is_some());
    PointCacheProbe {
        cache_hits: report.demand.cache_hits,
        cache_misses: report.demand.cache_misses,
        file_pages_read: report.demand.file_pages_read,
    }
}

fn measure_scan(
    reader: &RelationalRowPageSnapshotReader,
    selectivity_percent: usize,
) -> ScanEvidence {
    let threshold = 100usize.saturating_sub(selectivity_percent) as i64;
    let owned_warmup = run_owned_scan(reader, threshold);
    let borrowed_warmup = run_borrowed_scan(reader, threshold);
    assert_equivalent(&owned_warmup, &borrowed_warmup);

    let mut owned_nanos = Vec::with_capacity(SAMPLES);
    let mut borrowed_nanos = Vec::with_capacity(SAMPLES);
    let mut owned_allocated_bytes = Vec::with_capacity(SAMPLES);
    let mut borrowed_allocated_bytes = Vec::with_capacity(SAMPLES);
    let mut representative = None;
    for sample in 0..SAMPLES {
        let (owned, borrowed) = if sample % 2 == 0 {
            (
                run_owned_scan(reader, threshold),
                run_borrowed_scan(reader, threshold),
            )
        } else {
            let borrowed = run_borrowed_scan(reader, threshold);
            let owned = run_owned_scan(reader, threshold);
            (owned, borrowed)
        };
        assert_equivalent(&owned, &borrowed);
        assert_eq!(owned.borrowed_rows, 0);
        assert_eq!(owned.owned_rows, owned.rows_scanned);
        assert_eq!(borrowed.borrowed_rows, borrowed.rows_scanned);
        assert_eq!(borrowed.owned_rows, 0);
        assert_eq!(owned.peak_pins, 1);
        assert_eq!(borrowed.peak_pins, 1);
        owned_nanos.push(owned.elapsed_nanos);
        borrowed_nanos.push(borrowed.elapsed_nanos);
        owned_allocated_bytes.push(owned.allocated_bytes);
        borrowed_allocated_bytes.push(borrowed.allocated_bytes);
        representative = Some((owned, borrowed));
    }
    owned_nanos.sort_unstable();
    borrowed_nanos.sort_unstable();
    owned_allocated_bytes.sort_unstable();
    borrowed_allocated_bytes.sort_unstable();
    let owned_nanos = median(&owned_nanos);
    let borrowed_nanos = median(&borrowed_nanos);
    let owned_allocated_bytes = median(&owned_allocated_bytes);
    let borrowed_allocated_bytes = median(&borrowed_allocated_bytes);
    let speedup = owned_nanos as f64 / borrowed_nanos.max(1) as f64;
    let allocation_reduction = reduction(owned_allocated_bytes, borrowed_allocated_bytes);
    let admitted =
        speedup >= REQUIRED_SPEEDUP || allocation_reduction >= REQUIRED_ALLOCATION_REDUCTION;
    let (owned, borrowed) = representative.expect("scan evidence has at least one sample");

    ScanEvidence {
        selectivity_percent,
        threshold,
        rows_scanned: owned.rows_scanned,
        rows_selected: owned.rows_selected,
        owned_nanos,
        borrowed_nanos,
        owned_allocated_bytes,
        borrowed_allocated_bytes,
        owned_materialized_variable_bytes: owned.materialized_variable_bytes,
        borrowed_materialized_variable_bytes: borrowed.materialized_variable_bytes,
        speedup,
        allocation_reduction,
        checksum: owned.checksum,
        admitted,
    }
}

fn run_owned_scan(reader: &RelationalRowPageSnapshotReader, threshold: i64) -> ScanOutcome {
    let before_allocated = allocated_bytes();
    let started = Instant::now();
    let mut rows_selected = 0usize;
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    let mut materialized_variable_bytes = 0u64;
    let mut hydration = RelationalHydrationBudget::default();
    let report = reader
        .visit_projected_range_fields_resolving(
            projected_range(),
            RelationalRowPageSnapshotReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
            |_, _, _| Ok(()),
            |row, _| {
                materialized_variable_bytes =
                    materialized_variable_bytes.saturating_add(projected_variable_bytes(&row));
                if owner(&row) >= threshold {
                    rows_selected = rows_selected.saturating_add(1);
                    checksum = hash_bytes(checksum, body(&row).as_bytes());
                }
                rows_selected < OUTPUT_LIMIT
            },
        )
        .expect("owned benchmark scan");
    let elapsed_nanos = started.elapsed().as_nanos();
    let allocated_bytes = allocated_bytes().saturating_sub(before_allocated);
    black_box(checksum);
    ScanOutcome {
        elapsed_nanos,
        allocated_bytes,
        materialized_variable_bytes,
        rows_scanned: report.demand.rows_emitted,
        rows_selected,
        borrowed_rows: report.demand.borrowed_rows_emitted,
        owned_rows: report.demand.owned_rows_emitted,
        peak_pins: report.demand.peak_pins,
        checksum,
    }
}

fn run_borrowed_scan(reader: &RelationalRowPageSnapshotReader, threshold: i64) -> ScanOutcome {
    let before_allocated = allocated_bytes();
    let started = Instant::now();
    let mut rows_selected = 0usize;
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    let mut materialized_variable_bytes = 0u64;
    let mut hydration = RelationalHydrationBudget::default();
    let report = reader
        .visit_projected_range_fields_resolving_ref(
            projected_range(),
            RelationalRowPageSnapshotReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
            |_, _, _| Ok(()),
            |row, _| {
                let owner = match row.value(1).expect("owner field is projected") {
                    RelationalValueRef::BigInt(value) => value,
                    value => panic!("expected BIGINT owner, got {value:?}"),
                };
                if owner >= threshold {
                    rows_selected = rows_selected.saturating_add(1);
                    let body = row
                        .value(2)
                        .expect("body field is projected")
                        .to_owned_value();
                    materialized_variable_bytes = materialized_variable_bytes
                        .saturating_add(body.estimated_payload_bytes() as u64);
                    let RelationalValue::Text(body) = body else {
                        panic!("expected TEXT body")
                    };
                    checksum = hash_bytes(checksum, body.as_bytes());
                }
                rows_selected < OUTPUT_LIMIT
            },
        )
        .expect("borrowed benchmark scan");
    let elapsed_nanos = started.elapsed().as_nanos();
    let allocated_bytes = allocated_bytes().saturating_sub(before_allocated);
    black_box(checksum);
    ScanOutcome {
        elapsed_nanos,
        allocated_bytes,
        materialized_variable_bytes,
        rows_scanned: report.demand.rows_emitted,
        rows_selected,
        borrowed_rows: report.demand.borrowed_rows_emitted,
        owned_rows: report.demand.owned_rows_emitted,
        peak_pins: report.demand.peak_pins,
        checksum,
    }
}

fn measure_point_lookup(reader: &RelationalRowPageSnapshotReader) -> PointEvidence {
    let keys = (0..POINT_PROBES)
        .map(|probe| {
            let ordinal = probe.wrapping_mul(2_654_435_761usize) % ROWS;
            skein_storage::RelationalKey(vec![RelationalValue::Text(row_id(ordinal))])
        })
        .collect::<Vec<_>>();
    let mut point_nanos = Vec::with_capacity(SAMPLES);
    let mut exact_range_nanos = Vec::with_capacity(SAMPLES);
    let mut point_allocated_bytes = Vec::with_capacity(SAMPLES);
    let mut exact_range_allocated_bytes = Vec::with_capacity(SAMPLES);
    let mut checksum = None;
    for sample in 0..SAMPLES {
        let (point, range) = if sample % 2 == 0 {
            (
                run_point_reads(reader, &keys),
                run_exact_range_reads(reader, &keys),
            )
        } else {
            let range = run_exact_range_reads(reader, &keys);
            let point = run_point_reads(reader, &keys);
            (point, range)
        };
        assert_eq!(point.checksum, range.checksum);
        point_nanos.push(point.elapsed_nanos);
        exact_range_nanos.push(range.elapsed_nanos);
        point_allocated_bytes.push(point.allocated_bytes);
        exact_range_allocated_bytes.push(range.allocated_bytes);
        checksum = Some(point.checksum);
    }
    point_nanos.sort_unstable();
    exact_range_nanos.sort_unstable();
    point_allocated_bytes.sort_unstable();
    exact_range_allocated_bytes.sort_unstable();
    let point_nanos = median(&point_nanos);
    let exact_range_nanos = median(&exact_range_nanos);
    let allowed_regression_nanos = ((exact_range_nanos as f64 * POINT_MAX_RELATIVE_REGRESSION)
        as u128)
        .max(POINT_ABSOLUTE_NOISE_BUDGET_NANOS);
    let point_allocated_bytes = median(&point_allocated_bytes);
    let exact_range_allocated_bytes = median(&exact_range_allocated_bytes);
    let allowed_allocated_regression_bytes =
        ((exact_range_allocated_bytes as f64 * POINT_MAX_RELATIVE_REGRESSION) as u128).max(4_096);
    let latency_admitted =
        point_nanos <= exact_range_nanos.saturating_add(allowed_regression_nanos);
    let allocation_admitted = point_allocated_bytes
        <= exact_range_allocated_bytes.saturating_add(allowed_allocated_regression_bytes);
    let admitted = allocation_admitted && (!POINT_LATENCY_GATE_REQUIRED || latency_admitted);
    PointEvidence {
        probes: POINT_PROBES,
        point_nanos,
        exact_range_nanos,
        point_allocated_bytes,
        exact_range_allocated_bytes,
        allowed_regression_nanos,
        allowed_allocated_regression_bytes,
        latency_gate_required: POINT_LATENCY_GATE_REQUIRED,
        latency_admitted,
        allocation_admitted,
        checksum: checksum.expect("point evidence has at least one sample"),
        admitted,
    }
}

fn run_point_reads(
    reader: &RelationalRowPageSnapshotReader,
    keys: &[skein_storage::RelationalKey],
) -> PointOutcome {
    let before_allocated = allocated_bytes();
    let started = Instant::now();
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    for key in keys {
        let mut hydration = RelationalHydrationBudget::default();
        let (row, report) = reader
            .point_projected_fields(
                "messages",
                key,
                RelationalRowPageProjectedFields {
                    requested_fields: POINT_FIELDS,
                    hydration_fields: &[],
                },
                RelationalRowPageSnapshotReadLimits::default(),
                &mut hydration,
                &RuntimeTaskContext::default(),
            )
            .expect("benchmark point read");
        assert_eq!(report.demand.owned_rows_emitted, 1);
        checksum = hash_projected_row(checksum, &row.expect("benchmark point row"));
    }
    let elapsed_nanos = started.elapsed().as_nanos();
    let allocated_bytes = allocated_bytes().saturating_sub(before_allocated);
    black_box(checksum);
    PointOutcome {
        elapsed_nanos,
        allocated_bytes,
        checksum,
    }
}

fn run_exact_range_reads(
    reader: &RelationalRowPageSnapshotReader,
    keys: &[skein_storage::RelationalKey],
) -> PointOutcome {
    let before_allocated = allocated_bytes();
    let started = Instant::now();
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    for key in keys {
        let mut hydration = RelationalHydrationBudget::default();
        let report = reader
            .visit_projected_range_fields_resolving(
                RelationalRowPageProjectedRangeFields {
                    range: RelationalRowPageProjectedRange {
                        table: "messages",
                        lower: Bound::Included(key),
                        upper: Bound::Included(key),
                        requested_fields: POINT_FIELDS,
                    },
                    hydration_fields: &[],
                },
                RelationalRowPageSnapshotReadLimits::default(),
                &mut hydration,
                &RuntimeTaskContext::default(),
                |_, _, _| Ok(()),
                |row, _| {
                    checksum = hash_projected_row(checksum, &row);
                    false
                },
            )
            .expect("benchmark exact range read");
        assert_eq!(report.demand.rows_emitted, 1);
        assert_eq!(report.demand.owned_rows_emitted, 1);
    }
    let elapsed_nanos = started.elapsed().as_nanos();
    let allocated_bytes = allocated_bytes().saturating_sub(before_allocated);
    black_box(checksum);
    PointOutcome {
        elapsed_nanos,
        allocated_bytes,
        checksum,
    }
}

fn projected_range() -> RelationalRowPageProjectedRangeFields<'static> {
    RelationalRowPageProjectedRangeFields {
        range: RelationalRowPageProjectedRange {
            table: "messages",
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
            requested_fields: REQUESTED_FIELDS,
        },
        hydration_fields: &[],
    }
}

fn owner(row: &RelationalProjectedRow) -> i64 {
    match projected_value(row, 1) {
        RelationalValue::BigInt(value) => *value,
        value => panic!("expected BIGINT owner, got {value:?}"),
    }
}

fn body(row: &RelationalProjectedRow) -> &str {
    match projected_value(row, 2) {
        RelationalValue::Text(value) => value,
        value => panic!("expected TEXT body, got {value:?}"),
    }
}

fn projected_value(row: &RelationalProjectedRow, ordinal: usize) -> &RelationalValue {
    &row.fields
        .iter()
        .find(|field| field.ordinal == ordinal)
        .unwrap_or_else(|| panic!("projected field {ordinal} is missing"))
        .value
}

fn projected_variable_bytes(row: &RelationalProjectedRow) -> u64 {
    row.primary_key
        .0
        .iter()
        .chain(row.fields.iter().map(|field| &field.value))
        .map(|value| value.estimated_payload_bytes() as u64)
        .fold(0u64, u64::saturating_add)
}

fn hash_projected_row(mut checksum: u64, row: &RelationalProjectedRow) -> u64 {
    for field in &row.fields {
        match &field.value {
            RelationalValue::Text(value) => checksum = hash_bytes(checksum, value.as_bytes()),
            value => panic!("point projection expected TEXT, got {value:?}"),
        }
    }
    checksum
}

fn hash_bytes(mut checksum: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        checksum ^= u64::from(*byte);
        checksum = checksum.wrapping_mul(0x0000_0100_0000_01b3);
    }
    checksum
}

fn assert_equivalent(owned: &ScanOutcome, borrowed: &ScanOutcome) {
    assert_eq!(owned.rows_scanned, borrowed.rows_scanned);
    assert_eq!(owned.rows_selected, borrowed.rows_selected);
    assert_eq!(owned.checksum, borrowed.checksum);
    assert_eq!(owned.rows_selected, OUTPUT_LIMIT);
}

fn reduction(baseline: u128, candidate: u128) -> f64 {
    if baseline == 0 {
        return 0.0;
    }
    1.0 - candidate as f64 / baseline as f64
}

fn median(samples: &[u128]) -> u128 {
    samples[samples.len() / 2]
}

fn allocated_bytes() -> u128 {
    u128::from(ALLOCATED_BYTES.load(Ordering::Relaxed))
}

fn add_allocated_bytes(bytes: usize) {
    ALLOCATED_BYTES.fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
}

struct Fixture {
    directory: PathBuf,
    reader: RelationalRowPageSnapshotReader,
}

impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "skein-relational-row-page-lending-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        seed_database(&directory);
        let (reader, _) = open_reader(&directory, StoreId(990));
        Self { directory, reader }
    }

    fn fresh_reader(&self) -> (RelationalRowPageSnapshotReader, Arc<SegmentCache>) {
        open_reader(&self.directory, StoreId(991))
    }

    fn remove(self) {
        let directory = self.directory.clone();
        drop(self);
        std::fs::remove_dir_all(directory).expect("remove lending benchmark fixture");
    }
}

fn open_reader(
    directory: &Path,
    store_id: StoreId,
) -> (RelationalRowPageSnapshotReader, Arc<SegmentCache>) {
    let row_root = Arc::new(
        RelationalRowPageRootReader::open_generation(
            directory,
            1,
            RelationalRowPagePublicationConfig::default(),
        )
        .expect("open benchmark row root"),
    );
    let overflow_root = Arc::new(
        RelationalOverflowRootReader::open_generation(
            directory,
            1,
            RelationalOverflowPublicationConfig::default(),
        )
        .expect("open benchmark overflow root"),
    );
    let view = Arc::new(RelationalRowPageReadView::from_base(row_root));
    let cache = Arc::new(SegmentCache::new(CACHE_BYTES));
    let reader = RelationalRowPageSnapshotReader::new(
        view,
        overflow_root,
        None,
        Arc::clone(&cache),
        store_id,
    )
    .expect("open benchmark snapshot reader");
    (reader, cache)
}

fn seed_database(directory: &Path) {
    let mut database = Database::open_with_config(directory, DatabaseConfig::default())
        .expect("open lending benchmark database");
    database
        .query_sql(
            "CREATE TABLE messages (id TEXT PRIMARY KEY, owner_bucket BIGINT NOT NULL, body TEXT NOT NULL)",
        )
        .expect("create lending benchmark table");
    for transaction_start in (0..ROWS).step_by(INSERT_TRANSACTION_ROWS) {
        let transaction_end = (transaction_start + INSERT_TRANSACTION_ROWS).min(ROWS);
        let mut transaction = database.begin_transaction();
        for start in (transaction_start..transaction_end).step_by(INSERT_BATCH_ROWS) {
            let end = (start + INSERT_BATCH_ROWS).min(transaction_end);
            let mut statement =
                String::from("INSERT INTO messages (id, owner_bucket, body) VALUES ");
            let mut parameters = Vec::with_capacity((end - start) * 3);
            for ordinal in start..end {
                if ordinal != start {
                    statement.push_str(", ");
                }
                let parameter = parameters.len() + 1;
                write!(
                    statement,
                    "(${}, ${}, ${})",
                    parameter,
                    parameter + 1,
                    parameter + 2
                )
                .expect("format insert statement");
                parameters.push(Value::String(row_id(ordinal)));
                parameters.push(Value::Int((ordinal % 100) as i64));
                parameters.push(Value::String(row_body(ordinal)));
            }
            transaction
                .query_sql_with_params(&statement, &parameters)
                .expect("insert lending benchmark batch");
        }
        transaction.commit().expect("commit lending benchmark rows");
    }
    database
        .checkpoint()
        .expect("checkpoint lending benchmark rows");

    let explain = database
        .query_sql("EXPLAIN ANALYZE SELECT body FROM messages WHERE owner_bucket >= 99 LIMIT 32")
        .expect("explain production lending query");
    let rendered = format!("{:?}", explain.rows);
    assert!(rendered.contains("row_borrowed_rows="));
    assert!(rendered.contains("row_owned_rows=0"));
}

fn row_id(ordinal: usize) -> String {
    format!("message-{ordinal:08}")
}

fn row_body(ordinal: usize) -> String {
    let prefix = format!("body-{ordinal:08}:");
    let mut body = String::with_capacity(BODY_BYTES);
    body.push_str(&prefix);
    body.extend(std::iter::repeat_n(
        char::from(b'a' + (ordinal % 26) as u8),
        BODY_BYTES - prefix.len(),
    ));
    body
}

struct ScanOutcome {
    elapsed_nanos: u128,
    allocated_bytes: u128,
    materialized_variable_bytes: u64,
    rows_scanned: usize,
    rows_selected: usize,
    borrowed_rows: usize,
    owned_rows: usize,
    peak_pins: usize,
    checksum: u64,
}

struct ScanEvidence {
    selectivity_percent: usize,
    threshold: i64,
    rows_scanned: usize,
    rows_selected: usize,
    owned_nanos: u128,
    borrowed_nanos: u128,
    owned_allocated_bytes: u128,
    borrowed_allocated_bytes: u128,
    owned_materialized_variable_bytes: u64,
    borrowed_materialized_variable_bytes: u64,
    speedup: f64,
    allocation_reduction: f64,
    checksum: u64,
    admitted: bool,
}

impl ScanEvidence {
    fn json(&self) -> serde_json::Value {
        json!({
            "selectivity_percent": self.selectivity_percent,
            "predicate": format!("owner_bucket >= {}", self.threshold),
            "rows_scanned": self.rows_scanned,
            "rows_selected": self.rows_selected,
            "owned_nanos": self.owned_nanos,
            "borrowed_nanos": self.borrowed_nanos,
            "speedup": self.speedup,
            "owned_allocated_bytes": self.owned_allocated_bytes,
            "borrowed_allocated_bytes": self.borrowed_allocated_bytes,
            "allocation_reduction": self.allocation_reduction,
            "owned_materialized_variable_bytes": self.owned_materialized_variable_bytes,
            "borrowed_materialized_variable_bytes": self.borrowed_materialized_variable_bytes,
            "checksum": self.checksum,
            "admitted": self.admitted,
        })
    }
}

struct PointOutcome {
    elapsed_nanos: u128,
    allocated_bytes: u128,
    checksum: u64,
}

struct PointEvidence {
    probes: usize,
    point_nanos: u128,
    exact_range_nanos: u128,
    point_allocated_bytes: u128,
    exact_range_allocated_bytes: u128,
    allowed_regression_nanos: u128,
    allowed_allocated_regression_bytes: u128,
    latency_gate_required: bool,
    latency_admitted: bool,
    allocation_admitted: bool,
    checksum: u64,
    admitted: bool,
}

struct PointCacheProbe {
    cache_hits: usize,
    cache_misses: usize,
    file_pages_read: usize,
}

struct PointCacheEvidence {
    cold: PointCacheProbe,
    warm: PointCacheProbe,
    resident_bytes: u64,
    admitted: bool,
}

impl PointCacheEvidence {
    fn json(&self) -> serde_json::Value {
        json!({
            "cold_cache_hits": self.cold.cache_hits,
            "cold_cache_misses": self.cold.cache_misses,
            "cold_file_pages_read": self.cold.file_pages_read,
            "warm_cache_hits": self.warm.cache_hits,
            "warm_cache_misses": self.warm.cache_misses,
            "warm_file_pages_read": self.warm.file_pages_read,
            "resident_bytes": self.resident_bytes,
            "admitted": self.admitted,
        })
    }
}

impl PointEvidence {
    fn json(&self) -> serde_json::Value {
        json!({
            "probes": self.probes,
            "point_nanos": self.point_nanos,
            "exact_owned_range_nanos": self.exact_range_nanos,
            "point_allocated_bytes": self.point_allocated_bytes,
            "exact_owned_range_allocated_bytes": self.exact_range_allocated_bytes,
            "allowed_regression_nanos": self.allowed_regression_nanos,
            "allowed_allocated_regression_bytes": self.allowed_allocated_regression_bytes,
            "latency_gate_required": self.latency_gate_required,
            "latency_admitted": self.latency_admitted,
            "allocation_admitted": self.allocation_admitted,
            "checksum": self.checksum,
            "admitted": self.admitted,
        })
    }
}
