use serde_json::{json, Value as JsonValue};
use skein::{
    AppendTableSchema, AppendTransaction, AppendWrite, Database, RelationalColumnSchema,
    RelationalKey, RelationalRow, RelationalScalarType, RelationalValue, SearchDocument,
    SearchIndex, SearchMode, Value, ValueRef,
};
use skein_fuzz::{emit_fuzz_report, DEFAULT_FUZZ_LOG_DIRECTORY};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

const PROTOCOL: &str = "skein-storage-parser-fuzz-v1";
const DEFAULT_CASES: usize = 256;
const MAX_CASES: usize = 10_000;

fn main() -> ExitCode {
    match run() {
        Ok(success) if success => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("skein-storage-fuzz: {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<bool, String> {
    let options = Options::parse(std::env::args().skip(1))?;
    let workspace = unique_workspace();
    let fixture = workspace.join("fixture");
    create_fixture(&fixture)?;
    let targets = parser_targets(&fixture)?;
    if targets.is_empty() {
        return Err("generated fixture contains no parser targets".to_string());
    }

    let indexes = options.case_index.map_or_else(
        || (0..options.cases).collect::<Vec<_>>(),
        |index| vec![index],
    );
    let mut cases = Vec::with_capacity(indexes.len());
    let mut success = true;
    for index in indexes {
        let report = run_case(&workspace, &fixture, &targets, options.seed, index)?;
        success &= report["success"].as_bool() == Some(true);
        cases.push(report);
    }
    let _ = fs::remove_dir_all(&workspace);

    let report = json!({
        "protocol": PROTOCOL,
        "seed": options.seed,
        "requested_case_count": cases.len(),
        "failed_case_count": cases.iter().filter(|case| case["success"] == false).count(),
        "success": success,
        "cases": cases,
    });
    let failed_case_count = report["failed_case_count"].as_u64().unwrap_or_default();
    let paths = emit_fuzz_report(
        &options.log_directory,
        "skein-storage-fuzz",
        &run_id(&options),
        &report,
        success,
        options.print_report,
        &mut io::stdout().lock(),
    )?;
    if let Some(path) = paths.failure {
        eprintln!(
            "skein-storage-fuzz: {failed_case_count} failing case(s); reproduction report: {}",
            path.display()
        );
    }
    Ok(success)
}

fn run_case(
    workspace: &Path,
    fixture: &Path,
    targets: &[PathBuf],
    campaign_seed: u64,
    index: usize,
) -> Result<JsonValue, String> {
    let case_seed = mix_seed(campaign_seed, index as u64);
    let mut rng = DeterministicRng::new(case_seed);
    let target = &targets[index % targets.len()];
    let case_root = workspace.join(format!("case-{index}"));
    copy_tree(fixture, &case_root)?;
    let target_path = case_root.join(target);
    let mutation = mutate_file(&target_path, &mut rng)?;
    let subsystem = target
        .components()
        .next()
        .and_then(|component| component.as_os_str().to_str())
        .ok_or_else(|| "parser target has no subsystem".to_string())?;
    let outcome = catch_unwind(AssertUnwindSafe(|| match subsystem {
        "graph" => validate_graph_fixture(&case_root.join("graph")),
        "search" => validate_search_fixture(&case_root.join("search")),
        _ => Err(ValidationError::Rejected(format!(
            "unknown parser target subsystem '{subsystem}'"
        ))),
    }));
    let (case_success, result_kind, detail, validation) = match outcome {
        Ok(Ok(validation)) => (
            true,
            "accepted_valid",
            "mutated artifact opened and the fixture hydrated exactly".to_string(),
            Some(validation),
        ),
        Ok(Err(ValidationError::Rejected(error))) => (
            true,
            "rejected",
            format!("storage rejected the mutated artifact: {error}"),
            None,
        ),
        Ok(Err(ValidationError::Mismatch(error))) => (
            false,
            "silent_corruption",
            format!("storage returned data that did not match the fixture: {error}"),
            None,
        ),
        Err(_) => (
            false,
            "panic",
            "persistent-format open or hydration panicked".to_string(),
            None,
        ),
    };
    let _ = fs::remove_dir_all(case_root);
    Ok(json!({
        "index": index,
        "case_seed": case_seed,
        "target": target.to_string_lossy(),
        "subsystem": subsystem,
        "mutation": mutation,
        "result_kind": result_kind,
        "detail": detail,
        "validation": validation,
        "success": case_success,
        "reproduction_command": format!(
            "bazel run //crates/fuzz:skein_storage_fuzz -- --seed {campaign_seed} --case-index {index}"
        ),
    }))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ValidationError {
    Rejected(String),
    Mismatch(String),
}

fn validate_graph_fixture(path: &Path) -> Result<JsonValue, ValidationError> {
    let mut database = Database::open(path)
        .map_err(|error| ValidationError::Rejected(format!("graph open failed: {error}")))?;
    let scrub = database.scrub_storage().map_err(|error| {
        ValidationError::Rejected(format!("graph integrity scrub failed: {error}"))
    })?;
    if scrub.checked_file_count == 0 || scrub.checked_bytes == 0 {
        return Err(ValidationError::Mismatch(
            "graph integrity scrub checked no persistent bytes".to_string(),
        ));
    }

    validate_memory_title(&mut database, "checkpoint", "checkpoint payload")?;
    validate_memory_title(&mut database, "wal", "wal payload")?;

    let partition = RelationalKey(vec![RelationalValue::Text("alpha".to_string())]);
    let append = database
        .read_append_partition("events", &partition, None, 73)
        .map_err(|error| {
            ValidationError::Rejected(format!("append partition hydration failed: {error}"))
        })?;
    if append.rows.len() != 72 {
        return Err(ValidationError::Mismatch(format!(
            "append partition returned {} rows instead of 72",
            append.rows.len()
        )));
    }
    for (sequence, row) in append.rows.iter().enumerate() {
        let sequence = sequence as i64;
        let expected_partition = RelationalKey(vec![RelationalValue::Text("alpha".to_string())]);
        let expected_order = RelationalKey(vec![RelationalValue::BigInt(sequence)]);
        let expected_row = RelationalRow::new(vec![
            RelationalValue::Text("alpha".to_string()),
            RelationalValue::BigInt(sequence),
            RelationalValue::Text(format!("payload-{sequence:04}")),
        ]);
        if row.partition_key != expected_partition
            || row.order_key != expected_order
            || row.row != expected_row
        {
            return Err(ValidationError::Mismatch(format!(
                "append row {sequence} did not match the fixture"
            )));
        }
    }

    Ok(json!({
        "graph": {
            "memory_count": 2,
            "append_row_count": append.rows.len(),
            "commit_epoch": database.commit_epoch(),
        },
        "integrity": {
            "checked_file_count": scrub.checked_file_count,
            "checked_bytes": scrub.checked_bytes,
            "sha256_verified_file_count": scrub.sha256_verified_file_count,
            "wal_record_count": scrub.wal_record_count,
            "wal_bytes": scrub.wal_bytes,
        },
    }))
}

fn validate_memory_title(
    database: &mut Database,
    id: &str,
    expected_title: &str,
) -> Result<(), ValidationError> {
    let parameters = BTreeMap::from([("id".to_string(), Value::String(id.to_string()))]);
    let output = database
        .query_with_params(
            "MATCH (memory:Memory {id: $id}) RETURN memory.title AS title",
            &parameters,
        )
        .map_err(|error| {
            ValidationError::Rejected(format!("Memory '{id}' hydration failed: {error}"))
        })?;
    if output.rows.len() != 1 {
        return Err(ValidationError::Mismatch(format!(
            "Memory '{id}' returned {} rows instead of 1",
            output.rows.len()
        )));
    }
    match output.rows.get(0, "title") {
        Some(ValueRef::String(title)) if title == expected_title => Ok(()),
        Some(value) => Err(ValidationError::Mismatch(format!(
            "Memory '{id}' returned unexpected title {value:?}"
        ))),
        None => Err(ValidationError::Mismatch(format!(
            "Memory '{id}' omitted the title column"
        ))),
    }
}

fn validate_search_fixture(path: &Path) -> Result<JsonValue, ValidationError> {
    let index = SearchIndex::open(path)
        .map_err(|error| ValidationError::Rejected(format!("search open failed: {error}")))?;
    let freshness = index.projection_freshness();
    if freshness.document_count != 1 {
        return Err(ValidationError::Mismatch(format!(
            "search projection reported {} documents instead of 1",
            freshness.document_count
        )));
    }

    let text_hits = index.search("persistent search payload", None, SearchMode::Text, 2);
    validate_search_hits("text", &text_hits)?;
    let vector_hits = index.search("", Some(&[0.25, -0.5, 0.75, 1.0]), SearchMode::Vector, 2);
    validate_search_hits("vector", &vector_hits)?;

    Ok(json!({
        "search": {
            "document_count": freshness.document_count,
            "text_hit_count": text_hits.len(),
            "vector_hit_count": vector_hits.len(),
        },
    }))
}

fn validate_search_hits(mode: &str, hits: &[skein::SearchHit]) -> Result<(), ValidationError> {
    if hits.len() == 1 && hits[0].id == "memory:checkpoint" {
        return Ok(());
    }
    Err(ValidationError::Mismatch(format!(
        "{mode} search returned ids {:?} instead of [\"memory:checkpoint\"]",
        hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>()
    )))
}

fn create_fixture(root: &Path) -> Result<(), String> {
    let graph_path = root.join("graph");
    let mut database = Database::open(&graph_path).map_err(|error| error.to_string())?;
    database
        .query("CREATE (:Memory {id: 'checkpoint', title: 'checkpoint payload'})")
        .map_err(|error| error.to_string())?;
    database
        .append_transaction(AppendTransaction {
            writes: vec![
                AppendWrite::CreateTable {
                    schema: append_schema(),
                },
                AppendWrite::Append {
                    table: "events".to_string(),
                    rows: append_rows(0, 64),
                },
            ],
        })
        .map_err(|error| error.to_string())?;
    database.checkpoint().map_err(|error| error.to_string())?;
    database
        .query("CREATE (:Memory {id: 'wal', title: 'wal payload'})")
        .map_err(|error| error.to_string())?;
    database
        .append_transaction(AppendTransaction {
            writes: vec![AppendWrite::Append {
                table: "events".to_string(),
                rows: append_rows(64, 8),
            }],
        })
        .map_err(|error| error.to_string())?;
    drop(database);

    let search_path = root.join("search");
    let mut search = SearchIndex::open(&search_path).map_err(|error| error.to_string())?;
    search
        .upsert(SearchDocument {
            id: "memory:checkpoint".to_string(),
            title: "Parser fixture".to_string(),
            content: "persistent search payload".to_string(),
            embedding: Some(vec![0.25, -0.5, 0.75, 1.0]),
            metadata: BTreeMap::from([
                ("lifecycle_state".to_string(), "active".to_string()),
                ("space_id".to_string(), "default".to_string()),
            ]),
        })
        .map_err(|error| error.to_string())?;
    search.checkpoint().map_err(|error| error.to_string())?;
    Ok(())
}

fn append_schema() -> AppendTableSchema {
    AppendTableSchema {
        name: "events".to_string(),
        columns: vec![
            append_column("stream", RelationalScalarType::Text),
            append_column("sequence", RelationalScalarType::BigInt),
            append_column("payload", RelationalScalarType::Text),
        ],
        partition_key: vec!["stream".to_string()],
        order_key: vec!["sequence".to_string()],
    }
}

fn append_column(name: &str, scalar_type: RelationalScalarType) -> RelationalColumnSchema {
    RelationalColumnSchema {
        name: name.to_string(),
        scalar_type,
        nullable: false,
        default: None,
    }
}

fn append_rows(start: i64, count: usize) -> Vec<RelationalRow> {
    (0..count)
        .map(|offset| {
            let sequence = start + offset as i64;
            RelationalRow::new(vec![
                RelationalValue::Text("alpha".to_string()),
                RelationalValue::BigInt(sequence),
                RelationalValue::Text(format!("payload-{sequence:04}")),
            ])
        })
        .collect()
}

fn parser_targets(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut targets = Vec::new();
    collect_targets(root, root, &mut targets)?;
    targets.sort();
    Ok(targets)
}

fn collect_targets(root: &Path, path: &Path, targets: &mut Vec<PathBuf>) -> Result<(), String> {
    for entry in fs::read_dir(path).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        if path.is_dir() {
            collect_targets(root, &path, targets)?;
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name == "owner.skein.lock" || name.ends_with(".tmp") {
            continue;
        }
        if name.ends_with(".skein") || name.ends_with(".tvim") {
            targets.push(
                path.strip_prefix(root)
                    .map_err(|error| error.to_string())?
                    .to_path_buf(),
            );
        }
    }
    Ok(())
}

fn mutate_file(path: &Path, rng: &mut DeterministicRng) -> Result<JsonValue, String> {
    let mut bytes = fs::read(path).map_err(|error| error.to_string())?;
    let original_len = bytes.len();
    let mutation_kind = rng.next_u64() % 4;
    let (kind, offset, length) = match mutation_kind {
        0 if !bytes.is_empty() => {
            let offset = (rng.next_u64() as usize) % bytes.len();
            let bit = 1u8 << (rng.next_u64() % 8);
            bytes[offset] ^= bit;
            ("bit_flip", offset, 1)
        }
        1 if !bytes.is_empty() => {
            let offset = (rng.next_u64() as usize) % bytes.len();
            bytes.truncate(offset);
            ("truncate", offset, original_len.saturating_sub(offset))
        }
        2 if !bytes.is_empty() => {
            let offset = (rng.next_u64() as usize) % bytes.len();
            let length = (bytes.len() - offset).min(1 + (rng.next_u64() as usize % 16));
            for byte in &mut bytes[offset..offset + length] {
                *byte = rng.next_u64() as u8;
            }
            ("overwrite", offset, length)
        }
        _ => {
            let offset = bytes.len();
            let length = 1 + (rng.next_u64() as usize % 16);
            bytes.extend((0..length).map(|_| rng.next_u64() as u8));
            ("append", offset, length)
        }
    };
    fs::write(path, &bytes).map_err(|error| error.to_string())?;
    Ok(json!({
        "kind": kind,
        "offset": offset,
        "length": length,
        "original_bytes": original_len,
        "mutated_bytes": bytes.len(),
    }))
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), String> {
    fs::create_dir_all(destination).map_err(|error| error.to_string())?;
    for entry in fs::read_dir(source).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if source_path.is_dir() {
            copy_tree(&source_path, &destination_path)?;
        } else {
            fs::copy(&source_path, &destination_path).map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

fn unique_workspace() -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "skein-storage-fuzz-{}-{timestamp}",
        std::process::id()
    ))
}

fn mix_seed(seed: u64, index: u64) -> u64 {
    let mut value = seed ^ index.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[derive(Debug, Clone, Copy)]
struct DeterministicRng(u64);

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0xa076_1d64_78bd_642f)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0 = self.0.wrapping_mul(0x2545_f491_4f6c_dd1d);
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Options {
    seed: u64,
    cases: usize,
    case_index: Option<usize>,
    log_directory: PathBuf,
    print_report: bool,
}

impl Options {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut options = Self {
            seed: 0,
            cases: DEFAULT_CASES,
            case_index: None,
            log_directory: PathBuf::from(DEFAULT_FUZZ_LOG_DIRECTORY),
            print_report: false,
        };
        let mut args = args.into_iter();
        while let Some(argument) = args.next() {
            match argument.as_str() {
                "--seed" => {
                    options.seed = next_value(&mut args, "--seed")?
                        .parse()
                        .map_err(|_| "--seed must be an unsigned 64-bit integer".to_string())?;
                }
                "--cases" => {
                    options.cases = next_value(&mut args, "--cases")?
                        .parse()
                        .map_err(|_| "--cases must be a non-negative integer".to_string())?;
                }
                "--case-index" => {
                    options.case_index = Some(
                        next_value(&mut args, "--case-index")?
                            .parse()
                            .map_err(|_| {
                                "--case-index must be a non-negative integer".to_string()
                            })?,
                    );
                }
                "--log-directory" => {
                    options.log_directory =
                        PathBuf::from(next_value(&mut args, "--log-directory")?);
                }
                "--print-report" => options.print_report = true,
                "--help" | "-h" => return Err(usage().to_string()),
                _ => return Err(format!("unknown argument '{argument}'\n{}", usage())),
            }
        }
        if options.cases > MAX_CASES {
            return Err(format!("--cases must not exceed {MAX_CASES}"));
        }
        if options.case_index.is_none() && options.cases == 0 {
            return Err("--cases must be greater than zero".to_string());
        }
        Ok(options)
    }
}

fn next_value(args: &mut impl Iterator<Item = String>, option: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("{option} requires a value\n{}", usage()))
}

fn usage() -> &'static str {
    "usage: skein-storage-fuzz [--seed <u64>] [--cases <usize>] [--case-index <usize>] [--log-directory <path>] [--print-report]"
}

fn run_id(options: &Options) -> String {
    options.case_index.map_or_else(
        || format!("seed-{}-cases-{}", options.seed, options.cases),
        |index| format!("seed-{}-case-{index}", options.seed),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_seed_is_stable_and_indexed() {
        assert_eq!(mix_seed(7, 19), mix_seed(7, 19));
        assert_ne!(mix_seed(7, 19), mix_seed(7, 20));
    }

    #[test]
    fn parser_rejects_excessive_campaigns() {
        assert!(Options::parse(["--cases".to_string(), "10001".to_string()]).is_err());
    }

    #[test]
    fn clean_fixture_passes_targeted_hydration() {
        let workspace = unique_workspace();
        create_fixture(&workspace).unwrap();

        assert!(validate_graph_fixture(&workspace.join("graph")).is_ok());
        assert!(validate_search_fixture(&workspace.join("search")).is_ok());

        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn targeted_hydration_detects_wrong_fixture_content() {
        let workspace = unique_workspace();
        create_fixture(&workspace).unwrap();
        let mut database = Database::open(workspace.join("graph")).unwrap();
        database
            .query("CREATE (:Memory {id: 'checkpoint', title: 'duplicate payload'})")
            .unwrap();
        drop(database);

        assert!(matches!(
            validate_graph_fixture(&workspace.join("graph")),
            Err(ValidationError::Mismatch(_))
        ));

        let _ = fs::remove_dir_all(workspace);
    }
}
