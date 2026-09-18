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

use hawdb::{
    AppendTableSchema, AppendTransaction, AppendWrite, Database, RelationalColumnSchema,
    RelationalKey, RelationalRow, RelationalScalarType, RelationalValue, SearchDocument,
    SearchIndex, SearchMode, Value, ValueRef,
};
use hawdb_fuzz::{
    compiled_capabilities_json, emit_fuzz_report, run_row_page_compaction_case,
    run_wal_tail_recovery_case, DEFAULT_FUZZ_LOG_DIRECTORY, ROW_PAGE_COMPACTION_PROTOCOL,
    WAL_TAIL_RECOVERY_PROTOCOL,
};
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const PROTOCOL: &str = "hawdb-storage-parser-fuzz-v1";
const DEFAULT_CASES: usize = 256;
const MAX_CASES: usize = 10_000;
const MAX_WORKSPACE_ATTEMPTS: usize = 128;

static NEXT_WORKSPACE_ID: AtomicU64 = AtomicU64::new(0);

fn main() -> ExitCode {
    match run() {
        Ok(success) if success => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("hawdb-storage-fuzz: {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<bool, String> {
    let options = Options::parse(std::env::args().skip(1))?;
    let workspace =
        unique_workspace().map_err(|error| format!("create storage fuzz workspace: {error}"))?;
    let fixture = workspace.join("fixture");
    let targets = if options.wal_tail || options.row_page_compaction {
        Vec::new()
    } else {
        create_fixture(&fixture)?;
        let targets = parser_targets(&fixture)?;
        if targets.is_empty() {
            return Err("generated fixture contains no parser targets".to_string());
        }
        targets
    };

    let indexes = options.case_index.map_or_else(
        || (0..options.cases).collect::<Vec<_>>(),
        |index| vec![index],
    );
    let mut cases = Vec::with_capacity(indexes.len());
    let mut success = true;
    for index in indexes {
        let report = if options.wal_tail || options.row_page_compaction {
            let seed = mix_seed(options.seed, index as u64);
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                if options.wal_tail {
                    run_wal_tail_recovery_case(seed)
                } else {
                    run_row_page_compaction_case(seed)
                }
            }));
            let mut report = match outcome {
                Ok(Ok(report)) => report,
                Ok(Err(error)) => json!({"success": false, "detail": error}),
                Err(_) => json!({"success": false, "detail": "storage state machine panicked"}),
            };
            report["case_seed"] = json!(seed);
            report["index"] = json!(index);
            let oracle = if options.wal_tail {
                "--wal-tail"
            } else {
                "--row-page-compaction"
            };
            report["reproduction_command"] = json!(format!(
                "bazel run //crates/fuzz:hawdb_storage_fuzz -- {oracle} --seed {} --case-index {index}", options.seed
            ));
            report
        } else {
            run_case(&workspace, &fixture, &targets, options.seed, index)?
        };
        success &= report["success"].as_bool() == Some(true);
        cases.push(report);
    }
    let _ = fs::remove_dir_all(&workspace);

    let report = json!({
        "protocol": if options.wal_tail { WAL_TAIL_RECOVERY_PROTOCOL }
            else if options.row_page_compaction { ROW_PAGE_COMPACTION_PROTOCOL } else { PROTOCOL },
        "compiled_capabilities": compiled_capabilities_json(),
        "seed": options.seed,
        "requested_case_count": cases.len(),
        "failed_case_count": cases.iter().filter(|case| case["success"] == false).count(),
        "success": success,
        "cases": cases,
    });
    let failed_case_count = report["failed_case_count"].as_u64().unwrap_or_default();
    let paths = emit_fuzz_report(
        &options.log_directory,
        "hawdb-storage-fuzz",
        &run_id(&options),
        &report,
        success,
        options.print_report,
        &mut io::stdout().lock(),
    )?;
    if let Some(path) = paths.failure {
        eprintln!(
            "hawdb-storage-fuzz: {failed_case_count} failing case(s); reproduction report: {}",
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
            "bazel run //crates/fuzz:hawdb_storage_fuzz -- --seed {campaign_seed} --case-index {index}"
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

    let text_hits = index
        .search("persistent search payload", None, SearchMode::Text, 2)
        .map_err(|error| ValidationError::Rejected(format!("text search failed: {error}")))?;
    validate_search_hits("text", &text_hits)?;
    let vector_hits = index
        .search("", Some(&[0.25, -0.5, 0.75, 1.0]), SearchMode::Vector, 2)
        .map_err(|error| ValidationError::Rejected(format!("vector search failed: {error}")))?;
    validate_search_hits("vector", &vector_hits)?;

    Ok(json!({
        "search": {
            "document_count": freshness.document_count,
            "text_hit_count": text_hits.len(),
            "vector_hit_count": vector_hits.len(),
        },
    }))
}

fn validate_search_hits(mode: &str, hits: &[hawdb::SearchHit]) -> Result<(), ValidationError> {
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
        order_mode: Default::default(),
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
        if name == "owner.hawdb.lock" || name.ends_with(".tmp") {
            continue;
        }
        if name.ends_with(".hawdb") || name.ends_with(".tvim") {
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

fn unique_workspace() -> io::Result<PathBuf> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    reserve_workspace(&std::env::temp_dir(), timestamp, &NEXT_WORKSPACE_ID)
}

fn reserve_workspace(parent: &Path, timestamp: u128, sequence: &AtomicU64) -> io::Result<PathBuf> {
    for _ in 0..MAX_WORKSPACE_ATTEMPTS {
        // The counter supplies unique candidates, not synchronization of data.
        let id = sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| io::Error::other("storage fuzz workspace sequence exhausted"))?;
        let path = parent.join(format!(
            "hawdb-storage-fuzz-{}-{timestamp}-{id}",
            std::process::id()
        ));
        // Only atomic creation establishes ownership; never reuse a stale path.
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "storage fuzz workspace allocation exhausted its collision retries",
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
    wal_tail: bool,
    row_page_compaction: bool,
}

impl Options {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut options = Self {
            seed: 0,
            cases: DEFAULT_CASES,
            case_index: None,
            log_directory: PathBuf::from(DEFAULT_FUZZ_LOG_DIRECTORY),
            print_report: false,
            wal_tail: false,
            row_page_compaction: false,
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
                "--wal-tail" => options.wal_tail = true,
                "--row-page-compaction" => options.row_page_compaction = true,
                "--help" | "-h" => return Err(usage().to_string()),
                _ => return Err(format!("unknown argument '{argument}'\n{}", usage())),
            }
        }
        if options.wal_tail && options.row_page_compaction {
            return Err("select only one storage state-machine oracle".to_string());
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
    "usage: hawdb-storage-fuzz [--wal-tail | --row-page-compaction] [--seed <u64>] [--cases <usize>] [--case-index <usize>] [--log-directory <path>] [--print-report]"
}

fn run_id(options: &Options) -> String {
    let prefix = if options.wal_tail {
        "wal-tail-"
    } else if options.row_page_compaction {
        "row-page-compaction-"
    } else {
        ""
    };
    options.case_index.map_or_else(
        || format!("{prefix}seed-{}-cases-{}", options.seed, options.cases),
        |index| format!("{prefix}seed-{}-case-{index}", options.seed),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_record_admission_seeded_campaign_preserves_published_generation() {
        use hawdb::{
            SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationWriter,
            SearchOutOfCoreReader,
        };
        use std::num::NonZeroU64;

        fn text(rng: &mut DeterministicRng) -> String {
            const PARTS: &[&str] = &[
                "word ",
                "\t",
                "\n",
                "\0",
                ";=",
                "\u{4e2d}\u{6587}",
                "\u{1f980}",
                "e\u{301}",
            ];
            (0..rng.next_u64() % 24)
                .map(|_| PARTS[rng.next_u64() as usize % PARTS.len()])
                .collect()
        }

        // Independent size oracle for the existing hex/tab/comma wire format.
        fn encoded_bytes(document: &SearchDocument) -> u64 {
            let text_bytes = document.id.len() + document.title.len() + document.content.len();
            let vector_bytes = document.embedding.as_ref().map_or(0, |values| {
                values
                    .iter()
                    .map(|value| value.to_string().len())
                    .sum::<usize>()
                    + values.len().saturating_sub(1)
            });
            let metadata_bytes = document
                .metadata
                .iter()
                .map(|(key, value)| 2 * (key.len() + value.len()) + 1)
                .sum::<usize>()
                + document.metadata.len().saturating_sub(1);
            (9 + 2 * text_bytes + vector_bytes + metadata_bytes) as u64
        }

        let workspace = unique_workspace().unwrap();
        for case in 0..48 {
            let mut rng = DeterministicRng::new(mix_seed(392, case));
            let document = SearchDocument {
                id: format!("record-{case}"),
                title: text(&mut rng),
                content: text(&mut rng),
                embedding: Some(vec![1.0, (rng.next_u64() as u32) as f32 / u32::MAX as f32]),
                metadata: (0..rng.next_u64() % 6)
                    .map(|field| (format!("field-{field}-{}", text(&mut rng)), text(&mut rng)))
                    .collect(),
            };
            let bytes = encoded_bytes(&document);
            let root = workspace.join(format!("case-{case}"));
            let options = SearchOutOfCoreGenerationBuildOptions {
                max_record_bytes: NonZeroU64::new(bytes).unwrap(),
                max_logical_document_bytes: NonZeroU64::new(bytes).unwrap(),
                ..Default::default()
            };
            let mut writer =
                SearchOutOfCoreGenerationWriter::create(&root, options.clone()).unwrap();
            writer.push(document.clone()).unwrap();
            let report = writer.finish().unwrap();
            assert_eq!(report.logical_document_bytes, bytes, "case {case}");

            let mut rejected_options = options;
            match case % 3 {
                0 => rejected_options.max_record_bytes = NonZeroU64::new(bytes - 1).unwrap(),
                1 => {
                    rejected_options.max_logical_document_bytes =
                        NonZeroU64::new(bytes - 1).unwrap()
                }
                _ => {
                    rejected_options.max_spool_bytes =
                        NonZeroU64::new(report.spool_bytes - 1).unwrap()
                }
            }
            let mut rejected =
                SearchOutOfCoreGenerationWriter::create(&root, rejected_options).unwrap();
            assert!(rejected.push(document.clone()).is_err(), "case {case}");
            assert!(rejected
                .finish()
                .unwrap_err()
                .to_string()
                .contains("poisoned"));
            let reader = SearchOutOfCoreReader::open(&root).unwrap();
            assert_eq!(reader.generation(), report.generation, "case {case}");
            assert_eq!(
                reader
                    .hydrate_documents(std::slice::from_ref(&document.id))
                    .unwrap()
                    .documents,
                vec![document]
            );
            assert!(!fs::read_dir(&root).unwrap().any(|entry| entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".search-generation.")));
        }
        fs::remove_dir_all(workspace).unwrap();
    }

    #[test]
    fn row_page_compaction_oracle_is_replayable_and_exclusive() {
        let options = Options::parse(
            ["--row-page-compaction", "--seed", "7", "--case-index", "3"].map(str::to_string),
        )
        .unwrap();
        assert!(options.row_page_compaction);
        assert_eq!(options.case_index, Some(3));
        assert_eq!(run_id(&options), "row-page-compaction-seed-7-case-3");
        assert!(
            Options::parse(["--row-page-compaction", "--wal-tail"].map(str::to_string)).is_err()
        );
    }

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
    fn workspace_is_reserved_before_it_is_returned() {
        let workspace = unique_workspace().unwrap();
        assert!(workspace.is_dir(), "workspace must be atomically reserved");
        fs::remove_dir_all(workspace).unwrap();
    }

    #[test]
    fn workspace_same_timestamp_reserves_distinct_directories() {
        let parent = unique_workspace().unwrap();
        let sequence = AtomicU64::new(0);
        let first = reserve_workspace(&parent, 7, &sequence).unwrap();
        let second = reserve_workspace(&parent, 7, &sequence).unwrap();

        assert_ne!(first, second);
        assert!(first.is_dir());
        assert!(second.is_dir());
        fs::write(second.join("retained"), b"other owner").unwrap();
        fs::remove_dir_all(first).unwrap();
        assert_eq!(fs::read(second.join("retained")).unwrap(), b"other owner");
        fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn workspace_concurrent_fixed_timestamp_allocations_are_exclusive() {
        use std::collections::HashSet;
        use std::sync::Barrier;

        let parent = unique_workspace().unwrap();
        let sequence = AtomicU64::new(0);
        let barrier = Barrier::new(8);
        let paths = std::thread::scope(|scope| {
            let workers = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        (0..64)
                            .map(|_| reserve_workspace(&parent, 7, &sequence).unwrap())
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>();
            workers
                .into_iter()
                .flat_map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });

        assert_eq!(paths.len(), 512);
        assert_eq!(paths.iter().collect::<HashSet<_>>().len(), paths.len());
        assert!(paths.iter().all(|path| path.is_dir()));
        assert_eq!(fs::read_dir(&parent).unwrap().count(), paths.len());
        fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn workspace_collision_preserves_existing_directories_and_files() {
        let parent = unique_workspace().unwrap();
        let existing = reserve_workspace(&parent, 7, &AtomicU64::new(0)).unwrap();
        fs::write(existing.join("retained"), b"previous owner").unwrap();
        let existing_file = parent.join(format!("hawdb-storage-fuzz-{}-7-1", std::process::id()));
        fs::write(&existing_file, b"existing file").unwrap();
        let sequence = AtomicU64::new(0);

        let workspace = reserve_workspace(&parent, 7, &sequence).unwrap();
        assert_ne!(workspace, existing);
        assert_ne!(workspace, existing_file);
        assert!(workspace.is_dir());
        assert_eq!(sequence.load(Ordering::Relaxed), 3);
        fs::remove_dir_all(workspace).unwrap();
        assert_eq!(
            fs::read(existing.join("retained")).unwrap(),
            b"previous owner"
        );
        assert_eq!(fs::read(existing_file).unwrap(), b"existing file");
        fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn workspace_collision_retries_are_bounded() {
        let parent = unique_workspace().unwrap();
        let existing_sequence = AtomicU64::new(0);
        let existing = (0..MAX_WORKSPACE_ATTEMPTS)
            .map(|_| reserve_workspace(&parent, 7, &existing_sequence).unwrap())
            .collect::<Vec<_>>();
        let sequence = AtomicU64::new(0);

        let error = reserve_workspace(&parent, 7, &sequence).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(
            sequence.load(Ordering::Relaxed),
            MAX_WORKSPACE_ATTEMPTS as u64
        );
        assert_eq!(fs::read_dir(&parent).unwrap().count(), existing.len());
        assert!(existing.iter().all(|path| path.is_dir()));
        fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn workspace_invalid_parent_errors_are_not_retried() {
        let parent = unique_workspace().unwrap();
        let file = parent.join("file");
        fs::write(&file, b"not a directory").unwrap();
        for invalid_parent in [parent.join("missing"), file.clone()] {
            let expected = fs::create_dir(invalid_parent.join("probe")).unwrap_err();
            let sequence = AtomicU64::new(0);
            let error = reserve_workspace(&invalid_parent, 7, &sequence).unwrap_err();
            assert_eq!(error.kind(), expected.kind());
            assert_eq!(error.raw_os_error(), expected.raw_os_error());
            assert_eq!(sequence.load(Ordering::Relaxed), 1);
        }
        assert_eq!(fs::read(file).unwrap(), b"not a directory");
        assert_eq!(fs::read_dir(&parent).unwrap().count(), 1);
        fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn workspace_sequence_exhaustion_does_not_wrap_or_create_a_directory() {
        let parent = unique_workspace().unwrap();
        let sequence = AtomicU64::new(u64::MAX - 1);
        let last = reserve_workspace(&parent, 7, &sequence).unwrap();
        assert!(last.is_dir());

        let error = reserve_workspace(&parent, 7, &sequence).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(
            error.to_string(),
            "storage fuzz workspace sequence exhausted"
        );
        assert_eq!(sequence.load(Ordering::Relaxed), u64::MAX);
        assert_eq!(fs::read_dir(&parent).unwrap().count(), 1);
        fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn clean_fixture_passes_targeted_hydration() {
        let workspace = unique_workspace().unwrap();
        create_fixture(&workspace).unwrap();

        assert!(validate_graph_fixture(&workspace.join("graph")).is_ok());
        assert!(validate_search_fixture(&workspace.join("search")).is_ok());

        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn targeted_hydration_detects_wrong_fixture_content() {
        let workspace = unique_workspace().unwrap();
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
