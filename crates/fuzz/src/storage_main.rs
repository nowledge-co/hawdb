use serde_json::{json, Value as JsonValue};
use skein::{
    AppendTableSchema, AppendTransaction, AppendWrite, Database, RelationalColumnSchema,
    RelationalRow, RelationalScalarType, RelationalValue, SearchDocument, SearchIndex,
};
use std::collections::BTreeMap;
use std::fs;
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
    println!(
        "{}",
        serde_json::to_string_pretty(&report)
            .map_err(|error| format!("failed to encode report: {error}"))?
    );
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
        "graph" => Database::open(case_root.join("graph"))
            .map(|database| format!("opened graph at epoch {}", database.commit_epoch()))
            .map_err(|error| error.to_string()),
        "search" => SearchIndex::open(case_root.join("search"))
            .map(|index| {
                format!(
                    "opened search with {} documents",
                    index.projection_freshness().document_count
                )
            })
            .map_err(|error| error.to_string()),
        _ => Err(format!("unknown parser target subsystem '{subsystem}'")),
    }));
    let (case_success, result_kind, detail) = match outcome {
        Ok(Ok(_)) => (
            true,
            "accepted",
            "parser accepted mutated artifact".to_string(),
        ),
        Ok(Err(_)) => (
            true,
            "rejected",
            "parser rejected mutated artifact".to_string(),
        ),
        Err(_) => (
            false,
            "panic",
            "persistent-format parser panicked".to_string(),
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
        "success": case_success,
        "reproduction_command": format!(
            "cargo run -p skein-fuzz --bin skein-storage-fuzz -- --seed {campaign_seed} --case-index {index}"
        ),
    }))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Options {
    seed: u64,
    cases: usize,
    case_index: Option<usize>,
}

impl Options {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut options = Self {
            seed: 0,
            cases: DEFAULT_CASES,
            case_index: None,
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
    "usage: skein-storage-fuzz [--seed <u64>] [--cases <usize>] [--case-index <usize>]"
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
}
