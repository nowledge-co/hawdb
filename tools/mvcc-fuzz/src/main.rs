// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

mod case;
mod engine;
mod oracle;
mod pessimistic;
mod shrink;
mod text;

use case::{generate, Case, Mode, FORMAT_VERSION};
use engine::{observe, Observation};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{Duration, Instant};

const HELP: &str = "hawdb-mvcc-fuzz: standalone local MVCC campaign\n\nCommands:\n  run [--mode optimistic|pessimistic] [--seed N] [--cases N] [--seconds N] [--explain | --print-report]\n  generate [--mode optimistic|pessimistic] [--seed N] [--index N]\n  replay FILE [--explain | --print-report]\n  shrink FILE [--attempts N] [--explain | --print-report]\n\nrun/replay/shrink accept --log-directory DIR (default target/mvcc-fuzz) and\n--wait-probe-ms N (pessimistic wait probe, default 200).\ngenerate prints an editable text case to stdout. FILE accepts .mvcc text,\nversioned JSON cases and saved JSON artifacts. --explain shows human-readable\nsteps and expected/actual results; --print-report prints a JSON summary.\nDefaults: seed=1, index=0, cases=1000, attempts=256. With --seconds alone,\nrun until the time budget or 1000000 cases; stop between complete cases.\nSuccess is otherwise quiet. Mismatch: exit 1; invalid input or I/O: exit 2.\nThis tool controls statement order, not internal thread scheduling.\nPessimistic mode runs each transaction on its own thread and also checks\nwhich statements wait for a lock, deadlock victims and serialization aborts.\n";

type ToolResult<T> = Result<T, Box<dyn Error>>;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    format_version: u32,
    source_revision: String,
    case: Case,
    observation: Observation,
}

struct Options {
    command: String,
    mode: Mode,
    wait_probe_ms: u64,
    input: Option<PathBuf>,
    seed: u64,
    index: u64,
    cases: u64,
    seconds: Option<u64>,
    attempts: usize,
    directory: PathBuf,
    print: bool,
    explain: bool,
}

fn options(args: &[String]) -> Result<Options, String> {
    let Some(command) = args.first() else {
        return Err("missing command; use --help".into());
    };
    if !matches!(command.as_str(), "run" | "generate" | "replay" | "shrink") {
        return Err(format!("unknown command {command}; use --help"));
    }
    let mut result = Options {
        command: command.clone(),
        mode: Mode::Optimistic,
        wait_probe_ms: 200,
        input: None,
        seed: 1,
        index: 0,
        cases: 1000,
        seconds: None,
        attempts: 256,
        directory: "target/mvcc-fuzz".into(),
        print: false,
        explain: false,
    };
    let mut remaining = args[1..].iter();
    if matches!(command.as_str(), "replay" | "shrink") {
        result.input = Some(
            remaining
                .next()
                .filter(|p| !p.starts_with("--"))
                .ok_or("missing input file")?
                .into(),
        );
    }
    let mut seen = std::collections::BTreeSet::new();
    while let Some(flag) = remaining.next() {
        if !seen.insert(flag) {
            return Err(format!("duplicate option {flag}"));
        }
        if flag == "--print-report" && command != "generate" {
            result.print = true;
            continue;
        }
        if flag == "--explain" && command != "generate" {
            result.explain = true;
            continue;
        }
        let value = remaining
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--seed" if matches!(command.as_str(), "run" | "generate") => {
                result.seed = value.parse().map_err(|_| "invalid seed")?
            }
            "--mode" if matches!(command.as_str(), "run" | "generate") => {
                result.mode = match value.as_str() {
                    "optimistic" => Mode::Optimistic,
                    "pessimistic" => Mode::Pessimistic,
                    _ => return Err("mode must be optimistic or pessimistic".into()),
                }
            }
            "--index" if command == "generate" => {
                result.index = value.parse().map_err(|_| "invalid index")?
            }
            "--cases" if command == "run" => {
                result.cases = value.parse().map_err(|_| "invalid cases")?
            }
            "--seconds" if command == "run" => {
                result.seconds = Some(value.parse().map_err(|_| "invalid seconds")?)
            }
            "--wait-probe-ms" if command != "generate" => {
                result.wait_probe_ms = value.parse().map_err(|_| "invalid wait probe")?
            }
            "--attempts" if command == "shrink" => {
                result.attempts = value.parse().map_err(|_| "invalid attempts")?
            }
            "--log-directory" if command != "generate" => result.directory = value.into(),
            _ => return Err(format!("unsupported option {flag} for {command}")),
        }
    }
    if result.seconds.is_some() && !seen.contains(&"--cases".to_string()) {
        result.cases = 1_000_000;
    }
    if !(1..=1_000_000).contains(&result.cases)
        || !(1..=10_000).contains(&result.attempts)
        || !(10..=5_000).contains(&result.wait_probe_ms)
        || result
            .seconds
            .is_some_and(|seconds| !(1..=3600).contains(&seconds))
    {
        return Err(
            "cases must be 1..1000000, attempts 1..10000, seconds 1..3600, wait probe 10..5000 ms"
                .into(),
        );
    }
    if result.print && result.explain {
        return Err("choose --explain or --print-report".into());
    }
    Ok(result)
}

fn read_case(path: &Path) -> ToolResult<Case> {
    if fs::metadata(path)?.len() > 16 * 1_048_576 {
        return Err("input exceeds 16 MiB".into());
    }
    let contents = fs::read_to_string(path)?;
    if !contents.trim_start().starts_with('{') {
        return Ok(text::parse(&contents)?);
    }
    let value: serde_json::Value = serde_json::from_str(&contents)?;
    let case = if value.get("case").is_some() {
        let artifact: Artifact = serde_json::from_value(value)?;
        if artifact.format_version != FORMAT_VERSION {
            return Err("unsupported artifact format_version".into());
        }
        artifact.case
    } else {
        serde_json::from_value(value)?
    };
    case.validate()?;
    Ok(case)
}

fn revision() -> String {
    // Provenance for the runtime source checkout, not a build attestation.
    Command::new("git")
        .args(["-C", env!("CARGO_MANIFEST_DIR"), "rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn write_bytes(path: &Path, bytes: &[u8]) -> ToolResult<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    Ok(())
}

fn write_json(path: &Path, value: &impl Serialize) -> ToolResult<()> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    write_bytes(path, &bytes)
}

fn save_case(directory: &Path, name: &str, artifact: Artifact, explain: bool) -> ToolResult<()> {
    let readable = text::explain(&artifact.case, &artifact.observation);
    write_bytes(
        &directory.join(format!("{name}.mvcc")),
        text::format(&artifact.case).as_bytes(),
    )?;
    write_bytes(&directory.join(format!("{name}.txt")), readable.as_bytes())?;
    write_json(&directory.join(format!("{name}.json")), &artifact)?;
    if explain {
        print!("{readable}");
    }
    Ok(())
}

fn generate_case(mode: Mode, seed: u64, index: u64) -> Case {
    match mode {
        Mode::Optimistic => generate(seed, index),
        Mode::Pessimistic => pessimistic::generate(seed, index),
    }
}

fn execute(options: Options) -> ToolResult<bool> {
    if options.command == "generate" {
        print!(
            "{}",
            text::format(&generate_case(options.mode, options.seed, options.index))
        );
        return Ok(false);
    }
    pessimistic::set_wait_probe(options.wait_probe_ms);
    fs::create_dir_all(&options.directory)?;
    let invocation = options.directory.join(format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    fs::create_dir(&invocation)?;
    let source_revision = revision();
    let mut completed = 0;
    let mut failed = false;
    let mut attempts = None;
    let mut outcomes = std::collections::BTreeMap::new();
    let family_count = match options.mode {
        Mode::Optimistic => 8,
        Mode::Pessimistic => pessimistic::FAMILIES.len(),
    };
    let mut families = vec![[0_u64; 2]; family_count];
    let started = Instant::now();
    if options.command == "run" {
        for index in 0..options.cases {
            if completed > 0
                && options
                    .seconds
                    .is_some_and(|seconds| started.elapsed() >= Duration::from_secs(seconds))
            {
                break;
            }
            let case = generate_case(options.mode, options.seed, index);
            let left_terminal = case.schedule.iter().rposition(|tx| *tx == 0);
            let right_terminal = case.schedule.iter().rposition(|tx| *tx == 1);
            let first = usize::from(right_terminal < left_terminal);
            families[(index % family_count as u64) as usize][first] += 1;
            let observation = observe(&case)?;
            pessimistic::tally(&observation, &mut outcomes);
            completed += 1;
            if observation.failure.is_some() {
                save_case(
                    &invocation,
                    "failure",
                    Artifact {
                        format_version: FORMAT_VERSION,
                        source_revision: source_revision.clone(),
                        case,
                        observation,
                    },
                    options.explain,
                )?;
                eprintln!(
                    "MVCC mismatch: {}",
                    invocation.join("failure.json").display()
                );
                failed = true;
                break;
            }
        }
    } else {
        let original = read_case(options.input.as_deref().ok_or("missing input file")?)?;
        let (case, observation) = if options.command == "shrink" {
            let (case, observation, used) = shrink::shrink(original, options.attempts)?;
            attempts = Some(used);
            (case, observation)
        } else {
            let observation = observe(&original)?;
            (original, observation)
        };
        failed = observation.failure.is_some();
        completed = 1;
        let name = if options.command == "shrink" {
            "reduced"
        } else {
            "replay"
        };
        save_case(
            &invocation,
            name,
            Artifact {
                format_version: FORMAT_VERSION,
                source_revision: source_revision.clone(),
                case,
                observation,
            },
            options.explain,
        )?;
        if failed {
            eprintln!(
                "MVCC mismatch: {}",
                invocation.join(format!("{name}.json")).display()
            );
        }
    }
    let elapsed_ms = started.elapsed().as_millis();
    let report = serde_json::json!({
        "format_version": FORMAT_VERSION, "command": options.command,
        "source_revision": source_revision,
        "provenance": "runtime checkout HEAD; may include uncommitted changes; not build attestation",
        "seed": (options.command == "run").then_some(options.seed),
        "requested_cases": (options.command == "run").then_some(options.cases),
        "time_budget_seconds": options.seconds, "elapsed_ms": elapsed_ms,
        "completed_cases": completed,
        "families_by_first_terminal": (options.command == "run").then_some(&families),
        "reduction_attempts": attempts, "status": if failed { "mismatch" } else { "passed" },
        "directory": invocation,
        "mode": (options.command == "run").then_some(options.mode),
        "pessimistic_outcomes": (options.mode == Mode::Pessimistic).then_some(&outcomes),
        "wait_probe_ms": (options.mode == Mode::Pessimistic).then_some(options.wait_probe_ms),
        "scope": match options.mode {
            Mode::Optimistic => "two optimistic snapshots; deterministic statement order; in-memory relational primary keys and broad predicates",
            Mode::Pessimistic => "two pessimistic transactions on worker threads; deterministic statement order; lock waits, deadlock victims, snapshot refresh and serialization aborts; in-memory relational primary keys and broad predicates",
        }
    });
    write_json(&invocation.join("report.json"), &report)?;
    if options.print {
        println!("{}", serde_json::to_string_pretty(&report)?);
    }
    if options.explain {
        println!(
            "{} | {completed} cases | {elapsed_ms} ms | {}",
            if failed { "FAIL" } else { "PASS" },
            invocation.display()
        );
        if options.command == "run" {
            let names = match options.mode {
                Mode::Optimistic => &[
                    "same_key",
                    "disjoint",
                    "net_zero",
                    "absent_delete",
                    "delete_insert",
                    "broad_narrow",
                    "duplicate_insert",
                    "rollback",
                ],
                Mode::Pessimistic => &pessimistic::FAMILIES[..],
            };
            for (name, counts) in names.iter().zip(&families) {
                println!(
                    "  {name:<18} T1 first: {:>6}  T2 first: {:>6}",
                    counts[0], counts[1]
                );
            }
        }
    }
    Ok(failed)
}

fn main() -> ExitCode {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() == 1 && matches!(args[0].as_str(), "--help" | "-h") {
        print!("{HELP}");
        return ExitCode::SUCCESS;
    }
    match options(&args)
        .map_err(|e| -> Box<dyn Error> { e.into() })
        .and_then(execute)
    {
        Ok(false) => ExitCode::SUCCESS,
        Ok(true) => ExitCode::from(1),
        Err(error) => {
            eprintln!("hawdb-mvcc-fuzz: {error}");
            ExitCode::from(2)
        }
    }
}
