use serde_json::json;
use skein_fuzz::{
    emit_fuzz_report, run_append_state_machine_case, APPEND_STATE_MACHINE_PROTOCOL,
    DEFAULT_FUZZ_LOG_DIRECTORY,
};
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::process::ExitCode;

const DEFAULT_CASES: usize = 64;
const DEFAULT_STEPS: usize = 128;
const MAX_CASES: usize = 10_000;
const MAX_STEPS: usize = 10_000;

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("skein-append-fuzz: {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<bool, String> {
    let options = Options::parse(std::env::args().skip(1))?;
    let indexes = options.case_index.map_or_else(
        || (0..options.cases).collect::<Vec<_>>(),
        |index| vec![index],
    );
    let mut reports = Vec::with_capacity(indexes.len());
    let mut success = true;
    for index in indexes {
        let case_seed = mix_seed(options.seed, index as u64);
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            run_append_state_machine_case(case_seed, options.steps)
        }));
        let report = match outcome {
            Ok(Ok(report)) => report,
            Ok(Err(error)) => {
                success = false;
                json!({
                    "protocol": APPEND_STATE_MACHINE_PROTOCOL,
                    "seed": case_seed,
                    "steps": options.steps,
                    "success": false,
                    "error": error,
                })
            }
            Err(_) => {
                success = false;
                json!({
                    "protocol": APPEND_STATE_MACHINE_PROTOCOL,
                    "seed": case_seed,
                    "steps": options.steps,
                    "success": false,
                    "error": "append state-machine case panicked",
                })
            }
        };
        reports.push(json!({
            "index": index,
            "case_seed": case_seed,
            "report": report,
            "reproduction_command": format!(
                "bazel run //crates/fuzz:skein_append_fuzz -- --seed {} --cases {} --steps {} --case-index {index}",
                options.seed, options.cases, options.steps
            ),
        }));
    }
    let failed_case_count = reports
        .iter()
        .filter(|case| case["report"]["success"] == false)
        .count();
    let report = json!({
        "protocol": APPEND_STATE_MACHINE_PROTOCOL,
        "campaign_seed": options.seed,
        "steps_per_case": options.steps,
        "case_count": reports.len(),
        "failed_case_count": failed_case_count,
        "success": success,
        "cases": reports,
    });
    let paths = emit_fuzz_report(
        &options.log_directory,
        "skein-append-fuzz",
        &run_id(&options),
        &report,
        success,
        options.print_report,
        &mut io::stdout().lock(),
    )?;
    if let Some(path) = paths.failure {
        eprintln!(
            "skein-append-fuzz: {failed_case_count} failing case(s); reproduction report: {}",
            path.display()
        );
    }
    Ok(success)
}

struct Options {
    seed: u64,
    cases: usize,
    steps: usize,
    case_index: Option<usize>,
    log_directory: PathBuf,
    print_report: bool,
}

impl Options {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut seed = 0u64;
        let mut cases = DEFAULT_CASES;
        let mut steps = DEFAULT_STEPS;
        let mut case_index = None;
        let mut log_directory = PathBuf::from(DEFAULT_FUZZ_LOG_DIRECTORY);
        let mut print_report = false;
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--seed" => seed = parse_value(&mut args, "--seed")?,
                "--cases" => cases = parse_value(&mut args, "--cases")?,
                "--steps" => steps = parse_value(&mut args, "--steps")?,
                "--case-index" => case_index = Some(parse_value(&mut args, "--case-index")?),
                "--log-directory" => {
                    log_directory = PathBuf::from(
                        args.next()
                            .ok_or_else(|| "--log-directory requires a value".to_string())?,
                    );
                }
                "--print-report" => print_report = true,
                "--help" | "-h" => return Err(usage().to_string()),
                _ => return Err(format!("unknown argument {arg}; {}", usage())),
            }
        }
        if cases == 0 || cases > MAX_CASES {
            return Err(format!("--cases must be in 1..={MAX_CASES}"));
        }
        if steps == 0 || steps > MAX_STEPS {
            return Err(format!("--steps must be in 1..={MAX_STEPS}"));
        }
        if case_index.is_some_and(|index| index >= cases) {
            return Err("--case-index must be less than --cases".to_string());
        }
        Ok(Self {
            seed,
            cases,
            steps,
            case_index,
            log_directory,
            print_report,
        })
    }
}

fn parse_value<T: std::str::FromStr>(
    args: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<T, String> {
    args.next()
        .ok_or_else(|| format!("{option} requires a value"))?
        .parse()
        .map_err(|_| format!("{option} value is invalid"))
}

fn usage() -> &'static str {
    "usage: skein-append-fuzz [--seed <u64>] [--cases <usize>] [--steps <usize>] [--case-index <usize>] [--log-directory <path>] [--print-report]"
}

fn run_id(options: &Options) -> String {
    options.case_index.map_or_else(
        || {
            format!(
                "seed-{}-cases-{}-steps-{}",
                options.seed, options.cases, options.steps
            )
        },
        |index| format!("seed-{}-case-{index}-steps-{}", options.seed, options.steps),
    )
}

fn mix_seed(seed: u64, index: u64) -> u64 {
    let mut value = seed ^ index.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
