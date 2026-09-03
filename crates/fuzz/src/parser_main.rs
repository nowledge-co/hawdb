use serde_json::json;
use skein_fuzz::{
    emit_fuzz_report, generate_parser_fuzz_case, parser_input_fingerprint, run_parser_fuzz_case,
    DEFAULT_FUZZ_LOG_DIRECTORY, PARSER_FUZZ_PROTOCOL,
};
use std::io;
use std::path::PathBuf;
use std::process::ExitCode;

const DEFAULT_CASES: usize = 256;
const MAX_CASES: usize = 100_000;

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("skein-parser-fuzz: {error}");
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
    let mut success = true;
    let mut cases = Vec::with_capacity(indexes.len());
    for index in indexes {
        let case = generate_parser_fuzz_case(options.seed, index);
        let fingerprint = parser_input_fingerprint(&case.input);
        let input_bytes = case.input.len();
        let reproduction_command = format!(
            "bazel run //crates/fuzz:skein_parser_fuzz -- --seed {} --case-index {index}",
            options.seed
        );
        match run_parser_fuzz_case(&case) {
            Ok(observation) => cases.push(json!({
                "index": index,
                "case_seed": case.case_seed,
                "source_kind": case.source_kind,
                "seed_name": case.seed_name,
                "mutation_count": case.mutation_count,
                "input_bytes": input_bytes,
                "input_fingerprint": fingerprint,
                "used_lossy_utf8": observation.used_lossy_utf8,
                "accepted": {
                    "cypher": observation.cypher_accepted,
                    "relational_sql": observation.relational_sql_accepted,
                    "postgres_syntax": observation.postgres_syntax_accepted,
                    "pgq": observation.pgq_accepted,
                },
                "success": true,
                "reproduction_command": reproduction_command,
            })),
            Err(error) => {
                success = false;
                cases.push(json!({
                    "index": index,
                    "case_seed": case.case_seed,
                    "source_kind": case.source_kind,
                    "seed_name": case.seed_name,
                    "mutation_count": case.mutation_count,
                    "input_bytes": input_bytes,
                    "input_fingerprint": fingerprint,
                    "success": false,
                    "error": error,
                    "reproduction_command": reproduction_command,
                }));
            }
        }
    }

    let report = json!({
        "protocol": PARSER_FUZZ_PROTOCOL,
        "seed": options.seed,
        "requested_case_count": cases.len(),
        "failed_case_count": cases.iter().filter(|case| case["success"] == false).count(),
        "success": success,
        "cases": cases,
    });
    let paths = emit_fuzz_report(
        &options.log_directory,
        "skein-parser-fuzz",
        &run_id(&options),
        &report,
        success,
        options.print_report,
        &mut io::stdout().lock(),
    )?;
    if let Some(path) = paths.failure {
        eprintln!(
            "skein-parser-fuzz: parser panic detected; reproduction report: {}",
            path.display()
        );
    }
    Ok(success)
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
    "usage: skein-parser-fuzz [--seed <u64>] [--cases <usize>] [--case-index <usize>] [--log-directory <path>] [--print-report]"
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
    fn parses_campaign_and_exact_replay_options() {
        let campaign = Options::parse([
            "--seed".to_string(),
            "7".to_string(),
            "--cases".to_string(),
            "128".to_string(),
            "--print-report".to_string(),
        ])
        .unwrap();
        assert_eq!(campaign.seed, 7);
        assert_eq!(campaign.cases, 128);
        assert!(campaign.print_report);

        let replay = Options::parse([
            "--seed".to_string(),
            "19".to_string(),
            "--case-index".to_string(),
            "42".to_string(),
        ])
        .unwrap();
        assert_eq!(replay.case_index, Some(42));
    }

    #[test]
    fn rejects_empty_or_excessive_campaigns() {
        assert!(Options::parse(["--cases".to_string(), "0".to_string()]).is_err());
        assert!(Options::parse(["--cases".to_string(), "100001".to_string()]).is_err());
    }
}
