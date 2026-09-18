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

use hawdb_fuzz::{
    emit_fuzz_report, fuzz_current_report_path, read_fuzz_current_report,
    run_append_state_machine_case, write_fuzz_current_report, CampaignExecutionOptions,
    CampaignOptions, APPEND_STATE_MACHINE_PROTOCOL, DEFAULT_FUZZ_LOG_DIRECTORY,
    DEFAULT_FUZZ_PROGRESS_INTERVAL,
};
use serde_json::{json, Value as JsonValue};
use std::io::{self, Write};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const CAMPAIGN_NAME: &str = "hawdb-append-fuzz";
const DEFAULT_CASES: usize = 64;
const DEFAULT_STEPS: usize = 128;
const MAX_CASES: usize = 10_000;
const MAX_STEPS: usize = 10_000;

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("{CAMPAIGN_NAME}: {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<bool, String> {
    run_with_args(std::env::args().skip(1), &mut io::stdout().lock())
}

fn run_with_args(
    args: impl Iterator<Item = String>,
    stdout: &mut impl Write,
) -> Result<bool, String> {
    let mut options = Options::parse(args)?;
    let run_id = run_id(&options);
    let current_path = fuzz_current_report_path(&options.log_directory, CAMPAIGN_NAME, &run_id);
    let mut cases = if options.resume {
        load_resume_cases(&current_path, &options)?
    } else {
        Vec::new()
    };
    let resumed_case_count = cases.len();
    options.execution.resume_after_case = cases
        .last()
        .and_then(|case| case["index"].as_u64())
        .and_then(|index| usize::try_from(index).ok());

    let mut completed_since_resume = 0usize;
    for index in selected_indexes(&options) {
        cases.push(run_append_case(&options, index));
        completed_since_resume += 1;
        if completed_since_resume == 1
            || completed_since_resume.is_multiple_of(options.progress_interval)
        {
            write_fuzz_current_report(
                &current_path,
                &append_campaign_json(&options, &cases, resumed_case_count, false),
            )?;
        }
    }

    let report = append_campaign_json(&options, &cases, resumed_case_count, true);
    let success = report["success"].as_bool() == Some(true);
    let failed_case_count = report["failed_case_count"].as_u64().unwrap_or_default();
    let paths = emit_fuzz_report(
        &options.log_directory,
        CAMPAIGN_NAME,
        &run_id,
        &report,
        success,
        options.print_report,
        stdout,
    )?;
    if let Some(path) = paths.failure {
        eprintln!(
            "{CAMPAIGN_NAME}: {failed_case_count} failing case(s); reproduction report: {}",
            path.display()
        );
    }
    Ok(success)
}

fn selected_indexes(options: &Options) -> impl Iterator<Item = usize> + '_ {
    let resume_after_case = options.execution.resume_after_case;
    (0..options.cases).filter(move |index| {
        options.case_index.map_or_else(
            || {
                index % options.execution.shard_count == options.execution.shard_index
                    && resume_after_case.is_none_or(|completed| *index > completed)
            },
            |target| *index == target,
        )
    })
}

fn run_append_case(options: &Options, index: usize) -> JsonValue {
    let case_seed = mix_seed(options.seed, index as u64);
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        run_append_state_machine_case(case_seed, options.steps)
    }));
    let report = match outcome {
        Ok(Ok(report)) => report,
        Ok(Err(error)) => json!({
            "protocol": APPEND_STATE_MACHINE_PROTOCOL,
            "seed": case_seed,
            "steps": options.steps,
            "success": false,
            "error": error,
        }),
        Err(_) => json!({
            "protocol": APPEND_STATE_MACHINE_PROTOCOL,
            "seed": case_seed,
            "steps": options.steps,
            "success": false,
            "error": "append state-machine case panicked",
        }),
    };
    json!({
        "index": index,
        "case_seed": case_seed,
        "report": report,
        "reproduction_command": format!(
            "bazel run //crates/fuzz:hawdb_append_fuzz -- --seed {} --cases {} --steps {} --case-index {index}",
            options.seed, options.cases, options.steps
        ),
    })
}

fn append_campaign_json(
    options: &Options,
    cases: &[JsonValue],
    resumed_case_count: usize,
    terminal: bool,
) -> JsonValue {
    let failed_case_count = cases
        .iter()
        .filter(|case| case["report"]["success"] == false)
        .count();
    let requested_case_count = options
        .execution
        .selected_case_count(options.campaign_options());
    let complete = terminal && cases.len() == requested_case_count;
    json!({
        "protocol": APPEND_STATE_MACHINE_PROTOCOL,
        "complete": complete,
        "success": complete && failed_case_count == 0 && !cases.is_empty(),
        "campaign_seed": options.seed,
        "configured_case_count": options.cases,
        "requested_case_count": requested_case_count,
        "executed_case_count": cases.len(),
        "case_count": cases.len(),
        "passed_case_count": cases.len().saturating_sub(failed_case_count),
        "failed_case_count": failed_case_count,
        "current_case_index": cases.last().and_then(|case| case["index"].as_u64()),
        "shard_index": options.execution.shard_index,
        "shard_count": options.execution.shard_count,
        "steps_per_case": options.steps,
        "resumed_case_count": resumed_case_count,
        "cases": cases,
    })
}

fn load_resume_cases(path: &Path, options: &Options) -> Result<Vec<JsonValue>, String> {
    let report = read_fuzz_current_report(path)?
        .ok_or_else(|| format!("cannot resume because '{}' does not exist", path.display()))?;
    if report["protocol"] != APPEND_STATE_MACHINE_PROTOCOL
        || report["campaign_seed"].as_u64() != Some(options.seed)
        || report["configured_case_count"].as_u64() != Some(options.cases as u64)
        || report["steps_per_case"].as_u64() != Some(options.steps as u64)
        || report["shard_index"].as_u64() != Some(options.execution.shard_index as u64)
        || report["shard_count"].as_u64() != Some(options.execution.shard_count as u64)
    {
        return Err("current report does not match the requested append campaign identity".into());
    }
    let cases = report["cases"]
        .as_array()
        .cloned()
        .ok_or_else(|| "current report has no case array".to_string())?;
    validate_resume_cases(options, &cases)?;
    Ok(cases)
}

fn validate_resume_cases(options: &Options, cases: &[JsonValue]) -> Result<(), String> {
    for (position, case) in cases.iter().enumerate() {
        let expected_index =
            options.execution.shard_index + position.saturating_mul(options.execution.shard_count);
        let index = case["index"]
            .as_u64()
            .and_then(|index| usize::try_from(index).ok());
        if index != Some(expected_index) || expected_index >= options.cases {
            return Err(
                "current report cases are not the completed prefix of the requested shard"
                    .to_string(),
            );
        }
        let expected_seed = mix_seed(options.seed, expected_index as u64);
        if case["case_seed"].as_u64() != Some(expected_seed)
            || case["report"]["protocol"] != APPEND_STATE_MACHINE_PROTOCOL
            || case["report"]["seed"].as_u64() != Some(expected_seed)
            || case["report"]["steps"].as_u64() != Some(options.steps as u64)
            || case["report"]["success"].as_bool().is_none()
        {
            return Err("current report contains an invalid append case".to_string());
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Options {
    seed: u64,
    cases: usize,
    steps: usize,
    case_index: Option<usize>,
    execution: CampaignExecutionOptions,
    progress_interval: usize,
    resume: bool,
    log_directory: PathBuf,
    print_report: bool,
}

impl Options {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut seed = 0u64;
        let mut cases = DEFAULT_CASES;
        let mut steps = DEFAULT_STEPS;
        let mut case_index = None;
        let mut execution = CampaignExecutionOptions::default();
        let mut progress_interval = DEFAULT_FUZZ_PROGRESS_INTERVAL;
        let mut resume = false;
        let mut log_directory = PathBuf::from(DEFAULT_FUZZ_LOG_DIRECTORY);
        let mut print_report = false;
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--seed" => seed = parse_value(&mut args, "--seed")?,
                "--cases" => cases = parse_value(&mut args, "--cases")?,
                "--steps" => steps = parse_value(&mut args, "--steps")?,
                "--case-index" => case_index = Some(parse_value(&mut args, "--case-index")?),
                "--shard-index" => {
                    execution.shard_index = parse_value(&mut args, "--shard-index")?;
                }
                "--shard-count" => {
                    execution.shard_count = parse_value(&mut args, "--shard-count")?;
                }
                "--progress-interval" => {
                    progress_interval = parse_value(&mut args, "--progress-interval")?;
                }
                "--resume" => resume = true,
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
        if progress_interval == 0 {
            return Err("--progress-interval must be greater than zero".to_string());
        }
        if case_index.is_some_and(|index| index >= cases) {
            return Err("--case-index must be less than --cases".to_string());
        }
        let campaign = CampaignOptions {
            seed,
            case_count: cases,
            case_index,
        };
        execution
            .validate(campaign)
            .map_err(|error| error.to_string())?;
        if resume && case_index.is_some() {
            return Err("exact case replay cannot be combined with resume".to_string());
        }
        if execution.selected_case_count(campaign) == 0 {
            return Err("campaign shard selects no cases".to_string());
        }
        Ok(Self {
            seed,
            cases,
            steps,
            case_index,
            execution,
            progress_interval,
            resume,
            log_directory,
            print_report,
        })
    }

    const fn campaign_options(&self) -> CampaignOptions {
        CampaignOptions {
            seed: self.seed,
            case_count: self.cases,
            case_index: self.case_index,
        }
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
    "usage: hawdb-append-fuzz [--seed <u64>] [--cases <usize>] [--steps <usize>] [--case-index <usize>] [--shard-index <usize>] [--shard-count <usize>] [--progress-interval <usize>] [--resume] [--log-directory <path>] [--print-report]"
}

fn run_id(options: &Options) -> String {
    let base = options.case_index.map_or_else(
        || {
            format!(
                "seed-{}-cases-{}-steps-{}",
                options.seed, options.cases, options.steps
            )
        },
        |index| format!("seed-{}-case-{index}-steps-{}", options.seed, options.steps),
    );
    if options.execution.shard_count == 1 {
        base
    } else {
        format!(
            "{base}-shard-{}-of-{}",
            options.execution.shard_index, options.execution.shard_count
        )
    }
}

fn mix_seed(seed: u64, index: u64) -> u64 {
    let mut value = seed ^ index.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn parses_sharding_progress_and_resume_options() {
        let options = Options::parse(
            [
                "--seed",
                "7",
                "--cases",
                "12",
                "--steps",
                "32",
                "--shard-index",
                "2",
                "--shard-count",
                "5",
                "--progress-interval",
                "3",
                "--resume",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert_eq!(options.seed, 7);
        assert_eq!(options.cases, 12);
        assert_eq!(options.steps, 32);
        assert_eq!(options.execution.shard_index, 2);
        assert_eq!(options.execution.shard_count, 5);
        assert_eq!(options.progress_interval, 3);
        assert!(options.resume);
    }

    #[test]
    fn exact_replay_rejects_sharding_and_resume() {
        let sharded = Options::parse(
            ["--case-index", "2", "--shard-count", "2"]
                .into_iter()
                .map(str::to_string),
        )
        .unwrap_err();
        assert!(sharded.contains("exact case replay"));

        let resumed = Options::parse(
            ["--case-index", "2", "--resume"]
                .into_iter()
                .map(str::to_string),
        )
        .unwrap_err();
        assert!(resumed.contains("exact case replay"));
    }

    #[test]
    fn resume_requires_a_strict_shard_prefix() {
        let options = Options::parse(
            [
                "--seed",
                "7",
                "--cases",
                "8",
                "--steps",
                "1",
                "--shard-index",
                "1",
                "--shard-count",
                "2",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();
        let first = run_append_case(&options, 1);
        let skipped = run_append_case(&options, 5);

        assert!(validate_resume_cases(&options, std::slice::from_ref(&first)).is_ok());
        assert_eq!(
            validate_resume_cases(&options, &[first, skipped]).unwrap_err(),
            "current report cases are not the completed prefix of the requested shard"
        );
    }

    #[test]
    fn sharded_campaign_is_quiet_and_writes_complete_report() {
        let directory = unique_directory("shard");
        let args = [
            "--seed",
            "7",
            "--cases",
            "4",
            "--steps",
            "1",
            "--shard-index",
            "1",
            "--shard-count",
            "2",
            "--progress-interval",
            "1",
            "--log-directory",
            directory.to_str().unwrap(),
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
        let mut stdout = Vec::new();

        assert!(run_with_args(args.into_iter(), &mut stdout).unwrap());
        assert!(stdout.is_empty());
        let current =
            directory.join("hawdb-append-fuzz-seed-7-cases-4-steps-1-shard-1-of-2-cur.json");
        let report = read_fuzz_current_report(&current).unwrap().unwrap();
        assert_eq!(report["complete"], true);
        assert_eq!(report["success"], true);
        assert_eq!(report["configured_case_count"], 4);
        assert_eq!(report["requested_case_count"], 2);
        assert_eq!(report["executed_case_count"], 2);
        assert_eq!(report["current_case_index"], 3);
        assert_eq!(report["cases"][0]["index"], 1);
        assert_eq!(report["cases"][1]["index"], 3);

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn campaign_resumes_without_reexecuting_completed_cases() {
        let directory = unique_directory("resume");
        let base_args = [
            "--seed",
            "11",
            "--cases",
            "3",
            "--steps",
            "1",
            "--progress-interval",
            "1",
            "--log-directory",
            directory.to_str().unwrap(),
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
        let options = Options::parse(base_args.clone().into_iter()).unwrap();
        let current = fuzz_current_report_path(&directory, CAMPAIGN_NAME, &run_id(&options));
        let first = run_append_case(&options, 0);
        write_fuzz_current_report(
            &current,
            &append_campaign_json(&options, &[first], 0, false),
        )
        .unwrap();

        let mut resume_args = base_args;
        resume_args.push("--resume".to_string());
        let mut stdout = Vec::new();
        assert!(run_with_args(resume_args.into_iter(), &mut stdout).unwrap());
        assert!(stdout.is_empty());

        let report = read_fuzz_current_report(&current).unwrap().unwrap();
        assert_eq!(report["complete"], true);
        assert_eq!(report["resumed_case_count"], 1);
        assert_eq!(report["executed_case_count"], 3);
        assert_eq!(report["cases"][0]["index"], 0);
        assert_eq!(report["cases"][1]["index"], 1);
        assert_eq!(report["cases"][2]["index"], 2);

        fs::remove_dir_all(directory).unwrap();
    }

    fn unique_directory(name: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "hawdb-append-fuzz-{name}-{}-{timestamp}",
            std::process::id()
        ))
    }
}
