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

use serde_json::Value as JsonValue;
use shiro_core::{
    campaign_progress_json, emit_fuzz_report, fuzz_current_report_path, merge_campaign_report_json,
    read_fuzz_current_report, run_campaign_until, run_campaign_with_case_observer,
    write_fuzz_current_report, CampaignExecutionOptions, CampaignOptions, FuzzError,
    CAMPAIGN_PROTOCOL, DEFAULT_FUZZ_LOG_DIRECTORY, DEFAULT_FUZZ_PROGRESS_INTERVAL,
};
use std::cell::Cell;
use std::io;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

mod qualification_cli;

fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() == Some("qualify") {
        return qualification_cli::run(std::env::args().skip(2));
    }
    if std::env::args().skip(1).collect::<Vec<_>>().as_slice() == ["--help"] {
        println!("{}", usage());
        return ExitCode::SUCCESS;
    }
    match run() {
        Ok(success) => {
            if success {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(error) => {
            eprintln!("shiro: {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<bool, FuzzError> {
    let mut options = parse_options(std::env::args().skip(1))?;
    options.execution.validate(options.campaign)?;
    if !options.finite {
        return run_continuously(&options);
    }
    let run_id = run_id(options.campaign, options.execution);
    let current_path = fuzz_current_report_path(&options.log_directory, "shiro-query", &run_id);
    let mut cases = if options.resume {
        load_resume_cases(&current_path, options.campaign, options.execution)?
    } else {
        Vec::new()
    };
    let prior_cases = cases.clone();
    options.execution.resume_after_case = cases
        .last()
        .and_then(|case| case["index"].as_u64())
        .and_then(|index| usize::try_from(index).ok());
    let mut completed_since_resume = 0usize;
    let report = run_campaign_with_case_observer(options.campaign, options.execution, |case| {
        cases.push(case.json());
        completed_since_resume += 1;
        if completed_since_resume == 1
            || completed_since_resume.is_multiple_of(options.progress_interval)
        {
            write_fuzz_current_report(
                &current_path,
                &campaign_progress_json(options.campaign, options.execution, &cases),
            )
            .map_err(FuzzError::new)?;
        }
        Ok(())
    })?;
    let report_json =
        merge_campaign_report_json(&report, &prior_cases, options.campaign, options.execution);
    let success = report_json["success"].as_bool() == Some(true);
    let paths = emit_fuzz_report(
        &options.log_directory,
        "shiro-query",
        &run_id,
        &report_json,
        success,
        options.print_report,
        &mut io::stdout().lock(),
    )
    .map_err(FuzzError::new)?;
    if let Some(path) = paths.failure {
        eprintln!(
            "shiro: {} failing case(s); reproduction report: {}",
            report_json["failed_case_count"]
                .as_u64()
                .unwrap_or_default(),
            path.display()
        );
    }
    Ok(success)
}

fn run_continuously(options: &Options) -> Result<bool, FuzzError> {
    let started = Instant::now();
    let deadline = options.timeout_seconds.map(Duration::from_secs);
    let mut progress_at = started;
    let mut total_cases = 0usize;
    let mut batch = 0u64;
    let run_id = format!("seed-{}-continuous", options.campaign.seed);
    let current_path = fuzz_current_report_path(&options.log_directory, "shiro-query", &run_id);
    eprintln!(
        "shiro: continuous fuzzing; seed={}; stop with Ctrl+C{}",
        options.campaign.seed,
        options
            .timeout_seconds
            .map_or_else(String::new, |seconds| format!("; timeout={seconds}s"))
    );
    loop {
        let campaign = CampaignOptions {
            seed: options.campaign.seed.wrapping_add(batch),
            case_count: 128,
            case_index: None,
        };
        let failed = Cell::new(false);
        let completed = Cell::new(0usize);
        let mut cases = Vec::new();
        let report = run_campaign_until(
            campaign,
            options.execution,
            |case| {
                total_cases += 1;
                completed.set(completed.get() + 1);
                failed.set(!case.success);
                cases.push(case.json());
                if cases.len() == 1
                    || cases.len().is_multiple_of(options.progress_interval)
                    || failed.get()
                    || progress_at.elapsed() >= Duration::from_secs(5)
                {
                    let mut progress = campaign_progress_json(campaign, options.execution, &cases);
                    progress["session"] = serde_json::json!({
                        "seed": options.campaign.seed, "batch": batch,
                        "completed_cases": total_cases, "elapsed_seconds": started.elapsed().as_secs_f64(),
                        "status": "running"
                    });
                    write_fuzz_current_report(&current_path, &progress).map_err(FuzzError::new)?;
                }
                if progress_at.elapsed() >= Duration::from_secs(5) {
                    print_progress(total_cases, started);
                    progress_at = Instant::now();
                }
                Ok(())
            },
            || {
                failed.get()
                    || (completed.get() > 0
                        && deadline.is_some_and(|limit| started.elapsed() >= limit))
            },
        )?;
        let timed_out = deadline.is_some_and(|limit| started.elapsed() >= limit);
        let success = !failed.get();
        if !success || timed_out {
            let mut json = report.json();
            json["session"] = serde_json::json!({
                "seed": options.campaign.seed, "batch": batch,
                "completed_cases": total_cases, "elapsed_seconds": started.elapsed().as_secs_f64(),
                "stop_reason": if success { "timeout" } else { "oracle_failure" },
                "retained_cases": "last_batch_only"
            });
            let paths = emit_fuzz_report(
                &options.log_directory,
                "shiro-query",
                &run_id,
                &json,
                success,
                options.print_report,
                &mut io::stdout().lock(),
            )
            .map_err(FuzzError::new)?;
            print_progress(total_cases, started);
            eprintln!(
                "shiro: stopped ({}); report: {}",
                if success { "timeout" } else { "oracle failure" },
                paths.current.display()
            );
            return Ok(success);
        }
        batch = batch
            .checked_add(1)
            .ok_or_else(|| FuzzError::new("batch counter exhausted"))?;
    }
}

fn print_progress(completed: usize, started: Instant) {
    let elapsed = started.elapsed().as_secs_f64();
    eprintln!(
        "shiro: {completed} cases; {elapsed:.1}s; {:.1} cases/s",
        completed as f64 / elapsed.max(0.001)
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Options {
    campaign: CampaignOptions,
    execution: CampaignExecutionOptions,
    log_directory: PathBuf,
    progress_interval: usize,
    resume: bool,
    print_report: bool,
    finite: bool,
    timeout_seconds: Option<u64>,
}

fn parse_options(args: impl IntoIterator<Item = String>) -> Result<Options, FuzzError> {
    let mut campaign = CampaignOptions::default();
    let mut execution = CampaignExecutionOptions::default();
    let mut log_directory = PathBuf::from(DEFAULT_FUZZ_LOG_DIRECTORY);
    let mut progress_interval = DEFAULT_FUZZ_PROGRESS_INTERVAL;
    let mut resume = false;
    let mut print_report = false;
    let mut finite = false;
    let mut timeout_seconds = None;
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--seed" => {
                let value = args.next().ok_or_else(|| FuzzError::new(usage()))?;
                campaign.seed = value
                    .parse()
                    .map_err(|_| FuzzError::new("--seed must be an unsigned 64-bit integer"))?;
            }
            "--cases" => {
                finite = true;
                let value = args.next().ok_or_else(|| FuzzError::new(usage()))?;
                campaign.case_count = value
                    .parse()
                    .map_err(|_| FuzzError::new("--cases must be a non-negative integer"))?;
            }
            "--case-index" => {
                finite = true;
                let value = args.next().ok_or_else(|| FuzzError::new(usage()))?;
                campaign.case_index =
                    Some(value.parse().map_err(|_| {
                        FuzzError::new("--case-index must be a non-negative integer")
                    })?);
            }
            "--shard-index" => {
                execution.shard_index = next_usize(&mut args, "--shard-index")?;
            }
            "--shard-count" => {
                execution.shard_count = next_usize(&mut args, "--shard-count")?;
            }
            "--progress-interval" => {
                progress_interval = next_usize(&mut args, "--progress-interval")?;
            }
            "--timeout-seconds" => {
                let seconds = args
                    .next()
                    .ok_or_else(|| FuzzError::new(usage()))?
                    .parse::<u64>()
                    .map_err(|_| FuzzError::new("--timeout-seconds must be a positive integer"))?;
                if seconds == 0 {
                    return Err(FuzzError::new("--timeout-seconds must be positive"));
                }
                timeout_seconds = Some(seconds);
            }
            "--resume" => resume = true,
            "--log-directory" => {
                log_directory = PathBuf::from(args.next().ok_or_else(|| FuzzError::new(usage()))?);
            }
            "--print-report" => print_report = true,
            "--help" | "-h" => return Err(FuzzError::new(usage())),
            _ => {
                return Err(FuzzError::new(format!(
                    "unknown argument '{argument}'\n{}",
                    usage()
                )));
            }
        }
    }
    if progress_interval == 0 {
        return Err(FuzzError::new(
            "--progress-interval must be greater than zero",
        ));
    }
    if finite && timeout_seconds.is_some() {
        return Err(FuzzError::new(
            "--timeout-seconds cannot be combined with --cases or --case-index",
        ));
    }
    if resume && !finite {
        return Err(FuzzError::new(
            "--resume requires an explicit finite --cases campaign",
        ));
    }
    Ok(Options {
        campaign,
        execution,
        log_directory,
        progress_interval,
        resume,
        print_report,
        finite,
        timeout_seconds,
    })
}

fn next_usize(args: &mut impl Iterator<Item = String>, option: &str) -> Result<usize, FuzzError> {
    args.next()
        .ok_or_else(|| FuzzError::new(usage()))?
        .parse()
        .map_err(|_| FuzzError::new(format!("{option} must be a non-negative integer")))
}

fn usage() -> &'static str {
    "usage: shiro [--timeout-seconds <u64>] [--seed <u64>] [--cases <usize>] [--case-index <usize>] [--shard-index <usize>] [--shard-count <usize>] [--progress-interval <usize>] [--resume] [--log-directory <path>] [--print-report]"
}

fn run_id(options: CampaignOptions, execution: CampaignExecutionOptions) -> String {
    let base = options.case_index.map_or_else(
        || format!("seed-{}-cases-{}", options.seed, options.case_count),
        |index| format!("seed-{}-case-{index}", options.seed),
    );
    if execution.shard_count == 1 {
        base
    } else {
        format!(
            "{base}-shard-{}-of-{}",
            execution.shard_index, execution.shard_count
        )
    }
}

fn load_resume_cases(
    current_path: &std::path::Path,
    campaign: CampaignOptions,
    execution: CampaignExecutionOptions,
) -> Result<Vec<JsonValue>, FuzzError> {
    let report = read_fuzz_current_report(current_path)
        .map_err(FuzzError::new)?
        .ok_or_else(|| {
            FuzzError::new(format!(
                "cannot resume because '{}' does not exist",
                current_path.display()
            ))
        })?;
    if report["protocol"] != CAMPAIGN_PROTOCOL
        || report["seed"].as_u64() != Some(campaign.seed)
        || report["configured_case_count"].as_u64() != Some(campaign.case_count as u64)
        || report["shard_index"].as_u64() != Some(execution.shard_index as u64)
        || report["shard_count"].as_u64() != Some(execution.shard_count as u64)
    {
        return Err(FuzzError::new(
            "current report does not match the requested campaign identity",
        ));
    }
    let cases = report["cases"]
        .as_array()
        .cloned()
        .ok_or_else(|| FuzzError::new("current report has no case array"))?;
    if cases.iter().any(|case| {
        case["index"].as_u64().is_none_or(|index| {
            index >= campaign.case_count as u64
                || index % execution.shard_count as u64 != execution.shard_index as u64
        })
    }) {
        return Err(FuzzError::new(
            "current report contains a case outside the requested shard",
        ));
    }
    if cases.windows(2).any(|cases| {
        cases[0]["index"].as_u64().unwrap_or_default()
            >= cases[1]["index"].as_u64().unwrap_or_default()
    }) {
        return Err(FuzzError::new(
            "current report case indexes are not strictly increasing",
        ));
    }
    Ok(cases)
}

#[cfg(test)]
mod tests {
    use super::parse_options;

    #[test]
    fn defaults_to_continuous_and_accepts_timeout() {
        assert!(!parse_options([]).unwrap().finite);
        let timed = parse_options(["--timeout-seconds".into(), "30".into()]).unwrap();
        assert_eq!(timed.timeout_seconds, Some(30));
        assert!(!timed.finite);
        assert!(parse_options(["--timeout-seconds".into(), "0".into()]).is_err());
        assert!(parse_options([
            "--timeout-seconds".into(),
            "1".into(),
            "--cases".into(),
            "2".into()
        ])
        .is_err());
        assert!(parse_options(["--resume".into()]).is_err());
    }

    #[test]
    fn controlled_campaign_stops_between_checked_cases() {
        let completed = std::cell::Cell::new(0);
        let report = shiro_core::run_campaign_until(
            shiro_core::CampaignOptions {
                seed: 7,
                case_count: 2,
                case_index: None,
            },
            shiro_core::CampaignExecutionOptions::default(),
            |_| {
                completed.set(completed.get() + 1);
                Ok(())
            },
            || completed.get() == 1,
        )
        .unwrap();
        assert_eq!(report.executed_case_count, 1);
        assert!(report.success());
    }

    #[test]
    fn parses_seed_and_case_count() {
        let options = parse_options([
            "--seed".to_string(),
            "7".to_string(),
            "--cases".to_string(),
            "12".to_string(),
        ])
        .unwrap();

        assert_eq!(options.campaign.seed, 7);
        assert_eq!(options.campaign.case_count, 12);
        assert_eq!(options.campaign.case_index, None);
        assert_eq!(
            options.log_directory,
            std::path::Path::new(shiro_core::DEFAULT_FUZZ_LOG_DIRECTORY)
        );
        assert!(!options.print_report);
    }

    #[test]
    fn parses_exact_case_index() {
        let options = parse_options([
            "--seed".to_string(),
            "7".to_string(),
            "--case-index".to_string(),
            "19".to_string(),
        ])
        .unwrap();

        assert_eq!(options.campaign.seed, 7);
        assert_eq!(options.campaign.case_index, Some(19));
    }

    #[test]
    fn parses_output_options() {
        let options = parse_options([
            "--log-directory".to_string(),
            "/tmp/hawdb-fuzz".to_string(),
            "--print-report".to_string(),
        ])
        .unwrap();

        assert_eq!(
            options.log_directory,
            std::path::Path::new("/tmp/hawdb-fuzz")
        );
        assert!(options.print_report);
    }

    #[test]
    fn parses_sharding_progress_and_resume_options() {
        let options = parse_options([
            "--shard-index".to_string(),
            "2".to_string(),
            "--shard-count".to_string(),
            "5".to_string(),
            "--progress-interval".to_string(),
            "7".to_string(),
            "--resume".to_string(),
            "--cases".to_string(),
            "32".to_string(),
        ])
        .unwrap();

        assert_eq!(options.execution.shard_index, 2);
        assert_eq!(options.execution.shard_count, 5);
        assert_eq!(options.progress_interval, 7);
        assert!(options.resume);
    }
}
