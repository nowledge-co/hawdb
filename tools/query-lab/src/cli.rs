// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use crate::{
    cardinality,
    manifest::{write_new, Frozen},
    runner,
};
use query_workloads::smith::Limits;
use std::{fs, io::Write, path::Path};

pub const USAGE: &str = "query-lab freeze PATH [SEED CASES ROWS]\nquery-lab freeze-cost PATH [SEED CASES ROWS]\nquery-lab freeze-ce PATH [SEED ROWS]\nquery-lab freeze-graph-stats PATH [SEED ROWS]\nquery-lab compare MANIFEST REPORT\nquery-lab ce-run MANIFEST REPORT\nquery-lab ce-report MANIFEST RUN_REPORT OUTPUT\nquery-lab ce-diff MANIFEST BASELINE_RUN CANDIDATE_RUN OUTPUT\nquery-lab cost MANIFEST REPORT\nquery-lab cost-check MANIFEST REPORT\nquery-lab replay MANIFEST CELL\nAll commands are manual. Cost requires a release build and collects a frozen ordinary-plan baseline, not qualified alternative-plan regret. Cost-check exercises sampling under the checked profile. CE reports analyze validation observations only; CE diffs are diagnostics, not automatic performance gates.";

fn load_run(path: &str) -> Result<runner::Report, String> {
    use std::io::Read;
    let mut bytes = vec![];
    fs::File::open(path)
        .map_err(|e| e.to_string())?
        .take(crate::manifest::MAX_ARTIFACT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() as u64 > crate::manifest::MAX_ARTIFACT_BYTES {
        return Err("run report exceeds artifact budget".into());
    }
    serde_json::from_slice(&bytes).map_err(|e| e.to_string())
}

pub fn run(args: impl IntoIterator<Item = String>) -> Result<bool, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    let parse = |index: usize, default: u64| {
        args.get(index)
            .map_or(Ok(default), |v| v.parse::<u64>().map_err(|e| e.to_string()))
    };
    match args.first().map(String::as_str) {
        Some("--help") => {
            println!("{USAGE}");
            Ok(true)
        }
        Some("freeze-graph-stats") if args.len() == 2 || args.len() == 4 => {
            let frozen = Frozen::graph_statistics(
                parse(2, 7)?,
                usize::try_from(parse(3, 128)?).map_err(|e| e.to_string())?,
            )?;
            frozen.save(Path::new(&args[1]))?;
            println!("{}", frozen.sha256);
            Ok(true)
        }
        Some("freeze-ce") if args.len() == 2 || args.len() == 4 => {
            let frozen = Frozen::cardinality(
                parse(2, 7)?,
                usize::try_from(parse(3, 128)?).map_err(|e| e.to_string())?,
            )?;
            frozen.save(Path::new(&args[1]))?;
            println!("{}", frozen.sha256);
            Ok(true)
        }
        Some("ce-report") if args.len() == 4 => {
            let frozen = Frozen::load(Path::new(&args[1]))?;
            let report = cardinality::analyze(&frozen, &load_run(&args[2])?)?;
            write_new(
                Path::new(&args[3]),
                &serde_json::to_vec(&report).map_err(|e| e.to_string())?,
            )?;
            println!(
                "{}",
                serde_json::to_string(&report.summary).map_err(|e| e.to_string())?
            );
            Ok(report
                .cases
                .iter()
                .all(|c| c.state == cardinality::CaseState::Observed))
        }
        Some("ce-diff") if args.len() == 5 => {
            let frozen = Frozen::load(Path::new(&args[1]))?;
            let report = cardinality::compare(&frozen, &load_run(&args[2])?, &load_run(&args[3])?)?;
            write_new(
                Path::new(&args[4]),
                &serde_json::to_vec(&report).map_err(|e| e.to_string())?,
            )?;
            println!(
                "{}",
                serde_json::json!({"cases":report.case_counts,"operators":report.operator_counts})
            );
            Ok(true)
        }
        Some("freeze" | "freeze-cost") if args.len() == 2 || args.len() == 5 => {
            let cost = args[0] == "freeze-cost";
            let frozen = Frozen::generate(
                parse(2, 7)?,
                usize::try_from(parse(3, 24)?).map_err(|e| e.to_string())?,
                usize::try_from(parse(4, if cost { 1024 } else { 128 })?)
                    .map_err(|e| e.to_string())?,
                if cost {
                    Limits::cost_baseline()
                } else {
                    Limits::default()
                },
            )?;
            frozen.save(Path::new(&args[1]))?;
            println!("{}", frozen.sha256);
            Ok(true)
        }
        Some("compare" | "cost" | "cost-check" | "ce-run") if args.len() == 3 => {
            if args[0] == "cost" && cfg!(debug_assertions) {
                return Err("cost requires the repository release profile; use --release or cost-check for checked-profile validation".into());
            }
            if Path::new(&args[2]).exists() {
                return Err("report path already exists; choose a fresh path".into());
            }
            let frozen = Frozen::load(Path::new(&args[1]))?;
            if args[0] == "ce-run"
                && frozen.manifest.protocol != query_workloads::cardinality::PROTOCOL
                && frozen.manifest.protocol != query_workloads::compressed_statistics::PROTOCOL
            {
                return Err("ce-run requires a freeze-ce or freeze-graph-stats manifest".into());
            }
            let progress_path = format!("{}.progress.jsonl", args[2]);
            let mut progress = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&progress_path)
                .map_err(|e| format!("{progress_path}: {e}"))?;
            let report = runner::run_with_progress(
                &frozen,
                matches!(args[0].as_str(), "cost" | "cost-check"),
                |event| {
                    serde_json::to_writer(&mut progress, &event).map_err(|e| e.to_string())?;
                    progress.write_all(b"\n").map_err(|e| e.to_string())?;
                    progress.flush().map_err(|e| e.to_string())
                },
            )?;
            let success = report.fixture_error.is_none()
                && report
                    .attempts
                    .iter()
                    .all(|a| a.observation.status == runner::Status::Completed);
            write_new(
                Path::new(&args[2]),
                &serde_json::to_vec(&report).map_err(|e| e.to_string())?,
            )?;
            println!("{}", report.summary);
            Ok(success)
        }
        Some("replay") if args.len() == 3 => {
            let frozen = Frozen::load(Path::new(&args[1]))?;
            let observation = runner::replay(
                &frozen,
                usize::try_from(parse(2, 0)?).map_err(|e| e.to_string())?,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&observation).map_err(|e| e.to_string())?
            );
            Ok(observation.status == runner::Status::Completed)
        }
        _ => Err(USAGE.into()),
    }
}
