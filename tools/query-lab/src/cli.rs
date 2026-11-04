// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use crate::{
    manifest::{write_new, Frozen},
    runner,
};
use query_workloads::smith::Limits;
use std::{fs, io::Write, path::Path};

pub const USAGE: &str = "query-lab freeze PATH [SEED CASES ROWS]\nquery-lab freeze-cost PATH [SEED CASES ROWS]\nquery-lab compare MANIFEST REPORT\nquery-lab cost MANIFEST REPORT\nquery-lab cost-check MANIFEST REPORT\nquery-lab replay MANIFEST CELL\nAll commands are manual. Cost requires a release build and collects a frozen ordinary-plan baseline, not qualified alternative-plan regret. Cost-check exercises sampling under the checked profile.";

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
        Some("compare" | "cost" | "cost-check") if args.len() == 3 => {
            if args[0] == "cost" && cfg!(debug_assertions) {
                return Err("cost requires the repository release profile; use --release or cost-check for checked-profile validation".into());
            }
            if Path::new(&args[2]).exists() {
                return Err("report path already exists; choose a fresh path".into());
            }
            let frozen = Frozen::load(Path::new(&args[1]))?;
            let progress_path = format!("{}.progress.jsonl", args[2]);
            let mut progress = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&progress_path)
                .map_err(|e| format!("{progress_path}: {e}"))?;
            let report = runner::run_with_progress(&frozen, args[0] != "compare", |event| {
                serde_json::to_writer(&mut progress, &event).map_err(|e| e.to_string())?;
                progress.write_all(b"\n").map_err(|e| e.to_string())?;
                progress.flush().map_err(|e| e.to_string())
            })?;
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
