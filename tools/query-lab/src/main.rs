// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

fn main() -> std::process::ExitCode {
    match query_lab::cli::run(std::env::args().skip(1)) {
        Ok(true) => std::process::ExitCode::SUCCESS,
        Ok(false) => std::process::ExitCode::FAILURE,
        Err(error) => {
            eprintln!("query-lab: {error}");
            std::process::ExitCode::from(2)
        }
    }
}
