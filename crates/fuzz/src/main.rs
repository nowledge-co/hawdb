use skein_fuzz::{
    emit_fuzz_report, run_campaign, CampaignOptions, FuzzError, DEFAULT_FUZZ_LOG_DIRECTORY,
};
use std::io;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    match run() {
        Ok(success) => {
            if success {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(error) => {
            eprintln!("skein-fuzz: {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<bool, FuzzError> {
    let options = parse_options(std::env::args().skip(1))?;
    let report = run_campaign(options.campaign)?;
    let success = report.success();
    let run_id = run_id(options.campaign);
    let paths = emit_fuzz_report(
        &options.log_directory,
        "skein-optimizer-fuzz",
        &run_id,
        &report.json(),
        success,
        options.print_report,
        &mut io::stdout().lock(),
    )
    .map_err(FuzzError::new)?;
    if let Some(path) = paths.failure {
        eprintln!(
            "skein-fuzz: {} failing case(s); reproduction report: {}",
            report.failed_case_count,
            path.display()
        );
    }
    Ok(success)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Options {
    campaign: CampaignOptions,
    log_directory: PathBuf,
    print_report: bool,
}

fn parse_options(args: impl IntoIterator<Item = String>) -> Result<Options, FuzzError> {
    let mut campaign = CampaignOptions::default();
    let mut log_directory = PathBuf::from(DEFAULT_FUZZ_LOG_DIRECTORY);
    let mut print_report = false;
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
                let value = args.next().ok_or_else(|| FuzzError::new(usage()))?;
                campaign.case_count = value
                    .parse()
                    .map_err(|_| FuzzError::new("--cases must be a non-negative integer"))?;
            }
            "--case-index" => {
                let value = args.next().ok_or_else(|| FuzzError::new(usage()))?;
                campaign.case_index =
                    Some(value.parse().map_err(|_| {
                        FuzzError::new("--case-index must be a non-negative integer")
                    })?);
            }
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
    Ok(Options {
        campaign,
        log_directory,
        print_report,
    })
}

fn usage() -> &'static str {
    "usage: skein-fuzz [--seed <u64>] [--cases <usize>] [--case-index <usize>] [--log-directory <path>] [--print-report]"
}

fn run_id(options: CampaignOptions) -> String {
    options.case_index.map_or_else(
        || format!("seed-{}-cases-{}", options.seed, options.case_count),
        |index| format!("seed-{}-case-{index}", options.seed),
    )
}

#[cfg(test)]
mod tests {
    use super::parse_options;

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
            std::path::Path::new(skein_fuzz::DEFAULT_FUZZ_LOG_DIRECTORY)
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
            "/tmp/skein-fuzz".to_string(),
            "--print-report".to_string(),
        ])
        .unwrap();

        assert_eq!(
            options.log_directory,
            std::path::Path::new("/tmp/skein-fuzz")
        );
        assert!(options.print_report);
    }
}
