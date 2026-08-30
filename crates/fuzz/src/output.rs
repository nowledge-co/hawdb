use serde_json::Value as JsonValue;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub const DEFAULT_FUZZ_LOG_DIRECTORY: &str = "target/fuzz-logs";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzReportPaths {
    pub current: PathBuf,
    pub failure: Option<PathBuf>,
}

pub fn emit_fuzz_report(
    log_directory: &Path,
    campaign: &str,
    run_id: &str,
    report: &JsonValue,
    success: bool,
    print_report: bool,
    stdout: &mut impl Write,
) -> Result<FuzzReportPaths, String> {
    fs::create_dir_all(log_directory).map_err(|error| {
        format!(
            "failed to create fuzz log directory '{}': {error}",
            log_directory.display()
        )
    })?;

    let encoded = serde_json::to_string_pretty(report)
        .map(|mut encoded| {
            encoded.push('\n');
            encoded
        })
        .map_err(|error| format!("failed to encode fuzz report: {error}"))?;
    let stem = format!("{campaign}-{run_id}");
    let current = log_directory.join(format!("{stem}-cur.json"));
    write_report_file(&current, &encoded)?;

    let failure = if success {
        None
    } else {
        let failure = log_directory.join(format!("{stem}-failure.json"));
        write_report_file(&failure, &encoded)?;
        Some(failure)
    };

    if print_report {
        stdout
            .write_all(encoded.as_bytes())
            .map_err(|error| format!("failed to write fuzz report to stdout: {error}"))?;
    }

    Ok(FuzzReportPaths { current, failure })
}

fn write_report_file(path: &Path, encoded: &str) -> Result<(), String> {
    fs::write(path, encoded)
        .map_err(|error| format!("failed to write fuzz report '{}': {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn success_writes_only_the_current_report() {
        let directory = unique_directory("success");
        let mut stdout = Vec::new();
        let paths = emit_fuzz_report(
            &directory,
            "skein-test-fuzz",
            "seed-7-cases-8",
            &json!({"success": true}),
            true,
            false,
            &mut stdout,
        )
        .unwrap();

        assert_eq!(
            paths.current,
            directory.join("skein-test-fuzz-seed-7-cases-8-cur.json")
        );
        assert_eq!(paths.failure, None);
        assert_eq!(
            fs::read_to_string(&paths.current).unwrap(),
            "{\n  \"success\": true\n}\n"
        );
        assert!(stdout.is_empty());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn failure_preserves_a_reproduction_report() {
        let directory = unique_directory("failure");
        let report = json!({"success": false, "seed": 7});
        let mut stdout = Vec::new();
        let paths = emit_fuzz_report(
            &directory,
            "skein-test-fuzz",
            "seed-7-case-3",
            &report,
            false,
            false,
            &mut stdout,
        )
        .unwrap();
        let failure = paths.failure.unwrap();

        assert_eq!(
            failure,
            directory.join("skein-test-fuzz-seed-7-case-3-failure.json")
        );
        assert_eq!(
            fs::read_to_string(paths.current).unwrap(),
            fs::read_to_string(&failure).unwrap()
        );
        assert!(stdout.is_empty());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn explicit_report_output_writes_json_to_stdout() {
        let directory = unique_directory("stdout");
        let mut stdout = Vec::new();
        emit_fuzz_report(
            &directory,
            "skein-test-fuzz",
            "seed-7-cases-8",
            &json!({"success": true}),
            true,
            true,
            &mut stdout,
        )
        .unwrap();

        assert_eq!(stdout, b"{\n  \"success\": true\n}\n");
        fs::remove_dir_all(directory).unwrap();
    }

    fn unique_directory(name: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein-fuzz-output-{name}-{}-{timestamp}",
            std::process::id()
        ))
    }
}
