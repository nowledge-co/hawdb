use serde_json::Value as JsonValue;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const DEFAULT_FUZZ_LOG_DIRECTORY: &str = "target/fuzz-logs";
pub const DEFAULT_FUZZ_PROGRESS_INTERVAL: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzReportPaths {
    pub current: PathBuf,
    pub failure: Option<PathBuf>,
}

pub fn fuzz_current_report_path(log_directory: &Path, campaign: &str, run_id: &str) -> PathBuf {
    log_directory.join(format!("{campaign}-{run_id}-cur.json"))
}

pub fn read_fuzz_current_report(path: &Path) -> Result<Option<JsonValue>, String> {
    let encoded = match fs::read_to_string(path) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "failed to read fuzz report '{}': {error}",
                path.display()
            ));
        }
    };
    serde_json::from_str(&encoded)
        .map(Some)
        .map_err(|error| format!("failed to decode fuzz report '{}': {error}", path.display()))
}

pub fn write_fuzz_current_report(path: &Path, report: &JsonValue) -> Result<(), String> {
    let encoded = encode_report(report)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to create fuzz log directory '{}': {error}",
                parent.display()
            )
        })?;
    }
    write_report_file(path, &encoded)
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

    let encoded = encode_report(report)?;
    let stem = format!("{campaign}-{run_id}");
    let current = fuzz_current_report_path(log_directory, campaign, run_id);
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

fn encode_report(report: &JsonValue) -> Result<String, String> {
    serde_json::to_string_pretty(report)
        .map(|mut encoded| {
            encoded.push('\n');
            encoded
        })
        .map_err(|error| format!("failed to encode fuzz report: {error}"))
}

fn write_report_file(path: &Path, encoded: &str) -> Result<(), String> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temporary = path.with_extension(format!("tmp-{}-{timestamp}", std::process::id()));
    fs::write(&temporary, encoded).map_err(|error| {
        format!(
            "failed to write temporary fuzz report '{}': {error}",
            temporary.display()
        )
    })?;
    match fs::rename(&temporary, path) {
        Ok(()) => Ok(()),
        Err(error) if cfg!(windows) && path.exists() => {
            fs::remove_file(path).map_err(|remove_error| {
                format!(
                    "failed to replace fuzz report '{}': {remove_error}",
                    path.display()
                )
            })?;
            fs::rename(&temporary, path).map_err(|rename_error| {
                format!(
                    "failed to publish fuzz report '{}' after replace error {error}: {rename_error}",
                    path.display()
                )
            })
        }
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            Err(format!(
                "failed to publish fuzz report '{}': {error}",
                path.display()
            ))
        }
    }
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

    #[test]
    fn current_report_refresh_is_readable_and_replaces_previous_state() {
        let directory = unique_directory("refresh");
        let current = fuzz_current_report_path(&directory, "skein-test-fuzz", "seed-7");

        write_fuzz_current_report(&current, &json!({"current_case_index": 3})).unwrap();
        assert_eq!(
            read_fuzz_current_report(&current).unwrap().unwrap(),
            json!({"current_case_index": 3})
        );
        write_fuzz_current_report(&current, &json!({"current_case_index": 19})).unwrap();
        assert_eq!(
            read_fuzz_current_report(&current).unwrap().unwrap(),
            json!({"current_case_index": 19})
        );

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
