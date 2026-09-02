use skein::ProductionQualificationIdentity;
use skein_qualification::{
    evaluate_production_release_qualification_bundle, ProductionReleaseQualificationArtifacts,
    ProductionReleaseQualificationPolicy, PRODUCTION_RELEASE_QUALIFICATION_BUNDLE_PROTOCOL,
};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const MAX_ARTIFACT_BYTES: u64 = 32 * 1024 * 1024;

fn main() -> ExitCode {
    match run(std::env::args().skip(1)) {
        Ok(Some(report)) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report.json())
                    .expect("release qualification report must serialize")
            );
            if report.ready {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Ok(None) => {
            println!("{}", usage());
            ExitCode::SUCCESS
        }
        Err(error) => {
            println!(
                "{}",
                serde_json::json!({
                    "protocol": PRODUCTION_RELEASE_QUALIFICATION_BUNDLE_PROTOCOL,
                    "production_eligible": true,
                    "ready": false,
                    "blocker_codes": ["bundle_input_invalid"],
                    "errors": [error],
                })
            );
            eprintln!("skein-qualification-bundle: {error}");
            ExitCode::from(2)
        }
    }
}

fn run(
    args: impl IntoIterator<Item = String>,
) -> Result<Option<skein_qualification::ProductionReleaseQualificationBundleReport>, String> {
    let Some(config) = parse_config(args)? else {
        return Ok(None);
    };
    let expected_identity =
        read_json::<ProductionQualificationIdentity>(&config.expected_identity)?;
    let artifacts = ProductionReleaseQualificationArtifacts {
        content_store_memory_profiles: Some(read_value(&config.content_store_memory_profiles)?),
        content_store_read: Some(read_value(&config.content_store_read)?),
        content_store_512_mib_read: Some(read_value(&config.content_store_512_mib_read)?),
        content_store_512_mib_overflow_compaction: Some(read_value(
            &config.content_store_512_mib_overflow_compaction,
        )?),
        content_store_desktop_overflow_compaction: Some(read_value(
            &config.content_store_desktop_overflow_compaction,
        )?),
        content_store_mutation_matrix: Some(read_value(&config.content_store_mutation_matrix)?),
        graph_storage: Some(read_value(&config.graph)?),
        graph_index_matrix: Some(read_value(&config.graph_index_matrix)?),
        search: Some(read_value(&config.search)?),
        vector_targets: config
            .vectors
            .iter()
            .map(|path| read_value(path))
            .collect::<Result<Vec<_>, _>>()?,
        morsel_profiles: config
            .morsels
            .iter()
            .map(|path| read_value(path))
            .collect::<Result<Vec<_>, _>>()?,
        blocking_operators: Some(read_value(&config.blocking)?),
        storage_crash_recovery: Some(read_value(&config.storage_crash_recovery)?),
        release_controls: Some(read_value(&config.release_controls)?),
    };
    Ok(Some(evaluate_production_release_qualification_bundle(
        artifacts,
        expected_identity,
        config.policy,
    )))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Config {
    expected_identity: PathBuf,
    content_store_memory_profiles: PathBuf,
    content_store_read: PathBuf,
    content_store_512_mib_read: PathBuf,
    content_store_512_mib_overflow_compaction: PathBuf,
    content_store_desktop_overflow_compaction: PathBuf,
    content_store_mutation_matrix: PathBuf,
    graph: PathBuf,
    graph_index_matrix: PathBuf,
    search: PathBuf,
    vectors: Vec<PathBuf>,
    morsels: Vec<PathBuf>,
    blocking: PathBuf,
    storage_crash_recovery: PathBuf,
    release_controls: PathBuf,
    policy: ProductionReleaseQualificationPolicy,
}

fn parse_config(args: impl IntoIterator<Item = String>) -> Result<Option<Config>, String> {
    let mut expected_identity = None;
    let mut content_store_memory_profiles = None;
    let mut content_store_read = None;
    let mut content_store_512_mib_read = None;
    let mut content_store_512_mib_overflow_compaction = None;
    let mut content_store_desktop_overflow_compaction = None;
    let mut content_store_mutation_matrix = None;
    let mut graph = None;
    let mut graph_index_matrix = None;
    let mut search = None;
    let mut vectors = Vec::new();
    let mut morsels = Vec::new();
    let mut blocking = None;
    let mut storage_crash_recovery = None;
    let mut release_controls = None;
    let mut policy = ProductionReleaseQualificationPolicy::default();
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        if matches!(argument.as_str(), "--help" | "-h") {
            return Ok(None);
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {argument}"))?;
        match argument.as_str() {
            "--expected-identity-json" => expected_identity = Some(PathBuf::from(value)),
            "--content-store-memory-profiles-json" => {
                content_store_memory_profiles = Some(PathBuf::from(value));
            }
            "--content-store-read-json" => content_store_read = Some(PathBuf::from(value)),
            "--content-store-512-mib-read-json" => {
                content_store_512_mib_read = Some(PathBuf::from(value));
            }
            "--content-store-512-mib-overflow-compaction-json" => {
                content_store_512_mib_overflow_compaction = Some(PathBuf::from(value));
            }
            "--content-store-desktop-overflow-compaction-json" => {
                content_store_desktop_overflow_compaction = Some(PathBuf::from(value));
            }
            "--content-store-mutation-matrix-json" => {
                content_store_mutation_matrix = Some(PathBuf::from(value));
            }
            "--graph-json" => graph = Some(PathBuf::from(value)),
            "--graph-index-matrix-json" => graph_index_matrix = Some(PathBuf::from(value)),
            "--search-json" => search = Some(PathBuf::from(value)),
            "--vector-json" => vectors.push(PathBuf::from(value)),
            "--morsel-json" => morsels.push(PathBuf::from(value)),
            "--blocking-json" => blocking = Some(PathBuf::from(value)),
            "--storage-crash-recovery-json" => storage_crash_recovery = Some(PathBuf::from(value)),
            "--release-controls-json" => release_controls = Some(PathBuf::from(value)),
            "--min-throughput-gain-per-million" => {
                policy.morsel.min_throughput_gain_per_million = parse_u32(&argument, &value)?;
            }
            "--max-p99-regression-per-million" => {
                policy.morsel.max_p99_regression_per_million = parse_u32(&argument, &value)?;
            }
            "--max-peak-rss-regression-per-million" => {
                policy.morsel.max_peak_rss_regression_per_million = parse_u32(&argument, &value)?;
            }
            "--max-cancellation-latency-micros" => {
                policy.morsel.max_cancellation_latency_micros = parse_u64(&argument, &value)?;
            }
            "--require-rabitq-reference-verification" => {
                policy.require_rabitq_reference_verification = parse_bool(&argument, &value)?;
            }
            _ => return Err(format!("unknown argument '{argument}'\n{}", usage())),
        }
    }
    Ok(Some(Config {
        expected_identity: required(expected_identity, "--expected-identity-json")?,
        content_store_memory_profiles: required(
            content_store_memory_profiles,
            "--content-store-memory-profiles-json",
        )?,
        content_store_read: required(content_store_read, "--content-store-read-json")?,
        content_store_512_mib_read: required(
            content_store_512_mib_read,
            "--content-store-512-mib-read-json",
        )?,
        content_store_512_mib_overflow_compaction: required(
            content_store_512_mib_overflow_compaction,
            "--content-store-512-mib-overflow-compaction-json",
        )?,
        content_store_desktop_overflow_compaction: required(
            content_store_desktop_overflow_compaction,
            "--content-store-desktop-overflow-compaction-json",
        )?,
        content_store_mutation_matrix: required(
            content_store_mutation_matrix,
            "--content-store-mutation-matrix-json",
        )?,
        graph: required(graph, "--graph-json")?,
        graph_index_matrix: required(graph_index_matrix, "--graph-index-matrix-json")?,
        search: required(search, "--search-json")?,
        vectors,
        morsels,
        blocking: required(blocking, "--blocking-json")?,
        storage_crash_recovery: required(storage_crash_recovery, "--storage-crash-recovery-json")?,
        release_controls: required(release_controls, "--release-controls-json")?,
        policy,
    }))
}

fn required<T>(value: Option<T>, name: &str) -> Result<T, String> {
    value.ok_or_else(|| format!("{name} is required"))
}

fn read_value(path: &Path) -> Result<serde_json::Value, String> {
    read_json(path)
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    let file =
        File::open(path).map_err(|error| format!("failed to open {}: {error}", path.display()))?;
    let mut bytes = Vec::new();
    file.take(MAX_ARTIFACT_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_ARTIFACT_BYTES {
        return Err(format!(
            "qualification artifact exceeds {MAX_ARTIFACT_BYTES} bytes: {}",
            path.display()
        ));
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid qualification JSON {}: {error}", path.display()))
}

fn parse_u32(name: &str, value: &str) -> Result<u32, String> {
    value
        .parse()
        .map_err(|_| format!("{name} must be an unsigned 32-bit integer"))
}

fn parse_u64(name: &str, value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|_| format!("{name} must be an unsigned 64-bit integer"))
}

fn parse_bool(name: &str, value: &str) -> Result<bool, String> {
    value
        .parse()
        .map_err(|_| format!("{name} must be true or false"))
}

fn usage() -> &'static str {
    "usage: skein-qualification-bundle \
     --expected-identity-json <path> --content-store-memory-profiles-json <path> \
     --content-store-read-json <path> --content-store-512-mib-read-json <path> \
     --content-store-512-mib-overflow-compaction-json <path> \
     --content-store-desktop-overflow-compaction-json <path> \
     --content-store-mutation-matrix-json <path> --graph-json <path> \
     --graph-index-matrix-json <path> --search-json <path> \
     --vector-json <path>... --morsel-json <path>... --blocking-json <path> \
     --storage-crash-recovery-json <path> --release-controls-json <path> \
     [--min-throughput-gain-per-million <u32>] \
     [--max-p99-regression-per-million <u32>] \
     [--max-peak-rss-regression-per-million <u32>] \
     [--max-cancellation-latency-micros <u64>] \
     [--require-rabitq-reference-verification <bool>]"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_repeated_cross_process_artifacts() {
        let config = parse_config([
            "--expected-identity-json".to_string(),
            "identity.json".to_string(),
            "--content-store-memory-profiles-json".to_string(),
            "content-store-memory-profiles.json".to_string(),
            "--content-store-read-json".to_string(),
            "content-store-read.json".to_string(),
            "--content-store-512-mib-read-json".to_string(),
            "content-store-512-mib-read.json".to_string(),
            "--content-store-512-mib-overflow-compaction-json".to_string(),
            "content-store-512-mib-overflow-compaction.json".to_string(),
            "--content-store-desktop-overflow-compaction-json".to_string(),
            "content-store-desktop-overflow-compaction.json".to_string(),
            "--content-store-mutation-matrix-json".to_string(),
            "content-store-mutation.json".to_string(),
            "--graph-json".to_string(),
            "graph.json".to_string(),
            "--graph-index-matrix-json".to_string(),
            "graph-index-matrix.json".to_string(),
            "--search-json".to_string(),
            "search.json".to_string(),
            "--vector-json".to_string(),
            "vector-linux.json".to_string(),
            "--vector-json".to_string(),
            "vector-macos.json".to_string(),
            "--morsel-json".to_string(),
            "morsel-4.json".to_string(),
            "--blocking-json".to_string(),
            "blocking.json".to_string(),
            "--storage-crash-recovery-json".to_string(),
            "crash.json".to_string(),
            "--release-controls-json".to_string(),
            "controls.json".to_string(),
        ])
        .unwrap()
        .unwrap();

        assert_eq!(config.vectors.len(), 2);
        assert_eq!(config.morsels.len(), 1);
        assert_eq!(
            config.content_store_memory_profiles,
            PathBuf::from("content-store-memory-profiles.json")
        );
        assert_eq!(
            config.content_store_read,
            PathBuf::from("content-store-read.json")
        );
        assert_eq!(
            config.content_store_512_mib_read,
            PathBuf::from("content-store-512-mib-read.json")
        );
        assert_eq!(
            config.content_store_512_mib_overflow_compaction,
            PathBuf::from("content-store-512-mib-overflow-compaction.json")
        );
        assert_eq!(
            config.content_store_desktop_overflow_compaction,
            PathBuf::from("content-store-desktop-overflow-compaction.json")
        );
        assert_eq!(
            config.content_store_mutation_matrix,
            PathBuf::from("content-store-mutation.json")
        );
        assert_eq!(
            config.graph_index_matrix,
            PathBuf::from("graph-index-matrix.json")
        );
        assert_eq!(config.storage_crash_recovery, PathBuf::from("crash.json"));
        assert_eq!(config.release_controls, PathBuf::from("controls.json"));
        assert!(config.policy.require_rabitq_reference_verification);
    }
}
