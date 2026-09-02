use super::fixture::{corpus_statement, thread_message_parameters, thread_page_parameters};
use super::{
    ContentStoreProcessResourceEvidence, ContentStoreResourceEvidence,
    ContentStoreResourceProfileKind, ContentStoreRuntimeMemoryEvidence,
};
use crate::evidence_digest::rows_sha256;
use crate::{elapsed_micros, latency_percentiles, ContentStoreSqlCorpus};
use skein::{
    Database, ProcessMemoryProfile, ProcessMemorySnapshot, QueryStreamOptions, Result,
    RuntimeMemorySnapshot, SkeinError, Value,
};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

pub(super) struct ContentStoreResourceProbeConfig<'a> {
    pub(super) profile_kind: ContentStoreResourceProfileKind,
    pub(super) configured_available_memory_bytes: u64,
    pub(super) read_samples: usize,
    pub(super) database_path: &'a Path,
    pub(super) database_config: &'a skein::DatabaseConfig,
    pub(super) message_position: usize,
    pub(super) message_payload_bytes: usize,
}

pub(super) fn qualify_content_store_resources(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    config: ContentStoreResourceProbeConfig<'_>,
) -> Result<ContentStoreResourceEvidence> {
    // Publish all earlier correctness phases first so the measured write
    // amplification belongs only to the controlled mutation below.
    database.checkpoint()?;
    let process_start = ProcessMemorySnapshot::capture()?;
    let runtime_memory = RuntimeMemorySnapshot::detect();
    let probe_started = Instant::now();
    let page = corpus_statement(corpus, "thread_messages_page")?;
    let page_parameters = thread_page_parameters(config.message_position + 1);
    let page_options = QueryStreamOptions {
        max_rows: Some(page.max_rows),
        max_payload_bytes: Some(page.max_payload_bytes),
    };
    let mut read_latency_micros = Vec::with_capacity(config.read_samples);
    let mut expected_sha256 = None;
    let mut output_rows = 0usize;
    let mut output_payload_bytes = 0usize;
    for _ in 0..config.read_samples {
        let started = Instant::now();
        let output =
            database.query_sql_with_params_options(&page.sql, &page_parameters, page_options)?;
        read_latency_micros.push(elapsed_micros(started));
        if output.rows.len() != config.message_position + 1 {
            return Err(SkeinError::Execution(format!(
                "content-store resource probe returned {} rows, expected {}",
                output.rows.len(),
                config.message_position + 1
            )));
        }
        let output_sha256 = rows_sha256(&output.rows);
        if expected_sha256
            .as_ref()
            .is_some_and(|expected| expected != &output_sha256)
        {
            return Err(SkeinError::Execution(
                "content-store resource probe returned unstable read results".to_string(),
            ));
        }
        expected_sha256 = Some(output_sha256);
        output_rows = output.rows.len();
        output_payload_bytes = output.payload_bytes();
    }

    let files_before = regular_file_bytes_by_name(config.database_path)?;
    let wal_before = database.storage_pressure_snapshot().wal_bytes;
    let message = corpus_statement(corpus, "upsert_thread_message")?;
    let mutation_parameters = thread_message_parameters(
        config.message_position,
        config.message_payload_bytes,
        "resource",
    );
    let logical_mutation_bytes = values_payload_bytes(&mutation_parameters);
    let mutation_started = Instant::now();
    database.query_sql_with_params(&message.sql, &mutation_parameters)?;
    let mutation_latency_micros = elapsed_micros(mutation_started);
    let wal_after = database.storage_pressure_snapshot().wal_bytes;
    let wal_append_bytes = wal_after.checked_sub(wal_before).ok_or_else(|| {
        SkeinError::Execution(format!(
            "content-store resource probe observed WAL bytes decrease from {wal_before} to {wal_after} before checkpoint"
        ))
    })?;
    if wal_append_bytes == 0 {
        return Err(SkeinError::Execution(
            "content-store resource probe mutation wrote no WAL bytes".to_string(),
        ));
    }

    let checkpoint_started = Instant::now();
    database.checkpoint()?;
    let checkpoint_latency_micros = elapsed_micros(checkpoint_started);
    let files_after = regular_file_bytes_by_name(config.database_path)?;
    let new_generation_artifact_bytes = files_after
        .iter()
        .filter(|(name, _)| !files_before.contains_key(*name))
        .fold(0u64, |total, (_, bytes)| total.saturating_add(*bytes));
    if new_generation_artifact_bytes == 0 {
        return Err(SkeinError::Execution(
            "content-store resource probe checkpoint published no new generation artifacts"
                .to_string(),
        ));
    }
    let durable_write_bytes_lower_bound =
        wal_append_bytes.saturating_add(new_generation_artifact_bytes);
    let process_end = ProcessMemorySnapshot::capture()?;
    let process = ProcessMemoryProfile::between(process_start, process_end);
    let observed_peak_within_configured_profile =
        process.peak_resident_bytes <= config.configured_available_memory_bytes;

    Ok(ContentStoreResourceEvidence {
        profile_kind: config.profile_kind,
        configured_available_memory_bytes: config.configured_available_memory_bytes,
        segment_cache_capacity_bytes: config.database_config.segment_cache_capacity_bytes,
        max_relational_index_read_bytes: config
            .database_config
            .max_relational_index_read_bytes
            .get(),
        max_relational_hydration_bytes: config.database_config.max_relational_hydration_bytes.get(),
        max_read_result_rows: config.database_config.max_read_result_rows,
        max_read_result_payload_bytes: config.database_config.max_read_result_payload_bytes,
        execution_batch_rows: config.database_config.execution_memory.batch_rows.get(),
        execution_batch_payload_bytes: config
            .database_config
            .execution_memory
            .batch_payload_bytes
            .get(),
        blocking_operator_bytes: config
            .database_config
            .execution_memory
            .blocking_operator_bytes
            .get(),
        max_wal_replay_bytes: config.database_config.max_wal_replay_bytes,
        max_out_of_core_delta_bytes: config.database_config.max_out_of_core_delta_bytes,
        read_samples: config.read_samples,
        read_latency: latency_percentiles(&read_latency_micros),
        output_rows,
        output_payload_bytes,
        output_sha256: expected_sha256.unwrap_or_default(),
        mutation_latency_micros,
        checkpoint_latency_micros,
        probe_latency_micros: elapsed_micros(probe_started),
        logical_mutation_bytes,
        wal_append_bytes,
        new_generation_artifact_bytes,
        durable_write_bytes_lower_bound,
        durable_write_amplification_lower_bound_per_million: ratio_per_million(
            durable_write_bytes_lower_bound,
            logical_mutation_bytes,
        ),
        write_measurement_scope: "wal_append_plus_new_generation_artifacts".to_string(),
        process: process_evidence(process),
        runtime_memory: runtime_memory_evidence(runtime_memory),
        observed_peak_within_configured_profile,
    })
}

fn values_payload_bytes(values: &[Value]) -> u64 {
    values.iter().fold(0u64, |total, value| {
        total.saturating_add(value_payload_bytes(value))
    })
}

fn value_payload_bytes(value: &Value) -> u64 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) | Value::Float(_) => 8,
        Value::String(value) => value.len() as u64,
        Value::Binary(value) => value.len() as u64,
        Value::Uuid(_) => 16,
        Value::List(values) => values_payload_bytes(values),
        Value::Map(values) => values.iter().fold(0u64, |total, (key, value)| {
            total
                .saturating_add(key.len() as u64)
                .saturating_add(value_payload_bytes(value))
        }),
    }
}

pub(super) fn regular_file_bytes_by_name(path: &Path) -> Result<BTreeMap<String, u64>> {
    let mut files = BTreeMap::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            let name = entry.file_name().into_string().map_err(|_| {
                SkeinError::Execution(
                    "content-store resource probe found a non-UTF-8 artifact name".to_string(),
                )
            })?;
            files.insert(name, metadata.len());
        }
    }
    Ok(files)
}

pub(super) fn process_evidence(
    profile: ProcessMemoryProfile,
) -> ContentStoreProcessResourceEvidence {
    ContentStoreProcessResourceEvidence {
        resident_memory_supported: profile.capabilities.resident_memory,
        total_page_faults_supported: profile.capabilities.total_page_faults,
        split_page_faults_supported: profile.capabilities.split_page_faults,
        start_resident_bytes: profile.start_resident_bytes,
        steady_resident_bytes: profile.steady_resident_bytes,
        peak_resident_bytes: profile.peak_resident_bytes,
        steady_resident_growth_bytes: profile.steady_resident_growth_bytes,
        lifetime_peak_resident_growth_bytes: profile.lifetime_peak_resident_growth_bytes,
        total_page_faults: profile.total_page_faults,
        minor_page_faults: profile.minor_page_faults,
        major_page_faults: profile.major_page_faults,
    }
}

pub(super) fn runtime_memory_evidence(
    snapshot: RuntimeMemorySnapshot,
) -> ContentStoreRuntimeMemoryEvidence {
    ContentStoreRuntimeMemoryEvidence {
        host_total_bytes: snapshot.host_total_bytes,
        host_available_bytes: snapshot.host_available_bytes,
        cgroup_limit_bytes: snapshot.cgroup_limit_bytes,
        cgroup_high_bytes: snapshot.cgroup_high_bytes,
        cgroup_current_bytes: snapshot.cgroup_current_bytes,
        effective_limit_bytes: snapshot.effective_limit_bytes,
        effective_available_bytes: snapshot.effective_available_bytes,
        pressure: snapshot.pressure.as_str().to_string(),
    }
}

fn ratio_per_million(numerator: u64, denominator: u64) -> u64 {
    u64::try_from(u128::from(numerator).saturating_mul(1_000_000) / u128::from(denominator.max(1)))
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein::Uuid;

    #[test]
    fn uuid_payload_size_is_fixed_width() {
        let uuid = Uuid::parse_str("0198f7c9-64a1-7d6a-8e67-5df1dcb3e319").unwrap();

        assert_eq!(value_payload_bytes(&Value::Uuid(uuid)), 16);
    }
}
