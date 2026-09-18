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

use super::{
    elapsed_micros, execute_out_of_core, ProductionSearchLifecycleConfig,
    ProductionSearchLifecycleReport, ProductionSearchQualificationError, ProductionSearchQueryCase,
};
use hawdb::{
    ProcessMemoryProfile, ProcessMemorySnapshot, SearchOutOfCoreConfig,
    SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader, SearchProjectionDelta,
    SearchProjectionQualificationIdentity, SearchResultSet,
};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const OUT_OF_CORE_MANIFEST_FILE: &str = "search_projection.out_of_core.manifest.hawdb";
const RABITQ_ARTIFACT_PREFIX: &str = "search_rabitq.";
const RABITQ_ARTIFACT_SUFFIX: &str = ".hawdb";

pub(super) fn run_lifecycle_probes(
    config: &ProductionSearchLifecycleConfig,
    expected_projection_identity: &SearchProjectionQualificationIdentity,
    out_of_core_config: &SearchOutOfCoreConfig,
    compressed_vector_probe: &ProductionSearchQueryCase,
) -> Result<ProductionSearchLifecycleReport, ProductionSearchQualificationError> {
    let process_start =
        ProcessMemorySnapshot::capture().map_err(ProductionSearchQualificationError::from_error)?;
    let mut update_micros = Vec::with_capacity(config.replica_paths.len());
    let mut checkpoint_micros = Vec::with_capacity(config.replica_paths.len());
    let mut reopen_micros = Vec::with_capacity(config.replica_paths.len());
    let mut incremental_upsert_delete = true;
    let mut checkpoint_reopen = true;
    let mut stale_generation = true;
    let mut mixed_foreground_background = true;
    let mut checkpoint_write_amplification_per_million = 0;
    let mut bounded_generation_update = true;
    let mut max_update_resident_document_count = 0usize;
    let mut max_update_peak_segment_document_bytes = 0u64;
    let mut rabitq_serving = true;
    let mut rabitq_preferred_serving = true;
    let mut rabitq_raw_rerank = true;
    let mut rabitq_metadata_filter_pushdown = true;
    let mut rabitq_payload_bytes_read = 0u64;
    let logical_delta_bytes = delta_logical_bytes(&config.delta);

    for path in &config.replica_paths {
        let old_reader = SearchOutOfCoreReader::open_with_config(path, out_of_core_config.clone())
            .map_err(ProductionSearchQualificationError::from_error)?;
        require_projection_identity(&old_reader, expected_projection_identity)?;
        let old_generation = old_reader.generation();
        let upsert_before = execute_out_of_core(&old_reader, &config.upsert_verification)?;
        let delete_before = execute_out_of_core(&old_reader, &config.delete_verification)?;
        incremental_upsert_delete &=
            !contains_hit(&upsert_before.result, &config.expected_upsert_document_id)
                && contains_hit(&delete_before.result, &config.expected_deleted_document_id);

        let bytes_before = directory_regular_file_bytes(path)?;
        let update_started = Instant::now();
        let update = SearchOutOfCoreGenerationWriter::prepare_delta(
            &old_reader,
            config.delta.clone(),
            config.generation_build_options.clone(),
        )
        .map_err(ProductionSearchQualificationError::from_error)?;
        update_micros.push(elapsed_micros(update_started));
        bounded_generation_update &= update.delta_report().action == "bounded_generation_update";
        max_update_peak_segment_document_bytes = max_update_peak_segment_document_bytes
            .max(update.source_read_metrics().peak_segment_document_bytes);

        let barrier = Arc::new(Barrier::new(2));
        let worker_barrier = Arc::clone(&barrier);
        let worker_case = config.upsert_verification.clone();
        let worker_runs = config.mixed_load_probe_runs;
        let expected_old_digest = super::result_digest(&upsert_before.result);
        let worker = thread::spawn(move || {
            worker_barrier.wait();
            let mut succeeded = true;
            let mut stable = true;
            for _ in 0..worker_runs {
                match execute_out_of_core(&old_reader, &worker_case) {
                    Ok(output) => {
                        stable &= super::result_digest(&output.result) == expected_old_digest;
                    }
                    Err(_) => succeeded = false,
                }
            }
            (succeeded, stable)
        });
        barrier.wait();
        let checkpoint_started = Instant::now();
        let (_, build_report, _) = update
            .finish()
            .map_err(ProductionSearchQualificationError::from_error)?;
        checkpoint_micros.push(elapsed_micros(checkpoint_started));
        max_update_resident_document_count =
            max_update_resident_document_count.max(build_report.resident_document_count);
        bounded_generation_update &= build_report.resident_document_count == 0;
        let (worker_succeeded, worker_stable) = worker.join().map_err(|_| {
            ProductionSearchQualificationError::new(
                "production search mixed-load probe thread panicked",
            )
        })?;
        mixed_foreground_background &= worker_succeeded;
        stale_generation &= worker_stable;
        let bytes_after = directory_regular_file_bytes(path)?;
        checkpoint_write_amplification_per_million = checkpoint_write_amplification_per_million
            .max(ratio_per_million(
                bytes_after.saturating_sub(bytes_before),
                logical_delta_bytes,
            ));
        let reopen_started = Instant::now();
        let new_reader = SearchOutOfCoreReader::open_with_config(path, out_of_core_config.clone())
            .map_err(ProductionSearchQualificationError::from_error)?;
        reopen_micros.push(elapsed_micros(reopen_started));
        stale_generation &= new_reader.generation() > old_generation;
        let upsert_after = execute_out_of_core(&new_reader, &config.upsert_verification)?;
        let delete_after = execute_out_of_core(&new_reader, &config.delete_verification)?;
        incremental_upsert_delete &=
            contains_hit(&upsert_after.result, &config.expected_upsert_document_id)
                && !contains_hit(&delete_after.result, &config.expected_deleted_document_id);
        let compressed = super::run_rabitq_serving_probe(&new_reader, compressed_vector_probe)?;
        rabitq_serving &= compressed.required_serving;
        rabitq_preferred_serving &= compressed.preferred_serving;
        rabitq_raw_rerank &= compressed.raw_rerank;
        rabitq_metadata_filter_pushdown &= compressed.metadata_filter_pushdown;
        rabitq_payload_bytes_read =
            rabitq_payload_bytes_read.saturating_add(compressed.payload_bytes_read);
        let expected_upsert_digest = super::result_digest(&upsert_after.result);
        let expected_delete_digest = super::result_digest(&delete_after.result);
        drop(new_reader);
        let reopened = SearchOutOfCoreReader::open_with_config(path, out_of_core_config.clone())
            .map_err(ProductionSearchQualificationError::from_error)?;
        checkpoint_reopen &= super::result_digest(
            &execute_out_of_core(&reopened, &config.upsert_verification)?.result,
        ) == expected_upsert_digest
            && super::result_digest(
                &execute_out_of_core(&reopened, &config.delete_verification)?.result,
            ) == expected_delete_digest;
    }

    let rabitq_corruption_path = config
        .replica_paths
        .last()
        .expect("validated lifecycle paths are non-empty");
    let rabitq_reader =
        SearchOutOfCoreReader::open_with_config(rabitq_corruption_path, out_of_core_config.clone())
            .map_err(ProductionSearchQualificationError::from_error)?;
    let rabitq_generation = rabitq_reader.generation();
    let rabitq_attached = rabitq_reader
        .vector_projection_qualification_identity()
        .is_some();
    drop(rabitq_reader);

    let rabitq_path = rabitq_corruption_path.join(format!(
        "{RABITQ_ARTIFACT_PREFIX}{rabitq_generation}{RABITQ_ARTIFACT_SUFFIX}"
    ));
    flip_last_byte(&rabitq_path)?;
    let corrupt_rabitq_rejected =
        SearchOutOfCoreReader::open_with_config(rabitq_corruption_path, out_of_core_config.clone())
            .is_err();
    flip_last_byte(&rabitq_path)?;
    let rabitq_restored =
        SearchOutOfCoreReader::open_with_config(rabitq_corruption_path, out_of_core_config.clone())
            .is_ok();

    let corrupt_reader = SearchOutOfCoreReader::open_with_config(
        &config.corruption_replica_path,
        out_of_core_config.clone(),
    )
    .map_err(ProductionSearchQualificationError::from_error)?;
    require_projection_identity(&corrupt_reader, expected_projection_identity)?;
    drop(corrupt_reader);
    corrupt_out_of_core_manifest(&config.corruption_replica_path)?;
    let corrupt_manifest_rejected = SearchOutOfCoreReader::open_with_config(
        &config.corruption_replica_path,
        out_of_core_config.clone(),
    )
    .is_err();
    let corrupt_artifact_rejected =
        rabitq_attached && corrupt_rabitq_rejected && rabitq_restored && corrupt_manifest_rejected;
    let process_end =
        ProcessMemorySnapshot::capture().map_err(ProductionSearchQualificationError::from_error)?;

    Ok(ProductionSearchLifecycleReport {
        bounded_generation_update,
        incremental_upsert_delete,
        checkpoint_reopen,
        stale_generation,
        corrupt_artifact_rejected,
        mixed_foreground_background,
        update_latency: crate::latency_percentiles(&update_micros),
        checkpoint_latency: crate::latency_percentiles(&checkpoint_micros),
        reopen_latency: crate::latency_percentiles(&reopen_micros),
        checkpoint_write_amplification_per_million,
        max_update_resident_document_count,
        max_update_peak_segment_document_bytes,
        rabitq_serving,
        rabitq_preferred_serving,
        rabitq_raw_rerank,
        rabitq_metadata_filter_pushdown,
        rabitq_payload_bytes_read,
        process_memory: ProcessMemoryProfile::between(process_start, process_end),
    })
}

fn require_projection_identity(
    reader: &SearchOutOfCoreReader,
    expected: &SearchProjectionQualificationIdentity,
) -> Result<(), ProductionSearchQualificationError> {
    if reader.production_qualification_identity() == *expected {
        Ok(())
    } else {
        Err(ProductionSearchQualificationError::new(
            "production search lifecycle replica identity does not match the source projection",
        ))
    }
}

fn corrupt_out_of_core_manifest(path: &Path) -> Result<(), ProductionSearchQualificationError> {
    flip_last_byte(&path.join(OUT_OF_CORE_MANIFEST_FILE))
}

fn flip_last_byte(path: &Path) -> Result<(), ProductionSearchQualificationError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(ProductionSearchQualificationError::from_error)?;
    let length = file
        .metadata()
        .map_err(ProductionSearchQualificationError::from_error)?
        .len();
    if length == 0 {
        return Err(ProductionSearchQualificationError::new(
            "production search corruption artifact is empty",
        ));
    }
    file.seek(SeekFrom::End(-1))
        .map_err(ProductionSearchQualificationError::from_error)?;
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte)
        .map_err(ProductionSearchQualificationError::from_error)?;
    file.seek(SeekFrom::End(-1))
        .map_err(ProductionSearchQualificationError::from_error)?;
    byte[0] ^= 0xff;
    file.write_all(&byte)
        .and_then(|_| file.sync_all())
        .map_err(ProductionSearchQualificationError::from_error)
}

fn contains_hit(result: &SearchResultSet, document_id: &str) -> bool {
    result.hits.iter().any(|hit| hit.id == document_id)
}

fn delta_logical_bytes(delta: &SearchProjectionDelta) -> u64 {
    let upserts = delta.upserts.iter().fold(0u64, |bytes, row| {
        bytes
            .saturating_add(row.external_id.len() as u64)
            .saturating_add(row.title.len() as u64)
            .saturating_add(row.body.len() as u64)
            .saturating_add(
                row.embedding
                    .as_ref()
                    .map(|embedding| {
                        (embedding.len() as u64).saturating_mul(std::mem::size_of::<f32>() as u64)
                    })
                    .unwrap_or_default(),
            )
            .saturating_add(
                row.metadata
                    .iter()
                    .map(|(name, value)| (name.len() + value.len()) as u64)
                    .fold(0u64, u64::saturating_add),
            )
    });
    delta
        .deletes
        .iter()
        .map(|id| id.len() as u64)
        .fold(upserts, u64::saturating_add)
        .max(1)
}

fn directory_regular_file_bytes(path: &Path) -> Result<u64, ProductionSearchQualificationError> {
    let mut total = 0u64;
    let entries =
        std::fs::read_dir(path).map_err(ProductionSearchQualificationError::from_error)?;
    for entry in entries {
        let entry = entry.map_err(ProductionSearchQualificationError::from_error)?;
        let metadata = entry
            .metadata()
            .map_err(ProductionSearchQualificationError::from_error)?;
        if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

fn ratio_per_million(numerator: u64, denominator: u64) -> u64 {
    u64::try_from(u128::from(numerator).saturating_mul(1_000_000) / u128::from(denominator.max(1)))
        .unwrap_or(u64::MAX)
}
