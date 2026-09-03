use super::query::{
    execute_query, query_options, result_contains, result_digest, VectorExecutionProfile,
};
use super::{
    elapsed_micros, fallback_is_projection_unavailable, latency_percentiles,
    ProductionVectorLifecycleReport, ProductionVectorQualificationConfig,
    ProductionVectorQualificationError,
};
use skein::{
    AdaptiveVectorSearchOptions, CompressedVectorSearchMode, RuntimeCancellationToken,
    RuntimeTaskContext, SearchIndex, SearchProjectionDelta, SearchResultSet,
    VectorProjectionQualificationIdentity, VectorSearchKernelPreference,
};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const RABITQ_ARTIFACT_PREFIX: &str = "search_rabitq.";
const RABITQ_ARTIFACT_SUFFIX: &str = ".skein";

pub(super) fn run_lifecycle(
    config: &ProductionVectorQualificationConfig,
) -> Result<ProductionVectorLifecycleReport, ProductionVectorQualificationError> {
    let expected_projection_identity = SearchIndex::open(&config.projection_path)
        .map_err(ProductionVectorQualificationError::from_error)?
        .vector_projection_qualification_identity()
        .ok_or_else(|| {
            ProductionVectorQualificationError::new(
                "production vector lifecycle requires a valid source projection identity",
            )
        })?;
    let mut update_micros = Vec::with_capacity(config.lifecycle.replica_paths.len());
    let mut checkpoint_micros = Vec::with_capacity(config.lifecycle.replica_paths.len());
    let mut reopen_micros = Vec::with_capacity(config.lifecycle.replica_paths.len());
    let mut incremental_fallback_safe = true;
    let mut checkpoint_reopen_restores_projection = true;
    let mut stale_generation_isolated = true;
    let mut mixed_foreground_background = true;
    let mut checkpoint_write_amplification_per_million = 0;
    let logical_delta_bytes = delta_logical_bytes(&config.lifecycle.delta);

    for path in &config.lifecycle.replica_paths {
        let stale_reader = Arc::new(
            SearchIndex::open(path).map_err(ProductionVectorQualificationError::from_error)?,
        );
        require_projection_identity(&stale_reader, &expected_projection_identity)?;
        let stale_identity = stale_reader
            .vector_projection_qualification_identity()
            .expect("validated projection identity exists");
        let task_context =
            RuntimeTaskContext::default().with_admitted_parallelism(config.max_parallelism);
        let stale_result = execute_query(
            &stale_reader,
            &config.lifecycle.verification_case,
            config,
            &task_context,
            VectorExecutionProfile::AutoCandidate,
        )?;
        let stale_digest = result_digest(&stale_result);
        incremental_fallback_safe &=
            !result_contains(&stale_result, &config.lifecycle.expected_upsert_document_id)
                && result_contains(
                    &stale_result,
                    &config.lifecycle.expected_deleted_document_id,
                );

        let bytes_before = directory_regular_file_bytes(path)?;
        let mut writer =
            SearchIndex::open(path).map_err(ProductionVectorQualificationError::from_error)?;
        let update_started = Instant::now();
        writer
            .apply_projection_delta(config.lifecycle.delta.clone())
            .map_err(ProductionVectorQualificationError::from_error)?;
        update_micros.push(elapsed_micros(update_started));

        let fallback_result = execute_preferred(
            &writer,
            &config.lifecycle.verification_case,
            config,
            &task_context,
        )?;
        incremental_fallback_safe &= (fallback_is_projection_unavailable(&fallback_result)
            || uses_scalar_vector_backend(&fallback_result))
            && result_contains(
                &fallback_result,
                &config.lifecycle.expected_upsert_document_id,
            )
            && !result_contains(
                &fallback_result,
                &config.lifecycle.expected_deleted_document_id,
            );

        let barrier = Arc::new(Barrier::new(2));
        let worker_barrier = Arc::clone(&barrier);
        let worker_reader = Arc::clone(&stale_reader);
        let worker_case = config.lifecycle.verification_case.clone();
        let worker_config = config.clone();
        let worker_runs = config.lifecycle.mixed_load_probe_runs;
        let expected_stale_digest = stale_digest.clone();
        let worker = thread::spawn(move || {
            let task_context = RuntimeTaskContext::default()
                .with_admitted_parallelism(worker_config.max_parallelism);
            worker_barrier.wait();
            (0..worker_runs).all(|_| {
                execute_query(
                    &worker_reader,
                    &worker_case,
                    &worker_config,
                    &task_context,
                    VectorExecutionProfile::AutoCandidate,
                )
                .is_ok_and(|result| result_digest(&result) == expected_stale_digest)
            })
        });
        barrier.wait();
        let checkpoint_started = Instant::now();
        writer
            .checkpoint()
            .map_err(ProductionVectorQualificationError::from_error)?;
        checkpoint_micros.push(elapsed_micros(checkpoint_started));
        mixed_foreground_background &= worker.join().map_err(|_| {
            ProductionVectorQualificationError::new(
                "production vector mixed-load probe thread panicked",
            )
        })?;
        drop(writer);

        let stale_after = execute_query(
            &stale_reader,
            &config.lifecycle.verification_case,
            config,
            &task_context,
            VectorExecutionProfile::AutoCandidate,
        )?;
        stale_generation_isolated &= result_digest(&stale_after) == stale_digest
            && stale_reader.vector_projection_qualification_identity()
                == Some(stale_identity.clone());

        let bytes_after = directory_regular_file_bytes(path)?;
        checkpoint_write_amplification_per_million = checkpoint_write_amplification_per_million
            .max(ratio_per_million(
                bytes_after.saturating_sub(bytes_before),
                logical_delta_bytes,
            ));
        let reopen_started = Instant::now();
        let reopened =
            SearchIndex::open(path).map_err(ProductionVectorQualificationError::from_error)?;
        reopen_micros.push(elapsed_micros(reopen_started));
        let reopened_identity = reopened
            .vector_projection_qualification_identity()
            .ok_or_else(|| {
                ProductionVectorQualificationError::new(
                    "production vector checkpoint did not publish a RaBitQ projection",
                )
            })?;
        let reopened_result = execute_query(
            &reopened,
            &config.lifecycle.verification_case,
            config,
            &task_context,
            VectorExecutionProfile::AutoCandidate,
        )?;
        checkpoint_reopen_restores_projection &= reopened_identity.projection_generation
            > stale_identity.projection_generation
            && reopened_identity.source_graph_commit_epoch
                == Some(config.expected_identity.canonical_graph_commit_epoch)
            && result_contains(
                &reopened_result,
                &config.lifecycle.expected_upsert_document_id,
            )
            && !result_contains(
                &reopened_result,
                &config.lifecycle.expected_deleted_document_id,
            )
            && !fallback_is_projection_unavailable(&reopened_result);
        stale_generation_isolated &=
            reopened_identity.projection_generation > stale_identity.projection_generation;
    }

    let corrupt_projection_rejected = run_corruption_probe(config, &expected_projection_identity)?;
    let (cancellation_propagated, cancellation_micros) = run_cancellation_probe(config)?;

    Ok(ProductionVectorLifecycleReport {
        incremental_fallback_safe,
        checkpoint_reopen_restores_projection,
        stale_generation_isolated,
        corrupt_projection_rejected,
        cancellation_propagated,
        serving_cancellation_propagated: false,
        mixed_foreground_background,
        update_latency: latency_percentiles(&update_micros),
        checkpoint_latency: latency_percentiles(&checkpoint_micros),
        reopen_latency: latency_percentiles(&reopen_micros),
        cancellation_latency: latency_percentiles(&[cancellation_micros]),
        serving_cancellation_latency: crate::LatencyPercentiles::default(),
        checkpoint_write_amplification_per_million,
    })
}

fn execute_preferred(
    index: &SearchIndex,
    query_case: &super::ProductionVectorQueryCase,
    config: &ProductionVectorQualificationConfig,
    task_context: &RuntimeTaskContext,
) -> Result<SearchResultSet, ProductionVectorQualificationError> {
    index
        .try_search_with_options_adaptive_vector_projection_context(
            "",
            Some(&query_case.query_embedding),
            skein::SearchMode::Vector,
            query_options(query_case, config)?,
            AdaptiveVectorSearchOptions::new(CompressedVectorSearchMode::Preferred),
            config.execution_options(task_context, VectorSearchKernelPreference::Auto),
        )
        .map_err(ProductionVectorQualificationError::from_error)
}

fn run_corruption_probe(
    config: &ProductionVectorQualificationConfig,
    expected: &VectorProjectionQualificationIdentity,
) -> Result<bool, ProductionVectorQualificationError> {
    let path = &config.lifecycle.corruption_replica_path;
    let before = SearchIndex::open(path).map_err(ProductionVectorQualificationError::from_error)?;
    require_projection_identity(&before, expected)?;
    drop(before);

    let corrupted_name = corrupt_latest_rabitq_artifact(path)?;
    let reopened =
        SearchIndex::open(path).map_err(ProductionVectorQualificationError::from_error)?;
    let actual = reopened.vector_projection_qualification_identity();
    // Rejection quarantines the corrupt artifact away from its original name,
    // and generation cleanup may reclaim the quarantine copy within the same
    // open. The durable rejection signal is therefore the original name no
    // longer existing; a still-visible quarantine copy is equally acceptable.
    let original_removed = !path.join(&corrupted_name).exists();
    let quarantine_visible = std::fs::read_dir(path)
        .map_err(ProductionVectorQualificationError::from_error)?
        .filter_map(Result::ok)
        .any(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(&format!("{corrupted_name}.corrupt.")))
        });
    if actual.as_ref() == Some(expected) || !(original_removed || quarantine_visible) {
        return Ok(false);
    }

    let task_context =
        RuntimeTaskContext::default().with_admitted_parallelism(config.max_parallelism);
    let result = execute_preferred(
        &reopened,
        &config.lifecycle.verification_case,
        config,
        &task_context,
    )?;
    let safe_backend = actual.is_some()
        || fallback_is_projection_unavailable(&result)
        || uses_scalar_vector_backend(&result);
    Ok(safe_backend
        && result_contains(&result, &config.lifecycle.expected_deleted_document_id)
        && !result_contains(&result, &config.lifecycle.expected_upsert_document_id))
}

fn run_cancellation_probe(
    config: &ProductionVectorQualificationConfig,
) -> Result<(bool, u64), ProductionVectorQualificationError> {
    let index = SearchIndex::open(&config.projection_path)
        .map_err(ProductionVectorQualificationError::from_error)?;
    let token = RuntimeCancellationToken::new();
    token.cancel();
    let task_context = RuntimeTaskContext::without_deadline(token)
        .with_admitted_parallelism(config.max_parallelism);
    let started = Instant::now();
    let result = execute_query(
        &index,
        &config.lifecycle.verification_case,
        config,
        &task_context,
        VectorExecutionProfile::AutoCandidate,
    );
    let elapsed = elapsed_micros(started);
    Ok((
        matches!(result, Err(ref error) if error.to_string().contains("cancelled")),
        elapsed,
    ))
}

fn require_projection_identity(
    index: &SearchIndex,
    expected: &VectorProjectionQualificationIdentity,
) -> Result<(), ProductionVectorQualificationError> {
    if index.vector_projection_qualification_identity().as_ref() == Some(expected) {
        Ok(())
    } else {
        Err(ProductionVectorQualificationError::new(
            "production vector lifecycle replica identity does not match the source projection",
        ))
    }
}

fn corrupt_latest_rabitq_artifact(
    path: &Path,
) -> Result<String, ProductionVectorQualificationError> {
    let mut artifacts = std::fs::read_dir(path)
        .map_err(ProductionVectorQualificationError::from_error)?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().to_str()?.to_string();
            let generation = name
                .strip_prefix(RABITQ_ARTIFACT_PREFIX)?
                .strip_suffix(RABITQ_ARTIFACT_SUFFIX)?
                .parse::<u64>()
                .ok()?;
            Some((generation, name, entry.path()))
        })
        .collect::<Vec<_>>();
    artifacts.sort_unstable_by_key(|(generation, _, _)| std::cmp::Reverse(*generation));
    let (_, name, artifact) = artifacts.into_iter().next().ok_or_else(|| {
        ProductionVectorQualificationError::new(
            "production vector corruption replica has no RaBitQ artifact",
        )
    })?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&artifact)
        .map_err(ProductionVectorQualificationError::from_error)?;
    if file
        .metadata()
        .map_err(ProductionVectorQualificationError::from_error)?
        .len()
        == 0
    {
        return Err(ProductionVectorQualificationError::new(
            "production vector corruption replica has an empty RaBitQ artifact",
        ));
    }
    file.seek(SeekFrom::End(-1))
        .map_err(ProductionVectorQualificationError::from_error)?;
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte)
        .map_err(ProductionVectorQualificationError::from_error)?;
    file.seek(SeekFrom::End(-1))
        .map_err(ProductionVectorQualificationError::from_error)?;
    byte[0] ^= 0xff;
    file.write_all(&byte)
        .and_then(|_| file.sync_all())
        .map_err(ProductionVectorQualificationError::from_error)?;
    Ok(name)
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

fn uses_scalar_vector_backend(result: &SearchResultSet) -> bool {
    result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "vector")
        .is_some_and(|retriever| retriever.backend == "scalar_vector_scan")
}

fn directory_regular_file_bytes(path: &Path) -> Result<u64, ProductionVectorQualificationError> {
    let mut total = 0u64;
    for entry in std::fs::read_dir(path).map_err(ProductionVectorQualificationError::from_error)? {
        let entry = entry.map_err(ProductionVectorQualificationError::from_error)?;
        let metadata = entry
            .metadata()
            .map_err(ProductionVectorQualificationError::from_error)?;
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
