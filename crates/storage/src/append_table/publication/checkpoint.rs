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

mod compaction;
pub(super) mod read;
pub(super) mod state;

pub(super) use compaction::plan_compaction;

use super::*;
use crate::append_table::checkpoint::work_error;
use crate::file_io::OpenOptions;
use crate::relational::RelationalValue;

pub(super) fn write_artifact(
    directory: &Path,
    file_name: &str,
    bytes: &[u8],
    work: Option<&CheckpointWorkContext>,
) -> Result<(), AppendTableError> {
    let Some(work) = work else {
        return write_durable_artifact(directory, file_name, bytes);
    };
    let destination = directory.join(file_name);
    let temporary = temporary_path(&destination);
    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(durability("create append candidate"))?;
    // Own cleanup only after exclusive creation succeeds. An earlier
    // interrupted candidate's temporary artifact remains evidence.
    let mut cleanup = TemporaryArtifact(Some(temporary.clone()));
    drop(wave);
    unit.finish();
    for block in bytes.chunks(64 * 1024) {
        let unit = work.start_unit().map_err(work_error)?;
        let wave = work.io_wave().map_err(work_error)?;
        file.write_all(block)
            .map_err(durability("write append candidate"))?;
        drop(wave);
        unit.finish();
    }
    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    file.sync_all()
        .map_err(durability("synchronize append candidate"))?;
    drop(file);
    drop(wave);
    unit.finish();
    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    durable_replace_file(&temporary, &destination)
        .map_err(durability("publish append candidate"))?;
    cleanup.0 = None;
    drop(wave);
    unit.finish();
    work.checkpoint().map_err(|error| match error.stop_reason() {
        Some(reason) => AppendTableError::PublishOutcomeUnknown(Box::new(
            hawdb_core::HawDBError::PublishOutcomeUnknown {
                destination,
                message: format!(
                    "append candidate publication completed before the runtime task stopped: {reason}"
                ),
            },
        )),
        None => work_error(error),
    })
}

pub(super) fn after_publication<T>(
    result: Result<T, AppendTableError>,
    destination: &Path,
) -> Result<T, AppendTableError> {
    result.map_err(|error| match error {
        AppendTableError::Stopped(reason) => AppendTableError::PublishOutcomeUnknown(Box::new(
            hawdb_core::HawDBError::PublishOutcomeUnknown {
                destination: destination.to_path_buf(),
                message: format!(
                    "append candidate publication completed before the runtime task stopped: {reason}"
                ),
            },
        )),
        error => error,
    })
}

struct TemporaryArtifact(Option<PathBuf>);

impl Drop for TemporaryArtifact {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = fs::remove_file(path);
        }
    }
}

#[cfg(test)]
pub(super) fn manifest_payload(
    manifest: &AppendGenerationManifest,
    config: AppendPublicationConfig,
    work: &CheckpointWorkContext,
) -> Result<crate::background::CheckpointBytes, AppendTableError> {
    super::manifest_encoding::admitted(&manifest.into(), config, work)
}

#[cfg(test)]
pub(super) fn manifest_envelope(
    manifest: &AppendGenerationManifest,
    payload: &[u8],
    config: AppendPublicationConfig,
    work: &CheckpointWorkContext,
) -> Result<crate::background::CheckpointBytes, AppendTableError> {
    let encoded_len = MANIFEST_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or_else(|| AppendTableError::Admission("append manifest size overflow".to_string()))?;
    if encoded_len > config.max_manifest_bytes {
        return Err(AppendTableError::Admission(format!(
            "append manifest contains {encoded_len} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }
    let mut encoded =
        crate::background::CheckpointBytes::new(encoded_len, work).map_err(work_error)?;
    encoded
        .append(
            &super::manifest_encoding::header(&manifest.into(), payload.len())?,
            work,
        )
        .map_err(work_error)?;

    encoded.append(payload, work).map_err(work_error)?;
    super::manifest_encoding::seal(&mut encoded, work)?;
    Ok(encoded)
}

#[cfg(test)]
mod tests;

pub(super) fn validate_request(
    request: &AppendCheckpointPublicationRequest<'_>,
    work: Option<&CheckpointWorkContext>,
) -> Result<(), AppendTableError> {
    let AppendCheckpointPublicationRequest {
        generation,
        source_commit_epoch,
        previous,
        state,
        rows,
        config,
        ..
    } = *request;
    let schemas = state.schemas;
    let generated_order_watermarks = state.generated_order_watermarks;
    let Some(work) = work else {
        return validate_publication_request(
            generation,
            source_commit_epoch,
            previous,
            schemas,
            generated_order_watermarks,
            rows,
            config,
        );
    };
    work.checkpoint().map_err(work_error)?;
    validate_config(config)?;
    let empty_epoch_zero = source_commit_epoch == 0
        && schemas.is_empty()
        && rows.is_empty()
        && previous.is_none_or(|reader| {
            reader.manifest.schemas.is_empty() && reader.manifest.segments.is_empty()
        });
    if generation == 0 || (source_commit_epoch == 0 && !empty_epoch_zero) {
        return Err(AppendTableError::Admission(
            "append generation must be non-zero and source commit epoch zero is reserved for an empty append generation"
                .to_string(),
        ));
    }
    if schemas.len() > config.max_schemas {
        return Err(AppendTableError::Admission(format!(
            "append generation contains {} schemas, exceeding limit {}",
            schemas.len(),
            config.max_schemas
        )));
    }
    for (name, schema) in schemas {
        let unit = work.start_unit().map_err(work_error)?;
        if name != &schema.name {
            return Err(AppendTableError::Schema(format!(
                "append schema map key {name} differs from schema name {}",
                schema.name
            )));
        }
        unit.finish();
    }
    let base_watermarks = effective_watermarks(previous, rows, work)?;
    validate_generated_watermarks(schemas, &base_watermarks, generated_order_watermarks, work)?;
    if let Some(previous) = previous {
        if generation <= previous.manifest.generation
            || source_commit_epoch < previous.manifest.source_commit_epoch
        {
            return Err(AppendTableError::Constraint(
                "append generation and commit epoch must advance monotonically".to_string(),
            ));
        }
        for (name, prior) in &previous.manifest.schemas {
            let unit = work.start_unit().map_err(work_error)?;
            if schemas.get(name) != Some(prior) {
                return Err(AppendTableError::Schema(format!(
                    "append table {name} cannot be removed or rewritten"
                )));
            }
            unit.finish();
        }
        for (table, prior) in &previous.manifest.generated_order_watermarks {
            let unit = work.start_unit().map_err(work_error)?;
            if generated_order_watermarks
                .get(table)
                .is_none_or(|current| current < prior)
            {
                return Err(AppendTableError::Constraint(format!(
                    "append generated-order watermark for table {table} cannot regress"
                )));
            }
            unit.finish();
        }
    }
    for row in rows {
        let unit = work.start_unit().map_err(work_error)?;
        if !schemas.contains_key(&row.table) {
            return Err(AppendTableError::Schema(format!(
                "append checkpoint row references unknown table {}",
                row.table
            )));
        }
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(())
}

fn effective_watermarks(
    previous: Option<&AppendGenerationReader>,
    rows: &[AppendTableRow],
    work: &CheckpointWorkContext,
) -> Result<BTreeMap<String, BTreeMap<RelationalKey, RelationalKey>>, AppendTableError> {
    let mut watermarks: BTreeMap<String, BTreeMap<RelationalKey, RelationalKey>> = BTreeMap::new();
    if let Some(previous) = previous {
        for segment in previous.segments.iter() {
            let unit = work.start_unit().map_err(work_error)?;
            unit.finish();
            for descriptor in segment.descriptors() {
                let unit = work.start_unit().map_err(work_error)?;
                watermarks
                    .entry(descriptor.table.clone())
                    .or_default()
                    .insert(
                        descriptor.partition_key.clone(),
                        descriptor.max_order_key.clone(),
                    );
                unit.finish();
            }
        }
    }
    for row in rows {
        let unit = work.start_unit().map_err(work_error)?;
        let watermark = watermarks
            .entry(row.table.clone())
            .or_default()
            .entry(row.partition_key.clone())
            .or_insert_with(|| row.order_key.clone());
        if row.order_key > *watermark {
            *watermark = row.order_key.clone();
        }
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(watermarks)
}

fn validate_generated_watermarks(
    schemas: &BTreeMap<String, AppendTableSchema>,
    base: &BTreeMap<String, BTreeMap<RelationalKey, RelationalKey>>,
    generated: &BTreeMap<String, i64>,
    work: &CheckpointWorkContext,
) -> Result<(), AppendTableError> {
    validate_generated_watermarks_by_order(schemas, generated, work, |table| {
        base.get(table)
            .into_iter()
            .flat_map(|partitions| partitions.values())
    })
}

fn validate_generated_watermarks_by_order<'a, I>(
    schemas: &BTreeMap<String, AppendTableSchema>,
    generated: &BTreeMap<String, i64>,
    work: &CheckpointWorkContext,
    mut orders: impl FnMut(&str) -> I,
) -> Result<(), AppendTableError>
where
    I: IntoIterator<Item = &'a RelationalKey>,
{
    for (table, watermark) in generated {
        let unit = work.start_unit().map_err(work_error)?;
        let schema = schemas.get(table).ok_or_else(|| {
            AppendTableError::Corruption(format!(
                "generated-order watermark references unknown table {table}"
            ))
        })?;
        if schema.order_mode != super::super::AppendOrderMode::CommitSequence || *watermark < 0 {
            return Err(AppendTableError::Corruption(format!(
                "table {table} has an invalid generated-order watermark"
            )));
        }
        unit.finish();
    }
    for (table, schema) in schemas {
        let unit = work.start_unit().map_err(work_error)?;
        match schema.order_mode {
            super::super::AppendOrderMode::CallerProvided => {
                if generated.contains_key(table) {
                    return Err(AppendTableError::Corruption(format!(
                        "caller-provided order table {table} has a generated-order watermark"
                    )));
                }
                unit.finish();
            }
            super::super::AppendOrderMode::CommitSequence => {
                let watermark = *generated.get(table).ok_or_else(|| {
                    AppendTableError::Corruption(format!(
                        "generated-order table {table} has no durable watermark"
                    ))
                })?;
                unit.finish();
                let mut derived = 0;
                for order in orders(table) {
                    let unit = work.start_unit().map_err(work_error)?;
                    let value = match order.0.as_slice() {
                        [RelationalValue::BigInt(value)] if *value > 0 => *value,
                        _ => {
                            return Err(AppendTableError::Corruption(format!(
                                "generated-order table {table} has an invalid checkpoint order key"
                            )))
                        }
                    };
                    derived = derived.max(value);
                    unit.finish();
                }
                if derived != watermark {
                    return Err(AppendTableError::Corruption(format!(
                        "generated-order table {table} checkpoint watermark {watermark} does not match row watermark {derived}"
                    )));
                }
            }
        }
    }
    work.checkpoint().map_err(work_error)
}
