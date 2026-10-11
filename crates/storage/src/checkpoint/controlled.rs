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

//! Controlled encoding preserves the independent ordinary V1 text oracle.
//! Complete output allocation and individual relational key codecs remain open.

use super::*;
use crate::background::CheckpointWorkContext;
use hawdb_core::Value;
use std::fmt;

pub(crate) fn encode_checkpoint_body_with_work_context<'a>(
    image: &CheckpointImage<'_>,
    generation: u64,
    changes: impl Iterator<Item = &'a SearchProjectionGraphChange> + Clone,
    work: &CheckpointWorkContext,
) -> Result<String> {
    validate_changes(
        image.search_projection_change_log_start_epoch,
        image.commit_epoch,
        changes.clone(),
        work,
    )?;
    let encode_string = |value: &str| hex(value.as_bytes(), work);
    let encode_string_vec = |values: &[String]| strings(values, work);
    let encode_u64_vec = |values| numbers(values, work);
    let encode_value_vec = |values: &[hawdb_core::Value]| values_text(values, work);
    let encode_search_projection_relational_primary_key_changes =
        |capture: &RelationalPrimaryKeyChangeCapture| capture_text(capture, work);
    let mut body = Body::new(work);
    body.write(format_args!("{CHECKPOINT_HEADER_V1}\n"))?;
    body.write(format_args!("version\t{STORAGE_VERSION}\n"))?;
    body.write(format_args!("generation\t{generation}\n"))?;
    body.write(format_args!("commit_epoch\t{}\n", image.commit_epoch))?;
    if let Some(relational) = image.relational_checkpoint {
        body.write(format_args!(
            "relational_checkpoint_encoded_len\t{}\n",
            relational.encoded_len
        ))?;
        body.write(format_args!(
            "relational_checkpoint_encoded_checksum\t{}\n",
            relational.encoded_checksum
        ))?;
        body.write(format_args!(
            "relational_checkpoint_encoded_sha256\t{}\n",
            relational.encoded_sha256
        ))?;
    }
    body.write(format_args!("next_node_id\t{}\n", image.next_node_id))?;
    body.write(format_args!("next_rel_id\t{}\n", image.next_rel_id))?;
    body.write(format_args!("{}", "canonical_records\ttrue\n"))?;
    body.write(format_args!(
        "search_projection_change_log_start_epoch\t{}\n",
        image.search_projection_change_log_start_epoch
    ))?;
    if let Some(identity) = image.search_projection_database_identity {
        body.write(format_args!(
            "search_projection_database_identity\t{identity}\n"
        ))?;
    }
    if let Some(source_fingerprint) = image.initial_import_source_fingerprint {
        body.write(format_args!(
            "initial_import_source_fingerprint\t{}\n",
            encode_string(source_fingerprint)?
        ))?;
    }
    for change in changes {
        let (relational_kind, relational_changes) =
            encode_search_projection_relational_primary_key_changes(
                &change.relational_primary_key_changes,
            )?;
        body.write(format_args!(
            "search_projection_change\t{}\t{}\t{}\t{}\t{}\n",
            change.commit_epoch,
            encode_u64_vec(change.upsert_node_ids.iter().copied())?,
            encode_string_vec(&change.delete_document_ids)?,
            relational_kind,
            relational_changes,
        ))?;
    }
    for label in image.catalog.labels() {
        if !label.name.is_empty() {
            body.write(format_args!(
                "label\t{}\t{}\n",
                label.id.0,
                encode_string(&label.name)?
            ))?;
        }
    }
    for rel_type in image.catalog.rel_types() {
        if !rel_type.name.is_empty() {
            body.write(format_args!(
                "rel_type\t{}\t{}\n",
                rel_type.id.0,
                encode_string(&rel_type.name)?
            ))?;
        }
    }
    for index in image.catalog.property_indexes() {
        body.write(format_args!(
            "property_index\t{}\t{}\t{}\t{}\n",
            index.id.0,
            index.label_id.0,
            encode_string(&index.property)?,
            encode_index_kind(index.kind)
        ))?;
    }
    for index in image.catalog.composite_property_indexes() {
        body.write(format_args!(
            "composite_property_index\t{}\t{}\t{}\n",
            index.id.0,
            index.label_id.0,
            encode_string_vec(&index.properties)?
        ))?;
    }
    for table in image.catalog.table_descriptors() {
        body.write(format_args!(
            "table\t{}\t{}\t{}\t{}\n",
            table.id.0,
            encode_table_kind(table.kind),
            encode_string(&table.name)?,
            encode_schema_object_state(table.state)
        ))?;
    }
    for property in image.catalog.property_descriptors() {
        body.write(format_args!(
            "property\t{}\t{}\t{}\t{}\t{}\t{}\n",
            property.id.0,
            property.table_id.0,
            encode_string(&property.name)?,
            encode_property_type(property.value_type),
            encode_nullable(property.nullable),
            encode_schema_object_state(property.state)
        ))?;
    }
    for constraint in image.catalog.unique_constraints() {
        let ConstraintSubject::Node(label_id) = constraint.subject else {
            continue;
        };
        body.write(format_args!(
            "unique_constraint\t{}\t{}\t{}\n",
            constraint.id.0,
            label_id.0,
            encode_string(&constraint.property)?
        ))?;
    }
    for constraint in image.catalog.node_property_exists_constraints() {
        let ConstraintSubject::Node(label_id) = constraint.subject else {
            continue;
        };
        body.write(format_args!(
            "node_property_exists_constraint\t{}\t{}\t{}\n",
            constraint.id.0,
            label_id.0,
            encode_string(&constraint.property)?
        ))?;
    }
    for constraint in image.catalog.relationship_property_exists_constraints() {
        let ConstraintSubject::Relationship(rel_type_id) = constraint.subject else {
            continue;
        };
        body.write(format_args!(
            "relationship_property_exists_constraint\t{}\t{}\t{}\n",
            constraint.id.0,
            rel_type_id.0,
            encode_string(&constraint.property)?
        ))?;
    }
    for constraint in image.catalog.relationship_unique_constraints() {
        let ConstraintSubject::Relationship(rel_type_id) = constraint.subject else {
            continue;
        };
        body.write(format_args!(
            "relationship_unique_constraint\t{}\t{}\t{}\n",
            constraint.id.0,
            rel_type_id.0,
            encode_string(&constraint.property)?
        ))?;
    }
    let statistics = image.statistics;
    body.write(format_args!(
        "stat_commit_epoch\t{}\n",
        statistics.computed_at_commit_epoch
    ))?;
    body.write(format_args!(
        "stat_advanced_complete\t{}\n",
        statistics.advanced_statistics_complete
    ))?;
    body.write(format_args!(
        "stat_histogram_sample_limit\t{}\n",
        statistics.histogram_sample_limit
    ))?;
    body.write(format_args!("stat_node_count\t{}\n", statistics.node_count))?;
    body.write(format_args!(
        "stat_relationship_count\t{}\n",
        statistics.relationship_count
    ))?;
    for (label_id, count) in &statistics.label_counts {
        body.write(format_args!(
            "stat_label_count\t{}\t{}\n",
            label_id.0, count
        ))?;
    }
    for (rel_type_id, count) in &statistics.rel_type_counts {
        body.write(format_args!(
            "stat_rel_type_count\t{}\t{}\n",
            rel_type_id.0, count
        ))?;
    }
    for (rel_type_id, count) in &statistics.rel_type_source_counts {
        body.write(format_args!(
            "stat_rel_type_source_count\t{}\t{}\n",
            rel_type_id.0, count
        ))?;
    }
    for (rel_type_id, count) in &statistics.rel_type_target_counts {
        body.write(format_args!(
            "stat_rel_type_target_count\t{}\t{}\n",
            rel_type_id.0, count
        ))?;
    }
    for ((source_label_id, rel_type_id, target_label_id), count) in &statistics.path_counts {
        body.write(format_args!(
            "stat_path_count\t{}\t{}\t{}\t{}\n",
            source_label_id.0, rel_type_id.0, target_label_id.0, count
        ))?;
    }
    for ((source_label_id, rel_type_id, target_label_id), count) in
        &statistics.path_source_distinct_counts
    {
        body.write(format_args!(
            "stat_path_source_distinct_count\t{}\t{}\t{}\t{}\n",
            source_label_id.0, rel_type_id.0, target_label_id.0, count
        ))?;
    }
    for ((source_label_id, rel_type_id, target_label_id), count) in
        &statistics.path_target_distinct_counts
    {
        body.write(format_args!(
            "stat_path_target_distinct_count\t{}\t{}\t{}\t{}\n",
            source_label_id.0, rel_type_id.0, target_label_id.0, count
        ))?;
    }
    for ((source_label_id, rel_type_id, target_label_id, hops), count) in
        &statistics.bounded_path_counts
    {
        body.write(format_args!(
            "stat_bounded_path_count\t{}\t{}\t{}\t{}\t{}\n",
            source_label_id.0, rel_type_id.0, target_label_id.0, hops, count
        ))?;
    }
    for ((source_label_id, rel_type_id, target_label_id, hops), count) in
        &statistics.bounded_path_source_distinct_counts
    {
        body.write(format_args!(
            "stat_bounded_path_source_distinct_count\t{}\t{}\t{}\t{}\t{}\n",
            source_label_id.0, rel_type_id.0, target_label_id.0, hops, count
        ))?;
    }
    for ((source_label_id, rel_type_id, target_label_id, hops), count) in
        &statistics.bounded_path_target_distinct_counts
    {
        body.write(format_args!(
            "stat_bounded_path_target_distinct_count\t{}\t{}\t{}\t{}\t{}\n",
            source_label_id.0, rel_type_id.0, target_label_id.0, hops, count
        ))?;
    }
    for (index_id, sample) in &statistics.index_samples {
        body.write(format_args!(
            "stat_index_sample\t{}\t{}\t{}\t{}\t{}\n",
            index_id.0,
            sample.index_size,
            sample.unique_values,
            sample.sample_size,
            sample.updates_since_sample
        ))?;
    }
    for ((label_id, property), count) in &statistics.property_distinct_counts {
        body.write(format_args!(
            "stat_property_distinct_count\t{}\t{}\t{}\n",
            label_id.0,
            encode_string(property)?,
            count
        ))?;
    }
    for ((rel_type_id, property), count) in &statistics.rel_property_distinct_counts {
        body.write(format_args!(
            "stat_rel_property_distinct_count\t{}\t{}\t{}\n",
            rel_type_id.0,
            encode_string(property)?,
            count
        ))?;
    }
    for ((rel_type_id, property), values) in &statistics.rel_property_histograms {
        body.write(format_args!(
            "stat_rel_property_histogram\t{}\t{}\t{}\n",
            rel_type_id.0,
            encode_string(property)?,
            encode_value_vec(values)?
        ))?;
    }
    for ((rel_type_id, property), sampled) in &statistics.sampled_rel_property_histograms {
        body.write(format_args!(
            "stat_rel_property_histogram_sampled\t{}\t{}\t{}\n",
            rel_type_id.0,
            encode_string(property)?,
            encode_bool(*sampled)
        ))?;
    }
    for ((label_id, property), values) in &statistics.property_histograms {
        body.write(format_args!(
            "stat_property_histogram\t{}\t{}\t{}\n",
            label_id.0,
            encode_string(property)?,
            encode_value_vec(values)?
        ))?;
    }
    for ((label_id, property), sampled) in &statistics.sampled_property_histograms {
        body.write(format_args!(
            "stat_property_histogram_sampled\t{}\t{}\t{}\n",
            label_id.0,
            encode_string(property)?,
            encode_bool(*sampled)
        ))?;
    }
    for (name, definition) in image.projected_graphs {
        body.write(format_args!(
            "project_graph\t{}\t{}\t{}\t",
            encode_string(name)?,
            encode_string_vec(&definition.node_labels)?,
            encode_string_vec(&definition.rel_types)?
        ))?;
        crate::projection::predicate_checkpoint::encode_into(
            &definition.relationship_predicates,
            work,
            |chunk| {
                body.write(format_args!(
                    "{}",
                    std::str::from_utf8(chunk).expect("predicate text is ASCII")
                ))
            },
        )?;
        body.write(format_args!("\n"))?;
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(body.output)
}

struct Body<'a> {
    output: String,
    work: &'a CheckpointWorkContext,
    failure: Option<HawDBError>,
}

impl<'a> Body<'a> {
    fn new(work: &'a CheckpointWorkContext) -> Self {
        Self {
            output: String::new(),
            work,
            failure: None,
        }
    }

    fn write(&mut self, args: fmt::Arguments<'_>) -> Result<()> {
        fmt::write(self, args).map_err(|error| {
            self.failure
                .take()
                .unwrap_or_else(|| HawDBError::from_storage_error(error))
        })
    }
}

impl fmt::Write for Body<'_> {
    fn write_str(&mut self, mut input: &str) -> fmt::Result {
        while !input.is_empty() {
            let mut end = input.len().min(64 * 1024);
            while !input.is_char_boundary(end) {
                end -= 1;
            }
            let unit = match self.work.start_unit() {
                Ok(unit) => unit,
                Err(error) => {
                    self.failure = Some(HawDBError::from_storage_error(error));
                    return Err(fmt::Error);
                }
            };
            self.output.push_str(&input[..end]);
            unit.finish();
            input = &input[end..];
        }
        Ok(())
    }
}

fn hex(input: &[u8], work: &CheckpointWorkContext) -> Result<String> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::new();
    for block in input.chunks(32 * 1024) {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        for byte in block {
            output.push(char::from(HEX[usize::from(byte >> 4)]));
            output.push(char::from(HEX[usize::from(byte & 15)]));
        }
        unit.finish();
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(output)
}

fn strings(values: &[String], work: &CheckpointWorkContext) -> Result<String> {
    let mut body = Body::new(work);
    for (position, value) in values.iter().enumerate() {
        body.write(format_args!(
            "{}{}",
            if position == 0 { "" } else { ":" },
            hex(value.as_bytes(), work)?
        ))?;
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(body.output)
}

fn numbers(values: impl Iterator<Item = u64>, work: &CheckpointWorkContext) -> Result<String> {
    let mut body = Body::new(work);
    for (position, value) in values.enumerate() {
        body.write(format_args!(
            "{}{value}",
            if position == 0 { "" } else { "," }
        ))?;
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(body.output)
}

fn values_text(
    values: &[Value],
    work: &CheckpointWorkContext,
) -> Result<crate::background::CheckpointText> {
    let mut body = crate::background::CheckpointText::new();
    body.values(values, work)?;
    Ok(body)
}

fn capture_text(
    capture: &RelationalPrimaryKeyChangeCapture,
    work: &CheckpointWorkContext,
) -> Result<(String, String)> {
    let RelationalPrimaryKeyChangeCapture::Captured { tables, .. } = capture else {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let result = encode_search_projection_relational_primary_key_changes(capture);
        unit.finish();
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        return result;
    };
    let mut body = Body::new(work);
    for (position, table) in tables.iter().enumerate() {
        body.write(format_args!(
            "{}{}=",
            if position == 0 { "" } else { ";" },
            hex(table.table.as_bytes(), work)?
        ))?;
        for (position, key) in table.primary_keys.iter().enumerate() {
            let encoded =
                crate::relational::encode_relational_primary_key_with_checkpoint_work(key, work)
                    .map_err(|error| match error {
                        crate::relational::CheckpointKeyEncodeError::Key(error) => {
                            HawDBError::from_storage_error(error)
                        }
                        crate::relational::CheckpointKeyEncodeError::Work(error) => {
                            HawDBError::from_storage_error(error)
                        }
                    })?;
            body.write(format_args!(
                "{}{}",
                if position == 0 { "" } else { ":" },
                hex(&encoded, work)?
            ))?;
        }
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(("exact".into(), body.output))
}

fn ordered<T>(
    values: &[T],
    before: impl Fn(&T, &T) -> bool,
    work: &CheckpointWorkContext,
) -> Result<bool> {
    for start in (0..values.len().saturating_sub(1)).step_by(1024) {
        let end = (start + 1025).min(values.len());
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let valid = values[start..end]
            .windows(2)
            .all(|pair| before(&pair[0], &pair[1]));
        unit.finish();
        if !valid {
            return Ok(false);
        }
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(true)
}

fn validate_changes<'a>(
    start_epoch: u64,
    checkpoint_commit_epoch: u64,
    changes: impl Iterator<Item = &'a SearchProjectionGraphChange>,
    work: &CheckpointWorkContext,
) -> Result<()> {
    let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    if start_epoch > checkpoint_commit_epoch {
        return Err(HawDBError::Storage(format!("search projection change log start epoch {start_epoch} exceeds checkpoint commit epoch {checkpoint_commit_epoch}")));
    }
    unit.finish();
    let mut previous_epoch = start_epoch;
    for change in changes {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        if change.commit_epoch <= previous_epoch {
            return Err(HawDBError::Storage(format!("search projection change commit epoch {} is not greater than previous epoch {previous_epoch}", change.commit_epoch)));
        }
        if change.commit_epoch > checkpoint_commit_epoch {
            return Err(HawDBError::Storage(format!("search projection change commit epoch {} exceeds checkpoint commit epoch {checkpoint_commit_epoch}", change.commit_epoch)));
        }
        unit.finish();
        if !ordered(&change.upsert_node_ids, |left, right| left < right, work)? {
            return Err(HawDBError::Storage(format!("search projection change at commit epoch {} has unordered or duplicate upsert node ids",change.commit_epoch)));
        }
        if !ordered(
            &change.delete_document_ids,
            |left, right| left < right,
            work,
        )? {
            return Err(HawDBError::Storage(format!("search projection change at commit epoch {} has unordered or duplicate delete document ids",change.commit_epoch)));
        }
        if let RelationalPrimaryKeyChangeCapture::Captured { tables, .. } =
            &change.relational_primary_key_changes
        {
            if !ordered(tables, |left, right| left.table < right.table, work)? {
                return Err(HawDBError::Storage(format!("search projection change at commit epoch {} has unordered or duplicate relational tables",change.commit_epoch)));
            }
            for table in tables {
                let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
                let empty = table.primary_keys.is_empty();
                unit.finish();
                if empty || !ordered(&table.primary_keys, |left, right| left < right, work)? {
                    return Err(HawDBError::Storage(format!("search projection change at commit epoch {} has empty, unordered, or duplicate primary keys for table {}",change.commit_epoch,table.table)));
                }
            }
        }
        previous_epoch = change.commit_epoch;
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)
}

#[cfg(test)]
mod tests;
