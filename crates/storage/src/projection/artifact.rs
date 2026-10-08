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

//! Projected graph artifact text format.
//!
//! The facade owns graph construction, file publication, epoch admission, and
//! recovery fallback. This module only encodes and validates storage data.

use super::{ProjectedGraphArtifact, ProjectedGraphArtifactData, ProjectedGraphDefinition};
use crate::background::CheckpointWorkContext;
use crate::text::parse_u64;
use crate::NodeId;
use hawdb_core::{HawDBError, Result};
use std::collections::BTreeMap;

const PROJECTED_GRAPH_ARTIFACT_VERSION: u64 = 2;

/// Consume one projection at a time without materializing another graph map.
pub fn encode_projected_graph_artifacts<'a>(
    projection_epoch: u64,
    commit_epoch: u64,
    artifacts: impl IntoIterator<
        Item = (
            &'a str,
            &'a ProjectedGraphDefinition,
            ProjectedGraphArtifactData,
        ),
    >,
) -> String {
    encode_projected_graph_artifacts_with_work_context(
        projection_epoch,
        commit_epoch,
        artifacts.into_iter().map(Ok),
        &CheckpointWorkContext::default(),
    )
    .expect("default projected graph encoding context cannot stop")
}

/// Encode prebuilt projection arrays in bounded numeric chunks. Constructing
/// the projection is the producer's responsibility, not one codec work unit.
#[doc(hidden)]
pub fn encode_projected_graph_artifacts_with_work_context<'a>(
    projection_epoch: u64,
    commit_epoch: u64,
    artifacts: impl IntoIterator<
        Item = Result<(
            &'a str,
            &'a ProjectedGraphDefinition,
            ProjectedGraphArtifactData,
        )>,
    >,
    work: &CheckpointWorkContext,
) -> Result<String> {
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    let mut body = String::new();
    body.push_str("HAWDB_PROJECTED_GRAPHS_V1\n");
    body.push_str(&format!(
        "artifact_version\t{PROJECTED_GRAPH_ARTIFACT_VERSION}\n"
    ));
    body.push_str(&format!("projection_epoch\t{projection_epoch}\n"));
    body.push_str(&format!("commit_epoch\t{commit_epoch}\n"));
    for artifact in artifacts {
        let (name, definition, data) = artifact?;
        {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            body.push_str("graph\t");
            unit.finish();
        }
        append_projected_name(&mut body, name, work)?;
        append_projected_name_list(&mut body, &definition.node_labels, work)?;
        append_projected_name_list(&mut body, &definition.rel_types, work)?;
        {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            body.push('\t');
            unit.finish();
        }
        super::predicate_checkpoint::encode_into(
            &definition.relationship_predicates,
            work,
            |chunk| {
                let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
                body.push_str(std::str::from_utf8(chunk).expect("predicate text is ASCII"));
                unit.finish();
                Ok(())
            },
        )?;
        {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            body.push_str(&format!("\t{}\t{}\n", data.node_count(), data.edge_count()));
            unit.finish();
        }
        append_number_vector(
            &mut body,
            "nodes",
            data.nodes.iter().map(|node| node.0),
            work,
        )?;
        for (name, values) in [
            ("csr_offsets", &data.csr_offsets),
            ("csr_targets", &data.csr_targets),
            ("csc_offsets", &data.csc_offsets),
            ("csc_sources", &data.csc_sources),
        ] {
            append_number_vector(&mut body, name, values.iter().copied(), work)?;
        }
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(body)
}

fn append_projected_name(
    output: &mut String,
    name: &str,
    work: &CheckpointWorkContext,
) -> Result<()> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for block in name.as_bytes().chunks(64 * 1024) {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        for byte in block {
            output.push(char::from(HEX[usize::from(byte >> 4)]));
            output.push(char::from(HEX[usize::from(byte & 15)]));
        }
        unit.finish();
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)
}

fn append_projected_name_list(
    output: &mut String,
    names: &[String],
    work: &CheckpointWorkContext,
) -> Result<()> {
    {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        output.push('\t');
        unit.finish();
    }
    for (index, name) in names.iter().enumerate() {
        {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            if index > 0 {
                output.push(':');
            }
            unit.finish();
        }
        append_projected_name(output, name, work)?;
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)
}

#[cfg(test)]
fn decode_projected_name(input: &str, work: &CheckpointWorkContext) -> Result<String> {
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    if !input.len().is_multiple_of(2) {
        return Err(HawDBError::Storage(format!(
            "invalid hex string length: {}",
            input.len()
        )));
    }
    let mut bytes = Vec::new();
    for start in (0..input.len()).step_by(128 * 1024) {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        for offset in (start..input.len().min(start.saturating_add(128 * 1024))).step_by(2) {
            let byte = input
                .get(offset..offset + 2)
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .ok_or_else(|| {
                    HawDBError::Storage(format!("invalid hex string at byte offset {offset}"))
                })?;
            bytes.push(byte);
        }
        unit.finish();
    }
    let mut decoded = String::new();
    let mut offset = 0;
    while offset < bytes.len() {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let end = bytes.len().min(offset.saturating_add(64 * 1024));
        match std::str::from_utf8(&bytes[offset..end]) {
            Ok(block) => {
                decoded.push_str(block);
                offset = end;
            }
            Err(error) if error.error_len().is_none() && end < bytes.len() => {
                // Retry the incomplete code point with the following block.
                // A UTF-8 sequence is at most four bytes, so the preceding
                // complete prefix advances even at a chunk boundary.
                let valid_end = offset + error.valid_up_to();
                decoded.push_str(
                    std::str::from_utf8(&bytes[offset..valid_end])
                        .expect("UTF-8 validation identified the complete prefix"),
                );
                offset = valid_end;
            }
            Err(error) => {
                let index = offset + error.valid_up_to();
                return Err(HawDBError::Storage(match error.error_len() {
                    Some(length) => {
                        format!("invalid utf-8 sequence of {length} bytes from index {index}")
                    }
                    None => format!("incomplete utf-8 byte sequence from index {index}"),
                }));
            }
        }
        unit.finish();
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(decoded)
}

#[cfg(test)]
fn decode_projected_name_list(input: &str, work: &CheckpointWorkContext) -> Result<Vec<String>> {
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    if input.is_empty() {
        return Ok(Vec::new());
    }
    let mut names = Vec::new();
    for encoded in input.split(':') {
        let name = decode_projected_name(encoded, work)?;
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        names.push(name);
        unit.finish();
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(names)
}

fn append_number_vector(
    body: &mut String,
    name: &str,
    values: impl IntoIterator<Item = impl std::fmt::Display>,
    work: &CheckpointWorkContext,
) -> Result<()> {
    use std::fmt::Write;
    let mut unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    body.push_str(name);
    body.push('\t');
    for (index, value) in values.into_iter().enumerate() {
        if index != 0 && index.is_multiple_of(1024) {
            unit.finish();
            unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        }
        if index != 0 {
            body.push(',');
        }
        write!(body, "{value}").expect("writing a numeric value to String cannot fail");
    }
    body.push('\n');
    unit.finish();
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(())
}

pub fn decode_projected_graph_artifacts(
    body: &str,
) -> Result<(u64, BTreeMap<String, ProjectedGraphArtifact>)> {
    let (epoch, artifacts) = owned::decode(body, &CheckpointWorkContext::default())?;
    Ok((epoch, artifacts.into_unadmitted()))
}

#[doc(hidden)]
pub fn decode_projected_graph_artifacts_with_work_context(
    body: &str,
    work: &CheckpointWorkContext,
) -> Result<(u64, CheckpointProjectedGraphArtifacts)> {
    owned::decode(body, work)
}

struct ProjectedTextLines<'a> {
    input: &'a str,
    offset: usize,
}

impl<'a> ProjectedTextLines<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, offset: 0 }
    }

    fn next(&mut self, work: &CheckpointWorkContext) -> Result<Option<&'a str>> {
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        if self.offset == self.input.len() {
            return Ok(None);
        }
        let start = self.offset;
        while self.offset < self.input.len() {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            let end = self.input.len().min(self.offset.saturating_add(64 * 1024));
            if let Some(relative) = self.input.as_bytes()[self.offset..end]
                .iter()
                .position(|byte| *byte == b'\n')
            {
                let mut line_end = self.offset + relative;
                self.offset = line_end + 1;
                if line_end > start && self.input.as_bytes()[line_end - 1] == b'\r' {
                    line_end -= 1;
                }
                unit.finish();
                return Ok(Some(&self.input[start..line_end]));
            }
            self.offset = end;
            unit.finish();
        }
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        Ok(Some(&self.input[start..]))
    }
}

#[cfg(test)]
fn projected_text_fields<'a>(
    line: &'a str,
    max_fields: usize,
    work: &CheckpointWorkContext,
) -> Result<Vec<&'a str>> {
    let mut fields = Vec::new();
    let mut field_start = 0;
    let mut offset = 0;
    while offset < line.len() {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let end = line.len().min(offset.saturating_add(64 * 1024));
        if let Some(relative) = line.as_bytes()[offset..end]
            .iter()
            .position(|byte| *byte == b'\t')
        {
            let field_end = offset + relative;
            fields.push(&line[field_start..field_end]);
            unit.finish();
            if fields.len() == max_fields {
                return Ok(fields);
            }
            offset = field_end + 1;
            field_start = offset;
        } else {
            offset = end;
            unit.finish();
        }
    }
    let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    fields.push(&line[field_start..]);
    unit.finish();
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(fields)
}

pub fn split_projected_graph_artifact_checksum(text: &str) -> Result<(&str, u64)> {
    let Some((body, footer)) = text.rsplit_once("checksum\t") else {
        return Err(HawDBError::Storage(
            "projected graph artifact missing checksum footer".to_string(),
        ));
    };
    let checksum = parse_u64(footer.trim(), "projected graph artifact checksum")?;
    Ok((body, checksum))
}

#[cfg(test)]
fn decode_number_vector<T>(
    input: &str,
    work: &CheckpointWorkContext,
    parse: impl Fn(&str) -> Result<T>,
) -> Result<Vec<T>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    let mut values = Vec::new();
    let mut unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    for (index, value) in input.split(',').enumerate() {
        if index != 0 && index.is_multiple_of(1024) {
            unit.finish();
            unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        }
        values.push(parse(value)?);
    }
    unit.finish();
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(values)
}

#[cfg(test)]
mod tests;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod memory_tests;

mod owned;
pub(crate) use owned::CheckpointProjectedGraphRoot;
pub use owned::{CheckpointProjectedGraphArtifact, CheckpointProjectedGraphArtifacts};
