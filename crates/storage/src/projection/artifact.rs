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

//! Existing v1 projected graph artifact text format.
//!
//! The facade owns graph construction, file publication, epoch admission, and
//! recovery fallback. This module only encodes and validates storage data.

use super::{ProjectedGraphArtifact, ProjectedGraphArtifactData, ProjectedGraphDefinition};
use crate::background::CheckpointWorkContext;
use crate::text::{decode_string, decode_string_vec, encode_string, encode_string_vec, parse_u64};
use crate::NodeId;
use hawdb_core::{HawDBError, Result};
use std::collections::BTreeMap;

const PROJECTED_GRAPH_ARTIFACT_VERSION: u64 = 1;

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
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        body.push_str(&format!(
            "graph\t{}\t{}\t{}\t{}\t{}\n",
            encode_string(name),
            encode_string_vec(&definition.node_labels),
            encode_string_vec(&definition.rel_types),
            data.node_count(),
            data.edge_count()
        ));
        unit.finish();
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
    decode_projected_graph_artifacts_with_work_context(body, &CheckpointWorkContext::default())
}

#[doc(hidden)]
pub fn decode_projected_graph_artifacts_with_work_context(
    body: &str,
    work: &CheckpointWorkContext,
) -> Result<(u64, BTreeMap<String, ProjectedGraphArtifact>)> {
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    let mut lines = body.lines();
    match lines.next() {
        Some("HAWDB_PROJECTED_GRAPHS_V1") => {}
        _ => {
            return Err(HawDBError::Storage(
                "invalid projected graph artifact header".to_string(),
            ));
        }
    }
    let artifact_version = decode_projected_graph_u64_header(
        lines.next(),
        "artifact_version",
        "projected graph artifact version",
    )?;
    if artifact_version != PROJECTED_GRAPH_ARTIFACT_VERSION {
        return Err(HawDBError::Storage(format!(
            "unsupported projected graph artifact version: {artifact_version}"
        )));
    }
    let projection_epoch = decode_projected_graph_u64_header(
        lines.next(),
        "projection_epoch",
        "projected graph artifact projection epoch",
    )?;
    let commit_epoch = decode_projected_graph_u64_header(
        lines.next(),
        "commit_epoch",
        "projected graph artifact commit epoch",
    )?;

    let mut artifacts = BTreeMap::new();
    while let Some(line) = lines.next() {
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["graph", raw_name, raw_node_labels, raw_rel_types, raw_node_count, raw_edge_count] => {
                let name = decode_string(raw_name)?;
                let definition = ProjectedGraphDefinition {
                    node_labels: decode_string_vec(raw_node_labels)?,
                    rel_types: decode_string_vec(raw_rel_types)?,
                };
                let node_count = parse_u64(raw_node_count, "projected graph artifact node count")?;
                let edge_count = parse_u64(raw_edge_count, "projected graph artifact edge count")?;
                let nodes = decode_projected_graph_nodes_line(lines.next(), work)?;
                let csr_offsets =
                    decode_projected_graph_usize_line(lines.next(), "csr_offsets", work)?;
                let csr_targets =
                    decode_projected_graph_usize_line(lines.next(), "csr_targets", work)?;
                let csc_offsets =
                    decode_projected_graph_usize_line(lines.next(), "csc_offsets", work)?;
                let csc_sources =
                    decode_projected_graph_usize_line(lines.next(), "csc_sources", work)?;
                if nodes.len() as u64 != node_count {
                    return Err(HawDBError::Storage(format!(
                        "projected graph artifact node count mismatch for {name}"
                    )));
                }
                if csr_targets.len() as u64 != edge_count || csc_sources.len() as u64 != edge_count
                {
                    return Err(HawDBError::Storage(format!(
                        "projected graph artifact edge count mismatch for {name}"
                    )));
                }
                let data = ProjectedGraphArtifactData::new_with_work_context(
                    nodes,
                    csr_offsets,
                    csr_targets,
                    csc_offsets,
                    csc_sources,
                    work,
                )?;
                artifacts.insert(
                    name,
                    ProjectedGraphArtifact {
                        projection_epoch,
                        commit_epoch,
                        definition,
                        data,
                    },
                );
            }
            [""] => {}
            _ => {
                return Err(HawDBError::Storage(format!(
                    "invalid projected graph artifact line: {line}"
                )));
            }
        }
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok((commit_epoch, artifacts))
}

fn decode_projected_graph_u64_header(
    line: Option<&str>,
    expected: &str,
    name: &str,
) -> Result<u64> {
    let Some(line) = line else {
        return Err(HawDBError::Storage(format!(
            "missing projected graph artifact {expected}"
        )));
    };
    let fields = line.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        [field, raw] if *field == expected => parse_u64(raw, name),
        _ => Err(HawDBError::Storage(format!(
            "invalid projected graph artifact line: {line}"
        ))),
    }
}

fn decode_projected_graph_nodes_line(
    line: Option<&str>,
    work: &CheckpointWorkContext,
) -> Result<Vec<NodeId>> {
    let Some(line) = line else {
        return Err(HawDBError::Storage(
            "missing projected graph artifact nodes line".to_string(),
        ));
    };
    let fields = line.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        ["nodes", raw_values] => decode_number_vector(raw_values, work, |value| {
            parse_u64(value, "projected graph artifact node id").map(NodeId)
        }),
        _ => Err(HawDBError::Storage(format!(
            "invalid projected graph artifact line: {line}"
        ))),
    }
}

fn decode_projected_graph_usize_line(
    line: Option<&str>,
    expected: &str,
    work: &CheckpointWorkContext,
) -> Result<Vec<usize>> {
    let Some(line) = line else {
        return Err(HawDBError::Storage(format!(
            "missing projected graph artifact {expected} line"
        )));
    };
    let fields = line.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        [name, raw_values] if *name == expected => {
            decode_number_vector(raw_values, work, |value| {
                value.parse().map_err(|_| {
                    HawDBError::Storage(format!("invalid projected graph artifact index: {value}"))
                })
            })
        }
        _ => Err(HawDBError::Storage(format!(
            "invalid projected graph artifact line: {line}"
        ))),
    }
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
