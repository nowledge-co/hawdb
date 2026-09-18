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
use crate::text::{
    decode_string, decode_string_vec, decode_u64_vec, encode_string, encode_string_vec,
    encode_u64_vec, parse_u64,
};
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
    let mut body = String::new();
    body.push_str("HAWDB_PROJECTED_GRAPHS_V1\n");
    body.push_str(&format!(
        "artifact_version\t{PROJECTED_GRAPH_ARTIFACT_VERSION}\n"
    ));
    body.push_str(&format!("projection_epoch\t{projection_epoch}\n"));
    body.push_str(&format!("commit_epoch\t{commit_epoch}\n"));
    for (name, definition, data) in artifacts {
        body.push_str(&format!(
            "graph\t{}\t{}\t{}\t{}\t{}\n",
            encode_string(name),
            encode_string_vec(&definition.node_labels),
            encode_string_vec(&definition.rel_types),
            data.node_count(),
            data.edge_count()
        ));
        body.push_str(&format!(
            "nodes\t{}\n",
            encode_u64_vec(data.nodes.iter().map(|node| node.0))
        ));
        body.push_str(&format!(
            "csr_offsets\t{}\n",
            encode_usize_vec(data.csr_offsets.iter().copied())
        ));
        body.push_str(&format!(
            "csr_targets\t{}\n",
            encode_usize_vec(data.csr_targets.iter().copied())
        ));
        body.push_str(&format!(
            "csc_offsets\t{}\n",
            encode_usize_vec(data.csc_offsets.iter().copied())
        ));
        body.push_str(&format!(
            "csc_sources\t{}\n",
            encode_usize_vec(data.csc_sources.iter().copied())
        ));
    }
    body
}

pub fn decode_projected_graph_artifacts(
    body: &str,
) -> Result<(u64, BTreeMap<String, ProjectedGraphArtifact>)> {
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
                let nodes = decode_projected_graph_nodes_line(lines.next())?;
                let csr_offsets = decode_projected_graph_usize_line(lines.next(), "csr_offsets")?;
                let csr_targets = decode_projected_graph_usize_line(lines.next(), "csr_targets")?;
                let csc_offsets = decode_projected_graph_usize_line(lines.next(), "csc_offsets")?;
                let csc_sources = decode_projected_graph_usize_line(lines.next(), "csc_sources")?;
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
                let data = ProjectedGraphArtifactData::new(
                    nodes,
                    csr_offsets,
                    csr_targets,
                    csc_offsets,
                    csc_sources,
                )
                .map_err(HawDBError::Storage)?;
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

fn decode_projected_graph_nodes_line(line: Option<&str>) -> Result<Vec<NodeId>> {
    let Some(line) = line else {
        return Err(HawDBError::Storage(
            "missing projected graph artifact nodes line".to_string(),
        ));
    };
    let fields = line.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        ["nodes", raw_values] => decode_u64_vec(raw_values, "projected graph artifact node id")
            .map(|nodes| nodes.into_iter().map(NodeId).collect()),
        _ => Err(HawDBError::Storage(format!(
            "invalid projected graph artifact line: {line}"
        ))),
    }
}

fn decode_projected_graph_usize_line(line: Option<&str>, expected: &str) -> Result<Vec<usize>> {
    let Some(line) = line else {
        return Err(HawDBError::Storage(format!(
            "missing projected graph artifact {expected} line"
        )));
    };
    let fields = line.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        [name, raw_values] if *name == expected => {
            decode_usize_vec(raw_values, "projected graph artifact index")
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

fn encode_usize_vec(values: impl IntoIterator<Item = usize>) -> String {
    values
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_usize_vec(input: &str, name: &str) -> Result<Vec<usize>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input
        .split(',')
        .map(|value| {
            value
                .parse()
                .map_err(|_| HawDBError::Storage(format!("invalid {name}: {value}")))
        })
        .collect()
}

#[cfg(test)]
mod tests;
