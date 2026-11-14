// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use query_workloads::cardinality::{self, CaseMetadata};
use query_workloads::smith::{self, Case, Fixture, Limits, Mode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Read, Write};
use std::path::Path;

pub const MAX_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Frontend {
    #[default]
    Sql,
    Cypher,
}
impl Frontend {
    fn is_sql(&self) -> bool {
        *self == Self::Sql
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cell {
    pub index: usize,
    pub mode: Mode,
    pub case: Option<Case>,
    pub refusal: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cardinality: Option<CaseMetadata>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub protocol: String,
    #[serde(default, skip_serializing_if = "Frontend::is_sql")]
    pub frontend: Frontend,
    pub seed: u64,
    pub limits: Limits,
    pub fixture: Fixture,
    pub cells: Vec<Cell>,
    pub attempt_timeout_ms: u64,
    pub campaign_timeout_ms: u64,
    pub work_limit: usize,
    pub memory_bytes: u64,
    pub intermediate_row_limit: usize,
    pub warmups: usize,
    pub repetitions: usize,
    pub schedule_seed: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Frozen {
    pub sha256: String,
    pub manifest: Manifest,
}

pub fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

impl Frozen {
    pub fn generate(seed: u64, cases: usize, rows: usize, limits: Limits) -> Result<Self, String> {
        if !(1..=512).contains(&cases) {
            return Err("cases must be in 1..=512 per mode".into());
        }
        limits.validate()?;
        let fixture = smith::fixture(rows)?;
        // Reserve for repeated dimension scans and unfavorable join orders,
        // independently of the much smaller delivered-output bound.
        let intermediate_row_limit = rows
            .saturating_add(8)
            .saturating_mul(8usize.pow((limits.max_relations - 1) as u32))
            .saturating_mul(limits.max_relations)
            .clamp(1024, 100_000_000);
        let mut cells = Vec::with_capacity(cases * 2);
        for index in 0..cases {
            for mode in [Mode::Template, Mode::Recursive] {
                let generated = smith::generate(seed, index, mode, &limits);
                cells.push(match generated {
                    Ok(case) => Cell {
                        index,
                        mode,
                        case: Some(case),
                        refusal: None,
                        cardinality: None,
                    },
                    Err(refusal) => Cell {
                        index,
                        mode,
                        case: None,
                        refusal: Some(refusal),
                        cardinality: None,
                    },
                });
            }
        }
        Self::seal(Manifest {
            protocol: smith::PROTOCOL.into(),
            frontend: Frontend::Sql,
            seed,
            limits,
            fixture,
            cells,
            attempt_timeout_ms: 5000,
            campaign_timeout_ms: 120_000,
            work_limit: 10_000_000,
            memory_bytes: 256 * 1024 * 1024,
            intermediate_row_limit,
            warmups: 3,
            repetitions: 15,
            schedule_seed: seed ^ 0x5eed,
        })
    }
    pub fn cardinality(seed: u64, rows: usize) -> Result<Self, String> {
        let workload = cardinality::generate(seed, rows)?;
        Self::seal(Manifest {
            protocol: cardinality::PROTOCOL.into(),
            frontend: Frontend::Sql,
            seed,
            limits: Limits {
                max_output_rows: 1024,
                max_output_bytes: 1_048_576,
                ..Limits::default()
            },
            fixture: workload.fixture,
            cells: workload
                .cases
                .into_iter()
                .map(|(case, metadata)| Cell {
                    index: case.index,
                    mode: case.mode,
                    case: Some(case),
                    refusal: None,
                    cardinality: Some(metadata),
                })
                .collect(),
            attempt_timeout_ms: 5000,
            campaign_timeout_ms: 120_000,
            work_limit: 10_000_000,
            memory_bytes: 256 * 1024 * 1024,
            intermediate_row_limit: 1_000_000,
            warmups: 3,
            repetitions: 15,
            schedule_seed: seed ^ 0x5eed,
        })
    }
    pub fn graph_statistics(seed: u64, rows: usize) -> Result<Self, String> {
        let mut frozen = Self::cardinality(seed, 128)?;
        let workload = query_workloads::compressed_statistics::generate(seed, rows)?;
        frozen.manifest.protocol = query_workloads::compressed_statistics::PROTOCOL.into();
        frozen.manifest.frontend = Frontend::Cypher;
        frozen.manifest.fixture = workload.fixture;
        frozen.manifest.cells = workload
            .cases
            .into_iter()
            .map(|(case, metadata)| Cell {
                index: case.index,
                mode: case.mode,
                case: Some(case),
                refusal: None,
                cardinality: Some(metadata),
            })
            .collect();
        Self::seal(frozen.manifest)
    }
    pub fn seal(manifest: Manifest) -> Result<Self, String> {
        let sha256 = digest(&serde_json::to_vec(&manifest).map_err(|e| e.to_string())?);
        let frozen = Self { sha256, manifest };
        frozen.validate()?;
        Ok(frozen)
    }
    pub fn validate(&self) -> Result<(), String> {
        let m = &self.manifest;
        m.limits.validate()?;
        let graph = m.protocol == query_workloads::compressed_statistics::PROTOCOL;
        if (m.protocol != smith::PROTOCOL && m.protocol != cardinality::PROTOCOL && !graph)
            || (graph != (m.frontend == Frontend::Cypher))
            || (graph && !(64..=1024).contains(&m.fixture.rows))
            || m.cells.is_empty()
            || m.cells.len() > 1024
            || !(8..=100_000).contains(&m.fixture.rows)
            || m.fixture.setup.len() > 100_020
            || m.fixture.setup.is_empty()
            || !(1..=30_000).contains(&m.attempt_timeout_ms)
            || !(1..=3_600_000).contains(&m.campaign_timeout_ms)
            || !(1..=100_000_000).contains(&m.work_limit)
            || !(1024..=536_870_912).contains(&m.memory_bytes)
            || !(1..=100_000_000).contains(&m.intermediate_row_limit)
            || m.warmups != 3
            || m.repetitions != 15
        {
            return Err("manifest protocol or campaign budget is invalid".into());
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut ce_ids = std::collections::BTreeSet::new();
        for cell in &m.cells {
            match (&cell.cardinality, m.protocol.as_str()) {
                (Some(meta), _)
                    if (m.protocol == cardinality::PROTOCOL || graph)
                        && cell.mode == Mode::Template
                        && cell.case.is_some()
                        && !meta.id.is_empty()
                        && meta.id.len() <= 256
                        && !meta.cohort.is_empty()
                        && meta.cohort.len() <= 128
                        && !meta.family.is_empty()
                        && meta.family.len() <= 128
                        && meta.expected_result_rows <= m.limits.max_output_rows
                        && ce_ids.insert(&meta.id) => {}
                (None, smith::PROTOCOL) => {}
                _ => {
                    return Err(
                        "invalid cardinality case metadata or duplicate case identity".into(),
                    )
                }
            }
            if !ids.insert((cell.index, format!("{:?}", cell.mode))) {
                return Err("duplicate manifest cell".into());
            }
            if graph
                && cell.case.as_ref().is_none_or(|case| {
                    !query_workloads::compressed_statistics::complete_query(&case.statement.sql)
                })
            {
                return Err(
                    "graph statistics manifest requires a complete-read query skeleton".into(),
                );
            }
            match (&cell.case, &cell.refusal) {
                (Some(c), None)
                    if c.index == cell.index
                        && c.mode == cell.mode
                        && c.nodes > 0
                        && c.nodes <= m.limits.max_nodes
                        && c.depth > 0
                        && c.depth <= m.limits.max_depth
                        && c.relations > 0
                        && c.relations <= m.limits.max_relations
                        && c.limit > 0
                        && c.limit <= m.limits.max_output_rows
                        && c.statement.sql.len() <= 65_536
                        && c.statement.parameters.len() <= m.limits.max_nodes => {}
                (None, Some(_)) => {}
                _ => {
                    return Err(
                        "manifest cell has inconsistent evidence or exceeds its bounds".into(),
                    )
                }
            }
        }
        for cell in &m.cells {
            let peer = match cell.mode {
                Mode::Template => Mode::Recursive,
                Mode::Recursive => Mode::Template,
            };
            if m.protocol == smith::PROTOCOL && !ids.contains(&(cell.index, format!("{peer:?}"))) {
                return Err("manifest must retain both modes for every case index".into());
            }
        }
        let bytes = serde_json::to_vec(m).map_err(|e| e.to_string())?;
        if bytes.len() as u64 > MAX_ARTIFACT_BYTES || digest(&bytes) != self.sha256 {
            return Err("manifest digest mismatch or artifact budget exceeded".into());
        }
        Ok(())
    }
    pub fn load(path: &Path) -> Result<Self, String> {
        let mut data = Vec::new();
        fs::File::open(path)
            .map_err(|e| e.to_string())?
            .take(MAX_ARTIFACT_BYTES + 1)
            .read_to_end(&mut data)
            .map_err(|e| e.to_string())?;
        if data.len() as u64 > MAX_ARTIFACT_BYTES {
            return Err("manifest artifact budget exceeded".into());
        }
        let frozen: Self = serde_json::from_slice(&data).map_err(|e| e.to_string())?;
        frozen.validate()?;
        Ok(frozen)
    }
    pub fn save(&self, path: &Path) -> Result<(), String> {
        self.validate()?;
        write_new(path, &serde_json::to_vec(self).map_err(|e| e.to_string())?)
    }
}

pub fn write_new(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    file.write_all(bytes).map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn frozen_workload_retains_exact_typed_values_and_rejects_changes() {
        let frozen = Frozen::generate(7, 12, 32, Limits::default()).unwrap();
        let bytes = serde_json::to_vec(&frozen).unwrap();
        let mut loaded: Frozen = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(frozen, loaded);
        loaded.manifest.cells[0]
            .case
            .as_mut()
            .unwrap()
            .statement
            .parameters
            .clear();
        assert!(loaded.validate().is_err());
        let mut missing = frozen.manifest.clone();
        missing.cells[0].case = None;
        assert!(Frozen::seal(missing).is_err());
    }
}
