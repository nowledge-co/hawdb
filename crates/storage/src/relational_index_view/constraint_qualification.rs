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

//! Relational constraint qualification report contracts.

use crate::relational_index_view::RelationalIndexReadViewReport;
use crate::RelationalKey;

pub const RELATIONAL_CONSTRAINT_QUALIFICATION_PROTOCOL: &str =
    "hawdb-relational-constraint-qualification-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RelationalConstraintQualificationUse {
    PrimaryKeyIdentity,
    UniqueEnforcement,
    UpsertConflict,
    ForeignKeyTarget,
    ForeignKeyReferrers,
    NullableUniqueNoConflict,
    AbsentKeyNoConflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalConstraintQualificationProbeReport {
    pub ordinal: usize,
    pub table: String,
    pub index: String,
    pub uses: Vec<RelationalConstraintQualificationUse>,
    pub key_digest: String,
    pub candidate_rows: usize,
    pub oracle_rows: usize,
    pub candidate_digest: String,
    pub oracle_digest: String,
    pub semantic_valid: bool,
    pub matched: bool,
    pub read: RelationalIndexReadViewReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalConstraintQualificationReport {
    pub protocol: &'static str,
    pub base_generation: u64,
    pub delta_generation: Option<u64>,
    pub base_commit_epoch: u64,
    pub visible_commit_epoch: u64,
    pub root_set_digest: String,
    pub tables_discovered: usize,
    pub tables_sampled: usize,
    pub rows_sampled: usize,
    pub unique_targets_discovered: usize,
    pub nullable_unique_targets_discovered: usize,
    pub foreign_keys_discovered: usize,
    pub probes: Vec<RelationalConstraintQualificationProbeReport>,
    pub uses_covered: usize,
    pub mismatches: usize,
    pub truncated: bool,
    pub ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConstraintProbeIdentity {
    pub table: String,
    pub index: String,
    pub key: RelationalKey,
}
