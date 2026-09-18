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

/// Stable, plan-local identifier for a physical relational access or join operator.
///
/// Identifiers are one-based. They identify profile entries, not logical SQL
/// wrappers such as projection or limit. A profile carries its own access path
/// so an identifier cannot be reinterpreted through an independently ordered
/// descriptor list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelationalOperatorId(usize);

impl RelationalOperatorId {
    pub const fn from_plan_index(index: usize) -> Self {
        Self(index.saturating_add(1))
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalOperatorKind {
    TableFullScan,
    TablePointGet,
    IndexRangeScan,
    NestedLoopJoin,
    NestedLoopLeftJoin,
    IndexNestedLoopJoin,
    IndexNestedLoopLeftJoin,
    BatchedIndexNestedLoopJoin,
    BatchedIndexNestedLoopLeftJoin,
    MergeJoin,
    HashJoin,
}

impl RelationalOperatorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TableFullScan => "TableFullScanExec",
            Self::TablePointGet => "TablePointGetExec",
            Self::IndexRangeScan => "IndexRangeScanExec",
            Self::NestedLoopJoin => "NestedLoopJoinExec",
            Self::NestedLoopLeftJoin => "NestedLoopLeftJoinExec",
            Self::IndexNestedLoopJoin => "IndexNestedLoopJoinExec",
            Self::IndexNestedLoopLeftJoin => "IndexNestedLoopLeftJoinExec",
            Self::BatchedIndexNestedLoopJoin => "BatchedIndexNestedLoopJoinExec",
            Self::BatchedIndexNestedLoopLeftJoin => "BatchedIndexNestedLoopLeftJoinExec",
            Self::MergeJoin => "MergeJoinExec",
            Self::HashJoin => "HashJoinExec",
        }
    }
}

/// Estimated and observed output cardinality at one relational operator
/// boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalOperatorCardinalityProfile {
    pub operator_id: RelationalOperatorId,
    pub operator: RelationalOperatorKind,
    pub table: String,
    /// The physical access path executed by this operator.
    pub access_path: crate::RelationalAccessPathDescriptor,
    pub estimated_rows: usize,
    /// `None` means execution never invoked the access pipeline. `Some(0)`
    /// means the operator ran and produced no rows.
    pub actual_rows: Option<usize>,
    /// True only when execution observed the end of the operator pipeline.
    pub fully_consumed: bool,
}
