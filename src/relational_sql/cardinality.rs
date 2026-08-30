/// Stable, plan-local identifier for a relational access or join operator.
///
/// Identifiers are one-based. The base access is always operator 1, followed
/// by join operators in execution order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelationalOperatorId(usize);

impl RelationalOperatorId {
    pub(crate) const fn from_plan_index(index: usize) -> Self {
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
    IndexNestedLoopJoin,
    IndexNestedLoopLeftJoin,
}

impl RelationalOperatorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TableFullScan => "TableFullScanExec",
            Self::TablePointGet => "TablePointGetExec",
            Self::IndexRangeScan => "IndexRangeScanExec",
            Self::IndexNestedLoopJoin => "IndexNestedLoopJoinExec",
            Self::IndexNestedLoopLeftJoin => "IndexNestedLoopLeftJoinExec",
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
    pub estimated_rows: usize,
    /// `None` means execution never invoked the access pipeline. `Some(0)`
    /// means the operator ran and produced no rows.
    pub actual_rows: Option<usize>,
    /// True only when execution observed the end of the operator pipeline.
    pub fully_consumed: bool,
}
