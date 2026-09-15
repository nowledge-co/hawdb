use crate::error::Result;
use crate::store::{GraphStore, RelationalTransactionRowView};
use skein_core::RuntimeTaskContext;
use skein_relational::field_plan::RelationalFieldPlan;
use skein_storage::{
    ProjectionGenerationReader, RelationalHydrationBudget, RelationalRowPageSnapshotReadLimits,
    RelationalState,
};
use std::collections::BTreeSet;

pub(super) use skein_relational::row_runtime::RelationalReadRow;
pub(crate) use skein_relational::row_runtime::{
    RelationalRowExecutionEvidence, RelationalRowRuntime,
};

#[derive(Debug, Clone, Copy)]
pub(crate) enum RelationalRowReadMode<'a> {
    CanonicalMemory,
    Store(&'a GraphStore),
    Transaction {
        store: &'a GraphStore,
        rows: &'a RelationalTransactionRowView,
    },
    ProjectionGeneration {
        store: &'a GraphStore,
        reader: &'a ProjectionGenerationReader,
        tables: &'a BTreeSet<String>,
    },
}

impl<'a> RelationalRowReadMode<'a> {
    pub(crate) fn is_projection_table(self, table: &str) -> bool {
        matches!(
            self,
            Self::ProjectionGeneration { tables, .. } if tables.contains(table)
        )
    }

    pub(crate) fn projection_estimated_rows(self, table: &str) -> Option<usize> {
        match self {
            Self::ProjectionGeneration { reader, tables, .. } if tables.contains(table) => {
                Some(usize::try_from(reader.manifest().member_count).unwrap_or(usize::MAX))
            }
            _ => None,
        }
    }

    pub(crate) fn open_runtime(
        self,
        state: &'a RelationalState,
        fields: RelationalFieldPlan,
        limits: RelationalRowPageSnapshotReadLimits,
        hydration: RelationalHydrationBudget,
        task: &'a RuntimeTaskContext,
    ) -> Result<RelationalRowRuntime<'a>> {
        let projection = match self {
            Self::ProjectionGeneration { reader, tables, .. } => Some((reader, tables)),
            _ => None,
        };
        let snapshot = match self {
            Self::CanonicalMemory => None,
            Self::Store(store) | Self::ProjectionGeneration { store, .. } => {
                store.open_relational_row_snapshot_reader()?
            }
            Self::Transaction { store, rows } => {
                Some(store.open_relational_transaction_row_snapshot_reader(rows)?)
            }
        };
        Ok(RelationalRowRuntime::new(
            state, snapshot, projection, fields, limits, hydration, task,
        ))
    }
}
