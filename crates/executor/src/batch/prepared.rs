use super::*;

/// The execution-facing form of a physical plan.
///
/// Planning owns operator selection. This boundary performs the one-time
/// recursive capability check that decides whether a read can enter the
/// streaming batch engine, leaving mutation and schema plans on the
/// materialized path. It deliberately borrows the planner-owned tree so plan
/// cache templates remain the sole owner of physical plan structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedExecutionMode {
    Batch,
    Materialized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedStorageCapability {
    InMemory,
    OutOfCore,
}

#[derive(Clone, Copy)]
pub struct PreparedPhysicalPlan<'a> {
    plan: &'a PhysicalPlan,
    batch_plan: Option<BatchPlanRef<'a>>,
    execution_mode: PreparedExecutionMode,
    storage_capability: PreparedStorageCapability,
    required_memory: ExecutionMemoryEstimate,
}

impl<'a> PreparedPhysicalPlan<'a> {
    pub fn prepare(
        plan: &'a PhysicalPlan,
        store: &dyn crate::store::GraphExecutionRead,
        memory: &ExecutionMemoryConfig,
    ) -> Self {
        let batch_plan = BatchPlanRef::try_new(plan);
        Self {
            plan,
            execution_mode: if batch_plan.is_some() {
                PreparedExecutionMode::Batch
            } else {
                PreparedExecutionMode::Materialized
            },
            batch_plan,
            storage_capability: if store.is_out_of_core() {
                PreparedStorageCapability::OutOfCore
            } else {
                PreparedStorageCapability::InMemory
            },
            required_memory: estimated_execution_memory(plan, memory),
        }
    }

    pub fn batch(self) -> Option<BatchPlanRef<'a>> {
        match self.execution_mode() {
            PreparedExecutionMode::Batch => self.batch_plan,
            PreparedExecutionMode::Materialized => None,
        }
    }

    pub fn plan(self) -> &'a PhysicalPlan {
        self.plan
    }

    pub fn execution_mode(self) -> PreparedExecutionMode {
        self.execution_mode
    }

    pub fn storage_capability(self) -> PreparedStorageCapability {
        self.storage_capability
    }

    pub fn required_memory(self) -> ExecutionMemoryEstimate {
        self.required_memory
    }
}
