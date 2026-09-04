use std::marker::PhantomData;

use crate::{LogicalPlan, PhysicalPlan};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanPhaseKind {
    Logical,
    LoweringReady,
    Physical,
    FastPathPhysical,
}

pub trait PlanPhase {
    const KIND: PlanPhaseKind;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalPhase;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoweringReadyPhase;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalPhase;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FastPathPhysicalPhase;

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalPlanRoot {
    plan: LogicalPlan,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LoweringReadyLogicalPlanRoot {
    plan: LogicalPlan,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalPlanRoot<T, P: PlanPhase = PhysicalPhase> {
    plan: PhysicalPlan,
    metadata: T,
    phase: PhantomData<P>,
}

pub type FastPathPhysicalPlanRoot<T> = PhysicalPlanRoot<T, FastPathPhysicalPhase>;

impl PlanPhase for LogicalPhase {
    const KIND: PlanPhaseKind = PlanPhaseKind::Logical;
}

impl PlanPhase for LoweringReadyPhase {
    const KIND: PlanPhaseKind = PlanPhaseKind::LoweringReady;
}

impl PlanPhase for PhysicalPhase {
    const KIND: PlanPhaseKind = PlanPhaseKind::Physical;
}

impl PlanPhase for FastPathPhysicalPhase {
    const KIND: PlanPhaseKind = PlanPhaseKind::FastPathPhysical;
}

impl LogicalPlanRoot {
    pub fn new(plan: LogicalPlan) -> Self {
        Self { plan }
    }

    pub fn plan(&self) -> &LogicalPlan {
        &self.plan
    }

    pub fn phase(&self) -> PlanPhaseKind {
        LogicalPhase::KIND
    }

    pub fn into_lowering_ready(self) -> LoweringReadyLogicalPlanRoot {
        LoweringReadyLogicalPlanRoot { plan: self.plan }
    }

    pub fn into_plan(self) -> LogicalPlan {
        self.plan
    }
}

impl LoweringReadyLogicalPlanRoot {
    pub fn new(plan: LogicalPlan) -> Self {
        Self { plan }
    }

    pub fn plan(&self) -> &LogicalPlan {
        &self.plan
    }

    pub fn phase(&self) -> PlanPhaseKind {
        LoweringReadyPhase::KIND
    }

    pub fn into_plan(self) -> LogicalPlan {
        self.plan
    }
}

impl<T, P: PlanPhase> PhysicalPlanRoot<T, P> {
    pub fn new(plan: PhysicalPlan, metadata: T) -> Self {
        Self {
            plan,
            metadata,
            phase: PhantomData,
        }
    }

    pub fn plan(&self) -> &PhysicalPlan {
        &self.plan
    }

    pub fn metadata(&self) -> &T {
        &self.metadata
    }

    pub fn trace(&self) -> &T {
        &self.metadata
    }

    pub fn phase(&self) -> PlanPhaseKind {
        P::KIND
    }

    pub fn into_parts(self) -> (PhysicalPlan, T) {
        (self.plan, self.metadata)
    }
}

impl<T> PhysicalPlanRoot<T, PhysicalPhase> {
    pub fn into_fast_path(self) -> FastPathPhysicalPlanRoot<T> {
        PhysicalPlanRoot {
            plan: self.plan,
            metadata: self.metadata,
            phase: PhantomData,
        }
    }
}
