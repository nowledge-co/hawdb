//! Storage-neutral inputs for one executor invocation.

use crate::result_delivery::OutputLimits;
use crate::ExecutionMemoryConfig;
use skein_core::{RuntimeTaskContext, Value};
use skein_plan::PhysicalPlan;
use std::collections::BTreeMap;

#[derive(Clone, Copy)]
pub struct ExecutionRequest<'a> {
    plan: &'a PhysicalPlan,
    parameters: &'a BTreeMap<String, Value>,
    output_limits: OutputLimits,
    memory: &'a ExecutionMemoryConfig,
    task_context: Option<&'a RuntimeTaskContext>,
}

impl<'a> ExecutionRequest<'a> {
    pub fn new(
        plan: &'a PhysicalPlan,
        parameters: &'a BTreeMap<String, Value>,
        memory: &'a ExecutionMemoryConfig,
    ) -> Self {
        Self {
            plan,
            parameters,
            output_limits: OutputLimits::default(),
            memory,
            task_context: None,
        }
    }

    pub fn with_output_limits(
        mut self,
        max_rows: Option<usize>,
        max_payload_bytes: Option<usize>,
    ) -> Self {
        self.output_limits = OutputLimits {
            max_rows,
            max_payload_bytes,
        };
        self
    }

    pub fn with_optional_task_context(
        mut self,
        task_context: Option<&'a RuntimeTaskContext>,
    ) -> Self {
        self.task_context = task_context;
        self
    }

    pub fn with_task_context(mut self, task_context: &'a RuntimeTaskContext) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub fn plan(self) -> &'a PhysicalPlan {
        self.plan
    }

    pub fn parameters(self) -> &'a BTreeMap<String, Value> {
        self.parameters
    }

    pub fn output_limits(self) -> OutputLimits {
        self.output_limits
    }

    pub fn memory(self) -> &'a ExecutionMemoryConfig {
        self.memory
    }

    pub fn task_context(self) -> Option<&'a RuntimeTaskContext> {
        self.task_context
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_preserves_storage_neutral_execution_inputs() {
        let plan = PhysicalPlan::EmptyExec;
        let parameters = BTreeMap::from([("limit".to_string(), Value::Int(7))]);
        let memory = ExecutionMemoryConfig::default();
        let task_context = RuntimeTaskContext::default();

        let request = ExecutionRequest::new(&plan, &parameters, &memory)
            .with_output_limits(Some(4), Some(1024))
            .with_task_context(&task_context);

        assert!(matches!(request.plan(), PhysicalPlan::EmptyExec));
        assert!(std::ptr::eq(request.parameters(), &parameters));
        assert_eq!(request.output_limits().max_rows, Some(4));
        assert_eq!(request.output_limits().max_payload_bytes, Some(1024));
        assert!(std::ptr::eq(request.memory(), &memory));
        assert!(std::ptr::eq(request.task_context().unwrap(), &task_context));
    }
}
