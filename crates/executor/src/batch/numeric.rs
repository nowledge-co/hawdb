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

//! Batch context adapter for executor-owned numeric read fragments.

use super::*;
use crate::numeric::{self, NumericExecutionContext};

impl<'a> BatchReadContext<'a> {
    pub fn numeric_context(self) -> NumericExecutionContext<'a> {
        NumericExecutionContext {
            catalog: self.catalog,
            store: self.store,
            memory: self.memory,
            memory_ledger: self.memory_ledger,
            task_context: self.task_context,
            observer: self.observer,
        }
    }
}

pub(super) fn try_stream_columnar_projection_batches(
    items: &[Projection],
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Option<Result<BatchControl>> {
    numeric::try_stream_columnar_projection_batches(
        items,
        input,
        context.numeric_context(),
        execution_limit,
        emit,
    )
}

pub(super) fn try_stream_columnar_node_projection_batches(
    variable: &str,
    label: &str,
    predicate: Option<&Predicate>,
    items: &[Projection],
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Option<Result<BatchControl>> {
    numeric::try_stream_columnar_node_projection_batches(
        variable,
        label,
        predicate,
        items,
        context.numeric_context(),
        execution_limit,
        emit,
    )
}
