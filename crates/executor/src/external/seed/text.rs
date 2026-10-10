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

use super::*;

pub struct TextSeedScanSpec<'a> {
    pub query_parameter: &'a str,
    pub top_k: usize,
    pub output_external_id: bool,
    pub metadata_filters: &'a BTreeMap<String, String>,
    pub resource_profile: hawdb_plan_cypher::VectorExecutionResourceProfile,
}

impl TextSeedScanSpec<'_> {
    pub fn stream(
        self,
        context: VectorSeedContext<'_>,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let max_rows = self
            .top_k
            .min(execution_limit.output_rows.unwrap_or(usize::MAX));
        if max_rows == 0 {
            return emit_owned_binding_batches(Vec::new(), context.memory.batch_rows.get(), emit);
        }
        let Some(Value::String(query_text)) = context.parameters.get(self.query_parameter) else {
            return Err(HawDBError::Semantic(
                "text search query must be a string parameter".into(),
            ));
        };
        let budget = external_read_memory_budget(self.resource_profile, context.memory);
        let resources = ExternalReadResourceContract {
            priority: self.resource_profile.priority,
            max_parallelism: NonZeroUsize::new(
                self.resource_profile.max_parallelism.max(1).min(
                    context
                        .task_context
                        .map_or(1, |task| task.admitted_parallelism().get()),
                ),
            )
            .unwrap(),
            max_working_memory_bytes: budget.max_working_bytes,
            result: ExternalReadResultBudget {
                max_rows,
                max_memory_bytes: budget.max_result_bytes,
            },
            task_context: context.task_context,
        };
        let total = resources
            .max_working_memory_bytes
            .get()
            .checked_add(resources.result.max_memory_bytes.get())
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| HawDBError::Execution("text external allowance size overflow".into()))?;
        let external = context.memory_ledger.account(
            QueryMemoryClass::ExternalRead,
            "TextSeedScan external read",
            total,
        );
        let admitted = external.sub_account(total)?;
        let working = admitted.sibling(
            QueryMemoryClass::ExternalRead,
            "TextSeedScan working",
            resources.max_working_memory_bytes,
        );
        let result = admitted.sibling(
            QueryMemoryClass::ExternalRead,
            "TextSeedScan result",
            resources.result.max_memory_bytes,
        );
        resources.checkpoint()?;
        let output = context
            .external
            .execute_text_seed(TextSeedExecutionRequest {
                query_text,
                metadata_filters: self.metadata_filters,
                resources,
                working_account: &working,
                result_account: &result,
            })?;
        resources.checkpoint()?;
        output.validate(&result, resources.result)?;
        let binding_account = context.memory_ledger.account(
            QueryMemoryClass::BlockingState,
            "TextSeedScan",
            context.memory.blocking_operator_bytes,
        );
        let mut reservation = binding_account.reserve(0)?;
        let mut bindings = Vec::new();
        for row in output.rows() {
            resources.checkpoint()?;
            // One small value map plus its keys, row/Vec replacement overlap and
            // private score/hop annotations. The payload strings are additional.
            let bytes = 4096usize
                .checked_add(row.id.len())
                .and_then(|n| n.checked_add(row.external_id.as_ref().map_or(0, String::len)))
                .ok_or_else(|| HawDBError::Execution("text seed binding size overflow".into()))?;
            reservation.grow(bytes)?;
            let mut values = BTreeMap::from([
                ("id".into(), Value::String(row.id.clone())),
                (
                    hawdb_plan_cypher::VECTOR_SEED_SCORE_COLUMN.into(),
                    Value::Float(row.score),
                ),
            ]);
            if context.observer.seed_graph_scoring_input().is_some() {
                crate::scoring::annotate_seed(&mut values, row.score);
            }
            if self.output_external_id
                && let Some(id) = &row.external_id
            {
                values.insert("external_id".into(), Value::String(id.clone()));
            }
            bindings.push(Binding {
                values,
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            });
        }
        drop(output);
        drop(result);
        drop(working);
        drop(admitted);
        emit_owned_binding_batches(bindings, context.memory.batch_rows.get(), emit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn text_seed_emission_capacity_fits_the_shared_root_and_releases_on_every_terminal() {
        struct Source {
            calls: Cell<usize>,
        }
        impl BatchExternalRead for Source {
            fn execute_vector_seed(
                &self,
                _: VectorSeedExecutionRequest<'_>,
            ) -> Result<VectorSeedExecutionOutput> {
                unreachable!()
            }
            fn execute_text_seed(
                &self,
                request: TextSeedExecutionRequest<'_>,
            ) -> Result<TextSeedExecutionOutput> {
                self.calls.set(self.calls.get() + 1);
                let mut output =
                    TextSeedExecutionOutput::new(request.result_account, request.resources.result)?;
                for _ in 0..request.resources.result.max_rows {
                    output.push("doc", Some("a"), 1.0)?;
                }
                Ok(output)
            }
        }
        let nz = |n| NonZeroUsize::new(n).unwrap();
        for (window, batch_rows, terminal) in
            [(1, 8192, 0), (3, 2, 0), (3, 2, 1), (3, 2, 2), (0, 8192, 0)]
        {
            let memory = ExecutionMemoryConfig {
                query_memory_bytes: nz(64 * 1024),
                blocking_operator_bytes: nz(16 * 1024),
                batch_rows: nz(batch_rows),
                ..Default::default()
            };
            let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
            let parameters = BTreeMap::from([("text".into(), Value::String("graph".into()))]);
            let filters = BTreeMap::new();
            let observer = QueryExecutionObserver::default();
            let source = Source {
                calls: Cell::new(0),
            };
            let mut emitted = 0;
            let result = TextSeedScanSpec { query_parameter: "text", top_k: window, output_external_id: true, metadata_filters: &filters, resource_profile: hawdb_plan_cypher::VectorExecutionResourceProfile { priority: 1, max_parallelism: 1, max_working_memory_bytes: Some(16 * 1024) } }
                .stream(VectorSeedContext { parameters: &parameters, external: &source, memory: &memory, memory_ledger: &ledger, task_context: None, observer: &observer }, ExecutionLimit::unlimited(), &mut |batch| {
                    emitted += batch.len();
                    let actual_slots = batch.capacity().checked_mul(std::mem::size_of::<Binding>()).unwrap();
                    assert!(actual_slots <= ledger.snapshot().used_bytes, "actual emitted binding capacity {actual_slots} exceeds retained query charge {}", ledger.snapshot().used_bytes);
                    assert!(actual_slots <= memory.query_memory_bytes.get(), "actual batch exceeds query root");
                    match terminal { 1 => Ok(BatchControl::Stop), 2 => Err(HawDBError::Execution("consumer refused".into())), _ => Ok(BatchControl::Continue) }
                });
            match terminal {
                1 => assert_eq!(result.unwrap(), BatchControl::Stop),
                2 => assert!(matches!(result, Err(HawDBError::Execution(_)))),
                _ => assert_eq!(result.unwrap(), BatchControl::Continue),
            }
            assert_eq!(emitted, if terminal == 0 { window } else { batch_rows });
            assert_eq!(source.calls.get(), usize::from(window > 0));
            assert_eq!(
                ledger.snapshot().used_bytes,
                0,
                "lease leaked after terminal {terminal}"
            );
            assert!(ledger.snapshot().peak_bytes <= memory.query_memory_bytes.get());
        }
    }
}
