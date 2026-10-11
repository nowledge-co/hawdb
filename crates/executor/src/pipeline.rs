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

//! Streaming pipeline control and batch emission helpers.

use crate::binding::{binding_memory_bytes, Binding};
use crate::kernel::OperatorMemoryTracker;
use crate::observer::ExecutionObserver;
use crate::{
    ExecutionLimit, ExecutionMemoryConfig, QueryMemoryAccount, QueryMemoryClass, QueryMemoryLease,
    QueryMemoryLedger,
};
use hawdb_core::{Catalog, HawDBError, Result, RuntimeTaskContext};
use hawdb_plan_cypher::PhysicalPlan;
use std::num::NonZeroUsize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchControl {
    Continue,
    Stop,
}

pub type BindingBatch = Vec<Binding>;

/// Call after admitting the next row's storage. Spare configured slots are
/// not part of that admission; grow only for the row about to be inserted.
pub(crate) fn reserve_binding_slot(bindings: &mut BindingBatch) {
    if bindings.len() == bindings.capacity() {
        bindings.reserve_exact(1);
    }
}

/// Recursively executes an input while honoring the requested row cap and
/// propagating consumer stop/error without emitting subsequent batches.
///
/// Generic over the plan-node type `P` so a caller that does not walk a
/// `hawdb_plan_cypher::PhysicalPlan` tree (e.g. `hawdb-relational`, which drives its
/// own row sources) can implement this against a zero-sized marker instead of
/// fabricating a placeholder graph plan node. Graph call sites are unaffected
/// by the default.
pub trait BindingBatchSource<P = PhysicalPlan> {
    fn execute(
        &mut self,
        input: &P,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl>;
}

#[derive(Clone, Copy)]
pub struct BatchExecutionContext<'a> {
    pub catalog: &'a Catalog,
    pub memory: &'a ExecutionMemoryConfig,
    pub memory_ledger: &'a QueryMemoryLedger,
    pub task_context: Option<&'a RuntimeTaskContext>,
    pub observer: &'a dyn ExecutionObserver,
}

impl BatchExecutionContext<'_> {
    pub(crate) fn source_account(&self, operator: &'static str) -> QueryMemoryAccount {
        self.memory_ledger.source_account(
            operator,
            self.memory.query_memory_bytes,
            self.memory.blocking_operator_bytes,
        )
    }

    pub(crate) fn source_account_retaining(
        &self,
        operator: &'static str,
        retained: QueryMemoryAccount,
    ) -> QueryMemoryAccount {
        self.memory_ledger.source_account_retaining(
            operator,
            self.memory.query_memory_bytes,
            retained,
        )
    }

    pub fn operator_account(&self, operator: &'static str) -> QueryMemoryAccount {
        self.memory_ledger.account(
            QueryMemoryClass::BlockingState,
            operator,
            self.memory.blocking_operator_bytes,
        )
    }

    pub fn operator_tracker(&self, operator: &'static str) -> OperatorMemoryTracker {
        OperatorMemoryTracker::with_account(
            self.memory.blocking_operator_bytes,
            self.operator_account(operator),
        )
    }
}

pub struct AccountedBindingBatch {
    operator: &'static str,
    bindings: BindingBatch,
    batch_rows: usize,
    tracker: OperatorMemoryTracker,
}

/// A transform output batch that reserves its complete configured payload
/// budget before allocating output rows. This makes allocation failure an
/// admission failure rather than an after-the-fact ledger observation.
pub struct TransformBatchBuilder {
    bindings: BindingBatch,
    batch_rows: usize,
    payload_bytes: usize,
    used_payload_bytes: usize,
    reservation: QueryMemoryLease,
}

impl TransformBatchBuilder {
    pub fn new(
        operator: &'static str,
        batch_rows: usize,
        memory_budget: NonZeroUsize,
        memory_ledger: &QueryMemoryLedger,
    ) -> Result<Self> {
        let account = memory_ledger.account(
            QueryMemoryClass::PipelineBatch,
            format!("{operator} output batch"),
            memory_budget,
        );
        let reservation = account.reserve(memory_budget.get())?;
        Ok(Self {
            bindings: Vec::new(),
            batch_rows: batch_rows
                .max(1)
                .min((memory_budget.get() / std::mem::size_of::<Binding>()).max(1)),
            payload_bytes: memory_budget.get(),
            used_payload_bytes: 0,
            reservation,
        })
    }

    /// Must be called before allocating data for the next output row.
    pub fn reserve_before_allocation(&mut self) -> Result<()> {
        if self.reservation.bytes() == 0 {
            self.reservation.grow(self.payload_bytes)?;
        }
        let slots = self
            .bindings
            .len()
            .checked_add(1)
            .and_then(|rows| rows.checked_mul(std::mem::size_of::<Binding>()))
            .ok_or_else(|| HawDBError::Execution("transform batch capacity overflow".into()))?;
        if slots > self.payload_bytes {
            return Err(HawDBError::Execution(format!(
                "transform batch row storage uses {slots} bytes, exceeding batch_payload_bytes {}",
                self.payload_bytes
            )));
        }
        reserve_binding_slot(&mut self.bindings);
        Ok(())
    }

    /// Admit the complete row before cloning or constructing its owned values.
    /// A byte boundary completes the existing batch before allocating the next
    /// row, so downstream Stop prevents that row's payload copies.
    pub fn reserve_row_before_allocation(
        &mut self,
        bytes: usize,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        if bytes > self.payload_bytes {
            return Err(HawDBError::Execution(format!(
                "intermediate row uses {bytes} bytes, exceeding batch_payload_bytes {}",
                self.payload_bytes
            )));
        }
        if self.used_payload_bytes.saturating_add(bytes) > self.payload_bytes
            && !self.bindings.is_empty()
            && self.emit(emit)? == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
        self.reserve_before_allocation()?;
        Ok(BatchControl::Continue)
    }

    pub(crate) fn would_exceed_payload(&self, bytes: usize) -> bool {
        self.used_payload_bytes.saturating_add(bytes) > self.payload_bytes
    }

    pub fn push(&mut self, binding: Binding) {
        debug_assert!(self.reservation.bytes() >= self.payload_bytes);
        self.used_payload_bytes = self
            .used_payload_bytes
            .saturating_add(binding_memory_bytes(&binding));
        debug_assert!(self.used_payload_bytes <= self.payload_bytes);
        self.bindings.push(binding);
    }

    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

    pub fn len(&self) -> usize {
        self.bindings.len()
    }

    pub fn is_full(&self) -> bool {
        self.bindings.len() >= self.batch_rows || self.used_payload_bytes >= self.payload_bytes
    }

    pub fn emit(
        &mut self,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        if self.bindings.is_empty() {
            return Ok(BatchControl::Continue);
        }
        self.reservation.reset();
        self.used_payload_bytes = 0;
        emit(std::mem::take(&mut self.bindings))
    }
}

impl AccountedBindingBatch {
    pub fn with_ledger(
        operator: &'static str,
        batch_rows: usize,
        memory_budget: NonZeroUsize,
        memory_ledger: &QueryMemoryLedger,
    ) -> Self {
        Self::with_account(
            operator,
            batch_rows,
            memory_budget,
            memory_ledger.account(
                QueryMemoryClass::PipelineBatch,
                format!("{operator} output batch"),
                memory_budget,
            ),
        )
    }

    pub fn with_account(
        operator: &'static str,
        batch_rows: usize,
        memory_budget: NonZeroUsize,
        account: QueryMemoryAccount,
    ) -> Self {
        Self {
            operator,
            bindings: Vec::new(),
            batch_rows: batch_rows.max(1),
            tracker: OperatorMemoryTracker::with_account(memory_budget, account),
        }
    }

    pub(crate) fn transfer_from(
        &mut self,
        source: &mut OperatorMemoryTracker,
        source_bytes: usize,
        binding: Binding,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let target_bytes = binding_memory_bytes(&binding);
        self.check_row_size(target_bytes)?;
        if self.tracker.would_exceed(target_bytes)
            && !self.bindings.is_empty()
            && self.emit(emit)? == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
        source
            .transfer_to(source_bytes, &mut self.tracker, target_bytes)
            .map_err(|error| {
                HawDBError::Execution(format!(
                    "{} output batch could not take ownership of a {target_bytes}-byte binding: {error}",
                    self.operator
                ))
            })?;
        reserve_binding_slot(&mut self.bindings);
        self.bindings.push(binding);
        Ok(BatchControl::Continue)
    }

    pub fn push(
        &mut self,
        binding: Binding,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        if self.reserve_row(binding_memory_bytes(&binding), emit)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        reserve_binding_slot(&mut self.bindings);
        self.bindings.push(binding);
        Ok(BatchControl::Continue)
    }

    pub(crate) fn push_cloned(
        &mut self,
        binding: &Binding,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        if self.reserve_row(binding_memory_bytes(binding), emit)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        reserve_binding_slot(&mut self.bindings);
        self.bindings.push(binding.clone());
        Ok(BatchControl::Continue)
    }

    /// The caller previews the final row while values remain borrowed. Never
    /// invoke the ownership conversion before its complete output admission.
    pub(crate) fn push_generated(
        &mut self,
        bytes: usize,
        create: impl FnOnce() -> Binding,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        if self.reserve_row(bytes, emit)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        let binding = create();
        debug_assert_eq!(binding_memory_bytes(&binding), bytes);
        reserve_binding_slot(&mut self.bindings);
        self.bindings.push(binding);
        Ok(BatchControl::Continue)
    }

    pub(crate) fn would_exceed_payload(&self, bytes: usize) -> bool {
        self.tracker.would_exceed(bytes)
    }

    fn reserve_row(
        &mut self,
        bytes: usize,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        self.check_row_size(bytes)?;
        if self.tracker.would_exceed(bytes)
            && !self.bindings.is_empty()
            && self.emit(emit)? == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
        self.tracker.try_charge(bytes)?;
        Ok(BatchControl::Continue)
    }

    pub(crate) fn check_row_size(&self, bytes: usize) -> Result<()> {
        if bytes > self.tracker.budget_bytes {
            return Err(HawDBError::Execution(format!(
                "intermediate row uses {bytes} bytes, exceeding batch_payload_bytes {}",
                self.tracker.budget_bytes
            )));
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

    pub fn len(&self) -> usize {
        self.bindings.len()
    }

    pub fn is_full(&self) -> bool {
        self.bindings.len() == self.batch_rows
    }

    pub fn emit(
        &mut self,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        if self.bindings.is_empty() {
            return Ok(BatchControl::Continue);
        }
        self.tracker.reset();
        emit(std::mem::take(&mut self.bindings))
    }
}

pub struct AccountedBindingSet {
    bindings: Vec<Binding>,
    tracker: OperatorMemoryTracker,
}

impl AccountedBindingSet {
    pub(crate) fn new(bindings: Vec<Binding>, tracker: OperatorMemoryTracker) -> Self {
        debug_assert_eq!(
            tracker.used_bytes,
            bindings.iter().fold(0usize, |bytes, binding| {
                bytes.saturating_add(binding_memory_bytes(binding))
            })
        );
        Self { bindings, tracker }
    }

    pub fn emit_batches(
        self,
        batch_rows: usize,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let Self {
            bindings,
            mut tracker,
        } = self;
        emit_owned_binding_batches(bindings, batch_rows, &mut |batch| {
            let bytes = batch.iter().fold(0usize, |bytes, binding| {
                bytes.saturating_add(binding_memory_bytes(binding))
            });
            tracker.release(bytes);
            emit(batch)
        })
    }
}

pub fn runtime_checkpoint(task_context: Option<&RuntimeTaskContext>) -> Result<()> {
    match task_context {
        Some(task_context) => task_context
            .checkpoint()
            .map_err(|reason| HawDBError::Execution(format!("runtime task stopped: {reason}"))),
        None => Ok(()),
    }
}

/// Emission consumes already-owned rows. Reuse their allocation for a short
/// result, and stage only actual rows for larger results; configured capacity
/// must not amplify an admitted producer's retained allocation.
pub fn emit_owned_binding_batches(
    bindings: Vec<Binding>,
    batch_rows: usize,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    if bindings.is_empty() {
        return Ok(BatchControl::Continue);
    }
    if bindings.len() <= batch_rows.max(1) {
        return emit(bindings);
    }
    emit_binding_iterator(bindings, batch_rows, emit)
}

pub fn emit_binding_iterator(
    bindings: impl IntoIterator<Item = Binding>,
    batch_rows: usize,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut bindings = bindings.into_iter();
    let batch_rows = batch_rows.max(1);
    while let Some(first) = bindings.next() {
        // A lower bound can never reserve more slots than actual remaining
        // input. Allocate the next batch only after the previous emit returns.
        let capacity = batch_rows.min(bindings.size_hint().0.saturating_add(1));
        let mut batch = Vec::with_capacity(capacity);
        batch.push(first);
        batch.extend(bindings.by_ref().take(batch_rows - 1));
        if emit(batch)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
    }
    Ok(BatchControl::Continue)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{QueryMemoryClass, QueryMemoryLedger};
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;

    fn binding(value: i64) -> Binding {
        Binding {
            values: BTreeMap::from([("value".to_string(), hawdb_core::Value::Int(value))]),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }
    }

    #[test]
    fn iterator_emits_bounded_batches_and_honors_stop() {
        let mut sizes = Vec::new();
        let control =
            emit_binding_iterator([binding(1), binding(2), binding(3)], 2, &mut |batch| {
                sizes.push(batch.len());
                Ok(BatchControl::Stop)
            })
            .unwrap();

        assert_eq!(control, BatchControl::Stop);
        assert_eq!(sizes, vec![2]);
    }

    #[test]
    fn accounted_set_releases_remaining_rows_after_consumer_stop() {
        let bindings = vec![binding(1), binding(2), binding(3)];
        let budget = NonZeroUsize::new(4096).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let mut tracker = OperatorMemoryTracker::with_account(
            budget,
            ledger.account(QueryMemoryClass::BlockingState, "test rows", budget),
        );
        for binding in &bindings {
            tracker.try_charge(binding_memory_bytes(binding)).unwrap();
        }
        let accounted = AccountedBindingSet::new(bindings, tracker);

        let control = accounted
            .emit_batches(2, &mut |_| Ok(BatchControl::Stop))
            .unwrap();

        assert_eq!(control, BatchControl::Stop);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn accounted_set_capacity_never_amplifies_configured_rows() {
        let budget = NonZeroUsize::new(4096).unwrap();
        for (rows, batch_rows, terminal) in
            [(1, 8192, 0), (3, 2, 0), (3, 2, 1), (3, 2, 2), (0, 8192, 0)]
        {
            let ledger = QueryMemoryLedger::new(budget);
            let bindings = (0..rows).map(binding).collect::<Vec<_>>();
            let mut tracker = OperatorMemoryTracker::with_account(
                budget,
                ledger.account(QueryMemoryClass::BlockingState, "source", budget),
            );
            for row in &bindings {
                tracker.try_charge(binding_memory_bytes(row)).unwrap();
            }
            let mut emitted = 0;
            let result = AccountedBindingSet::new(bindings, tracker).emit_batches(
                batch_rows,
                &mut |batch| {
                    let slots = batch
                        .capacity()
                        .checked_mul(std::mem::size_of::<Binding>())
                        .unwrap();
                    assert!(
                        slots <= budget.get(),
                        "owned-set capacity {slots} exceeds admitted root {}",
                        budget
                    );
                    emitted += batch.len();
                    match terminal {
                        1 => Ok(BatchControl::Stop),
                        2 => Err(HawDBError::Execution("set consumer refused".into())),
                        _ => Ok(BatchControl::Continue),
                    }
                },
            );
            match terminal {
                1 => assert_eq!(result.unwrap(), BatchControl::Stop),
                2 => assert!(result.is_err()),
                _ => assert_eq!(result.unwrap(), BatchControl::Continue),
            }
            assert_eq!(
                emitted,
                if terminal == 0 {
                    rows as usize
                } else {
                    (rows as usize).min(batch_rows)
                }
            );
            assert_eq!(ledger.snapshot().used_bytes, 0);
            assert!(ledger.snapshot().peak_bytes <= budget.get());
        }
    }

    #[test]
    fn accounted_batch_flushes_before_a_byte_budget_overflow() {
        let first = binding(1);
        let second = binding(2);
        let row_bytes = binding_memory_bytes(&first);
        let root_budget = NonZeroUsize::new(row_bytes.saturating_mul(3)).unwrap();
        let output_budget = NonZeroUsize::new(row_bytes.saturating_add(1)).unwrap();
        let ledger = QueryMemoryLedger::new(root_budget);
        let mut source = OperatorMemoryTracker::with_account(
            root_budget,
            ledger.account(QueryMemoryClass::BlockingState, "source", root_budget),
        );
        let mut output = AccountedBindingBatch::with_ledger("test", 8, output_budget, &ledger);
        let mut batch_sizes = Vec::new();
        let mut emit = |batch: BindingBatch| {
            batch_sizes.push(batch.len());
            Ok(BatchControl::Continue)
        };

        source.try_charge(row_bytes).unwrap();
        assert_eq!(
            output
                .transfer_from(&mut source, row_bytes, first, &mut emit)
                .unwrap(),
            BatchControl::Continue
        );
        source.try_charge(row_bytes).unwrap();
        assert_eq!(
            output
                .transfer_from(&mut source, row_bytes, second, &mut emit)
                .unwrap(),
            BatchControl::Continue
        );
        output.emit(&mut emit).unwrap();

        assert_eq!(batch_sizes, vec![1, 1]);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn transform_builder_reserves_before_output_allocation_and_releases_on_emit() {
        let budget = NonZeroUsize::new(4096).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let mut builder = TransformBatchBuilder::new("project", 2, budget, &ledger).unwrap();

        assert_eq!(ledger.snapshot().used_bytes, 4096);
        assert_eq!(builder.bindings.capacity(), 0);
        builder.reserve_before_allocation().unwrap();
        assert_eq!(builder.bindings.capacity(), 1);
        builder.push(binding(1));
        let control = builder
            .emit(&mut |batch| {
                assert_eq!(batch.len(), 1);
                assert_eq!(ledger.snapshot().used_bytes, 0);
                Ok(BatchControl::Continue)
            })
            .unwrap();

        assert_eq!(control, BatchControl::Continue);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn transform_builder_fails_before_any_output_allocation_when_root_is_too_small() {
        let root_budget = NonZeroUsize::new(1024).unwrap();
        let output_budget = NonZeroUsize::new(2048).unwrap();
        let ledger = QueryMemoryLedger::new(root_budget);

        let error = TransformBatchBuilder::new("project", 1, output_budget, &ledger)
            .err()
            .expect("undersized root budget must reject before output allocation");

        assert!(error.to_string().contains("query memory ledger would use"));
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}
