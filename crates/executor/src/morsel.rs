//! Storage-neutral morsel partitioning and resource admission.

use crate::concurrent::{BoundedOrderedStreamControl, BoundedOrderedStreamReport};
use crate::{BoundedExecutor, QueryMemoryAccount, QueryMemoryLease, SharedExecutorPool};
use skein_core::{Result, RuntimeTaskContext, SkeinError};
use std::num::NonZeroUsize;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PipelineId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MorselOrdinal(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Morsel {
    pub pipeline_id: PipelineId,
    pub ordinal: MorselOrdinal,
    pub start_row: usize,
    pub row_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MorselAdmissionRequest {
    pub pipeline_id: PipelineId,
    pub input_rows: usize,
    pub target_rows: NonZeroUsize,
    pub requested_parallelism: NonZeroUsize,
    pub bytes_per_worker: NonZeroUsize,
    pub memory_budget_bytes: NonZeroUsize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MorselAdmission {
    pipeline_id: PipelineId,
    input_rows: usize,
    target_rows: NonZeroUsize,
    morsel_count: usize,
    max_workers: usize,
    reserved_bytes: usize,
}

impl MorselAdmission {
    pub fn try_new(request: MorselAdmissionRequest) -> Result<Self> {
        if request.input_rows == 0 {
            return Ok(Self {
                pipeline_id: request.pipeline_id,
                input_rows: 0,
                target_rows: request.target_rows,
                morsel_count: 0,
                max_workers: 0,
                reserved_bytes: 0,
            });
        }

        let memory_workers = request.memory_budget_bytes.get() / request.bytes_per_worker.get();
        if memory_workers == 0 {
            return Err(SkeinError::Execution(format!(
                "morsel pipeline {} requires {} bytes for one worker, exceeding memory budget {}",
                request.pipeline_id.0, request.bytes_per_worker, request.memory_budget_bytes
            )));
        }

        let morsel_count = request.input_rows.div_ceil(request.target_rows.get());
        let max_workers = request
            .requested_parallelism
            .get()
            .min(memory_workers)
            .min(morsel_count);
        let reserved_bytes = max_workers.saturating_mul(request.bytes_per_worker.get());
        Ok(Self {
            pipeline_id: request.pipeline_id,
            input_rows: request.input_rows,
            target_rows: request.target_rows,
            morsel_count,
            max_workers,
            reserved_bytes,
        })
    }

    pub fn morsels(&self) -> MorselIter {
        MorselIter {
            pipeline_id: self.pipeline_id,
            input_rows: self.input_rows,
            target_rows: self.target_rows,
            next_ordinal: 0,
            morsel_count: self.morsel_count,
        }
    }

    pub fn morsel_count(&self) -> usize {
        self.morsel_count
    }

    pub fn max_workers(&self) -> usize {
        self.max_workers
    }

    pub fn reserved_bytes(&self) -> usize {
        self.reserved_bytes
    }

    fn morsel_at(&self, index: usize) -> Option<Morsel> {
        if index >= self.morsel_count {
            return None;
        }
        let start_row = index.saturating_mul(self.target_rows.get());
        Some(Morsel {
            pipeline_id: self.pipeline_id,
            ordinal: MorselOrdinal(index as u64),
            start_row,
            row_count: self
                .target_rows
                .get()
                .min(self.input_rows.saturating_sub(start_row)),
        })
    }
}

pub struct MorselIter {
    pipeline_id: PipelineId,
    input_rows: usize,
    target_rows: NonZeroUsize,
    next_ordinal: usize,
    morsel_count: usize,
}

impl Iterator for MorselIter {
    type Item = Morsel;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_ordinal == self.morsel_count {
            return None;
        }
        let ordinal = self.next_ordinal;
        let start_row = ordinal.saturating_mul(self.target_rows.get());
        let row_count = self
            .target_rows
            .get()
            .min(self.input_rows.saturating_sub(start_row));
        self.next_ordinal = self.next_ordinal.saturating_add(1);
        Some(Morsel {
            pipeline_id: self.pipeline_id,
            ordinal: MorselOrdinal(ordinal as u64),
            start_row,
            row_count,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.morsel_count.saturating_sub(self.next_ordinal);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for MorselIter {}

pub fn admit_morsels(request: MorselAdmissionRequest) -> Result<MorselAdmission> {
    MorselAdmission::try_new(request)
}

/// Materializing sequential baseline for scheduler qualification and benchmarks.
/// Production bounded output uses `SharedPoolMorselScheduler::execute_accounted_ordered`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SequentialMorselScheduler;

impl SequentialMorselScheduler {
    pub fn execute<T>(
        self,
        admission: &MorselAdmission,
        mut execute: impl FnMut(Morsel) -> Result<T>,
    ) -> Result<Vec<T>> {
        let mut output = Vec::with_capacity(admission.morsel_count);
        for morsel in admission.morsels() {
            output.push(execute(morsel)?);
        }
        Ok(output)
    }
}

#[derive(Debug, Clone)]
pub struct SharedPoolMorselScheduler {
    pool: SharedExecutorPool,
}

impl SharedPoolMorselScheduler {
    pub fn new(pool: SharedExecutorPool) -> Self {
        Self { pool }
    }

    pub fn execute<T, F>(&self, admission: &MorselAdmission, execute: F) -> Result<Vec<T>>
    where
        T: Send,
        F: Fn(Morsel) -> Result<T> + Sync,
    {
        if admission.morsel_count() == 0 {
            return Ok(Vec::new());
        }
        let max_workers = NonZeroUsize::new(admission.max_workers()).ok_or_else(|| {
            SkeinError::Execution("non-empty morsel admission reserved no workers".to_string())
        })?;
        let morsels = admission.morsels().collect::<Vec<_>>();
        BoundedExecutor::with_pool(max_workers, self.pool.clone())
            .map_ordered(&morsels, |morsel| execute_catching_panic(&execute, *morsel))
            .into_iter()
            .collect()
    }

    pub fn execute_with_context<T, F>(
        &self,
        admission: &MorselAdmission,
        context: &RuntimeTaskContext,
        execute: F,
    ) -> Result<Vec<T>>
    where
        T: Send,
        F: Fn(Morsel) -> Result<T> + Sync,
    {
        if admission.morsel_count() == 0 {
            context.checkpoint().map_err(|reason| {
                SkeinError::Execution(format!("runtime task stopped: {reason}"))
            })?;
            return Ok(Vec::new());
        }
        let max_workers = NonZeroUsize::new(admission.max_workers()).ok_or_else(|| {
            SkeinError::Execution("non-empty morsel admission reserved no workers".to_string())
        })?;
        let morsels = admission.morsels().collect::<Vec<_>>();
        BoundedExecutor::with_pool(max_workers, self.pool.clone())
            .map_ordered_with_context(&morsels, context, |morsel| {
                execute_catching_panic(&execute, *morsel)
            })
            .map_err(|reason| SkeinError::Execution(format!("runtime task stopped: {reason}")))?
            .into_iter()
            .collect()
    }

    /// Streams worker outputs through a bounded channel and invokes the
    /// coordinator consumer in stable morsel ordinal order.
    ///
    /// `output_reservation_bytes` is charged before the worker calls
    /// `execute`. The returned resident estimate must fit that reservation.
    /// The lease remains live through `consume`, so a consumer that retains
    /// the value must transfer it into another query-owned account.
    pub fn execute_accounted_ordered<T, F, C>(
        &self,
        admission: &MorselAdmission,
        resources: MorselStreamResources<'_>,
        execute: F,
        mut consume: C,
    ) -> Result<MorselStreamReport>
    where
        T: Send,
        F: Fn(Morsel) -> Result<MorselOutput<T>> + Sync,
        C: FnMut(Morsel, T) -> Result<MorselStreamControl>,
    {
        if admission.morsel_count() == 0 {
            if let Some(context) = resources.task_context {
                context.checkpoint().map_err(|reason| {
                    SkeinError::Execution(format!("runtime task stopped: {reason}"))
                })?;
            }
            return Ok(MorselStreamReport::default());
        }
        let max_workers = NonZeroUsize::new(admission.max_workers()).ok_or_else(|| {
            SkeinError::Execution("non-empty morsel admission reserved no workers".to_string())
        })?;
        let counters = Arc::new(MorselOutputCounters::default());
        let concurrent_report = BoundedExecutor::with_pool(max_workers, self.pool.clone())
            .try_for_each_index_ordered(
                admission.morsel_count(),
                resources.task_context,
                |index| {
                    let morsel = admission
                        .morsel_at(index)
                        .expect("admitted morsel index is in range");
                    let mut lease = resources
                        .output_account
                        .reserve(resources.output_reservation_bytes.get())?;
                    let output = execute_catching_panic(&execute, morsel)?;
                    if output.resident_bytes > resources.output_reservation_bytes.get() {
                        return Err(SkeinError::Execution(format!(
                            "morsel pipeline {} output at ordinal {} uses {} bytes, exceeding its {}-byte reservation",
                            morsel.pipeline_id.0,
                            morsel.ordinal.0,
                            output.resident_bytes,
                            resources.output_reservation_bytes
                        )));
                    }
                    lease.shrink(
                        resources.output_reservation_bytes.get() - output.resident_bytes,
                    );
                    Ok(PendingMorselOutput::new(
                        output.value,
                        output.resident_bytes,
                        lease,
                        Arc::clone(&counters),
                    ))
                },
                |index, mut output| {
                    let morsel = admission
                        .morsel_at(index)
                        .expect("admitted morsel index is in range");
                    let value = output.take_value();
                    let control = catch_unwind(AssertUnwindSafe(|| consume(morsel, value)))
                        .unwrap_or_else(|_| {
                            Err(SkeinError::Execution(format!(
                                "morsel pipeline {} consumer panicked at ordinal {}",
                                morsel.pipeline_id.0, morsel.ordinal.0
                            )))
                        })?;
                    Ok(match control {
                        MorselStreamControl::Continue => BoundedOrderedStreamControl::Continue,
                        MorselStreamControl::Stop => BoundedOrderedStreamControl::Stop,
                    })
                },
            )?;
        Ok(MorselStreamReport::from_parts(concurrent_report, &counters))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MorselStreamResources<'a> {
    pub task_context: Option<&'a RuntimeTaskContext>,
    pub output_account: &'a QueryMemoryAccount,
    pub output_reservation_bytes: NonZeroUsize,
}

#[derive(Debug)]
pub struct MorselOutput<T> {
    value: T,
    resident_bytes: usize,
}

impl<T> MorselOutput<T> {
    pub fn new(value: T, resident_bytes: usize) -> Self {
        Self {
            value,
            resident_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MorselStreamControl {
    Continue,
    Stop,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MorselStreamReport {
    pub peak_buffered_outputs: usize,
    pub peak_buffered_output_bytes: usize,
    pub peak_reorder_entries: usize,
    pub stopped_early: bool,
}

impl MorselStreamReport {
    fn from_parts(concurrent: BoundedOrderedStreamReport, counters: &MorselOutputCounters) -> Self {
        Self {
            peak_buffered_outputs: counters.peak_outputs.load(Ordering::Relaxed),
            peak_buffered_output_bytes: counters.peak_bytes.load(Ordering::Relaxed),
            peak_reorder_entries: concurrent.peak_reorder_entries,
            stopped_early: concurrent.stopped_early,
        }
    }
}

#[derive(Debug, Default)]
struct MorselOutputCounters {
    current_outputs: AtomicUsize,
    peak_outputs: AtomicUsize,
    current_bytes: AtomicUsize,
    peak_bytes: AtomicUsize,
}

struct PendingMorselOutput<T> {
    value: Option<T>,
    resident_bytes: usize,
    _lease: QueryMemoryLease,
    counters: Arc<MorselOutputCounters>,
}

impl<T> PendingMorselOutput<T> {
    fn new(
        value: T,
        resident_bytes: usize,
        lease: QueryMemoryLease,
        counters: Arc<MorselOutputCounters>,
    ) -> Self {
        let outputs = counters.current_outputs.fetch_add(1, Ordering::Relaxed) + 1;
        counters.peak_outputs.fetch_max(outputs, Ordering::Relaxed);
        let bytes = counters
            .current_bytes
            .fetch_add(resident_bytes, Ordering::Relaxed)
            .saturating_add(resident_bytes);
        counters.peak_bytes.fetch_max(bytes, Ordering::Relaxed);
        Self {
            value: Some(value),
            resident_bytes,
            _lease: lease,
            counters,
        }
    }

    fn take_value(&mut self) -> T {
        self.value
            .take()
            .expect("pending morsel output value is consumed once")
    }
}

impl<T> Drop for PendingMorselOutput<T> {
    fn drop(&mut self) {
        self.counters
            .current_outputs
            .fetch_sub(1, Ordering::Relaxed);
        self.counters
            .current_bytes
            .fetch_sub(self.resident_bytes, Ordering::Relaxed);
    }
}

fn execute_catching_panic<T>(
    execute: &(impl Fn(Morsel) -> Result<T> + Sync),
    morsel: Morsel,
) -> Result<T> {
    catch_unwind(AssertUnwindSafe(|| execute(morsel))).unwrap_or_else(|_| {
        Err(SkeinError::Execution(format!(
            "morsel pipeline {} worker panicked at ordinal {}",
            morsel.pipeline_id.0, morsel.ordinal.0
        )))
    })
}

/// Executes morsels in ordinal order, stopping at the first callback error.
///
/// This materializing baseline is consumed by the executor vectorization
/// benchmark and `tests/morsel_oracle.rs`, which qualifies both materializing
/// shared-pool methods and the production accounted stream against an independent
/// row oracle. Parallel callbacks may already be running when one fails; their
/// side effects need not match the sequential baseline's fail-fast behavior.
pub fn execute_morsels_ordered<T>(
    admission: &MorselAdmission,
    mut execute: impl FnMut(Morsel) -> Result<T>,
) -> Result<Vec<T>> {
    SequentialMorselScheduler.execute(admission, &mut execute)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{QueryMemoryClass, QueryMemoryLedger};

    fn request(input_rows: usize) -> MorselAdmissionRequest {
        MorselAdmissionRequest {
            pipeline_id: PipelineId(7),
            input_rows,
            target_rows: NonZeroUsize::new(64).unwrap(),
            requested_parallelism: NonZeroUsize::new(8).unwrap(),
            bytes_per_worker: NonZeroUsize::new(1024).unwrap(),
            memory_budget_bytes: NonZeroUsize::new(3 * 1024).unwrap(),
        }
    }

    #[test]
    fn admission_is_bounded_by_work_and_memory() {
        let admission = admit_morsels(request(130)).unwrap();

        assert_eq!(admission.max_workers(), 3);
        assert_eq!(admission.reserved_bytes(), 3 * 1024);
        assert_eq!(
            admission
                .morsels()
                .map(|morsel| (morsel.start_row, morsel.row_count))
                .collect::<Vec<_>>(),
            vec![(0, 64), (64, 64), (128, 2)]
        );
    }

    #[test]
    fn admission_rejects_a_worker_that_cannot_fit() {
        let mut request = request(1);
        request.bytes_per_worker = NonZeroUsize::new(4096).unwrap();

        assert!(admit_morsels(request).is_err());
    }

    #[test]
    fn ordered_executor_preserves_morsel_ordinals() {
        let admission = MorselAdmission::try_new(request(130)).unwrap();
        let ordinals = SequentialMorselScheduler
            .execute(&admission, |morsel| Ok(morsel.ordinal.0))
            .unwrap();

        assert_eq!(ordinals, vec![0, 1, 2]);
    }

    #[test]
    fn empty_input_reserves_no_workers() {
        let admission = admit_morsels(request(0)).unwrap();

        assert_eq!(admission.morsel_count(), 0);
        assert_eq!(admission.max_workers(), 0);
        assert_eq!(admission.reserved_bytes(), 0);
    }

    #[test]
    fn shared_pool_scheduler_matches_sequential_ordinal_order() {
        let admission = admit_morsels(request(1025)).unwrap();
        let sequential = execute_morsels_ordered(&admission, |morsel| {
            Ok((morsel.ordinal.0, morsel.start_row, morsel.row_count))
        })
        .unwrap();
        let pool = SharedExecutorPool::new(NonZeroUsize::new(3).unwrap()).unwrap();
        let parallel = SharedPoolMorselScheduler::new(pool)
            .execute(&admission, |morsel| {
                std::thread::yield_now();
                Ok((morsel.ordinal.0, morsel.start_row, morsel.row_count))
            })
            .unwrap();

        assert_eq!(parallel, sequential);
    }

    #[test]
    fn shared_pool_scheduler_converts_worker_panics_to_stable_errors() {
        let admission = admit_morsels(request(130)).unwrap();
        let pool = SharedExecutorPool::new(NonZeroUsize::new(2).unwrap()).unwrap();
        let error = SharedPoolMorselScheduler::new(pool)
            .execute(&admission, |morsel| -> Result<u64> {
                assert_ne!(morsel.ordinal.0, 1, "injected worker panic");
                Ok(morsel.ordinal.0)
            })
            .unwrap_err();

        assert!(error.to_string().contains("ordinal 1"), "{error}");
    }

    #[test]
    fn shared_pool_scheduler_observes_cancellation_between_morsels() {
        let admission = admit_morsels(request(1025)).unwrap();
        let pool = SharedExecutorPool::new(NonZeroUsize::MIN).unwrap();
        let token = skein_core::RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        let error = SharedPoolMorselScheduler::new(pool)
            .execute_with_context(&admission, &context, |morsel| {
                if morsel.ordinal.0 == 0 {
                    token.cancel();
                }
                Ok(morsel.ordinal.0)
            })
            .unwrap_err();

        assert!(error.to_string().contains("cancelled"));
    }

    #[test]
    fn accounted_stream_preserves_order_and_bounds_completed_outputs() {
        let admission = admit_morsels(request(320)).unwrap();
        let pool = SharedExecutorPool::new(NonZeroUsize::new(3).unwrap()).unwrap();
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(1024).unwrap());
        let account = ledger.account(
            QueryMemoryClass::MorselOutput,
            "test reorder",
            NonZeroUsize::new(96).unwrap(),
        );
        let later_completed = AtomicUsize::new(0);
        let mut ordinals = Vec::new();

        let report = SharedPoolMorselScheduler::new(pool)
            .execute_accounted_ordered(
                &admission,
                MorselStreamResources {
                    task_context: None,
                    output_account: &account,
                    output_reservation_bytes: NonZeroUsize::new(32).unwrap(),
                },
                |morsel| {
                    if morsel.ordinal.0 == 0 {
                        while later_completed.load(Ordering::Acquire) < 2 {
                            std::thread::yield_now();
                        }
                    } else if morsel.ordinal.0 <= 2 {
                        later_completed.fetch_add(1, Ordering::Release);
                    }
                    Ok(MorselOutput::new(morsel.ordinal.0, 8))
                },
                |morsel, value| {
                    assert_eq!(morsel.ordinal.0, value);
                    ordinals.push(value);
                    Ok(MorselStreamControl::Continue)
                },
            )
            .unwrap();

        assert_eq!(ordinals, vec![0, 1, 2, 3, 4]);
        assert!((1..=admission.max_workers()).contains(&report.peak_buffered_outputs));
        assert!(report.peak_reorder_entries <= admission.max_workers());
        assert!(report.peak_buffered_output_bytes <= admission.max_workers() * 8);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn accounted_stream_reserves_before_worker_execution() {
        let admission = admit_morsels(request(130)).unwrap();
        let pool = SharedExecutorPool::new(NonZeroUsize::new(2).unwrap()).unwrap();
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(1024).unwrap());
        let account = ledger.account(
            QueryMemoryClass::MorselOutput,
            "test reservation",
            NonZeroUsize::new(16).unwrap(),
        );
        let executed = AtomicUsize::new(0);

        let error = SharedPoolMorselScheduler::new(pool)
            .execute_accounted_ordered(
                &admission,
                MorselStreamResources {
                    task_context: None,
                    output_account: &account,
                    output_reservation_bytes: NonZeroUsize::new(32).unwrap(),
                },
                |morsel| {
                    executed.fetch_add(1, Ordering::SeqCst);
                    Ok(MorselOutput::new(morsel.ordinal.0, 8))
                },
                |_morsel, _value| Ok(MorselStreamControl::Continue),
            )
            .unwrap_err();

        assert!(error.to_string().contains("16-byte budget"));
        assert_eq!(executed.load(Ordering::SeqCst), 0);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn accounted_stream_releases_outputs_after_worker_panic() {
        let admission = admit_morsels(request(130)).unwrap();
        let pool = SharedExecutorPool::new(NonZeroUsize::new(2).unwrap()).unwrap();
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(1024).unwrap());
        let account = ledger.account(
            QueryMemoryClass::MorselOutput,
            "test panic",
            NonZeroUsize::new(64).unwrap(),
        );

        let error = SharedPoolMorselScheduler::new(pool)
            .execute_accounted_ordered(
                &admission,
                MorselStreamResources {
                    task_context: None,
                    output_account: &account,
                    output_reservation_bytes: NonZeroUsize::new(32).unwrap(),
                },
                |morsel| {
                    assert_ne!(morsel.ordinal.0, 1, "injected worker panic");
                    Ok(MorselOutput::new(morsel.ordinal.0, 8))
                },
                |_morsel, _value| Ok(MorselStreamControl::Continue),
            )
            .unwrap_err();

        assert!(error.to_string().contains("ordinal 1"), "{error}");
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn accounted_stream_stop_does_not_open_another_issue_window() {
        let admission = admit_morsels(request(640)).unwrap();
        let pool = SharedExecutorPool::new(NonZeroUsize::new(3).unwrap()).unwrap();
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(1024).unwrap());
        let account = ledger.account(
            QueryMemoryClass::MorselOutput,
            "test stop",
            NonZeroUsize::new(96).unwrap(),
        );
        let executed = AtomicUsize::new(0);

        let report = SharedPoolMorselScheduler::new(pool)
            .execute_accounted_ordered(
                &admission,
                MorselStreamResources {
                    task_context: None,
                    output_account: &account,
                    output_reservation_bytes: NonZeroUsize::new(32).unwrap(),
                },
                |morsel| {
                    executed.fetch_add(1, Ordering::SeqCst);
                    Ok(MorselOutput::new(morsel.ordinal.0, 8))
                },
                |_morsel, _value| Ok(MorselStreamControl::Stop),
            )
            .unwrap();

        assert!(report.stopped_early);
        assert!(executed.load(Ordering::SeqCst) <= admission.max_workers());
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn accounted_stream_converts_consumer_panics_and_releases_outputs() {
        let admission = admit_morsels(request(130)).unwrap();
        let pool = SharedExecutorPool::new(NonZeroUsize::new(2).unwrap()).unwrap();
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(1024).unwrap());
        let account = ledger.account(
            QueryMemoryClass::MorselOutput,
            "test consumer panic",
            NonZeroUsize::new(64).unwrap(),
        );

        let error = SharedPoolMorselScheduler::new(pool)
            .execute_accounted_ordered(
                &admission,
                MorselStreamResources {
                    task_context: None,
                    output_account: &account,
                    output_reservation_bytes: NonZeroUsize::new(32).unwrap(),
                },
                |morsel| Ok(MorselOutput::new(morsel.ordinal.0, 8)),
                |morsel, _value| -> Result<MorselStreamControl> {
                    assert_ne!(morsel.ordinal.0, 0, "injected consumer panic");
                    Ok(MorselStreamControl::Continue)
                },
            )
            .unwrap_err();

        assert!(error.to_string().contains("consumer panicked at ordinal 0"));
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}
