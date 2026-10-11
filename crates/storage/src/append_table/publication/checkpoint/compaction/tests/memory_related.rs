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
use hawdb_core::{
    RuntimeCancellationToken, RuntimeIoWaveController, RuntimeIoWaveError, RuntimeIoWavePermit,
    RuntimeIoWaveTryAcquire,
};

#[derive(Debug)]
struct IoProbe {
    task: RuntimeTaskContext,
    waves: AtomicU64,
    cancel_on_wave: u64,
    cancellation: RuntimeCancellationToken,
}

impl IoProbe {
    fn admitted(&self) {
        let wave = self.waves.fetch_add(1, Ordering::SeqCst) + 1;
        if wave == self.cancel_on_wave {
            self.cancellation.cancel();
        }
    }
}

impl RuntimeIoWaveController for IoProbe {
    fn try_acquire(
        &self,
        slots: NonZeroUsize,
        context: &RuntimeTaskContext,
    ) -> Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
        context.checkpoint().map_err(RuntimeIoWaveError::Stopped)?;
        match self.task.try_acquire_io_wave(slots)? {
            RuntimeIoWaveTryAcquire::Acquired(permit) => {
                assert!(permit.is_some(), "the real governor must own this I/O wave");
                self.admitted();
                Ok(permit)
            }
            RuntimeIoWaveTryAcquire::Pending => Ok(None),
        }
    }

    fn acquire(
        &self,
        slots: NonZeroUsize,
        context: &RuntimeTaskContext,
    ) -> Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
        context.checkpoint().map_err(RuntimeIoWaveError::Stopped)?;
        let permit = self.task.acquire_io_wave(slots)?;
        self.admitted();
        Ok(permit.expect("the real governor must own this I/O wave"))
    }
}

#[derive(Debug)]
struct MemoryProbe {
    task: RuntimeTaskContext,
    ceiling: u64,
    peak: AtomicU64,
}

impl MemoryProbe {
    fn used(&self) -> u64 {
        match self.task.reserve_working_memory(self.ceiling) {
            Err(RuntimeMemoryError::ReservationExceeded {
                available_bytes, ..
            }) => self.ceiling - available_bytes,
            _ => panic!("expected the real governor's available working capacity"),
        }
    }
}

impl QosTelemetrySink for MemoryProbe {
    fn record_qos(&self, _: QosTelemetryEvent) {
        self.peak.fetch_max(self.used(), Ordering::SeqCst);
    }
}

#[test]
fn checkpoint_units_append_compaction_memory_charges_sort_overlap_and_retries_same_reservation() {
    let fixture = Fixture::new();
    let authority = fixture.authority();
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let probe = Arc::new(MemoryProbe {
        task: task.clone(),
        ceiling,
        peak: AtomicU64::new(0),
    });
    let local = scheduler();
    local.set_telemetry_sink(Some(probe.clone()));
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let plan = plan_compaction(
        Some(&fixture.previous),
        &fixture.live,
        fixture.config,
        &work,
    )
    .unwrap();
    assert_eq!(plan.checkpoint_rows.as_ref().unwrap(), &fixture.all);
    let retained = probe.used();
    let peak = probe.peak.load(Ordering::SeqCst);
    let array_bytes = (fixture.all.len() * std::mem::size_of::<AppendTableRow>()) as u64;
    assert!(retained >= array_bytes);
    assert!(
        peak >= retained + 2 * array_bytes,
        "compaction input, run and output arrays overlap: peak={peak}, retained={retained}, array_bytes={array_bytes}"
    );
    assert!(peak < ceiling);
    drop(plan);
    assert_eq!(probe.used(), 0);

    // A compressed/decoded block can have a larger peak than sorting. Measure
    // the actual compaction sorter separately so that unrelated decoder bytes
    // cannot hide an omitted run/heap lease. Use the complete reference rows.
    let mut input = CheckpointAppendRows::new(fixture.all.len(), &work).unwrap();
    for row in &fixture.all {
        input.push_clone(row, &work).unwrap();
    }
    probe.peak.store(0, Ordering::SeqCst);
    let sorted =
        crate::append_table::checkpoint::sort_owned_rows_with_work_context(input, &work).unwrap();
    assert_eq!(sorted, fixture.all);
    let sort_retained = probe.used();
    let sort_peak = probe.peak.load(Ordering::SeqCst);
    assert!(
        sort_peak >= sort_retained + 2 * array_bytes,
        "the measured sort alone must retain input, runs and output: peak={sort_peak}, retained={sort_retained}, array_bytes={array_bytes}"
    );
    drop(sorted);
    assert_eq!(probe.used(), 0);

    let held = task.reserve_working_memory(ceiling - peak).unwrap();
    let held_usage = probe.used();
    assert!(matches!(
        work.classify(|work| {
            plan_compaction(Some(&fixture.previous), &fixture.live, fixture.config, work)
        }),
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { .. }
        )))
    ));
    assert_eq!(probe.used(), held_usage);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(fixture.authority(), authority);
    drop(held);
    assert_eq!(probe.used(), 0);
    let retry = plan_compaction(
        Some(&fixture.previous),
        &fixture.live,
        fixture.config,
        &work,
    )
    .unwrap();
    assert_eq!(retry.checkpoint_rows.as_ref().unwrap(), &fixture.all);
    assert_eq!(governor.snapshot().admissions, 1);
    local.set_telemetry_sink(None);
    drop(work);
    drop(probe);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    drop(retry);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_append_compaction_memory_block_denial_cannot_defer_and_retry_is_complete() {
    let fixture = Fixture::new();
    let authority = fixture.authority();
    let first = &fixture.previous.segments[0].descriptors()[0];
    let compressed = usize::try_from(first.compressed_bytes).unwrap();
    let chunk = compressed.min(64 * 1024);
    assert!(usize::try_from(first.decoded_bytes).unwrap() > chunk + 4096);
    // The full result array and compressed/read buffers fit, but the first
    // decoded block does not. This reaches the actual file-read path before
    // the memory rejection and exercises the data-deferral distinction.
    let allowance =
        fixture.all.len() * std::mem::size_of::<AppendTableRow>() + compressed + chunk + 4096;
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let probe = MemoryProbe {
        task: task.clone(),
        ceiling,
        peak: AtomicU64::new(0),
    };
    let io = Arc::new(IoProbe {
        task: task.clone(),
        waves: AtomicU64::new(0),
        cancel_on_wave: 0,
        cancellation: RuntimeCancellationToken::new(),
    });
    let work = CheckpointWorkContext::new(task.clone().with_io_wave_controller(io.clone()));
    let held = task
        .reserve_working_memory(ceiling - allowance as u64)
        .unwrap();
    let held_usage = probe.used();
    assert!(matches!(
        work.classify(|work| {
            plan_compaction(Some(&fixture.previous), &fixture.live, fixture.config, work)
        }),
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { .. }
        )))
    ));
    assert!(io.waves.load(Ordering::SeqCst) > 0);
    assert_eq!(probe.used(), held_usage);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(fixture.authority(), authority);
    drop(held);
    assert_eq!(probe.used(), 0);
    let retry = plan_compaction(
        Some(&fixture.previous),
        &fixture.live,
        fixture.config,
        &work,
    )
    .unwrap();
    assert_eq!(retry.checkpoint_rows.as_ref().unwrap(), &fixture.all);
    assert_eq!(governor.snapshot().admissions, 1);
    drop(retry);
    drop(probe);
    drop(task);
    drop(work);
    drop(io);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_append_compaction_memory_returns_real_capacity_on_cpu_and_every_io_cut() {
    let fixture = Fixture::new();
    let authority = fixture.authority();
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let memory = MemoryProbe {
        task: task.clone(),
        ceiling,
        peak: AtomicU64::new(0),
    };
    let local = scheduler();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    local.set_telemetry_sink(Some(baseline.clone()));
    let io = Arc::new(IoProbe {
        task: task.clone(),
        waves: AtomicU64::new(0),
        cancel_on_wave: 0,
        cancellation: RuntimeCancellationToken::new(),
    });
    let work = CheckpointWorkContext::new(task.clone().with_io_wave_controller(io.clone()))
        .with_scheduler(local.clone());
    let plan = plan_compaction(
        Some(&fixture.previous),
        &fixture.live,
        fixture.config,
        &work,
    )
    .unwrap();
    assert_eq!(plan.checkpoint_rows.as_ref().unwrap(), &fixture.all);
    let total = baseline.completed.load(Ordering::SeqCst);
    let waves = io.waves.load(Ordering::SeqCst);
    assert!(total > fixture.all.len() * 4);
    assert!(waves > 8);
    drop(plan);
    drop(work);
    drop(io);
    local.set_telemetry_sink(None);
    assert_eq!(memory.used(), 0);
    baseline.assert_released(&local);

    let cuts = (1..=waves).map(|wave| (0, wave)).chain(
        [2, 17, total / 4, total / 2, total * 3 / 4, total]
            .into_iter()
            .map(|cpu| (cpu, 0)),
    );
    for (cpu, wave) in cuts {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(cpu, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let current = permit.bind_task_context(RuntimeTaskContext::without_deadline(
            probe.cancellation.clone(),
        ));
        let io = Arc::new(IoProbe {
            task: current.clone(),
            waves: AtomicU64::new(0),
            cancel_on_wave: wave,
            cancellation: probe.cancellation.clone(),
        });
        let work = CheckpointWorkContext::new(current.with_io_wave_controller(io.clone()))
            .with_scheduler(local.clone());
        assert!(matches!(
            work.classify(|work| {
                plan_compaction(Some(&fixture.previous), &fixture.live, fixture.config, work)
            }),
            Err(CheckpointOperationError::Work(
                CheckpointWorkError::Stopped(_)
            ))
        ));
        if wave > 0 {
            assert_eq!(io.waves.load(Ordering::SeqCst), wave);
        } else {
            assert_eq!(probe.completed.load(Ordering::SeqCst), cpu);
        }
        assert_eq!(memory.used(), 0, "cpu={cpu}, wave={wave}");
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        assert_eq!(fixture.authority(), authority);
        probe.assert_released(&local);
        drop(work);
        drop(io);
        local.set_telemetry_sink(None);

        // Bind fresh cancellation to the same governor reservation. Every
        // aborted attempt must leave sufficient capacity for the full retry.
        let retry =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()))
                .with_scheduler(local.clone());
        let plan = plan_compaction(
            Some(&fixture.previous),
            &fixture.live,
            fixture.config,
            &retry,
        )
        .unwrap();
        assert_eq!(plan.checkpoint_rows.as_ref().unwrap(), &fixture.all);
        drop(plan);
        drop(retry);
        assert_eq!(memory.used(), 0);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        assert_eq!(governor.snapshot().admissions, 1);
        assert_eq!(fixture.authority(), authority);
    }
    drop(memory);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
