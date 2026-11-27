// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

struct Gate {
    open: Mutex<bool>,
    wake: Condvar,
}

impl Gate {
    fn new() -> Self {
        Self {
            open: Mutex::new(false),
            wake: Condvar::new(),
        }
    }

    fn open(&self) {
        *lock(&self.open) = true;
        self.wake.notify_all();
    }

    fn wait(&self) {
        let mut open = lock(&self.open);
        while !*open {
            let (next, timeout) = self
                .wake
                .wait_timeout(open, Duration::from_secs(10))
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            open = next;
            if timeout.timed_out() {
                return;
            }
        }
    }
}

fn config(
    business_workers: usize,
    background_workers: usize,
    queue_capacity: usize,
) -> SchedulerConfig {
    SchedulerConfig {
        business_workers,
        background_workers,
        queue_capacity,
        quantum: Duration::from_millis(5),
        aging_interval: Duration::from_millis(50),
    }
}

#[test]
fn business_priorities_run_before_lower_levels() {
    let scheduler = Scheduler::new(config(1, 1, 16)).unwrap();
    let gate = Arc::new(Gate::new());
    let order = Arc::new(Mutex::new(Vec::new()));
    let blocker_gate = Arc::clone(&gate);
    let blocker = scheduler
        .spawn(TaskKey::new(10), Priority::High, move || {
            blocker_gate.wait()
        })
        .unwrap();
    let mut handles = Vec::new();
    for (key, label, priority) in [
        (1, "normal", Priority::Normal),
        (2, "low", Priority::Low),
        (3, "high", Priority::High),
    ] {
        let order = Arc::clone(&order);
        handles.push(
            scheduler
                .spawn(TaskKey::new(key), priority, move || {
                    lock(&order).push(label);
                })
                .unwrap(),
        );
    }
    gate.open();
    blocker.join().unwrap();
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(*lock(&order), ["high", "normal", "low"]);
    scheduler.shutdown();
}

#[test]
fn background_work_is_isolated_from_business_workers() {
    let scheduler = Scheduler::new(config(1, 1, 16)).unwrap();
    let gate = Arc::new(Gate::new());
    let (started_tx, started_rx) = mpsc::channel();
    let blocker_gate = Arc::clone(&gate);
    let blocker = scheduler
        .spawn(TaskKey::new(1), Priority::High, move || {
            started_tx.send(()).unwrap();
            blocker_gate.wait();
        })
        .unwrap();
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("business blocker must start");
    let background_ran = Arc::new(AtomicBool::new(false));
    let background_flag = Arc::clone(&background_ran);
    let background = scheduler
        .spawn_background(TaskKey::new(2), Priority::Low, move || {
            background_flag.store(true, Ordering::Release);
        })
        .unwrap();
    background.join().unwrap();
    assert!(background_ran.load(Ordering::Acquire));
    let queued_ran = Arc::new(AtomicBool::new(false));
    let queued_flag = Arc::clone(&queued_ran);
    let queued = scheduler
        .spawn(TaskKey::new(3), Priority::Normal, move || {
            queued_flag.store(true, Ordering::Release);
        })
        .unwrap();
    assert!(!queued_ran.load(Ordering::Acquire));
    gate.open();
    blocker.join().unwrap();
    queued.join().unwrap();
    assert!(queued_ran.load(Ordering::Acquire));
    scheduler.shutdown();
}

struct YieldingTask {
    levels: Arc<Mutex<Vec<usize>>>,
    yields: usize,
}

impl Task for YieldingTask {
    type Output = ();

    fn run(&mut self, context: &TaskContext) -> TaskProgress<()> {
        lock(&self.levels).push(context.level());
        if self.yields == 0 {
            TaskProgress::Done(())
        } else {
            self.yields -= 1;
            TaskProgress::Yield
        }
    }
}

struct RecordLevel {
    level: Arc<Mutex<Option<usize>>>,
}

impl Task for RecordLevel {
    type Output = ();

    fn run(&mut self, context: &TaskContext) -> TaskProgress<()> {
        *lock(&self.level) = Some(context.level());
        TaskProgress::Done(())
    }
}

#[test]
fn yielding_tasks_demote_and_feedback_applies_to_the_key() {
    let scheduler = Scheduler::new(config(1, 1, 16)).unwrap();
    let levels = Arc::new(Mutex::new(Vec::new()));
    scheduler
        .spawn_task(
            TaskKey::new(7),
            Priority::High,
            YieldingTask {
                levels: Arc::clone(&levels),
                yields: 2,
            },
        )
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(*lock(&levels), [0, 1, 2]);

    let level = Arc::new(Mutex::new(None));
    scheduler
        .spawn_task(
            TaskKey::new(7),
            Priority::High,
            RecordLevel {
                level: Arc::clone(&level),
            },
        )
        .unwrap()
        .join()
        .unwrap();
    // The final short dispatch promoted the key one level, but it is still
    // below the High base level: feedback carried across dispatches.
    assert_eq!(*lock(&level), Some(1));
    scheduler.shutdown();
}

#[test]
fn aging_promotes_a_waiting_task_one_level_per_interval() {
    struct NoopTask;

    impl ErasedTask for NoopTask {
        fn run(&mut self, _: &TaskContext) -> ErasedProgress {
            ErasedProgress::Done
        }

        fn fail(&mut self, _: TaskError) {}

        fn publish(&mut self) {}
    }

    let now = Instant::now();
    let aging = Duration::from_millis(10);
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut state = PoolState::new();
    state.levels[2].push_back(QueuedTask {
        key: TaskKey::new(1),
        class: TaskClass::Business,
        priority: Priority::Low,
        level: 2,
        enqueued_at: now.checked_sub(aging * 2).unwrap(),
        cancelled,
        task: Box::new(NoopTask),
    });
    state.queued = 1;
    promote_aged(&mut state, now, aging);
    assert_eq!(state.levels[1].len(), 1);
    assert!(state.levels[2].is_empty());
    promote_aged(&mut state, now, aging);
    assert_eq!(state.levels[0].len(), 1);
    assert!(state.levels[1].is_empty());
}

#[test]
fn cancellation_refuses_a_queued_task() {
    let scheduler = Scheduler::new(config(1, 1, 16)).unwrap();
    let gate = Arc::new(Gate::new());
    let (started_tx, started_rx) = mpsc::channel();
    let blocker_gate = Arc::clone(&gate);
    let blocker = scheduler
        .spawn(TaskKey::new(1), Priority::High, move || {
            started_tx.send(()).unwrap();
            blocker_gate.wait();
        })
        .unwrap();
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("business blocker must start");
    let (ran_tx, ran_rx) = mpsc::channel();
    let queued = scheduler
        .spawn(TaskKey::new(2), Priority::Normal, move || {
            ran_tx.send(()).unwrap();
        })
        .unwrap();
    queued.cancel();
    gate.open();
    blocker.join().unwrap();
    assert!(matches!(queued.join(), Err(TaskError::Cancelled)));
    assert!(ran_rx.try_recv().is_err());
    scheduler.shutdown();
}

#[test]
fn a_panicking_task_does_not_poison_the_worker() {
    let scheduler = Scheduler::new(config(1, 1, 16)).unwrap();
    let panicked = scheduler
        .spawn(TaskKey::new(1), Priority::High, || -> () {
            panic!("intentional scheduler test panic");
        })
        .unwrap();
    assert!(matches!(panicked.join(), Err(TaskError::Panicked)));
    let value = scheduler
        .spawn(TaskKey::new(2), Priority::Normal, || 7)
        .unwrap();
    assert_eq!(value.join().unwrap(), 7);
    scheduler.shutdown();
}

#[test]
fn queue_capacity_refuses_while_the_worker_is_busy() {
    let scheduler = Scheduler::new(config(1, 1, 1)).unwrap();
    let gate = Arc::new(Gate::new());
    let (started_tx, started_rx) = mpsc::channel();
    let blocker_gate = Arc::clone(&gate);
    let blocker = scheduler
        .spawn(TaskKey::new(1), Priority::High, move || {
            started_tx.send(()).unwrap();
            blocker_gate.wait();
        })
        .unwrap();
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("business blocker must start");
    let _queued = scheduler
        .spawn(TaskKey::new(2), Priority::Normal, || {})
        .unwrap();
    assert!(matches!(
        scheduler.spawn(TaskKey::new(3), Priority::Low, || {}),
        Err(SchedulerError::QueueFull)
    ));
    gate.open();
    blocker.join().unwrap();
    scheduler.shutdown();
}

#[test]
fn shutdown_refuses_new_tasks() {
    let scheduler = Scheduler::new(config(1, 1, 1)).unwrap();
    scheduler.shutdown();
    assert!(matches!(
        scheduler.spawn(TaskKey::new(1), Priority::Normal, || {}),
        Err(SchedulerError::Shutdown)
    ));
    assert!(matches!(
        scheduler.spawn_background(TaskKey::new(2), Priority::Low, || {}),
        Err(SchedulerError::Shutdown)
    ));
}

struct ShutdownAwareTask {
    started: mpsc::Sender<()>,
    observed: Arc<Mutex<Option<TaskError>>>,
}

impl Task for ShutdownAwareTask {
    type Output = ();

    fn run(&mut self, context: &TaskContext) -> TaskProgress<()> {
        self.started.send(()).unwrap();
        loop {
            match context.checkpoint() {
                Ok(()) => std::thread::yield_now(),
                Err(error) => {
                    *lock(&self.observed) = Some(error);
                    return TaskProgress::Done(());
                }
            }
        }
    }
}

#[test]
fn shutdown_interrupts_a_cooperative_dispatch() {
    let scheduler = Scheduler::new(config(1, 1, 8)).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let observed = Arc::new(Mutex::new(None));
    let handle = scheduler
        .spawn_task(
            TaskKey::new(1),
            Priority::High,
            ShutdownAwareTask {
                started: started_tx,
                observed: Arc::clone(&observed),
            },
        )
        .unwrap();
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("task must enter its dispatch");
    scheduler.shutdown();
    assert_eq!(*lock(&observed), Some(TaskError::Shutdown));
    assert!(matches!(handle.join(), Err(TaskError::Shutdown)));
}

struct GateYieldTask {
    gate: Arc<Gate>,
    started: mpsc::Sender<()>,
    yielded: bool,
}

impl Task for GateYieldTask {
    type Output = ();

    fn run(&mut self, _: &TaskContext) -> TaskProgress<()> {
        if self.yielded {
            TaskProgress::Done(())
        } else {
            self.started.send(()).unwrap();
            self.gate.wait();
            self.yielded = true;
            TaskProgress::Yield
        }
    }
}

#[test]
fn continuation_is_admitted_when_the_new_task_queue_is_full() {
    let scheduler = Scheduler::new(config(1, 1, 1)).unwrap();
    let gate = Arc::new(Gate::new());
    let (started_tx, started_rx) = mpsc::channel();
    let first = scheduler
        .spawn_task(
            TaskKey::new(1),
            Priority::High,
            GateYieldTask {
                gate: Arc::clone(&gate),
                started: started_tx,
                yielded: false,
            },
        )
        .unwrap();
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("first task must enter its dispatch");
    let second = scheduler
        .spawn(TaskKey::new(2), Priority::Normal, || {})
        .unwrap();
    gate.open();
    first.join().unwrap();
    second.join().unwrap();
    scheduler.shutdown();
}

struct SlowTask;

impl Task for SlowTask {
    type Output = ();

    fn run(&mut self, _: &TaskContext) -> TaskProgress<()> {
        // Exceed the one-nanosecond quantum without sleeping; the test is
        // about feedback publication, not about racing the clock.
        for index in 0..1024 {
            std::hint::black_box(index);
        }
        TaskProgress::Done(())
    }
}

#[test]
fn feedback_is_published_before_join_returns() {
    let scheduler = Scheduler::new(SchedulerConfig {
        business_workers: 1,
        background_workers: 1,
        queue_capacity: 64,
        quantum: Duration::from_nanos(1),
        aging_interval: Duration::from_millis(50),
    })
    .unwrap();
    for index in 0..32u64 {
        let key = TaskKey::new(index);
        scheduler
            .spawn_task(key, Priority::High, SlowTask)
            .unwrap()
            .join()
            .unwrap();
        let level = Arc::new(Mutex::new(None));
        scheduler
            .spawn_task(
                key,
                Priority::High,
                RecordLevel {
                    level: Arc::clone(&level),
                },
            )
            .unwrap()
            .join()
            .unwrap();
        assert_eq!(*lock(&level), Some(1), "iteration {index}");
    }
    scheduler.shutdown();
}

struct PanicOnDropTask;

impl Task for PanicOnDropTask {
    type Output = ();

    fn run(&mut self, _: &TaskContext) -> TaskProgress<()> {
        TaskProgress::Done(())
    }
}

impl Drop for PanicOnDropTask {
    fn drop(&mut self) {
        panic!("intentional task teardown panic");
    }
}

#[test]
fn task_teardown_panic_does_not_retire_the_worker() {
    let scheduler = Scheduler::new(config(1, 1, 8)).unwrap();
    scheduler
        .spawn_task(TaskKey::new(1), Priority::High, PanicOnDropTask)
        .unwrap()
        .join()
        .unwrap();
    let second = scheduler
        .spawn(TaskKey::new(2), Priority::Normal, || 7)
        .unwrap();
    let result = second
        .receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("the worker must survive task teardown")
        .expect("the second task must complete");
    assert_eq!(result, 7);
    scheduler.shutdown();
}
