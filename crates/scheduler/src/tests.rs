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
        enqueued_at: now.checked_sub(aging).unwrap(),
        cancelled,
        task: Box::new(NoopTask),
    });
    state.queued = 1;
    promote_aged(&mut state, now, aging);
    assert_eq!(state.levels[1].len(), 1);
    assert!(state.levels[2].is_empty());
    promote_aged(&mut state, now, aging);
    assert_eq!(state.levels[1].len(), 1);
    assert!(state.levels[0].is_empty());
    promote_aged(&mut state, now + aging, aging);
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

struct CancelAwareTask {
    started: mpsc::Sender<()>,
}

impl Task for CancelAwareTask {
    type Output = ();

    fn run(&mut self, context: &TaskContext) -> TaskProgress<()> {
        self.started.send(()).unwrap();
        loop {
            if context.checkpoint().is_err() {
                return TaskProgress::Done(());
            }
            std::thread::yield_now();
        }
    }
}

#[test]
fn cancellation_interrupts_a_running_dispatch() {
    let scheduler = Scheduler::new(config(1, 1, 8)).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let handle = scheduler
        .spawn_task(
            TaskKey::new(1),
            Priority::High,
            CancelAwareTask {
                started: started_tx,
            },
        )
        .unwrap();
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("task must enter its dispatch");
    handle.cancel();
    assert!(matches!(handle.join(), Err(TaskError::Cancelled)));
    scheduler.shutdown();
}

struct PanicOnDropOutput {
    dropped: mpsc::Sender<()>,
}

impl Drop for PanicOnDropOutput {
    fn drop(&mut self) {
        let _ = self.dropped.send(());
        panic!("intentional output teardown panic");
    }
}

#[test]
fn shutdown_wakes_an_idle_worker_before_the_aging_interval() {
    let scheduler = Arc::new(
        Scheduler::new(SchedulerConfig {
            business_workers: 1,
            background_workers: 1,
            queue_capacity: 8,
            quantum: Duration::from_millis(5),
            aging_interval: Duration::from_secs(2),
        })
        .unwrap(),
    );
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
        .expect("blocker must start");

    let (probe, entered_rx) = WorkerWaitProbe::new();
    let probe = Arc::new(probe);
    scheduler.business.set_wait_probe(Some(Arc::clone(&probe)));
    let (stop_tx, stop_rx) = mpsc::channel();
    scheduler.business.set_stop_probe(Some(stop_tx));
    gate.open();
    blocker.join().unwrap();
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("worker must enter its idle wait probe");

    let (done_tx, done_rx) = mpsc::channel();
    let shutdown_scheduler = Arc::clone(&scheduler);
    let shutdown = std::thread::spawn(move || {
        shutdown_scheduler.shutdown();
        let _ = done_tx.send(());
    });
    stop_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("shutdown must reach the pool stop");
    let started = Instant::now();
    probe.release();
    let completed = done_rx.recv_timeout(Duration::from_secs(1));
    let elapsed = started.elapsed();
    if completed.is_err() {
        // Let the old lost-notification path finish before reporting it.
        let _ = done_rx.recv_timeout(Duration::from_secs(5));
    }
    shutdown.join().unwrap();
    assert!(
        completed.is_ok(),
        "shutdown waited for the aging interval instead of waking the idle worker"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "idle-worker shutdown took {elapsed:?}"
    );
}

#[test]
fn concurrent_shutdown_callers_wait_for_the_same_workers() {
    let scheduler = Arc::new(Scheduler::new(config(1, 1, 8)).unwrap());
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
        .expect("blocker must start");

    let (first_tx, first_rx) = mpsc::channel();
    let first_scheduler = Arc::clone(&scheduler);
    let first = std::thread::spawn(move || {
        first_scheduler.shutdown();
        let _ = first_tx.send(());
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while lock(&scheduler.shutdown).workers.is_some() {
        assert!(
            Instant::now() < deadline,
            "first shutdown caller did not take the worker handles"
        );
        std::thread::yield_now();
    }

    let (second_tx, second_rx) = mpsc::channel();
    let second_scheduler = Arc::clone(&scheduler);
    let second = std::thread::spawn(move || {
        second_scheduler.shutdown();
        let _ = second_tx.send(());
    });
    let early_return = second_rx.recv_timeout(Duration::from_millis(100)).is_ok();
    gate.open();
    let first_done = first_rx.recv_timeout(Duration::from_secs(5));
    let second_done = second_rx.recv_timeout(Duration::from_secs(5));
    first.join().unwrap();
    second.join().unwrap();
    assert!(
        !early_return,
        "a concurrent shutdown caller returned before the worker exited"
    );
    assert!(first_done.is_ok());
    assert!(second_done.is_ok());
    assert!(matches!(blocker.join(), Err(TaskError::Shutdown)));
}

struct BlockingDropTask {
    started: mpsc::Sender<()>,
    release: Arc<Gate>,
}

impl Task for BlockingDropTask {
    type Output = ();

    fn run(&mut self, _: &TaskContext) -> TaskProgress<()> {
        TaskProgress::Done(())
    }
}

impl Drop for BlockingDropTask {
    fn drop(&mut self) {
        let _ = self.started.send(());
        self.release.wait();
    }
}

#[test]
fn concurrent_shutdown_waits_for_queue_teardown() {
    let scheduler = Arc::new(Scheduler::new(config(1, 1, 8)).unwrap());
    let blocker_gate = Arc::new(Gate::new());
    let (started_tx, started_rx) = mpsc::channel();
    let blocker_wait = Arc::clone(&blocker_gate);
    let blocker = scheduler
        .spawn(TaskKey::new(1), Priority::High, move || {
            started_tx.send(()).unwrap();
            blocker_wait.wait();
        })
        .unwrap();
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("blocker must start");

    let drop_gate = Arc::new(Gate::new());
    let (drop_started_tx, drop_started_rx) = mpsc::channel();
    let queued = scheduler
        .spawn_task(
            TaskKey::new(2),
            Priority::Normal,
            BlockingDropTask {
                started: drop_started_tx,
                release: Arc::clone(&drop_gate),
            },
        )
        .unwrap();

    let (first_done_tx, first_done_rx) = mpsc::channel();
    let first_scheduler = Arc::clone(&scheduler);
    let first = std::thread::spawn(move || {
        first_scheduler.shutdown();
        let _ = first_done_tx.send(());
    });
    drop_started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("first shutdown must drain the queued task");

    let (second_entered_tx, second_entered_rx) = mpsc::channel();
    let (second_done_tx, second_done_rx) = mpsc::channel();
    let second_scheduler = Arc::clone(&scheduler);
    let second = std::thread::spawn(move || {
        let _ = second_entered_tx.send(());
        second_scheduler.shutdown();
        let _ = second_done_tx.send(());
    });
    second_entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("second shutdown caller must start");

    // Let the worker exit, but keep the first caller inside the queued task's
    // destructor. A second caller that only waits for worker joins would
    // return here while the destructor is still running.
    blocker_gate.open();
    let early_return = second_done_rx
        .recv_timeout(Duration::from_millis(200))
        .is_ok();
    drop_gate.open();
    let first_done = first_done_rx.recv_timeout(Duration::from_secs(5));
    let second_done = second_done_rx.recv_timeout(Duration::from_secs(5));
    first.join().unwrap();
    second.join().unwrap();
    assert!(
        !early_return,
        "a concurrent shutdown caller returned before queue teardown finished"
    );
    assert!(first_done.is_ok());
    assert!(second_done.is_ok());
    assert!(matches!(blocker.join(), Err(TaskError::Shutdown)));
    assert!(matches!(queued.join(), Err(TaskError::Shutdown)));
}

struct ReentrantShutdownTask {
    scheduler: Arc<Scheduler>,
    entered: mpsc::Sender<()>,
    returned: mpsc::Sender<()>,
}

struct JoinOnDropTask {
    handle: Option<TaskHandle<()>>,
    finished: mpsc::Sender<Result<(), TaskError>>,
}

impl Task for JoinOnDropTask {
    type Output = ();

    fn run(&mut self, _: &TaskContext) -> TaskProgress<()> {
        TaskProgress::Done(())
    }
}

impl Drop for JoinOnDropTask {
    fn drop(&mut self) {
        let result = self.handle.take().expect("background handle").join();
        let _ = self.finished.send(result);
    }
}

struct WaitForShutdown {
    started: mpsc::Sender<()>,
    fallback: Arc<AtomicBool>,
}

impl Task for WaitForShutdown {
    type Output = ();

    fn run(&mut self, context: &TaskContext) -> TaskProgress<()> {
        let _ = self.started.send(());
        while context.checkpoint().is_ok() && !self.fallback.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        TaskProgress::Done(())
    }
}

#[test]
fn shutdown_signals_both_pools_before_task_teardown() {
    let scheduler = Arc::new(Scheduler::new(config(1, 1, 8)).unwrap());
    let gate = Arc::new(Gate::new());
    let (business_started_tx, business_started_rx) = mpsc::channel();
    let business_gate = Arc::clone(&gate);
    let business = scheduler
        .spawn(TaskKey::new(1), Priority::High, move || {
            let _ = business_started_tx.send(());
            business_gate.wait();
        })
        .unwrap();
    business_started_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();

    let fallback = Arc::new(AtomicBool::new(false));
    let (background_started_tx, background_started_rx) = mpsc::channel();
    let background = scheduler
        .spawn_background_task(
            TaskKey::new(2),
            Priority::Normal,
            WaitForShutdown {
                started: background_started_tx,
                fallback: Arc::clone(&fallback),
            },
        )
        .unwrap();
    background_started_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    let (finished_tx, finished_rx) = mpsc::channel();
    let queued = scheduler
        .spawn_task(
            TaskKey::new(3),
            Priority::Normal,
            JoinOnDropTask {
                handle: Some(background),
                finished: finished_tx,
            },
        )
        .unwrap();

    let shutdown_scheduler = Arc::clone(&scheduler);
    let (done_tx, done_rx) = mpsc::channel();
    let shutdown = std::thread::spawn(move || {
        shutdown_scheduler.shutdown();
        let _ = done_tx.send(());
    });
    let result = finished_rx.recv_timeout(Duration::from_secs(2));
    fallback.store(true, Ordering::Release);
    gate.open();
    let done = done_rx.recv_timeout(Duration::from_secs(5));
    if done.is_ok() {
        shutdown.join().unwrap();
    }
    assert_eq!(result.unwrap(), Err(TaskError::Shutdown));
    assert!(done.is_ok());
    assert_eq!(business.join(), Err(TaskError::Shutdown));
    assert_eq!(queued.join(), Err(TaskError::Shutdown));
}

#[test]
fn shutdown_fails_both_queues_before_task_teardown() {
    let scheduler = Arc::new(Scheduler::new(config(1, 1, 8)).unwrap());
    let gate = Arc::new(Gate::new());
    let (started_tx, started_rx) = mpsc::channel();
    let business_gate = Arc::clone(&gate);
    let business_started = started_tx.clone();
    let business = scheduler
        .spawn(TaskKey::new(1), Priority::High, move || {
            let _ = business_started.send(());
            business_gate.wait();
        })
        .unwrap();
    let background_gate = Arc::clone(&gate);
    let background = scheduler
        .spawn_background(TaskKey::new(2), Priority::High, move || {
            let _ = started_tx.send(());
            background_gate.wait();
        })
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let background_queued = scheduler
        .spawn_background(TaskKey::new(3), Priority::Normal, || {})
        .unwrap();
    let (finished_tx, finished_rx) = mpsc::channel();
    let business_queued = scheduler
        .spawn_task(
            TaskKey::new(4),
            Priority::Normal,
            JoinOnDropTask {
                handle: Some(background_queued),
                finished: finished_tx,
            },
        )
        .unwrap();
    let shutdown_scheduler = Arc::clone(&scheduler);
    let (done_tx, done_rx) = mpsc::channel();
    let shutdown = std::thread::spawn(move || {
        shutdown_scheduler.shutdown();
        let _ = done_tx.send(());
    });
    let result = finished_rx.recv_timeout(Duration::from_secs(2));
    gate.open();
    let done = done_rx.recv_timeout(Duration::from_secs(5));
    if done.is_ok() {
        shutdown.join().unwrap();
    }
    assert_eq!(result.unwrap(), Err(TaskError::Shutdown));
    assert!(done.is_ok());
    assert_eq!(business.join(), Err(TaskError::Shutdown));
    assert_eq!(background.join(), Err(TaskError::Shutdown));
    assert_eq!(business_queued.join(), Err(TaskError::Shutdown));
}

impl Task for ReentrantShutdownTask {
    type Output = ();

    fn run(&mut self, _: &TaskContext) -> TaskProgress<()> {
        TaskProgress::Done(())
    }
}

impl Drop for ReentrantShutdownTask {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        self.scheduler.shutdown();
        let _ = self.returned.send(());
    }
}

#[test]
fn reentrant_shutdown_from_queue_teardown_does_not_deadlock() {
    let scheduler = Arc::new(Scheduler::new(config(1, 1, 8)).unwrap());
    let blocker_gate = Arc::new(Gate::new());
    let (started_tx, started_rx) = mpsc::channel();
    let blocker_wait = Arc::clone(&blocker_gate);
    let blocker = scheduler
        .spawn(TaskKey::new(1), Priority::High, move || {
            started_tx.send(()).unwrap();
            blocker_wait.wait();
        })
        .unwrap();
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("blocker must start");

    let (entered_tx, entered_rx) = mpsc::channel();
    let (returned_tx, returned_rx) = mpsc::channel();
    let queued = scheduler
        .spawn_task(
            TaskKey::new(2),
            Priority::Normal,
            ReentrantShutdownTask {
                scheduler: Arc::clone(&scheduler),
                entered: entered_tx,
                returned: returned_tx,
            },
        )
        .unwrap();

    let (done_tx, done_rx) = mpsc::channel();
    let shutdown_scheduler = Arc::clone(&scheduler);
    let shutdown = std::thread::spawn(move || {
        shutdown_scheduler.shutdown();
        let _ = done_tx.send(());
    });
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("queued task teardown must begin");
    let returned = returned_rx.recv_timeout(Duration::from_secs(2));
    blocker_gate.open();
    let done = done_rx.recv_timeout(Duration::from_secs(2));
    if done.is_ok() {
        shutdown.join().unwrap();
    }
    assert!(
        returned.is_ok(),
        "reentrant shutdown from a task destructor deadlocked"
    );
    assert!(
        done.is_ok(),
        "the original shutdown did not complete after reentrant teardown"
    );
    assert!(matches!(blocker.join(), Err(TaskError::Shutdown)));
    assert!(matches!(queued.join(), Err(TaskError::Shutdown)));
}

#[test]
fn scheduler_drop_on_its_worker_does_not_self_join() {
    let scheduler = Arc::new(Scheduler::new(config(1, 1, 8)).unwrap());
    let gate = Arc::new(Gate::new());
    let (started_tx, started_rx) = mpsc::channel();
    let worker_scheduler = Arc::clone(&scheduler);
    let blocker_gate = Arc::clone(&gate);
    let handle = scheduler
        .spawn(TaskKey::new(1), Priority::High, move || {
            started_tx.send(()).unwrap();
            blocker_gate.wait();
            drop(worker_scheduler);
        })
        .unwrap();
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("task must start");
    drop(scheduler);
    gate.open();
    let result = handle
        .receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("worker-originated scheduler drop must not self-join");
    assert!(matches!(result, Err(TaskError::Shutdown)));
}

#[test]
fn output_teardown_panic_does_not_retire_the_worker() {
    let scheduler = Scheduler::new(config(1, 1, 8)).unwrap();
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let handle = scheduler
        .spawn(TaskKey::new(1), Priority::High, move || PanicOnDropOutput {
            dropped: dropped_tx,
        })
        .unwrap();
    drop(handle);
    dropped_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("the output must be dropped by the worker");
    let second = scheduler
        .spawn(TaskKey::new(2), Priority::Normal, || 7)
        .unwrap();
    let result = second
        .receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("the worker must survive output teardown")
        .expect("the second task must complete");
    assert_eq!(result, 7);
    scheduler.shutdown();
}
