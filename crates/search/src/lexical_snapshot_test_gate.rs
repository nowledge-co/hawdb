//! Rendezvous for synchronous lexical snapshot tests.
//!
//! `QueryGate::run` and snapshot capture must execute on the same OS thread.
//! Tests that offload capture must move the gate onto that worker explicitly.
//! When a query returns or panics without reaching the hook, the scoped reset
//! disconnects the controller immediately. A running query still has a bounded
//! capture wait. Runtime qualification probes have separate reporting and
//! latency-budget contracts and do not use this assertion-based controller.

use std::cell::RefCell;
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

/// Bound the controller wait below the CI job timeout while allowing a query
/// to reach its capture point under load.
const CAPTURE_TIMEOUT: Duration = Duration::from_secs(60);

thread_local! {
    static PENDING_QUERY: RefCell<Option<QueryGate>> = const { RefCell::new(None) };
}

pub(crate) struct QueryGate {
    captured: Sender<()>,
    resume: Receiver<()>,
}

pub(crate) struct QueryController {
    captured: Receiver<()>,
    resume: Sender<()>,
}

pub(crate) fn query_gate() -> (QueryGate, QueryController) {
    let (captured_tx, captured_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    (
        QueryGate {
            captured: captured_tx,
            resume: resume_rx,
        },
        QueryController {
            captured: captured_rx,
            resume: resume_tx,
        },
    )
}

impl QueryGate {
    pub(crate) fn run<T>(self, query: impl FnOnce() -> T) -> T {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                // An early return or panic must disconnect a waiting controller.
                PENDING_QUERY.with(|pending| pending.borrow_mut().take());
            }
        }

        PENDING_QUERY.with(|pending| {
            let mut pending = pending.borrow_mut();
            assert!(pending.is_none(), "query gates cannot be nested");
            *pending = Some(self);
        });
        let _reset = Reset;
        query()
    }
}

impl QueryController {
    pub(crate) fn wait_until_captured(&self) {
        match self.captured.recv_timeout(CAPTURE_TIMEOUT) {
            Ok(()) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!(
                    "query ended without reaching the lexical snapshot hook; QueryGate::run and snapshot capture must run on the same OS thread"
                )
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!(
                    "query did not capture its lexical snapshot within {CAPTURE_TIMEOUT:?}; QueryGate::run and snapshot capture must run on the same OS thread"
                )
            }
        }
    }
}

impl Drop for QueryController {
    fn drop(&mut self) {
        // Release the query before a scoped thread join, including during unwind.
        let _ = self.resume.send(());
    }
}

pub(crate) fn pause_after_capture() {
    if let Some(gate) = PENDING_QUERY.with(|pending| pending.borrow_mut().take()) {
        let _ = gate.captured.send(());
        let _ = gate.resume.recv();
    }
}

#[test]
fn early_query_return_disconnects_controller() {
    let (gate, controller) = query_gate();
    assert_eq!(gate.run(|| 42), 42);
    assert!(controller.captured.recv().is_err());
}

#[test]
fn controller_unwind_releases_captured_query() {
    let (gate, controller) = query_gate();
    std::thread::scope(|scope| {
        let query = scope.spawn(|| gate.run(pause_after_capture));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let controller = controller;
            controller.wait_until_captured();
            panic!("controlled checkpoint failure");
        }));
        assert!(result.is_err());
        query.join().unwrap();
    });
}

#[test]
fn offloaded_capture_reports_the_thread_affinity_contract() {
    let (gate, controller) = query_gate();
    gate.run(|| std::thread::spawn(pause_after_capture).join().unwrap());
    let error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        controller.wait_until_captured();
    }))
    .unwrap_err();
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .expect("capture failure carries a diagnostic");
    assert!(message.contains("same OS thread"), "{message}");
}

#[test]
fn query_unwind_disconnects_controller() {
    let (gate, controller) = query_gate();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        gate.run(|| panic!("query failed before capture"));
    }));
    assert!(result.is_err());
    assert!(controller.captured.recv().is_err());
}
