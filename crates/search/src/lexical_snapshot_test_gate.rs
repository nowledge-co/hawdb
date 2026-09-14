use std::cell::RefCell;
use std::sync::mpsc::{self, Receiver, Sender};

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
        self.captured
            .recv()
            .expect("query ended before capturing its lexical snapshot");
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
