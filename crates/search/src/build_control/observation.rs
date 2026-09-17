//! Count actual cooperative checks on the current test thread only.

use std::cell::Cell;

thread_local! {
    static CALLS: Cell<Option<usize>> = const { Cell::new(None) };
}

pub(super) fn record() {
    CALLS.with(|calls| calls.set(calls.get().map(|count| count + 1)));
}

pub(crate) fn measure<T>(work: impl FnOnce() -> T) -> (T, usize) {
    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CALLS.set(self.0);
        }
    }
    let _restore = Restore(CALLS.replace(Some(0)));
    let result = work();
    (result, CALLS.get().expect("active checkpoint observation"))
}
