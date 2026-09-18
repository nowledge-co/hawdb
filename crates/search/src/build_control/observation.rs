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
