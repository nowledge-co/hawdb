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

//! Runtime clocks shared by query timing, deadlines and UUID generation.
//!
//! Native builds retain the standard-library types. Browser WASM uses the host
//! performance/wall clocks: `std::time::Instant::now()` is unsupported there.

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use std::time::{Duration, Instant, SystemTime, SystemTimeError, UNIX_EPOCH};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use web_time::{Duration, Instant, SystemTime, SystemTimeError, UNIX_EPOCH};
