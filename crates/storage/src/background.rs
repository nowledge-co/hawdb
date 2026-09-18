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

//! Storage-owned admission boundary for optional background maintenance.

use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackgroundWorkRequest {
    pub cpu_slots: usize,
    pub memory_bytes: u64,
    pub io_slots: usize,
}

/// An explicitly implemented resource lease for admitted background work.
/// Implementations must retain their reservation until this value is dropped.
/// Providers may implement this trait without depending on a particular governor.
///
/// Arbitrary values are not permits:
///
/// ```compile_fail
/// use hawdb_storage::BackgroundWorkPermit;
/// let permit: Box<dyn BackgroundWorkPermit> = Box::new(());
/// ```
pub trait BackgroundWorkPermit: Debug + Send {}

pub trait BackgroundWorkAdmission: Debug + Send + Sync {
    fn try_admit(
        &self,
        request: BackgroundWorkRequest,
    ) -> std::result::Result<Box<dyn BackgroundWorkPermit>, String>;
}
