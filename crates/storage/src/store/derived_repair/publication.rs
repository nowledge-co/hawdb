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

use super::audit;
use crate::error::Result;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Carries the prepared journal through ordinary checkpoint/head publication.
/// The repair store retains its UUID lease until audit completion. Shared
/// immutable objects retain the normal no-replacement publication contract.
#[derive(Debug)]
pub(in crate::store) struct DerivedRepairPublication {
    directory: PathBuf,
    record: Mutex<audit::DerivedRepairAuditRecord>,
}

impl DerivedRepairPublication {
    pub(super) fn from_prepared(directory: &Path, record: audit::DerivedRepairAuditRecord) -> Self {
        Self {
            directory: directory.to_path_buf(),
            record: Mutex::new(record),
        }
    }

    pub(in crate::store) fn record_target_head(
        &self,
        target: &crate::branch_head::BranchHead,
    ) -> Result<()> {
        let mut record = self
            .record
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        audit::record_target_head(&self.directory, &mut record, target)
    }
}
