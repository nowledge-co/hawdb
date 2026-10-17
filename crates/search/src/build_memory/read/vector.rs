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

//! Vector reader uses the same query account and host governor as its parent.
use super::{ReadAdmission, ReaderMemory};
use crate::HawDBError;
use hawdb_vector_projection::{ProjectionError, ProjectionReadAdmission};

impl ProjectionReadAdmission for ReadAdmission<'_> {
    type Retained = ReaderMemory;
    type Scratch = hawdb_executor::QueryMemoryLease;
    type Io = Option<Box<dyn hawdb_core::RuntimeIoWavePermit>>;
    fn reserve_retained(&self, bytes: usize) -> hawdb_vector_projection::Result<Self::Retained> {
        self.retained(bytes).map_err(ProjectionError::Admission)
    }
    fn reserve_scratch(&self, bytes: usize) -> hawdb_vector_projection::Result<Self::Scratch> {
        self.scratch(bytes).map_err(ProjectionError::Admission)
    }
    fn acquire_io(&self) -> hawdb_vector_projection::Result<Self::Io> {
        self.io().map_err(|error| match error {
            hawdb_core::RuntimeIoWaveError::Stopped(reason) => ProjectionError::Cancelled(reason),
            error => ProjectionError::Admission(HawDBError::Execution(error.to_string())),
        })
    }
    fn checkpoint(&self) -> hawdb_vector_projection::Result<()> {
        ReadAdmission::checkpoint(self).map_err(ProjectionError::Cancelled)
    }
}
