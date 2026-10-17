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

//! Same-governor retained capacity for privately opened vector artifacts.

use super::GovernedSearchGenerationWriter;
use crate::build_memory::read::ReadAdmission;
use hawdb_vector_projection::ProjectionReadAdmission;

#[doc(hidden)]
pub use crate::build_memory::read::ReaderMemory as ProjectionReaderMemory;

impl GovernedSearchGenerationWriter {
    fn reader_admission(&self) -> ReadAdmission<'_> {
        ReadAdmission {
            memory: &self.writer.memory,
            task: &self.writer.task_context,
            permit: &self.admission.permit,
        }
    }
}

impl ProjectionReadAdmission for GovernedSearchGenerationWriter {
    type Retained = ProjectionReaderMemory;
    type Scratch = hawdb_executor::QueryMemoryLease;
    type Io = Option<Box<dyn hawdb_core::RuntimeIoWavePermit>>;

    fn reserve_retained(&self, bytes: usize) -> hawdb_vector_projection::Result<Self::Retained> {
        self.reader_admission().reserve_retained(bytes)
    }

    fn reserve_scratch(&self, bytes: usize) -> hawdb_vector_projection::Result<Self::Scratch> {
        self.reader_admission().reserve_scratch(bytes)
    }

    fn acquire_io(&self) -> hawdb_vector_projection::Result<Self::Io> {
        self.reader_admission().acquire_io()
    }

    fn checkpoint(&self) -> hawdb_vector_projection::Result<()> {
        ProjectionReadAdmission::checkpoint(&self.reader_admission())
    }
}

#[cfg(test)]
mod tests;
