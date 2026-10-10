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

use crate::build_memory::BuildMemory;
use crate::{Result, RuntimeTaskContext};
use hawdb_executor::QueryMemoryAccount;

/// A view of already admitted operation accounts; this never creates a ledger.
#[derive(Clone, Copy)]
pub(crate) enum Memory<'a> {
    Build(&'a BuildMemory),
    Query(&'a QueryMemoryAccount),
}

impl<'a> Memory<'a> {
    pub(crate) fn input(self) -> &'a QueryMemoryAccount {
        match self {
            Self::Build(memory) => &memory.input,
            Self::Query(memory) => memory,
        }
    }

    pub(crate) fn retained(self) -> &'a QueryMemoryAccount {
        match self {
            Self::Build(memory) => &memory.retained,
            Self::Query(memory) => memory,
        }
    }

    pub(crate) fn spool(self) -> &'a QueryMemoryAccount {
        match self {
            Self::Build(memory) => &memory.spool,
            Self::Query(memory) => memory,
        }
    }

    pub(crate) fn checkpoint(self, task: &RuntimeTaskContext) -> Result<()> {
        match self {
            Self::Build(_) => crate::build_control::checkpoint(task),
            Self::Query(_) => crate::query_control::checkpoint(task),
        }
    }
}
