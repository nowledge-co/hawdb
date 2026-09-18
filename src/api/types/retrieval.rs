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

pub use hawdb_nowledge_contracts::public::*;

#[cfg(test)]
mod facade_tests {
    use super::{BackgroundMaintenanceKind, KnowledgeGraphContextPath, KnowledgeRetrievalRequest};

    #[test]
    fn facade_reexports_nowledge_contract_types() {
        let _: fn(
            KnowledgeRetrievalRequest,
        ) -> hawdb_nowledge_contracts::KnowledgeRetrievalRequest = |value| value;
        let _: fn(
            BackgroundMaintenanceKind,
        ) -> hawdb_nowledge_contracts::BackgroundMaintenanceKind = |value| value;
        let _: fn(
            KnowledgeGraphContextPath,
        ) -> hawdb_nowledge_contracts::KnowledgeGraphContextPath = |value| value;
    }
}
