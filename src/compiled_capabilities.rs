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

use crate::RuntimeCapabilities;

pub use hawdb_search::compiled_runtime_capabilities;

pub(crate) const fn effective_runtime_capabilities(
    requested: RuntimeCapabilities,
) -> RuntimeCapabilities {
    requested.intersection(compiled_runtime_capabilities())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeCapability;

    #[test]
    fn compiled_matrix_matches_enabled_cargo_features() {
        let capabilities = compiled_runtime_capabilities();
        assert_eq!(capabilities, hawdb_search::compiled_runtime_capabilities());

        assert_eq!(
            capabilities.is_enabled(RuntimeCapability::AccessControl),
            cfg!(feature = "acl")
        );
        assert_eq!(
            capabilities.is_enabled(RuntimeCapability::FullTextSearch),
            cfg!(feature = "full-text-search")
        );
        assert_eq!(
            capabilities.is_enabled(RuntimeCapability::VectorSearch),
            cfg!(feature = "vector-search")
        );
        assert_eq!(
            capabilities.is_enabled(RuntimeCapability::GraphAnalytics),
            cfg!(feature = "graph-analytics")
        );
        assert_eq!(
            capabilities.is_enabled(RuntimeCapability::BackgroundMaintenance),
            cfg!(feature = "background-maintenance")
        );
    }
}
