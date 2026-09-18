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

#[path = "query/connected_enumeration.rs"]
mod connected_enumeration;
#[path = "query/costed_algorithms.rs"]
mod costed_algorithms;

fn constrained_hash_join_memory() -> hawdb_executor::ExecutionMemoryConfig {
    hawdb_executor::ExecutionMemoryConfig {
        blocking_operator_bytes: std::num::NonZeroUsize::new(512)
            .expect("non-zero blocking budget"),
        max_spill_bytes: std::num::NonZeroU64::new(64 * 1024).expect("non-zero spill budget"),
        max_spill_runs: std::num::NonZeroUsize::new(4).expect("non-zero spill run budget"),
        min_spill_free_bytes: std::num::NonZeroU64::MIN,
        spill_directory: std::env::temp_dir().join(format!(
            "hawdb-hash-join-spill-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        )),
        ..hawdb_executor::ExecutionMemoryConfig::default()
    }
}
