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

#[cfg(feature = "full-text-search")]
use hawdb_search::{SearchFusionWeights, SearchQueryOptions};
#[cfg(feature = "full-text-search")]
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

thread_local! {
    // Keep the fixture's FD domain owned until its test thread exits.
    static TEST_PROJECTS: std::cell::RefCell<Vec<hawdb_storage::file_descriptors::ProjectFileDescriptors>> = const { std::cell::RefCell::new(Vec::new()) };
}

pub(crate) fn test_dir(name: &str) -> PathBuf {
    let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = std::env::var_os("TEST_TMPDIR").map_or_else(std::env::temp_dir, PathBuf::from);
    let path = temporary.join(format!(
        "hawdb-search-out-of-core-{name}-{}-{sequence}",
        std::process::id()
    ));
    let files = hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire(
        &path,
        hawdb_storage::file_descriptors::DEFAULT_MAX_OPEN_FILES,
    )
    .unwrap();
    TEST_PROJECTS.with_borrow_mut(|owners| owners.push(files));
    path
}

#[cfg(feature = "full-text-search")]
pub(crate) fn search_options(limit: usize, rank_window: Option<usize>) -> SearchQueryOptions {
    SearchQueryOptions {
        limit,
        offset: 0,
        rank_window,
        fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    }
}
