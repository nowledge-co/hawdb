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

//! Isolate allocator instrumentation from the library and its other tests.

#[path = "support/allocation.rs"]
mod allocation;
use allocation::measure;

use hawdb_search::{SearchDocument, SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

struct TestDirectory(PathBuf);

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn round_trip(bytes: usize, report: bool) -> usize {
    let root = TestDirectory(std::env::temp_dir().join(format!(
        "hawdb-segment-allocation-{}-{}",
        std::process::id(), SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
    )));
    let source = SearchDocument {
        id: "large-document".into(),
        title: "segment encoding sentinel".into(),
        content: " ".repeat(bytes),
        embedding: None,
        metadata: BTreeMap::from([("kind".into(), "fixture".into())]),
    };
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root.0, Default::default()).unwrap();
    writer.push(source.clone()).unwrap();
    // This window includes spool decode, analysis, segment encoding and
    // publication. Caller-owned input and subsequent hydration are excluded.
    let (result, total) = measure(|| writer.finish());
    assert_eq!(result.unwrap().document_count, 1);
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(
        reader
            .hydrate_documents(std::slice::from_ref(&source.id))
            .unwrap()
            .documents,
        vec![source]
    );
    if report {
        println!("source_bytes={bytes} requested_bytes={total}");
    }
    total
}

#[test]
fn generation_finish_does_not_allocate_an_encoded_source_copy() {
    // Initialize shared analyzer state before observing allocator requests.
    round_trip(1024, false);
    let results = [1024 * 1024, 3 * 1024 * 1024].map(|bytes| (bytes, round_trip(bytes, true)));
    for (bytes, requested) in results {
        // The complete finish path still allocates decoded input and analyzer
        // state. Its cumulative requests must leave out the old encoded copies.
        assert!(
            requested <= bytes * 8,
            "source={bytes}, requested={requested}"
        );
    }
}
