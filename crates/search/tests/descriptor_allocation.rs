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
        "hawdb-descriptor-allocation-{}-{}",
        std::process::id(), SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
    )));
    let mut value = " ".repeat(bytes);
    value.replace_range(..1, "x");
    value.replace_range(bytes - 1.., "y");
    let source = SearchDocument {
        id: "large-document".into(),
        title: "descriptor encoding sentinel".into(),
        content: String::new(),
        embedding: None,
        metadata: BTreeMap::from([("note".into(), value)]),
    };
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root.0, Default::default()).unwrap();
    writer.push(source.clone()).unwrap();
    // This window includes spool decode, analysis, segment encoding and
    // publication. Caller-owned input and subsequent hydration are excluded.
    let (result, total) = measure(|| writer.finish());
    let result = result.unwrap();
    assert_eq!(result.document_count, 1);
    assert!(result.descriptor_bytes >= bytes as u64 * 2);
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
fn generation_descriptor_avoids_materializing_hex_dictionary() {
    // Initialize shared analyzer state before observing allocator requests.
    round_trip(1024, false);
    let results = [1024 * 1024, 3 * 1024 * 1024].map(|bytes| (bytes, round_trip(bytes, true)));
    for (bytes, requested) in results {
        // Keep the already admitted source/dictionary and analyzer costs. This
        // fixture budget excludes the old descriptor hex/body/footer copies.
        assert!(
            requested <= bytes * 12,
            "source={bytes}, requested={requested}"
        );
    }
}

fn repeated_labels(count: usize, json: bool) -> (usize, usize) {
    let root = TestDirectory(std::env::temp_dir().join(format!(
        "hawdb-descriptor-labels-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
    )));
    let value = if json {
        format!("[{}\"x\"]", "\"x\",".repeat(count - 1))
    } else {
        format!("{}x", "x,".repeat(count - 1))
    };
    let bytes = value.len();
    let source = SearchDocument {
        id: "repeated-labels".into(),
        title: String::new(),
        content: String::new(),
        embedding: None,
        metadata: BTreeMap::from([("labels".into(), value)]),
    };
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root.0, Default::default()).unwrap();
    writer.push(source.clone()).unwrap();
    let (result, requested) = measure(|| writer.finish());
    let result = result.unwrap();
    assert_eq!(result.document_count, 1);
    assert!(result.descriptor_bytes < 2048);
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(
        reader
            .hydrate_documents(std::slice::from_ref(&source.id))
            .unwrap()
            .documents,
        vec![source]
    );
    println!("label_count={count} json={json} source_bytes={bytes} requested_bytes={requested}");
    (bytes, requested)
}

#[test]
fn generation_descriptor_does_not_collect_repeated_labels() {
    round_trip(1024, false);
    let results = [false, true].map(|json| {
        let [small, large] = [32 * 1024, 128 * 1024].map(|count| repeated_labels(count, json));
        (json, small, large)
    });
    for (json, small, large) in results {
        // Exclude the fixed generation/analyzer floor. Growing the input must
        // not also grow a label array and per-occurrence owned strings.
        let growth = large.1.saturating_sub(small.1);
        assert!(
            growth <= (large.0 - small.0) * 8,
            "json={json}, small={small:?}, large={large:?}, growth={growth}"
        );
    }
}
