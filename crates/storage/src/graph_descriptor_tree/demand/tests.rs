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

use super::*;
use crate::graph_descriptor_page::{GraphDescriptorPageLimits, ImmutableGraphDescriptorPageBody};
use crate::graph_descriptor_tree::{
    GraphDescriptorTreeBuilder, GraphDescriptorTreePaths, GraphDescriptorTreeWriteOutput,
};
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(name: &str) -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hawdb-graph-descriptor-demand-{name}-{}-{sequence}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn paths(root: &Path) -> GraphDescriptorTreePaths {
    GraphDescriptorTreePaths::new(
        root.join("adjacency-descriptors-7.pages.hawdb"),
        root.join("adjacency-descriptors-7.root.hawdb"),
    )
}

fn config() -> GraphDescriptorTreeBuildConfig {
    GraphDescriptorTreeBuildConfig {
        page_limits: GraphDescriptorPageLimits {
            max_page_bytes: NonZeroUsize::new(512).unwrap(),
            max_entries: NonZeroUsize::new(4).unwrap(),
            max_key_bytes: NonZeroUsize::new(64).unwrap(),
            max_value_bytes: NonZeroUsize::new(128).unwrap(),
        },
        max_root_bytes: NonZeroUsize::new(4_096).unwrap(),
        max_page_count: NonZeroU64::new(16_384).unwrap(),
        max_page_artifact_bytes: NonZeroU64::new(16 * 1024 * 1024).unwrap(),
        max_intermediate_bytes: NonZeroU64::new(16 * 1024 * 1024).unwrap(),
    }
}

fn limits() -> GraphDescriptorTreeReadLimits {
    GraphDescriptorTreeReadLimits {
        max_pages: NonZeroU64::new(1_024).unwrap(),
        max_page_bytes: NonZeroU64::new(4 * 1024 * 1024).unwrap(),
        max_descriptors: NonZeroU64::new(1_024).unwrap(),
        max_tree_height: NonZeroU32::new(16).unwrap(),
    }
}

fn build(root: &Path) -> GraphDescriptorTreeWriteOutput {
    let mut builder = GraphDescriptorTreeBuilder::create(
        paths(root),
        GraphDescriptorKind::CanonicalAdjacency,
        7,
        19,
        0x534b_4744_4144_4a31,
        config(),
    )
    .unwrap();
    for group in 0u64..10 {
        for item in 0u64..100 {
            let mut key = Vec::with_capacity(16);
            key.extend_from_slice(&group.to_be_bytes());
            key.extend_from_slice(&item.to_be_bytes());
            builder.push(key, item.to_le_bytes().to_vec()).unwrap();
        }
    }
    builder.finish().unwrap().publish().unwrap()
}

fn open_reader(root: &Path, cache_bytes: u64) -> GraphDescriptorTreeDemandReader {
    let root_reader = GraphDescriptorTreeRootReader::open(paths(root), config()).unwrap();
    GraphDescriptorTreeDemandReader::open(
        root_reader,
        config(),
        Arc::new(SegmentCache::new(cache_bytes)),
        StoreId(17),
    )
    .unwrap()
}

fn scan_group(
    reader: &GraphDescriptorTreeDemandReader,
    group: u64,
    read_limits: GraphDescriptorTreeReadLimits,
) -> Result<(Vec<u64>, GraphDescriptorTreeReadReport), GraphDescriptorTreeError> {
    let mut values = Vec::new();
    let (report, control) =
        reader.scan_prefix(&group.to_be_bytes(), read_limits, |_, encoded_value| {
            values.push(u64::from_le_bytes(encoded_value.try_into().map_err(
                |_| corrupt("test descriptor value has an invalid length"),
            )?));
            Ok(GraphDescriptorTreeScanControl::Continue)
        })?;
    assert_eq!(control, GraphDescriptorTreeScanControl::Continue);
    Ok((values, report))
}

fn scan_from(
    reader: &GraphDescriptorTreeDemandReader,
    group: u64,
    item: u64,
    max_descriptors: u64,
) -> Result<(Vec<(u64, u64)>, GraphDescriptorTreeReadReport), GraphDescriptorTreeError> {
    let mut lower_bound = Vec::with_capacity(16);
    lower_bound.extend_from_slice(&group.to_be_bytes());
    lower_bound.extend_from_slice(&item.to_be_bytes());
    let mut values = Vec::new();
    let mut read_limits = limits();
    read_limits.max_descriptors = NonZeroU64::new(max_descriptors).unwrap();
    let (report, _) = reader.scan_from(&lower_bound, read_limits, |key, encoded_value| {
        let key_group = u64::from_be_bytes(key[..8].try_into().unwrap());
        let key_item = u64::from_be_bytes(key[8..].try_into().unwrap());
        let value = u64::from_le_bytes(encoded_value.try_into().unwrap());
        assert_eq!(key_item, value);
        values.push((key_group, key_item));
        Ok(if values.len() as u64 == max_descriptors {
            GraphDescriptorTreeScanControl::Stop
        } else {
            GraphDescriptorTreeScanControl::Continue
        })
    })?;
    Ok((values, report))
}

#[test]
fn prefix_scan_is_bounded_ordered_and_cacheable() {
    let directory = TestDirectory::new("prefix");
    let output = build(directory.path());
    let reader = open_reader(directory.path(), 128 * 1024);
    let (cold_values, cold) = scan_group(&reader, 7, limits()).unwrap();
    assert_eq!(cold_values, (0..100).collect::<Vec<_>>());
    assert_eq!(cold.descriptors_emitted, 100);
    assert!(cold.pages_visited < output.root.page_count);
    assert!(cold.storage_bytes_read > 0);
    assert!(cold.page_bytes_decoded >= cold.storage_bytes_read);
    assert!(cold.cache_misses > 0);

    let (warm_values, warm) = scan_group(&reader, 7, limits()).unwrap();
    assert_eq!(warm_values, cold_values);
    assert_eq!(warm.storage_bytes_read, 0);
    assert_eq!(warm.page_bytes_decoded, cold.page_bytes_decoded);
    assert_eq!(warm.cache_hits, warm.pages_visited);
    assert!(!reader.is_poisoned());
}

#[test]
fn lower_bound_scan_seeks_to_the_first_greater_or_equal_descriptor() {
    let directory = TestDirectory::new("lower-bound");
    let output = build(directory.path());
    let reader = open_reader(directory.path(), 128 * 1024);

    let (values, cold) = scan_from(&reader, 4, 37, 3).unwrap();
    assert_eq!(values, vec![(4, 37), (4, 38), (4, 39)]);
    assert_eq!(cold.descriptors_emitted, 3);
    assert!(cold.pages_visited < output.root.page_count);
    assert!(cold.storage_bytes_read > 0);

    let (after_last, _) = scan_from(&reader, 10, 0, 1).unwrap();
    assert!(after_last.is_empty());
    assert!(!reader.is_poisoned());
}

#[test]
fn page_admission_does_not_poison_the_reader() {
    let directory = TestDirectory::new("admission");
    build(directory.path());
    let reader = open_reader(directory.path(), 128 * 1024);
    let mut read_limits = limits();
    read_limits.max_pages = NonZeroU64::new(1).unwrap();
    let error = scan_group(&reader, 3, read_limits)
        .expect_err("one page cannot traverse a multi-level tree");
    assert!(error.to_string().contains("page"));
    assert!(!reader.is_poisoned());
    assert_eq!(scan_group(&reader, 3, limits()).unwrap().0.len(), 100);
}

#[test]
fn warm_cache_cannot_bypass_the_page_byte_budget() {
    let directory = TestDirectory::new("warm-byte-admission");
    build(directory.path());
    let reader = open_reader(directory.path(), 128 * 1024);
    assert_eq!(scan_group(&reader, 3, limits()).unwrap().0.len(), 100);

    let mut read_limits = limits();
    read_limits.max_page_bytes = NonZeroU64::new(1).unwrap();
    let error = scan_group(&reader, 3, read_limits)
        .expect_err("warm pages must remain subject to the decoded-byte budget");
    assert!(error.to_string().contains("page bytes"));
    assert!(!reader.is_poisoned());
}

#[test]
fn page_codec_admission_does_not_poison_the_reader() {
    let directory = TestDirectory::new("page-codec-admission");
    build(directory.path());
    let root_reader =
        GraphDescriptorTreeRootReader::open(paths(directory.path()), config()).unwrap();
    let mut read_config = config();
    read_config.page_limits.max_page_bytes = NonZeroUsize::new(1).unwrap();
    let reader = GraphDescriptorTreeDemandReader::open(
        root_reader,
        read_config,
        Arc::new(SegmentCache::new(128 * 1024)),
        StoreId(17),
    )
    .unwrap();

    let error = scan_group(&reader, 3, limits())
        .expect_err("page format admission must reject the oversized encoded page");
    assert!(matches!(
        error,
        GraphDescriptorTreeError::Page(GraphDescriptorPageError::Admission(_))
    ));
    assert!(!reader.is_poisoned());
}

#[test]
fn deep_scrub_checks_the_complete_tree_without_warming_the_cache() {
    let directory = TestDirectory::new("scrub");
    let output = build(directory.path());
    let reader = open_reader(directory.path(), 128 * 1024);
    let before = reader.cache.snapshot();
    let report = reader
        .deep_visit(|_, _| Ok(GraphDescriptorTreeScanControl::Continue))
        .unwrap();
    let after = reader.cache.snapshot();
    assert_eq!(report.checked_pages, output.root.page_count);
    assert_eq!(report.checked_leaf_pages, output.root.leaf_page_count);
    assert_eq!(report.checked_descriptors, output.root.descriptor_count);
    assert_eq!(report.artifact_bytes_hashed, output.root.page_artifact_len);
    assert_eq!(report.page_bytes_decoded, output.root.page_artifact_len);
    assert_eq!(before, after);
}

#[test]
fn deep_scrub_detects_corruption_outside_a_warm_lookup_path_and_poison_is_sticky() {
    let directory = TestDirectory::new("corrupt");
    let output = build(directory.path());
    let reader = open_reader(directory.path(), 128 * 1024);
    assert_eq!(scan_group(&reader, 0, limits()).unwrap().0.len(), 100);

    let target = find_leaf_for_prefix(
        &paths(directory.path()).page_artifact,
        output.root.root.as_ref().unwrap(),
        output.root.height,
        &9u64.to_be_bytes(),
    );
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(paths(directory.path()).page_artifact)
        .unwrap();
    file.seek(SeekFrom::Start(
        target.offset + target.length.get().saturating_sub(1),
    ))
    .unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_all().unwrap();

    assert_eq!(scan_group(&reader, 0, limits()).unwrap().0.len(), 100);
    let error = reader
        .deep_visit(|_, _| Ok(GraphDescriptorTreeScanControl::Continue))
        .expect_err("scrub must reject corruption outside the warm path");
    assert!(error.to_string().contains("checksum mismatch"));
    assert!(reader.is_poisoned());
    let poisoned =
        scan_group(&reader, 0, limits()).expect_err("poisoned descriptor reader must fail closed");
    assert!(poisoned.to_string().contains("poisoned"));
}

fn find_leaf_for_prefix(
    artifact_path: &Path,
    reference: &GraphDescriptorPageRef,
    remaining_height: u32,
    prefix: &[u8],
) -> GraphDescriptorPageRef {
    let mut file = File::open(artifact_path).unwrap();
    file.seek(SeekFrom::Start(reference.offset)).unwrap();
    let mut encoded = vec![0u8; reference.length.get() as usize];
    file.read_exact(&mut encoded).unwrap();
    let page = ImmutableGraphDescriptorPage::decode_bound(
        reference,
        GraphDescriptorKind::CanonicalAdjacency,
        19,
        &encoded,
        config().page_limits,
    )
    .unwrap();
    match page.body {
        ImmutableGraphDescriptorPageBody::Leaf(_) => {
            assert_eq!(remaining_height, 0);
            reference.clone()
        }
        ImmutableGraphDescriptorPageBody::Interior(entries) => {
            assert!(remaining_height > 0);
            let child = entries
                .into_iter()
                .find(|entry| range_can_contain_prefix(&entry.child, prefix))
                .expect("tree contains the requested prefix");
            find_leaf_for_prefix(artifact_path, &child.child, remaining_height - 1, prefix)
        }
    }
}
