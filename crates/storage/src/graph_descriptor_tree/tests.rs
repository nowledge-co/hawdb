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
use crate::durability::fail_durable_replace_for_destination;
use std::error::Error;
use std::io::{Seek, SeekFrom};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(name: &str) -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hawdb-graph-descriptor-tree-{name}-{}-{sequence}",
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

fn tiny_config() -> GraphDescriptorTreeBuildConfig {
    GraphDescriptorTreeBuildConfig {
        page_limits: GraphDescriptorPageLimits {
            max_page_bytes: NonZeroUsize::new(512).unwrap(),
            max_entries: NonZeroUsize::new(4).unwrap(),
            max_key_bytes: NonZeroUsize::new(64).unwrap(),
            max_value_bytes: NonZeroUsize::new(128).unwrap(),
        },
        max_root_bytes: NonZeroUsize::new(4096).unwrap(),
        max_page_count: NonZeroU64::new(4096).unwrap(),
        max_page_artifact_bytes: NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        max_intermediate_bytes: NonZeroU64::new(8 * 1024 * 1024).unwrap(),
    }
}

fn build(root: &Path, count: u64) -> GraphDescriptorTreeWriteOutput {
    let tree_paths = paths(root);
    let mut builder = GraphDescriptorTreeBuilder::create(
        tree_paths,
        GraphDescriptorKind::CanonicalAdjacency,
        7,
        19,
        0x534b_4744_4144_4a31,
        tiny_config(),
    )
    .unwrap();
    for value in 0..count {
        builder
            .push(value.to_be_bytes().to_vec(), vec![value as u8; 48])
            .unwrap();
    }
    let prepared = builder.finish().unwrap();
    prepared.verify_encoded_root().unwrap();
    prepared.publish().unwrap()
}

#[test]
fn streaming_tree_round_trips_with_bounded_residency() {
    let directory = TestDirectory::new("streaming");
    let output = build(directory.path(), 2_000);
    assert_eq!(output.root.descriptor_count, 2_000);
    assert!(output.root.height >= 2);
    assert!(output.report.page_count > output.report.leaf_page_count);
    assert!(output.report.peak_resident_bytes < 64 * 1024);
    assert!(output.report.peak_intermediate_level_bytes < output.report.page_artifact_bytes);

    let reader =
        GraphDescriptorTreeRootReader::open(paths(directory.path()), tiny_config()).unwrap();
    assert_eq!(reader.root(), &output.root);
    assert_eq!(reader.report().page_payload_bytes_read, 0);
    assert_eq!(reader.report().root_bytes_read, output.report.root_bytes);
}

#[test]
fn bound_open_rejects_root_artifact_or_identity_drift() {
    let directory = TestDirectory::new("bound-root");
    let output = build(directory.path(), 32);
    GraphDescriptorTreeRootReader::open_bound(
        paths(directory.path()),
        output.generation_artifacts(),
        tiny_config(),
    )
    .unwrap();

    let mut artifact_drift = output.generation_artifacts();
    artifact_drift.root_artifact.encoded_sha256 = Sha256Digest::from_bytes([0x5a; 32]);
    let error = GraphDescriptorTreeRootReader::open_bound(
        paths(directory.path()),
        artifact_drift,
        tiny_config(),
    )
    .expect_err("root artifact drift must fail closed");
    assert!(error.to_string().contains("canonical artifact binding"));

    let mut identity_drift = output.generation_artifacts();
    identity_drift.source_commit_epoch += 1;
    let error = GraphDescriptorTreeRootReader::open_bound(
        paths(directory.path()),
        identity_drift,
        tiny_config(),
    )
    .expect_err("root identity drift must fail closed");
    assert!(error
        .to_string()
        .contains("does not match canonical binding"));
}

#[test]
fn empty_tree_publishes_a_bound_empty_artifact() {
    let directory = TestDirectory::new("empty");
    let output = build(directory.path(), 0);
    assert_eq!(output.root.descriptor_count, 0);
    assert_eq!(output.root.page_count, 0);
    assert!(output.root.root.is_none());
    assert_eq!(
        fs::metadata(paths(directory.path()).page_artifact)
            .unwrap()
            .len(),
        0
    );
    GraphDescriptorTreeRootReader::open(paths(directory.path()), tiny_config()).unwrap();
}

#[test]
fn corrupted_root_fails_closed_before_page_payload_reads() {
    let directory = TestDirectory::new("corrupt-root");
    build(directory.path(), 32);
    let root_path = paths(directory.path()).root_manifest;
    let mut encoded = fs::read(&root_path).unwrap();
    encoded[84] ^= 0x40;
    fs::write(&root_path, encoded).unwrap();
    let error = GraphDescriptorTreeRootReader::open(paths(directory.path()), tiny_config())
        .expect_err("corrupted root must fail");
    assert!(error.to_string().contains("checksum mismatch"));
}

#[test]
fn page_artifact_is_durable_before_root_publication() {
    let directory = TestDirectory::new("root-failure");
    let tree_paths = paths(directory.path());
    let mut builder = GraphDescriptorTreeBuilder::create(
        tree_paths.clone(),
        GraphDescriptorKind::CanonicalAdjacency,
        7,
        19,
        0x534b_4744_4144_4a31,
        tiny_config(),
    )
    .unwrap();
    for value in 0u64..32 {
        builder
            .push(value.to_be_bytes().to_vec(), vec![value as u8; 48])
            .unwrap();
    }
    let prepared = builder.finish().unwrap();
    let _guard = fail_durable_replace_for_destination(
        tree_paths.root_manifest.file_name().unwrap().to_owned(),
    );
    let error = prepared.publish().unwrap_err();
    assert_eq!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .map(std::io::Error::kind),
        Some(std::io::ErrorKind::PermissionDenied)
    );
    assert!(tree_paths.page_artifact.exists());
    assert!(!tree_paths.root_manifest.exists());
}

#[test]
fn root_rejects_page_artifact_length_drift_without_reading_pages() {
    let directory = TestDirectory::new("artifact-length");
    build(directory.path(), 32);
    let tree_paths = paths(directory.path());
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(&tree_paths.page_artifact)
        .unwrap();
    file.seek(SeekFrom::End(0)).unwrap();
    file.write_all(&[0]).unwrap();
    file.sync_all().unwrap();
    let error = GraphDescriptorTreeRootReader::open(tree_paths, tiny_config())
        .expect_err("artifact length drift must fail");
    assert!(error.to_string().contains("length mismatch"));
}

#[test]
fn page_artifact_budget_rejects_before_writing_an_oversized_page() {
    let directory = TestDirectory::new("artifact-budget");
    let tree_paths = paths(directory.path());
    let mut config = tiny_config();
    config.max_page_artifact_bytes = NonZeroU64::new(100).unwrap();
    let mut builder = GraphDescriptorTreeBuilder::create(
        tree_paths.clone(),
        GraphDescriptorKind::CanonicalAdjacency,
        7,
        19,
        0x534b_4744_4144_4a31,
        config,
    )
    .unwrap();
    builder
        .push(1u64.to_be_bytes().to_vec(), vec![1; 48])
        .unwrap();
    let error = builder.finish().unwrap_err();
    assert!(error.to_string().contains("page artifact requires"));
    assert!(!tree_paths.page_artifact.exists());
    assert!(!tree_paths.root_manifest.exists());
    assert!(!tree_paths.page_tmp().exists());
    assert!(!tree_paths.ref_run(0).exists());
}

#[test]
fn builder_rejects_non_converging_interior_fanout() {
    let directory = TestDirectory::new("fanout");
    let tree_paths = paths(directory.path());
    let mut config = tiny_config();
    config.page_limits.max_entries = NonZeroUsize::new(1).unwrap();
    let error = GraphDescriptorTreeBuilder::create(
        tree_paths.clone(),
        GraphDescriptorKind::CanonicalAdjacency,
        7,
        19,
        0x534b_4744_4144_4a31,
        config,
    )
    .err()
    .expect("single-child interior pages must be rejected");
    assert!(error.to_string().contains("fanout of at least two"));
    assert!(!tree_paths.page_tmp().exists());
    assert!(!tree_paths.ref_run(0).exists());
}

#[test]
fn builder_rejects_owned_path_aliases() {
    let directory = TestDirectory::new("path-alias");
    let artifact = directory.path().join("descriptors.hawdb");
    let tree_paths = GraphDescriptorTreePaths::new(&artifact, &artifact);
    let error = GraphDescriptorTreeBuilder::create(
        tree_paths,
        GraphDescriptorKind::CanonicalAdjacency,
        7,
        19,
        0x534b_4744_4144_4a31,
        tiny_config(),
    )
    .err()
    .expect("owned graph descriptor paths must be distinct");
    assert!(error.to_string().contains("aliases another owned path"));
    assert!(!artifact.exists());
}

#[test]
fn root_rejects_height_beyond_its_interior_page_count() {
    let directory = TestDirectory::new("root-height");
    let mut root = build(directory.path(), 32).root;
    root.height = u32::try_from(root.page_count - root.leaf_page_count + 1).unwrap();
    let error = validate_root(&root, tiny_config(), ErrorClass::Corrupt)
        .expect_err("an impossible descriptor tree height must fail closed");
    assert!(error.to_string().contains("exceeds its"));
}

#[test]
fn publication_refuses_to_replace_an_existing_generation_artifact() {
    let directory = TestDirectory::new("immutable-publication");
    let tree_paths = paths(directory.path());
    let mut builder = GraphDescriptorTreeBuilder::create(
        tree_paths.clone(),
        GraphDescriptorKind::CanonicalAdjacency,
        7,
        19,
        0x534b_4744_4144_4a31,
        tiny_config(),
    )
    .unwrap();
    builder
        .push(1u64.to_be_bytes().to_vec(), vec![1; 48])
        .unwrap();
    let prepared = builder.finish().unwrap();
    fs::write(&tree_paths.page_artifact, b"existing").unwrap();
    let error = prepared
        .publish()
        .expect_err("immutable descriptor pages must not be replaced");
    assert!(error.to_string().contains("refuses to replace"));
    assert_eq!(fs::read(&tree_paths.page_artifact).unwrap(), b"existing");
    assert!(!tree_paths.root_manifest.exists());
}
