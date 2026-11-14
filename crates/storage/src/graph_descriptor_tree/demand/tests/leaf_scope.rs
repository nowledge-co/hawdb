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

//! #1615: retained descriptor leaves answer prefixes that lie strictly inside
//! them without a root traversal. Results must equal ordinary prefix scans at
//! every leaf edge, retention must stay scoped to one thread, one root and one
//! codec-limit selection, and limits and poison must still apply.

use super::super::leaf_scope::retained_leaves;
use super::*;

const ENDPOINTS: u64 = 600;
const PER_ENDPOINT: u64 = 3;

fn leaf_config() -> GraphDescriptorTreeBuildConfig {
    let mut config = config();
    config.page_limits.max_page_bytes = NonZeroUsize::new(8 * 1024).unwrap();
    config.page_limits.max_entries = NonZeroUsize::new(25).unwrap();
    config
}

fn key(endpoint: u64, item: u64) -> Vec<u8> {
    let mut key = endpoint.to_be_bytes().to_vec();
    key.extend_from_slice(&item.to_be_bytes());
    key
}

fn endpoint_of(key: &[u8]) -> u64 {
    u64::from_be_bytes(key[..8].try_into().unwrap())
}

/// Endpoints `e % 7 == 3` have no descriptors; every other endpoint has
/// three. Small pages give dozens of leaves under two interior levels.
fn build_endpoints(root: &Path) -> GraphDescriptorTreeWriteOutput {
    let mut builder = GraphDescriptorTreeBuilder::create(
        paths(root),
        GraphDescriptorKind::CanonicalAdjacency,
        7,
        19,
        0x534b_4744_4144_4a31,
        leaf_config(),
    )
    .unwrap();
    for endpoint in 0..ENDPOINTS {
        if endpoint % 7 == 3 {
            continue;
        }
        for item in 0..PER_ENDPOINT {
            builder
                .push(
                    key(endpoint, item),
                    (endpoint * 10 + item).to_le_bytes().to_vec(),
                )
                .unwrap();
        }
    }
    builder.finish().unwrap().publish().unwrap()
}

fn open_endpoints(root: &Path) -> GraphDescriptorTreeDemandReader {
    GraphDescriptorTreeDemandReader::open(
        GraphDescriptorTreeRootReader::open(paths(root), leaf_config()).unwrap(),
        leaf_config(),
        Arc::new(SegmentCache::new(1024 * 1024)),
        StoreId(17),
    )
    .unwrap()
}

type Scan = (
    Vec<u64>,
    GraphDescriptorTreeScanControl,
    GraphDescriptorTreeReadReport,
);

fn scan(
    reader: &GraphDescriptorTreeDemandReader,
    prefix: &[u8],
    retained: bool,
    read_limits: GraphDescriptorTreeReadLimits,
    stop_after: Option<usize>,
) -> Result<Scan, GraphDescriptorTreeError> {
    let mut values = Vec::new();
    let consumer = |_: &[u8], value: &[u8]| {
        values.push(u64::from_le_bytes(value.try_into().unwrap()));
        Ok(if stop_after.is_some_and(|limit| values.len() >= limit) {
            GraphDescriptorTreeScanControl::Stop
        } else {
            GraphDescriptorTreeScanControl::Continue
        })
    };
    let (report, control) = if retained {
        reader.scan_prefix_retained(prefix, read_limits, consumer)?
    } else {
        reader.scan_prefix(prefix, read_limits, consumer)?
    };
    Ok((values, control, report))
}

/// Every leaf page in key order with its keys.
fn leaves(
    artifact_path: &Path,
    reference: &GraphDescriptorPageRef,
    remaining_height: u32,
    output: &mut Vec<(GraphDescriptorPageRef, Vec<Vec<u8>>)>,
) {
    let mut file = File::open(artifact_path).unwrap();
    file.seek(SeekFrom::Start(reference.offset)).unwrap();
    let mut encoded = vec![0u8; reference.length.get() as usize];
    file.read_exact(&mut encoded).unwrap();
    let page = ImmutableGraphDescriptorPage::decode_bound(
        reference,
        GraphDescriptorKind::CanonicalAdjacency,
        19,
        &encoded,
        leaf_config().page_limits,
    )
    .unwrap();
    match page.body {
        ImmutableGraphDescriptorPageBody::Leaf(entries) => {
            assert_eq!(remaining_height, 0);
            output.push((
                reference.clone(),
                entries.into_iter().map(|entry| entry.key).collect(),
            ));
        }
        ImmutableGraphDescriptorPageBody::Interior(entries) => {
            for entry in entries {
                leaves(artifact_path, &entry.child, remaining_height - 1, output);
            }
        }
    }
}

fn all_leaves(
    directory: &Path,
    output: &GraphDescriptorTreeWriteOutput,
) -> Vec<(GraphDescriptorPageRef, Vec<Vec<u8>>)> {
    let mut found = Vec::new();
    leaves(
        &paths(directory).page_artifact,
        output.root.root.as_ref().unwrap(),
        output.root.height,
        &mut found,
    );
    found
}

/// A present endpoint whose descriptors lie strictly inside `leaf`.
fn inner_endpoint(leaf: &GraphDescriptorPageRef) -> Option<u64> {
    (endpoint_of(&leaf.lower_bound) + 1..endpoint_of(&leaf.upper_bound))
        .find(|endpoint| endpoint % 7 != 3)
}

fn hits() -> u64 {
    descriptor_retained_leaf_hits_for_test()
}

#[test]
fn retained_leaves_answer_consecutive_endpoint_seeks_without_root_traversal() {
    let directory = TestDirectory::new("leaf-scope-consecutive");
    let output = build_endpoints(directory.path());
    assert!(output.root.height >= 2, "fixture needs interior levels");
    let leaf_count = all_leaves(directory.path(), &output).len() as u64;
    let reader = open_endpoints(directory.path());
    let reference = open_endpoints(directory.path());
    let ordinary_pages = u64::from(output.root.height) + 1;

    // Outside a scope nothing is retained or reused.
    let before = hits();
    for endpoint in [5u64, 6] {
        let (values, _, report) =
            scan(&reader, &endpoint.to_be_bytes(), true, limits(), None).unwrap();
        assert_eq!(values.len() as u64, PER_ENDPOINT);
        assert_eq!(report.pages_visited, ordinary_pages);
        assert_eq!(report.retained_leaf_hits, 0);
    }
    assert_eq!(hits(), before);
    assert_eq!(retained_leaves(), 0);

    let scope = DescriptorLeafScope::enter();
    let mut pages = 0;
    let mut retained_hits = 0;
    for endpoint in 0..ENDPOINTS {
        let prefix = endpoint.to_be_bytes();
        let ordinary = scan(&reference, &prefix, false, limits(), None).unwrap();
        let (values, control, report) = scan(&reader, &prefix, true, limits(), None).unwrap();
        assert_eq!(
            (&values, control),
            (&ordinary.0, ordinary.1),
            "endpoint {endpoint}"
        );
        assert_eq!(report.descriptors_emitted, ordinary.2.descriptors_emitted);
        assert_eq!(
            report.leaf_entries_examined,
            ordinary.2.leaf_entries_examined
        );
        assert_eq!(report.maximum_depth, ordinary.2.maximum_depth);
        if report.retained_leaf_hits == 1 {
            retained_hits += 1;
            assert_eq!(report.pages_visited, 1);
            assert_eq!(report.cache_hits + report.cache_misses, 0);
            assert_eq!(report.storage_bytes_read, 0);
        } else {
            assert_eq!(report.pages_visited, ordinary.2.pages_visited);
        }
        pages += report.pages_visited;
    }
    assert_eq!(hits() - before, retained_hits);
    // Only prefixes touching a leaf edge (about two per leaf) traverse from
    // the root; every other seek reuses the leaf the previous one reached.
    assert!(
        retained_hits + 3 * leaf_count >= ENDPOINTS,
        "{retained_hits} retained hits over {leaf_count} leaves"
    );
    assert!(
        pages * 3 < ENDPOINTS * ordinary_pages * 2,
        "{pages} pages for {ENDPOINTS} seeks of {ordinary_pages} pages"
    );
    assert!(retained_leaves() > 0);
    assert!(reader.cache.snapshot().pinned_bytes > 0);

    // Nested scopes keep the leaves; the outermost releases every pin.
    drop(DescriptorLeafScope::enter());
    assert!(retained_leaves() > 0);
    drop(scope);
    assert_eq!(retained_leaves(), 0);
    assert_eq!(reader.cache.snapshot().pinned_bytes, 0);
    assert!(!reader.is_poisoned());
}

#[test]
fn retained_leaf_edges_spans_and_empty_ranges_match_ordinary_scans() {
    let directory = TestDirectory::new("leaf-scope-edges");
    let output = build_endpoints(directory.path());
    let all = all_leaves(directory.path(), &output);
    let reader = open_endpoints(directory.path());
    let reference = open_endpoints(directory.path());
    let _scope = DescriptorLeafScope::enter();
    // `expect_hit: None` reaches a leaf without asserting how.
    let check = |prefix: &[u8], expect_hit: Option<bool>| {
        let ordinary = scan(&reference, prefix, false, limits(), None).unwrap();
        let before = hits();
        let (values, control, report) = scan(&reader, prefix, true, limits(), None).unwrap();
        assert_eq!(
            (values.clone(), control),
            (ordinary.0, ordinary.1),
            "{prefix:?}"
        );
        assert_eq!(hits() - before, report.retained_leaf_hits);
        if let Some(expect_hit) = expect_hit {
            assert_eq!(
                report.retained_leaf_hits,
                u64::from(expect_hit),
                "{prefix:?}"
            );
        }
        values
    };

    // A leaf whose last endpoint continues in the next leaf.
    let (index, (leaf, keys)) = all
        .iter()
        .enumerate()
        .find(|(index, (leaf, keys))| {
            keys.len() >= 12
                && all.get(index + 1).is_some_and(|(next, _)| {
                    endpoint_of(&next.lower_bound) == endpoint_of(&leaf.upper_bound)
                })
        })
        .expect("some endpoint spans two leaves");
    // Reach `leaf` with a full key strictly inside it.
    let warm = || assert_eq!(check(&keys[1], None).len(), 1);
    warm();
    // Strictly inside: answered from the retained leaf.
    assert_eq!(check(&keys[2], Some(true)).len(), 1);
    // A prefix equal to the leaf's first or last key touches its edge.
    assert_eq!(check(&leaf.lower_bound, Some(false)).len(), 1);
    warm();
    assert_eq!(check(&leaf.upper_bound, Some(false)).len(), 1);
    // The endpoint prefix of the last key spans two leaves.
    warm();
    let spanning = check(&leaf.upper_bound[..8], Some(false));
    assert_eq!(spanning.len() as u64, PER_ENDPOINT);
    assert_eq!(
        endpoint_of(&all[index + 1].0.lower_bound),
        endpoint_of(&leaf.upper_bound)
    );
    // An empty range strictly inside the leaf: after one key, before the next.
    warm();
    let mut between = keys[1].clone();
    between.push(0);
    assert!(check(&between, Some(true)).is_empty());
    // A stopping consumer sees the same first descriptor from a retained leaf.
    // The middle key's endpoint is neither the leaf's first nor last endpoint.
    let mut partial = keys[keys.len() / 2].clone();
    partial.truncate(15);
    let ordinary = scan(&reference, &partial, false, limits(), Some(1)).unwrap();
    let retained = scan(&reader, &partial, true, limits(), Some(1)).unwrap();
    assert_eq!(retained.2.retained_leaf_hits, 1);
    assert_eq!((retained.0, retained.1), (ordinary.0, ordinary.1));

    // A missing endpoint strictly inside some leaf: empty, retained.
    let (gap_leaf, gap_keys) = all
        .iter()
        .find(|(leaf, _)| {
            (endpoint_of(&leaf.lower_bound) + 1..endpoint_of(&leaf.upper_bound))
                .any(|endpoint| endpoint % 7 == 3)
        })
        .expect("a missing endpoint lies inside some leaf");
    let gap = (endpoint_of(&gap_leaf.lower_bound) + 1..)
        .find(|endpoint| endpoint % 7 == 3)
        .unwrap();
    check(&gap_keys[1], None);
    assert!(check(&gap.to_be_bytes(), Some(true)).is_empty());
    // After every leaf's range.
    assert!(check(&ENDPOINTS.to_be_bytes(), Some(false)).is_empty());
    assert!(check(&[0xff], Some(false)).is_empty());
}

#[test]
fn retained_leaves_respect_identity_limits_threads_and_poison() {
    let directory = TestDirectory::new("leaf-scope-identity");
    let output = build_endpoints(directory.path());
    let all = all_leaves(directory.path(), &output);
    let (leaf, keys, inner) = all
        .iter()
        .find_map(|(leaf, keys)| inner_endpoint(leaf).map(|inner| (leaf, keys, inner)))
        .expect("some leaf holds an endpoint strictly inside it");
    let reader = open_endpoints(directory.path());
    let scope = DescriptorLeafScope::enter();
    let inner_prefix = inner.to_be_bytes();
    let warm = |reader: &GraphDescriptorTreeDemandReader| {
        scan(reader, &keys[1], true, limits(), None).unwrap();
    };
    let inside = |reader: &GraphDescriptorTreeDemandReader| {
        scan(reader, &inner_prefix, true, limits(), None).unwrap()
    };
    warm(&reader);
    assert_eq!(inside(&reader).2.retained_leaf_hits, 1);
    let expected = inside(&reader).0;
    assert_eq!(expected.len() as u64, PER_ENDPOINT);

    // A clone shares the root and limits, so it shares retained leaves.
    assert_eq!(inside(&reader.clone()).2.retained_leaf_hits, 1);
    // Different codec limits or another opened root never match.
    let mut relaxed = reader.clone();
    relaxed.config.page_limits.max_value_bytes = NonZeroUsize::new(127).unwrap();
    let relaxed_scan = inside(&relaxed);
    assert_eq!(relaxed_scan.2.retained_leaf_hits, 0);
    assert_eq!(relaxed_scan.0, expected);
    let reopened = open_endpoints(directory.path());
    assert_eq!(inside(&reopened).2.retained_leaf_hits, 0);

    // Another thread has no scope and sees nothing retained here.
    let other = reader.clone();
    let other_expected = expected.clone();
    std::thread::spawn(move || {
        let before = descriptor_retained_leaf_hits_for_test();
        let (values, _, report) = scan(&other, &inner_prefix, true, limits(), None).unwrap();
        assert_eq!(values, other_expected);
        assert_eq!(report.retained_leaf_hits, 0);
        assert_eq!(descriptor_retained_leaf_hits_for_test(), before);
        assert_eq!(retained_leaves(), 0);
    })
    .join()
    .unwrap();

    // A retained leaf still counts against the page and page-byte limits;
    // refusal is admission, not corruption.
    warm(&reader);
    let mut one_page = limits();
    one_page.max_pages = NonZeroU64::new(1).unwrap();
    let (values, _, report) = scan(&reader, &inner_prefix, true, one_page, None).unwrap();
    assert_eq!((values, report.pages_visited), (expected.clone(), 1));
    assert!(matches!(
        scan(&reader, &inner_prefix, false, one_page, None),
        Err(GraphDescriptorTreeError::Admission(_))
    ));
    let mut few_bytes = limits();
    few_bytes.max_page_bytes = NonZeroU64::new(leaf.length.get() - 1).unwrap();
    assert!(matches!(
        scan(&reader, &inner_prefix, true, few_bytes, None),
        Err(GraphDescriptorTreeError::Admission(_))
    ));
    assert!(!reader.is_poisoned());

    // A poisoned reader refuses even prefixes its retained leaves hold.
    let damaged = all
        .iter()
        .map(|(candidate, _)| candidate)
        .rfind(|candidate| candidate.page_id != leaf.page_id)
        .unwrap();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(paths(directory.path()).page_artifact)
        .unwrap();
    file.seek(SeekFrom::Start(
        damaged.offset + damaged.length.get().saturating_sub(1),
    ))
    .unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_all().unwrap();
    assert!(scan(&reader, &damaged.upper_bound, true, limits(), None).is_err());
    assert!(reader.is_poisoned());
    let poisoned = scan(&reader, &inner_prefix, true, limits(), None)
        .expect_err("poisoned reader must fail closed");
    assert!(poisoned.to_string().contains("poisoned"));
    drop(scope);
    assert_eq!(reader.cache.snapshot().pinned_bytes, 0);
}
