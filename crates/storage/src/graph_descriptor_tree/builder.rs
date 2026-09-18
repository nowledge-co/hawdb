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

//! Bounded streaming construction for immutable graph descriptor trees.

use super::{
    admission, corrupt, remove_if_exists, GraphDescriptorTreeBuildConfig,
    GraphDescriptorTreeBuildReport, GraphDescriptorTreeError, GraphDescriptorTreePaths,
    GraphDescriptorTreeRoot, PreparedGraphDescriptorTree, ROOT_HEADER_BYTES,
};
use crate::graph_descriptor_page::{
    decode_page_ref, encode_page_ref, GraphDescriptorInteriorEntry, GraphDescriptorKind,
    GraphDescriptorLeafEntry, GraphDescriptorPageId, GraphDescriptorPageLimits,
    GraphDescriptorPageRef, ImmutableGraphDescriptorPage, ImmutableGraphDescriptorPageBody,
    GRAPH_DESCRIPTOR_FIELD_HEADER_BYTES, GRAPH_DESCRIPTOR_PAGE_HEADER_BYTES,
};
use hawdb_integrity::IntegrityHasher;
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

const REF_RUN_MAGIC: &[u8; 8] = b"SKGDRF01";
const TREE_IO_BUFFER_BYTES: usize = 8 * 1024;

pub struct GraphDescriptorTreeBuilder {
    paths: GraphDescriptorTreePaths,
    kind: GraphDescriptorKind,
    generation: u64,
    source_commit_epoch: u64,
    artifact_id: u64,
    config: GraphDescriptorTreeBuildConfig,
    page_writer: BufWriter<File>,
    page_hasher: IntegrityHasher,
    page_artifact_bytes: u64,
    next_page_id: u64,
    descriptor_count: u64,
    page_count: u64,
    leaf_page_count: u64,
    leaf_entries: Vec<GraphDescriptorLeafEntry>,
    leaf_payload_bytes: usize,
    last_key: Option<Vec<u8>>,
    level_zero: Option<RefRunWriter>,
    total_intermediate_bytes: u64,
    peak_intermediate_level_bytes: u64,
    peak_resident_bytes: u64,
    temporary_files: TemporaryFiles,
}

impl GraphDescriptorTreeBuilder {
    pub fn create(
        paths: GraphDescriptorTreePaths,
        kind: GraphDescriptorKind,
        generation: u64,
        source_commit_epoch: u64,
        artifact_id: u64,
        config: GraphDescriptorTreeBuildConfig,
    ) -> Result<Self, GraphDescriptorTreeError> {
        if generation == 0 || artifact_id == 0 {
            return Err(admission(
                "graph descriptor tree generation and artifact id must be non-zero",
            ));
        }
        if config.page_limits.max_payload_bytes().is_none() {
            return Err(admission(
                "graph descriptor page limit is smaller than its fixed header",
            ));
        }
        if config.page_limits.max_entries.get() < 2 {
            return Err(admission(
                "graph descriptor tree requires an interior fanout of at least two",
            ));
        }
        if config.max_root_bytes.get() < ROOT_HEADER_BYTES {
            return Err(admission(format!(
                "graph descriptor root limit {} is smaller than its fixed header {ROOT_HEADER_BYTES}",
                config.max_root_bytes
            )));
        }
        if config.max_intermediate_bytes.get() < REF_RUN_MAGIC.len() as u64 {
            return Err(admission(
                "graph descriptor intermediate budget is smaller than a run header",
            ));
        }
        let page_tmp = paths.page_tmp();
        let root_tmp = paths.root_tmp();
        let level_zero_path = paths.ref_run(0);
        let owned_paths = [
            &paths.page_artifact,
            &paths.root_manifest,
            &page_tmp,
            &root_tmp,
            &level_zero_path,
        ];
        for (index, path) in owned_paths.iter().enumerate() {
            if owned_paths[index + 1..].contains(path) {
                return Err(admission(format!(
                    "graph descriptor tree path {} aliases another owned path",
                    path.display()
                )));
            }
        }
        for destination in [&paths.page_artifact, &paths.root_manifest] {
            if destination.exists() {
                return Err(admission(format!(
                    "immutable graph descriptor artifact {} already exists",
                    destination.display()
                )));
            }
        }
        remove_if_exists(&page_tmp)?;
        remove_if_exists(&root_tmp)?;
        remove_if_exists(&level_zero_path)?;
        let mut temporary_files = TemporaryFiles::default();
        temporary_files.track(page_tmp.clone());
        temporary_files.track(level_zero_path.clone());
        let page_writer = BufWriter::with_capacity(TREE_IO_BUFFER_BYTES, File::create(&page_tmp)?);
        let level_zero = RefRunWriter::create(level_zero_path)?;
        Ok(Self {
            paths,
            kind,
            generation,
            source_commit_epoch,
            artifact_id,
            config,
            page_writer,
            page_hasher: IntegrityHasher::new(),
            page_artifact_bytes: 0,
            next_page_id: 1,
            descriptor_count: 0,
            page_count: 0,
            leaf_page_count: 0,
            leaf_entries: Vec::new(),
            leaf_payload_bytes: 0,
            last_key: None,
            level_zero: Some(level_zero),
            total_intermediate_bytes: REF_RUN_MAGIC.len() as u64,
            peak_intermediate_level_bytes: 0,
            peak_resident_bytes: (2 * TREE_IO_BUFFER_BYTES) as u64,
            temporary_files,
        })
    }

    pub fn push(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), GraphDescriptorTreeError> {
        validate_leaf_input(&key, &value, self.config.page_limits)?;
        if self
            .last_key
            .as_ref()
            .is_some_and(|previous| previous.as_slice() >= key.as_slice())
        {
            return Err(admission(
                "graph descriptor keys must be supplied in strictly increasing order",
            ));
        }
        let entry_payload_bytes = leaf_entry_payload_bytes(key.len(), value.len())?;
        let max_payload_bytes = self
            .config
            .page_limits
            .max_payload_bytes()
            .expect("page limit was validated at builder creation");
        if entry_payload_bytes > max_payload_bytes {
            return Err(admission(format!(
                "graph descriptor leaf entry requires {entry_payload_bytes} payload bytes, exceeding page payload limit {max_payload_bytes}"
            )));
        }
        let page_full = !self.leaf_entries.is_empty()
            && (self.leaf_entries.len() == self.config.page_limits.max_entries.get()
                || self
                    .leaf_payload_bytes
                    .checked_add(entry_payload_bytes)
                    .is_none_or(|bytes| bytes > max_payload_bytes));
        if page_full {
            self.flush_leaf_page()?;
        }
        self.leaf_payload_bytes = self
            .leaf_payload_bytes
            .checked_add(entry_payload_bytes)
            .ok_or_else(|| admission("graph descriptor leaf payload length overflow"))?;
        self.last_key = Some(key.clone());
        self.leaf_entries
            .push(GraphDescriptorLeafEntry { key, value });
        self.descriptor_count = self
            .descriptor_count
            .checked_add(1)
            .ok_or_else(|| admission("graph descriptor count overflow"))?;
        self.observe_resident_leaf();
        Ok(())
    }

    pub fn finish(mut self) -> Result<PreparedGraphDescriptorTree, GraphDescriptorTreeError> {
        self.flush_leaf_page()?;
        let level_zero = self
            .level_zero
            .take()
            .expect("graph descriptor level-zero run is present")
            .finish()?;
        self.record_finished_run(&level_zero)?;
        let (root, height) = self.build_interior_levels(level_zero)?;
        self.page_writer.flush()?;
        self.page_writer.get_ref().sync_all()?;
        let page_integrity = self.page_hasher.clone().finish();
        let root_manifest = GraphDescriptorTreeRoot {
            kind: self.kind,
            generation: self.generation,
            source_commit_epoch: self.source_commit_epoch,
            page_artifact_id: self.artifact_id,
            page_artifact_len: self.page_artifact_bytes,
            page_artifact_crc32c: page_integrity.crc32c,
            page_artifact_sha256: page_integrity.sha256,
            descriptor_count: self.descriptor_count,
            page_count: self.page_count,
            leaf_page_count: self.leaf_page_count,
            height,
            root,
        };
        let encoded_root = root_manifest.encode(self.config)?;
        self.peak_resident_bytes = self.peak_resident_bytes.max(
            TREE_IO_BUFFER_BYTES
                .saturating_add(encoded_root.len())
                .saturating_add(
                    root_manifest
                        .root
                        .as_ref()
                        .map_or(0, page_ref_resident_bytes),
                ) as u64,
        );
        let report = GraphDescriptorTreeBuildReport {
            descriptor_count: self.descriptor_count,
            page_count: self.page_count,
            leaf_page_count: self.leaf_page_count,
            interior_page_count: self.page_count.saturating_sub(self.leaf_page_count),
            page_artifact_bytes: self.page_artifact_bytes,
            root_bytes: encoded_root.len() as u64,
            total_intermediate_bytes: self.total_intermediate_bytes,
            peak_intermediate_level_bytes: self.peak_intermediate_level_bytes,
            peak_resident_bytes: self.peak_resident_bytes,
        };
        let page_tmp = self.paths.page_tmp();
        self.temporary_files.disarm(&page_tmp);
        Ok(PreparedGraphDescriptorTree {
            paths: self.paths.clone(),
            config: self.config,
            root: root_manifest,
            encoded_root,
            report,
            page_tmp,
            published: false,
        })
    }

    fn flush_leaf_page(&mut self) -> Result<(), GraphDescriptorTreeError> {
        if self.leaf_entries.is_empty() {
            return Ok(());
        }
        let entries = std::mem::take(&mut self.leaf_entries);
        self.leaf_payload_bytes = 0;
        let page = ImmutableGraphDescriptorPage {
            kind: self.kind,
            physical_generation: self.generation,
            source_commit_epoch: self.source_commit_epoch,
            page_id: self.allocate_page_id()?,
            body: ImmutableGraphDescriptorPageBody::Leaf(entries),
        };
        let reference = self.write_page(page)?;
        self.leaf_page_count = self
            .leaf_page_count
            .checked_add(1)
            .ok_or_else(|| admission("graph descriptor leaf page count overflow"))?;
        self.write_level_zero_ref(&reference)
    }

    fn build_interior_levels(
        &mut self,
        mut current: RefRun,
    ) -> Result<(Option<GraphDescriptorPageRef>, u32), GraphDescriptorTreeError> {
        if current.count == 0 {
            self.remove_temporary(&current.path)?;
            return Ok((None, 0));
        }
        let mut height = 0u32;
        while current.count > 1 {
            height = height
                .checked_add(1)
                .ok_or_else(|| admission("graph descriptor tree height overflow"))?;
            let next_path = self.paths.ref_run(height);
            remove_if_exists(&next_path)?;
            self.temporary_files.track(next_path.clone());
            let required_intermediate = self
                .total_intermediate_bytes
                .checked_add(REF_RUN_MAGIC.len() as u64)
                .ok_or_else(|| admission("graph descriptor intermediate byte count overflow"))?;
            if required_intermediate > self.config.max_intermediate_bytes.get() {
                return Err(admission(format!(
                    "graph descriptor build requires {required_intermediate} intermediate bytes, exceeding limit {}",
                    self.config.max_intermediate_bytes
                )));
            }
            self.total_intermediate_bytes = required_intermediate;
            let mut next_writer = RefRunWriter::create(next_path)?;
            let mut reader = RefRunReader::open(&current.path, current.count, self.config)?;
            let mut children = Vec::new();
            let mut payload_bytes = 0usize;
            while let Some(child) = reader.next_ref()? {
                let child_bytes = interior_entry_payload_bytes(&child)?;
                let max_payload_bytes = self
                    .config
                    .page_limits
                    .max_payload_bytes()
                    .expect("page limit was validated at builder creation");
                if child_bytes > max_payload_bytes {
                    return Err(admission(format!(
                        "graph descriptor interior entry requires {child_bytes} payload bytes, exceeding page payload limit {max_payload_bytes}"
                    )));
                }
                let page_full = !children.is_empty()
                    && (children.len() == self.config.page_limits.max_entries.get()
                        || payload_bytes
                            .checked_add(child_bytes)
                            .is_none_or(|bytes| bytes > max_payload_bytes));
                if page_full {
                    let reference = self.flush_interior_page(&mut children)?;
                    self.write_ref(&mut next_writer, &reference)?;
                    payload_bytes = 0;
                }
                payload_bytes = payload_bytes
                    .checked_add(child_bytes)
                    .ok_or_else(|| admission("graph descriptor interior payload overflow"))?;
                children.push(GraphDescriptorInteriorEntry { child });
                self.observe_resident_interior(payload_bytes, &children);
            }
            reader.finish()?;
            drop(reader);
            if !children.is_empty() {
                let reference = self.flush_interior_page(&mut children)?;
                self.write_ref(&mut next_writer, &reference)?;
            }
            let next = next_writer.finish()?;
            self.record_finished_run(&next)?;
            self.remove_temporary(&current.path)?;
            current = next;
        }
        let mut reader = RefRunReader::open(&current.path, 1, self.config)?;
        let root = reader
            .next_ref()?
            .ok_or_else(|| corrupt("graph descriptor root reference run is empty"))?;
        reader.finish()?;
        drop(reader);
        self.remove_temporary(&current.path)?;
        Ok((Some(root), height))
    }

    fn flush_interior_page(
        &mut self,
        children: &mut Vec<GraphDescriptorInteriorEntry>,
    ) -> Result<GraphDescriptorPageRef, GraphDescriptorTreeError> {
        let page = ImmutableGraphDescriptorPage {
            kind: self.kind,
            physical_generation: self.generation,
            source_commit_epoch: self.source_commit_epoch,
            page_id: self.allocate_page_id()?,
            body: ImmutableGraphDescriptorPageBody::Interior(std::mem::take(children)),
        };
        self.write_page(page)
    }

    fn allocate_page_id(&mut self) -> Result<GraphDescriptorPageId, GraphDescriptorTreeError> {
        let page_id = NonZeroU64::new(self.next_page_id)
            .map(GraphDescriptorPageId::new)
            .ok_or_else(|| admission("graph descriptor page id overflow"))?;
        self.next_page_id = self
            .next_page_id
            .checked_add(1)
            .ok_or_else(|| admission("graph descriptor page id overflow"))?;
        Ok(page_id)
    }

    fn write_page(
        &mut self,
        page: ImmutableGraphDescriptorPage,
    ) -> Result<GraphDescriptorPageRef, GraphDescriptorTreeError> {
        let required_pages = self
            .page_count
            .checked_add(1)
            .ok_or_else(|| admission("graph descriptor page count overflow"))?;
        if required_pages > self.config.max_page_count.get() {
            return Err(admission(format!(
                "graph descriptor tree requires {required_pages} pages, exceeding limit {}",
                self.config.max_page_count
            )));
        }
        let (encoded, reference) = page.encode_with_ref(
            self.artifact_id,
            self.page_artifact_bytes,
            self.config.page_limits,
        )?;
        let required_artifact_bytes = self
            .page_artifact_bytes
            .checked_add(encoded.len() as u64)
            .ok_or_else(|| admission("graph descriptor page artifact length overflow"))?;
        if required_artifact_bytes > self.config.max_page_artifact_bytes.get() {
            return Err(admission(format!(
                "graph descriptor page artifact requires {required_artifact_bytes} bytes, exceeding limit {}",
                self.config.max_page_artifact_bytes
            )));
        }
        let page_resident_bytes = page_body_resident_bytes(&page.body)
            .saturating_add(encoded.len())
            .saturating_add(3 * TREE_IO_BUFFER_BYTES) as u64;
        self.peak_resident_bytes = self.peak_resident_bytes.max(page_resident_bytes);
        self.page_writer.write_all(&encoded)?;
        self.page_hasher.update(&encoded);
        self.page_artifact_bytes = required_artifact_bytes;
        self.page_count = required_pages;
        self.peak_resident_bytes = self.peak_resident_bytes.max(encoded.len() as u64);
        Ok(reference)
    }

    fn write_level_zero_ref(
        &mut self,
        reference: &GraphDescriptorPageRef,
    ) -> Result<(), GraphDescriptorTreeError> {
        let mut writer = self
            .level_zero
            .take()
            .expect("graph descriptor level-zero writer is present");
        let result = self.write_ref(&mut writer, reference);
        self.level_zero = Some(writer);
        result
    }

    fn write_ref(
        &mut self,
        writer: &mut RefRunWriter,
        reference: &GraphDescriptorPageRef,
    ) -> Result<(), GraphDescriptorTreeError> {
        let encoded = encode_page_ref(reference)?;
        let _ = u32::try_from(encoded.len())
            .map_err(|_| admission("graph descriptor page reference length exceeds u32"))?;
        let encoded_len = 4u64.saturating_add(encoded.len() as u64);
        let required = self
            .total_intermediate_bytes
            .checked_add(encoded_len)
            .ok_or_else(|| admission("graph descriptor intermediate byte count overflow"))?;
        if required > self.config.max_intermediate_bytes.get() {
            return Err(admission(format!(
                "graph descriptor build requires {required} intermediate bytes, exceeding limit {}",
                self.config.max_intermediate_bytes
            )));
        }
        writer.write_encoded_ref(&encoded)?;
        self.total_intermediate_bytes = required;
        Ok(())
    }

    fn record_finished_run(&mut self, run: &RefRun) -> Result<(), GraphDescriptorTreeError> {
        if run.bytes > self.config.max_intermediate_bytes.get() {
            return Err(admission(format!(
                "graph descriptor reference level contains {} bytes, exceeding limit {}",
                run.bytes, self.config.max_intermediate_bytes
            )));
        }
        self.peak_intermediate_level_bytes = self.peak_intermediate_level_bytes.max(run.bytes);
        Ok(())
    }

    fn observe_resident_leaf(&mut self) {
        let structural = self
            .leaf_entries
            .len()
            .saturating_mul(std::mem::size_of::<GraphDescriptorLeafEntry>());
        let bytes = GRAPH_DESCRIPTOR_PAGE_HEADER_BYTES
            .saturating_add(self.leaf_payload_bytes)
            .saturating_add(structural) as u64;
        self.peak_resident_bytes = self.peak_resident_bytes.max(
            bytes
                .saturating_add((2 * TREE_IO_BUFFER_BYTES) as u64)
                .saturating_add(self.last_key.as_ref().map_or(0, Vec::len) as u64),
        );
    }

    fn observe_resident_interior(
        &mut self,
        payload_bytes: usize,
        children: &[GraphDescriptorInteriorEntry],
    ) {
        let structural = children
            .len()
            .saturating_mul(std::mem::size_of::<GraphDescriptorInteriorEntry>());
        let bytes = GRAPH_DESCRIPTOR_PAGE_HEADER_BYTES
            .saturating_add(payload_bytes)
            .saturating_add(structural) as u64;
        self.peak_resident_bytes = self
            .peak_resident_bytes
            .max(bytes.saturating_add((3 * TREE_IO_BUFFER_BYTES) as u64));
    }

    fn remove_temporary(&mut self, path: &Path) -> Result<(), GraphDescriptorTreeError> {
        remove_if_exists(path)?;
        self.temporary_files.disarm(path);
        Ok(())
    }
}

fn validate_leaf_input(
    key: &[u8],
    value: &[u8],
    limits: GraphDescriptorPageLimits,
) -> Result<(), GraphDescriptorTreeError> {
    if key.is_empty() || value.is_empty() {
        return Err(admission(
            "graph descriptor leaf key and value must be non-empty",
        ));
    }
    if key.len() > limits.max_key_bytes.get() || value.len() > limits.max_value_bytes.get() {
        return Err(admission(format!(
            "graph descriptor leaf key/value bytes {}/{} exceed limits {}/{}",
            key.len(),
            value.len(),
            limits.max_key_bytes,
            limits.max_value_bytes
        )));
    }
    Ok(())
}

fn leaf_entry_payload_bytes(
    key_len: usize,
    value_len: usize,
) -> Result<usize, GraphDescriptorTreeError> {
    GRAPH_DESCRIPTOR_FIELD_HEADER_BYTES
        .checked_add(4)
        .and_then(|bytes| bytes.checked_add(key_len))
        .and_then(|bytes| bytes.checked_add(4))
        .and_then(|bytes| bytes.checked_add(value_len))
        .ok_or_else(|| admission("graph descriptor leaf entry length overflow"))
}

fn interior_entry_payload_bytes(
    reference: &GraphDescriptorPageRef,
) -> Result<usize, GraphDescriptorTreeError> {
    let encoded = encode_page_ref(reference)?;
    GRAPH_DESCRIPTOR_FIELD_HEADER_BYTES
        .checked_add(encoded.len())
        .ok_or_else(|| admission("graph descriptor interior entry length overflow"))
}

fn page_ref_resident_bytes(reference: &GraphDescriptorPageRef) -> usize {
    std::mem::size_of::<GraphDescriptorPageRef>()
        .saturating_add(reference.lower_bound.len())
        .saturating_add(reference.upper_bound.len())
}

fn page_body_resident_bytes(body: &ImmutableGraphDescriptorPageBody) -> usize {
    match body {
        ImmutableGraphDescriptorPageBody::Leaf(entries) => {
            entries
                .iter()
                .fold(std::mem::size_of_val(entries.as_slice()), |bytes, entry| {
                    bytes
                        .saturating_add(entry.key.len())
                        .saturating_add(entry.value.len())
                })
        }
        ImmutableGraphDescriptorPageBody::Interior(entries) => entries
            .iter()
            .fold(std::mem::size_of_val(entries.as_slice()), |bytes, entry| {
                bytes.saturating_add(page_ref_resident_bytes(&entry.child))
            }),
    }
}

struct RefRun {
    path: PathBuf,
    count: u64,
    bytes: u64,
}

struct RefRunWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    count: u64,
    bytes: u64,
}

impl RefRunWriter {
    fn create(path: PathBuf) -> Result<Self, GraphDescriptorTreeError> {
        let mut writer = BufWriter::with_capacity(TREE_IO_BUFFER_BYTES, File::create(&path)?);
        writer.write_all(REF_RUN_MAGIC)?;
        Ok(Self {
            path,
            writer,
            count: 0,
            bytes: REF_RUN_MAGIC.len() as u64,
        })
    }

    fn write_encoded_ref(&mut self, encoded: &[u8]) -> Result<(), GraphDescriptorTreeError> {
        let len = u32::try_from(encoded.len())
            .map_err(|_| admission("graph descriptor page reference length exceeds u32"))?;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(encoded)?;
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| admission("graph descriptor reference run count overflow"))?;
        self.bytes = self
            .bytes
            .checked_add(4 + encoded.len() as u64)
            .ok_or_else(|| admission("graph descriptor reference run length overflow"))?;
        Ok(())
    }

    fn finish(mut self) -> Result<RefRun, GraphDescriptorTreeError> {
        self.writer.flush()?;
        Ok(RefRun {
            path: self.path,
            count: self.count,
            bytes: self.bytes,
        })
    }
}

struct RefRunReader {
    reader: BufReader<File>,
    expected_count: u64,
    decoded_count: u64,
    config: GraphDescriptorTreeBuildConfig,
}

impl RefRunReader {
    fn open(
        path: &Path,
        expected_count: u64,
        config: GraphDescriptorTreeBuildConfig,
    ) -> Result<Self, GraphDescriptorTreeError> {
        let mut reader = BufReader::with_capacity(TREE_IO_BUFFER_BYTES, File::open(path)?);
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != REF_RUN_MAGIC {
            return Err(corrupt("invalid graph descriptor reference run header"));
        }
        Ok(Self {
            reader,
            expected_count,
            decoded_count: 0,
            config,
        })
    }

    fn next_ref(&mut self) -> Result<Option<GraphDescriptorPageRef>, GraphDescriptorTreeError> {
        let mut first = [0u8; 1];
        match self.reader.read(&mut first)? {
            0 => return Ok(None),
            1 => {}
            _ => unreachable!("one-byte graph descriptor reference read"),
        }
        let mut remaining_len = [0u8; 3];
        self.reader.read_exact(&mut remaining_len)?;
        let len = u32::from_le_bytes([
            first[0],
            remaining_len[0],
            remaining_len[1],
            remaining_len[2],
        ]) as usize;
        let max_ref_bytes = self
            .config
            .max_root_bytes
            .get()
            .min(self.config.page_limits.max_page_bytes.get());
        if len == 0 || len > max_ref_bytes {
            return Err(corrupt(format!(
                "graph descriptor reference run record length {len} exceeds limit {max_ref_bytes}"
            )));
        }
        let mut encoded = vec![0u8; len];
        self.reader.read_exact(&mut encoded)?;
        self.decoded_count = self
            .decoded_count
            .checked_add(1)
            .ok_or_else(|| corrupt("graph descriptor reference run count overflow"))?;
        if self.decoded_count > self.expected_count {
            return Err(corrupt(
                "graph descriptor reference run contains more records than declared",
            ));
        }
        decode_page_ref(&encoded, self.config.page_limits)
            .map(Some)
            .map_err(GraphDescriptorTreeError::Page)
    }

    fn finish(&mut self) -> Result<(), GraphDescriptorTreeError> {
        if self.decoded_count != self.expected_count {
            return Err(corrupt(format!(
                "graph descriptor reference run count mismatch: expected {}, got {}",
                self.expected_count, self.decoded_count
            )));
        }
        let mut trailing = [0u8; 1];
        if self.reader.read(&mut trailing)? != 0 {
            return Err(corrupt(
                "graph descriptor reference run contains trailing records",
            ));
        }
        Ok(())
    }
}

#[derive(Default)]
struct TemporaryFiles {
    paths: BTreeSet<PathBuf>,
}

impl TemporaryFiles {
    fn track(&mut self, path: PathBuf) {
        self.paths.insert(path);
    }

    fn disarm(&mut self, path: &Path) {
        self.paths.remove(path);
    }
}

impl Drop for TemporaryFiles {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = fs::remove_file(path);
        }
    }
}
