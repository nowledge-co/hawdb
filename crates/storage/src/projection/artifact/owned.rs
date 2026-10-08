//! Artifact data and admitted ownership move together. Shared roots keep the
//! inventory outside the Arc allocation so the root dies before its lease.

use super::super::predicate_checkpoint::decode::{self as predicates, Items, MapMemory};
use super::*;
use crate::background::{CheckpointAllocationOwner, CheckpointDecodeContext, CheckpointValues};
use crate::cow::CowSegment;

mod invalidations;
use invalidations::Invalidations;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

#[derive(Debug)]
#[doc(hidden)]
pub struct CheckpointProjectedGraphArtifacts {
    artifacts: BTreeMap<String, ProjectedGraphArtifact>,
    memory: CheckpointAllocationOwner,
}
impl std::ops::Deref for CheckpointProjectedGraphArtifacts {
    type Target = BTreeMap<String, ProjectedGraphArtifact>;
    fn deref(&self) -> &Self::Target {
        &self.artifacts
    }
}
impl PartialEq<BTreeMap<String, ProjectedGraphArtifact>> for CheckpointProjectedGraphArtifacts {
    fn eq(&self, other: &BTreeMap<String, ProjectedGraphArtifact>) -> bool {
        self.artifacts == *other
    }
}

#[derive(Debug)]
#[doc(hidden)]
pub struct CheckpointProjectedGraphArtifact {
    artifact: ProjectedGraphArtifact,
    _memory: CheckpointAllocationOwner,
}
impl std::ops::Deref for CheckpointProjectedGraphArtifact {
    type Target = ProjectedGraphArtifact;
    fn deref(&self) -> &Self::Target {
        &self.artifact
    }
}
impl CheckpointProjectedGraphArtifacts {
    /// Remove an artifact without detaching any of its allocation admission.
    pub fn remove(&mut self, name: &str) -> Option<CheckpointProjectedGraphArtifact> {
        self.artifacts
            .remove(name)
            .map(|artifact| CheckpointProjectedGraphArtifact {
                artifact,
                _memory: self.memory.clone(),
            })
    }
    pub(super) fn into_unadmitted(self) -> BTreeMap<String, ProjectedGraphArtifact> {
        self.artifacts
    }
    pub(crate) fn into_root(
        mut self,
        work: &CheckpointWorkContext,
    ) -> Result<CheckpointProjectedGraphRoot> {
        if self.artifacts.is_empty() {
            work.checkpoint().map_err(HawDBError::from_storage_error)?;
            // Destroy any empty map backing node and inventory before returning
            // an allocation-free root. Removed artifacts retain their own owner.
            drop(self);
            return Ok(CheckpointProjectedGraphRoot::default());
        }
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let bytes = std::mem::size_of::<BTreeMap<String, ProjectedGraphArtifact>>()
            + 2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>();
        let _token = self
            .memory
            .reserve(bytes, work)
            .map_err(HawDBError::from_storage_error)?;
        let data = ArtifactRootData::Shared(CowSegment::from(self.artifacts));
        unit.finish();
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        Ok(CheckpointProjectedGraphRoot {
            data,
            invalidations: None,
            memory: self.memory,
        })
    }
}

#[derive(Debug)]
pub(crate) struct CheckpointProjectedGraphRoot {
    // Release the complete map/root allocation before its last inventory.
    data: ArtifactRootData,
    invalidations: Option<std::sync::Arc<Invalidations>>,
    memory: CheckpointAllocationOwner,
}

#[derive(Debug, Clone, Default)]
enum ArtifactRootData {
    #[default]
    Empty,
    Shared(CowSegment<BTreeMap<String, ProjectedGraphArtifact>>),
}

impl std::ops::Deref for ArtifactRootData {
    type Target = BTreeMap<String, ProjectedGraphArtifact>;

    fn deref(&self) -> &Self::Target {
        static EMPTY: BTreeMap<String, ProjectedGraphArtifact> = BTreeMap::new();
        match self {
            Self::Empty => &EMPTY,
            Self::Shared(data) => data,
        }
    }
}

impl ArtifactRootData {
    #[cfg(all(test, not(target_arch = "wasm32")))]
    fn shares_storage_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Empty, Self::Empty) => true,
            (Self::Shared(left), Self::Shared(right)) => left.shares_storage_with(right),
            _ => false,
        }
    }
}
impl Clone for CheckpointProjectedGraphRoot {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            invalidations: self.invalidations.clone(),
            memory: self.memory.clone(),
        }
    }
}
impl Default for CheckpointProjectedGraphRoot {
    fn default() -> Self {
        BTreeMap::new().into()
    }
}
impl From<BTreeMap<String, ProjectedGraphArtifact>> for CheckpointProjectedGraphRoot {
    fn from(artifacts: BTreeMap<String, ProjectedGraphArtifact>) -> Self {
        let data = if artifacts.is_empty() {
            drop(artifacts);
            ArtifactRootData::Empty
        } else {
            ArtifactRootData::Shared(artifacts.into())
        };
        Self {
            data,
            invalidations: None,
            memory: CheckpointAllocationOwner::default(),
        }
    }
}
impl CheckpointProjectedGraphRoot {
    pub(crate) fn get(&self, name: &str) -> Option<&ProjectedGraphArtifact> {
        self.data
            .get(name)
            .filter(|artifact| !self.invalidated(artifact))
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &ProjectedGraphArtifact> {
        self.data
            .values()
            .filter(|artifact| !self.invalidated(artifact))
    }

    fn invalidated(&self, artifact: &ProjectedGraphArtifact) -> bool {
        self.invalidations.as_ref().is_some_and(|invalidations| {
            invalidations.contains(std::ptr::from_ref(artifact) as usize)
        })
    }

    /// Keep the immutable base and its admission while hiding only this name.
    /// Addresses identify entries in the retained map; they are never dereferenced.
    /// A failed admission/cancellation leaves this root and every snapshot intact.
    pub(crate) fn invalidate(&mut self, name: &str, work: &CheckpointWorkContext) -> Result<()> {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let artifact = self
            .data
            .get(name)
            .map(|artifact| std::ptr::from_ref(artifact) as usize);
        unit.finish();
        let Some(address) = artifact else {
            return work.checkpoint().map_err(HawDBError::from_storage_error);
        };
        let mut memory = self.memory.clone();
        let invalidations =
            Invalidations::insert(self.invalidations.as_ref(), address, &mut memory, work)?;
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        self.invalidations = Some(invalidations);
        self.memory = memory;
        Ok(())
    }

    pub(crate) fn empty(work: &CheckpointWorkContext) -> Result<Self> {
        CheckpointProjectedGraphArtifacts {
            artifacts: BTreeMap::new(),
            memory: CheckpointAllocationOwner::default(),
        }
        .into_root(work)
    }
}

pub(super) fn decode(
    body: &str,
    work: &CheckpointWorkContext,
) -> Result<(u64, CheckpointProjectedGraphArtifacts)> {
    let context = CheckpointDecodeContext {
        work: work.clone(),
        memory: std::cell::RefCell::new(Default::default()),
    };
    let (epoch, artifacts) = decode_inner(body, &context)?;
    context
        .checkpoint()
        .map_err(HawDBError::from_storage_error)?;
    Ok((
        epoch,
        CheckpointProjectedGraphArtifacts {
            artifacts,
            memory: context.memory.into_inner(),
        },
    ))
}

fn fields<'a>(
    line: &'a str,
    max: usize,
    work: &CheckpointDecodeContext,
) -> Result<CheckpointValues<&'a str>> {
    let mut fields = CheckpointValues::new(max, work).map_err(HawDBError::from_storage_error)?;
    let mut items = Items::new(line, b'\t');
    while let Some(field) = items.next(work)? {
        fields
            .push(field, work)
            .map_err(HawDBError::from_storage_error)?;
        if fields.as_slice().len() == max {
            break;
        }
    }
    Ok(fields)
}
fn header(
    line: Option<&str>,
    expected: &str,
    name: &str,
    work: &CheckpointDecodeContext,
) -> Result<u64> {
    let Some(line) = line else {
        return Err(HawDBError::Storage(format!(
            "missing projected graph artifact {expected}"
        )));
    };
    let fields = fields(line, 3, work)?;
    match fields.as_slice() {
        [field, raw] if *field == expected => predicates::parse_u64(raw, name, work),
        _ => Err(HawDBError::Storage(format!(
            "invalid projected graph artifact line: {line}"
        ))),
    }
}
fn names(input: &str, work: &CheckpointDecodeContext) -> Result<Vec<String>> {
    let mut names = Vec::new();
    if !input.is_empty() {
        let mut items = Items::new(input, b':');
        while let Some(item) = items.next(work)? {
            let name = predicates::decode_string(item, work)?;
            work.push(&mut names, name)?;
        }
    }
    Ok(names)
}
fn numbers<T>(
    input: &str,
    work: &CheckpointDecodeContext,
    parse: impl Fn(&str, &CheckpointDecodeContext) -> Result<T>,
) -> Result<Vec<T>> {
    let mut values = Vec::new();
    if !input.is_empty() {
        let mut items = Items::new(input, b',');
        while let Some(item) = items.next(work)? {
            work.push(&mut values, parse(item, work)?)?;
        }
    }
    Ok(values)
}
fn nodes(line: Option<&str>, work: &CheckpointDecodeContext) -> Result<Vec<NodeId>> {
    let Some(line) = line else {
        return Err(HawDBError::Storage(
            "missing projected graph artifact nodes line".into(),
        ));
    };
    let fields = fields(line, 3, work)?;
    match fields.as_slice() {
        ["nodes", raw] => numbers(raw, work, |item, work| {
            predicates::parse_u64(item, "projected graph artifact node id", work).map(NodeId)
        }),
        _ => Err(HawDBError::Storage(format!(
            "invalid projected graph artifact line: {line}"
        ))),
    }
}
fn indexes(
    line: Option<&str>,
    expected: &str,
    work: &CheckpointDecodeContext,
) -> Result<Vec<usize>> {
    let Some(line) = line else {
        return Err(HawDBError::Storage(format!(
            "missing projected graph artifact {expected} line"
        )));
    };
    let fields = fields(line, 3, work)?;
    match fields.as_slice() {
        [name, raw] if *name == expected => numbers(raw, work, |item, work| {
            predicates::parse_usize(item, "projected graph artifact index", work)
        }),
        _ => Err(HawDBError::Storage(format!(
            "invalid projected graph artifact line: {line}"
        ))),
    }
}

fn decode_inner(
    body: &str,
    work: &CheckpointDecodeContext,
) -> Result<(u64, BTreeMap<String, ProjectedGraphArtifact>)> {
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    let mut lines = ProjectedTextLines::new(body);
    match lines.next(work)? {
        Some("HAWDB_PROJECTED_GRAPHS_V1") => {}
        _ => {
            return Err(HawDBError::Storage(
                "invalid projected graph artifact header".to_string(),
            ));
        }
    }
    let artifact_version = header(
        lines.next(work)?,
        "artifact_version",
        "projected graph artifact version",
        work,
    )?;
    if !matches!(artifact_version, 1 | PROJECTED_GRAPH_ARTIFACT_VERSION) {
        return Err(HawDBError::Storage(format!(
            "unsupported projected graph artifact version: {artifact_version}"
        )));
    }
    let projection_epoch = header(
        lines.next(work)?,
        "projection_epoch",
        "projected graph artifact projection epoch",
        work,
    )?;
    let commit_epoch = header(
        lines.next(work)?,
        "commit_epoch",
        "projected graph artifact commit epoch",
        work,
    )?;

    let mut artifacts = BTreeMap::new();
    let mut memory = MapMemory::<ProjectedGraphArtifact>::default();
    while let Some(line) = lines.next(work)? {
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        let fields = fields(line, 7, work)?;
        let graph_fields = match (artifact_version, fields.as_slice()) {
            (
                1,
                ["graph", raw_name, raw_node_labels, raw_rel_types, raw_node_count, raw_edge_count],
            ) => Some((
                *raw_name,
                *raw_node_labels,
                *raw_rel_types,
                None,
                *raw_node_count,
                *raw_edge_count,
            )),
            (
                PROJECTED_GRAPH_ARTIFACT_VERSION,
                ["graph", raw_name, raw_node_labels, raw_rel_types, raw_relationship_predicates, raw_node_count, raw_edge_count],
            ) => Some((
                *raw_name,
                *raw_node_labels,
                *raw_rel_types,
                Some(*raw_relationship_predicates),
                *raw_node_count,
                *raw_edge_count,
            )),
            _ => None,
        };
        match graph_fields {
            Some((
                raw_name,
                raw_node_labels,
                raw_rel_types,
                raw_relationship_predicates,
                raw_node_count,
                raw_edge_count,
            )) => {
                let name = predicates::decode_string(raw_name, work)?;
                let definition = ProjectedGraphDefinition {
                    node_labels: names(raw_node_labels, work)?,
                    rel_types: names(raw_rel_types, work)?,
                    relationship_predicates: raw_relationship_predicates
                        .map(|encoded| predicates::decode(encoded, work))
                        .transpose()?
                        .unwrap_or_default(),
                };
                let node_count = predicates::parse_u64(
                    raw_node_count,
                    "projected graph artifact node count",
                    work,
                )?;
                let edge_count = predicates::parse_u64(
                    raw_edge_count,
                    "projected graph artifact edge count",
                    work,
                )?;
                let nodes = nodes(lines.next(work)?, work)?;
                let csr_offsets = indexes(lines.next(work)?, "csr_offsets", work)?;
                let csr_targets = indexes(lines.next(work)?, "csr_targets", work)?;
                let csc_offsets = indexes(lines.next(work)?, "csc_offsets", work)?;
                let csc_sources = indexes(lines.next(work)?, "csc_sources", work)?;
                if nodes.len() as u64 != node_count {
                    return Err(HawDBError::Storage(format!(
                        "projected graph artifact node count mismatch for {name}"
                    )));
                }
                if csr_targets.len() as u64 != edge_count || csc_sources.len() as u64 != edge_count
                {
                    return Err(HawDBError::Storage(format!(
                        "projected graph artifact edge count mismatch for {name}"
                    )));
                }
                let data = ProjectedGraphArtifactData::new_with_work_context(
                    nodes,
                    csr_offsets,
                    csr_targets,
                    csc_offsets,
                    csc_sources,
                    work,
                )?;
                let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
                memory.before_insert(artifacts.len(), work)?;
                artifacts.insert(
                    name,
                    ProjectedGraphArtifact {
                        projection_epoch,
                        commit_epoch,
                        definition,
                        data,
                    },
                );
                unit.finish();
            }
            None if fields.as_slice() == [""] => {}
            _ => {
                return Err(HawDBError::Storage(format!(
                    "invalid projected graph artifact line: {line}"
                )));
            }
        }
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok((commit_epoch, artifacts))
}
