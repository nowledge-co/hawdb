// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Hit hydration from the metadata and binary vector sidecars only.
//!
//! When a hit needs no matched spans, its stored body is never decoded: the
//! metadata sidecar supplies `kind`/`external_id`/`source_id` and the hit's own
//! vector row supplies the embedding for its vector score. Title and content
//! are left empty.

use super::*;
use crate::build_memory::{checked_add as add, MAP_ENTRY_BYTES, SET_ENTRY_BYTES};
use crate::lexical_projection::QueryContext;
use hawdb_executor::QueryMemoryLease;

pub(in crate::out_of_core) struct Hits {
    pub(in crate::out_of_core) values: BTreeMap<String, SearchDocument>,
    _memory: Option<QueryMemoryLease>,
}

pub(in crate::out_of_core) fn load(
    reader: &SearchOutOfCoreReader,
    candidates: &[SearchScoredCandidate],
    metrics: &mut SearchOutOfCoreMetrics,
    query: Option<QueryContext<'_>>,
) -> Result<Hits> {
    let checkpoint = || query.map_or(Ok(()), |query| query.checkpoint());
    let mut memory = query.map(|query| query.memory.reserve(0)).transpose()?;
    let grow = |memory: &mut Option<QueryMemoryLease>, bytes: usize| -> Result<()> {
        if let Some(memory) = memory {
            memory.grow(bytes)?;
        }
        Ok(())
    };
    let mut routes = BTreeMap::<(usize, u64), BTreeSet<&str>>::new();
    for candidate in candidates {
        checkpoint()?;
        let route = reader
            .segment_for_document_with_context(&candidate.id, query)?
            .ok_or_else(|| invalid("candidate has no visible document route"))?;
        if route.lexical_document_bytes_read > 0 {
            metrics.lexical_document_block_reads =
                metrics.lexical_document_block_reads.saturating_add(1);
            metrics.lexical_document_bytes_read = metrics
                .lexical_document_bytes_read
                .saturating_add(route.lexical_document_bytes_read);
        }
        grow(&mut memory, add(MAP_ENTRY_BYTES, SET_ENTRY_BYTES)?)?;
        routes
            .entry((route.artifact_index, route.segment.segment_id))
            .or_default()
            .insert(candidate.id.as_str());
    }
    let mut values = BTreeMap::<String, SearchDocument>::new();
    let mut used = 0u64;
    for ((layer, segment_id), ids) in &routes {
        checkpoint()?;
        let artifact = reader
            .segments
            .get(*layer)
            .ok_or_else(|| invalid("unknown hydration layer"))?;
        let segment = usize::try_from(*segment_id)
            .ok()
            .and_then(|index| artifact.descriptor.segments.get(index))
            .filter(|segment| segment.segment_id == *segment_id)
            .ok_or_else(|| invalid("unknown hydration segment"))?;
        let mut ordinals = BTreeSet::new();
        let mut found = 0usize;
        let mut admit = |document: &SearchMetadataDocument| -> Result<()> {
            if !ids.contains(document.id.as_str()) {
                return Ok(());
            }
            let hydrated = SearchDocument {
                id: document.id.clone(),
                title: String::new(),
                content: String::new(),
                embedding: None,
                metadata: document.metadata.clone(),
            };
            let bytes = search_document_bytes(&hydrated);
            charge(reader, &mut used, bytes)?;
            grow(
                &mut memory,
                usize::try_from(bytes.saturating_mul(2)).unwrap_or(usize::MAX),
            )?;
            if let Some(ordinal) = document.vector_ordinal {
                ordinals.insert(ordinal);
            }
            found += 1;
            values.insert(hydrated.id.clone(), hydrated);
            Ok(())
        };
        match query {
            Some(query) => {
                reader.visit_query_metadata_segment(artifact, segment, metrics, query, admit)?
            }
            None => {
                for document in reader.read_metadata_segment(artifact, segment, metrics)? {
                    admit(&document)?;
                }
            }
        }
        if found != ids.len() {
            return Err(invalid("candidate is missing from its metadata sidecar"));
        }
        if ordinals.is_empty() {
            continue;
        }
        let mut embedded = 0usize;
        reader.visit_vector_segment(
            artifact,
            segment,
            metrics,
            query,
            |ordinal| ordinals.contains(&ordinal),
            |document| {
                let bytes = (document.embedding.len() * std::mem::size_of::<f32>()) as u64;
                charge(reader, &mut used, bytes)?;
                grow(&mut memory, usize::try_from(bytes).unwrap_or(usize::MAX))?;
                let target = values
                    .get_mut(&document.id)
                    .ok_or_else(|| invalid("vector row does not match its metadata"))?;
                target.embedding = Some(document.embedding.clone());
                embedded += 1;
                Ok(())
            },
        )?;
        if embedded != ordinals.len() {
            return Err(invalid("vector rows do not match their metadata"));
        }
    }
    metrics.hydrated_bytes = used;
    checkpoint()?;
    Ok(Hits {
        values,
        _memory: memory,
    })
}

fn charge(reader: &SearchOutOfCoreReader, used: &mut u64, bytes: u64) -> Result<()> {
    let limit = reader.config.max_hydrated_bytes.get();
    let next = used
        .checked_add(bytes)
        .ok_or_else(|| invalid("byte count overflow"))?;
    if next > limit {
        return Err(ResourceBudgetError {
            resource: ResourceBudgetKind::MemoryBytes,
            scope: ResourceBudgetScope::Operator,
            owner: "search hydration".into(),
            memory_class: None,
            requested: bytes,
            used: *used,
            budget: limit,
            budget_option: "SearchOutOfCoreConfig.max_hydrated_bytes",
            message: "selected hit sidecars exceed hydration byte budget".into(),
        }
        .into());
    }
    *used = next;
    Ok(())
}
