// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Late hydration owns the query-funded routes, decoded rows and map entries.

use super::*;
use crate::build_memory::{
    checked_add as add, checked_mul as mul, reserve_capacity, MAP_ENTRY_BYTES, SET_ENTRY_BYTES,
};
use crate::lexical_projection::QueryContext;
use hawdb_executor::QueryMemoryLease;

#[cfg(test)]
mod tests;

pub(in crate::out_of_core) struct Loaded {
    storage: Storage,
}
enum Storage {
    Legacy(BTreeMap<String, SearchDocument>),
    Query(Documents),
}
struct Documents {
    values: BTreeMap<String, Document>,
    _memory: QueryMemoryLease,
}

pub(in crate::out_of_core) struct Document {
    document: SearchDocument,
    _memory: Option<QueryMemoryLease>,
}
impl std::ops::Deref for Document {
    type Target = SearchDocument;
    fn deref(&self) -> &Self::Target {
        &self.document
    }
}

impl Loaded {
    pub(in crate::out_of_core) fn new(
        reader: &SearchOutOfCoreReader,
        candidates: &[SearchScoredCandidate],
        metrics: &mut SearchOutOfCoreMetrics,
        query: Option<QueryContext<'_>>,
    ) -> Result<Self> {
        let storage = match query {
            Some(query) => Storage::Query(load(reader, candidates, metrics, query)?),
            None => {
                let ids = candidates
                    .iter()
                    .map(|candidate| candidate.id.clone())
                    .collect();
                Storage::Legacy(reader.load_documents(&ids, metrics)?)
            }
        };
        Ok(Self { storage })
    }
    pub(in crate::out_of_core) fn remove(&mut self, id: &str) -> Option<Document> {
        match &mut self.storage {
            Storage::Legacy(values) => values.remove(id).map(|document| Document {
                document,
                _memory: None,
            }),
            Storage::Query(documents) => documents.values.remove(id),
        }
    }
}

struct Routes {
    values: BTreeMap<(usize, u64), BTreeSet<String>>,
    _memory: QueryMemoryLease,
}
fn load(
    reader: &SearchOutOfCoreReader,
    candidates: &[SearchScoredCandidate],
    metrics: &mut SearchOutOfCoreMetrics,
    query: QueryContext<'_>,
) -> Result<Documents> {
    query.checkpoint()?;
    let mut routes = Routes {
        values: BTreeMap::new(),
        _memory: query.memory.reserve(0)?,
    };
    for candidate in candidates {
        query.checkpoint()?;
        let id = &candidate.id;
        let route = reader
            .segment_for_document_with_context(id, Some(query))?
            .ok_or_else(|| invalid("candidate has no visible document route"))?;
        if route.lexical_document_bytes_read > 0 {
            metrics.lexical_document_block_reads =
                metrics.lexical_document_block_reads.saturating_add(1);
            metrics.lexical_document_bytes_read = metrics
                .lexical_document_bytes_read
                .saturating_add(route.lexical_document_bytes_read);
        }
        let key = (route.artifact_index, route.segment.segment_id);
        if !routes.values.contains_key(&key) {
            routes._memory.grow(MAP_ENTRY_BYTES)?;
            routes.values.insert(key, BTreeSet::new());
        }
        let ids = routes.values.get_mut(&key).expect("route was inserted");
        if !ids.contains(id) {
            routes._memory.grow(add(SET_ENTRY_BYTES, id.len())?)?;
            ids.insert(id.clone());
        }
    }
    let mut output = Documents {
        values: BTreeMap::new(),
        _memory: query.memory.reserve(0)?,
    };
    let mut used = 0u64;
    for ((layer, segment_id), ids) in &routes.values {
        query.checkpoint()?;
        let artifact = reader
            .segments
            .get(*layer)
            .ok_or_else(|| invalid("unknown hydration layer"))?;
        let segment = usize::try_from(*segment_id)
            .ok()
            .and_then(|index| artifact.descriptor.segments.get(index))
            .filter(|segment| segment.segment_id == *segment_id)
            .ok_or_else(|| invalid("unknown hydration segment"))?;
        let selected = read_selected(reader, artifact, segment, ids, used, metrics, query)?;
        used = used
            .checked_add(selected.bytes)
            .ok_or_else(|| invalid("byte count overflow"))?;
        for document in selected.documents {
            // The row moves with its own lease. Only the map node and copied
            // lookup key require new funding; the document is never detached.
            output
                ._memory
                .grow(add(MAP_ENTRY_BYTES, document.id.len())?)?;
            if output
                .values
                .insert(document.id.clone(), document)
                .is_some()
            {
                return Err(invalid("duplicate selected document"));
            }
        }
    }
    metrics.hydrated_bytes = used;
    query.checkpoint()?;
    Ok(output)
}

struct Selection {
    documents: Vec<Document>,
    bytes: u64,
    peak_document_bytes: u64,
    _memory: QueryMemoryLease,
}
fn read_selected(
    reader: &SearchOutOfCoreReader,
    artifact: &SearchOutOfCoreSegmentReader,
    segment: &SearchSegmentDescriptorEntry,
    ids: &BTreeSet<String>,
    used: u64,
    metrics: &mut SearchOutOfCoreMetrics,
    query: QueryContext<'_>,
) -> Result<Selection> {
    let range = segment
        .payload_range
        .ok_or_else(|| invalid("segment has no payload range"))?;
    if range
        .offset
        .checked_add(range.length)
        .is_none_or(|end| end > artifact.payload_length)
    {
        return Err(invalid("segment range exceeds its artifact"));
    }
    let selected = read_validated_with_account(
        crate::build_memory::read::QueryFileRange {
            file: &artifact.payload,
            expected_length: Some(artifact.payload_length),
            offset: range.offset,
            remaining: range.length,
            context: query,
            require_execution: true,
        },
        range.length,
        range.checksum,
        reader.config.max_uncompressed_segment_bytes.get(),
        Some(DecodeAdmission {
            memory: query.memory,
            task: query.task,
            max_header_bytes: reader.config.max_document_header_bytes.get(),
        }),
        |text| {
            select_records(
                text,
                segment,
                ids,
                HydrationBudget {
                    limit: reader.config.max_hydrated_bytes.get(),
                    used,
                },
                reader.config.max_uncompressed_segment_bytes.get(),
                query,
            )
        },
    )?;
    metrics.segment_range_reads = metrics.segment_range_reads.saturating_add(1);
    metrics.segment_bytes_read = metrics.segment_bytes_read.saturating_add(range.length);
    metrics.hydration_segment_bytes_read = metrics
        .hydration_segment_bytes_read
        .saturating_add(range.length);
    metrics.peak_segment_document_bytes = metrics
        .peak_segment_document_bytes
        .max(selected.peak_document_bytes);
    Ok(selected)
}

fn select_records(
    text: &mut dyn BufRead,
    segment: &SearchSegmentDescriptorEntry,
    ids: &BTreeSet<String>,
    budget: HydrationBudget,
    max_record_bytes: u64,
    query: QueryContext<'_>,
) -> Result<Selection> {
    let remaining = budget
        .limit
        .checked_sub(budget.used)
        .ok_or_else(|| invalid("hydration byte budget exhausted"))?;
    let mut selected = Selection {
        documents: Vec::new(),
        bytes: 0,
        peak_document_bytes: 0,
        _memory: query.memory.reserve(0)?,
    };
    let mut line_memory = query.memory.reserve(0)?;
    let mut previous_memory = query.memory.reserve(0)?;
    let mut line = Vec::new();
    let mut previous = Vec::new();
    let mut count = 0usize;
    while read_record(
        text,
        &mut line,
        usize::try_from(max_record_bytes).unwrap_or(usize::MAX),
        query.task,
        &mut line_memory,
    )? {
        let record = std::str::from_utf8(&line).map_err(|_| invalid("invalid record UTF-8"))?;
        let record = match record.strip_suffix('\n') {
            Some(record) => record.strip_suffix('\r').unwrap_or(record),
            None => record,
        };
        if record.is_empty() || record == "HAWDB_SEARCH_SEGMENT_V1" {
            continue;
        }
        let document = decode_record(record, query)?;
        if count == 0 && document.id != segment.first_document_id {
            return Err(invalid("document bounds do not match its descriptor"));
        }
        if count != 0 && previous.as_slice() >= document.id.as_bytes() {
            return Err(invalid("documents are not strictly ordered"));
        }
        count = add(count, 1)?;
        if count > segment.document_count {
            return Err(invalid("document count exceeds its descriptor"));
        }
        previous.clear();
        reserve_capacity(&mut previous, document.id.len(), &mut previous_memory)?;
        previous.extend_from_slice(document.id.as_bytes());
        let bytes = search_document_bytes(&document);
        selected.peak_document_bytes = selected
            .peak_document_bytes
            .max(selected.bytes.saturating_add(bytes));
        if ids.contains(&document.id) {
            let previous_bytes = selected.bytes;
            selected.bytes = selected
                .bytes
                .checked_add(bytes)
                .ok_or_else(|| invalid("byte count overflow"))?;
            if selected.bytes > remaining {
                return Err(ResourceBudgetError {
                    resource: ResourceBudgetKind::MemoryBytes,
                    scope: ResourceBudgetScope::Operator,
                    owner: "search hydration".into(),
                    memory_class: None,
                    requested: bytes,
                    used: budget.used.saturating_add(previous_bytes),
                    budget: budget.limit,
                    budget_option: "SearchOutOfCoreConfig.max_hydrated_bytes",
                    message: "selected documents exceed hydration byte budget".into(),
                }
                .into());
            }
            let required = add(selected.documents.len(), 1)?;
            let capacity = required.max(selected.documents.capacity().saturating_mul(2));
            reserve_capacity(&mut selected.documents, capacity, &mut selected._memory)?;
            selected.documents.push(document);
        }
    }
    if count != segment.document_count || previous.as_slice() != segment.last_document_id.as_bytes()
    {
        return Err(invalid(
            "document count or bounds do not match its descriptor",
        ));
    }
    query.checkpoint()?;
    Ok(selected)
}

fn hex_size(raw: &str) -> Result<usize> {
    if !raw.len().is_multiple_of(2) {
        return Err(invalid("invalid hex string length"));
    }
    Ok(raw.len() / 2)
}
fn decode_record(record: &str, query: QueryContext<'_>) -> Result<Document> {
    query.checkpoint()?;
    let mut fields = record.split('\t');
    let (Some("doc"), Some(id), Some(title), Some(content), Some(embedding), Some(metadata), None) = (
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
    ) else {
        return Err(invalid("invalid document record"));
    };
    let dimension = if embedding.is_empty() {
        0
    } else {
        embedding.split(',').count()
    };
    let mut bytes = add(
        size_of::<Document>(),
        add(
            add(hex_size(id)?, hex_size(title)?)?,
            add(hex_size(content)?, mul(dimension, size_of::<f32>())?)?,
        )?,
    )?;
    if !metadata.is_empty() {
        for (index, pair) in metadata.split(';').enumerate() {
            if index.is_multiple_of(1024) {
                query.checkpoint()?;
            }
            let (key, value) = pair
                .split_once('=')
                .ok_or_else(|| invalid("invalid metadata pair"))?;
            bytes = add(
                bytes,
                add(MAP_ENTRY_BYTES, add(hex_size(key)?, hex_size(value)?)?)?,
            )?;
        }
    }
    let memory = query.memory.reserve(bytes)?;
    let mut vector = Vec::with_capacity(dimension);
    if vector.capacity() != dimension {
        return Err(invalid("vector decode exceeded admission"));
    }
    if dimension != 0 {
        for (index, raw) in embedding.split(',').enumerate() {
            if index.is_multiple_of(1024) {
                query.checkpoint()?;
            }
            vector.push(
                raw.parse::<f32>()
                    .map_err(|_| invalid("invalid embedding value"))?,
            );
        }
    }
    let mut values = BTreeMap::new();
    if !metadata.is_empty() {
        for pair in metadata.split(';') {
            let (key, value) = pair.split_once('=').expect("metadata pairs were validated");
            values.insert(decode_hex(key, query)?, decode_hex(value, query)?);
        }
    }
    let document = SearchDocument {
        id: decode_hex(id, query)?,
        title: decode_hex(title, query)?,
        content: decode_hex(content, query)?,
        embedding: (dimension != 0).then_some(vector),
        metadata: values,
    };
    query.checkpoint()?;
    Ok(Document {
        document,
        _memory: Some(memory),
    })
}

fn decode_hex(raw: &str, query: QueryContext<'_>) -> Result<String> {
    let mut bytes = Vec::with_capacity(hex_size(raw)?);
    if bytes.capacity() != raw.len() / 2 {
        return Err(invalid("string decode exceeded admission"));
    }
    for (index, pair) in raw.as_bytes().chunks_exact(2).enumerate() {
        if index.is_multiple_of(1024) {
            query.checkpoint()?;
        }
        let nibble = |byte| match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            b'A'..=b'F' => Ok(byte - b'A' + 10),
            _ => Err(invalid("invalid hex string")),
        };
        bytes.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
    }
    String::from_utf8(bytes).map_err(|_| invalid("invalid decoded UTF-8"))
}
