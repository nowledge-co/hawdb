use super::{query_io, search_document_bytes, SearchOutOfCoreMetrics, SearchOutOfCoreReader};
use crate::build_memory::{decoded_document_bytes_with_context, document_bytes};
use crate::{
    decode_search_document_line, validate_search_segment_documents, Result, RuntimeTaskContext,
    SearchDocument, SearchSegmentDescriptorEntry, SkeinError,
};
use skein_executor::{QueryMemoryAccount, QueryMemoryLease};
use std::mem::size_of;
use std::ops::Deref;

fn slots<T>(count: usize) -> Result<usize> {
    let bytes = query_io::mul(count, size_of::<T>())?;
    if bytes > isize::MAX as usize {
        return Err(SkeinError::Execution(
            "search hydration capacity exceeds address space".to_owned(),
        ));
    }
    Ok(bytes)
}

pub(super) struct Segment {
    documents: Vec<SearchDocument>,
    memory: QueryMemoryLease,
}

impl Segment {
    pub fn decode(
        text: &str,
        descriptor: &SearchSegmentDescriptorEntry,
        memory: &QueryMemoryAccount,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        query_io::checkpoint(task)?;
        let mut lines = text.lines();
        if lines.next() != Some("SKEIN_SEARCH_SEGMENT_V1") {
            return Err(invalid("header"));
        }
        slots::<SearchDocument>(descriptor.document_count)?;
        let mut count = 0;
        let mut required = 0;
        for line in lines {
            query_io::checkpoint(task)?;
            if line.is_empty() {
                continue;
            }
            if count == descriptor.document_count {
                return Err(invalid("document count"));
            }
            required = query_io::add(
                required,
                decoded_document_bytes_with_context(line, usize::MAX, Some(task))?,
            )?;
            count += 1;
        }
        if count != descriptor.document_count {
            return Err(invalid("document count"));
        }
        let mut lease = memory.reserve(required)?;
        #[cfg(test)]
        tests::record_decode();
        let mut documents = Vec::with_capacity(count);
        for line in text.lines().skip(1) {
            query_io::checkpoint(task)?;
            if !line.is_empty() {
                documents.push(decode_search_document_line(line)?);
            }
        }
        query_io::checkpoint(task)?;
        validate_search_segment_documents(descriptor, &documents)?;
        let mut retained = slots::<SearchDocument>(documents.capacity())?;
        for document in &documents {
            query_io::checkpoint(task)?;
            retained = query_io::add(retained, payload_bytes(document)?)?;
        }
        if retained > required {
            return Err(invalid("decoded capacity"));
        }
        lease.shrink(required - retained);
        Ok(Self {
            documents,
            memory: lease,
        })
    }

    pub fn logical_bytes(&self) -> Result<u64> {
        self.documents.iter().try_fold(0u64, |sum, document| {
            sum.checked_add(search_document_bytes(document))
                .ok_or_else(|| invalid("logical byte count"))
        })
    }

    /// The build consumer admits retained input on its existing build root.
    /// Source storage stays charged while the callback owns the current row.
    pub fn visit(
        mut self,
        task: &RuntimeTaskContext,
        consumer: &mut dyn FnMut(SearchDocument) -> Result<()>,
    ) -> Result<()> {
        for document in self.documents.drain(..) {
            query_io::checkpoint(task)?;
            let bytes = payload_bytes(&document)?;
            consumer(document)?;
            self.memory.shrink(bytes);
        }
        query_io::checkpoint(task)
    }
}

fn payload_bytes(document: &SearchDocument) -> Result<usize> {
    document_bytes(document)?
        .checked_sub(size_of::<SearchDocument>())
        .ok_or_else(|| invalid("document capacity"))
}

struct Request<'a> {
    id: &'a str,
    position: usize,
    segment: &'a SearchSegmentDescriptorEntry,
}

pub(super) struct Documents {
    documents: Vec<SearchDocument>,
    // Moving selected payloads keeps the originating segment's lease; no
    // per-document accounts, payload clones or uncharged transfer gap are needed.
    payloads: Vec<QueryMemoryLease>,
    _slots: QueryMemoryLease,
}

impl Deref for Documents {
    type Target = [SearchDocument];
    fn deref(&self) -> &Self::Target {
        &self.documents
    }
}

impl Documents {
    // The existing public API returns plain documents. Its retained ownership
    // contract is pending; release here must never be reported as output coverage.
    pub fn into_unowned_output(self) -> Vec<SearchDocument> {
        self.documents
    }
}

pub(super) fn load<'a>(
    reader: &'a SearchOutOfCoreReader,
    ids: impl ExactSizeIterator<Item = &'a str>,
    memory: &QueryMemoryAccount,
    task: &RuntimeTaskContext,
    metrics: &mut SearchOutOfCoreMetrics,
) -> Result<Documents> {
    query_io::checkpoint(task)?;
    let count = ids.len();
    if count > reader.config.max_hydrated_documents.get() {
        return Err(SkeinError::Storage(format!(
            "search hydration requires {count} documents, exceeding {}",
            reader.config.max_hydrated_documents
        )));
    }
    let _requests_memory = memory.reserve(slots::<Request<'_>>(count)?)?;
    #[cfg(test)]
    tests::record_requests();
    let mut requests = Vec::with_capacity(count);
    for (position, id) in ids.enumerate() {
        query_io::checkpoint(task)?;
        let segment = reader
            .segment_for_document(id)
            .ok_or_else(|| invalid("requested document range"))?;
        requests.push(Request {
            id,
            position,
            segment,
        });
    }
    requests.sort_unstable_by(|a, b| a.id.cmp(b.id));
    if requests.windows(2).any(|pair| pair[0].id == pair[1].id) {
        return Err(SkeinError::Storage(
            "search hydration document ids must be unique".to_owned(),
        ));
    }
    let groups = requests
        .chunk_by(|a, b| a.segment.segment_id == b.segment.segment_id)
        .count();
    let storage = memory.reserve(query_io::add(
        slots::<SearchDocument>(count)?,
        slots::<QueryMemoryLease>(groups)?,
    )?)?;
    #[cfg(test)]
    tests::record_selection();
    let mut output = Documents {
        documents: Vec::with_capacity(count),
        payloads: Vec::with_capacity(groups),
        _slots: storage,
    };
    let mut logical_bytes = 0u64;
    for requested in requests.chunk_by(|a, b| a.segment.segment_id == b.segment.segment_id) {
        query_io::checkpoint(task)?;
        let id = requested[0].segment.segment_id;
        let descriptor = usize::try_from(id)
            .ok()
            .and_then(|index| reader.descriptor.segments.get(index))
            .filter(|segment| segment.segment_id == id)
            .ok_or_else(|| invalid("segment identity"))?;
        let Segment {
            documents: decoded,
            memory: lease,
        } = reader.read_hydration_segment(descriptor, metrics, memory, task)?;
        // Attach the lease before moving any payload into the destination so
        // early returns drop selected documents before releasing their charge.
        output.payloads.push(lease);
        let mut retained = 0;
        for document in decoded {
            query_io::checkpoint(task)?;
            if requested
                .binary_search_by(|request| request.id.cmp(document.id.as_str()))
                .is_err()
            {
                continue;
            }
            logical_bytes = logical_bytes
                .checked_add(search_document_bytes(&document))
                .ok_or_else(|| invalid("logical byte count"))?;
            if logical_bytes > reader.config.max_hydrated_bytes.get() {
                return Err(SkeinError::Storage(format!(
                    "search hydration requires {logical_bytes} bytes, exceeding {}",
                    reader.config.max_hydrated_bytes
                )));
            }
            retained = query_io::add(retained, payload_bytes(&document)?)?;
            #[cfg(test)]
            tests::record_selected(&document);
            output.documents.push(document);
        }
        // The source Vec and all unselected documents have now dropped. The
        // remaining lease follows the moved payloads, while destination slots
        // remain covered by their own operation-scoped lease.
        let lease = output
            .payloads
            .last_mut()
            .expect("current segment has an owner");
        let released = lease
            .bytes()
            .checked_sub(retained)
            .ok_or_else(|| invalid("retained capacity"))?;
        lease.shrink(released);
    }
    if output.documents.len() != count {
        return Err(invalid("requested document count"));
    }
    output.documents.sort_unstable_by_key(|document| {
        let index = requests
            .binary_search_by(|request| request.id.cmp(document.id.as_str()))
            .expect("selected ID belongs to requests");
        requests[index].position
    });
    query_io::checkpoint(task)?;
    metrics.hydrated_bytes = logical_bytes;
    Ok(output)
}

fn invalid(field: &str) -> SkeinError {
    SkeinError::Storage(format!("search hydration has invalid {field}"))
}

#[cfg(test)]
pub(super) mod tests;
