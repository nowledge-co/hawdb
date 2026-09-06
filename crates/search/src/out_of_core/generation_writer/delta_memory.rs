use crate::build_control::checkpoint;
use crate::build_memory::{
    checked_add, checked_mul, document_bytes, projection_row_bytes, BuildMemory, MAP_ENTRY_BYTES,
};
use crate::{
    Result, RuntimeTaskContext, SearchDocument, SearchProjectionDelta, SearchProjectionRow,
    SkeinError,
};
use skein_executor::QueryMemoryLease;
use std::mem::size_of;

struct Rows {
    delta: SearchProjectionDelta,
    memory: QueryMemoryLease,
}

pub(super) struct Input {
    upserts: Vec<SearchDocument>,
    deletes: Vec<String>,
    // Payload and vector fields must drop before their operation-owned charge.
    memory: QueryMemoryLease,
}

impl Input {
    pub fn new(
        delta: SearchProjectionDelta,
        memory: &BuildMemory,
        limit: u64,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let required = conversion_bytes(&delta, task)?;
        if required as u128 > u128::from(limit) {
            return Err(SkeinError::Storage(format!(
                "incremental projection update requires {required} bytes, exceeding the generation admission {limit}"
            )));
        }
        let mut source = Rows {
            memory: memory.input.reserve(required)?,
            delta,
        };
        #[cfg(test)]
        tests::record_conversion();
        let mut upserts = Vec::with_capacity(source.delta.upserts.len());
        for row in source.delta.upserts.drain(..) {
            checkpoint(task)?;
            upserts.push(row.into_document());
            #[cfg(test)]
            tests::record_row();
        }
        checkpoint(task)?;
        upserts.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        source.delta.deletes.sort_unstable();
        super::delta::validate_delta_ids(&upserts, &source.delta.deletes)?;
        // Reverse once so pop consumes ascending IDs without another container.
        upserts.reverse();
        source.delta.deletes.reverse();
        let mut input = Self {
            upserts,
            deletes: std::mem::take(&mut source.delta.deletes),
            memory: source.memory,
        };
        drop(source.delta);
        let retained = input.retained_bytes(task)?;
        if retained > required {
            return Err(SkeinError::Execution(
                "search delta conversion exceeded its admitted capacity".to_owned(),
            ));
        }
        input.memory.shrink(required - retained);
        checkpoint(task)?;
        Ok(input)
    }

    pub fn upsert_id(&self) -> Option<&str> {
        self.upserts.last().map(|document| document.id.as_str())
    }

    pub fn delete_id(&self) -> Option<&str> {
        self.deletes.last().map(String::as_str)
    }

    pub fn consume_upsert(
        &mut self,
        consumer: impl FnOnce(SearchDocument) -> Result<()>,
    ) -> Result<()> {
        let document = self.upserts.pop().expect("upsert was present");
        let bytes = document_bytes(&document)? - size_of::<SearchDocument>();
        // The writer admits input on this same root while the delta still owns
        // its source charge. Keep vector slots until their allocation is dropped.
        consumer(document)?;
        self.memory.shrink(bytes);
        Ok(())
    }

    pub fn discard_delete(&mut self) {
        let id = self.deletes.pop().expect("delete was present");
        let bytes = id.capacity();
        drop(id);
        self.memory.shrink(bytes);
    }

    fn retained_bytes(&self, task: &RuntimeTaskContext) -> Result<usize> {
        let mut bytes = checked_add(
            slots::<SearchDocument>(self.upserts.capacity())?,
            slots::<String>(self.deletes.capacity())?,
        )?;
        for document in &self.upserts {
            checkpoint(task)?;
            bytes = checked_add(
                bytes,
                document_bytes(document)? - size_of::<SearchDocument>(),
            )?;
        }
        for id in &self.deletes {
            checkpoint(task)?;
            bytes = checked_add(bytes, id.capacity())?;
        }
        Ok(bytes)
    }
}

fn slots<T>(count: usize) -> Result<usize> {
    let bytes = checked_mul(count, size_of::<T>())?;
    if bytes > isize::MAX as usize {
        return Err(SkeinError::Execution(
            "search delta capacity exceeds address space".to_owned(),
        ));
    }
    Ok(bytes)
}

fn conversion_bytes(delta: &SearchProjectionDelta, task: &RuntimeTaskContext) -> Result<usize> {
    // Cover the original row allocation even when it has unused capacity. The
    // exact destination Vec overlaps that allocation until conversion finishes.
    let mut bytes = checked_add(
        slots::<SearchProjectionRow>(delta.upserts.capacity())?,
        slots::<String>(delta.deletes.capacity())?,
    )?;
    bytes = checked_add(bytes, slots::<SearchDocument>(delta.upserts.len())?)?;
    for row in &delta.upserts {
        checkpoint(task)?;
        bytes = checked_add(
            bytes,
            projection_row_bytes(row)? - size_of::<SearchProjectionRow>(),
        )?;
        let kind = row.kind.as_str();
        let id = slots::<u8>(checked_add(
            checked_add(kind.len(), 1)?,
            row.external_id.len(),
        )?)?;
        // into_document moves all original payloads. Only the ID and inserted
        // metadata keys/kind allocate. Count insertions even when replacing keys:
        // old values and incoming keys can overlap during the insertion.
        let mut growth = checked_add(id, checked_add("kind".len(), kind.len())?)?;
        growth = checked_add(growth, "external_id".len())?;
        let entries = 2 + usize::from(row.source_id.is_some());
        if row.source_id.is_some() {
            growth = checked_add(growth, "source_id".len())?;
        }
        growth = checked_add(growth, checked_mul(entries, MAP_ENTRY_BYTES)?)?;
        bytes = checked_add(bytes, growth)?;
    }
    for id in &delta.deletes {
        checkpoint(task)?;
        bytes = checked_add(bytes, id.capacity())?;
    }
    checkpoint(task)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests;
