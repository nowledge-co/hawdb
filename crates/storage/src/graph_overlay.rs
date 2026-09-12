//! Ordered checkpoint/delta overlay used by the embedded graph store.
//!
//! The facade captures the delta and tombstone snapshot. This owner merges it
//! with a lazy canonical reader without collecting the checkpoint into memory.

use crate::cow::CowSegment;
use crate::{
    CanonicalNodeIterator, CanonicalRelationshipIterator, CanonicalSegmentError, NodeId,
    NodeRecord, RelId, RelRecord,
};
use skein_core::{Result, SkeinError};
use std::collections::BTreeSet;
use std::iter::Peekable;

#[cfg(test)]
mod tests;

pub struct GraphNodeIterator {
    inner: OverlayIterator<NodeRecord, CanonicalNodeIterator>,
}

impl Iterator for GraphNodeIterator {
    type Item = Result<NodeRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct GraphRelationshipIterator {
    inner: OverlayIterator<RelRecord, CanonicalRelationshipIterator>,
}

impl Iterator for GraphRelationshipIterator {
    type Item = Result<RelRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// Assemble an owned snapshot from ID-ordered inputs captured by the facade.
pub fn node_records(
    base: Option<CanonicalNodeIterator>,
    delta: Vec<NodeRecord>,
    tombstones: CowSegment<BTreeSet<NodeId>>,
) -> GraphNodeIterator {
    GraphNodeIterator {
        inner: OverlayIterator::new(base, delta, tombstones),
    }
}

/// Assemble an owned snapshot from ID-ordered inputs captured by the facade.
pub fn relationship_records(
    base: Option<CanonicalRelationshipIterator>,
    delta: Vec<RelRecord>,
    tombstones: CowSegment<BTreeSet<RelId>>,
) -> GraphRelationshipIterator {
    GraphRelationshipIterator {
        inner: OverlayIterator::new(base, delta, tombstones),
    }
}

trait OverlayRecord {
    type Id: Copy + Ord;

    fn id(&self) -> Self::Id;
}

impl OverlayRecord for NodeRecord {
    type Id = NodeId;

    fn id(&self) -> NodeId {
        self.id
    }
}

impl OverlayRecord for RelRecord {
    type Id = RelId;

    fn id(&self) -> RelId {
        self.id
    }
}

struct OverlayIterator<R: OverlayRecord, B: Iterator> {
    base: Option<Peekable<B>>,
    delta: Peekable<std::vec::IntoIter<R>>,
    tombstones: CowSegment<BTreeSet<R::Id>>,
}

impl<R: OverlayRecord, B: Iterator> OverlayIterator<R, B> {
    fn new(base: Option<B>, delta: Vec<R>, tombstones: CowSegment<BTreeSet<R::Id>>) -> Self {
        Self {
            base: base.map(Iterator::peekable),
            delta: delta.into_iter().peekable(),
            tombstones,
        }
    }
}

impl<R: OverlayRecord, B: Iterator<Item = std::result::Result<R, CanonicalSegmentError>>>
    OverlayIterator<R, B>
{
    fn next_base(&mut self) -> Result<R> {
        self.base
            .as_mut()
            .and_then(Iterator::next)
            .expect("peeked base record exists")
            .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))
    }
}

impl<R: OverlayRecord, B: Iterator<Item = std::result::Result<R, CanonicalSegmentError>>> Iterator
    for OverlayIterator<R, B>
{
    type Item = Result<R>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // A tombstone or a newer delta must never hide a physical read error.
            let base_id = match self.base.as_mut().and_then(|base| base.peek()) {
                Some(Ok(record)) => Some(record.id()),
                Some(Err(_)) => return Some(self.next_base()),
                None => None,
            };
            let delta_id = self.delta.peek().map(OverlayRecord::id);
            let record = match (base_id, delta_id) {
                (None, None) => return None,
                (Some(base_id), Some(delta_id)) if base_id == delta_id => {
                    if let Err(error) = self.next_base() {
                        return Some(Err(error));
                    }
                    Ok(self.delta.next().expect("matching delta record exists"))
                }
                (None, Some(_)) => Ok(self.delta.next().expect("peeked delta record exists")),
                (Some(base_id), Some(delta_id)) if base_id > delta_id => {
                    Ok(self.delta.next().expect("peeked delta record exists"))
                }
                (Some(_), _) => self.next_base(),
            };
            match record {
                Ok(record) if self.tombstones.contains(&record.id()) => continue,
                other => return Some(other),
            }
        }
    }
}
