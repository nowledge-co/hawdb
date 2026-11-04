// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Admitted ordered composite reads over existing immutable block ordinals.

use super::*;
use crate::cache::{RepresentationKind, SegmentBytes, SegmentCacheError, SegmentCacheKey};
use crate::canonical::standalone_value_allocation_bytes;
use std::cmp::Ordering;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy)]
pub(crate) struct CompositeOrder<'a> {
    pub label: LabelId,
    pub properties: &'a [String],
    pub equality: &'a [(String, Value)],
    pub after: Option<&'a [Value]>,
    pub descending: bool,
}

impl CompositeOrder<'_> {
    fn validate(&self) -> Result<(), PersistentPropertyProjectionError> {
        if self.equality.is_empty()
            || self.equality.len() >= self.properties.len()
            || self
                .equality
                .iter()
                .zip(self.properties)
                .any(|((key, _), property)| key != property)
            || self
                .after
                .is_some_and(|after| after.len() != self.properties.len() - self.equality.len())
        {
            return Err(PersistentPropertyProjectionError::Source(
                "invalid ordered composite prefix or continuation arity".into(),
            ));
        }
        Ok(())
    }

    fn values<'a>(&self, key: &'a Value) -> Result<&'a [Value], PersistentPropertyProjectionError> {
        match key {
            Value::List(values) if values.len() == self.properties.len() => Ok(values),
            _ => Err(PersistentPropertyProjectionError::Corrupt(
                "ordered composite key has invalid arity".into(),
            )),
        }
    }

    fn prefix_order(&self, key: &Value) -> Result<Ordering, PersistentPropertyProjectionError> {
        Ok(self
            .values(key)?
            .iter()
            .zip(self.equality)
            .map(|(value, (_, expected))| value.cmp(expected))
            .find(|order| !order.is_eq())
            .unwrap_or(Ordering::Equal))
    }

    fn probe_order(&self, key: &Value) -> Result<Ordering, PersistentPropertyProjectionError> {
        let prefix = self.prefix_order(key)?;
        if !prefix.is_eq() {
            return Ok(prefix);
        }
        Ok(self.after.map_or(Ordering::Equal, |after| {
            self.values(key).expect("validated key arity")[self.equality.len()..].cmp(after)
        }))
    }

    fn accepts(&self, key: &Value) -> Result<bool, PersistentPropertyProjectionError> {
        Ok(self
            .values(key)?
            .iter()
            .all(|value| !matches!(value, Value::Null))
            && self.prefix_order(key)?.is_eq()
            && (self.after.is_none()
                || if self.descending {
                    self.probe_order(key)?.is_lt()
                } else {
                    self.probe_order(key)?.is_gt()
                }))
    }
}

struct Route<'a> {
    order: CompositeOrder<'a>,
    identity: String,
    prefix: Vec<u8>,
}

impl Route<'_> {
    fn key(&self, block: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.prefix.len() + 8);
        key.extend_from_slice(&self.prefix);
        key.extend_from_slice(&block.to_be_bytes());
        key
    }
}

enum Probe {
    First,
    Last,
    Exact(u64),
}

fn overflow() -> PersistentPropertyProjectionError {
    PersistentPropertyProjectionError::Corrupt(
        "ordered property projection accounting overflow".into(),
    )
}

fn add(target: &mut u64, value: u64) -> Result<(), PersistentPropertyProjectionError> {
    *target = target.checked_add(value).ok_or_else(overflow)?;
    Ok(())
}

fn descriptor_limits(
    report: &PersistentPropertyProjectionReadReport,
) -> Result<GraphDescriptorTreeReadLimits, PersistentPropertyProjectionError> {
    let limits = GraphDescriptorTreeReadLimits::default();
    let remaining = |ceiling: u64, used: u64| {
        ceiling
            .checked_sub(used)
            .and_then(NonZeroU64::new)
            .ok_or_else(|| {
                PersistentPropertyProjectionError::DescriptorTree(
                    GraphDescriptorTreeError::Admission(
                        "ordered descriptor read exhausted its cumulative allowance".into(),
                    ),
                )
            })
    };
    Ok(GraphDescriptorTreeReadLimits {
        max_pages: remaining(limits.max_pages.get(), report.descriptor_pages_visited)?,
        max_page_bytes: remaining(
            limits.max_page_bytes.get(),
            report.descriptor_page_bytes_decoded,
        )?,
        max_descriptors: remaining(limits.max_descriptors.get(), report.descriptors_examined)?,
        max_tree_height: limits.max_tree_height,
    })
}

fn merge_descriptor_report(
    report: &mut PersistentPropertyProjectionReadReport,
    step: GraphDescriptorTreeReadReport,
) -> Result<(), PersistentPropertyProjectionError> {
    for (target, value) in [
        (&mut report.descriptor_pages_visited, step.pages_visited),
        (
            &mut report.descriptor_page_bytes_decoded,
            step.page_bytes_decoded,
        ),
        (
            &mut report.descriptor_storage_bytes_read,
            step.storage_bytes_read,
        ),
        (&mut report.descriptors_examined, step.descriptors_emitted),
        (&mut report.descriptor_cache_hits, step.cache_hits),
        (&mut report.descriptor_cache_misses, step.cache_misses),
        (
            &mut report.descriptor_cache_admission_rejections,
            step.cache_admission_rejections,
        ),
    ] {
        add(target, value)?;
    }
    Ok(())
}

pub(super) fn descriptor_memory(
    encoded: &[u8],
    identity: &str,
    arity: usize,
) -> Result<usize, PersistentPropertyProjectionError> {
    if encoded.len() < DESCRIPTOR_VALUE_HEADER_BYTES {
        return Err(PersistentPropertyProjectionError::Corrupt(
            "ordered descriptor is shorter than its header".into(),
        ));
    }
    let length = |offset| {
        u32::from_le_bytes(
            encoded[offset..offset + 4]
                .try_into()
                .expect("checked descriptor header"),
        ) as usize
    };
    let property_end = DESCRIPTOR_VALUE_HEADER_BYTES
        .checked_add(length(56))
        .ok_or_else(overflow)?;
    let min_end = property_end.checked_add(length(60)).ok_or_else(overflow)?;
    let max_end = min_end.checked_add(length(64)).ok_or_else(overflow)?;
    if max_end != encoded.len() {
        return Err(PersistentPropertyProjectionError::Corrupt(
            "ordered descriptor field lengths disagree".into(),
        ));
    }
    if &encoded[DESCRIPTOR_VALUE_HEADER_BYTES..property_end] != identity.as_bytes() {
        return Err(PersistentPropertyProjectionError::Corrupt(
            "ordered descriptor escaped its property identity".into(),
        ));
    }
    let minimum = standalone_value_allocation_bytes(&encoded[property_end..min_end])?;
    let maximum = standalone_value_allocation_bytes(&encoded[min_end..max_end])?;
    // Includes identity validation's temporary key and decoded property names.
    std::mem::size_of::<PersistentPropertyProjectionBlockDescriptor>()
        .checked_add(length(56).checked_mul(4).ok_or_else(overflow)?)
        .and_then(|bytes| bytes.checked_add(arity.checked_mul(4 * std::mem::size_of::<String>())?))
        .and_then(|bytes| bytes.checked_add(4 * std::mem::size_of::<Vec<u8>>() + 15))
        .and_then(|bytes| bytes.checked_add(minimum))
        .and_then(|bytes| bytes.checked_add(maximum))
        .ok_or_else(overflow)
}

impl PersistentPropertyProjectionReader {
    pub(crate) fn scan_composite_ordered_entries<A>(
        &self,
        order: CompositeOrder<'_>,
        mut admit: impl FnMut(usize) -> Result<A, PersistentPropertyProjectionError>,
        mut consumer: impl FnMut(
            &Value,
            NodeId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<
        (PersistentPropertyProjectionReadReport, CanonicalScanControl),
        PersistentPropertyProjectionError,
    > {
        self.ensure_healthy()?;
        order.validate()?;
        let result = self.scan_composite_ordered_inner(order, &mut admit, &mut consumer);
        self.poison_on_physical_failure(&result);
        result
    }

    fn scan_composite_ordered_inner<A>(
        &self,
        order: CompositeOrder<'_>,
        admit: &mut impl FnMut(usize) -> Result<A, PersistentPropertyProjectionError>,
        consumer: &mut impl FnMut(
            &Value,
            NodeId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<
        (PersistentPropertyProjectionReadReport, CanonicalScanControl),
        PersistentPropertyProjectionError,
    > {
        let identity_bytes = order
            .properties
            .iter()
            .try_fold(
                COMPOSITE_PROPERTY_IDENTITY_PREFIX.len(),
                |bytes, property| bytes.checked_add(property.len().checked_mul(2)?.checked_add(1)?),
            )
            .ok_or_else(overflow)?;
        let _route_memory = admit(
            identity_bytes
                .checked_mul(3)
                .and_then(|bytes| bytes.checked_add(32))
                .ok_or_else(overflow)?,
        )?;
        let identity = persistent_composite_property_identity(order.properties)?;
        if !self.manifest.supports(
            order.label,
            &identity,
            PersistentPropertyProjectionKind::CompositeEquality,
        ) {
            return Err(PersistentPropertyProjectionError::Source(
                "ordered composite projection is unavailable or incomplete".into(),
            ));
        }
        let route = Route {
            order,
            prefix: property_projection_descriptor_prefix(
                PersistentPropertyProjectionKind::CompositeEquality,
                order.label,
                &identity,
            ),
            identity,
        };
        let mut report = PersistentPropertyProjectionReadReport::default();
        let Some((first, first_lease)) =
            self.ordered_descriptor(&route, Probe::First, &mut report, admit)?
        else {
            return Ok((report, CanonicalScanControl::Continue));
        };
        let first_id = first.block_id;
        drop((first, first_lease));
        let Some((last, last_lease)) =
            self.ordered_descriptor(&route, Probe::Last, &mut report, admit)?
        else {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "ordered projection lost its last descriptor".into(),
            ));
        };
        let last_id = last.block_id;
        drop((last, last_lease));
        let mut low = first_id;
        let mut high = last_id.checked_add(1).ok_or_else(overflow)?;
        while low < high {
            let middle = low + (high - low) / 2;
            let (block, _lease) = self
                .ordered_descriptor(&route, Probe::Exact(middle), &mut report, admit)?
                .ok_or_else(|| {
                    PersistentPropertyProjectionError::Corrupt(
                        "ordered projection has a missing block ordinal".into(),
                    )
                })?;
            let ordering = order.probe_order(if order.descending {
                &block.min_key
            } else {
                &block.max_key
            })?;
            if if order.descending {
                !ordering.is_gt()
            } else {
                ordering.is_lt()
            } {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        let start = if order.descending {
            low.checked_sub(1).filter(|id| *id >= first_id)
        } else {
            (low <= last_id).then_some(low)
        };
        let Some(start) = start else {
            return Ok((report, CanonicalScanControl::Continue));
        };
        let key = route.key(start);
        let limits = descriptor_limits(&report)?;
        let mut error = None;
        let mut stopped = false;
        let mut visit = |key: &[u8], value: &[u8]| {
            if !key.starts_with(&route.prefix) {
                return Ok(GraphDescriptorTreeScanControl::Stop);
            }
            let step = (|| {
                let _descriptor_lease = admit(descriptor_memory(
                    value,
                    &route.identity,
                    order.properties.len(),
                )?)?;
                let block =
                    PersistentPropertyProjectionBlockDescriptor::decode_descriptor_tree_entry(
                        key, value,
                    )?;
                self.validate_selected_descriptor(
                    &block,
                    PersistentPropertyProjectionKind::CompositeEquality,
                    order.label,
                    &route.identity,
                )?;
                add(&mut report.blocks_considered, 1)?;
                let boundary = order.prefix_order(if order.descending {
                    &block.max_key
                } else {
                    &block.min_key
                })?;
                if if order.descending {
                    boundary.is_lt()
                } else {
                    boundary.is_gt()
                } {
                    return Ok(CanonicalScanControl::Stop);
                }
                let mut returned = 0;
                let control = self.scan_ordered_block(
                    &block,
                    order,
                    &mut report,
                    admit,
                    &mut |value, id| {
                        if order.accepts(value)? {
                            add(&mut returned, 1)?;
                            let control = consumer(value, id)?;
                            stopped |= control == CanonicalScanControl::Stop;
                            Ok(control)
                        } else {
                            Ok(CanonicalScanControl::Continue)
                        }
                    },
                )?;
                add(&mut report.candidates_returned, returned)?;
                Ok(control)
            })();
            match step {
                Ok(CanonicalScanControl::Continue) => Ok(GraphDescriptorTreeScanControl::Continue),
                Ok(CanonicalScanControl::Stop) => Ok(GraphDescriptorTreeScanControl::Stop),
                Err(failure) => {
                    error = Some(failure);
                    Ok(GraphDescriptorTreeScanControl::Stop)
                }
            }
        };
        let descriptor_result = if order.descending {
            self.descriptor_reader
                .scan_through(&key, limits, &mut visit)
        } else {
            self.descriptor_reader.scan_from(&key, limits, &mut visit)
        };
        if let Some(error) = error {
            return Err(error);
        }
        let (step, _) = descriptor_result?;
        merge_descriptor_report(&mut report, step)?;
        Ok((
            report,
            if stopped {
                CanonicalScanControl::Stop
            } else {
                CanonicalScanControl::Continue
            },
        ))
    }

    fn ordered_descriptor<A>(
        &self,
        route: &Route<'_>,
        probe: Probe,
        report: &mut PersistentPropertyProjectionReadReport,
        admit: &mut impl FnMut(usize) -> Result<A, PersistentPropertyProjectionError>,
    ) -> Result<
        Option<(PersistentPropertyProjectionBlockDescriptor, A)>,
        PersistentPropertyProjectionError,
    > {
        let key = route.key(match probe {
            Probe::First => BLOCK_ID_BASE,
            Probe::Last => u64::MAX,
            Probe::Exact(id) => id,
        });
        let mut found = None;
        let mut visit = |key: &[u8], value: &[u8]| {
            if key.starts_with(&route.prefix) {
                found = Some((|| {
                    let lease = admit(descriptor_memory(
                        value,
                        &route.identity,
                        route.order.properties.len(),
                    )?)?;
                    let block =
                        PersistentPropertyProjectionBlockDescriptor::decode_descriptor_tree_entry(
                            key, value,
                        )?;
                    self.validate_selected_descriptor(
                        &block,
                        PersistentPropertyProjectionKind::CompositeEquality,
                        route.order.label,
                        &route.identity,
                    )?;
                    if let Probe::Exact(id) = probe
                        && block.block_id != id
                    {
                        return Err(PersistentPropertyProjectionError::Corrupt(
                            "ordered descriptor ordinal is not contiguous".into(),
                        ));
                    }
                    Ok((block, lease))
                })());
            }
            Ok(GraphDescriptorTreeScanControl::Stop)
        };
        let limits = descriptor_limits(report)?;
        let (step, _) = if matches!(probe, Probe::Last) {
            self.descriptor_reader
                .scan_through(&key, limits, &mut visit)?
        } else {
            self.descriptor_reader.scan_from(&key, limits, &mut visit)?
        };
        merge_descriptor_report(report, step)?;
        found.transpose()
    }

    fn scan_ordered_block<A>(
        &self,
        block: &PersistentPropertyProjectionBlockDescriptor,
        order: CompositeOrder<'_>,
        report: &mut PersistentPropertyProjectionReadReport,
        admit: &mut impl FnMut(usize) -> Result<A, PersistentPropertyProjectionError>,
        consumer: &mut impl FnMut(
            &Value,
            NodeId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<CanonicalScanControl, PersistentPropertyProjectionError> {
        let (view, _workspace) =
            self.ordered_block_view(block, order.properties.len(), report, admit)?;
        let count = block.entry_count as usize;
        let physical = usize::try_from(block.length.get()).map_err(|_| overflow())?;
        let read = |index: usize, report: &mut PersistentPropertyProjectionReadReport| {
            let offset = physical
                .checked_add(index.checked_mul(8).ok_or_else(overflow)?)
                .ok_or_else(overflow)?;
            let encoded = view
                .get(offset..offset.checked_add(8).ok_or_else(overflow)?)
                .ok_or_else(overflow)?;
            let offset = usize::try_from(u64::from_le_bytes(
                encoded.try_into().expect("checked offset"),
            ))
            .map_err(|_| overflow())?;
            let bytes = view.get(offset..physical).ok_or_else(overflow)?;
            let mut cursor = Cursor::new(bytes);
            let len = cursor.read_u32()? as usize;
            let value = decode_standalone_value(cursor.read_exact(len)?)?;
            let id = NodeId(cursor.read_u64()?);
            order.values(&value)?;
            add(&mut report.entries_decoded, 1)?;
            Ok::<_, PersistentPropertyProjectionError>((value, id))
        };
        // Selection metadata remains authoritative on warm representations too.
        let first = read(0, report)?;
        let last = read(count.checked_sub(1).ok_or_else(overflow)?, report)?;
        if first.0 != block.min_key || last.0 != block.max_key {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "ordered cached block bounds disagree with descriptor".into(),
            ));
        }
        drop((first, last));
        let mut low = 0;
        let mut high = count;
        while low < high {
            let middle = low + (high - low) / 2;
            let (value, _) = read(middle, report)?;
            let compare = order.probe_order(&value)?;
            let skip = if order.descending {
                compare.is_lt() || (order.after.is_none() && compare.is_eq())
            } else {
                compare.is_lt() || (order.after.is_some() && compare.is_eq())
            };
            if skip {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        let mut next = if order.descending {
            low.checked_sub(1)
        } else {
            (low < count).then_some(low)
        };
        while let Some(index) = next {
            let (value, id) = read(index, report)?;
            let prefix = order.prefix_order(&value)?;
            if if order.descending {
                prefix.is_lt()
            } else {
                prefix.is_gt()
            } {
                break;
            }
            if consumer(&value, id)? == CanonicalScanControl::Stop {
                return Ok(CanonicalScanControl::Stop);
            }
            next = if order.descending {
                index.checked_sub(1)
            } else {
                index.checked_add(1).filter(|index| *index < count)
            };
        }
        Ok(CanonicalScanControl::Continue)
    }

    fn ordered_block_view<A>(
        &self,
        block: &PersistentPropertyProjectionBlockDescriptor,
        arity: usize,
        report: &mut PersistentPropertyProjectionReadReport,
        admit: &mut impl FnMut(usize) -> Result<A, PersistentPropertyProjectionError>,
    ) -> Result<(SegmentBytes, A), PersistentPropertyProjectionError> {
        let physical = usize::try_from(block.length.get()).map_err(|_| overflow())?;
        if block.length.get() > self.max_block_bytes.get() {
            return Err(PersistentPropertyProjectionError::BlockTooLarge {
                block_bytes: block.length.get(),
                max_bytes: self.max_block_bytes.get(),
            });
        }
        let view_len = physical
            .checked_add(
                (block.entry_count as usize)
                    .checked_mul(8)
                    .ok_or_else(overflow)?,
            )
            .and_then(|bytes| bytes.checked_add(16))
            .ok_or_else(overflow)?;
        let key = SegmentCacheKey {
            store_id: self.store_id,
            manifest_generation: self.manifest.generation,
            segment_id: block.block_id,
            content_digest: block.content_digest,
            representation: RepresentationKind::PropertyProjectionOrderedBlock,
        };
        // Bind the reusable structural proof to the exact checksum-validated
        // immutable source bytes, not merely the containing manifest identity.
        // This bounded byte hash remains work on warm reads; key decoding does not.
        let read = self.read_block(block)?;
        let tag = *hawdb_integrity::sha256(&read.payload).as_bytes();
        add(&mut report.blocks_read, 1)?;
        add(&mut report.bytes_read, block.length.get())?;
        let workspace_bytes = |max_key: usize| {
            max_key
                .checked_mul(6)
                .and_then(|bytes| bytes.checked_add(block.property.len().checked_mul(4)?))
                .and_then(|bytes| {
                    bytes.checked_add(arity.checked_mul(4 * std::mem::size_of::<String>())?)
                })
                .ok_or_else(overflow)
        };
        if let Some(cached) = self.ordered_cache.get_verified(&key, tag) {
            let view = cached.into_bytes();
            if view.len() != view_len {
                return Err(PersistentPropertyProjectionError::Corrupt(
                    "ordered cached view has invalid length".into(),
                ));
            }
            let footer = &view[view.len() - 16..];
            let stored_physical = u64::from_le_bytes(footer[..8].try_into().expect("fixed footer"));
            if stored_physical != block.length.get() {
                return Err(PersistentPropertyProjectionError::Corrupt(
                    "ordered cached physical extent disagrees".into(),
                ));
            }
            block_cursor(&view[..physical], self.manifest.generation, block)?;
            let max_key = usize::try_from(u64::from_le_bytes(
                footer[8..].try_into().expect("fixed footer"),
            ))
            .map_err(|_| overflow())?;
            let workspace = admit(workspace_bytes(max_key)?)?;
            add(&mut report.cache_hits, 1)?;
            return Ok((view, workspace));
        }
        add(&mut report.cache_misses, 1)?;
        let mut max_key = 0;
        walk_entries(
            &read.payload,
            self.manifest.generation,
            block,
            Some(arity),
            |_, value| {
                max_key = max_key.max(standalone_value_allocation_bytes(value)?);
                Ok(())
            },
        )?;
        let workspace = admit(
            workspace_bytes(max_key)?
                .checked_add(view_len)
                .ok_or_else(overflow)?,
        )?;
        // Validate full ordering, arity and descriptor bounds before any row or
        // reusable proof is published. Cold validation work remains observable.
        decode_projection_block(&read.payload, self.manifest.generation, block, |_, _| {
            add(&mut report.entries_decoded, 1)
        })?;
        let mut view = Vec::new();
        view.try_reserve_exact(view_len).map_err(|_| {
            PersistentPropertyProjectionError::Source("ordered block view allocation failed".into())
        })?;
        view.extend_from_slice(&read.payload);
        walk_entries(
            &read.payload,
            self.manifest.generation,
            block,
            Some(arity),
            |offset, _| {
                view.extend_from_slice(&(offset as u64).to_le_bytes());
                Ok(())
            },
        )?;
        view.extend_from_slice(&block.length.get().to_le_bytes());
        view.extend_from_slice(&(max_key as u64).to_le_bytes());
        let view = match self.ordered_cache.insert_verified(key, tag, view) {
            Ok(lease) => lease.into_bytes(),
            Err(error) => {
                let (reason, bytes) = error.into_parts();
                match reason {
                    SegmentCacheError::EntryTooLarge { .. }
                    | SegmentCacheError::PinnedCapacity { .. } => bytes.into(),
                    error => {
                        return Err(PersistentPropertyProjectionError::Corrupt(format!(
                            "ordered view cache identity refused: {error}"
                        )))
                    }
                }
            }
        };
        Ok((view, workspace))
    }
}

pub(super) fn walk_entries(
    payload: &[u8],
    generation: ManifestGeneration,
    block: &PersistentPropertyProjectionBlockDescriptor,
    arity: Option<usize>,
    mut consume: impl FnMut(usize, &[u8]) -> Result<(), PersistentPropertyProjectionError>,
) -> Result<(), PersistentPropertyProjectionError> {
    let mut cursor = block_cursor(payload, generation, block)?;
    let count = block.entry_count;
    for _ in 0..count {
        let offset = cursor.offset;
        let length = cursor.read_u32()? as usize;
        let value = cursor.read_exact(length)?;
        if let Some(arity) = arity
            && (value.len() < 5
                || value[0] != 5
                || u32::from_le_bytes(
                    value[1..5]
                        .try_into()
                        .expect("checked composite list header"),
                ) as usize
                    != arity)
        {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "ordered block key has invalid composite arity".into(),
            ));
        }
        cursor.read_u64()?;
        consume(offset, value)?;
    }
    if !cursor.is_empty() {
        return Err(PersistentPropertyProjectionError::Corrupt(
            "ordered block has trailing bytes".into(),
        ));
    }
    Ok(())
}

fn block_cursor<'a>(
    payload: &'a [u8],
    generation: ManifestGeneration,
    block: &PersistentPropertyProjectionBlockDescriptor,
) -> Result<Cursor<'a>, PersistentPropertyProjectionError> {
    let mut cursor = Cursor::new(payload);
    if cursor.read_exact(8)? != BLOCK_HEADER {
        return Err(PersistentPropertyProjectionError::Corrupt(
            "ordered block has an invalid header".into(),
        ));
    }
    let stored_generation = cursor.read_u64()?;
    let id = cursor.read_u64()?;
    let kind = cursor.read_u8()?;
    let label = cursor.read_u32()?;
    let property_length = cursor.read_u32()? as usize;
    let property = cursor.read_exact(property_length)?;
    let count = cursor.read_u32()?;
    if stored_generation != generation.0
        || id != block.block_id
        || kind != kind_tag(block.kind)
        || label != block.label_id.0
        || property != block.property.as_bytes()
        || count != block.entry_count
        || count as usize > payload.len() / 13
    {
        return Err(PersistentPropertyProjectionError::Corrupt(
            "ordered block header disagrees with its selected descriptor".into(),
        ));
    }
    Ok(cursor)
}
