//! Persistent cache invalidations share artifact payloads with captured roots.
//! Branch bits strictly decrease, bounding lookup, insertion and drop depth by
//! usize::BITS. Each new Arc cell is admitted before allocation. The enclosing
//! root retains its inventory until both data and invalidation cells are gone.

use super::*;
use std::sync::Arc;

#[derive(Debug)]
pub(super) enum Invalidations {
    Leaf(usize),
    Branch { bit: u32, children: [Arc<Self>; 2] },
}

impl Invalidations {
    pub(super) fn contains(&self, address: usize) -> bool {
        let mut current = self;
        loop {
            match current {
                Self::Leaf(found) => return *found == address,
                Self::Branch { bit, children } => {
                    current = &children[(address >> bit) & 1];
                }
            }
        }
    }

    fn leaf(&self, address: usize, work: &CheckpointWorkContext) -> Result<usize> {
        let mut current = self;
        loop {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            match current {
                Self::Leaf(found) => {
                    unit.finish();
                    work.checkpoint().map_err(HawDBError::from_storage_error)?;
                    return Ok(*found);
                }
                Self::Branch { bit, children } => {
                    current = &children[(address >> bit) & 1];
                    unit.finish();
                }
            }
        }
    }

    fn admitted(
        value: Self,
        memory: &mut CheckpointAllocationOwner,
        work: &CheckpointWorkContext,
    ) -> Result<Arc<Self>> {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let bytes =
            std::mem::size_of::<Self>() + 2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>();
        let _token = memory
            .reserve(bytes, work)
            .map_err(HawDBError::from_storage_error)?;
        let result = Arc::new(value);
        unit.finish();
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        Ok(result)
    }

    pub(super) fn insert(
        previous: Option<&Arc<Self>>,
        address: usize,
        memory: &mut CheckpointAllocationOwner,
        work: &CheckpointWorkContext,
    ) -> Result<Arc<Self>> {
        let Some(previous) = previous else {
            return Self::admitted(Self::Leaf(address), memory, work);
        };
        let found = previous.leaf(address, work)?;
        if found == address {
            return Ok(previous.clone());
        }
        let bit = usize::BITS - 1 - (address ^ found).leading_zeros();
        Self::insert_at(previous, address, bit, memory, work)
    }

    fn insert_at(
        previous: &Arc<Self>,
        address: usize,
        new_bit: u32,
        memory: &mut CheckpointAllocationOwner,
        work: &CheckpointWorkContext,
    ) -> Result<Arc<Self>> {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let above = match previous.as_ref() {
            Self::Leaf(_) => true,
            Self::Branch { bit, .. } => *bit < new_bit,
        };
        unit.finish();
        if above {
            let incoming = Self::admitted(Self::Leaf(address), memory, work)?;
            let children = if (address >> new_bit) & 1 == 0 {
                [incoming, previous.clone()]
            } else {
                [previous.clone(), incoming]
            };
            return Self::admitted(
                Self::Branch {
                    bit: new_bit,
                    children,
                },
                memory,
                work,
            );
        }
        let Self::Branch { bit, children } = previous.as_ref() else {
            unreachable!("insertion below a leaf was excluded")
        };
        let index = (address >> bit) & 1;
        let incoming = Self::insert_at(&children[index], address, new_bit, memory, work)?;
        let children = if index == 0 {
            [incoming, children[1].clone()]
        } else {
            [children[0].clone(), incoming]
        };
        Self::admitted(
            Self::Branch {
                bit: *bit,
                children,
            },
            memory,
            work,
        )
    }
}
