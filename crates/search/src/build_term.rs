//! Private immutable terms retain their admitted payload through every consumer.

use crate::build_memory::shared::Shared;
use crate::build_memory::{checked_add, BuildMemory};
use crate::{Result, SkeinError};
use skein_executor::QueryMemoryLease;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::ops::Deref;

#[derive(Clone)]
pub(crate) struct Term(Value);

#[derive(Clone)]
enum Value {
    Untracked(String),
    Tracked(Shared<Payload>),
    Reserved(Shared<Payload<crate::build_memory::reserved::Grant>>),
}

struct Payload<M = QueryMemoryLease> {
    text: String,
    _memory: M,
}

impl Term {
    // Legacy and independently admitted copies must opt in explicitly. Keep
    // infallible conversions out of production budget-tracked call sites.
    pub(crate) fn untracked(text: String) -> Self {
        Self(Value::Untracked(text))
    }

    pub(crate) fn copy(text: &str, memory: Option<&BuildMemory>) -> Result<Self> {
        Self::build(text.len(), memory, || text.to_owned())
    }

    pub(crate) fn build(
        capacity: usize,
        memory: Option<&BuildMemory>,
        build: impl FnOnce() -> String,
    ) -> Result<Self> {
        let Some(memory) = memory else {
            return Ok(Self(Value::Untracked(build())));
        };
        let header = size_of::<Payload>() + 2 * size_of::<usize>();
        let mut lease = memory.retained.reserve(checked_add(capacity, header)?)?;
        let text = build();
        if text.capacity() > capacity {
            return Err(SkeinError::Execution(
                "search term allocation exceeded its admitted capacity".into(),
            ));
        }
        lease.shrink(capacity - text.capacity());
        Ok(Self(Value::Tracked(Shared::new(Payload {
            text,
            _memory: lease,
        }))))
    }

    pub(crate) fn build_reserved(
        capacity: usize,
        memory: &crate::build_memory::reserved::ReservedMemory,
        build: impl FnOnce() -> Result<String>,
    ) -> Result<Self> {
        let mut lease = memory.reserve(Self::reserved_bytes(capacity)?)?;
        let text = build()?;
        if text.capacity() > capacity {
            return Err(SkeinError::Execution(
                "search spill term allocation exceeded its admitted capacity".into(),
            ));
        }
        lease.shrink(capacity - text.capacity());
        Ok(Self(Value::Reserved(Shared::new(Payload {
            text,
            _memory: lease,
        }))))
    }

    pub(crate) fn reserved_bytes(capacity: usize) -> Result<usize> {
        use crate::build_memory::reserved::Grant;
        checked_add(
            capacity,
            size_of::<Payload<Grant>>() + 2 * size_of::<usize>(),
        )
    }

    fn text(&self) -> &String {
        match &self.0 {
            Value::Untracked(text) => text,
            Value::Tracked(payload) => &payload.text,
            Value::Reserved(payload) => &payload.text,
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        self.text()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.text().capacity()
    }

    pub(crate) fn clone_bytes(&self) -> usize {
        match &self.0 {
            Value::Untracked(text) => text.len(),
            Value::Tracked(_) => 0,
            Value::Reserved(_) => 0,
        }
    }

    // Retained artifact buffers must not monopolize a merge progress grant.
    pub(crate) fn retained_clone_bytes(&self) -> usize {
        match &self.0 {
            Value::Reserved(text) => text.text.len(),
            _ => self.clone_bytes(),
        }
    }

    pub(crate) fn clone_for_retention(&self) -> Self {
        match &self.0 {
            Value::Reserved(text) => Self::untracked(text.text.clone()),
            _ => self.clone(),
        }
    }

    pub(crate) fn into_untracked(self) -> Result<String> {
        match self.0 {
            Value::Untracked(text) => Ok(text),
            Value::Tracked(_) => Err(SkeinError::Execution(
                "admitted search term requires an ownership-preserving consumer".into(),
            )),
            Value::Reserved(_) => Err(SkeinError::Execution(
                "reserved search term requires an ownership-preserving consumer".into(),
            )),
        }
    }
}

#[cfg(test)]
impl From<String> for Term {
    fn from(text: String) -> Self {
        Self(Value::Untracked(text))
    }
}

#[cfg(test)]
impl From<&str> for Term {
    fn from(text: &str) -> Self {
        text.to_owned().into()
    }
}

impl Deref for Term {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for Term {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<String> for Term {
    fn borrow(&self) -> &String {
        self.text()
    }
}

impl std::fmt::Debug for Term {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(formatter)
    }
}

impl PartialEq for Term {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for Term {}

impl PartialOrd for Term {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Term {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl Hash for Term {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

#[cfg(test)]
mod tests;
