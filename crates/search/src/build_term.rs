//! Private immutable terms retain their admitted payload through every consumer.

use crate::build_memory::{checked_add, BuildMemory};
use crate::{Result, SkeinError};
use skein_executor::QueryMemoryLease;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct Term(Value);

#[derive(Clone)]
enum Value {
    Untracked(String),
    Tracked(Arc<Payload>),
}

struct Payload {
    text: String,
    _memory: QueryMemoryLease,
}

impl Term {
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
        Ok(Self(Value::Tracked(Arc::new(Payload {
            text,
            _memory: lease,
        }))))
    }

    fn text(&self) -> &String {
        match &self.0 {
            Value::Untracked(text) => text,
            Value::Tracked(payload) => &payload.text,
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
        }
    }

    pub(crate) fn into_untracked(self) -> Result<String> {
        match self.0 {
            Value::Untracked(text) => Ok(text),
            Value::Tracked(_) => Err(SkeinError::Execution(
                "admitted search term requires an ownership-preserving consumer".into(),
            )),
        }
    }
}

impl From<String> for Term {
    fn from(text: String) -> Self {
        Self(Value::Untracked(text))
    }
}

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
