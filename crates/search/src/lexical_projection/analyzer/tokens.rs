//! Fallible token emission with separately owned scratch and deduplication state.

use super::Charge;
use crate::build_control::checkpoint;
use crate::build_memory::{checked_add, checked_mul, BuildMemory, SET_ENTRY_BYTES};
use crate::cjk_tokenizer::visit_chinese_search_tokens;
use crate::error::{Result, SkeinError};
use crate::token_parts::{cjk_ngrams, suffix_variants, IdentifierParts};
use crate::SearchAnalyzerLexicon;
use skein_core::RuntimeTaskContext;
use std::collections::BTreeSet;

pub(in super::super) struct Text {
    value: String,
    // Heap data must be destroyed before its reservation is released.
    _memory: Charge,
}

impl Text {
    fn chars(
        chars: impl Iterator<Item = char> + Clone,
        memory: Option<&BuildMemory>,
    ) -> Result<Self> {
        let bytes = chars
            .clone()
            .try_fold(0, |bytes, ch| checked_add(bytes, ch.len_utf8()))?;
        let mut charge = Charge::new(memory)?;
        charge.grow(bytes)?;
        #[cfg(test)]
        evidence::allocation();
        let mut value = String::new();
        value
            .try_reserve_exact(bytes)
            .map_err(|error| SkeinError::Execution(error.to_string()))?;
        value.extend(chars);
        Ok(Self {
            value,
            _memory: charge,
        })
    }

    pub(in super::super) fn boundary(
        left: &str,
        right: &str,
        memory: Option<&BuildMemory>,
    ) -> Result<Self> {
        Self::chars(
            left.chars()
                .flat_map(char::to_lowercase)
                .chain(std::iter::once('_'))
                .chain(right.chars().flat_map(char::to_lowercase)),
            memory,
        )
    }

    pub(in super::super) fn lower(raw: &str, memory: Option<&BuildMemory>) -> Result<Self> {
        if raw.is_ascii() {
            return Self::chars(raw.chars().map(|ch| ch.to_ascii_lowercase()), memory);
        }
        let output = raw
            .chars()
            .flat_map(char::to_lowercase)
            .try_fold(0, |bytes, ch| checked_add(bytes, ch.len_utf8()))?;
        // Rust 1.97.1's str::to_lowercase starts with input.len() capacity and
        // pushes mapped chars with geometric String growth (minimum 8 bytes).
        // Charge old + replacement allocation, not just the final byte length.
        // Context-dependent final sigma has the same UTF-8 length as sigma.
        let bound = checked_mul(raw.len().max(output).max(8), 3)?;
        let mut charge = Charge::new(memory)?;
        charge.grow(bound)?;
        #[cfg(test)]
        evidence::allocation();
        let value = raw.to_lowercase();
        if value.capacity() > bound {
            return Err(SkeinError::Execution(
                "lowercase capacity exceeded preflight".to_string(),
            ));
        }
        charge.shrink(bound - value.capacity());
        Ok(Self {
            value,
            _memory: charge,
        })
    }

    pub(in super::super) fn as_str(&self) -> &str {
        &self.value
    }
}

struct Tokens<'a, F> {
    seen: BTreeSet<String>,
    charge: Charge,
    memory: Option<&'a BuildMemory>,
    task: Option<&'a RuntimeTaskContext>,
    lexicon: &'a SearchAnalyzerLexicon,
    emit: F,
}

impl<F: FnMut(&str) -> Result<()>> Tokens<'_, F> {
    fn unique(&mut self, token: &str) -> Result<()> {
        self.task.map_or(Ok(()), checkpoint)?;
        if !token.is_empty() && !self.lexicon.is_stopword(token) && !self.seen.contains(token) {
            self.charge
                .grow(checked_add(SET_ENTRY_BYTES, token.len())?)?;
            // Let the consumer enforce its term/token limit before cloning.
            // This reservation stays live through the consumer's own charges.
            (self.emit)(token)?;
            #[cfg(test)]
            evidence::insertion();
            self.seen.insert(token.to_string());
        }
        Ok(())
    }

    fn analyzed(&mut self, token: &str) -> Result<()> {
        self.unique(token)?;
        for (stem, suffix) in suffix_variants(token) {
            if suffix.is_empty() {
                self.unique(stem)?;
            } else {
                let normalized = Text::chars(stem.chars().chain(suffix.chars()), self.memory)?;
                self.unique(normalized.as_str())?;
            }
        }
        for alias in self.lexicon.semantic_alias_refs(token) {
            self.unique(alias)?;
        }
        Ok(())
    }
}

pub(in super::super) fn boundary(
    token: &str,
    lexicon: &SearchAnalyzerLexicon,
    task: Option<&RuntimeTaskContext>,
    memory: Option<&BuildMemory>,
    emit: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    Tokens {
        seen: BTreeSet::new(),
        charge: Charge::new(memory)?,
        memory,
        task,
        lexicon,
        emit,
    }
    .analyzed(token)
}

pub(in super::super) fn identifier(
    raw: &str,
    lexicon: &SearchAnalyzerLexicon,
    task: Option<&RuntimeTaskContext>,
    memory: Option<&BuildMemory>,
    emit: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    task.map_or(Ok(()), checkpoint)?;
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(());
    }
    let mut tokens = Tokens {
        seen: BTreeSet::new(),
        charge: Charge::new(memory)?,
        memory,
        task,
        lexicon,
        emit,
    };
    let lower = Text::lower(raw, memory)?;
    tokens.unique(lower.as_str())?;
    drop(lower);
    task.map_or(Ok(()), checkpoint)?;
    visit_chinese_search_tokens(raw, |word| tokens.analyzed(word))?;
    for gram in cjk_ngrams(raw) {
        tokens.unique(gram)?;
    }
    for part in IdentifierParts::new(raw) {
        task.map_or(Ok(()), checkpoint)?;
        let lower = Text::chars(part.chars().flat_map(char::to_lowercase), memory)?;
        tokens.analyzed(lower.as_str())?;
    }
    // Preserve the original emission order: all parts before all adjacent pairs.
    // Revisit borrowed spans instead of retaining every lowered part.
    let mut parts = IdentifierParts::new(raw);
    let mut previous = parts.next();
    for part in parts {
        task.map_or(Ok(()), checkpoint)?;
        if let Some(previous) = previous {
            let pair = Text::boundary(previous, part, memory)?;
            tokens.analyzed(pair.as_str())?;
        }
        previous = Some(part);
    }
    Ok(())
}

#[cfg(test)]
pub(in super::super) mod evidence {
    use std::cell::Cell;
    thread_local! {
        static COUNTS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    }
    pub(super) fn allocation() {
        COUNTS.with(|cell| {
            let (a, i) = cell.get();
            cell.set((a + 1, i));
        });
    }
    pub(super) fn insertion() {
        COUNTS.with(|cell| {
            let (a, i) = cell.get();
            cell.set((a, i + 1));
        });
    }
    pub(in super::super::super) fn take() -> (usize, usize) {
        COUNTS.with(|cell| cell.replace((0, 0)))
    }
}
