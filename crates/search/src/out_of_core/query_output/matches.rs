// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Token work borrows the actual query account; returned fields use its result grant.

use super::*;
use crate::analyzer_memory::Memory as AnalyzerMemory;
use crate::analyzer_stream::{visit_admitted_token_list, Control};
use crate::build_memory::SET_ENTRY_BYTES;
use crate::build_term::Term;
use crate::SearchMatchedSpan;

pub(in crate::out_of_core) struct Limits {
    pub(in crate::out_of_core) count: usize,
    pub(in crate::out_of_core) bytes: u64,
    pub(in crate::out_of_core) used: u64,
}
pub(in crate::out_of_core) struct Matches {
    pub(in crate::out_of_core) terms: Vec<String>,
    pub(in crate::out_of_core) spans: Vec<SearchMatchedSpan>,
}
struct Tokens {
    values: BTreeSet<Term>,
    _nodes: QueryMemoryLease,
}
impl Tokens {
    fn new(query: QueryContext<'_>) -> Result<Self> {
        Ok(Self {
            values: BTreeSet::new(),
            _nodes: query.memory.reserve(0)?,
        })
    }
    fn retain(&mut self, term: Term, terms: &BTreeSet<String>) -> Result<()> {
        if terms.contains(term.as_str()) && !self.values.contains(term.as_str()) {
            self._nodes.grow(SET_ENTRY_BYTES)?;
            self.values.insert(term);
        }
        Ok(())
    }
}
impl Memory<'_> {
    pub(in crate::out_of_core) fn matches(
        self,
        terms: &BTreeSet<String>,
        document: &SearchDocument,
        analyzer: &SearchAnalyzerLexicon,
        limits: Limits,
    ) -> Result<Matches> {
        let Some(frame) = self.0 else {
            return Ok(Matches {
                spans: matched_query_spans_bounded(
                    terms,
                    document,
                    analyzer,
                    limits.count,
                    limits.bytes,
                    limits.used,
                )?,
                terms: matched_query_terms(terms, document, analyzer),
            });
        };
        frame.query.checkpoint()?;
        if terms.is_empty() {
            return Ok(Matches {
                terms: Vec::new(),
                spans: Vec::new(),
            });
        }
        let mut chinese = false;
        for (text, _) in crate::analyzer_stream::document_token_fields(document) {
            for (index, ch) in text.chars().enumerate() {
                if index.is_multiple_of(1024) {
                    frame.query.checkpoint()?;
                }
                if crate::cjk_tokenizer::is_han_search_char(ch) {
                    chinese = true;
                    break;
                }
            }
            if chinese {
                break;
            }
        }
        let build = |workspace: Option<&crate::analyzer_workspace::Workspace>| {
            let control = Control {
                memory: Some(AnalyzerMemory::Query(frame.query.memory)),
                task: Some(frame.query.task),
                workspace,
                ..Default::default()
            };
            let mut matched = Tokens::new(frame.query)?;
            for (text, _) in crate::analyzer_stream::document_token_fields(document) {
                visit_admitted_token_list(text, analyzer, control, |term, _| {
                    matched.retain(term, terms)
                })?;
            }
            let mut output = Matches {
                terms: self.vec(matched.values.len())?,
                spans: Vec::new(),
            };
            for term in &matched.values {
                output.terms.push(self.text(term.as_str())?);
            }
            drop(matched);
            let mut bytes = 0u64;
            for (field, text) in [
                ("title", document.title.as_str()),
                ("content", document.content.as_str()),
            ] {
                let mut start = None;
                let mut push = |start: usize, end: usize| -> Result<()> {
                    let raw = &text[start..end];
                    let mut matches = Tokens::new(frame.query)?;
                    visit_admitted_token_list(raw, analyzer, control, |term, _| {
                        matches.retain(term, terms)
                    })?;
                    for term in &matches.values {
                        if output.spans.len() >= limits.count {
                            return Err(HawDBError::Storage(format!(
                                "search matched-span hydration exceeded {} spans",
                                limits.count
                            )));
                        }
                        let required = add(
                            size_of::<SearchMatchedSpan>(),
                            add(field.len(), add(raw.len(), term.len())?)?,
                        )? as u64;
                        let previous = bytes;
                        bytes = bytes.checked_add(required).ok_or_else(|| {
                            HawDBError::Execution("matched-span byte count overflow".into())
                        })?;
                        if bytes > limits.bytes.saturating_sub(limits.used) {
                            return Err(ResourceBudgetError {
                                resource: ResourceBudgetKind::MemoryBytes,
                                scope: ResourceBudgetScope::Operator,
                                owner: "search matched-span hydration".into(),
                                memory_class: None,
                                requested: required,
                                used: limits.used.saturating_add(previous),
                                budget: limits.bytes,
                                budget_option: "SearchOutOfCoreConfig.max_matched_span_bytes",
                                message: "search matched-span hydration exceeds its byte budget"
                                    .into(),
                            }
                            .into());
                        }
                        let span = SearchMatchedSpan {
                            field: self.text(field)?,
                            start_byte: start,
                            end_byte: end,
                            text: self.text(raw)?,
                            term: self.text(term.as_str())?,
                        };
                        self.push(&mut output.spans, span)?;
                    }
                    Ok(())
                };
                for (ordinal, (index, ch)) in text.char_indices().enumerate() {
                    if ordinal.is_multiple_of(1024) {
                        frame.query.checkpoint()?;
                    }
                    if ch.is_alphanumeric() || ch == '_' {
                        start.get_or_insert(index);
                    } else if let Some(begin) = start.take() {
                        push(begin, index)?;
                    }
                }
                if let Some(begin) = start {
                    push(begin, text.len())?;
                }
            }
            frame.query.checkpoint()?;
            Ok(output)
        };
        if chinese {
            crate::analyzer_workspace::run_query(
                frame.query.memory,
                frame.query.task,
                |workspace| build(Some(workspace)),
            )
        } else {
            build(None)
        }
    }
}
