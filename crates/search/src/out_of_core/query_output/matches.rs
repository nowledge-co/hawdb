// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Token work borrows the actual query account; returned fields use its result grant.

use super::*;
use crate::analyzer_memory::Memory as AnalyzerMemory;
use crate::analyzer_stream::Control;
use crate::build_memory::SET_ENTRY_BYTES;
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
impl Memory<'_> {
    pub(in crate::out_of_core) fn matches(
        self,
        terms: &BTreeSet<String>,
        document: &SearchDocument,
        analyzer: &SearchAnalyzerLexicon,
        limits: Limits,
    ) -> Result<Matches> {
        let Some(frame) = self.0 else {
            let (terms, spans) = crate::query_matches_bounded(
                terms,
                document,
                analyzer,
                limits.count,
                limits.bytes,
                limits.used,
            )?;
            return Ok(Matches { terms, spans });
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
            let mut matched = BTreeSet::<&str>::new();
            let mut matched_memory = frame.query.memory.reserve(0)?;
            let mut spans = Vec::new();
            let mut bytes = 0u64;
            crate::query_matches::visit(
                terms,
                document,
                analyzer,
                control,
                |term| {
                    if !matched.contains(term) {
                        matched_memory.grow(SET_ENTRY_BYTES)?;
                        matched.insert(term);
                    }
                    Ok(())
                },
                |span| {
                    if spans.len() >= limits.count {
                        return Err(HawDBError::Storage(format!(
                            "search matched-span hydration exceeded {} spans",
                            limits.count
                        )));
                    }
                    let required = add(
                        size_of::<SearchMatchedSpan>(),
                        add(span.field.len(), add(span.text.len(), span.term.len())?)?,
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
                            message: "search matched-span hydration exceeds its byte budget".into(),
                        }
                        .into());
                    }
                    let span = SearchMatchedSpan {
                        field: self.text(span.field)?,
                        start_byte: span.start,
                        end_byte: span.end,
                        text: self.text(span.text)?,
                        term: self.text(span.term)?,
                    };
                    self.push(&mut spans, span)
                },
            )?;
            let mut output = Matches {
                terms: self.vec(matched.len())?,
                spans,
            };
            for term in matched {
                output.terms.push(self.text(term)?);
            }
            drop(matched_memory);
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
