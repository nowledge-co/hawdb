// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use crate::analyzer_memory::Memory;
use crate::analyzer_stream::{visit_admitted_token_list, Control};
use crate::build_memory::SET_ENTRY_BYTES;

pub(crate) struct NormalizedQuery {
    // Tracked terms retain their own payloads; set nodes drop before their lease.
    pub terms: BTreeSet<Term>,
    _nodes: QueryMemoryLease,
}

impl LexicalProjectionReader {
    pub(crate) fn tokenize_accounted(
        &self,
        text: &str,
        analyzer: &SearchAnalyzerLexicon,
        max_term_bytes: NonZeroU64,
        context: QueryContext<'_>,
    ) -> Result<NormalizedQuery> {
        context.checkpoint()?;
        if text.len() as u64 > self.config.query_memory_bytes.get() {
            return Err(HawDBError::Storage(
                "lexical query source exceeds its memory budget".into(),
            ));
        }
        let mut needs_worker = false;
        for (index, ch) in text.chars().enumerate() {
            if index.is_multiple_of(1024) {
                context.checkpoint()?;
            }
            if crate::cjk_tokenizer::is_han_search_char(ch) {
                needs_worker = true;
                break;
            }
        }
        let normalize = |workspace: Option<&crate::analyzer_workspace::Workspace>| {
            let mut output = NormalizedQuery {
                terms: BTreeSet::new(),
                _nodes: context.memory.reserve(0)?,
            };
            let mut logical_bytes = 0u64;
            visit_admitted_token_list(
                text,
                analyzer,
                Control {
                    memory: Some(Memory::Query(context.memory)),
                    task: Some(context.task),
                    workspace,
                    ..Default::default()
                },
                |term, _| {
                    context.checkpoint()?;
                    admit_term_bytes(term.len() as u64, max_term_bytes)?;
                    if !output.terms.contains(term.as_str()) {
                        if output.terms.len() >= self.config.max_query_terms.get() {
                            return Err(HawDBError::Storage(format!(
                                "lexical query produced more than {} terms",
                                self.config.max_query_terms
                            )));
                        }
                        logical_bytes = logical_bytes
                            .saturating_add(term.len() as u64)
                            .saturating_add(32);
                        if logical_bytes > self.config.query_memory_bytes.get() {
                            return Err(HawDBError::Storage(
                                "lexical query terms exceed their memory budget".into(),
                            ));
                        }
                        output._nodes.grow(SET_ENTRY_BYTES)?;
                        output.terms.insert(term);
                    }
                    Ok(())
                },
            )?;
            context.checkpoint()?;
            Ok(output)
        };
        let output = if needs_worker {
            crate::analyzer_workspace::run_query(context.memory, context.task, |workspace| {
                normalize(Some(workspace))
            })?
        } else {
            normalize(None)?
        };
        // The fully owned payload and node charge precede the final task check.
        context.checkpoint()?;
        Ok(output)
    }
}
