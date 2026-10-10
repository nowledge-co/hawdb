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

pub(crate) struct AccountedCorpusStatistics {
    pub statistics: LexicalCorpusStatistics,
    // Both the aggregate and one temporary map remain covered during merge or
    // atomic retraction. The payload is destroyed before releasing this charge.
    _memory: QueryMemoryLease,
}

impl AccountedCorpusStatistics {
    pub(crate) fn aggregate<'a, T: Borrow<str> + Ord>(
        projections: impl IntoIterator<Item = &'a LexicalProjectionReader>,
        query_terms: &BTreeSet<T>,
        max_term_bytes: NonZeroU64,
        context: QueryContext<'_>,
    ) -> Result<Self> {
        context.checkpoint()?;
        let mut map_bytes = 0;
        for term in query_terms.iter().map(query_term) {
            context.checkpoint()?;
            let entry = checked_add(MAP_ENTRY_BYTES, term.len())?;
            map_bytes = checked_add(map_bytes, checked_add(entry, entry)?)?;
        }
        let memory = context.memory.reserve(map_bytes)?;
        let mut statistics = LexicalCorpusStatistics::default();
        for projection in projections {
            context.checkpoint()?;
            // Dictionary views borrow this one validated encoded block. Admit
            // its Vec before reading, while both frequency maps stay retained.
            let block_bytes = usize::try_from(projection.statistics_block_bytes(query_terms))
                .map_err(|_| {
                    HawDBError::Execution("lexical statistics block exceeds address space".into())
                })?;
            let _block = context.memory.reserve(block_bytes)?;
            let local = projection.query_statistics_for_terms(
                query_terms,
                max_term_bytes,
                Some(context),
            )?;
            statistics.merge(&local)?;
            context.checkpoint()?;
        }
        let output = Self {
            statistics,
            _memory: memory,
        };
        context.checkpoint()?;
        Ok(output)
    }
}

impl LexicalProjectionReader {
    pub(super) fn statistics_block_bytes<T: Borrow<str> + Ord>(
        &self,
        query_terms: &BTreeSet<T>,
    ) -> u64 {
        self.manifest
            .blocks
            .iter()
            .filter(|block| {
                block.kind == BlockKind::Postings
                    && query_terms.iter().map(query_term).any(|term| {
                        term >= block.min_key.as_str() && term <= block.max_key.as_str()
                    })
            })
            .map(|block| block.length)
            .max()
            .unwrap_or_default()
    }
}
