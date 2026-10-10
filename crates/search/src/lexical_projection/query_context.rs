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

use super::{LexicalCorpusStatistics, LexicalMiniDelta, LexicalQueryReport};
use crate::error::Result;
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::{QueryMemoryAccount, QueryMemoryLease};
use std::num::NonZeroU64;

#[derive(Clone, Copy)]
pub(crate) struct QueryContext<'a> {
    pub memory: &'a QueryMemoryAccount,
    pub task: &'a RuntimeTaskContext,
}

impl QueryContext<'_> {
    pub(crate) fn checkpoint(self) -> Result<()> {
        checkpoint(self.task)
    }
}

pub(crate) struct ScoringInputs<'a> {
    pub delta: &'a LexicalMiniDelta,
    pub max_term_bytes: NonZeroU64,
    pub retained_score_limit: Option<usize>,
    pub global_statistics: Option<&'a LexicalCorpusStatistics>,
    pub prune_blocks: bool,
    pub context: Option<QueryContext<'a>>,
}

pub(crate) struct AccountedQueryReport {
    // The score map is destroyed before its capacity lease.
    pub report: LexicalQueryReport,
    pub _memory: Option<QueryMemoryLease>,
}

pub(super) fn check(context: Option<QueryContext<'_>>) -> Result<()> {
    context.map_or(Ok(()), QueryContext::checkpoint)
}

pub(super) fn complete(
    context: Option<QueryContext<'_>>,
    report: AccountedQueryReport,
) -> Result<AccountedQueryReport> {
    // Own both fields before this fallible handoff: refusal destroys the score
    // map before releasing its retained capacity.
    check(context)?;
    Ok(report)
}

pub(super) fn checkpoint(task: &RuntimeTaskContext) -> Result<()> {
    crate::query_control::checkpoint(task)
}

pub(super) fn allowed(
    context: Option<QueryContext<'_>>,
    id: &str,
    predicate: &mut impl FnMut(&str) -> Result<bool>,
) -> Result<bool> {
    check(context)?;
    let value = predicate(id)?;
    check(context)?;
    Ok(value)
}
