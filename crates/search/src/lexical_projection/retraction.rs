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

//! Reanalysis uses the same spillable document reducer as initial construction.

use super::*;

pub(crate) struct RetractionContext<'a> {
    pub(crate) root: &'a Path,
    pub(crate) memory: &'a BuildMemory,
    pub(crate) task: &'a RuntimeTaskContext,
    pub(crate) needs_chinese: bool,
}
impl LexicalProjectionReader {
    pub(crate) fn source_retraction(
        &self,
        document: &(impl DocumentSource + Sync),
        analyzer: &SearchAnalyzerLexicon,
        context: RetractionContext<'_>,
        mut emit: impl FnMut(Term) -> Result<()> + Send,
    ) -> Result<u64> {
        let RetractionContext {
            root,
            memory,
            task,
            needs_chinese,
        } = context;
        let mut config = self.config;
        if let Some(reservation) = task.memory_reservation() {
            config.build_memory_bytes = NonZeroU64::new(
                config
                    .build_memory_bytes
                    .get()
                    .min((reservation.memory_bytes() / 8).max(1)),
            )
            .expect("positive reanalysis bytes");
        }
        let mut analyze = |workspace: Option<&crate::analyzer_workspace::Workspace>| {
            let mut pool = SpillRuns::with_context(root, 0, config, memory.clone(), task.clone())?;
            let mut pending = PendingPostings::new(Some(memory))?;
            let analyzed = document_frequency::analyze_with_control(
                document,
                analyzer,
                &mut pool,
                &mut pending,
                crate::analyzer_stream::Control {
                    memory: Some(memory),
                    task: Some(task),
                    workspace,
                    checkpoint_throttle: None,
                },
            )?;
            let length = u64::from(analyzed.document_len());
            analyzed.visit(config, |term, _, _| emit(term))?;
            Ok(length)
        };
        if needs_chinese {
            crate::analyzer_workspace::run(memory, task, |workspace| analyze(Some(workspace)))
        } else {
            analyze(None)
        }
    }
}
