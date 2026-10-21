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

//! Cooperative checkpoint replay uses the same complete preimage collector as
//! ordinary replay, including canonical records and schema rebuild barriers.

use super::*;
use crate::background::CheckpointWorkContext;
use crate::relational::RelationalPrimaryKeyChangeCapture;

impl GraphStore {
    pub(in crate::store) fn record_checkpoint_search_projection_changes_for_ops(
        &mut self,
        catalog: &Catalog,
        commit_epoch: u64,
        ops: &[WalOp],
        relational_changes: Option<RelationalPrimaryKeyChangeCapture>,
        work: &CheckpointWorkContext,
    ) -> Result<()> {
        let change = self.prepare_search_projection_changes_for_ops_with_work_context(
            catalog,
            commit_epoch,
            ops,
            relational_changes,
            Some(work),
        )?;
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        self.install_search_projection_change(change);
        Ok(())
    }
}
