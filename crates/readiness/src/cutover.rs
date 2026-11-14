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

//! Host-neutral cutover controls and fail-closed reduction for Nowledge Mem.

use crate::previous_wrapper_preflight::CUTOVER_CONTROLS_PROTOCOL;
use crate::runtime_status::EmbeddedProductionStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadControl {
    Legacy,
    HawDB,
}

impl ReadControl {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::HawDB => "hawdb",
        }
    }

    pub const fn selects_hawdb(self) -> bool {
        matches!(self, Self::HawDB)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkControl {
    Disabled,
    Enabled,
}

impl WorkControl {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Enabled => "enabled",
        }
    }

    pub const fn enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CutoverControls {
    pub graph_reads: ReadControl,
    pub search_reads: ReadControl,
    pub dual_writes: WorkControl,
    pub initial_import: WorkControl,
    pub projection_catch_up: WorkControl,
}

impl CutoverControls {
    pub const fn legacy() -> Self {
        Self {
            graph_reads: ReadControl::Legacy,
            search_reads: ReadControl::Legacy,
            dual_writes: WorkControl::Disabled,
            initial_import: WorkControl::Disabled,
            projection_catch_up: WorkControl::Disabled,
        }
    }

    pub const fn hawdb_shadow() -> Self {
        Self {
            graph_reads: ReadControl::Legacy,
            search_reads: ReadControl::Legacy,
            dual_writes: WorkControl::Enabled,
            initial_import: WorkControl::Enabled,
            projection_catch_up: WorkControl::Enabled,
        }
    }

    pub const fn hawdb_reads() -> Self {
        Self {
            graph_reads: ReadControl::HawDB,
            search_reads: ReadControl::HawDB,
            dual_writes: WorkControl::Enabled,
            initial_import: WorkControl::Disabled,
            projection_catch_up: WorkControl::Enabled,
        }
    }

    fn json(self) -> serde_json::Value {
        serde_json::json!({
            "graph_reads": self.graph_reads.as_str(),
            "search_reads": self.search_reads.as_str(),
            "dual_writes": self.dual_writes.as_str(),
            "initial_import": self.initial_import.as_str(),
            "projection_catch_up": self.projection_catch_up.as_str(),
        })
    }
}

impl Default for CutoverControls {
    fn default() -> Self {
        Self::legacy()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CutoverControlsReport {
    pub protocol: String,
    pub ready: bool,
    pub controls: CutoverControls,
    pub graph_read_selected_hawdb: bool,
    pub graph_read_effective: bool,
    pub search_read_selected_hawdb: bool,
    pub search_read_effective: bool,
    pub dual_writes_enabled: bool,
    pub initial_import_enabled: bool,
    pub initial_import_inactive_for_cutover: bool,
    pub initial_import_cutover_catch_up_ready: bool,
    pub initial_import_safe_for_read_cutover: bool,
    pub projection_catch_up_enabled: bool,
    pub blocker_codes: Vec<String>,
    pub production_status: EmbeddedProductionStatus,
}

impl CutoverControlsReport {
    #[doc(hidden)]
    pub fn from_production_status(
        controls: CutoverControls,
        production_status: EmbeddedProductionStatus,
        initial_import_cutover_catch_up_ready: bool,
    ) -> Self {
        let graph_read_selected_hawdb = controls.graph_reads.selects_hawdb();
        let search_read_selected_hawdb = controls.search_reads.selects_hawdb();
        let dual_writes_enabled = controls.dual_writes.enabled();
        let initial_import_enabled = controls.initial_import.enabled();
        let initial_import_inactive_for_cutover = !initial_import_enabled;
        let initial_import_safe_for_read_cutover =
            initial_import_inactive_for_cutover || initial_import_cutover_catch_up_ready;
        let projection_catch_up_enabled = controls.projection_catch_up.enabled();
        let graph_read_effective =
            !graph_read_selected_hawdb || production_status.graph_hawdb_cutover_effective;
        let search_read_effective =
            !search_read_selected_hawdb || production_status.search_hawdb_cutover_effective;
        let mut blocker_codes = Vec::new();
        if !graph_read_effective {
            blocker_codes.push("graph_read_selected_hawdb_but_not_effective".to_string());
        }
        if !search_read_effective {
            blocker_codes.push("search_read_selected_hawdb_but_not_effective".to_string());
        }
        if search_read_selected_hawdb && !projection_catch_up_enabled {
            blocker_codes
                .push("search_read_selected_hawdb_without_projection_catch_up".to_string());
        }
        if initial_import_enabled && !dual_writes_enabled {
            blocker_codes.push("initial_import_enabled_without_dual_writes".to_string());
        }
        if !initial_import_safe_for_read_cutover {
            blocker_codes.push("initial_import_active_blocks_read_cutover".to_string());
        }

        Self {
            protocol: CUTOVER_CONTROLS_PROTOCOL.to_string(),
            ready: blocker_codes.is_empty(),
            controls,
            graph_read_selected_hawdb,
            graph_read_effective,
            search_read_selected_hawdb,
            search_read_effective,
            dual_writes_enabled,
            initial_import_enabled,
            initial_import_inactive_for_cutover,
            initial_import_cutover_catch_up_ready,
            initial_import_safe_for_read_cutover,
            projection_catch_up_enabled,
            blocker_codes,
            production_status,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "controls": self.controls.json(),
            "graph": {
                "read_selected_hawdb": self.graph_read_selected_hawdb,
                "read_effective": self.graph_read_effective,
            },
            "search": {
                "read_selected_hawdb": self.search_read_selected_hawdb,
                "read_effective": self.search_read_effective,
            },
            "work": {
                "dual_writes_enabled": self.dual_writes_enabled,
                "initial_import_enabled": self.initial_import_enabled,
                "initial_import_inactive_for_cutover": self.initial_import_inactive_for_cutover,
                "initial_import_cutover_catch_up_ready": self.initial_import_cutover_catch_up_ready,
                "initial_import_safe_for_read_cutover": self.initial_import_safe_for_read_cutover,
                "projection_catch_up_enabled": self.projection_catch_up_enabled,
            },
            "blocker_codes": self.blocker_codes,
            "production_status": self.production_status.json(),
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{CutoverControls, CutoverControlsReport};
    use crate::{
        bounded_read_evidence::GraphMode,
        runtime_status::{EmbeddedProductionStatus, EmbeddedRuntimeStatus},
    };
    use hawdb_storage::projection::SearchProjectionChangefeedStatus;

    #[test]
    fn active_import_fails_closed_without_catch_up_evidence() {
        let status = EmbeddedProductionStatus::from_runtime(
            GraphMode::WritableCutover,
            false,
            EmbeddedRuntimeStatus {
                protocol: "test".to_string(),
                graph_commit_epoch: 0,
                changefeed: SearchProjectionChangefeedStatus {
                    graph_commit_epoch: 0,
                    resume_floor_commit_epoch: 0,
                    oldest_retained_mutation_id: None,
                    newest_retained_mutation_id: None,
                    first_rebuild_required_mutation_id: None,
                    retained_mutation_count: 0,
                    retained_bytes: 0,
                    max_retained_bytes: None,
                    restart_recoverable: true,
                },
                projection_freshness: None,
                file_descriptors: None,
            },
            None,
        );
        let report = CutoverControlsReport::from_production_status(
            CutoverControls::hawdb_shadow(),
            status,
            false,
        );

        assert!(!report.ready);
        assert!(!report.initial_import_safe_for_read_cutover);
        assert!(report
            .blocker_codes
            .contains(&"initial_import_active_blocks_read_cutover".to_string()));
    }
}
