//! Host-neutral cutover controls and fail-closed reduction for Nowledge Mem.

use crate::nowledge_mem_runtime_status::NowledgeMemProductionStatus;
use crate::previous_wrapper_preflight::NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemReadControl {
    Legacy,
    Skein,
}

impl NowledgeMemReadControl {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::Skein => "skein",
        }
    }

    pub const fn selects_skein(self) -> bool {
        matches!(self, Self::Skein)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemWorkControl {
    Disabled,
    Enabled,
}

impl NowledgeMemWorkControl {
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
pub struct NowledgeMemCutoverControls {
    pub graph_reads: NowledgeMemReadControl,
    pub search_reads: NowledgeMemReadControl,
    pub dual_writes: NowledgeMemWorkControl,
    pub initial_import: NowledgeMemWorkControl,
    pub projection_catch_up: NowledgeMemWorkControl,
}

impl NowledgeMemCutoverControls {
    pub const fn legacy() -> Self {
        Self {
            graph_reads: NowledgeMemReadControl::Legacy,
            search_reads: NowledgeMemReadControl::Legacy,
            dual_writes: NowledgeMemWorkControl::Disabled,
            initial_import: NowledgeMemWorkControl::Disabled,
            projection_catch_up: NowledgeMemWorkControl::Disabled,
        }
    }

    pub const fn skein_shadow() -> Self {
        Self {
            graph_reads: NowledgeMemReadControl::Legacy,
            search_reads: NowledgeMemReadControl::Legacy,
            dual_writes: NowledgeMemWorkControl::Enabled,
            initial_import: NowledgeMemWorkControl::Enabled,
            projection_catch_up: NowledgeMemWorkControl::Enabled,
        }
    }

    pub const fn skein_reads() -> Self {
        Self {
            graph_reads: NowledgeMemReadControl::Skein,
            search_reads: NowledgeMemReadControl::Skein,
            dual_writes: NowledgeMemWorkControl::Enabled,
            initial_import: NowledgeMemWorkControl::Disabled,
            projection_catch_up: NowledgeMemWorkControl::Enabled,
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

impl Default for NowledgeMemCutoverControls {
    fn default() -> Self {
        Self::legacy()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemCutoverControlsReport {
    pub protocol: String,
    pub ready: bool,
    pub controls: NowledgeMemCutoverControls,
    pub graph_read_selected_skein: bool,
    pub graph_read_effective: bool,
    pub search_read_selected_skein: bool,
    pub search_read_effective: bool,
    pub dual_writes_enabled: bool,
    pub initial_import_enabled: bool,
    pub initial_import_inactive_for_cutover: bool,
    pub initial_import_cutover_catch_up_ready: bool,
    pub initial_import_safe_for_read_cutover: bool,
    pub projection_catch_up_enabled: bool,
    pub blocker_codes: Vec<String>,
    pub production_status: NowledgeMemProductionStatus,
}

impl NowledgeMemCutoverControlsReport {
    #[doc(hidden)]
    pub fn from_production_status(
        controls: NowledgeMemCutoverControls,
        production_status: NowledgeMemProductionStatus,
        initial_import_cutover_catch_up_ready: bool,
    ) -> Self {
        let graph_read_selected_skein = controls.graph_reads.selects_skein();
        let search_read_selected_skein = controls.search_reads.selects_skein();
        let dual_writes_enabled = controls.dual_writes.enabled();
        let initial_import_enabled = controls.initial_import.enabled();
        let initial_import_inactive_for_cutover = !initial_import_enabled;
        let initial_import_safe_for_read_cutover =
            initial_import_inactive_for_cutover || initial_import_cutover_catch_up_ready;
        let projection_catch_up_enabled = controls.projection_catch_up.enabled();
        let graph_read_effective =
            !graph_read_selected_skein || production_status.graph_skein_cutover_effective;
        let search_read_effective =
            !search_read_selected_skein || production_status.search_skein_cutover_effective;
        let mut blocker_codes = Vec::new();
        if !graph_read_effective {
            blocker_codes.push("graph_read_selected_skein_but_not_effective".to_string());
        }
        if !search_read_effective {
            blocker_codes.push("search_read_selected_skein_but_not_effective".to_string());
        }
        if search_read_selected_skein && !projection_catch_up_enabled {
            blocker_codes
                .push("search_read_selected_skein_without_projection_catch_up".to_string());
        }
        if initial_import_enabled && !dual_writes_enabled {
            blocker_codes.push("initial_import_enabled_without_dual_writes".to_string());
        }
        if !initial_import_safe_for_read_cutover {
            blocker_codes.push("initial_import_active_blocks_read_cutover".to_string());
        }

        Self {
            protocol: NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL.to_string(),
            ready: blocker_codes.is_empty(),
            controls,
            graph_read_selected_skein,
            graph_read_effective,
            search_read_selected_skein,
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
                "read_selected_skein": self.graph_read_selected_skein,
                "read_effective": self.graph_read_effective,
            },
            "search": {
                "read_selected_skein": self.search_read_selected_skein,
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
    use super::{NowledgeMemCutoverControls, NowledgeMemCutoverControlsReport};
    use crate::{
        bounded_read_evidence::NowledgeMemGraphMode,
        nowledge_mem_runtime_status::{NowledgeMemProductionStatus, NowledgeMemRuntimeStatus},
    };
    use skein_storage::SearchProjectionChangefeedStatus;

    #[test]
    fn active_import_fails_closed_without_catch_up_evidence() {
        let status = NowledgeMemProductionStatus::from_runtime(
            NowledgeMemGraphMode::WritableCutover,
            false,
            NowledgeMemRuntimeStatus {
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
            },
            None,
        );
        let report = NowledgeMemCutoverControlsReport::from_production_status(
            NowledgeMemCutoverControls::skein_shadow(),
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
