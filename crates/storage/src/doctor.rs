//! WAL-tail repair protocol models shared by embedded storage facades.

use serde::{Deserialize, Serialize};

pub const WAL_DOCTOR_REPAIR_PROTOCOL: &str = "skein-wal-doctor-repair-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalDoctorOptions {
    pub max_wal_bytes: Option<u64>,
    pub max_record_bytes: Option<usize>,
    pub max_batch_operations: Option<usize>,
}

impl Default for WalDoctorOptions {
    fn default() -> Self {
        Self {
            max_wal_bytes: Some(crate::DEFAULT_MAX_WAL_REPLAY_BYTES),
            max_record_bytes: Some(crate::DEFAULT_MAX_WAL_RECORD_BYTES),
            max_batch_operations: Some(crate::DEFAULT_MAX_WAL_BATCH_OPERATIONS),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalTailRepairReason {
    IncompleteFinalRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalTailRepairPlan {
    pub protocol: String,
    pub plan_id: String,
    pub wal_generation: u64,
    pub wal_replay_start_lsn: u64,
    pub next_lsn_after_repair: u64,
    pub manifest_len: u64,
    pub manifest_crc32c: u64,
    pub manifest_sha256: String,
    pub original_wal_len: u64,
    pub original_wal_crc32c: u64,
    pub original_wal_sha256: String,
    pub retained_wal_len: u64,
    pub retained_wal_crc32c: u64,
    pub retained_wal_sha256: String,
    pub discarded_wal_tail_bytes: u64,
    pub reason: WalTailRepairReason,
    pub data_loss_possible: bool,
}

impl WalTailRepairPlan {
    pub fn acknowledge_potential_data_loss(&self) -> WalRepairAcknowledgement {
        WalRepairAcknowledgement {
            protocol: WAL_DOCTOR_REPAIR_PROTOCOL.to_string(),
            plan_id: self.plan_id.clone(),
            accepts_potential_data_loss: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRepairAcknowledgement {
    protocol: String,
    plan_id: String,
    accepts_potential_data_loss: bool,
}

impl WalRepairAcknowledgement {
    #[doc(hidden)]
    pub fn with_parts(
        protocol: String,
        plan_id: String,
        accepts_potential_data_loss: bool,
    ) -> Self {
        Self {
            protocol,
            plan_id,
            accepts_potential_data_loss,
        }
    }

    #[doc(hidden)]
    pub fn accepts(&self, plan: &WalTailRepairPlan) -> bool {
        self.protocol == WAL_DOCTOR_REPAIR_PROTOCOL
            && self.plan_id == plan.plan_id
            && self.accepts_potential_data_loss
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalTailRepairReport {
    pub protocol: String,
    pub plan_id: String,
    pub wal_generation: u64,
    pub retained_wal_len: u64,
    pub discarded_wal_tail_bytes: u64,
    pub next_lsn_after_repair: u64,
    pub quarantine_file: String,
    pub repair_record_file: String,
    pub resumed_interrupted_repair: bool,
}
