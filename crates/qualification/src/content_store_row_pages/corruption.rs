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

use super::evidence::{
    message_point_options, message_point_parameters, require_one_message, MESSAGE_POINT_SQL,
};
use super::ContentStoreCorruptionQualificationReport;
use crate::evidence_digest::rows_sha256;
use hawdb::{Database, DatabaseConfig, DurabilityPolicy, HawDBError, Result};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static PROBE_ID: AtomicU64 = AtomicU64::new(0);

pub(super) fn qualify_content_store_corruption(
    database: &mut Database,
    database_path: &Path,
    database_config: &DatabaseConfig,
    content_message_id: &str,
) -> Result<ContentStoreCorruptionQualificationReport> {
    let point_parameters = message_point_parameters(content_message_id);
    let point_options = message_point_options();
    let source_before = database.query_sql_with_params_options(
        MESSAGE_POINT_SQL,
        &point_parameters,
        point_options,
    )?;
    require_one_message(&source_before.rows, content_message_id, "corruption probe")?;
    let source_sha256 = rows_sha256(&source_before.rows);

    let (backup_path, restored_path) = probe_paths(database_path)?;
    let mut cleanup = ProbeCleanup::new([backup_path.clone(), restored_path.clone()]);
    let backup = database.backup_to(&backup_path)?;
    let restored = Database::restore_backup(&backup_path, &restored_path)?;
    if backup.generation != restored.generation {
        return Err(HawDBError::Execution(format!(
            "content-store corruption probe restored generation {} from backup generation {}",
            restored.generation, backup.generation
        )));
    }

    let (artifact_generation, artifact_name, artifact_path, artifact_len) =
        latest_non_empty_row_page_artifact(&restored_path)?;
    let bit_flip_offset = artifact_len - 1;
    bit_flip(&artifact_path, bit_flip_offset)?;

    let mut corrupted = Database::open_with_durability_and_config(
        &restored_path,
        DurabilityPolicy::SyncOnEveryWrite,
        database_config.clone(),
    )?;
    let scrub_error = match corrupted.scrub_storage() {
        Ok(_) => {
            return Err(HawDBError::Execution(
                "content-store corruption probe scrub accepted a bit-flipped row page".to_string(),
            ));
        }
        Err(error) => error,
    };
    let scrub_message = scrub_error.to_string();
    if !scrub_message.contains("checksum mismatch")
        && !scrub_message.contains("CRC32C mismatch")
        && !scrub_message.contains("SHA-256 mismatch")
    {
        return Err(HawDBError::Execution(format!(
            "content-store corruption probe returned an unexpected scrub error: {scrub_error}"
        )));
    }
    if !corrupted.storage_handle_poisoned() {
        return Err(HawDBError::Execution(
            "content-store corruption probe did not poison the damaged handle".to_string(),
        ));
    }
    let service_error = match corrupted.query_sql_with_params_options(
        MESSAGE_POINT_SQL,
        &point_parameters,
        point_options,
    ) {
        Ok(_) => {
            return Err(HawDBError::Execution(
                "content-store corruption probe served SQL after integrity failure".to_string(),
            ));
        }
        Err(error) => error,
    };
    if !service_error.to_string().contains("close and reopen") {
        return Err(HawDBError::Execution(format!(
            "content-store corruption probe expected fail-closed service, got: {service_error}"
        )));
    }
    drop(corrupted);

    let source_after = database.query_sql_with_params_options(
        MESSAGE_POINT_SQL,
        &point_parameters,
        point_options,
    )?;
    require_one_message(&source_after.rows, content_message_id, "corruption probe")?;
    if rows_sha256(&source_after.rows) != source_sha256 {
        return Err(HawDBError::Execution(
            "content-store corruption probe changed its healthy source database".to_string(),
        ));
    }
    cleanup.remove()?;

    Ok(ContentStoreCorruptionQualificationReport {
        artifact_name,
        artifact_generation,
        artifact_bytes: artifact_len,
        bit_flip_offset,
        scrub_rejected: true,
        damaged_handle_poisoned: true,
        post_failure_sql_rejected: true,
        source_preserved: true,
        source_row_sha256: source_sha256,
    })
}

fn bit_flip(path: &Path, offset: u64) -> Result<()> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte)?;
    byte[0] ^= 0xff;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&byte)?;
    file.sync_all()?;
    Ok(())
}

fn latest_non_empty_row_page_artifact(path: &Path) -> Result<(u64, String, PathBuf, u64)> {
    let mut selected = None;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(generation) = name
            .strip_prefix("relational-row-pages-")
            .and_then(|name| name.strip_suffix(".pages.hawdb"))
            .and_then(|generation| generation.parse::<u64>().ok())
        else {
            continue;
        };
        let artifact_len = entry.metadata()?.len();
        if artifact_len == 0 {
            continue;
        }
        if selected
            .as_ref()
            .is_none_or(|(selected_generation, _, _, _)| generation > *selected_generation)
        {
            selected = Some((generation, name, entry.path(), artifact_len));
        }
    }
    selected.ok_or_else(|| {
        HawDBError::Execution(
            "content-store corruption probe found no non-empty row-page artifact in the restored canonical closure"
                .to_string(),
        )
    })
}

fn probe_paths(database_path: &Path) -> Result<(PathBuf, PathBuf)> {
    let parent = database_path.parent().unwrap_or_else(|| Path::new("."));
    let name = database_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            HawDBError::Semantic(
                "content-store corruption probe requires a UTF-8 database directory name"
                    .to_string(),
            )
        })?;
    let id = PROBE_ID.fetch_add(1, Ordering::Relaxed);
    let suffix = format!("{}-{id}", std::process::id());
    let backup = parent.join(format!(".{name}.corruption-backup-{suffix}"));
    let restored = parent.join(format!(".{name}.corruption-restored-{suffix}"));
    for path in [&backup, &restored] {
        if path.exists() {
            return Err(HawDBError::Execution(format!(
                "content-store corruption probe path already exists: {}",
                path.display()
            )));
        }
    }
    Ok((backup, restored))
}

struct ProbeCleanup {
    paths: [PathBuf; 2],
    armed: bool,
}

impl ProbeCleanup {
    fn new(paths: [PathBuf; 2]) -> Self {
        Self { paths, armed: true }
    }

    fn remove(&mut self) -> Result<()> {
        for path in &self.paths {
            match std::fs::remove_dir_all(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        self.armed = false;
        Ok(())
    }
}

impl Drop for ProbeCleanup {
    fn drop(&mut self) {
        if self.armed {
            for path in &self.paths {
                let _ = std::fs::remove_dir_all(path);
            }
        }
    }
}
