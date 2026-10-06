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
use crate::append_table::checkpoint::work_error;
use crate::file_io::OpenOptions;

pub(super) fn write_artifact(
    directory: &Path,
    file_name: &str,
    bytes: &[u8],
    work: Option<&CheckpointWorkContext>,
) -> Result<(), AppendTableError> {
    let Some(work) = work else {
        return write_durable_artifact(directory, file_name, bytes);
    };
    let destination = directory.join(file_name);
    let temporary = temporary_path(&destination);
    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(durability("create append candidate"))?;
    // Own cleanup only after exclusive creation succeeds. An earlier
    // interrupted candidate's temporary artifact remains evidence.
    let mut cleanup = TemporaryArtifact(Some(temporary.clone()));
    drop(wave);
    unit.finish();
    for block in bytes.chunks(64 * 1024) {
        let unit = work.start_unit().map_err(work_error)?;
        let wave = work.io_wave().map_err(work_error)?;
        file.write_all(block)
            .map_err(durability("write append candidate"))?;
        drop(wave);
        unit.finish();
    }
    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    file.sync_all()
        .map_err(durability("synchronize append candidate"))?;
    drop(file);
    drop(wave);
    unit.finish();
    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    durable_replace_file(&temporary, &destination)
        .map_err(durability("publish append candidate"))?;
    cleanup.0 = None;
    drop(wave);
    unit.finish();
    work.checkpoint().map_err(work_error)
}

struct TemporaryArtifact(Option<PathBuf>);

impl Drop for TemporaryArtifact {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod tests;
