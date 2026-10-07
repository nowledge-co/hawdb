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

//! Private checkpoint reads use the captured file identity and bounded waves.
//! They bypass serving caches. Full output allocation still needs a byte ledger.

use super::*;
use crate::background::{CheckpointWorkContext, CheckpointWorkError};

#[derive(Debug)]
pub(crate) enum CheckpointRangeReadError {
    Read(SegmentReadError),
    Work(CheckpointWorkError),
}

impl From<SegmentReadError> for CheckpointRangeReadError {
    fn from(error: SegmentReadError) -> Self {
        Self::Read(error)
    }
}

impl From<CheckpointWorkError> for CheckpointRangeReadError {
    fn from(error: CheckpointWorkError) -> Self {
        Self::Work(error)
    }
}

impl Display for CheckpointRangeReadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => Display::fmt(error, formatter),
            Self::Work(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for CheckpointRangeReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read(error) => Some(error),
            Self::Work(error) => Some(error),
        }
    }
}

impl FileSegmentRangeReader {
    pub(crate) fn checkpoint_range_with_work_context(
        &self,
        range: &SegmentReadRange,
        work: &CheckpointWorkContext,
    ) -> Result<Vec<u8>, CheckpointRangeReadError> {
        work.checkpoint()?;
        let artifact =
            self.artifacts
                .get(&range.artifact_id)
                .ok_or(SegmentReadError::ArtifactNotFound {
                    artifact_id: range.artifact_id,
                })?;
        let length =
            usize::try_from(range.length.get()).map_err(|_| SegmentReadError::RangeTooLarge {
                artifact_id: range.artifact_id,
                length: range.length.get(),
            })?;
        let registration;
        let shared;
        {
            let unit = work.start_unit()?;
            let _wave = work.io_wave()?;
            registration = artifact
                .registration()
                .map_err(|source| range_io_error(range, source))?;
            shared = registration
                .binding
                .as_ref()
                .map(|binding| registration.handles.get(binding, &registration.context))
                .transpose()
                .map_err(|source| range_io_error(range, source))?;
            if shared.is_none() && artifact.file.get().is_none() {
                let opened = crate::file_io::OpenOptions::new()
                    .read(true)
                    .open_with_context(&artifact.path, &registration.context)
                    .map_err(|source| range_io_error(range, source))?;
                let _ = artifact.file.set(opened);
            }
            unit.finish();
        }
        let file = shared.as_ref().map_or_else(
            || {
                artifact
                    .file
                    .get()
                    .expect("checkpoint opened the captured artifact")
            },
            |file| file.as_ref(),
        );
        let mut payload = Vec::new();
        let mut scratch = {
            let unit = work.start_unit()?;
            let scratch = vec![0; length.min(64 * 1024)];
            unit.finish();
            scratch
        };
        for start in (0..length).step_by(64 * 1024) {
            let end = start.saturating_add(64 * 1024).min(length);
            let unit = work.start_unit()?;
            let _wave = work.io_wave()?;
            let offset = range.offset.checked_add(start as u64).ok_or_else(|| {
                range_io_error(
                    range,
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "segment range offset overflows u64",
                    ),
                )
            })?;
            read_exact_at(file, &mut scratch[..end - start], offset)
                .map_err(|source| range_io_error(range, source))?;
            payload.extend_from_slice(&scratch[..end - start]);
            unit.finish();
        }
        if let Some(expected) = range.content_digest {
            let actual = crate::cache::ContentDigest(work.checksum(&payload)?);
            if actual != expected {
                if let Some(cache) = &self.cache {
                    cache.record_digest_mismatch();
                }
                return Err(SegmentReadError::DigestMismatch {
                    artifact_id: range.artifact_id,
                    segment_id: range.segment_ids.first().copied().unwrap_or_default(),
                }
                .into());
            }
        }
        work.checkpoint()?;
        Ok(payload)
    }
}
