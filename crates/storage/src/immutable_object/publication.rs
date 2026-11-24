// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Namespace barriers owned by one sealed checkpoint closure.

use super::*;

#[derive(Default)]
pub(super) struct NamespaceBatch {
    directories: BTreeSet<PathBuf>,
    records: Vec<(ObjectReference, published::WrittenStamp)>,
}

impl NamespaceBatch {
    pub(super) fn finish(self) -> io::Result<()> {
        for directory in self.directories {
            sync_publication_directory(&directory)?;
        }
        // No process-local publication record may bypass the namespace barrier.
        for (reference, stamp) in self.records {
            published::record(reference, stamp);
        }
        Ok(())
    }
}

pub(super) struct PublicationWork<'work, 'batch> {
    pub(super) block: BlockWork<'work>,
    pub(super) namespace: Option<&'batch mut NamespaceBatch>,
}

impl<'work> From<BlockWork<'work>> for PublicationWork<'work, '_> {
    fn from(block: BlockWork<'work>) -> Self {
        Self {
            block,
            namespace: None,
        }
    }
}

impl PublicationWork<'_, '_> {
    pub(super) fn removed_staging(&mut self, path: &Path) -> io::Result<()> {
        let directory = path.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "staging file has no parent")
        })?;
        if let Some(batch) = &mut self.namespace {
            batch.directories.insert(directory.to_path_buf());
            Ok(())
        } else {
            sync_publication_directory(directory)
        }
    }

    pub(super) fn complete(
        &mut self,
        destination: &Path,
        staging_directory: Option<&Path>,
        written: Option<(ObjectReference, published::WrittenStamp)>,
    ) -> io::Result<()> {
        if let Some(batch) = &mut self.namespace {
            let parent = destination.parent().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "object has no parent directory",
                )
            })?;
            batch.directories.insert(parent.to_path_buf());
            if let Some(directory) = staging_directory {
                batch.directories.insert(directory.to_path_buf());
            }
            batch.records.extend(written);
        } else {
            sync_publication_directories(destination, staging_directory)?;
            if let Some((reference, stamp)) = written {
                published::record(reference, stamp);
            }
        }
        Ok(())
    }
}
