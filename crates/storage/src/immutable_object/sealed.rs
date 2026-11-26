// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Publication of write-once checkpoint producer files owned by GraphStore.

use super::*;
use crate::background::CheckpointWorkUnit;
use crate::immutable_files::ImmutableFileBinding;
use hawdb_core::RuntimeIoWavePermit;

#[cfg(test)]
thread_local! {
    #[cfg(unix)]
    pub(super) static BEFORE_DIRECT_LINK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    pub(super) static NEXT_LINK_ERROR: std::cell::Cell<Option<io::ErrorKind>> = const { std::cell::Cell::new(None) };
}

fn link_source(source: &Path, destination: &Path) -> io::Result<()> {
    #[cfg(test)]
    if let Some(kind) = NEXT_LINK_ERROR.with(|slot| slot.take()) {
        return Err(io::Error::from(kind));
    }
    fs::hard_link(source, destination)
}

impl ImmutableObjectStore {
    /// Only GraphStore's sealed checkpoint producers may use this entry point.
    /// All writable descriptors must be closed, and interrupted preparation
    /// must consume or unlink the generation rather than rewrite it in place.
    /// A published producer alias is mounted for COW before returning. Public
    /// caller-owned sources always use `publish_file_with_work`, which copies.
    /// The owning GraphStore must retain its registered project descriptor
    /// domain through all later producer reads/writes so COW bindings stay live.
    pub(crate) fn publish_sealed_file_with_work(
        &mut self,
        reference: ObjectReference,
        source: &Path,
        work: BlockWork<'_>,
    ) -> Result<PublishOutcome, ImmutableObjectError> {
        if self.poisoned {
            return Err(ImmutableObjectError::PublisherPoisoned);
        }
        if reference.format_version == 0 {
            return Err(ImmutableObjectError::InvalidFormatVersion);
        }
        if reference.byte_length > self.streamed_limit(reference.kind) {
            return Err(ImmutableObjectError::ObjectTooLarge {
                length: reference.byte_length,
                limit: self.streamed_limit(reference.kind),
            });
        }
        let destination = self.prepare_kind_directory(reference)?;
        let context = map_io(
            "resolve sealed checkpoint source",
            crate::file_descriptors::context_for_path(source),
        )?;
        let handles = context.state.immutable_handles();
        // A writable open detaches bound aliases. Reuse must precede that open
        // so unchanged row/overflow/append files keep sharing their old inode.
        let (result, mount) = if destination.exists() {
            (
                self.validate_existing(reference, None, &destination, work)?,
                true,
            )
        } else if map_io("inspect sealed checkpoint binding", handles.binding(source))?.is_some() {
            (self.publish_file_with_work(reference, source, work)?, true)
        } else if let Some(result) =
            self.publish_fresh_sealed_file(reference, source, &destination, work)?
        {
            (result, true)
        } else {
            // Sharing is unavailable, but the copied object has been verified.
            // Keep the independent producer inode and bind managed reads/COW.
            (self.publish_file_with_work(reference, source, work)?, false)
        };
        let binding = ImmutableFileBinding {
            reference,
            object_path: destination,
        };
        if !mount {
            map_io(
                "bind copied checkpoint source",
                handles.bind(source, binding),
            )?;
            return Ok(result);
        }
        let mut control = MountWork::new(work);
        let mounted = handles.mount_controlled(source, binding.clone(), &context, &mut control);
        if mounted.is_ok() {
            control.finish_block();
        }
        if let Some(work) = work.0 {
            work.checkpoint().map_err(block_work_error)?;
        }
        match mounted {
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::CrossesDevices | io::ErrorKind::Unsupported
                ) =>
            {
                // The object was already authenticated by publication/reuse.
                // An independent alias still supports managed reads and COW.
                map_io(
                    "bind copied checkpoint source",
                    handles.bind(source, binding),
                )?;
            }
            result => map_io("mount sealed checkpoint source", result)?,
        }
        Ok(result)
    }

    fn publish_fresh_sealed_file(
        &mut self,
        reference: ObjectReference,
        source: &Path,
        destination: &Path,
        work: BlockWork<'_>,
    ) -> Result<Option<PublishOutcome>, ImmutableObjectError> {
        #[cfg(unix)]
        {
            self.publish_direct_sealed_file(reference, source, destination, work)
        }
        #[cfg(not(unix))]
        {
            let Some((staging, stamp)) = self.stage_sealed_file(reference, source, work)? else {
                return Ok(None);
            };
            self.install_staged(reference, None, Some(stamp), &staging, destination, work)
                .map(Some)
        }
    }

    #[cfg(unix)]
    fn publish_direct_sealed_file(
        &mut self,
        reference: ObjectReference,
        source: &Path,
        destination: &Path,
        work: BlockWork<'_>,
    ) -> Result<Option<PublishOutcome>, ImmutableObjectError> {
        use std::os::unix::fs::MetadataExt;

        let Some((mut input, before)) = Self::open_sealed_source(source)? else {
            return Ok(None);
        };
        // Cross-device copies remain the existing single streaming pass.
        // Do not hash a source first when a native link cannot be attempted.
        let source_device = map_io("inspect sealed source device", input.metadata())?.dev();
        let destination_device = map_io(
            "inspect sealed destination device",
            fs::metadata(destination.parent().expect("object kind directory")),
        )?
        .dev();
        if source_device != destination_device {
            return Ok(None);
        }
        Self::verify_sealed_source(&mut input, reference, before, work)?;
        #[cfg(test)]
        BEFORE_DIRECT_LINK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        match link_source(source, destination) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return self
                    .validate_existing(reference, None, destination, work)
                    .map(Some);
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::CrossesDevices | io::ErrorKind::Unsupported
                ) =>
            {
                return Ok(None);
            }
            Err(source) => {
                self.poisoned = true;
                return Err(ImmutableObjectError::PublicationUncertain { source });
            }
        }
        // The held source prevents inode reuse. Never acknowledge a link to a
        // replacement source, even if its bytes, length and timestamps match.
        // On uncertainty retain the final entry as evidence; do not unlink a
        // possibly foreign birth by its saved pathname.
        let unchanged = (|| -> io::Result<bool> {
            let linked = fs::symlink_metadata(destination)?;
            let held = input.metadata()?;
            Ok(linked.is_file()
                && crate::native_file_identity::of_metadata(&linked)
                    == crate::native_file_identity::of_metadata(&held)
                && input.with_native(published::WrittenStamp::of_native)? == before)
        })();
        let validation = unchanged.and_then(|unchanged| {
            if unchanged {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "sealed publication source binding changed",
                ))
            }
        });
        // Match the staged path's descriptor lifetime: the verified link now
        // owns the inode, so the final directory flush needs no source FD.
        drop(input);
        if let Err(source) =
            validation.and_then(|()| sync_publication_directories(destination, None))
        {
            self.poisoned = true;
            return Err(ImmutableObjectError::PublicationUncertain { source });
        }
        published::record(reference, before);
        Ok(Some(PublishOutcome::Published))
    }

    fn open_sealed_source(
        source: &Path,
    ) -> Result<Option<(File, published::WrittenStamp)>, ImmutableObjectError> {
        let metadata = map_io(
            "inspect sealed checkpoint source",
            fs::symlink_metadata(source),
        )?;
        if !metadata.is_file() {
            return Err(ImmutableObjectError::ReferenceMismatch);
        }
        // Windows FlushFileBuffers requires write access. This is a counted
        // native handle to a fresh, unbound, exclusively owned producer file.
        let input = match OpenOptions::new().read(true).write(true).open(source) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => return Ok(None),
            Err(source) => {
                return Err(ImmutableObjectError::Io {
                    operation: "open sealed checkpoint source",
                    source,
                })
            }
        };
        let before = match input.with_native(published::WrittenStamp::of_native) {
            Ok(stamp) => stamp,
            Err(error) if error.kind() == io::ErrorKind::Unsupported => return Ok(None),
            Err(source) => {
                return Err(ImmutableObjectError::Io {
                    operation: "identify sealed checkpoint source",
                    source,
                })
            }
        };
        Ok(Some((input, before)))
    }

    fn verify_sealed_source(
        input: &mut File,
        reference: ObjectReference,
        before: published::WrittenStamp,
        work: BlockWork<'_>,
    ) -> Result<(), ImmutableObjectError> {
        let mut identity = ObjectReferenceWriter::new(
            reference.kind,
            reference.format_version,
            reference.byte_length,
        )?;
        let mut buffer = vec![0; STREAM_BUFFER_BYTES];
        loop {
            let length = work.block(block_work_error, || {
                let length = map_io("read immutable object source", input.read(&mut buffer))?;
                identity
                    .write_all(&buffer[..length])
                    .map_err(|_| ImmutableObjectError::ReferenceMismatch)?;
                Ok(length)
            })?;
            if length == 0 {
                break;
            }
        }
        if identity.finish()? != reference
            || map_io(
                "recheck sealed checkpoint source",
                input.with_native(published::WrittenStamp::of_native),
            )? != before
        {
            return Err(ImmutableObjectError::ReferenceMismatch);
        }
        map_io("sync sealed checkpoint source", input.sync_all())?;
        Ok(())
    }

    #[cfg(not(unix))]
    fn stage_sealed_file(
        &self,
        reference: ObjectReference,
        source: &Path,
        work: BlockWork<'_>,
    ) -> Result<Option<(StagedFile, published::WrittenStamp)>, ImmutableObjectError> {
        let Some((mut input, before)) = Self::open_sealed_source(source)? else {
            return Ok(None);
        };
        let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = self
            .root
            .join(OBJECTS_DIRECTORY)
            .join(STAGING_DIRECTORY)
            .join(format!("{}-{sequence}.stage", std::process::id()));
        match link_source(source, &path) {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::CrossesDevices | io::ErrorKind::Unsupported
                ) =>
            {
                return Ok(None)
            }
            Err(source) => {
                return Err(ImmutableObjectError::Io {
                    operation: "link sealed checkpoint staging file",
                    source,
                })
            }
        }
        let staging = StagedFile(path);
        let staged_metadata = map_io(
            "inspect sealed checkpoint staging entry",
            fs::symlink_metadata(&*staging),
        )?;
        if !staged_metadata.is_file() {
            return Err(ImmutableObjectError::ReferenceMismatch);
        }
        let linked_stamp = map_io(
            "identify sealed checkpoint staging file",
            File::open(&*staging)
                .and_then(|file| file.with_native(published::WrittenStamp::of_native)),
        )?;
        if linked_stamp != before {
            return Err(ImmutableObjectError::ReferenceMismatch);
        }
        Self::verify_sealed_source(&mut input, reference, before, work)?;
        drop(input);
        Ok(Some((staging, before)))
    }
}

struct MountWork<'a> {
    work: BlockWork<'a>,
    unit: Option<CheckpointWorkUnit>,
    wave: Option<Box<dyn RuntimeIoWavePermit>>,
    validation_active: bool,
}

impl<'a> MountWork<'a> {
    fn new(work: BlockWork<'a>) -> Self {
        Self {
            work,
            unit: None,
            wave: None,
            validation_active: false,
        }
    }

    fn finish_block(&mut self) {
        self.wave = None;
        if let Some(unit) = self.unit.take() {
            unit.finish();
        }
    }
}

impl fs::ReadAdmissionControl for MountWork<'_> {
    fn admit_validation(&mut self, _bytes: u64) -> io::Result<()> {
        // Admission announces the full object once; checkpoints bracket each
        // bounded read in ImmutableHandles' 64 KiB verification loop.
        self.validation_active = true;
        Ok(())
    }

    fn checkpoint(&mut self) -> io::Result<()> {
        if let Some(work) = self.work.0 {
            if self.validation_active {
                self.finish_block();
                self.unit = Some(work.start_unit().map_err(io::Error::other)?);
                self.wave = work.io_wave().map_err(io::Error::other)?;
            } else {
                work.checkpoint().map_err(io::Error::other)?;
            }
        }
        Ok(())
    }
}
