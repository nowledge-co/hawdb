// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Complete read-only verification through the recorder's captured directory.
//! Child namespace exclusion remains a host premise. The native walker charges
//! every held directory/file/cursor and stops before exceeding the image's entry
//! count; failure never publishes a partial snapshot.

use super::{identity, invalid, CrashImage};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

pub(super) fn verify(root: &crate::file_io::PowerLossRoot, image: &CrashImage) -> io::Result<()> {
    let captured_files: BTreeSet<PathBuf> = image.paths().map(Path::to_path_buf).collect();
    let captured_directories: BTreeSet<PathBuf> =
        image.directory_paths().map(Path::to_path_buf).collect();
    let maximum_entries = captured_files
        .len()
        .checked_add(captured_directories.len())
        .and_then(|count| count.checked_sub(1))
        .ok_or_else(|| invalid("captured namespace entry count overflows"))?;
    let mut native_files = BTreeSet::new();
    let mut native_directories = BTreeSet::new();
    let mut native_by_inode: BTreeMap<super::image::InodeId, identity::NativeIdentity> =
        BTreeMap::new();
    let mut inode_by_native = BTreeMap::new();
    root.visit_tree(maximum_entries, |relative, mut file| {
        let metadata = file.metadata()?;
        if metadata.is_dir() {
            native_directories.insert(relative.to_path_buf());
            return Ok(());
        }
        let expected = image
            .bytes(relative)
            .ok_or_else(|| invalid("native file has no captured IO history"))?;
        let identity = identity::of_file(file)?;
        if metadata.len() != expected.len() as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("uncaptured native length at {}", relative.display()),
            ));
        }
        // Read at most one expected chunk at a time. Empty locked lease files
        // must not probe EOF: Windows locks may cover bytes beyond their length.
        let mut scratch = [0_u8; 8192];
        for expected in expected.chunks(scratch.len()) {
            let actual = &mut scratch[..expected.len()];
            file.read_exact(actual)?;
            if actual != expected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("uncaptured native bytes at {}", relative.display()),
                ));
            }
        }
        if (!expected.is_empty() && file.read(&mut scratch[..1])? != 0)
            || file.metadata()?.len() != expected.len() as u64
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("uncaptured native length at {}", relative.display()),
            ));
        }
        let inode = image
            .file_inode(relative)
            .ok_or_else(|| invalid("captured inode is absent"))?;
        if let Some(prior) = inode_by_native.insert(identity, inode)
            && prior != inode
        {
            return Err(invalid(
                "native hard link has separate captured inode identities",
            ));
        }
        if let Some(prior) = native_by_inode.insert(inode, identity)
            && prior != identity
        {
            return Err(invalid(
                "captured hard-link identity differs from native identity",
            ));
        }
        native_files.insert(relative.to_path_buf());
        Ok(())
    })?;
    if native_files != captured_files || native_directories != captured_directories {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
            format!("captured namespace differs from native namespace: missing file {:?}, missing directory {:?}, unexpected directory {:?}",
                captured_files.difference(&native_files).next(),
                captured_directories.difference(&native_directories).next(),
                native_directories.difference(&captured_directories).next())));
    }
    Ok(())
}
