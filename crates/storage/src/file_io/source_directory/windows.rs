// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::ffi::c_void;
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
use std::os::windows::io::{AsRawHandle, FromRawHandle};

#[repr(C)]
struct UnicodeString {
    length: u16,
    maximum_length: u16,
    buffer: *mut u16,
}
#[repr(C)]
struct ObjectAttributes {
    length: u32,
    root_directory: *mut c_void,
    object_name: *mut UnicodeString,
    attributes: u32,
    security_descriptor: *mut c_void,
    security_quality_of_service: *mut c_void,
}
#[repr(C)]
struct IoStatusBlock {
    status_or_pointer: usize,
    information: usize,
}

#[link(name = "ntdll")]
unsafe extern "system" {
    #[link_name = "NtCreateFile"]
    fn nt_create_file(
        file_handle: *mut *mut c_void,
        desired_access: u32,
        object_attributes: *mut ObjectAttributes,
        io_status: *mut IoStatusBlock,
        allocation_size: *const i64,
        file_attributes: u32,
        share_access: u32,
        create_disposition: u32,
        create_options: u32,
        ea_buffer: *const c_void,
        ea_length: u32,
    ) -> i32;
    #[link_name = "NtQueryDirectoryFile"]
    fn nt_query_directory(
        file: *mut c_void,
        event: *mut c_void,
        apc_routine: *mut c_void,
        apc_context: *mut c_void,
        status: *mut IoStatusBlock,
        information: *mut c_void,
        length: u32,
        class: u32,
        single_entry: u8,
        name: *const UnicodeString,
        restart: u8,
    ) -> i32;
    #[link_name = "RtlNtStatusToDosError"]
    fn status_to_error(status: i32) -> u32;
    #[link_name = "NtSetInformationFile"]
    fn nt_set_information_file(
        file: *mut c_void,
        io_status: *mut IoStatusBlock,
        information: *const c_void,
        bytes: u32,
        class: i32,
    ) -> i32;
}
#[link(name = "kernel32")]
unsafe extern "system" {
    #[link_name = "SetFileInformationByHandle"]
    fn set_file_information(
        file: *mut c_void,
        class: i32,
        information: *const c_void,
        bytes: u32,
    ) -> i32;
}

const SYNCHRONIZE: u32 = 0x0010_0000;
const READ_ATTRIBUTES: u32 = 0x80;
const READ_DATA: u32 = 1;
const DELETE: u32 = 0x0001_0000;
const SHARE_ALL: u32 = 7;
const NON_DIRECTORY: u32 = 0x40;
const SYNCHRONOUS_NONALERT: u32 = 0x20;
const OPEN_REPARSE_POINT: u32 = 0x0020_0000;
const ATTRIBUTE_REPARSE_POINT: u32 = 0x400;

pub(super) fn link_child_new(
    source: &std::fs::File,
    source_name: ChildName<'_>,
    expected: &std::fs::File,
    target: &std::fs::File,
    target_name: ChildName<'_>,
    context: &FileOpenContext,
) -> io::Result<()> {
    #[repr(C)]
    struct FileLinkInformation {
        replace_if_exists: u8,
        root_directory: *mut c_void,
        file_name_length: u32,
        file_name: [u16; 256],
    }
    // Modern Windows hard-link creation requires FILE_WRITE_ATTRIBUTES.
    // Open through the captured source directory and compare the fresh handle
    // before the first target effect; no saved path or reparse traversal.
    let _permit = context.acquire(DescriptorKind::Transient)?;
    let input = relative_open(source, source_name, 0x100, false)?;
    if crate::native_file_identity::of_file(&input)?
        != crate::native_file_identity::of_file(expected)?
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "immutable link source binding changed",
        ));
    }
    let mut information = FileLinkInformation {
        replace_if_exists: 0,
        root_directory: target.as_raw_handle(),
        file_name_length: u32::try_from(target_name.0.len() * 2)
            .expect("bounded component fits u32"),
        file_name: [0; 256],
    };
    for (slot, byte) in information.file_name.iter_mut().zip(target_name.0.bytes()) {
        *slot = u16::from(byte);
    }
    let mut status = IoStatusBlock {
        status_or_pointer: 0,
        information: 0,
    };
    // SAFETY: FILE_LINK_INFORMATION is repr(C), aligned and fully bounded by
    // its supplied buffer. Both handles remain live through this synchronous
    // NtSetInformationFile(FileLinkInformation=11). Replacement is disabled.
    let result = unsafe {
        nt_set_information_file(
            input.as_raw_handle(),
            &mut status,
            (&raw const information).cast(),
            u32::try_from(std::mem::size_of::<FileLinkInformation>())
                .expect("bounded link information fits u32"),
            11,
        )
    };
    if result < 0 {
        Err(io::Error::from_raw_os_error(
            unsafe { status_to_error(result) } as i32,
        ))
    } else {
        Ok(())
    }
}

pub(super) fn create_directory(root: &std::fs::File, name: ChildName<'_>) -> io::Result<()> {
    let mut encoded = [0_u16; 256];
    for (slot, byte) in encoded.iter_mut().zip(name.0.bytes()) {
        *slot = u16::from(byte);
    }
    // FILE_CREATE (2) + FILE_DIRECTORY_FILE (1): never adopt an existing entry.
    // FILE_LIST_DIRECTORY shares bit 1 with FILE_READ_DATA for regular files.
    // No fallible metadata query may hide a completed create from the recorder.
    let file = relative_open_encoded_raw(root, &mut encoded[..name.0.len()], 1, 2, 1)?;
    drop(file);
    Ok(())
}

fn component_units(name: DirectoryComponent<'_>) -> ([u16; 255], usize) {
    use std::os::windows::ffi::OsStrExt;
    let mut units = [0; 255];
    let mut length = 0;
    for unit in name.0.encode_wide() {
        units[length] = unit;
        length += 1;
    }
    (units, length)
}

pub(super) fn create_directory_component(
    root: &std::fs::File,
    name: DirectoryComponent<'_>,
) -> io::Result<()> {
    let (mut units, length) = component_units(name);
    // Exact native component, held RootDirectory, exclusive FILE_CREATE.
    // Keep successful creation observable before any fallible metadata query.
    drop(relative_open_encoded_raw(
        root,
        &mut units[..length],
        1,
        2,
        1,
    )?);
    Ok(())
}

pub(super) fn open_directory_component(
    root: &std::fs::File,
    name: DirectoryComponent<'_>,
) -> io::Result<std::fs::File> {
    let (mut units, length) = component_units(name);
    relative_open_encoded(root, &mut units[..length], READ_DATA, 1, 1)
}

pub(super) fn create_control_candidate(
    root: &std::fs::File,
    name: ChildName<'_>,
) -> io::Result<std::fs::File> {
    let mut encoded = [0_u16; 256];
    for (slot, byte) in encoded.iter_mut().zip(name.0.bytes()) {
        *slot = u16::from(byte);
    }
    // FILE_CREATE (2), not OPEN_IF: no existing entry may become writable.
    // GENERIC_WRITE supports FlushFileBuffers; DELETE supports held-file rename.
    relative_open_encoded_raw(
        root,
        &mut encoded[..name.0.len()],
        READ_DATA | 0x4000_0000 | DELETE,
        2,
        NON_DIRECTORY,
    )
}

pub(super) fn replace_control(
    root: &std::fs::File,
    file: &std::fs::File,
    _source: ChildName<'_>,
    target: ChildName<'_>,
) -> io::Result<()> {
    #[repr(C)]
    struct RenameInfo {
        flags: u32,
        root: *mut c_void,
        length: u32,
        name: [u16; 256],
    }
    let mut info = RenameInfo {
        // REPLACE_IF_EXISTS | POSIX_SEMANTICS: held old head readers retain
        // their inode. RootDirectory binds the destination to this capture.
        flags: 3,
        root: root.as_raw_handle(),
        length: u32::try_from(target.0.len() * 2).expect("bounded component"),
        name: [0; 256],
    };
    for (slot, byte) in info.name.iter_mut().zip(target.0.bytes()) {
        *slot = u16::from(byte);
    }
    let mut status = IoStatusBlock {
        status_or_pointer: 0,
        information: 0,
    };
    // Use the NT interface directly: the Win32 FileRenameInfoEx wrapper returned
    // ERROR_INVALID_PARAMETER for this non-null RootDirectory on native CI.
    // SAFETY: NT class 65 is FileRenameInformationEx; repr(C) matches
    // FILE_RENAME_INFORMATION, including platform handle alignment. The entire
    // bounded buffer, live DELETE-capable candidate, captured directory and
    // IO_STATUS_BLOCK outlive the synchronous call. FileNameLength counts only
    // the component's UTF-16 bytes; trailing zero capacity is not part of it.
    let result = unsafe {
        nt_set_information_file(
            file.as_raw_handle(),
            &mut status,
            std::ptr::from_ref(&info).cast(),
            u32::try_from(std::mem::size_of::<RenameInfo>()).expect("bounded rename buffer"),
            65,
        )
    };
    if result < 0 {
        // SAFETY: converts the returned NTSTATUS without dereferencing pointers.
        Err(io::Error::from_raw_os_error(
            unsafe { status_to_error(result) } as i32,
        ))
    } else {
        Ok(())
    }
}

pub(super) fn open_root(path: &Path) -> io::Result<std::fs::File> {
    const BACKUP_SEMANTICS: u32 = 0x0200_0000;
    std::fs::OpenOptions::new()
        .read(true)
        .share_mode(SHARE_ALL)
        .custom_flags(BACKUP_SEMANTICS)
        .open(path)
}

pub(super) fn open_wal_append_child(
    root: &std::fs::File,
    name: ChildName<'_>,
) -> io::Result<std::fs::File> {
    // FILE_GENERIC_WRITE without FILE_WRITE_DATA preserves kernel append-only
    // semantics after reads/seeks. Add FILE_READ_DATA for prefix/cursor replay.
    // Resizing requires a separately charged original-object ReOpenFile handle.
    relative_open(root, name, READ_DATA | 0x0012_0114, false)
}

pub(super) fn open_mutable_child(
    root: &std::fs::File,
    name: ChildName<'_>,
) -> io::Result<std::fs::File> {
    // Existing regular child; GENERIC_WRITE also authorizes repair/set_len.
    relative_open(root, name, READ_DATA | 0x4000_0000, false)
}

fn relative_open(
    root: &std::fs::File,
    name: ChildName<'_>,
    access: u32,
    create: bool,
) -> io::Result<std::fs::File> {
    relative_open_kind(root, name, access, create, NON_DIRECTORY)
}

pub(super) fn open_directory(
    root: &std::fs::File,
    name: ChildName<'_>,
) -> io::Result<std::fs::File> {
    // FILE_DIRECTORY_FILE; the same native root-relative, no-reparse open.
    relative_open_kind(root, name, READ_DATA, false, 1)
}

pub(super) fn sync_directory(root: &std::fs::File, context: &FileOpenContext) -> io::Result<()> {
    // FlushFileBuffers requires write authority. Upgrade through the held
    // object itself, without a pathname, and count the temporary before open.
    let _permit = context.acquire(DescriptorKind::Transient)?;
    let file = relative_open_kind(root, ChildName(""), READ_DATA | 0x4000_0000, false, 1)?;
    if crate::native_file_identity::of_file(&file)? != crate::native_file_identity::of_file(root)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "directory sync identity changed",
        ));
    }
    file.sync_all()
}

fn relative_open_kind(
    root: &std::fs::File,
    name: ChildName<'_>,
    access: u32,
    create: bool,
    kind: u32,
) -> io::Result<std::fs::File> {
    let mut encoded = [0_u16; 256];
    for (slot, byte) in encoded.iter_mut().zip(name.0.bytes()) {
        *slot = u16::from(byte);
    }
    relative_open_encoded(
        root,
        &mut encoded[..name.0.len()],
        access,
        if create { 3 } else { 1 },
        kind,
    )
}

fn relative_open_encoded(
    root: &std::fs::File,
    encoded: &mut [u16],
    access: u32,
    disposition: u32,
    kind: u32,
) -> io::Result<std::fs::File> {
    let file = relative_open_encoded_raw(root, encoded, access, disposition, kind)?;
    let metadata = file.metadata()?;
    if metadata.file_attributes() & ATTRIBUTE_REPARSE_POINT != 0
        || if kind == NON_DIRECTORY {
            !metadata.is_file()
        } else if kind == 1 {
            !metadata.is_dir()
        } else {
            !metadata.is_file() && !metadata.is_dir()
        }
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "source child is not a regular non-reparse file",
        ));
    }
    Ok(file)
}

fn relative_open_encoded_raw(
    root: &std::fs::File,
    encoded: &mut [u16],
    access: u32,
    disposition: u32,
    kind: u32,
) -> io::Result<std::fs::File> {
    let length = u16::try_from(encoded.len() * 2).expect("bounded name fits native scratch");
    let mut unicode = UnicodeString {
        length,
        maximum_length: length,
        buffer: encoded.as_mut_ptr(),
    };
    let mut attributes = ObjectAttributes {
        length: u32::try_from(std::mem::size_of::<ObjectAttributes>())
            .expect("native struct fits u32"),
        root_directory: root.as_raw_handle(),
        object_name: &mut unicode,
        // Exact component lookup; no Win32 drive/stream/path normalization.
        attributes: 0,
        security_descriptor: std::ptr::null_mut(),
        security_quality_of_service: std::ptr::null_mut(),
    };
    let mut status = IoStatusBlock {
        status_or_pointer: 0,
        information: 0,
    };
    let mut handle = std::ptr::null_mut();
    // SAFETY: all repr(C) buffers match winternl.h and outlive the synchronous
    // call. RootDirectory is borrowed live. A validated single native component
    // cannot escape it; FILE_OPEN_IF is used only for mutable lock controls,
    // neither disposition truncates existing files, and reparse targets
    // are not followed. No callback, security descriptor or EA buffer is passed.
    let result = unsafe {
        nt_create_file(
            &mut handle,
            access | READ_ATTRIBUTES | SYNCHRONIZE,
            &mut attributes,
            &mut status,
            std::ptr::null(),
            0,
            SHARE_ALL,
            disposition,
            kind | SYNCHRONOUS_NONALERT | OPEN_REPARSE_POINT,
            std::ptr::null(),
            0,
        )
    };
    if result < 0 {
        // SAFETY: pure documented NTSTATUS-to-Win32 error conversion.
        return Err(io::Error::from_raw_os_error(
            unsafe { status_to_error(result) } as i32,
        ));
    }
    if handle.is_null() || handle == std::ptr::without_provenance_mut(usize::MAX) {
        return Err(io::Error::other(
            "native source open returned an invalid handle",
        ));
    }
    // SAFETY: successful NtCreateFile transferred one fresh handle; this File
    // exclusively closes it, including validation and error paths below.
    Ok(unsafe { std::fs::File::from_raw_handle(handle) })
}

pub(super) fn open_child(root: &std::fs::File, name: ChildName<'_>) -> io::Result<std::fs::File> {
    relative_open(root, name, READ_DATA, false)
}

pub(super) fn remove_child(
    root: &std::fs::File,
    name: ChildName<'_>,
    context: &FileOpenContext,
) -> io::Result<()> {
    remove_entry(root, name, context, NON_DIRECTORY)
}

pub(super) fn remove_directory(
    root: &std::fs::File,
    name: ChildName<'_>,
    context: &FileOpenContext,
) -> io::Result<()> {
    // FILE_DIRECTORY_FILE; the kernel also rejects a nonempty directory.
    remove_entry(root, name, context, 1)
}

fn remove_entry(
    root: &std::fs::File,
    name: ChildName<'_>,
    context: &FileOpenContext,
    kind: u32,
) -> io::Result<()> {
    // Windows deletes by a handle opened relative to RootDirectory. Reserve
    // this extra handle before opening it, and close before returning capacity.
    let _permit = context.acquire(DescriptorKind::Transient)?;
    let file = relative_open_kind(root, name, DELETE, false, kind)?;
    // Remove the name when this DELETE handle closes, even while an immutable
    // reader/cache retains another share-delete handle. Legacy disposition only
    // marks delete-pending until all handles close, so a successful GC would
    // leave the old dependency name visible and prevent safe name recreation.
    // Do not bypass read-only attributes or retry through the original pathname.
    const FILE_DISPOSITION_INFO_EX: i32 = 21;
    const FILE_DISPOSITION_DELETE: u32 = 1;
    const FILE_DISPOSITION_POSIX_SEMANTICS: u32 = 2;
    let disposition = FILE_DISPOSITION_DELETE | FILE_DISPOSITION_POSIX_SEMANTICS;
    // SAFETY: FileDispositionInfoEx takes one ULONG flags field (four bytes).
    // The DELETE-capable relative handle and buffer outlive the synchronous call.
    let success = unsafe {
        set_file_information(
            file.as_raw_handle(),
            FILE_DISPOSITION_INFO_EX,
            std::ptr::from_ref(&disposition).cast(),
            4,
        )
    };
    if success == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(super) fn open_lock_file(
    root: &std::fs::File,
    name: ChildName<'_>,
    mode: SourceDirectoryLockMode,
) -> io::Result<std::fs::File> {
    const WRITE_DATA: u32 = 2;
    match mode {
        SourceDirectoryLockMode::ReadExisting => relative_open(root, name, READ_DATA, false),
        SourceDirectoryLockMode::ReadWriteCreate => {
            relative_open(root, name, READ_DATA | WRITE_DATA, true)
        }
    }
}

pub(super) fn visit_children(
    root: &std::fs::File,
    visitor: &mut dyn FnMut(&str) -> io::Result<()>,
) -> io::Result<()> {
    visit_native_units(root, &mut |units| {
        let mut encoded = [0_u8; 255];
        for (slot, unit) in encoded.iter_mut().zip(units) {
            if *unit > 127 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "source directory contains a non-ASCII name",
                ));
            }
            *slot = *unit as u8;
        }
        let name = std::str::from_utf8(&encoded[..units.len()]).expect("ASCII directory name");
        visitor(name)
    })
}

#[cfg(any(test, feature = "test-support"))]
pub(super) fn visit_names(
    root: &std::fs::File,
    visitor: &mut dyn FnMut(&std::ffi::OsStr) -> io::Result<()>,
) -> io::Result<()> {
    use std::os::windows::ffi::OsStringExt;
    visit_native_units(root, &mut |units| {
        visitor(&std::ffi::OsString::from_wide(units))
    })
}

fn visit_native_units(
    root: &std::fs::File,
    visitor: &mut dyn FnMut(&[u16]) -> io::Result<()>,
) -> io::Result<()> {
    // A trusted empty NT name reopens the held object itself through
    // RootDirectory. FILE_DIRECTORY_FILE selects a directory and creates an
    // independent cursor. Public component validation continues to reject
    // empty names. ReOpenFile rejects directories on native Windows.
    let cursor = relative_open_kind(root, ChildName(""), READ_DATA, false, 1)
        .map_err(|error| report_cursor_error("reopen held directory", error))?;
    let cursor_identity = crate::native_file_identity::of_file(&cursor)
        .map_err(|error| report_cursor_error("query cursor identity", error))?;
    let root_identity = crate::native_file_identity::of_file(root)
        .map_err(|error| report_cursor_error("query held directory identity", error))?;
    if cursor_identity != root_identity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "directory cursor identity differs from captured root",
        ));
    }
    let mut scratch = [0_u64; 512];
    let mut restart = 1;
    loop {
        let mut status = IoStatusBlock {
            status_or_pointer: 0,
            information: 0,
        };
        // SAFETY: synchronous handle, no APC/event, aligned 4096-byte output
        // buffer and matching IO_STATUS_BLOCK. FileNamesInformation is class
        // 12; ReturnSingleEntry limits parsing to one complete record.
        let result = unsafe {
            nt_query_directory(
                cursor.as_raw_handle(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut status,
                scratch.as_mut_ptr().cast(),
                4096,
                12,
                1,
                std::ptr::null(),
                restart,
            )
        };
        restart = 0;
        if result == 0x8000_0006_u32 as i32 {
            return Ok(());
        } // STATUS_NO_MORE_FILES
        if result != 0 {
            // SAFETY: pure NTSTATUS translation; overflow/pending are errors,
            // never a successfully truncated enumeration.
            return Err(report_cursor_error(
                "query directory entries",
                io::Error::from_raw_os_error(unsafe { status_to_error(result) } as i32),
            ));
        }
        // SAFETY: fixed initialized scratch, borrowed only until next query.
        let bytes = unsafe { std::slice::from_raw_parts(scratch.as_ptr().cast::<u8>(), 4096) };
        let used = status.information;
        if !(12..=bytes.len()).contains(&used) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "incomplete native directory record",
            ));
        }
        let next = u32::from_ne_bytes(bytes[0..4].try_into().unwrap());
        let length = u32::from_ne_bytes(bytes[8..12].try_into().unwrap()) as usize;
        if next != 0 || !length.is_multiple_of(2) || length > used - 12 || length / 2 > 255 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported native directory record",
            ));
        }
        let mut name = [0_u16; 255];
        for (slot, unit) in name.iter_mut().zip(bytes[12..12 + length].chunks_exact(2)) {
            *slot = u16::from_ne_bytes([unit[0], unit[1]]);
        }
        let name = &name[..length / 2];
        if !matches!(name, [46] | [46, 46]) {
            visitor(name)?;
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(super) fn open_recorded_child(
    root: &std::fs::File,
    name: &std::ffi::OsStr,
) -> io::Result<std::fs::File> {
    use std::os::windows::ffi::OsStrExt;
    let mut encoded = [0_u16; 255];
    let mut length = 0;
    for unit in name.encode_wide() {
        if length == encoded.len() || matches!(unit, 0 | 47 | 58 | 92) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid native child name",
            ));
        }
        encoded[length] = unit;
        length += 1;
    }
    if length == 0 || matches!(&encoded[..length], [46] | [46, 46]) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid native child name",
        ));
    }
    // Neither DIRECTORY_FILE nor NON_DIRECTORY_FILE: open an existing entry
    // without following reparses, then require a regular file or directory.
    relative_open_encoded(root, &mut encoded[..length], READ_DATA, 1, 0)
}

#[cfg(any(test, feature = "test-support"))]
pub(super) fn reopen_directory(root: &std::fs::File) -> io::Result<std::fs::File> {
    let file = relative_open_kind(root, ChildName(""), READ_DATA, false, 1)?;
    if crate::native_file_identity::of_file(&file)? != crate::native_file_identity::of_file(root)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "reopened directory identity changed",
        ));
    }
    Ok(file)
}

#[cfg(any(test, feature = "test-support"))]
pub(super) fn open_recorded_lock(
    root: &std::fs::File,
    name: ChildName<'_>,
    create_new: bool,
) -> io::Result<std::fs::File> {
    let mut encoded = [0_u16; 255];
    for (slot, byte) in encoded.iter_mut().zip(name.0.bytes()) {
        *slot = u16::from(byte);
    }
    // FILE_CREATE refuses a foreign existing name; FILE_OPEN cannot recreate a
    // missing recorded entry. Neither disposition truncates an existing file.
    relative_open_encoded(
        root,
        &mut encoded[..name.0.len()],
        READ_DATA | 2,
        if create_new { 2 } else { 1 },
        NON_DIRECTORY,
    )
}

fn report_cursor_error(stage: &str, error: io::Error) -> io::Error {
    // Preserve raw native errors for descriptor-exhaustion classification.
    // Test binaries identify the failing API without changing access or retry.
    #[cfg(test)]
    eprintln!(
        "source-directory cursor {stage}: kind={:?} raw={:?} error={error}",
        error.kind(),
        error.raw_os_error()
    );
    #[cfg(not(test))]
    let _ = stage;
    error
}

pub(super) fn matches_child_file(
    root: &std::fs::File,
    name: ChildName<'_>,
    file: &std::fs::File,
    context: &FileOpenContext,
) -> io::Result<bool> {
    let _permit = context.acquire(DescriptorKind::Transient)?;
    // FILE_READ_ATTRIBUTES through the held directory, no reparse traversal.
    let named = relative_open(root, name, 0x80, false)?;
    Ok(
        crate::native_file_identity::of_file(file)?
            == crate::native_file_identity::of_file(&named)?,
    )
}
