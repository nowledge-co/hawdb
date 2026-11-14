// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::ffi::c_void;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::ptr;

#[repr(C)]
struct SidAndAttributes {
    sid: *mut c_void,
    attributes: u32,
}

#[link(name = "kernel32")]
unsafe extern "system" {
    #[link_name = "GetCurrentProcess"]
    fn get_current_process() -> *mut c_void;
}

#[link(name = "advapi32")]
unsafe extern "system" {
    #[link_name = "OpenProcessToken"]
    fn open_process_token(process: *mut c_void, access: u32, token: *mut *mut c_void) -> i32;
    #[link_name = "CreateWellKnownSid"]
    fn create_well_known_sid(
        kind: i32,
        domain: *const c_void,
        sid: *mut c_void,
        size: *mut u32,
    ) -> i32;
    #[link_name = "CreateRestrictedToken"]
    fn create_restricted_token(
        token: *mut c_void,
        flags: u32,
        disabled_count: u32,
        disabled: *const SidAndAttributes,
        deleted_count: u32,
        deleted: *const c_void,
        restricted_count: u32,
        restricted: *const c_void,
        result: *mut *mut c_void,
    ) -> i32;
    #[link_name = "ImpersonateLoggedOnUser"]
    fn impersonate_logged_on_user(token: *mut c_void) -> i32;
    #[link_name = "RevertToSelf"]
    fn revert_to_self() -> i32;
}

struct NonAdmin {
    _token: OwnedHandle,
    _thread: std::marker::PhantomData<*mut ()>,
}

impl NonAdmin {
    fn enter() -> std::io::Result<Self> {
        let mut token = ptr::null_mut();
        // SAFETY: the pseudo process handle is valid and token is an out pointer.
        if unsafe { open_process_token(get_current_process(), 10, &mut token) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: successful OpenProcessToken returns an owned native handle.
        let token = unsafe { OwnedHandle::from_raw_handle(token) };
        let mut sid = [0_u32; 17];
        let mut sid_bytes = 68;
        // SAFETY: the aligned buffer holds SECURITY_MAX_SID_SIZE bytes. Kind 26
        // identifies the built-in Administrators group without account lookup.
        if unsafe {
            create_well_known_sid(26, ptr::null(), sid.as_mut_ptr().cast(), &mut sid_bytes)
        } == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        let disabled = SidAndAttributes {
            sid: sid.as_mut_ptr().cast(),
            attributes: 0,
        };
        let mut restricted = ptr::null_mut();
        // SAFETY: the live source token and SID outlive the call. Flag 1 removes
        // privileges; the admin SID becomes deny-only. No machine ACL is changed.
        if unsafe {
            create_restricted_token(
                token.as_raw_handle(),
                1,
                1,
                &disabled,
                0,
                ptr::null(),
                0,
                ptr::null(),
                &mut restricted,
            )
        } == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: successful token creation transfers ownership to this guard.
        let restricted = unsafe { OwnedHandle::from_raw_handle(restricted) };
        // SAFETY: impersonation affects only this fresh test thread; the guard
        // restores its original process identity before releasing the token.
        if unsafe { impersonate_logged_on_user(restricted.as_raw_handle()) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self {
            _token: restricted,
            _thread: std::marker::PhantomData,
        })
    }
}

impl Drop for NonAdmin {
    fn drop(&mut self) {
        // SAFETY: this guard belongs to the same thread that entered impersonation.
        assert_ne!(unsafe { revert_to_self() }, 0, "restore the test identity");
    }
}

#[test]
fn namespace_anchor_opens_with_standard_user_permissions() {
    std::thread::spawn(|| {
        let _identity = NonAdmin::enter().unwrap();
        let scratch = Scratch::new();
        // The OS-owned temp directory is the host's already durable boundary.
        let anchor = std::env::temp_dir();
        let root = scratch.0.join("new/graph");
        let project = open(&root, Some(&anchor), true).unwrap();
        assert_eq!(project.metrics().open, 0);
        let search = scratch.0.join("new/search");
        let component =
            ProjectFileDescriptors::acquire_component_with_namespace_anchor(&search, true, &anchor)
                .unwrap();
        assert_eq!(component.metrics().open, 0);
        drop(component);
        assert!(
            ProjectFileDescriptors::acquire_component_with_namespace_anchor(
                &search, false, &anchor,
            )
            .is_ok()
        );
        drop(project);
        assert!(open(&root, Some(&anchor), true).is_ok());
    })
    .join()
    .unwrap();
}
