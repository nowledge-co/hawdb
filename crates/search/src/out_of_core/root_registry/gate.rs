// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Process-lifetime registry exclusion must not retain a query's first-use heap.
//! The Apple gate is immovable static storage and parks through the OS API.

#[cfg(not(target_vendor = "apple"))]
pub(super) use std::sync::{Mutex as Gate, MutexGuard as Guard};

#[cfg(not(target_vendor = "apple"))]
pub(super) fn lock<T>(gate: &'static Gate<T>) -> Guard<'static, T> {
    gate.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(target_vendor = "apple")]
pub(super) use apple::{lock, Gate, Guard};

#[cfg(target_vendor = "apple")]
mod apple {
    use std::cell::UnsafeCell;
    use std::marker::PhantomData;
    use std::ops::{Deref, DerefMut};

    // os/lock.h: os_unfair_lock_s contains one uint32_t; INIT is { 0 }.
    // Call exported public APIs so OS tooling observes synchronization.
    unsafe extern "C" {
        fn os_unfair_lock_lock(lock: *mut u32);
        fn os_unfair_lock_unlock(lock: *mut u32);
    }

    pub(in super::super) struct Gate<T> {
        native: UnsafeCell<u32>,
        value: UnsafeCell<T>,
    }

    // SAFETY: access to value requires the unique native lock. T can cross
    // threads only when Send; no references escape their guard's lifetime.
    unsafe impl<T: Send> Sync for Gate<T> {}

    impl<T> Gate<T> {
        pub(in super::super) const fn new(value: T) -> Self {
            Self {
                native: UnsafeCell::new(0),
                value: UnsafeCell::new(value),
            }
        }
    }

    pub(in super::super) struct Guard<'a, T> {
        gate: &'a Gate<T>,
        // Native ownership must be released by the acquiring thread.
        _same_thread: PhantomData<*mut ()>,
    }

    pub(in super::super) fn lock<T>(gate: &'static Gate<T>) -> Guard<'static, T> {
        // SAFETY: 'static pins the initialized, aligned native word; exclusive
        // nonrecursive acquisition protects value until this thread unlocks.
        unsafe { os_unfair_lock_lock(gate.native.get()) };
        Guard {
            gate,
            _same_thread: PhantomData,
        }
    }

    impl<T> Deref for Guard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            // SAFETY: this guard holds the native lock for the reference's life.
            unsafe { &*self.gate.value.get() }
        }
    }

    impl<T> DerefMut for Guard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            // SAFETY: the lock and exclusive guard borrow exclude other access.
            unsafe { &mut *self.gate.value.get() }
        }
    }

    impl<T> Drop for Guard<'_, T> {
        fn drop(&mut self) {
            // SAFETY: this !Send guard owns the lock on the acquiring thread.
            unsafe { os_unfair_lock_unlock(self.gate.native.get()) };
        }
    }
}
