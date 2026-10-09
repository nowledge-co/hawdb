// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0
//go:build windows

package main

import "syscall"

func loadProfileLibrary(path string) (uintptr, error) {
	handle, err := syscall.LoadLibrary(path)
	return uintptr(handle), err
}
func lookupProfileSymbol(handle uintptr, name string) (uintptr, error) {
	return syscall.GetProcAddress(syscall.Handle(handle), name)
}
