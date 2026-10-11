// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0
//go:build !windows

package main

import "github.com/ebitengine/purego"

func loadProfileLibrary(path string) (uintptr, error) {
	return purego.Dlopen(path, purego.RTLD_LAZY|purego.RTLD_GLOBAL)
}
func lookupProfileSymbol(handle uintptr, name string) (uintptr, error) {
	return purego.Dlsym(handle, name)
}
