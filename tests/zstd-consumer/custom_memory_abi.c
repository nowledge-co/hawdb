/*
 * Copyright 2026 Nowledge
 * SPDX-License-Identifier: Apache-2.0
 */

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

// Mirror the private repr(C) bridge without using Zstd's typedefs.
struct CustomMemory {
    void *(*allocate)(void *, size_t);
    void (*free)(void *, void *);
    void *opaque;
};

_Static_assert(sizeof(struct CustomMemory) == sizeof(ZSTD_customMem), "size");
_Static_assert(_Alignof(struct CustomMemory) == _Alignof(ZSTD_customMem), "alignment");
_Static_assert(offsetof(struct CustomMemory, allocate) == offsetof(ZSTD_customMem, customAlloc), "allocate offset");
_Static_assert(offsetof(struct CustomMemory, free) == offsetof(ZSTD_customMem, customFree), "free offset");
_Static_assert(offsetof(struct CustomMemory, opaque) == offsetof(ZSTD_customMem, opaque), "opaque offset");
_Static_assert(_Generic((ZSTD_allocFunction)0, void *(*)(void *, size_t): 1, default: 0), "allocate signature");
_Static_assert(_Generic((ZSTD_freeFunction)0, void (*)(void *, void *): 1, default: 0), "free signature");
_Static_assert(_Generic(&ZSTD_createCCtx_advanced, ZSTD_CCtx *(*)(ZSTD_customMem): 1, default: 0), "compressor signature");
_Static_assert(_Generic(&ZSTD_createDCtx_advanced, ZSTD_DCtx *(*)(ZSTD_customMem): 1, default: 0), "decoder signature");
