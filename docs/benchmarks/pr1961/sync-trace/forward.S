// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0
.text
.p2align 2
.globl _trace_fcntl
_trace_fcntl:
    cmp w1, #51
    b.eq Ltrace_sync
    cmp w1, #85
    b.eq Ltrace_sync
    b _fcntl

Ltrace_sync:
    b _trace_sync_command
