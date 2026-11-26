// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#if !defined(__APPLE__) || !defined(__aarch64__)
#error This diagnostic requires macOS arm64.
#endif
_Static_assert(F_FULLFSYNC == 51, "assembly command mismatch");
_Static_assert(F_BARRIERFSYNC == 85, "assembly command mismatch");
static int trace_fd = -1;
static uint64_t now_ns(void) {
    struct timespec value;
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0) abort();
    return (uint64_t)value.tv_sec * 1000000000ULL + (uint64_t)value.tv_nsec;
}
__attribute__((constructor)) static void initialize_trace(void) {
    const char *root = getenv("HAWDB_SYNC_TRACE_DIR");
    if (!root) return;
    char path[PATH_MAX];
    int length = snprintf(path, sizeof(path), "%s/%d.tsv", root, getpid());
    if (length < 0 || (size_t)length >= sizeof(path)) abort();
    trace_fd = open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (trace_fd < 0) abort();
}
static int trace_sync(int fd, int command) {
    uint64_t started = now_ns();
    int result = command == -1 ? fsync(fd) : fcntl(fd, command);
    int saved_errno = errno;
    uint64_t ended = now_ns();
    if (trace_fd >= 0) {
        char path[PATH_MAX] = "?";
        if (fcntl(fd, F_GETPATH, path) < 0) path[0] = '?', path[1] = 0;
        struct stat metadata;
        int directory = fstat(fd, &metadata) == 0 && S_ISDIR(metadata.st_mode);
        char line[PATH_MAX + 256];
        int length = snprintf(line, sizeof(line), "%llu\t%llu\t%d\t%d\t%d\t%d\t%d\t%s\n",
            (unsigned long long)started, (unsigned long long)ended,
            fd, command, result, saved_errno, directory, path);
        if (length < 0 || (size_t)length >= sizeof(line)) abort();
        ssize_t written = write(trace_fd, line, (size_t)length);
        if (written != length) abort();
    }
    errno = saved_errno;
    return result;
}
int trace_sync_command(int fd, int command) { return trace_sync(fd, command); }
static int trace_fsync(int fd) { return trace_sync(fd, -1); }
extern int trace_fcntl(int fd, int command, ...);
// Mach-O interposition keeps references in this image bound to the originals.
// The assembly tail-forwarder preserves Darwin's stack-based variadic ABI.
__attribute__((used, section("__DATA,__interpose"))) static const struct {
    const void *replacement;
    const void *original;
} hooks[] = {
    {(const void *)trace_fcntl, (const void *)fcntl},
    {(const void *)trace_fsync, (const void *)fsync},
};
