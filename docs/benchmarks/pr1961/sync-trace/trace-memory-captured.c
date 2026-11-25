// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <string.h>
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
#define MAX_EVENTS 65536
#define MAX_PATH_BYTES (16 * 1024 * 1024)
struct TraceEvent {
    uint64_t started, ended;
    int fd, command, result, error, directory;
    size_t path_offset;
};
static struct TraceEvent *events;
static char *paths;
static size_t event_count, path_bytes;
static pthread_mutex_t trace_lock = PTHREAD_MUTEX_INITIALIZER;
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
    events = malloc(MAX_EVENTS * sizeof(*events));
    paths = malloc(MAX_PATH_BYTES);
    if (!events || !paths) abort();
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
        size_t length = strlen(path) + 1;
        pthread_mutex_lock(&trace_lock);
        if (event_count == MAX_EVENTS || path_bytes + length > MAX_PATH_BYTES) abort();
        events[event_count++] = (struct TraceEvent){started, ended, fd, command,
            result, saved_errno, directory, path_bytes};
        memcpy(paths + path_bytes, path, length);
        path_bytes += length;
        pthread_mutex_unlock(&trace_lock);
    }
    errno = saved_errno;
    return result;
}
// No trace writes occur between measured barriers. The controller ends all
// timed steps before letting either worker exit and flush these records.
__attribute__((destructor)) static void flush_trace(void) {
    if (trace_fd < 0) return;
    for (size_t i = 0; i < event_count; ++i) {
        const struct TraceEvent *e = &events[i];
        char line[PATH_MAX + 256];
        int length = snprintf(line, sizeof(line), "%llu\t%llu\t%d\t%d\t%d\t%d\t%d\t%s\n",
            (unsigned long long)e->started, (unsigned long long)e->ended,
            e->fd, e->command, e->result, e->error, e->directory, paths + e->path_offset);
        if (length < 0 || (size_t)length >= sizeof(line)) abort();
        ssize_t written = write(trace_fd, line, (size_t)length);
        if (written != length) abort();
    }
    if (close(trace_fd) != 0) abort();
    free(events);
    free(paths);
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
