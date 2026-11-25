// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <string.h>
#include <unistd.h>
int main(int argc, char **argv) {
    assert(argc == 2);
    int fd = open(argv[1], O_RDWR | O_CREAT | O_EXCL, 0600);
    assert(fd >= 0);
    assert(fcntl(fd, F_GETFD) == 0);
    assert(fcntl(fd, F_SETFD, FD_CLOEXEC) == 0);
    assert(fcntl(fd, F_GETFD) == FD_CLOEXEC);
    char path[PATH_MAX];
    assert(fcntl(fd, F_GETPATH, path) == 0 && strlen(path) > 0);
    assert(write(fd, "probe", 5) == 5);
    assert(fcntl(fd, F_FULLFSYNC) == 0);
    assert(fsync(fd) == 0);
    errno = 0;
    assert(fcntl(-1, F_FULLFSYNC) == -1 && errno == EBADF);
    assert(close(fd) == 0);
    return 0;
}
