"""在已打开的受管目录中操作子项，保留宿主平台的路径边界。"""

import os
import stat
from contextlib import contextmanager


def is_link(info):
    return stat.S_ISLNK(info.st_mode)


class PosixDirectory:
    def __init__(self, fd):
        self.fd = fd

    @classmethod
    @contextmanager
    def open(cls, path):
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            yield cls(fd)
        finally:
            os.close(fd)

    @contextmanager
    def child(self, name):
        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self.fd)
        try:
            yield PosixDirectory(fd)
        finally:
            os.close(fd)

    def open_file(self, name, flags):
        return os.open(
            name, flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=self.fd
        )

    def list(self):
        return os.listdir(self.fd)

    def stat(self, name=None):
        return (
            os.fstat(self.fd)
            if name is None
            else os.stat(name, dir_fd=self.fd, follow_symlinks=False)
        )

    def mkdir(self, name):
        os.mkdir(name, mode=0o700, dir_fd=self.fd)

    def rename(self, source, target, *, replace=False):
        operation = os.replace if replace else os.rename
        operation(source, target, src_dir_fd=self.fd, dst_dir_fd=self.fd)

    def unlink(self, name):
        os.unlink(name, dir_fd=self.fd)

    def rmdir(self, name):
        os.rmdir(name, dir_fd=self.fd)

    def sync(self):
        os.fsync(self.fd)
