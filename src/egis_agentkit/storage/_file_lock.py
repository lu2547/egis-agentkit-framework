"""在直接挂载 NAS 的 POSIX 宿主上加文件锁。"""

from contextlib import contextmanager


@contextmanager
def hold_file_lock(fd, *, exclusive):
    # HTTP 模式的 Agent 不加载 POSIX 文件锁模块。
    import fcntl

    fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
    try:
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
