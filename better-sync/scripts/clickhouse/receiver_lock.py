"""Host-local, crash-released exclusion for a ClickHouse receiver authority.

This does not coordinate different OS users or hosts. Distributed deployments
must supply additional coordination; it is not safe to share an authority among
multiple receiver hosts merely because this lock exists.
"""
import contextlib
import fcntl
import hashlib
import os
from pathlib import Path
import re
import stat


@contextlib.contextmanager
def authority_lock(authority,root=None):
    if not isinstance(authority,str) or not re.fullmatch(r'[A-Za-z0-9_-]{16,128}',authority):
        raise ValueError('invalid receiver authority')
    root=Path(root) if root is not None else Path.home()/'.local/state/better-sync/receiver-locks'
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    info=root.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode & 0o077:
        raise ValueError('receiver lock directory must be private and owned by this user')
    name=hashlib.sha256(authority.encode()).hexdigest()+'.lock'
    fd=os.open(root/name,os.O_CREAT|os.O_RDWR|os.O_CLOEXEC|os.O_NOFOLLOW,0o600)
    try:
        info=os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode & 0o077:
            raise ValueError('receiver lock file must be private and owned by this user')
        try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise RuntimeError('receiver authority already locked on this host; refusing another writer') from None
        yield
    finally:
        os.close(fd)  # kernel releases on crash; never unlink a foreign lock file
