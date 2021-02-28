# -*- coding: utf-8 -*-

import errno
import logging
import os
import time

logger = logging.getLogger(__name__)


class FileLockException(Exception):
    pass


class FileLock(object):
    """A file locking mechanism that has context-manager support so
    you can use it in a with statement. This should be relatively cross
    compatible as it doesn't rely on msvcrt or fcntl for the locking.
    """

    def __init__(self, folder, filename, timeout_sec=300, retry_delay_sec=0.1):
        self.lockfile = os.path.join(folder, "{}.lock".format(filename))
        self.fd = None
        self.is_locked = False
        self.timeout_sec = timeout_sec
        self.retry_delay_sec = retry_delay_sec

    def acquire(self):
        """Acquire the lock, if possible. If the lock is in use, it check again
        every `wait` seconds. It does this until it either gets the lock or
        exceeds `timeout` number of seconds, in which case it throws
        an exception.
        """
        start_time = time.time()
        while True:
            try:
                # For details on flags: https://man7.org/linux/man-pages/man2/open.2.html
                self.fd = os.open(self.lockfile, os.O_CREAT | os.O_RDWR | os.O_EXCL)
                self.is_locked = True
                break
            except OSError as e:
                if e.errno not in (errno.EEXIST, errno.EACCES):
                    raise
                if (time.time() - start_time) >= self.timeout_sec:
                    raise FileLockException("Timeout occurred for lockfile '%s'" % self.lockfile)

                logger.debug("Waiting for another worker. Retry after '%s' sec", self.retry_delay_sec)
                time.sleep(self.retry_delay_sec)

    def release(self):
        """Get rid of the lock by deleting the lockfile.
        When working in a `with` statement, this gets automatically called at the end.
        """
        os.close(self.fd)
        os.unlink(self.lockfile)
        self.is_locked = False

    def __enter__(self):
        """Activated when used in the with statement.
        Should automatically acquire a lock to be used in the with block.
        """
        if not self.is_locked:
            self.acquire()
            logger.debug("Worker '%s' lock acquire '%s'", os.getpid(), self.lockfile)

        return self

    def __exit__(self, *args):
        if self.is_locked:
            self.release()
            logger.debug("Worker '%s' lock release '%s'", os.getpid(), self.lockfile)
