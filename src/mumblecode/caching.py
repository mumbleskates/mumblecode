# coding=utf-8
import base64
from datetime import datetime, timezone, timedelta
from hashlib import sha3_256
import json
import os
from queue import Queue, Empty, Full
import re
import sqlite3
from threading import Thread, Event, Semaphore
from time import monotonic
import zlib

from lockfile import LockFile


_max_age_finder = re.compile(r"(^|\s)max-age=(\d+)", re.IGNORECASE)


def now():
    return datetime.now(timezone.utc)


class FileCache(object):  # stolen & modified from cachecontrol
    def __init__(self, directory, forever=False, filemode=0o0600,
                 dirmode=0o0700, ):
        self.directory = directory
        self.forever = forever
        self.filemode = filemode
        self.dirmode = dirmode

    @staticmethod
    def _secure_open_write(filename, fmode):  # stolen & modified from cachecontrol
        # We only want to write to this file, so open it in write only mode
        flags = os.O_WRONLY

        # os.O_CREAT | os.O_EXCL will fail if the file already exists, so we only
        #  will open *new* files.
        # We specify this because we want to ensure that the mode we pass is the
        # mode of the file.
        flags |= os.O_CREAT | os.O_EXCL

        # Do not follow symlinks to prevent someone from making a symlink that
        # we follow and insecurely open a cache file.
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW

        # On Windows we'll mark this file as binary
        if hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY

        # Before we open our file, we want to delete any existing file that is
        # there
        try:
            os.remove(filename)
        except (IOError, OSError):
            # The file must not exist already, so we can just skip ahead to opening
            pass

        # Open our file, the use of os.O_CREAT | os.O_EXCL will ensure that if a
        # race condition happens between the os.remove and this line, that an
        # error will be raised. Because we utilize a lockfile this should only
        # happen if someone is attempting to attack us.
        fd = os.open(filename, flags, fmode)
        try:
            return os.fdopen(fd, "wb")
        except:
            # An error occurred wrapping our FD in a file object
            os.close(fd)
            raise

    @staticmethod
    def encode(x):
        return sha3_256(x.encode()).hexdigest()

    def hash_to_filepath(self, name):
        # NOTE: This method should not change as some may depend on it.
        #       See: https://github.com/ionrock/cachecontrol/issues/63
        hashed = self.encode(name)
        parts = [hashed[0:3], hashed[3:6], hashed + ".cache"]
        return os.path.join(self.directory, *parts)

    def get(self, key):
        path = self.hash_to_filepath(key)
        if not os.path.exists(path):
            return None

        with open(path, 'rb') as fh:
            return fh.read()

    def set(self, key, value):
        path = self.hash_to_filepath(key)

        # Make sure the directory exists
        try:
            os.makedirs(os.path.dirname(path), self.dirmode)
        except (IOError, OSError):
            pass

        with LockFile(path) as lock:
            # Write our actual file
            with FileCache._secure_open_write(lock.path, self.filemode) as fh:
                fh.write(value)

    def delete(self, key):
        path = self.hash_to_filepath(key)
        if not self.forever:
            os.remove(path)


class SQLCache(object):
    """
    Maintains a worker thread with an active cursor in a sqlite database that commits periodically.
    Functions as a bare-bones key-value store on the front end (text to bytes). Rather more performant
    than committing every interaction, and does not require anything to be installed or set up.
    """

    def __init__(self, filepath, worker_keepalive=2.0, commit_spacing=2.0):
        self._path = os.path.abspath(filepath)
        self._worker_keepalive = worker_keepalive
        self._commit_spacing = commit_spacing
        self._work_queue = Queue(maxsize=64)
        self._worker_sem = Semaphore()
        self._worker = None

        # ensure path for our file is created
        path, filename = os.path.split(self._path)
        if not os.path.exists(path):
            os.makedirs(path)

    def _handoff_work(self, action):
        """Add job to queue and wake worker if necesary"""
        self._work_queue.put(action)
        # start a thread if one is not already running
        if self._worker_sem.acquire(blocking=False):
            self._worker = _SQLStoreThread(
                self._path,
                self._work_queue,
                self._worker_keepalive,
                self._commit_spacing,
                self._worker_sem
            )

    def get(self, key):
        done = Event()
        box = []

        def act(cur):
            result = cur.execute("SELECT val FROM bucket WHERE key = ?", (key,))
            try:
                # unbox the value from the row tuple
                box.append(next(result)[0])
            except StopIteration:
                # no value stored for that key
                box.append(None)
            done.set()

        self._handoff_work(act)
        done.wait()
        return box.pop()

    def set(self, key, value):
        def act(cur):
            cur.execute("REPLACE INTO bucket (key, val) VALUES (?, ?)", (key, value))
        self._handoff_work(act)

    def delete(self, key):
        def act(cur):
            cur.execute("DELETE FROM bucket WHERE key = ?", (key,))
        self._handoff_work(act)

    def close(self):
        # signal worker thread to shut down
        if self._worker:
            self._worker.join(0)

    def __del__(self):
        self.close()


class _SQLStoreThread(Thread):
    _create_sql = """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS bucket
        (
          key TEXT PRIMARY KEY,
          val BLOB NOT NULL
        );
    """

    def __init__(self, path, queue, keepalive, commit_spacing, semaphore):
        super().__init__()
        self.path = path
        self.queue = queue
        self.keepalive = keepalive  # time before we close the thread after last commit
        self.commit_spacing = commit_spacing  # maximum time after a value is set before we will commit
        self.semaphore = semaphore
        self.stopped = Event()
        self.start()

    def run(self):
        try:
            conn = sqlite3.Connection(self.path)
            conn.executescript(self._create_sql)
            cur = conn.cursor()
            first_uncommitted = None
            print('SQLCache thread starting for "{}"'.format(self.path))
            time_now = monotonic()
            while not self.stopped.is_set():
                try:
                    if first_uncommitted is None:
                        timeout = self.keepalive
                    else:
                        timeout = self.commit_spacing - (time_now - first_uncommitted)
                    action = self.queue.get(timeout=timeout)
                except Empty:  # timed out
                    if first_uncommitted is None:
                        break  # close this thread if there's nothing to do
                    time_now = monotonic()
                else:  # got an action
                    action(cur)
                    time_now = monotonic()
                    if first_uncommitted is None:
                        first_uncommitted = time_now

                if first_uncommitted is not None and time_now - first_uncommitted >= self.commit_spacing:
                    # it's been too long since we committed, make a commit
                    conn.commit()
                    first_uncommitted = None

            # loop has ended, close transaction if needed
            if conn.in_transaction:
                conn.commit()
            conn.close()
            print('SQLCache thread for "{}" shutting down'.format(self.path))
        finally:
            self.semaphore.release()  # allow another thread to start

    def join(self, timeout=None):
        # shut down work thread
        # no longer wait for more items to come into the queue; finish as soon as it's empty
        self.keepalive = 0
        # push a NOP through the work queue to wake the thread if it's sleeping
        if self.queue.empty():
            try:
                self.queue.put_nowait(lambda x: None)
            except Full:  # gotta be safe
                pass
        super().join(timeout)


class Response(object):
    def __init__(
            self,
            date=None,
            expiry=None,
            status=None,
            headers=None,
            encoding='',
            content=b'',
            transform=(lambda x: None),
            from_cache=False,
    ):
        self.date = date
        self.expiry = expiry
        self.status = status
        self.headers = headers
        self.encoding = encoding
        self.content = content
        self.transformed = transform(self)
        self.from_cache = from_cache

    @property
    def text(self):
        return self.content.decode(self.encoding)


class CacheWrapper(object):
    """
    Wrap a requests session and provides access through it, buffered by a cache (that provides get, set, and delete
    by key) and controlled by a heuristic that determines the longevity of the caching.

    The objects provided
    are very similar to requests.Response objects and in many cases should function as drop-in replacements.
    """
    # TODO implement additional REST verbs

    def __init__(self, session, cache, heuristic, transform=None, limiter=None, max_inflight=0):
        """
        :param session: requests session to use

        :param cache: cache to use

        :param heuristic: function that accepts a partially constructed Response object (with only
          `expiry` set to `None`) and returns the number of seconds this data will be fresh for.

        :param transform: function that accepts a partially constructed Response object (with `expiry` and
          `transformed` still set to `None`) and returns any object to represent this data, which may be used
          to determine the result's lifetime

        :param limiter: This object is called once every time the network is accessed. Any returned data is discarded.

        """
        self.session = session
        self.cache = cache
        self.heuristic = heuristic
        self.transform = transform or (lambda x: None)
        self.limiter = limiter or (lambda: None)
        if max_inflight > 0:
            self.inflight = Semaphore(max_inflight)
        else:
            self.inflight = None

    @staticmethod
    def _serialize(key, date, expiry, status, headers, encoding, data):
        return zlib.compress(
            json.dumps([
                key,
                int(date.timestamp()),
                int(expiry.timestamp()),
                status,
                headers,
                encoding,
                base64.b64encode(data).decode('ascii')
            ]
            ).encode('utf8'))

    @staticmethod
    def _deserialize(key, data):
        """Return a tuple of (date, expiry, encoding, data), or None if the key does not match"""
        # noinspection PyBroadException
        try:
            load_key, date, expiry, status, headers, encoding, data = json.loads(zlib.decompress(data).decode('utf-8'))
            if load_key != key:
                return None
            date = datetime.fromtimestamp(date, timezone.utc)
            expiry = datetime.fromtimestamp(expiry, timezone.utc)
            # headers = headers
            data = base64.b64decode(data)
            return date, expiry, status, headers, encoding, data
        except:
            return None

    def get(self, url, expired_ok=False, **kwargs):
        """Call the session's get object with these parameters, or retrieves from cache"""
        key = url
        date = None
        expiry = None
        status = None
        data = None
        headers = None
        encoding = None
        cached = None

        # attempt to fetch from cache
        if self.cache:
            cached = self.cache.get(key)
            if cached:
                cached = CacheWrapper._deserialize(key, cached)
                if cached:  # cached would be None if the key is a mismatch
                    date, expiry, status, headers, encoding, data = cached
                    if not expired_ok and expiry < now():
                        cached = None
                        self.cache.delete(key)

        if not cached:  # not fetched from cache
            self.limiter()
            if self.inflight:
                with self.inflight:
                    response = self.session.get(url, **kwargs)
                    data = response.content
            else:
                response = self.session.get(url, **kwargs)
                data = response.content

            date = now()
            status = response.status_code
            headers = {k.lower(): v for k, v in response.headers.items()}
            encoding = response.encoding or response.apparent_encoding

        result = Response(
            date=date,
            expiry=expiry,
            status=status,
            headers=headers,
            encoding=encoding,
            content=data,
            transform=self.transform,
            from_cache=bool(cached),
        )

        if self.cache and not cached:  # construct an expiry and possibly cache if we just fetched this
            if data is not None:
                lifetime = self.heuristic(result)
                result.expiry = expiry = date + timedelta(seconds=lifetime)

                # cache if appropriate
                if status == 200 and lifetime > 0:
                    self.cache.set(key, CacheWrapper._serialize(key, date, expiry, status, headers, encoding, data))

        return result


def header_max_age_heuristic(response):
    # noinspection PyBroadException
    try:
        return int(_max_age_finder.search(response.headers['cache-control']).group(2))
    except:
        return 0
