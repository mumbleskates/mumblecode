# coding=utf-8
from queue import Queue, Empty, Full
import sqlite3
from threading import Thread
from time import monotonic


class CloseableQueue(Queue):
    def __init__(self, maxsize=0):
        super().__init__(maxsize)
        self._closed = False

    def get(self, block=True, timeout=None):
        # This class changes the condition for self.not_empty to be
        #   'not self._qsize() and not self.closed'; so, if the queue
        #   is empty and the queue is stopped, we raise StopIteration
        #   instead of waiting or raising Empty.
        with self.not_empty:
            if not self._qsize() and self._closed:
                raise StopIteration

            # copypasta from queue.Queue.get(); mutex is not reentrant, we can't just call it :(
            if not block:
                if not self._qsize():
                    raise Empty
            elif timeout is None:
                while not self._qsize():
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = monotonic() + timeout
                while not self._qsize():
                    remaining = endtime - monotonic()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
            item = self._get()
            self.not_full.notify()
            return item

    def put(self, item, block=True, timeout=None):
        with self.not_full:
            if self._closed:
                raise ValueError("Queue is closed!")

            # copypasted from queue.Queue.put(); mutex is not reentrant, we can't just call it :(
            if self.maxsize > 0:
                if not block:
                    if self._qsize() >= self.maxsize:
                        raise Full
                elif timeout is None:
                    while self._qsize() >= self.maxsize:
                        self.not_full.wait()
                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                else:
                    endtime = monotonic() + timeout
                    while self._qsize() >= self.maxsize:
                        remaining = endtime - monotonic()
                        if remaining <= 0.0:
                            raise Full
                        self.not_full.wait(remaining)
            self._put(item)
            self.unfinished_tasks += 1
            self.not_empty.notify()

    def close(self):
        with self.not_empty:
            self._closed = True
            self.not_empty.notify_all()
            self.not_full.notify_all()


class IterProvider(object):
    """
    Provide a generator to multiple threads.

    Every time this object provides a new iterator, it spawns a worker thread that gets a new iterator
    from the provided generator; the iterator returned can then be used safely by any number of threads.
    Objects provided by this iterator are guaranteed to be passed exactly one time.
    """

    def __init__(self, generator, queue_length=16):
        self.generator = generator
        self.queue_length = queue_length

    def __iter__(self):
        class Yielder(object):
            def __init__(self, generator, queue_length):
                queue = self.queue = CloseableQueue(maxsize=queue_length)

                # must not hold a reference to self to prevent the thread from keeping the iterator alive
                def work():
                    for thing in generator:
                        try:
                            queue.put(thing)
                        except ValueError:
                            # queue was closed by someone else
                            return
                    self.queue.close()

                Thread(target=work).start()

            def __next__(self):
                try:
                    return self.queue.get()
                except StopIteration:
                    raise

            def __del__(self):
                self.queue.close()

            def __iter__(self):
                return self

        return Yielder(self.generator, self.queue_length)


class QueryProvider(IterProvider):
    """Provides a database query selection to multiple threads"""

    def __init__(self, db_path, query, params, queue_length=16):
        def generator():
            for row in sqlite3.Connection(db_path).execute(query, params):
                yield row

        super().__init__(generator, queue_length)
