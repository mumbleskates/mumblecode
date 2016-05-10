# coding=utf-8
from queue import Queue
import sqlite3
from threading import Thread, RLock
from weakref import finalize


class CloseableQueue(Queue):
    def __init__(self, maxsize=0):
        super().__init__(maxsize)
        self.mutex = RLock()
        self._closed = False

    def get(self, block=True, timeout=None):
        # This class changes the condition for self.not_empty to be
        #   'not self._qsize() and not self.closed'; so, if the queue
        #   is empty and the queue is stopped, we raise StopIteration
        #   instead of waiting or raising Empty.
        with self.mutex:
            if not self._qsize() and self._closed:
                raise StopIteration
            return super(CloseableQueue, self).get(block, timeout)

    def put(self, item, block=True, timeout=None):
        with self.mutex:
            if self._closed:
                raise ValueError("Queue is closed!")
            super(CloseableQueue, self).put(item, block, timeout)

    def close(self):
        with self.mutex:
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

                finalize(self, self.queue.close)

            def __next__(self):
                try:
                    return self.queue.get()
                except StopIteration:
                    raise

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
