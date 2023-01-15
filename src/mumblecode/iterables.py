# coding=utf-8
from collections import namedtuple
import io
from heapq import heapify, heappush, heappop
from itertools import chain, count, islice
import sys


def grouper_it(n, iterable):
    """Unflatten an iterable into groups of n elements"""
    it = iter(iterable)
    while True:
        chunk_it = islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield chain((first_el,), chunk_it)


class _MaxHeapItem(namedtuple("MaxHeapItemTuple", "item")):
    def __lt__(self, other):
        return other.item < self.item


def collate(iterables, *, reverse=False):
    """
    Accepts multiple iterables and yields items from them in a roughly sorted
    order.

    Each item from each iterator will appear once. If a unique item x is
    produced by an iterator before a unique item y from that same iterator, x
    will ALWAYS come before y in the resulting stream.

    Identical values are taken first from the earliest ordered iterator.

    >>> list(collate([[1, 2, 11, 12], [4, 5, 6]]))
    [1, 2, 4, 5, 6, 11, 12]
    >>> list(collate([
    ...     [5, 6, 7, 4, 5, 6],
    ...     [6, 1, 2, 3],
    ...     [8, 10, 12],
    ...     [8, 9, 11, 13],
    ... ]))
    [5, 6, 6, 1, 2, 3, 7, 4, 5, 6, 8, 8, 9, 10, 11, 12, 13]
    """
    wrap = _MaxHeapItem if reverse else lambda x: x
    # populate the heap with each iterator's first item, if present
    heap = []
    c = count()
    for feed in iterables:
        it = iter(feed)
        try:
            item = next(it)
        except StopIteration:
            continue
        else:
            heap.append((wrap(item), next(c), item, it))
    heapify(heap)

    while heap:
        _wrapped_item, iter_index, item, top_iter = heappop(heap)
        # advance only the feed we just took the item from
        yield item
        try:
            new_item = next(top_iter)
            heappush(heap, (wrap(new_item), iter_index, new_item, top_iter))
        except StopIteration:
            pass  # iterator was exhausted


def remove_repeats(iterable):
    """
    Returns an iterable that yields the items from the given iterable with
    successive equal items removed.
    """
    last_item = object()
    for item in iterable:
        if item != last_item:
            yield item
        last_item = item


def merge(iterables, *, reverse=False):
    """
    Merges multiple possibly incomplete iterators that may include selections
    or stretches of identical content into a single feed that includes every
    unique value at least once.

    Example usage: A sorted list of unique and comparable items is spread out
    among several smaller lists, which is each created by iterating over the
    original list and only taking certain items by an arbitrary standard. Many
    of the lists are very incomplete, but each item in the original list
    appears one or more times among the smaller lists. Passing all these lists
    into merge() will produce an iterator in the exact order and items of the
    original list, with none repeated.

    Likewise: merge(sorted_iterables) is equivalent to
    iter(sorted(set(chain.from_iterable(sorted_iterables))))

    >>> from random import sample, randint
    >>> original = list(range(1000))
    >>> small_lists = [[] for _ in range(10)]
    >>> for item in original:
    ...     for put_in in sample(range(10), randint(1, 5)):
    ...         small_lists[put_in].append(item)
    >>> original == list(merge(small_lists))
    True

    """
    return remove_repeats(collate(iterables, reverse=reverse))


class IteratorFile(io.TextIOBase):
    """ given an iterator which yields strings,
    return a file like object for reading those strings.
    From github/jsheedy """

    def __init__(self, it):
        super().__init__()
        self._it = iter(it)
        self._f = io.StringIO()

    def read(self, length=sys.maxsize):

        try:
            while self._f.tell() < length:
                self._f.write(next(self._it))

        finally:
            self._f.seek(0)
            data = self._f.read(length)

            # save the remainder for next read
            remainder = self._f.read()
            self._f.seek(0)
            self._f.truncate(0)
            self._f.write(remainder)
            return data

    def readline(self):
        return next(self._it)
