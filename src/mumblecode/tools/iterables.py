# coding=utf-8
from operator import itemgetter


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
    ...     [8, 9, 11, 13]
    ... ]))
    [5, 6, 6, 1, 2, 3, 7, 4, 5, 6, 8, 8, 9, 10, 11, 12, 13]
    """
    first = max if reverse else min
    opened = []
    current = []
    # get the first items if they exist
    for feed in iterables:
        it = iter(feed)
        try:
            current.append(next(it))
        except StopIteration:
            continue
        else:
            opened.append(it)

    while opened:
        i, result = first(enumerate(current), key=itemgetter(1))
        # advance only the feed we just took the item from
        try:
            current[i] = next(opened[i])
        except StopIteration:
            # feed is finished, remove its entry in the lists
            del current[i]
            del opened[i]

        yield result


# this inspector is kinda dumb
# noinspection PyTypeChecker
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
    first = max if reverse else min
    opened = []
    current = []
    for feed in iterables:
        it = iter(feed)
        try:
            current.append(next(it))
        except StopIteration:
            continue
        else:
            opened.append(it)

    while opened:
        result = first(current)
        # advance every feed that is going to provide an equal value
        # in reversed order so we can delete in-place while iterating
        for i in reversed(range(len(opened))):
            if current[i] == result:
                try:
                    current[i] = next(opened[i])
                except StopIteration:
                    del current[i]
                    del opened[i]

        yield result
