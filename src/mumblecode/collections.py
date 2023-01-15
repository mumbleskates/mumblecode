# coding=utf-8
from bisect import bisect_left, bisect_right
from collections import namedtuple, defaultdict
from datetime import datetime, timezone
from functools import total_ordering
from itertools import groupby, tee
from operator import itemgetter
import random

from intervaltree import IntervalTree, Interval


#
# Some tricky sorting bullshit
#

class _LeastValue(object):
    """
    Instances of this class always sort before any other object, and are only equivalent
    to other LeastValue instances.
    """
    __slots__ = ()

    _instance = None

    # def __new__(cls, *args, **kwargs):
    #     global LeastValue
    #     LeastValue = LeastValue or super(_LeastValue, cls).__new__(cls, *args)
    #     return LeastValue

    def __lt__(self, other):
        return True

    __le__ = __lt__

    def __eq__(self, other):
        return type(other) is _LeastValue

    __ge__ = __eq__

    def __gt__(self, other):
        return False

    def __str__(self):
        return "<Least value>"

    def __repr__(self):
        return "LeastValue"

    __hash__ = object.__hash__

    # some pickling singleton trickery, since there is only ever reason to have one of these
    @staticmethod
    def _provider():
        return LeastValue

    def __reduce_ex__(self, *args, **kwargs):
        return _LeastValue._provider, ()

LeastValue = _LeastValue()


class _GreatestValue(object):
    """
    Instances of this class always sort before any other object, and are only equivalent
    to other GreatestValue instances.
    """
    __slots__ = ()

    def __gt__(self, other):
        return True

    __ge__ = __gt__

    def __eq__(self, other):
        return type(other) is _GreatestValue

    __le__ = __eq__

    def __lt__(self, other):
        return False

    def __str__(self):
        return "<Greatest value>"

    def __repr__(self):
        return "GreatestValue"

    __hash__ = object.__hash__

    # TODO: __add__ ?

    # some pickling singleton trickery, since there is only ever reason to have one of these
    @staticmethod
    def _provider():
        return GreatestValue

    def __reduce_ex__(self, *args, **kwargs):
        return _GreatestValue._provider, ()

GreatestValue = _GreatestValue()


class JustBefore(object):
    """
    `JustBefore(x)` sorts exactly the same as `x`, except it is always less than x rather than equal.
    """
    __slots__ = ('wrap',)

    def __init__(self, wrap):
        self.wrap = wrap

    def __gt__(self, other):
        return self.wrap > other

    __ge__ = __gt__

    def __eq__(self, other):
        return type(other) is JustBefore and other.wrap == self.wrap

    def __le__(self, other):
        return self.wrap <= other

    __lt__ = __le__

    def __hash__(self):
        return hash((self.wrap, JustBefore))

    def __repr__(self):
        return "JustBefore({})".format(repr(self.wrap))


class JustAfter(object):
    """
    `JustAfter(x)` sorts exactly the same as `x`, except it is always greater than x rather than equal.
    """
    __slots__ = ('wrap',)

    def __init__(self, wrap):
        self.wrap = wrap

    def __gt__(self, other):
        return self.wrap >= other

    __ge__ = __gt__

    def __eq__(self, other):
        return type(other) is JustAfter and other.wrap == self.wrap

    def __le__(self, other):
        return self.wrap < other

    __lt__ = __le__

    def __hash__(self):
        return hash((self.wrap, JustAfter))

    def __repr__(self):
        return "JustAfter({})".format(repr(self.wrap))

#
# End tricky sorting bullshit
#


class _SentinelCls(object):
    __slots__ = ()

    def __repr__(self):
        return '_SENTINEL'

    @staticmethod
    def _provider():
        return _SENTINEL

    def __reduce_ex__(self, *args, **kwargs):
        return _SentinelCls._provider, ()

_SENTINEL = _SentinelCls()


def now():
    return datetime.now(timezone.utc)


def interval_overlap(a, b):
    """
    :type a: Interval
    :type b: Interval
    :rtype: Interval
    :return: Return an interval representing the overlapping portion of a and b, with the value of a.
    """
    # if a.data != b.data:
    #     return Interval(a.begin, a.begin)
    # else:
    return Interval(max(a.begin, b.begin), min(a.end, b.end), a.data)


def interval_tree_intersection(tree, intervals):
    """
    :type tree: IntervalTree
    :type intervals: collections.Iterable[Interval]
    :rtype: collections.Iterable[Interval]
    :return: Return an iterable of intervals with all commonalities between the two given IntervalTrees.
        This will contain intervals for all overlaps between intervals in one tree to another where the values of
        the intervals are the same.
    """
    for iv in intervals:
        for touching in tree[iv]:
            if iv.data == touching.data:
                yield interval_overlap(iv, touching)


def merge_interval_overlaps(intervals, open_ended=()):
    """
    Merge a set of intervals so that there are a minimum number of intervals.

    Any multiplicity of intervals
    with the same data value that covers a single contiguous range will be replaced with a single interval
    over that range.

    open_ended may be omitted or a dictionary of key:begin pairs representing open-ended intervals (intervals
    that have a beginning but no set end.)
    If it is included, it may be modified to merge intervals into it.

    A new iterable of intervals is returned. Due to the algorithm used, the iterable of intervals produced will be
    first grouped by key (in arbitrary key-order), then sorted by interval position.
    """
    # pull intervals into bins sorted by key
    by_key = defaultdict(list)
    for iv in intervals:
        by_key[iv.data].append(iv)
    # render merged intervals
    for key, bucket in by_key.items():
        bucket.sort(key=itemgetter(0))  # sort intervals by begin

        if key in open_ended:
            first = bucket[0].begin  # first and last keep track of the bounds of the current run
            if first >= open_ended[key]:
                continue  # all in open-ended; skip the rest
            last = bucket[0].end
            if last >= open_ended[key]:
                open_ended[key] = first  # merge with this interval
                continue  # skip the rest
            for iv in bucket[1:]:
                if iv.begin <= last:  # new interval overlaps the current run
                    last = max(last, iv.end)  # merge
                    if last >= open_ended[key]:  # check for collision with open interval
                        open_ended[key] = first  # merge with open-ended
                        break  # all intervals after this would also merge with the open-ended
                else:  # current run ends
                    yield Interval(begin=first, end=last, data=key)  # yield accumulated interval
                    first = iv.begin  # start new run
                    if first >= open_ended[key]:
                        break  # all intervals from here on out will merge silently with open interval
                    last = iv.end
                    if last >= open_ended[key]:
                        open_ended[key] = first  # merge with open-ended
                        break  # skip the rest
            else:  # did not merge with an open-ended interval
                yield Interval(begin=first, end=last, data=key)  # yield the last run

        # This is much simpler. Understand this first; the above just keeps checking open-ended
        # whenever first or last are set, and is otherwise identical.

        else:  # no open-ended interval for this key
            first = bucket[0].begin  # first and last keep track of the bounds of the current run
            last = bucket[0].end
            for iv in bucket[1:]:
                if iv.begin <= last:  # new interval overlaps the current run
                    last = max(last, iv.end)  # merge
                else:  # current run ends
                    yield Interval(begin=first, end=last, data=key)  # yield accumulated interval
                    first = iv.begin  # start new run
                    last = iv.end
            yield Interval(begin=first, end=last, data=key)  # yield the last run


class _ViewMixin(object):
    """Mixin for set operators on dictionary views"""
    __slots__ = ()

    def __iter__(self):
        pass

    def __and__(self, other):
        result = set()
        for x in self:
            if x in other:
                result.add(x)
        return result

    def __xor__(self, other):
        result = set()
        for x in self:
            if x not in other:
                result.add(x)
        for x in other:
            if x not in self:
                result.add(x)
        return result

    def __or__(self, other):
        result = set(self)
        result.update(other)
        return result


# Sample = namedtuple("Sample", ["time", "value"])
@total_ordering
class Sample(namedtuple('Sample', 'point value')):
    """Sample(time, value)"""
    __slots__ = ()

    def __lt__(self, other):
        if not isinstance(other, tuple):
            raise TypeError
        return self[0] < other[0]

    def __le__(self, other):
        return self < other or self == other

    def __ge__(self, other):
        return self > other or self == other

    def __gt__(self, other):
        if not isinstance(other, tuple):
            raise TypeError
        return self[0] > other[0]

    def __hash__(self):
        return tuple.__hash__(self)

    def __reduce_ex__(self, *args, **kwargs):
        return Sample, tuple(self)


class SampledValue(object):
    """
    Holds a time-line for a single value. This structure allows any number of consecutive entries with the same
    value, allowing you to keep track of known values for a variable that may change unobserved, and gaps in
    observations may be important to know.

    This is much more useful as a history of observations than as a canonical history.
    """

    __slots__ = ('history',)

    def __init__(self, samples=(), *, initial_value=None, time=None):
        if initial_value is None and time is None:
            self.history = sorted(samples)
        else:
            if samples:
                raise ValueError("Both initial value and samples were provided")
            self.history = [Sample(point=time if time is not None else now(), value=initial_value)]

    def __len__(self):
        return len(self.history)

    def __iter__(self):
        return iter(self.history)

    def __reversed__(self):
        return reversed(self.history)

    def sort(self):
        self.history.sort()

    def all_values(self):
        result = set()
        for entry in self.history:
            result.add(entry.value)
        return result

    def get(self, *, time=None, default=None):
        if not self.history:  # history is empty
            if default is not None:
                return default
            raise KeyError
        if time is None:
            # fetch the current value
            return self.history[-1].value
        else:
            # get the index of the last sample before or at time
            index = bisect_right(self.history, (time,)) - 1
            if index < 0:  # no samples this far back
                if default is not None:
                    return default
                raise KeyError
            return self.history[index].value

    def set(self, value, *, time=None):
        if time is None:
            self.history.append(Sample(point=now(), value=value))
        else:
            # find the first sample at >= time
            index = bisect_left(self.history, (time,))
            entry = Sample(point=time, value=value)
            # again, we must check if there is an entry at that exact time
            if index < len(self.history) and self.history[index].point == time:
                self.history[index] = entry
            else:
                self.history.insert(index, entry)

    def begin(self):
        return self.history[0].point

    def time_slice(self, begin, end):
        """
        Return an iterable over all the intervals intersecting the given half-open interval from begin to end,
        chopped to fit within it
        """
        if begin is None or end is None:
            raise ValueError("Both the beginning and end of the interval must be included")
        if begin >= end or not self.history:
            return

        # get index of first sample at or before begin
        start_index = max(0, bisect_right(self.history, (begin,)) - 1)

        def important_values():  # yields only the first entry for groups of consecutive values in history
            for key, values in groupby(self.history[start_index:], key=itemgetter(1)):
                yield next(values)

        i1, i2 = tee(important_values())
        last_end = next(i2)
        for a, b in zip(i1, i2):
            yield Interval(begin=max(begin, a.point), end=min(b.point, end), data=a.value)
            last_end = b
            if end <= b.point:
                break
        else:  # end is after the end of our history list or we have only one entry
            if last_end.point != end:  # try not to yield null intervals
                yield Interval(begin=last_end.point, end=end, data=last_end.value)

    def intervals(self, end_time=GreatestValue):
        if not self.history:
            return iter(())
        return self.time_slice(self.begin(), end_time)


class IntervalMapping(object):
    __slots__ = ('ivs',)

    def __init__(self, intervals=()):
        self.ivs = sorted(iv for iv in intervals if not iv.is_null())

    def begin(self):
        return self.ivs[0].begin if self.ivs else 0

    def first_value(self):
        """Return the value of the first interval, or None if empty"""
        if not self.ivs:
            return None
        return self.ivs[0].data

    def end(self):
        return self.ivs[-1].end if self.ivs else 0

    def last_value(self):
        """Return the value of the last interval, or None if empty"""
        if not self.ivs:
            return None
        return self.ivs[-1].data

    def clear(self):
        self.ivs.clear()

    def copy(self):
        result = IntervalMapping()
        result.ivs = self.ivs.copy()
        return result

    def _position_index(self, position):
        """Return the index of the last interval with a beginning <= position"""
        lo = 0
        hi = len(self.ivs)
        # copied from bisect_right, because we only care about .begin
        while lo < hi:
            mid = (lo + hi) // 2
            if position < self.ivs[mid].begin:
                hi = mid
            else:
                lo = mid + 1
        return lo - 1

    def _end_position_index(self, position):
        """Return the index of the last interval with a beginning < position"""
        lo = 0
        hi = len(self.ivs)
        # copied from bisect_left, because we only care about .begin
        while lo < hi:
            mid = (lo + hi) // 2
            if self.ivs[mid].begin < position:
                lo = mid + 1
            else:
                hi = mid
        return lo - 1

    def _contains_point(self, position):
        index = self._position_index(position)
        return index >= 0 and position < self.ivs[index].end

    def _overlaps_range(self, begin, end):
        begin_index = self._position_index(begin)
        end_index = self._end_position_index(end)
        return begin_index != end_index or (begin_index >= 0 and begin < self.ivs[begin_index].end)

    def _value_at_position(self, position):
        index = self._position_index(position)
        if index >= 0:
            iv = self.ivs[index]
            if position < iv.end:
                return iv.data
        raise KeyError

    def _slice(self, begin, end):
        if begin is None:
            begin = LeastValue
        if end is None:
            end = GreatestValue
        if end <= begin:
            return

        begin_index = self._position_index(begin)
        end_index = self._end_position_index(end)

        if begin_index >= 0:  # begins somewhere in the middle
            iv = self.ivs[begin_index]
            if begin < iv.end:  # slice touches this interval
                yield Interval(begin, min(iv.end, end), iv.data)

        if end_index == begin_index:  # ends on the same of our intervals; we're done
            return

        yield from self.ivs[begin_index + 1:end_index]

        iv = self.ivs[end_index]
        if iv.begin < end:  # must more than simply touch on end
            yield Interval(iv.begin, min(iv.end, end), iv.data)

    def _set_range(self, begin, end, wrapped_value):
        if begin is None:
            begin = LeastValue
        if end is None:
            end = GreatestValue
        if end <= begin:
            raise KeyError("Invalid interval: {}".format((begin, end)))  # invalid interval

        # inclusive first index of the intervals we plan to overwrite
        begin_index = self._position_index(begin)
        # will be the exclusive last index of the intervals we plan to overwrite
        end_index = self._end_position_index(end)

        replacement = []

        # check beginning
        # print("begin_index", begin_index)
        if begin_index < 0:  # before first interval
            # print("begin index was <0")
            begin_index = 0
        elif begin_index < len(self.ivs):
            if begin < self.ivs[begin_index].end:  # begins inside this interval, needs to be sliced or covered
                iv = self.ivs[begin_index]
                # print("inside interval", iv)
                if begin == iv.begin or (iv.data,) == wrapped_value:
                    begin = iv.begin  # cover this interval
                    # print("covering beginning")
                else:
                    # slice this interval; cannot be in-place in case we are insetting
                    replacement.append(Interval(iv.begin, begin, iv.data))
                    # print("slicing beginning", iv, replacement[0])
            else:  # after this interval; don't touch it
                begin_index += 1
                # print("beginning is after interval")

        # check end
        # print("end_index", end_index)
        if end_index < 0:
            # print("end index was <0")
            end_index = 0
        elif end_index < len(self.ivs):
            iv = self.ivs[end_index]
            # check if it ends inside this interval, and thus needs to be sliced or covered
            if end < iv.end:
                # print("inside interval", iv)
                if end == iv.end or (iv.data,) == wrapped_value:
                    end = iv.end  # cover this interval
                    end_index += 1
                    # print("covering end")
                else:
                    # slice this interval in-place, and do not cover it
                    self.ivs[end_index] = Interval(end, iv.end, iv.data)
                    # print("slicing end", self.ivs[end_index])
            else:  # ends after this interval; cover it completely
                end_index += 1
                # print("ending is after interval")

        # modify the list
        if wrapped_value != ():
            replacement.append(Interval(begin, end, wrapped_value[0]))
        # print("intervals being covered", slice(begin_index, end_index), self.ivs[begin_index:end_index])
        # print("replacing with", replacement)
        self.ivs[begin_index:end_index] = replacement

    def __contains__(self, key):
        if type(key) is Interval:
            return self._overlaps_range(key.begin, key.end)
        else:
            return self._contains_point(key)

    def envelops(self, interval):
        """Return True if there is an interval in this mapping that completely encloses the given one.
        Raise ValueError if the interval given is null."""
        if type(interval) is not Interval:
            raise TypeError
        if interval.is_null():
            raise ValueError
        begin_index = self._position_index(interval.begin)
        end_index = self._end_position_index(interval.end)
        return (
            begin_index >= 0 and
            begin_index == end_index and
            interval.end <= self.ivs[begin_index].end
        )

    def is_continuous_over_interval(self, begin, end):
        """Return True if there are no values in the half-open interval [begin, end) that are un-mapped"""
        begin_index = self._position_index(begin)
        end_index = self._position_index(end)
        # begin and end must be mapped
        if begin_index < 0 or self.ivs[begin_index].end <= begin or self.ivs[end_index].end < end:
            return False
        # each pair of adjacent intervals in the series from begin to end must share the same end/begin value
        for i in range(begin_index, end_index):
            if self.ivs[i].end != self.ivs[i + 1].begin:
                return False
        return True

    def __getitem__(self, key):
        if type(key) in (slice, Interval):
            a, b = (key.start, key.stop) if type(key) is slice else (key.begin, key.end)
            return self._slice(a, b)
        else:
            return self._value_at_position(key)

    def interval_at_position(self, position):
        """Return the interval that contains position, or None."""
        index = self._position_index(position)
        if index < 0:
            return None
        iv = self.ivs[index]
        return iv if position < iv.end else None

    def interval_at_or_after(self, position):
        """Return the first interval >= position or None."""
        index = self._position_index(position)
        if index < 0 or position < self.ivs[index].end:
            index += 1
        return None if index >= len(self.ivs) else self.ivs[index]

    def interval_at_or_before(self, position):
        """Return the last interval <= position or None."""
        index = self._position_index(position)
        return None if index < 0 else self.ivs[index]

    def next_interval_after(self, position_or_interval):
        """Return the first interval > position or None."""
        if type(position_or_interval) is Interval:
            index = self._end_position_index(position_or_interval.end) + 1
        else:
            index = self._position_index(position_or_interval) + 1
        return None if index >= len(self.ivs) else self.ivs[index]

    def next_interval_before(self, position_or_interval):
        """Return the last interval < position or None."""
        if type(position_or_interval) is Interval:
            point = position_or_interval.begin
        else:
            point = position_or_interval
        index = self._position_index(point)
        if point < self.ivs[index].end:
            index -= 1
        return None if index < 0 else self.ivs[index]

    def __setitem__(self, key, value):
        if type(key) not in (slice, Interval):
            raise TypeError("IntervalMapping must be modified with index that are Intervals or slices")
        a, b = (key.start, key.stop) if type(key) is slice else (key.begin, key.end)
        self._set_range(a, b, (value,))

    def apply(self, interval):
        """Apply this interval to the mapping. If the interval is null, raises a KeyError."""
        if type(interval) is not Interval:
            raise TypeError
        self._set_range(interval.begin, interval.end, (interval.data,))

    def __delitem__(self, key):
        if type(key) not in (slice, Interval):
            raise TypeError("IntervalMapping must be modified with index that are Intervals or slices")
        a, b = (key.start, key.stop) if type(key) is slice else (key.begin, key.end)
        self._set_range(a, b, ())

    def __len__(self):
        return len(self.ivs)

    def __iter__(self):
        return iter(self.ivs)

    def __reversed__(self):
        return reversed(self.ivs)

    def __repr__(self):
        if self.ivs:
            return "IntervalMapping({})".format(self.ivs)
        else:
            return "IntervalMapping()"


class HistorySet(object):
    __slots__ = ('current', 'history')

    def __init__(self, values=(), *, time=None):
        time = time if time is not None else now()
        self.current = {v: time for v in values}
        self.history = IntervalTree()

    @staticmethod
    def from_intervals(intervals):
        result = HistorySet()
        for iv in intervals:
            result.add_interval(iv)

    def add_interval(self, iv):
        if iv.end is GreatestValue:
            self.current[iv.data] = iv.begin
        else:
            if iv.data in self.current and self.current[iv.data] <= iv.end:
                del self.current[iv.data]
            self.history.add(iv)

    def refine_history(self):
        """
        Scrub the internal IntervalTree history so that there are a minimum number of intervals.

        Any multiplicity of intervals with the same data value that covers a single contiguous range will
        be replaced with a single interval over that range.

        This is an expensive operation, both in time and memory, that should only be performed when the
        history is being modified carelessly, such as naively merging with the history from another HistorySet
        or adding and removing elements out of chronological order.

        Behavior for the HistorySet should be identical before and after calling refine_history(), but may be
        slightly faster and consume less memory afterwards. The only change will be that it should no longer
        return incorrect values for the effective added date of currently contained items after merging with
        history intervals.
        """
        self.history = IntervalTree(merge_interval_overlaps(self.history, self.current))

    def __getitem__(self, index):
        if type(index) is slice:
            if index.step is not None:
                raise ValueError("Slice indexing is used for intervals, which do not have a step.")
            iv = Interval(index.start, index.stop)
            result = {x.data for x in self.history[iv]}
            result.update(x[0] for x in self.current.items() if iv.overlaps(Interval(begin=x[1], end=None)))
        else:
            result = {x.data for x in self.history[index]}
            result.update(item_ for item_, time_ in self.current.items() if time_ <= index)
        return result

    def time_slice(self, begin, end):
        """
        Return an iterable over all the intervals intersecting the given half-open interval from begin to end,
        chopped to fit within it
        """
        if begin is None or end is None:
            raise ValueError("Both the beginning and end of the interval must be included")
        if end <= begin:
            raise ValueError("begin must be < end")
        for iv in self.history[begin:end]:
            yield Interval(begin=max(iv.begin, begin), end=min(iv.end, end), data=iv.data)
        for value, added in self.current.items():
            if added < end:
                yield Interval(begin=added, end=end, data=value)

    def intervals(self):
        """
        Return an iterator over all the intervals in this set. Currently contained values have intervals
        ending with a GreatestValue object.
        """
        yield from self.history
        end = GreatestValue
        for value, begin in self.current.items():
            yield Interval(begin=begin, end=end, data=value)

    def all_values(self):
        result = self.copy()
        for old in self.history:
            result.add(old.data)
        return result

    def item_added_time(self, value):
        return self.current[value]

    def ordered_by_addition(self, *, time=None):
        if time is None:
            result = list(self.current.items())
        else:
            result = [(x.begin, x.data) for x in self.history[time]]
            result.extend((added, item) for item, added in self.current.items() if added <= time)
        result.sort(key=itemgetter(0))
        return [x[1] for x in result]

    def add(self, value, *, time=None):
        time = time if time is not None else now()
        if value not in self.current or self.current[value] > time:
            self.current[value] = time

    def remove(self, value, *, time=None):
        self.history.addi(self.current.pop(value), time if time is not None else now(), value)

    def discard(self, value, *, time=None):
        if value in self.current:
            self.remove(value, time=time)

    def copy(self, *, time=None):
        if time is None:
            return set(self.current)
        else:
            return self[time]

    def members_in_interval(self, begin, end):
        return self[begin:end]

    def clear(self, *, time=None):
        time = time if time is not None else now()
        for item in self.current.items():
            self.history.addi(item[1], time, item[0])
        self.current.clear()

    def union(self, *others):
        result = self.copy()
        result.update(*others)
        return result

    def difference(self, *others):
        result = self.copy()
        result.difference_update(*others)
        return result

    def symmetric_difference(self, other):
        result = self.copy()
        result.symmetric_difference_update(other)
        return result

    def intersection(self, *others):
        result = self.copy()
        result.intersection_update(*others)
        return result

    def update(self, *others, time=None):
        time = time if time is not None else now()
        for other in others:
            for value in other:
                self.add(value, time=time)

    def difference_update(self, *others, time=None):
        time = time if time is not None else now()
        for other in others:
            for value in other:
                self.discard(value, time=time)

    def symmetric_difference_update(self, other, *, time=None):
        time = time if time is not None else now()
        for value in other:
            if value in self.current:
                self.remove(value, time=time)
            else:
                self.add(value, time=time)

    def intersection_update(self, *others, time=None):
        time = time if time is not None else now()
        toss = self.difference(*others)
        for value in toss:
            self.discard(value, time=time)

    def pop(self, *, time=None):
        time = time if time is not None else now()
        item = self.current.popitem()
        self.history.addi(item[1], time, item[0])
        return item[0]

    def isdisjoint(self, other):
        # noinspection PyUnresolvedReferences
        return self.current.keys().isdisjoint(other)

    def issubset(self, other):
        return other > self.current

    def issuperset(self, other):
        return other < self.current

    def __iter__(self):
        return iter(self.current)

    def __len__(self):
        return len(self.current)

    def __eq__(self, other):
        if isinstance(other, (set, frozenset)):
            return self.current.keys() == other
        elif isinstance(other, HistorySet):
            return self.current.keys() == other.current.keys()
        return False

    def __lt__(self, other):
        return self < other or self == other

    def __gt__(self, other):
        return self > other or self == other

    def __contains__(self, item):
        return item in self.current

    __le__ = issubset
    __ge__ = issuperset
    __or__ = union
    __and__ = intersection
    __sub__ = difference
    __xor__ = symmetric_difference
    __ior__ = update
    __iand__ = intersection_update
    __isub__ = difference_update
    __ixor__ = symmetric_difference_update


class HistoryDict(object):
    __slots__ = ('d', '_len')

    def __init__(self):
        self.d = {}
        self._len = 0

    def set(self, key, value, *, time=None):
        time = time if time is not None else now()
        if key in self.d:
            self.d[key].set(value, time=time)
        else:
            self.d[key] = SampledValue(initial_value=value, time=time)
            self._len += 1

    def __setitem__(self, key, value):
        self.set(key, value)

    def setdefault(self, key, default=None, *, time=None):
        time = time if time is not None else now()
        result = self._fetch(key, time)
        if result is _SENTINEL:
            self.set(key, default, time=time)
            return default
        else:
            return result

    def update(self, other, *, time=None):
        time = time if time is not None else now()
        if isinstance(other, (dict, HistoryDict)):
            update_from = other.items()
        else:
            update_from = other
        for item in update_from:
            self.set(item[0], item[1], time=time)

    def _fetch(self, key, time):
        if key in self.d:
            return self.d[key].get(time=time, default=_SENTINEL)
        else:
            return _SENTINEL

    def get(self, key, default=None, *, time=None):
        time = time if time is not None else now()
        result = self._fetch(key, time)
        return default if result is _SENTINEL else result

    def __getitem__(self, item):
        result = self._fetch(item, now())
        if result is _SENTINEL:
            raise KeyError
        else:
            return result

    def copy(self, *, time=None):
        # no need to normalize time from None, fetching with None for current is faster
        return {k: v for k, v in self.d.items() if v.get(time=time, default=_SENTINEL) is not _SENTINEL}

    def __len__(self):
        return self._len

    def delete(self, key, *, time=None):
        time = time if time is not None else now()
        if key not in self.d:
            raise KeyError
        if self.d[key].get(time=time, default=_SENTINEL) is _SENTINEL:
            raise KeyError
        self.d[key].set(_SENTINEL, time=time)
        self._len -= 1

    def __delitem__(self, key):
        self.delete(key)

    def clear(self, *, time=None):
        time = time if time is not None else now()
        for value in self.d.values():
            value.set(_SENTINEL, time=time)
        self._len = 0

    def contains(self, key, *, time=None):
        return self._fetch(key, time) is not _SENTINEL

    def __contains__(self, item):
        return self.contains(item)

    def pop(self, key, *, time=None):
        time = time if time is not None else now()
        result = self.get(key, time=time)
        self.delete(key, time=time)
        return result

    def popitem(self, *, time=None):
        if self._len == 0:
            raise KeyError
        time = time if time is not None else now()
        key = next(self)
        value = self.get(key)
        self.delete(key, time=time)
        return key, value

    def __iter__(self):
        for key in self.d:
            if self._fetch(key, None) is not _SENTINEL:
                yield key

    def __eq__(self, other):
        if len(other) != len(self):
            return False
        if not isinstance(other, (dict, HistoryDict)):
            return False
        for key, other_value in other.items():
            self_value = self._fetch(key, None)
            if self_value != other_value:
                return False
        return True

    def all_keys(self):
        return self.d.keys()

    def all_items(self):
        for key, value in self.d.items():
            for past_value in value.all_values():
                if past_value is not _SENTINEL:
                    yield (key, past_value)

    def all_values_of(self, key):
        for past_value in self.d[key].all_values():
            if past_value is not _SENTINEL:
                yield past_value

    def items(self):
        class HistoryDictItems(_ViewMixin):
            __slots__ = ('hd',)

            def __init__(self, hd):
                self.hd = hd

            def __contains__(self, item):
                if not isinstance(item, tuple) or len(item) != 2:
                    return False
                if item[0] in self.hd:
                    return self.hd[item[0]] == item[1]
                else:
                    return False

            def __iter__(self):
                time = now()
                for key in self.hd:
                    yield (key, self.hd.get(key, time=time))

        return HistoryDictItems(self)

    def keys(self):
        class HistoryDictKeys(_ViewMixin):
            __slots__ = ('hd',)

            def __init__(self, hd):
                self.hd = hd

            def __contains__(self, item):
                return item in self.hd

            def __iter__(self):
                yield from self.hd

        return HistoryDictKeys(self)

    def values(self):
        class HistoryDictValues(object):
            __slots__ = ('hd',)

            def __init__(self, hd):
                self.hd = hd

            def __contains__(self, item):
                return item in iter(self)

            def __iter__(self):
                for key in self.hd:
                    return self.hd[key]

        return HistoryDictValues(self)


class WeightedSet(object):
    """
    An fast O(log n) mapping that holds values with weightings. Can be used to select random values from a mutable
    weighted set; does not work or make any sense with negative weights.

    Populating many values costs O(n log n)
    Iterating over all the values and their weights costs O(n)
    Importing/exporting values with summed weights, with export_heap()/import_heap(), costs O(n)
    Modifying, adding, or deleting a single weight costs O(log n)
    Choosing a single weighted-random value from the set costs O(log n)
    Querying the sum of weights in the set costs O(1)
    Querying a single weight in the set costs O(1)

    Add or set a weight with `weights[key] = w`
    Get a weight with `weights[key]`
    Remove a weight with `del weights[key]`
    Increase a weight by 1 with `weights.modify(key)`
    Increase or decrease a weight by a different value with `weights.modify(key, delta)`
    Get the sum of all weights in the set with `weights.sum()`
    Get the number of weights in the set with `len(weights)`
    Get a weighted random choice from the set with `weights.choice()`
    Get an un-weighted random choice from the set with `weights.unweighted_choice()`
    Get a random choice using your own random number generator with `weights.choose(your_random_in_range(weights.sum())`
    """

    # modeled after http://stackoverflow.com/a/2149533/3088947 once I knew to look for a sum-heap implementation
    # In this version we are starting our indices at 0, so the functions are:
    # left child: (i << 1) + 1
    # right child: (i << 1) + 2
    # parent: (i - 1) >> 1

    class Node(object):
        __slots__ = ('v', 'w', 'tw')

        def __init__(self, value, weight, total_weight=0):
            self.v = value
            self.w = weight
            self.tw = total_weight

    def __init__(self, items=()):
        self.dict = {}  # tracks where in the heap each value is
        self.heap = []  # stores the nodes of the sum-tree
        for key, weight in items:
            self.modify(key, weight)

    @staticmethod
    def import_heap(heap_data):
        result = WeightedSet()
        result.__setstate__(heap_data)
        return result

    def export_heap(self):
        return self.__getstate__()

    def copy(self):
        return WeightedSet.import_heap(self.export_heap())

    def __setstate__(self, heap_data):
        for i, (v, w, tw) in enumerate(heap_data):
            self.heap.append(WeightedSet.Node(v, w, tw))
            self.dict[v] = i

    def __getstate__(self):
        return [(node.v, node.w, node.tw) for node in self.heap]

    def __iter__(self):
        """Iterate over (value, weight) pairs in the set, with tuples"""
        for node in self.heap:
            yield node.v, node.w

    def _propagate(self, i, delta):
        while i >= 0:
            self.heap[i].tw += delta
            i = (i - 1) >> 1

    def modify(self, key, delta=1):
        if key in self.dict:
            i = self.dict[key]
            self.heap[i].w += delta
        else:
            i = len(self.heap)
            self.heap.append(WeightedSet.Node(key, delta))
            self.dict[key] = i
        self._propagate(i, delta)

    def __getitem__(self, key):
        return self.heap[self.dict[key]].weight

    def __setitem__(self, key, value):
        self.modify(key, value - self[key])

    def __delitem__(self, key):
        # get this key's index in the heap
        # and delete this key from the dict
        i = self.dict.pop(key)
        # remove weight from heap at this key's location
        self._propagate(i, -self.heap[i].w)
        # if this is the last node, just delete it
        if i == len(self.heap) - 1:
            self.heap.pop()
        else:
            # otherwise, get the last node from heap and put it here
            replacement = self.heap.pop()
            replacement.tw = self.heap[i].tw
            self.heap[i] = replacement
            # re-propagate its weight
            self._propagate(i, replacement.w)
            # and change that node's key's value in dict to its new heap location, where the old key was
            self.dict[replacement.v] = i

    def __len__(self):
        """Return the number of key/weight pairs"""
        return len(self.heap)

    def sum(self):
        """Return sum of all weights"""
        if self:
            return self.heap[0].tw
        raise IndexError("Empty mapping has no sum")

    def choose(self, choose_at):
        i = 0                               # start driving at the root
        while choose_at >= self.heap[i].w:  # while we have enough gas to get past node i:
            choose_at -= self.heap[i].w       # drive past node i
            i = (i << 1) + 1                  # move to first child
            if choose_at >= self.heap[i].tw:  # if we have enough gas:
                choose_at -= self.heap[i].tw    # drive past first child and descendants
                i += 1                          # move to second child

        return self.heap[i].v               # out of gas at heap[i]

    def choice(self):
        return self.choose(self.sum() * random.random())

    def pop(self):
        val = self.choice()
        weight = self[val]
        del self[val]
        return val, weight

    def unweighted_choice(self):
        return random.choice(self.heap).v
