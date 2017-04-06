# coding=utf-8
from collections import namedtuple
from functools import total_ordering
from heapq import heappush, heappop
from itertools import count, zip_longest


INITIAL_START = object()


class SumTuple(tuple):
    """
    A handy class for storing priority-costs. Acts just like a regular tuple, but addition
    adds together corresponding elements rather than appending.
    """

    def __add__(self, other):
        if other == 0:
            return self
        if not isinstance(other, tuple):
            raise TypeError("Cannot add '{0} to {1}".format(self, other))
        return SumTuple(x + y for x, y in zip_longest(self, other, fillvalue=0))

    def __radd__(self, other):
        return self + other


class MaxFirstSumTuple(tuple):
    """
    Like SumTuple, but the first element in the tuple reduces with max instead of sum. This allows an undesirable
    edge to equally taint any route that passes through it.
    """

    def __add__(self, other):
        if other == 0:
            return self
        if not isinstance(other, tuple):
            raise TypeError("Cannot add '{0} to {1}".format(self, other))
        return MaxFirstSumTuple(self._adder(other))

    def __radd__(self, other):
        return self + other

    def _adder(self, other):
        it = zip_longest(self, other, fillvalue=0)
        yield max(next(it))
        yield from (x + y for x, y in it)


@total_ordering
class Worker(object):
    """
    Worker is a class for helper objects that can transform the costs of traversing a graph on an instance basis.
    Work costs will be added together directly, so recommended return types include int, float, and SumTuple.
    Workers can also be used for the task of computing paths from multiple starting points, where the
    point you begin will affect the cost of your traversal overall (different workers beginning at different locations).

    Workers essentially conditionally transform the edge-cost into a summable value. When using workers, edgefinder
    should produce a cost that DESCRIBES the work to be performed to traverse the edge, which is passed into the
    perform_work function as its sole parameter. The return value of this function must then be the COST of doing the
    work thus described; for instance, edgefinder should describe the distance between the edge and the neighbor,
    and the worker will accept that distance and return the amount of time to travel that distance.
    """

    def __init__(self, name, perform_work):
        """
        :type name: str
        :type perform_work: (Any) -> Any
        """
        self.name = name
        self.perform_work = perform_work

    def __add__(self, other):
        if other == 0:
            return self
        else:
            return WorkPerformed(self.perform_work(other), self)

    def __radd__(self, other):
        return self + other

    def __eq__(self, other):
        if isinstance(other, Worker):
            return self.name == other.name
        else:
            raise TypeError

    def __lt__(self, other):
        if isinstance(other, Worker):
            return self.name < other.name
        else:
            raise TypeError

    def __str__(self):
        return "Worker({})".format(self.name)

    __repr__ = __str__


# justification: namedtuple already has an init
# noinspection PyClassHasNoInit
class WorkPerformed(namedtuple("WorkPerformed", "cost worker")):
    def __add__(self, other):
        if other == 0:
            return self
        else:
            return WorkPerformed(self.cost + self.worker.perform_work(other), self.worker)

    def __radd__(self, other):
        return self + other


def with_initial(initial):
    """
    :param initial: iterable of (start node, worker) tuples
    :return: Decorate an edgefinder to start the given initial costs at the given locations.
        If these initial costs are Workers, The edgefinder being decorated should normally
        return edge costs that are compatible work descriptors. To use this decorator to
        populate the map traversal with workers, send the constant INITIAL_START as the
        starting node.
    """
    def dec(edgefinder):
        def new_edgefinder(node):
            if node is INITIAL_START:
                return initial
            else:
                return edgefinder(node)

        return new_edgefinder

    return dec


def astar(
        starts, valid_destination,
        edgefinder=lambda node: ((x, 1) for x in node),
        heuristic=None,
        tol=None
):
    # TODO: flag to optionally exclude paths with cycles
    # TODO: optimize multiple path iteration
    """
    :param starts: iterable of any type, only used as keys.
    :param valid_destination: a predicate function returning true for any node that is a suitable destination
    :param edgefinder: A function that returns an iterable of tuples
        of (neighbor, distance) from the node it is passed
    :param heuristic: An optional function that returns an optimistic estimate of the distance from a node
        to the nearest valid destination
    :param tol: An optional value; all paths with a total distance less or equal to the best distance plus tol will
        be yielded
    :return: A generator of the paths from any starting node to any valid destination, shortest to longest.
        Results are yielded as a tuple of (total cost, path). 'path' here is a tuple-chain from the destination
        (path[0]) back to the starting point (path[1][1][1]...[0]). For a path that goes from 1 to 2 to 3, this
        path would be (3, (2, (1, ()))). See also: convert_path()
    """
    if tol is None:
        visited = set()

        def is_visited(node_, _):
            return node_ in visited

        def visit(node_, _):
            if node_ in visited:
                return False
            else:
                visited.add(node_)
                return True

    else:
        distances = {}

        def is_visited(node_, dist_):
            return node_ in distances and dist_ > distances[node_] + tol

        def visit(node_, dist_):
            if node_ not in distances:
                distances[node_] = dist_
                return True
            else:
                return dist_ <= distances[node_] + tol

    index = count()
    heap = []

    def process():
        for seed in starts:
            if heuristic is None:
                yield 0, 0, None, seed, ()
            else:
                yield heuristic(seed), 0, None, seed, ()
        while heap:
            yield heappop(heap)

    # Heap values are:
    #   distance + heuristic,
    #   distance value,
    #   a unique counter for sorting,
    #   the next place to go,
    #   and the (path, (so, (far,)))
    for _, dist, _, node, path in process():
        if visit(node, dist):
            path = (node, path)
            if valid_destination(node):
                yield dist, path

            for neighbor, dist_to_neighbor in edgefinder(node):
                neighbor_dist = dist + dist_to_neighbor
                if is_visited(neighbor, neighbor_dist):
                    continue
                if heuristic is None:
                    h_dist = neighbor_dist
                else:
                    h_dist = neighbor_dist + heuristic(neighbor)
                heappush(heap, (h_dist, neighbor_dist, next(index), neighbor, path))


def dijkstra(starts, valid_destination, edgefinder=lambda node: ((x, 1) for x in node)):
    """
    :param starts: iterable of any type, only used as keys.
    :param valid_destination: a predicate function returning true for any node that is a suitable destination
    :param edgefinder: A function that returns an iterable of tuples
        of (neighbor, distance) from the node it is passed
    :return: A generator of the paths from any starting node to any valid destination, shortest to longest.
        Results are yielded as a tuple of (total cost, path). 'path' here is a tuple-chain from the destination
        (path[0]) back to the starting point (path[1][1][1]...[0]). For a path that goes from 1 to 2 to 3, this
        path would be (3, (2, (1, ()))). See also: convert_path()
    """
    return astar(starts, valid_destination, edgefinder)


def dijkstra_first(starts, valid_destination, edgefinder=lambda node: ((x, 1) for x in node)):
    """
    :param starts: iterable of any type, only used as keys.
    :param valid_destination: a predicate function returning true for any node that is a suitable destination
    :param edgefinder: A function that returns an iterable of tuples
        of (neighbor, distance) from the node it is passed
    :return: the shortest path from any starting node to any valid destination, or (None, ()) if none exists
    """
    try:
        return next(dijkstra(starts, valid_destination, edgefinder))
    except StopIteration:
        return None, ()  # no path exists


def dijkstra_simple(start, destination, edgefinder=lambda node: ((x, 1) for x in node)):
    """
    :param start: The start node
    :param destination: The destination node
    :param edgefinder: A function that returns an iterable of tuples
        of (neighbor, distance) from the node it is passed
    :return: Returns the shortest path from the start to the destination.
        Only accepts one start and one end.
    """
    return dijkstra_first((start,), lambda node: node == destination, edgefinder)


def dijkstra_multiple(starts, destinations, edgefinder=lambda node: ((x, 1) for x in node)):
    """
    :param starts: iterable of any type, only used as keys.
    :param destinations: a set or other container of valid destinations to search for paths to
    :param edgefinder: A function that returns an iterable of tuples
        of (neighbor, distance) from the node it is passed
    :return: Like dijkstra(), but accepts a container (like a set) of destinations instead of a predicate.
        May yield fewer items than the length of the set if not all are pathable.
    """
    num_to_find = len(destinations)
    # stop trying to find results after we have one for each possible destination
    for result, _ in zip(dijkstra(starts, (lambda x: x in destinations), edgefinder), range(num_to_find)):
        yield result


def convert_path(path):
    """Convert a reverse linked tuple path (3, (2, (1, ()))) to a forwards list [1, 2, 3]."""
    result = []
    while path:
        result.append(path[0])
        path = path[1]
    result.reverse()
    return result


# TODO: spanning tree algorithms: Prim, Wilson
