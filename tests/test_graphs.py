# coding=utf-8
import pytest


directions = (
    (1, 0),
    (0, 1),
    (-1, 0),
    (0, -1),
)


def directional_neighbors(node):
    x, y = node
    for dx, dy in directions:
        yield (x + dx, y + dy)


def test_multiple_paths_3x3():
    from mumblecode.graphs import astar

    def in_bounds(node):
        """bounds check for 3x3 grid"""
        return min(*node) >= 0 and max(*node) < 3

    def edgefinder(node):
        for nb in directional_neighbors(node):
            if in_bounds(nb):
                yield nb, 1

    count = 0
    for _ in astar(
        [(0, 0)],
        lambda n: n == (2, 2),
        edgefinder,
        tol=0
    ):
        count += 1

    # there should be exactly 6 distinct shortest paths through a 3x3 grid
    assert count == 6


MULTIPLE_PATHS = [
    (None, 1),
    (0, 2),
    (1, 3),
]


@pytest.mark.parametrize('tolerance,expected', MULTIPLE_PATHS)
def test_multiple_path_tolerance(tolerance, expected):
    from mumblecode.graphs import astar

    edges = {
        'start': [(0, 1), (1, 1), (2, 2)],
        0: [('end', 1)],
        1: [('end', 1)],
        2: [('end', 1)],
        'end': [],
    }

    count = 0
    for _ in astar(
        ['start'],
        lambda n: n == 'end',
        lambda n: edges[n],
        tol=tolerance
    ):
        count += 1

    assert count == expected
