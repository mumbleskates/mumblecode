# coding=utf-8
from mumblecode.tools.collections import IntervalMapping, WeightedSet
from itertools import chain


def simplify(x):
    return list(chain.from_iterable(x))


def test_intervalmapping():
    im = IntervalMapping()
    assert simplify(im) == []
    im[1:2] = 'a'
    assert simplify(im) == [1, 2, 'a']
    im[9:10] = 'b'
    assert simplify(im) == [1, 2, 'a', 9, 10, 'b']
    im[4:5] = 'between'
    assert simplify(im) == [1, 2, 'a', 4, 5, 'between', 9, 10, 'b']
    im.clear()
    assert simplify(im) == []
    im[1:10] = 'wabl'
    assert simplify(im) == [1, 10, 'wabl']
    im[5:15] = 'cover end'
    assert simplify(im) == [1, 5, 'wabl', 5, 15, 'cover end']
    im[0:3] = 'cover beginning'
    assert simplify(im) == [0, 3, 'cover beginning', 3, 5, 'wabl', 5, 15, 'cover end']
    im[0:15] = 'cover exactly'
    assert simplify(im) == [0, 15, 'cover exactly']
    im[5:6] = 'inset'
    assert simplify(im) == [0, 5, 'cover exactly', 5, 6, 'inset', 6, 15, 'cover exactly']
    im[-100:100] = 'cover'
    assert simplify(im) == [-100, 100, 'cover']
    del im[-10:60]
    assert simplify(im) == [-100, -10, 'cover', 60, 100, 'cover']
    for i in range(10):
        im[i:i + 1] = 'v' + str(i)
    assert simplify(im) == [-100, -10, 'cover', 0, 1, 'v0', 1, 2, 'v1', 2, 3, 'v2', 3, 4, 'v3', 4, 5, 'v4', 5, 6, 'v5',
                            6, 7, 'v6', 7, 8, 'v7', 8, 9, 'v8', 9, 10, 'v9', 60, 100, 'cover']
    del im[3.2:]
    assert simplify(im) == [-100, -10, 'cover', 0, 1, 'v0', 1, 2, 'v1', 2, 3, 'v2', 3, 3.2, 'v3']
    del im[:.6]
    assert simplify(im) == [.6, 1, 'v0', 1, 2, 'v1', 2, 3, 'v2', 3, 3.2, 'v3']
    del im[:]
    assert simplify(im) == []
    im[1:10] = 'a'
    im[10:20] = 'b'
    im[5:15] = 'c'
    assert simplify(im) == [1, 5, 'a', 5, 15, 'c', 15, 20, 'b']
    im[-100:-90] = 'before'
    assert simplify(im) == [-100, -90, 'before', 1, 5, 'a', 5, 15, 'c', 15, 20, 'b']
    im[90:100] = 'after'
    assert simplify(im) == [-100, -90, 'before', 1, 5, 'a', 5, 15, 'c', 15, 20, 'b', 90, 100, 'after']
    im[50:60] = 'between'
    assert simplify(im) == [-100, -90, 'before', 1, 5, 'a', 5, 15, 'c', 15, 20, 'b', 50, 60, 'between', 90, 100,
                            'after']


def test_weighted_set():
    def test_ws(length):
        w = WeightedSet((i, 1) for i in range(length))
        return len(set(w.choose(i) for i in range(length)))

    assert all(test_ws(n) == n for n in range(1, 50))
