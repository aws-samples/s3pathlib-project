# -*- coding: utf-8 -*-

import pytest
from s3pathlib.iterproxy import IterProxy


def is_odd(i):
    return i % 2


def is_even(i):
    return not (i % 2)


def gte_7(i):
    return i >= 7


class TestIterProxy:
    def test_iterator_behavior(self):
        # play 1
        iter_proxy = IterProxy(range(10))

        with pytest.raises(TypeError):
            next(iter_proxy)

        assert iter_proxy.one() == 0
        assert iter_proxy.one() == 1
        assert iter_proxy.many(3) == [2, 3, 4]
        assert iter_proxy.many(3) == [5, 6, 7]
        assert iter_proxy.many(3) == [8, 9]

        with pytest.raises(StopIteration):
            iter_proxy.one()

        assert iter_proxy.one_or_none() is None

        # play 2
        iter_proxy = IterProxy(range(3))
        assert iter_proxy.all() == [0, 1, 2]

        # play 3
        iter_proxy = IterProxy(range(5))
        assert iter_proxy.many(2) == [0, 1]
        assert iter_proxy.all() == [2, 3, 4]

    def test_many(self):
        iter_proxy = IterProxy(range(5))
        assert iter_proxy.many(3) == [0, 1, 2]
        assert iter_proxy.many(3) == [3, 4]
        with pytest.raises(StopIteration):
            iter_proxy.many(3)

    def test_skip_case_1(self):
        iter_proxy = IterProxy(range(5))
        iter_proxy.skip(2)
        assert iter_proxy.all() == [2, 3, 4]

    def test_skip_case_2(self):
        assert IterProxy(range(5)).skip(2).all() == [2, 3, 4]

    def test_skip_case_3(self):
        iter_proxy = IterProxy(range(10))

        iter_proxy.skip(2)
        assert iter_proxy.many(2) == [2, 3]

        iter_proxy.skip(3)
        assert iter_proxy.many(2) == [7, 8]

        iter_proxy.skip(5)
        assert iter_proxy.all() == []

    def test_filter(self):
        assert list(IterProxy(range(10)).filter(is_odd)) == [1, 3, 5, 7, 9]
        assert list(IterProxy(range(10)).filter(is_even)) == [0, 2, 4, 6, 8]
        assert list(IterProxy(range(10)).filter(is_odd, is_even)) == []

        assert list(IterProxy(range(10)).filter(is_odd, gte_7)) == [7, 9]

    def test_freeze_filters_after_itartion(self):
        proxy = IterProxy(range(10))
        proxy.filter(is_odd)
        _ = proxy.one()
        with pytest.raises(PermissionError):
            proxy.filter(is_even)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
