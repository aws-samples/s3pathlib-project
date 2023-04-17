# -*- coding: utf-8 -*-

import pytest

from s3pathlib.marker import (
    deprecate_v1,
    deprecate_v2,
)
from s3pathlib.tests import run_cov_test


def test_deprecate_v1():
    @deprecate_v1(
        version="1.1.1",
        message="THIS IS FOR UNITTEST: please use another method instead",
    )
    def my_func(a: int, b: int) -> int:
        return a + b

    class MyClass:
        @deprecate_v1(
            version="1.1.1",
            message="THIS IS FOR UNITTEST: please use another method instead",
        )
        def my_method(self, a: int, b: int):
            return a + b

    with pytest.warns():
        assert MyClass().my_method(a=1, b=2) == 3
        assert my_func(1, 2) == 3


def test_deprecate_v2():
    @deprecate_v2(
        version="1.1.1",
        message="THIS IS FOR UNITTEST: please use another method instead",
    )
    def my_func(a: int, b: int) -> int:
        return a + b

    class MyClass:
        @deprecate_v2(
            version="1.1.1",
            message="THIS IS FOR UNITTEST: please use another method instead",
        )
        def my_method(self, a: int, b: int):
            return a + b

    with pytest.warns():
        assert MyClass().my_method(a=1, b=2) == 3
        assert my_func(1, 2) == 3


if __name__ == "__main__":
    run_cov_test(__file__, "s3pathlib.marker", preview=False)
