# -*- coding: utf-8 -*-

from s3pathlib.core.filterable_property import FilterableProperty
from s3pathlib.tests import run_cov_test


class User:
    def __init__(self, name: str):
        self.name = name

    @FilterableProperty
    def username(self) -> str:
        return self.name


class TestFilterableProperty:
    def test(self):
        user = User(name="alice")
        func = User.username == "alice"
        assert func(user) is True
        assert func(User(name="Bob")) is False


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.filterable_property", open_browser=False)
