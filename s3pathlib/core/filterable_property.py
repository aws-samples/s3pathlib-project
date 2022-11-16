# -*- coding: utf-8 -*-

"""
Many popular ORM framework offers a popular feature that can use the following
syntax to filter an iterable of object by its attributes. For example, in
`sqlalchemy <https://docs.sqlalchemy.org/en/14/orm/quickstart.html#simple-select>`_
you can use:

.. code-block:: python

    class User(Base):
        name: str

    select(User).where(User.name == "alice"))

This module implements the same feature.
"""

import typing as T
import functools

FilterableType = T.TypeVar("FilterableType")


class FilterableProperty(T.Generic[FilterableType]):
    """
    A descriptor decorator that convert a method to a property method.
    ALSO, convert the class attribute to be a comparable object that returns
    filter function for :class:`~s3pathlib.iterproxy.IterProxy` to use.

    .. code-block:: python

        class User:
            def __init__(self, name: str):
                self.name = name

            @FilterableProperty
            def username(self) -> str:
                return self.name

        filter_function = User.username == "alice
        assert filter_function(User(name="alice")) == True
        assert filter_function(User(name="bob")) == False
    """

    def __init__(self, func: callable):
        functools.wraps(func)(self)
        self._func = func

    def __get__(self, obj: T.Union['FilterableType', None], obj_type):
        if obj is None:
            return self
        return self._func(obj)

    def __eq__(self, other):
        def filter_(obj):
            return self._func(obj) == other

        return filter_

    def __ne__(self, other):
        def filter_(obj):
            return self._func(obj) != other

        return filter_

    def __gt__(self, other):
        def filter_(obj):
            return self._func(obj) > other

        return filter_

    def __lt__(self, other):
        def filter_(obj):
            return self._func(obj) < other

        return filter_

    def __ge__(self, other):
        def filter_(obj):
            return self._func(obj) >= other

        return filter_

    def __le__(self, other):
        def filter_(obj):
            return self._func(obj) <= other

        return filter_

    def equal_to(self, other):  # pragma: no cover
        """
        Return a filter function that returns True
        only if ``FilterableType.attribute_name == ``other``

        .. versionadded:: 1.0.3
        """
        return self.__eq__(other)

    def not_equal_to(self, other):  # pragma: no cover
        """
        Return a filter function that returns True
        only if ``FilterableType.attribute_name != ``other``

        .. versionadded:: 1.0.4
        """
        return self.__ne__(other)

    def greater(self, other):  # pragma: no cover
        """
        Return a filter function that returns True
        only if ``FilterableType.attribute_name > ``other``

        .. versionadded:: 1.0.4
        """
        return self.__gt__(other)

    def less(self, other):  # pragma: no cover
        """
        Return a filter function that returns True
        only if ``FilterableType.attribute_name < ``other``

        .. versionadded:: 1.0.4
        """
        return self.__lt__(other)

    def greater_equal(self, other):  # pragma: no cover
        """
        Return a filter function that returns True
        only if ``FilterableType.attribute_name >= ``other``

        .. versionadded:: 1.0.4
        """
        return self.__eq__(other)

    def less_equal(self, other):  # pragma: no cover
        """
        Return a filter function that returns True
        only if ``FilterableType.attribute_name <= ``other``

        .. versionadded:: 1.0.4
        """
        return self.__eq__(other)

    def between(self, lower, upper):
        """
        Return a filter function that returns True
        only if ``lower <= FilterableType.attribute_name <= upper``

        .. versionadded:: 1.0.3
        """

        def filter_(obj):
            return lower <= self._func(obj) <= upper

        return filter_

    def startswith(self, other: str):
        """
        Return a filter function that returns True
        only if ``FilterableType.attribute_name.startswith(other)``.
        The attribute has to be a string attribute.

        .. versionadded:: 1.0.3
        """

        def filter_(obj):
            return self._func(obj).startswith(other)

        return filter_

    def endswith(self, other: str):
        """
        Return a filter function that returns True
        only if ``FilterableType.attribute_name.endswith(other)``.
        The attribute has to be a string attribute.

        .. versionadded:: 1.0.3
        """

        def filter_(obj):
            return self._func(obj).endswith(other)

        return filter_

    def contains(self, other):
        """
        Return a filter function that returns True
        only if ``other in FilterableType.attribute_name``

        .. versionadded:: 1.0.3
        """

        def filter_(obj):
            return other in self._func(obj)

        return filter_
