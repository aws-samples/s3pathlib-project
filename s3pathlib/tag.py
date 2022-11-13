# -*- coding: utf-8 -*-

"""
This module provides AWS Tags manipulation helpers.
"""

import typing as T
from urllib.parse import urlencode


def parse_tags(
    data: T.Union[
        T.List[T.Dict[str, str]]
    ]
) -> T.Dict[str, str]:
    """
    Convert tags to pythonic dictionary.
    """
    if isinstance(data, list):
        return {dct["Key"]: dct["Value"] for dct in data}
    else:  # pragma: no cover
        raise NotImplementedError


def encode_tag_set(tags: dict) -> T.List[T.Dict[str, str]]:
    """
    Some API requires: ``[{"Key": "name", "Value": "Alice"}, {...}, ...]``
    for tagging parameter.
    """
    return [
        {"Key": k, "Value": v}
        for k, v in tags.items()
    ]


def encode_url_query(tags: dict) -> str:
    """
    Some API requires: ``Key1=Value1&Key2=Value2`` for tagging parameter.
    """
    return urlencode(tags)
