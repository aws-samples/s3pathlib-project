# -*- coding: utf-8 -*-

"""
This module provides AWS Tags manipulation helpers.

Note:

- This module is not for public API
"""

import typing as T
from urllib.parse import urlencode
from .type import TagType, TagSetType


def parse_tags(
    data: T.Union[
        TagSetType,
    ]
) -> TagType:
    """
    Convert tags to pythonic dictionary.
    """
    if isinstance(data, list):
        return {dct["Key"]: dct["Value"] for dct in data}
    else:  # pragma: no cover
        raise NotImplementedError


def encode_tag_set(tags: TagType) -> TagSetType:
    """
    Some API requires: ``[{"Key": "name", "Value": "Alice"}, {...}, ...]``
    for tagging parameter.
    """
    return [
        {"Key": k, "Value": v}
        for k, v in tags.items()
    ]


def encode_url_query(tags: TagType) -> str:
    """
    Some API requires: ``Key1=Value1&Key2=Value2`` for tagging parameter.
    """
    return urlencode(tags)
