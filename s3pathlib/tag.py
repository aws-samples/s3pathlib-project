# -*- coding: utf-8 -*-

"""
This module provides AWS Tags manipulation helpers.

.. _get_bucket_tagging: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_bucket_tagging.html
.. _get_object_tagging: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object_tagging.html
.. _put_object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
.. _put_bucket_tagging: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_bucket_tagging.html
.. _put_object_tagging: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object_tagging.html
"""

from urllib.parse import urlencode

from .type import TagType, TagSetType


def parse_tags(data: TagSetType) -> TagType:
    """
    Convert the tag set in boto3 API response into pythonic dictionary key value
    pairs.

    - get_bucket_tagging_: it's a tag set.
    - get_object_tagging_: it's a tag set.

    :param data: the tag set in boto3 API response.
    :return: the pythonic dictionary key value pairs.
    """
    if isinstance(data, list):
        return {dct["Key"]: dct["Value"] for dct in data}
    else:  # pragma: no cover
        raise NotImplementedError


def encode_tag_set(tags: TagType) -> TagSetType:
    """
    Some API requires: ``[{"Key": "name", "Value": "Alice"}, {...}, ...]``
    for tagging parameter.

    Example::

        >>> encode_tag_set({"name": "Alice", ...})
        [{"Key": "name", "Value": "Alice"}, ...]
    """
    return [{"Key": k, "Value": v} for k, v in tags.items()]


def encode_url_query(tags: TagType) -> str:
    """
    Some API requires: ``Key1=Value1&Key2=Value2`` for tagging parameter.

    Example::

        >>> encode_url_query({"name": "Alice", ...})
        "name=Alice&..."
    """
    return urlencode(tags)


def encode_for_put_object(tags: TagType) -> str:
    """
    Encode tags for put_object_.
    """
    return encode_url_query(tags)


def encode_for_put_bucket_tagging(tags: TagType) -> TagSetType:
    """
    Encode tags for put_bucket_tagging_.
    """
    return encode_tag_set(tags)


def encode_for_put_object_tagging(tags: TagType) -> TagSetType:
    """
    Encode tags for put_object_tagging_.
    """
    return encode_tag_set(tags)
