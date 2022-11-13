# -*- coding: utf-8 -*-

"""
S3 botocore client wrapper.

Note:

- This module is not for public API
"""

import typing as T

from .type import TagType, MetadataType
from .exc import S3ObjectNotExist
from .tag import parse_tags, encode_tag_set, encode_url_query


def put_object(
    s3_client,
    bucket: str,
    key: str,
    body: T.Optional[bytes] = None,
    metadata: T.Optional[MetadataType] = None,
    tags: T.Optional[TagType] = None
) -> dict:
    """
    wrapper of `put_object <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object>`_
    """
    kwargs = dict(
        Bucket=bucket,
        Key=key,
    )
    if body is not None:
        kwargs["Body"] = body
    if metadata is not None:
        kwargs["Metadata"] = metadata
    if tags is not None:
        kwargs["Tagging"] = encode_url_query(tags)
    return s3_client.put_object(**kwargs)


def get_object_tagging(
    s3_client,
    bucket: str,
    key: str,
) -> TagType:
    """
    wrapper of `get_object_tagging <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object_tagging>`_
    """
    kwargs = dict(
        Bucket=bucket,
        Key=key,
    )
    try:
        response = s3_client.get_object_tagging(**kwargs)
        return parse_tags(response["TagSet"])
    except Exception as e:
        if "The specified key does not exist" in str(e):
            raise S3ObjectNotExist(f"s3://{bucket}/{key}")
        else:  # pragma: no cover
            raise e


def put_object_tagging(
    s3_client,
    bucket: str,
    key: str,
    tags: TagType,
):
    """
    wrapper of `put_object_tagging <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object_tagging>`_
    """
    kwargs = dict(
        Bucket=bucket,
        Key=key,
        Tagging={"TagSet": encode_tag_set(tags)},
    )
    return s3_client.put_object_tagging(**kwargs)


def update_object_tagging(
    s3_client,
    bucket: str,
    key: str,
    tags: TagType,
) -> TagType:
    """
    Allow you to use ``dict.update`` liked API to update s3 object tagging.
    """
    existing_tags = get_object_tagging(s3_client, bucket, key)
    existing_tags.update(tags)
    put_object_tagging(s3_client, bucket, key, existing_tags)
    return existing_tags
