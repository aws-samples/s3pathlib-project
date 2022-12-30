# -*- coding: utf-8 -*-

"""
S3 botocore client wrapper.

Note:

- This module is not for public API
"""

import typing as T

import botocore.exceptions

from .type import TagType, MetadataType
from .exc import S3ObjectNotExist
from .tag import parse_tags, encode_tag_set, encode_url_query


def is_bucket_exists(
    s3_client,
    bucket: str,
) -> bool:
    """
    Use head_bucket() api to check if an S3 bucket exists.

    wrapper of `head_bucket <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_bucket>`_
    """
    try:
        s3_client.head_bucket(Bucket=bucket)
        return True
    except botocore.exceptions.ClientError as e:
        if "Not Found" in str(e):
            return False
        else:  # pragma: no cover
            raise e


def head_object(
    s3_client,
    bucket: str,
    key: str,
) -> dict:
    """
    Use head_object() api to return metadata of an object.

    wrapper of `head_object <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object>`_
    """
    try:
        dct = s3_client.head_object(Bucket=bucket, Key=key)
        del dct["ResponseMetadata"]
        return dct
    except botocore.exceptions.ClientError as e:
        if "Not Found" in str(e):
            raise S3ObjectNotExist(str(e))
        else:  # pragma: no cover
            raise e


def head_object_or_none(
    s3_client,
    bucket: str,
    key: str,
) -> T.Optional[dict]:
    """
    Use head_object() api to return metadata of an object.

    Behavior:

    1. return ``dict`` head_object() api response if the object exists.
    2. return ``None`` if object does not exist.
    3. raise exception if other error raised.
    """
    try:
        return head_object(s3_client, bucket, key)
    except S3ObjectNotExist:
        return None


def put_object(
    s3_client,
    bucket: str,
    key: str,
    body: T.Optional[bytes] = None,
    metadata: T.Optional[MetadataType] = None,
    tags: T.Optional[TagType] = None,
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


def copy_object(
    s3_client,
    src_bucket: str,
    src_key: str,
    dst_bucket: str,
    dst_key: str,
    metadata: T.Optional[MetadataType] = None,
    tags: T.Optional[TagType] = None,
    **kwargs
) -> dict:
    """
    wrapper of `copy_object <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.copy_object>`_
    """
    api_kwargs = dict(
        Bucket=dst_bucket,
        Key=dst_key,
        CopySource=dict(
            Bucket=src_bucket,
            Key=src_key,
        ),
    )
    api_kwargs.update(kwargs)
    if metadata is not None:
        api_kwargs["Metadata"] = metadata
        api_kwargs["MetadataDirective"] = "REPLACE"
    if tags is not None:
        api_kwargs["Tagging"] = encode_url_query(tags)
        api_kwargs["TaggingDirective"] = "REPLACE"
    return s3_client.copy_object(**api_kwargs)
