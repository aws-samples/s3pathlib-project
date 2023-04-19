# -*- coding: utf-8 -*-

"""
Improve the head_object_ API.

.. _head_object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object
"""

import typing as T
from datetime import datetime

import botocore.exceptions
from func_args import NOTHING, resolve_kwargs

from .. import exc

if T.TYPE_CHECKING: # pragma: no cover
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.type_defs import HeadObjectOutputTypeDef


def head_object(
    s3_client: "S3Client",
    bucket: str,
    key: str,
    if_match: str = NOTHING,
    if_modified_since: datetime = NOTHING,
    if_none_match: str = NOTHING,
    if_unmodified_since: datetime = NOTHING,
    range: str = NOTHING,
    version_id: str = NOTHING,
    sse_customer_algorithm: str = NOTHING,
    sse_customer_key: str = NOTHING,
    request_payer: str = NOTHING,
    part_number: int = NOTHING,
    expected_bucket_owner: str = NOTHING,
    checksum_mode: str = NOTHING,
    ignore_not_found: bool = False,
) -> T.Optional[T.Union[dict, "HeadObjectOutputTypeDef"]]:
    """
    Wrapper of head_object_.

    Example:

        >>> response = head_object(s3_client, "my-bucket", "file.txt")
        >>> if response is None:
        ...     print("Object not found")
        ... else:
        ...     print(response["LastModified"])

    :param s3_client: See head_object_
    :param bucket: See head_object_
    :param key: See head_object_
    :param if_match: See head_object_
    :param if_modified_since: See head_object_
    :param if_none_match: See head_object_
    :param if_unmodified_since: See head_object_
    :param range: See head_object_
    :param version_id: See head_object_
    :param sse_customer_algorithm: See head_object_
    :param sse_customer_key: See head_object_
    :param request_payer: See head_object_
    :param part_number: See head_object_
    :param expected_bucket_owner: See head_object_
    :param checksum_mode: See head_object_
    :param ignore_not_found: Default is ``False``; if ``True``, return ``None``
        when object is not found instead of raising an error.

    :return: See head_object_
    """
    try:
        dct = s3_client.head_object(
            **resolve_kwargs(
                Bucket=bucket,
                Key=key,
                IfMatch=if_match,
                IfModifiedSince=if_modified_since,
                IfNoneMatch=if_none_match,
                IfUnmodifiedSince=if_unmodified_since,
                Range=range,
                VersionId=version_id,
                SSECustomerAlgorithm=sse_customer_algorithm,
                SSECustomerKey=sse_customer_key,
                RequestPayer=request_payer,
                PartNumber=part_number,
                ExpectedBucketOwner=expected_bucket_owner,
                ChecksumMode=checksum_mode,
            )
        )
        return dct
    except botocore.exceptions.ClientError as e:
        if "Not Found" in str(e):
            if ignore_not_found:
                return None
            else:
                raise exc.S3FileNotExist.make(f"s3://{bucket}/{key}")
        else:  # pragma: no cover
            raise e


def is_object_exists(
    s3_client: "S3Client",
    bucket: str,
    key: str,
    version_id: str = NOTHING,
) -> bool:
    """
    Check if an object exists. If you want to use the head_object_ API response
    immediately when the object exists, use :func:`head_object` instead.

    Example::

        >>> is_object_exists(s3_client, "my-bucket", "file.txt")
        True

    :param s3_client: See head_object_
    :param bucket: See head_object_
    :param key: See head_object_
    :param version_id: See head_object_

    :return: A Boolean flag to indicate whether the object exists.
    """
    response = head_object(
        s3_client=s3_client,
        bucket=bucket,
        key=key,
        version_id=version_id,
        ignore_not_found=True,
    )
    if response is None:
        return False
    else:
        return True
