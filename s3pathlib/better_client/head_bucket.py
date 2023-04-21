# -*- coding: utf-8 -*-

"""
Improve the head_bucket_ API.

.. _head_bucket: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_bucket
"""

import typing as T

import botocore.exceptions


if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3 import S3Client


def is_bucket_exists(
    s3_client: "S3Client",
    bucket: str,
) -> bool:
    """
    Check if a bucket exists.

    Example::

        >>> is_bucket_exists(s3_client, "my-bucket")
        True

    :param s3_client: See head_bucket_
    :param bucket: See head_bucket_

    :return: A Boolean flag to indicate whether the bucket exists.
    """
    try:
        s3_client.head_bucket(Bucket=bucket)
        return True
    except botocore.exceptions.ClientError as e:
        if "Not Found" in str(e):
            return False
        else:  # pragma: no cover
            raise e
