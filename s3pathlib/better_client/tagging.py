# -*- coding: utf-8 -*-

"""
Update S3 bucket, object tagging.

.. _get_bucket_tagging: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_bucket_tagging.html
.. _put_bucket_tagging: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_bucket_tagging.html

.. _get_object_tagging: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object_tagging.html
.. _put_object_tagging: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object_tagging.html
"""

import typing as T
import botocore.exceptions
from func_args import NOTHING, resolve_kwargs

from .. import tag


if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3 import S3Client


def update_bucket_tagging(
    s3_client: "S3Client",
    bucket: str,
    tags: tag.TagType,
    checksum_algorithm: str = NOTHING,
    expected_bucket_owner: str = NOTHING,
) -> tag.TagType:
    """
    Allow you to use ``dict.update`` liked API to update s3 bucket tagging.
    It is a combination of get, update and put.

    :return: the updated tags in Python dict.
    """
    try:
        res = s3_client.get_bucket_tagging(
            **resolve_kwargs(
                Bucket=bucket,
                ExpectedBucketOwner=expected_bucket_owner,
            )
        )
        existing_tags = tag.parse_tags(res.get("TagSet", []))
    except botocore.exceptions.ClientError as e:
        if "NoSuchTagSet" in str(e):
            existing_tags = {}
        else: # pragma: no cover
            raise e

    existing_tags.update(tags)
    s3_client.put_bucket_tagging(
        **resolve_kwargs(
            Bucket=bucket,
            Tagging=dict(TagSet=tag.encode_for_put_bucket_tagging(existing_tags)),
            ChecksumAlgorithm=checksum_algorithm,
            ExpectedBucketOwner=expected_bucket_owner,
        )
    )
    return existing_tags


def update_object_tagging(
    s3_client: "S3Client",
    bucket: str,
    key: str,
    tags: tag.TagType,
    version_id: str = NOTHING,
    content_md5: str = NOTHING,
    checksum_algorithm: str = NOTHING,
    expected_bucket_owner: str = NOTHING,
    request_payer: str = NOTHING,
) -> T.Tuple[T.Optional[str], tag.TagType]:
    """
    Allow you to use ``dict.update`` liked API to update s3 object tagging.
    It is a combination of get, update and put.

    :return: the tuple of ``(version_id, tags)``, where version_id is optional,
        and tags is the updated tags in Python dict.
    """
    res = s3_client.get_object_tagging(
        **resolve_kwargs(
            Bucket=bucket,
            Key=key,
            VersionId=version_id,
            ExpectedBucketOwner=expected_bucket_owner,
            RequestPayer=request_payer,
        )
    )
    existing_version_id = res.get("VersionId", None)
    existing_tags = tag.parse_tags(res.get("TagSet", []))
    existing_tags.update(tags)
    s3_client.put_object_tagging(
        **resolve_kwargs(
            Bucket=bucket,
            Key=key,
            Tagging=dict(TagSet=tag.encode_for_put_object_tagging(existing_tags)),
            VersionId=res.get("VersionId", NOTHING),
            ContentMD5=content_md5,
            ChecksumAlgorithm=checksum_algorithm,
            ExpectedBucketOwner=expected_bucket_owner,
            RequestPayer=request_payer,
        )
    )
    return existing_version_id, existing_tags
