# -*- coding: utf-8 -*-

"""
Improve the delete_object_ API.

.. _delete_object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
.. _delete_objects: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_objects.html
"""

import typing as T


import botocore.exceptions
from func_args import NOTHING, resolve_kwargs

from .. import exc
from ..utils import grouper_list, ensure_s3_dir
from .list_objects import (
    paginate_list_objects_v2,
    is_content_an_object,
)
from .list_object_versions import paginate_list_object_versions


if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.type_defs import (
        DeleteObjectOutputTypeDef,
    )


def delete_object(
    s3_client: "S3Client",
    bucket: str,
    key: str,
    version_id: str = NOTHING,
    mfa: str = NOTHING,
    request_payer: str = NOTHING,
    bypass_governance_retention: bool = NOTHING,
    expected_bucket_owner: str = NOTHING,
    ignore_not_found: bool = False,
) -> T.Optional["DeleteObjectOutputTypeDef"]:
    """
    Wrapper of delete_object_.

    :param s3_client: ``boto3.session.Session().client("s3")`` object.
    :param bucket: See delete_object_.
    :param key: See delete_object_.
    :param version_id: See delete_object_.
    :param mfa: See delete_object_.
    :param request_payer: See delete_object_.
    :param bypass_governance_retention: See delete_object_.
    :param expected_bucket_owner: See delete_object_.
    :param ignore_not_found: Default is ``False``; if ``True``, silently
        return 0 if the object does not exist.

    :return: See delete_object_.
    """
    try:
        response = s3_client.delete_object(
            **resolve_kwargs(
                Bucket=bucket,
                Key=key,
                MFA=mfa,
                VersionId=version_id,
                RequestPayer=request_payer,
                BypassGovernanceRetention=bypass_governance_retention,
                ExpectedBucketOwner=expected_bucket_owner,
            )
        )
        return response
    except botocore.exceptions.ClientError as e:  # pragma: no cover
        if "Not Found" in str(e):
            if ignore_not_found:
                return None
            else:
                raise exc.S3FileNotExist.make(f"s3://{bucket}/{key}")
        else:  # pragma: no cover
            raise e


def delete_dir(
    s3_client,
    bucket: str,
    prefix: str,
    batch_size: int = 1000,
    limit: int = NOTHING,
    mfa: str = NOTHING,
    request_payer: str = NOTHING,
    bypass_governance_retention: bool = NOTHING,
    expected_bucket_owner: str = NOTHING,
    check_sum_algorithm: str = NOTHING,
    skip_prompt: bool = False,
) -> int:
    """
    Recursively delete all objects under a s3 prefix. It is a wrapper of
    delete_objects_. Include the hard folder itself.

    :param s3_client: ``boto3.session.Session().client("s3")`` object.
    :param bucket: S3 bucket name.
    :param prefix: The s3 prefix (logic directory) you want to delete,
        it has to be a directory (end with "/").
    :param batch_size: Number of s3 object to delete per micro-batch,
        valid value is from 1 ~ 1000. large number can reduce IO.
    :param limit: Total Number of s3 object to delete.
    :param mfa: See delete_object_.
    :param request_payer: See delete_object_.
    :param bypass_governance_retention: See delete_object_.
    :param expected_bucket_owner: See delete_object_.
    :param check_sum_algorithm: See delete_object_.
    :param skip_prompt: Default False, it will prompt you to confirm when deleting
        everything in an S3 bucket.

    :return: number of deleted objects

    .. versionadded:: 2.0.1
    """
    if prefix == "": # pragma: no cover
        if skip_prompt is False:
            answer = input(
                "You are deleting everything in an S3 Bucket, "
                "are you sure you want to do this? (YES/NO): "
            ).strip()
            if answer != "YES":
                print("Aborting")
                return 0
    else:
        ensure_s3_dir(prefix)

    contents_iterproxy = paginate_list_objects_v2(
        s3_client=s3_client,
        bucket=bucket,
        prefix=prefix,
        batch_size=batch_size,
        limit=limit,
        request_payer=request_payer,
        expected_bucket_owner=expected_bucket_owner,
    ).contents()

    count = 0
    for contents in grouper_list(contents_iterproxy, 1000):
        kwargs = resolve_kwargs(
            Bucket=bucket,
            Delete={"Objects": [dict(Key=dct["Key"]) for dct in contents]},
            MFA=mfa,
            RequestPayer=request_payer,
            BypassGovernanceRetention=bypass_governance_retention,
            ExpectedBucketOwner=expected_bucket_owner,
            ChecksumAlgorithm=check_sum_algorithm,
        )
        s3_client.delete_objects(**kwargs)
        count += len(contents)

    return count


def delete_object_versions(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    batch_size: int = 1000,
    limit: int = NOTHING,
    mfa: str = NOTHING,
    request_payer: str = NOTHING,
    bypass_governance_retention: bool = NOTHING,
    expected_bucket_owner: str = NOTHING,
    check_sum_algorithm: str = NOTHING,
    skip_prompt: bool = False,
) -> int:
    """
    Recursively delete all objects and their versions under a s3 prefix.
    It is a wrapper of delete_objects_. It will delete all historical versions
    and hard folder permanently.

    :param s3_client: ``boto3.session.Session().client("s3")`` object.
    :param bucket: S3 bucket name.
    :param prefix: The s3 prefix (logic directory) you want to delete,
        it has to be a directory (end with "/").
    :param batch_size: Number of s3 object to delete per micro-batch,
        valid value is from 1 ~ 1000. large number can reduce IO.
    :param limit: Total Number of s3 object to delete.
    :param mfa: See delete_object_.
    :param request_payer: See delete_object_.
    :param bypass_governance_retention: See delete_object_.
    :param expected_bucket_owner: See delete_object_.
    :param check_sum_algorithm: See delete_object_.
    :param skip_prompt: Default False, it will prompt you to confirm when deleting
        everything in an S3 bucket.

    :return: number of deleted objects

    .. versionadded:: 2.0.1
    """
    if prefix == "": # pragma: no cover
        if skip_prompt is False:
            answer = input(
                "You are deleting everything in an S3 Bucket, "
                "including all historical versions, "
                "are you sure you want to do this? (YES/NO): "
            ).strip()
            if answer != "YES":
                print("Aborting")
                return 0

    proxy = paginate_list_object_versions(
        s3_client=s3_client,
        bucket=bucket,
        prefix=prefix,
        batch_size=batch_size,
        limit=limit,
        expected_bucket_owner=expected_bucket_owner,
    )
    count = 0
    for key_and_version_id_pairs in grouper_list(
        proxy.iterate_key_and_version(),
        1000,
    ):
        kwargs = resolve_kwargs(
            Bucket=bucket,
            Delete={
                "Objects": [
                    dict(Key=key, VersionId=version_id)
                    for key, version_id in key_and_version_id_pairs
                ]
            },
            MFA=mfa,
            RequestPayer=request_payer,
            BypassGovernanceRetention=bypass_governance_retention,
            ExpectedBucketOwner=expected_bucket_owner,
            ChecksumAlgorithm=check_sum_algorithm,
        )
        s3_client.delete_objects(**kwargs)
        count += len(key_and_version_id_pairs)
    return count
