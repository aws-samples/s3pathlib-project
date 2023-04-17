# -*- coding: utf-8 -*-

"""
Improve the delete_object_ API.

.. _delete_object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
"""

import typing as T


import botocore.exceptions
from func_args import NOTHING, resolve_kwargs

from .. import exc

if T.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


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
) -> bool:
    """
    Wrapper of delete_object_.

    :param ignore_not_found: Default is ``False``; if ``True``, silently
        return 0 if the object does not exist.

    :return: A boolean flag to indicate if a deletion happened or not
    """
    try:
        s3_client.delete_object(
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
        return True
    except botocore.exceptions.ClientError as e:
        if "Not Found" in str(e):
            if ignore_not_found:
                return False
            else:
                raise exc.S3ObjectNotExist.make(f"s3://{bucket}/{key}")
        else:  # pragma: no cover
            raise e
