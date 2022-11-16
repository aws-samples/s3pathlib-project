# -*- coding: utf-8 -*-

"""
Delete S3 file or folder.
"""

import typing as T

from .resolve_s3_client import resolve_s3_client
from .. import utils
from ..aws import context

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


class DeleteAPIMixin:
    """
    A mixin class that implements delete method.
    """

    def delete_if_exists(
        self: "S3Path",
        mfa: str = None,
        version_id: str = None,
        request_payer: str = None,
        bypass_governance_retention: bool = None,
        expected_bucket_owner: str = None,
        include_folder: bool = True,
        bsm: T.Optional["BotoSesManager"] = None,
    ):
        """
        Delete an object or an entire directory. Will do nothing
        if it doesn't exist.

        Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object

        :param include_folder: see :meth:`iter_objects`

        :return: number of object is deleted

        .. versionadded:: 1.0.1
        """
        s3_client = resolve_s3_client(context, bsm)
        if self.is_file():
            if self.exists(bsm=bsm):
                kwargs = dict(
                    Bucket=self.bucket,
                    Key=self.key,
                )
                additional_kwargs = utils.collect_not_null_kwargs(
                    MFA=mfa,
                    VersionId=version_id,
                    RequestPayer=request_payer,
                    BypassGovernanceRetention=bypass_governance_retention,
                    ExpectedBucketOwner=expected_bucket_owner,
                )
                kwargs.update(additional_kwargs)
                s3_client.delete_object(**kwargs)
                return 1
            else:
                return 0
        elif self.is_dir():
            return utils.delete_dir(
                s3_client=s3_client,
                bucket=self.bucket,
                prefix=self.key,
                mfa=mfa,
                request_payer=request_payer,
                bypass_governance_retention=bypass_governance_retention,
                expected_bucket_owner=expected_bucket_owner,
                include_folder=include_folder,
            )
        else:  # pragma: no cover
            raise ValueError
