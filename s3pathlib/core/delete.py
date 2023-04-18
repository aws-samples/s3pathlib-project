# -*- coding: utf-8 -*-

"""
Delete S3 file or folder.

.. _delete_object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
"""

import typing as T
from func_args import NOTHING

from .resolve_s3_client import resolve_s3_client
from ..aws import context
from ..better_client.delete_object import delete_object, delete_dir

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


class DeleteAPIMixin:
    """
    A mixin class that implements delete method.
    """

    def delete_if_exists(
        self: "S3Path",
        version_id: str = NOTHING,
        mfa: str = NOTHING,
        request_payer: str = NOTHING,
        bypass_governance_retention: bool = NOTHING,
        expected_bucket_owner: str = NOTHING,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> int:
        """
        Delete an object or an entire directory. Will do nothing if it doesn't exist.

        Reference:

        - delete_object_

        :param mfa: see delete_object_.
        :param version_id: see delete_object_.
        :param request_payer: see delete_object_.
        :param bypass_governance_retention: see delete_object_.
        :param expected_bucket_owner: see delete_object_.
        :param bsm: ``boto_session_manager.BotoSesManager`` object.

        :return: number of object is deleted

        .. versionadded:: 1.0.1
        """
        s3_client = resolve_s3_client(context, bsm)
        if self.is_file():
            if self.exists(bsm=bsm):
                delete_object(
                    s3_client=s3_client,
                    bucket=self.bucket,
                    key=self.key,
                    version_id=mfa,
                    mfa=version_id,
                    request_payer=request_payer,
                    bypass_governance_retention=bypass_governance_retention,
                    expected_bucket_owner=expected_bucket_owner,
                    ignore_not_found=True,
                )
                return 1
            else:
                return 0
        elif self.is_dir():
            return delete_dir(
                s3_client=s3_client,
                bucket=self.bucket,
                prefix=self.key,
                mfa=mfa,
                request_payer=request_payer,
                bypass_governance_retention=bypass_governance_retention,
                expected_bucket_owner=expected_bucket_owner,
            )
        else:  # pragma: no cover
            raise ValueError
