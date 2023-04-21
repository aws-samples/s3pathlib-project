# -*- coding: utf-8 -*-

"""
Delete S3 file or folder.

.. _bsm: https://github.com/aws-samples/boto-session-manager-project
.. _delete_object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
"""

import typing as T
from func_args import NOTHING

from .resolve_s3_client import resolve_s3_client
from ..aws import context
from ..better_client.delete_object import (
    delete_object,
    delete_dir,
    delete_object_versions,
)
from ..marker import warn_deprecate


if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


class DeleteAPIMixin:
    """
    A mixin class that implements delete method.
    """
    def delete(
        self: "S3Path",
        version_id: str = NOTHING,
        mfa: str = NOTHING,
        request_payer: str = NOTHING,
        bypass_governance_retention: bool = NOTHING,
        expected_bucket_owner: str = NOTHING,
        check_sum_algorithm: str = NOTHING,
        is_hard_delete: bool = False,
        skip_prompt: bool = False,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> "S3Path":
        """
        Can delete:

        - an object.
        - all objects in a directory.
        - specific version or delete marker of an object.
        - all historical versions and delete markers of an object.
        - all objects, all versions in a directory.

        It won't raise any error if the object or the directory doesn't exist.

        Example:

            >>> s3path = S3Path.from_s3_uri("s3://my-bucket/my-file.txt")
            >>> s3dir = S3Path.from_s3_uri("s3://my-bucket/my-folder/")
            # Delete an object
            # for versioning enabled bucket, it just creates a delete maker
            >>> s3path.delete()
            # Delete all objects in a directory
            # for versioning enabled bucket, it just creates delete makers for all objects
            >>> s3path.delete()
            # Delete specific version or delete marker of an object
            >>> s3path.delete(version_id="v123456")
            # Delete all historical versions and delete markers of an object
            >>> s3path.delete(is_hard_delete=True)
            # Delete all objects, all versions in a directory
            >>> s3dir.delete(is_hard_delete=True)

        :param version_id: see delete_object_.
        :param mfa: see delete_object_.
        :param request_payer: see delete_object_.
        :param bypass_governance_retention: see delete_object_.
        :param expected_bucket_owner: see delete_object_.
        :param check_sum_algorithm: See delete_object_.
        :param is_hard_delete: if ``True``, then it will delete all versions
            of the object, then the data is permanently deleted.
        :param skip_prompt: Default False, it will prompt you to confirm when deleting
            everything in an S3 bucket.
        :param bsm: See bsm_.

        :return: a new ``S3Path`` object representing the deleted object

            - if it's a file and the versioning is NOT enabled, then it will
                return the deleted file itself.
            - if it's a file and the versioning is ENABLED,
                - if ``version_id`` is not given, then it will return the
                    ``S3Path`` representing the delete-marker.
                - if ``version_id`` is given, then it will return the ``S3Path``
                    representing the deleted version.
            - if it's a directory, then it will return the deleted folder itself.

        .. versionadded:: 2.0.1

            Use this method to replace the :meth:`DeleteAPIMixin.delete_if_exists` method.
        """
        s3_client = resolve_s3_client(context, bsm)
        bucket = self.bucket
        if self.is_file():
            if is_hard_delete:
                delete_object_versions(
                    s3_client=s3_client,
                    bucket=bucket,
                    prefix=self.key,
                    mfa=mfa,
                    request_payer=request_payer,
                    bypass_governance_retention=bypass_governance_retention,
                    expected_bucket_owner=expected_bucket_owner,
                    check_sum_algorithm=check_sum_algorithm,
                )
                return self

            response = delete_object(
                s3_client=s3_client,
                bucket=bucket,
                key=self.key,
                version_id=version_id,
                mfa=mfa,
                request_payer=request_payer,
                bypass_governance_retention=bypass_governance_retention,
                expected_bucket_owner=expected_bucket_owner,
                ignore_not_found=True,
            )
            # delete_object API succeeded
            if bool(response):
                del response["ResponseMetadata"]
                # print(response)
                # it could be a delete marker or a deleted version
                if "VersionId" in response:
                    s3path = self.copy()
                    s3path._meta = response
                    return s3path
                # not a versioned bucket
                else:
                    return self
            # object not exists, nothing happen
            else:  # pragma: no cover
                return self
        elif self.is_dir():
            if is_hard_delete:
                delete_object_versions(
                    s3_client=s3_client,
                    bucket=bucket,
                    prefix=self.key,
                    mfa=mfa,
                    request_payer=request_payer,
                    bypass_governance_retention=bypass_governance_retention,
                    expected_bucket_owner=expected_bucket_owner,
                    check_sum_algorithm=check_sum_algorithm,
                    skip_prompt=skip_prompt,
                )
                return self

            delete_dir(
                s3_client=s3_client,
                bucket=bucket,
                prefix=self.key,
                mfa=mfa,
                request_payer=request_payer,
                bypass_governance_retention=bypass_governance_retention,
                expected_bucket_owner=expected_bucket_owner,
                skip_prompt=skip_prompt,
            )
            return self
        else:  # pragma: no cover
            raise ValueError

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
        You can delete a specific version of an object, or remove a delete-marker
        using the ``version_id`` parameter. Not that it will permanenatly delete
        the data.

        Example:

            >>> S3Path.from_s3_uri("s3://my-bucket/my-file.txt").delete_if_exists()
            1 # number of object deleted
            >>> S3Path.from_s3_uri("s3://my-bucket/my-folder/").delete_if_exists()
            3 # number of object deleted

        :param mfa: see delete_object_.
        :param version_id: see delete_object_.
        :param request_payer: see delete_object_.
        :param bypass_governance_retention: see delete_object_.
        :param expected_bucket_owner: see delete_object_.
        :param bsm: See bsm_.

        :return: number of object is deleted

        .. versionadded:: 1.0.1

        .. deprecated:: 2.0.1

            This method will be removed in 3.X. Use ``delete`` instead.
        """
        warn_deprecate(
            func_name="S3Path.delete_if_exists",
            version="3.0.1",
            message="use S3Path.delete instead",
        )

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
