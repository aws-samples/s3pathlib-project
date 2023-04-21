# -*- coding: utf-8 -*-

"""
Tagging related API.
"""

import typing as T
from func_args import NOTHING

from .. import exc
from ..better_client.head_bucket import is_bucket_exists
from ..better_client.head_object import head_object
from ..aws import context

from .resolve_s3_client import resolve_s3_client

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


class ExistsAPIMixin:
    """
    A mixin class that implements the exists test related methods.
    """

    def exists(
        self: "S3Path",
        version_id: str = NOTHING,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> bool:
        """
        Test if an S3Path object actually exists in S3 bucket.

        - For S3 bucket: check if the bucket exists. If you don't have the
            access, then it raise exception.
        - For S3 object: check if the object exists
        - For S3 directory: check if the directory exists, it returns ``True``
            even if the folder doesn't have any object.
        - For versioning enabled bucket, you can explicitly check if a specific
            version exists, otherwise it will check the latest version.

        Example:

            >>> S3Path("bucket").exists()
            >>> S3Path("bucket/file.txt").exists()
            >>> S3Path("bucket/folder/").exists()
            >>> S3Path("bucket/file.txt").exists(version_id="v123456")

        .. versionadded:: 1.0.1

        .. versionchanged:: 2.0.1

            Add ``version_id`` parameter.
        """
        if self.is_bucket():
            s3_client = resolve_s3_client(context, bsm)
            return is_bucket_exists(s3_client, self.bucket)
        elif self.is_file():
            s3_client = resolve_s3_client(context, bsm)
            dct = head_object(
                s3_client=s3_client,
                bucket=self.bucket,
                key=self.key,
                version_id=version_id,
                ignore_not_found=True,
            )
            if dct is None:
                return False
            else:
                if "ResponseMetadata" in dct:
                    del dct["ResponseMetadata"]
                self._meta = dct
                return True
        elif self.is_dir():
            l = list(
                self.iterdir(
                    batch_size=1,
                    limit=1,
                    bsm=bsm,
                )
            )
            if len(l):
                return True
            else:
                return False
        else:  # pragma: no cover
            raise TypeError

    def ensure_not_exists(
        self: "S3Path",
        version_id: str = NOTHING,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> None:
        """
        A validator method ensure that it doesn't exists.

        .. versionadded:: 1.0.1

        .. versionchanged:: 2.0.1

            Add ``version_id`` parameter.
        """
        if self.exists(version_id=version_id, bsm=bsm):
            raise exc.S3AlreadyExist(
                (
                    "cannot write to {}, s3 object ALREADY EXISTS! "
                    "open console for more details {}."
                ).format(self.uri, self.console_url)
            )
