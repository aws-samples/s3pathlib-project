# -*- coding: utf-8 -*-

"""
Tagging related API.
"""

import typing as T
import botocore.exceptions

from .resolve_s3_client import resolve_s3_client
from .. import utils, client as better_client
from ..aws import context

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


class ExistsAPIMixin:
    """
    A mixin class that implements the exists test related methods.
    """
    def exists(
        self: "S3Path",
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> bool:
        """
        - For S3 bucket: check if the bucket exists. If you don't have the
            access, then it raise exception.
        - For S3 object: check if the object exists
        - For S3 directory: check if the directory exists, it returns ``True``
            even if the folder doesn't have any object.

        .. versionadded:: 1.0.1
        """
        if self.is_bucket():
            s3_client = resolve_s3_client(context, bsm)
            return better_client.is_bucket_exists(s3_client, self.bucket)
        elif self.is_file():
            s3_client = resolve_s3_client(context, bsm)
            dct = utils.head_object_if_exists(
                s3_client=s3_client,
                bucket=self.bucket,
                key=self.key,
            )
            if isinstance(dct, dict):
                self._meta = dct
                return True
            else:
                return False
        elif self.is_dir():
            l = list(
                self.iter_objects(
                    batch_size=1,
                    limit=1,
                    include_folder=True,
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
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> None:
        """
        A validator method ensure that it doesn't exists.

        .. versionadded:: 1.0.1
        """
        if self.exists(bsm=bsm):
            utils.raise_file_exists_error(self.uri)
