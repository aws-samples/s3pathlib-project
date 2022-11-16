# -*- coding: utf-8 -*-

"""
Upload file from local to s3.
"""

import typing as T

from pathlib_mate import Path
from .resolve_s3_client import resolve_s3_client
from .. import utils
from ..aws import context

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


class UploadAPIMixin:
    """
    A mixin class that implements upload method.
    """

    def upload_file(
        self: "S3Path",
        path: str,
        overwrite: bool = False,
        extra_args: dict = None,
        callback: callable = None,
        config=None,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> dict:
        """
        Upload a file from local file system to targeted S3 path

        Example::

            >>> s3path = S3Path("bucket", "artifacts", "deployment.zip")
            >>> s3path.upload_file(path="/tmp/build/deployment.zip", overwrite=True)

        :param path: absolute path of the file on the local file system
            you want to upload
        :param overwrite: if False, non of the file will be upload / overwritten
            if any of target s3 location already taken.

        .. versionadded:: 1.0.1
        """
        self.ensure_object()
        if overwrite is False:
            self.ensure_not_exists(bsm=bsm)
        p = Path(path)
        s3_client = resolve_s3_client(context, bsm)
        return s3_client.upload_file(
            p.abspath,
            Bucket=self.bucket,
            Key=self.key,
            ExtraArgs=extra_args,
            Callback=callback,
            Config=config,
        )

    def upload_dir(
        self: "S3Path",
        local_dir: str,
        pattern: str = "**/*",
        overwrite: bool = False,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> int:
        """
        Upload a directory on local file system and all sub-folders, files to
        a S3 prefix (logical directory)

        Example::

            >>> s3path = S3Path("bucket", "datalake", "orders/")
            >>> s3path.upload_dir(path="/data/orders", overwrite=True)

        :param local_dir: absolute path of the directory on the
            local file system you want to upload
        :param pattern: linux styled glob pattern match syntax. see this
            official reference
            https://docs.python.org/3/library/pathlib.html#pathlib.Path.glob
            for more details
        :param overwrite: if False, non of the file will be upload / overwritten
            if any of target s3 location already taken.

        :return: number of files uploaded

        .. versionadded:: 1.0.1
        """
        self.ensure_dir()
        s3_client = resolve_s3_client(context, bsm)
        return utils.upload_dir(
            s3_client=s3_client,
            bucket=self.bucket,
            prefix=self.key,
            local_dir=local_dir,
            pattern=pattern,
            overwrite=overwrite,
        )
