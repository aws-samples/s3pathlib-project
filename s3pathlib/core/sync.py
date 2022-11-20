# -*- coding: utf-8 -*-

"""
Sync file, folder between s3-to-s3, s3-to-local, local-to-s3.
"""

import typing as T

import subprocess

from ..aws import context


if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


class SyncAPIMixin:
    """
    A mixin class that implements aws s3 sync feature.
    """

    @classmethod
    def sync(
        cls: T.Type["S3Path"],
        src: T.Union["S3Path", str],
        dst: T.Union["S3Path", str],
        bsm: T.Optional["BotoSesManager"] = None,
    ):
        """
        Implement the `aws s3 sync <https://docs.aws.amazon.com/cli/latest/reference/s3/sync.html>`_
        CLI behavior.

        :param src:
        :param dst:

        .. versionadded:: 1.2.1

        TODO: add support for all aws s3 sync supported arguments
        """

        args = [
            "aws",
            "s3",
            "sync",
        ]

        if isinstance(src, str):
            if src.startswith("s3://"):
                src_is_s3 = True
            else:
                src_is_s3 = False
            src_arg = src
        else:
            src_is_s3 = True
            src_arg = src.uri

        if isinstance(dst, str):
            if dst.startswith("s3://"):
                dst_is_s3 = True
            else:
                dst_is_s3 = False
            dst_arg = dst
        else:
            dst_is_s3 = True
            dst_arg = dst.uri

        if src_is_s3 is False and dst_is_s3 is False:
            raise ValueError

        args.extend([src_arg, dst_arg])

        if bsm is None:  # pragma: no cover
            if context.boto_ses.profile_name:
                if context.boto_ses.profile_name != "default":
                    args.extend(["--profile", context.boto_ses.profile_name])
        else:  # pragma: no cover
            if bsm.boto_ses.profile_name:
                if bsm.boto_ses.profile_name != "default":
                    args.extend(["--profile", context.boto_ses.profile_name])

        response = subprocess.run(args)
        if response.returncode != 0: # pragma: no cover
            raise SystemError

    def sync_from(
        self: "S3Path",
        src: T.Union["S3Path", str],
        bsm: T.Optional["BotoSesManager"] = None,
    ):
        """
        Sync data from external place to this S3 location.
        """
        return self.sync(src=src, dst=self, bsm=bsm)

    def sync_to(
        self: "S3Path",
        dst: T.Union["S3Path", str],
        bsm: T.Optional["BotoSesManager"] = None,
    ):
        """
        Sync the data at this S3 location to external place.
        """
        return self.sync(src=self, dst=dst, bsm=bsm)
