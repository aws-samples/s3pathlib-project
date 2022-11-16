# -*- coding: utf-8 -*-

"""
Smart open library integration.
"""

import typing as T

from .resolve_s3_client import resolve_s3_client
from ..aws import context
from ..compat import smart_open, compat

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


class OpenerAPIMixin:
    """
    A mixin class that implements the file-object protocol.
    """
    def open(
        self: 'S3Path',
        mode: T.Optional[str] = "r",
        buffering: T.Optional[int] = -1,
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
        closefd=True,
        opener=None,
        ignore_ext: bool = False,
        compression: T.Optional[str] = None,
        transport_params: T.Optional[dict] = None,
        bsm: T.Optional['BotoSesManager'] = None,
    ):
        """
        Open S3Path as a file-liked object.

        :return: a file-like object.

        See https://github.com/RaRe-Technologies/smart_open for more info.
        """
        s3_client = resolve_s3_client(context, bsm)
        if transport_params is None:
            transport_params = dict()
        transport_params["client"] = s3_client
        kwargs = dict(
            uri=self.uri,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            closefd=closefd,
            opener=opener,
            transport_params=transport_params,
        )
        if compat.smart_open_version_major < 6:  # pragma: no cover
            kwargs["ignore_ext"] = ignore_ext
        if compat.smart_open_version_major >= 5 and compat.smart_open_version_major >= 1:  # pragma: no cover
            if compression is not None:
                kwargs["compression"] = compression
        return smart_open.open(**kwargs)
