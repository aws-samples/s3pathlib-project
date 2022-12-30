# -*- coding: utf-8 -*-

"""
Smart open library integration.
"""

import typing as T

from .resolve_s3_client import resolve_s3_client
from ..aws import context
from ..compat import smart_open, compat
from ..type import MetadataType, TagType
from ..tag import encode_url_query

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
        multipart_upload: bool = True,
        metadata: T.Optional[MetadataType] = None,
        tags: T.Optional[TagType] = None,
        transport_params: T.Optional[dict] = None,
        bsm: T.Optional['BotoSesManager'] = None,
    ):
        """
        Open S3Path as a file-liked object.

        :param mode: "r", "w", "rb", "wb"
        :param compression: whether do you want to compress the content
        :param multipart_upload: do you want to use multi-parts upload,
            by default it is True
        :param metadata: also put the user defined metadata dictionary
        :param tags: also put the tag dictionary
        :param bsm: optional ``boto_session_manager.BotoSesManager`` object

        :return: a file-like object that has ``read()`` and ``write()`` method.

        See https://github.com/RaRe-Technologies/smart_open for more info.

        .. versionadded:: 1.0.1

        .. versionchanged:: 1.2.1
        """
        s3_client = resolve_s3_client(context, bsm)
        if transport_params is None:
            transport_params = dict()
        transport_params["client"] = s3_client
        transport_params["multipart_upload"] = multipart_upload

        open_kwargs = dict(
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
            open_kwargs["ignore_ext"] = ignore_ext
        if compat.smart_open_version_major >= 5 and compat.smart_open_version_major >= 1:  # pragma: no cover
            if compression is not None:
                open_kwargs["compression"] = compression

        # if any of additional parameters exists, we need additional handling
        if sum([metadata is not None, tags is not None]) > 0:
            kwargs = {}
            if metadata is not None:
                kwargs["Metadata"] = metadata
            if tags is not None:
                kwargs["Tagging"] = encode_url_query(tags)

            if multipart_upload:
                client_kwargs = {"S3.Client.create_multipart_upload": kwargs}
            else:
                client_kwargs = {"S3.Client.put_object": kwargs}
            if "client_kwargs" in transport_params: # pragma: no cover
                transport_params["client_kwargs"].update(client_kwargs)
            else:
                transport_params["client_kwargs"] = client_kwargs
        return smart_open.open(**open_kwargs)
