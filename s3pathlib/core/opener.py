# -*- coding: utf-8 -*-

"""
Smart open library integration.

.. _bsm: https://github.com/aws-samples/boto-session-manager-project
.. _smart_open: https://github.com/RaRe-Technologies/smart_open
"""

import typing as T

from func_args import NOTHING, resolve_kwargs

from ..metadata import warn_upper_case_in_metadata_key
from ..aws import context
from ..compat import smart_open, compat
from ..type import MetadataType, TagType
from ..tag import encode_url_query

from .resolve_s3_client import resolve_s3_client

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
        version_id: T.Optional[str] = NOTHING,
        buffering: T.Optional[int] = -1,
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
        closefd=True,
        opener=None,
        ignore_ext: bool = False,
        compression: T.Optional[str] = None,
        multipart_upload: bool = True,
        metadata: T.Optional[MetadataType] = NOTHING,
        tags: T.Optional[TagType] = NOTHING,
        transport_params: T.Optional[dict] = None,
        bsm: T.Optional['BotoSesManager'] = None,
    ):
        """
        Open S3Path as a file-liked object.

        Example::

            >>> import json
            >>> with S3Path("s3://bucket/data.json").open("w") as f:
            ...     json.dump({"a": 1}, f)

            >>> with S3Path("s3://bucket/data.json").open("r") as f:
            ...     data = json.load(f)

        :param mode: "r", "w", "rb", "wb".
        :param version_id: optional version id you want to read from.
        :param buffering: See smart_open_.
        :param encoding: See smart_open_.
        :param errors: See smart_open_.
        :param newline: See smart_open_.
        :param closefd: See smart_open_.
        :param opener: See smart_open_.
        :param ignore_ext: See smart_open_.
        :param compression: whether do you want to compress the content.
        :param multipart_upload: do you want to use multi-parts upload,
            by default it is True.
        :param metadata: also put the user defined metadata dictionary.
        :param tags: also put the tag dictionary.
        :param bsm: See bsm_.

        :return: a file-like object that has ``read()`` and ``write()`` method.

        See smart_open_ for more info.
        Also see https://github.com/RaRe-Technologies/smart_open/blob/develop/howto.md#how-to-access-s3-anonymously
        for S3 related info.

        .. versionadded:: 1.0.1

        .. versionchanged:: 1.2.1

            add ``metadata`` and ``tags`` parameters

        .. versionchanged:: 2.0.1

            add ``version_id`` parameter
        """
        s3_client = resolve_s3_client(context, bsm)
        if transport_params is None:
            transport_params = dict()
        transport_params["client"] = s3_client
        transport_params["multipart_upload"] = multipart_upload
        # write API doesn't take version_id parameter
        # set it to NOTHING in case human made a mistake
        if mode.startswith("r") is False:  # pragma: no cover
            version_id = NOTHING
            if metadata is not NOTHING:
                warn_upper_case_in_metadata_key(metadata)
        if version_id is not NOTHING:
            transport_params["version_id"] = version_id

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
            s3_client_kwargs = resolve_kwargs(
                Metadata=metadata,
                Tagging=tags if tags is NOTHING else encode_url_query(tags),
            )
            if multipart_upload:
                client_kwargs = {"S3.Client.create_multipart_upload": s3_client_kwargs}
            else:
                client_kwargs = {"S3.Client.put_object": s3_client_kwargs}
            if "client_kwargs" in transport_params: # pragma: no cover
                transport_params["client_kwargs"].update(client_kwargs)
            else:
                transport_params["client_kwargs"] = client_kwargs
        return smart_open.open(**open_kwargs)
