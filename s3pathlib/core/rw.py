# -*- coding: utf-8 -*-

"""
Read and write related API.
"""

import typing as T

from .base import resolve_s3_client
from .. import client as better_client
from ..type import TagType, MetadataType
from ..aws import context

if T.TYPE_CHECKING: # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager

MixinReadWriteAPI: T.Type['S3Path']


class MixinReadWriteAPI:
    def read_text(
        self: 'S3Path',
        encoding="utf-8",
        errors=None,
        bsm: T.Optional['BotoSesManager'] = None,
    ) -> str:
        with self.open(
            mode="r",
            encoding=encoding,
            errors=errors,
            bsm=bsm,
        ) as f:
            return f.read()

    def read_bytes(
        self: 'S3Path',
        bsm: T.Optional['BotoSesManager'] = None,
    ) -> bytes:
        with self.open(mode="rb", bsm=bsm) as f:
            return f.read()

    def write_text(
        self: 'S3Path',
        data: str,
        encoding="utf-8",
        errors=None,
        metadata: T.Optional[MetadataType] = None,
        tags: T.Optional[TagType] = None,
        bsm: T.Optional['BotoSesManager'] = None,
    ):
        """
        Write text to s3 object. A simple wrapper around
        `s3_client.put_object <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.put_object>`_

        :param data: the text you want to write.
        :param encoding: how do you want to encode text?
        :param errors: how do you want to handle encode error? can be 'strict',
            ``ignore``, ``replace``, ``xmlcharrefreplace``, ``backslashreplace``.
            see more details `here <https://docs.python.org/3/library/stdtypes.html#str.encode>`_.
        :param metadata: the s3 object metadata in string key value pair dict.
        :param tags: the s3 object tags in string key value pair dict.

        .. versionadded:: 1.0.3

        .. versionchanged:: 1.1.1
        """
        s3_client = resolve_s3_client(context, bsm)
        if errors:
            body = data.encode(encoding, errors=errors)
        else:
            body = data.encode(encoding)
        response = better_client.put_object(
            s3_client=s3_client,
            bucket=self.bucket,
            key=self.key,
            body=body,
            metadata=metadata,
            tags=tags,
        )
        self._tags = tags
        return response

    def write_bytes(
        self: 'S3Path',
        data: bytes,
        metadata: T.Optional[dict] = None,
        tags: T.Optional[TagType] = None,
        bsm: T.Optional['BotoSesManager'] = None,
    ):
        """
        Write binary data to s3 object. A simple wrapper around
        `s3_client.put_object <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.put_object>`_

        :param data: the text you want to write.
        :param metadata: the s3 object metadata in string key value pair dict.
        :param tags: the s3 object tags in string key value pair dict.

        .. versionadded:: 1.0.3

        .. versionchanged:: 1.1.1
        """
        s3_client = resolve_s3_client(context, bsm)
        response = better_client.put_object(
            s3_client=s3_client,
            bucket=self.bucket,
            key=self.key,
            body=data,
            metadata=metadata,
            tags=tags,
        )
        self._tags = tags
        return response
