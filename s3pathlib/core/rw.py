# -*- coding: utf-8 -*-

"""
Read and write related API.
"""

import typing as T

from .base import resolve_s3_client
from .. import client as better_client
from ..type import TagType, MetadataType
from ..aws import context

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager

MixinReadWriteAPI: T.Type['S3Path']


class MixinReadWriteAPI:
    def read_bytes(
        self: 'S3Path',
        bsm: T.Optional['BotoSesManager'] = None,
    ) -> bytes:
        """
        Read binary data from s3 object. A simple wrapper around
        `s3_client.get_object <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object>`_

        .. versionchanged:: 1.1.2

            automatically store metadata in cache.
        """
        s3_client = resolve_s3_client(context, bsm)
        response = s3_client.get_object(
            Bucket=self.bucket,
            Key=self.key,
        )
        data = response["Body"].read()
        del response["Body"]
        del response["ResponseMetadata"]
        self._meta = response
        return data

    def read_text(
        self: 'S3Path',
        encoding="utf-8",
        errors="strict",
        bsm: T.Optional['BotoSesManager'] = None,
    ) -> str:
        """
        Read text data from s3 object. A simple wrapper around
        `s3_client.get_object <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object>`_

        .. versionchanged:: 1.1.2

            automatically store metadata in cache.
        """
        data = self.read_bytes(bsm=bsm)
        return data.decode(encoding, errors=errors)

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

            allow update metadata and tags as well
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
        # TODO: update _meta attributes to avoid unnecessary head_object api call
        return response

    def write_text(
        self: 'S3Path',
        data: str,
        encoding="utf-8",
        errors="strict",
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

            allow update metadata and tags as well
        """
        body = data.encode(encoding, errors=errors)
        return self.write_bytes(
            data=body,
            metadata=metadata,
            tags=tags,
            bsm=bsm,
        )
