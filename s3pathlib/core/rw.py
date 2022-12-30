# -*- coding: utf-8 -*-

"""
Read and write related API.
"""

import typing as T

from .. import utils, client as better_client
from ..type import TagType, MetadataType
from ..aws import context

from .resolve_s3_client import resolve_s3_client

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


class ReadAndWriteAPIMixin:
    """
    A mixin class that implements the Text / Bytes, Read / Write methods.
    """
    def read_bytes(
        self: "S3Path",
        bsm: T.Optional["BotoSesManager"] = None,
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
        self: "S3Path",
        encoding="utf-8",
        errors="strict",
        bsm: T.Optional["BotoSesManager"] = None,
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
        self: "S3Path",
        data: bytes,
        metadata: T.Optional[MetadataType] = None,
        tags: T.Optional[TagType] = None,
        bsm: T.Optional["BotoSesManager"] = None,
    ):
        """
        Write binary data to s3 object. A simple wrapper around
        `s3_client.put_object <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object>`_

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
        self._meta = response
        self._meta["Metadata"] = metadata
        return response

    def write_text(
        self: "S3Path",
        data: str,
        encoding="utf-8",
        errors="strict",
        metadata: T.Optional[MetadataType] = None,
        tags: T.Optional[TagType] = None,
        bsm: T.Optional["BotoSesManager"] = None,
    ):
        """
        Write text to s3 object. A simple wrapper around
        `s3_client.put_object <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object>`_

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

    def touch(
        self: "S3Path",
        exist_ok: bool = True,
        metadata: T.Optional[MetadataType] = None,
        tags: T.Optional[TagType] = None,
        bsm: T.Optional["BotoSesManager"] = None,
    ):
        """
        Create an empty S3 object at the S3 location if the S3 object not exists.
        Do nothing if already exists.

        :param exist_ok: if True, it won't raise error when the S3 object
            already exists.

        .. versionchanged:: 1.0.6

        .. versionchanged:: 1.2.1
        """
        self.ensure_object()

        if self.exists(bsm=bsm):
            if exist_ok:
                pass
            else:
                raise FileExistsError
        else:
            self.write_text("", metadata=metadata, tags=tags, bsm=bsm)

    def mkdir(
        self: "S3Path",
        exist_ok: bool = False,
        parents: bool = False,
        bsm: T.Optional["BotoSesManager"] = None,
    ):
        """
        Make an S3 folder (empty "/" file)

        .. versionchanged:: 1.0.6
        """
        if not self.is_dir():
            raise ValueError

        s3_client = resolve_s3_client(context, bsm)
        dct = utils.head_object_if_exists(
            s3_client=s3_client,
            bucket=self.bucket,
            key=self.key,
        )
        if dct:
            if exist_ok:
                pass
            else:
                raise FileExistsError
        else:
            s3_client.put_object(
                Bucket=self.bucket,
                Key=self.key,
                Body="",
            )

        if parents:
            for p in self.parents:
                if p.is_bucket() is False:
                    p.mkdir(exist_ok=True, parents=False, bsm=bsm)
