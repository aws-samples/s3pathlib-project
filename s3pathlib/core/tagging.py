# -*- coding: utf-8 -*-

"""
Tagging related API.
"""

import typing as T

from .resolve_s3_client import resolve_s3_client
from .. import client as better_client
from ..aws import context
from ..type import TagType

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


class TaggingAPIMixin:
    """
    A mixin class that implements the tagging related methods.
    """

    def get_tags(
        self: "S3Path",
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> TagType:
        """
        Get s3 object tags in key value pairs dict.

        :return: the s3 object tags in string key value pairs dict.

        .. versionadded:: 1.1.1
        """
        self.ensure_object()
        s3_client = resolve_s3_client(context, bsm)
        return better_client.get_object_tagging(s3_client, self.bucket, self.key)

    def put_tags(
        self: "S3Path",
        tags: TagType,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> TagType:
        """
        Do full replacement of s3 object tags.

        :param tags: the s3 object tags in string key value pairs dict.

        :return: the s3 object tags in string key value pairs dict.

        .. versionadded:: 1.1.1
        """
        self.ensure_object()
        s3_client = resolve_s3_client(context, bsm)
        better_client.put_object_tagging(s3_client, self.bucket, self.key, tags)
        return tags

    def update_tags(
        self: "S3Path",
        tags: TagType,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> TagType:
        """
        Do partial updates of s3 object tags.

        :param tags: the s3 object tags in string key value pairs dict.

        :return: the latest, merged object tags in string key value pairs dict.

        .. versionadded:: 1.1.1
        """
        self.ensure_object()
        s3_client = resolve_s3_client(context, bsm)
        return better_client.update_object_tagging(
            s3_client,
            self.bucket,
            self.key,
            tags,
        )
