# -*- coding: utf-8 -*-

"""
Tagging related API.

.. _get_object_tagging: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object_tagging.html
.. _put_object_tagging: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object_tagging.html
"""

import typing as T

from func_args import NOTHING, resolve_kwargs

from .resolve_s3_client import resolve_s3_client
from ..better_client.tagging import update_object_tagging
from ..aws import context
from ..type import TagType
from ..tag import parse_tags, encode_tag_set


if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


class TaggingAPIMixin:
    """
    A mixin class that implements the tagging related methods.
    """

    def get_tags(
        self: "S3Path",
        version_id: str = NOTHING,
        expected_bucket_owner: str = NOTHING,
        request_payer: str = NOTHING,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> T.Tuple[T.Optional[str], TagType]:
        """
        Get s3 object tags in key value pairs dict.

        :return: ``(version_id, tags)``, tags is in string key value pairs dict.

        .. versionadded:: 1.1.1

        .. versionchanged:: 2.0.1

            Add ``version_id``, ``expected_bucket_owner``, ``request_payer`` parameter.
        """
        self.ensure_object()
        s3_client = resolve_s3_client(context, bsm)
        res = s3_client.get_object_tagging(
            **resolve_kwargs(
                Bucket=self.bucket,
                Key=self.key,
                VersionId=version_id,
                ExpectedBucketOwner=expected_bucket_owner,
                RequestPayer=request_payer,
            )
        )
        returned_version_id = res.get("VersionId", None)
        tags = parse_tags(res.get("TagSet", []))
        return returned_version_id, tags

    def put_tags(
        self: "S3Path",
        tags: TagType,
        version_id: str = NOTHING,
        content_md5: str = NOTHING,
        checksum_algorithm: str = NOTHING,
        expected_bucket_owner: str = NOTHING,
        request_payer: str = NOTHING,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> T.Tuple[T.Optional[str], TagType]:
        """
        Do full replacement of s3 object tags.

        :param tags: the s3 object tags in string key value pairs dict.

        :return: ``(version_id, tags)``, tags is in string key value pairs dict.

        .. versionadded:: 1.1.1

        .. versionchanged:: 2.0.1

            Add ``version_id``, ``expected_bucket_owner``, ``request_payer`` parameter.
        """
        self.ensure_object()
        s3_client = resolve_s3_client(context, bsm)
        res = s3_client.put_object_tagging(
            **resolve_kwargs(
                Bucket=self.bucket,
                Key=self.key,
                Tagging=dict(TagSet=encode_tag_set(tags)),
                VersionId=version_id,
                ContentMD5=content_md5,
                ChecksumAlgorithm=checksum_algorithm,
                ExpectedBucketOwner=expected_bucket_owner,
                RequestPayer=request_payer,
            )
        )
        returned_version_id = res.get("VersionId", None)
        return returned_version_id, tags

    def update_tags(
        self: "S3Path",
        tags: TagType,
        version_id: str = NOTHING,
        content_md5: str = NOTHING,
        checksum_algorithm: str = NOTHING,
        expected_bucket_owner: str = NOTHING,
        request_payer: str = NOTHING,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> T.Tuple[T.Optional[str], TagType]:
        """
        Do partial updates of s3 object tags.

        :param tags: the s3 object tags in string key value pairs dict.

        :return: ``(version_id, tags)``, tags is the latest, merged object tags
            in string key value pairs dict.

        .. versionadded:: 1.1.1
        """
        self.ensure_object()
        s3_client = resolve_s3_client(context, bsm)
        return update_object_tagging(
            s3_client=s3_client,
            bucket=self.bucket,
            key=self.key,
            tags=tags,
            version_id=version_id,
            content_md5=content_md5,
            checksum_algorithm=checksum_algorithm,
            expected_bucket_owner=expected_bucket_owner,
            request_payer=request_payer,
        )
