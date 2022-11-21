# -*- coding: utf-8 -*-

"""
Metadata related API.
"""

import typing as T
import warnings
from datetime import datetime

from .resolve_s3_client import resolve_s3_client
from .filterable_property import FilterableProperty
from .. import utils, client as better_client
from ..aws import context

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


def alert_upper_case(metadata: dict):
    """
    Alert if there is upper case used in user defined metadata.

    Ref:

    - https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#UserMetadata
    """
    for k, v in metadata.items():
        if k.lower() != k:
            msg = (
                f"based on this document "
                f"https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#UserMetadata "
                f"Amazon will automatically convert user defined metadata to lower case, "
                f"but you have a key: {k!r} in metadata"
            )
            warnings.warn(msg, UserWarning)


class MetadataAPIMixin:
    """
    A mixin class that implements the metadata related methods.

    Note:

        1. only S3 object can have metadata.
        2. metadata is immutable.
        3. user metadata key is always lower case.
    """

    def head_object(
        self: "S3Path",
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> dict:
        """
        Call head_object() api, store metadata value.
        """
        s3_client = resolve_s3_client(context, bsm)
        dct = better_client.head_object(s3_client, self.bucket, self.key)
        self._meta = dct
        return dct

    def _get_meta_value(
        self: "S3Path",
        key: str,
        default: T.Any = None,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> T.Any:
        if self._meta is None:
            self.head_object(bsm=bsm)
        return self._meta.get(key, default)

    def _get_or_pull_meta_value(
        self: "S3Path",
        key: str,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> T.Any:
        """
        Note:

            This method is for those metadata fields that always exists.
        """
        value = self._get_meta_value(key, default=None, bsm=bsm)
        if value is None:
            self.head_object(bsm=bsm)
        return self._meta[key]

    @FilterableProperty
    def etag(self: "S3Path") -> str:
        """
        For small file, it is the md5 check sum. For large file, because it is
        created from multi part upload, it is the sum of md5 for each part and
        md5 of the sum.

        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        .. versionadded:: 1.0.1
        """
        return self._get_or_pull_meta_value(key="ETag")[1:-1]

    @FilterableProperty
    def last_modified_at(self: "S3Path") -> datetime:
        """
        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        .. versionadded:: 1.0.1
        """
        return self._get_or_pull_meta_value(key="LastModified")

    @FilterableProperty
    def size(self: "S3Path") -> int:
        """
        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        .. versionadded:: 1.0.1
        """
        return self._get_or_pull_meta_value(key="ContentLength")

    @property
    def size_for_human(self: "S3Path") -> str:
        """
        A human readable string version of the size.

        .. versionadded:: 1.0.1
        """
        return utils.repr_data_size(self.size)

    @FilterableProperty
    def version_id(self: "S3Path") -> int:
        """
        Only available if you turned on versioning for the bucket.

        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        .. versionadded:: 1.0.1
        """
        return self._get_meta_value(key="VersionId")

    @FilterableProperty
    def expire_at(self: "S3Path") -> datetime:
        """
        Only available if you turned on TTL

        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        .. versionadded:: 1.0.1
        """
        return self._get_meta_value(key="Expires")

    @property
    def metadata(self: "S3Path") -> dict:
        """
        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        .. versionadded:: 1.0.1
        """
        return self._get_or_pull_meta_value(key="Metadata")

    def clear_cache(self: "S3Path") -> None:
        """
        Clear all cache that stores metadata information.

        .. versionadded:: 1.0.1
        """
        self._meta = None

    @classmethod
    def _from_content_dict(cls: T.Type["S3Path"], bucket: str, dct: dict) -> "S3Path":
        """
        Construct S3Path object from the response["Content"] dictionary data.

        Example ``dct``::

            {
                'Key': 'string',
                'LastModified': datetime(2015, 1, 1),
                'ETag': 'string',
                'ChecksumAlgorithm': [
                    'CRC32'|'CRC32C'|'SHA1'|'SHA256',
                ],
                'Size': 123,
                'StorageClass': 'STANDARD'|'REDUCED_REDUNDANCY'|'GLACIER'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'DEEP_ARCHIVE'|'OUTPOSTS'|'GLACIER_IR',
                'Owner': {
                    'DisplayName': 'string',
                    'ID': 'string'
                }
            }

        Ref:

        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2

        :return: a new S3Path object.
        """
        p = cls(bucket, dct["Key"])
        p._meta = {
            "Key": dct["Key"],
            "LastModified": dct["LastModified"],
            "ETag": dct["ETag"],
            "ContentLength": dct["Size"],
            "StorageClass": dct["StorageClass"],
            "Owner": dct.get("Owner", {}),
        }
        return p

    def update_metadata(self: "S3Path", metadata: dict):  # pragma: no cover
        raise NotImplementedError(
            "You CANNOT only update metadata without changing the content of the "
            "object! You can only do full replace ment via the .write_text() or "
            ".write_bytes() API. This method will NEVER be implemented!"
        )
