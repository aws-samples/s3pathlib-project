# -*- coding: utf-8 -*-

"""
Metadata related API.
"""

import typing as T
from datetime import datetime

from .. import utils
from ..constants import IS_DELETE_MARKER
from ..better_client.head_object import head_object
from ..aws import context

from .resolve_s3_client import resolve_s3_client
from .filterable_property import FilterableProperty

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


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
        dct = head_object(s3_client, self.bucket, self.key)
        self._meta = dct
        return dct

    def _get_meta_value(
        self: "S3Path",
        key: str,
        default: T.Any = None,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> T.Any:
        """
        Note:

            This method is for those metadata fields that conditionally exists.
        """
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
    def etag(self: "S3Path") -> T.Optional[str]:
        """
        For small file, it is the md5 check sum. For large file, because it is
        created from multi part upload, it is the sum of md5 for each part and
        md5 of the sum.

        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        .. versionadded:: 1.0.1
        """
        v = self._get_meta_value(key="ETag", default=None)
        if v is None:
            return v
        else:
            return v[1:-1]

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
        return self._get_meta_value(key="ContentLength", default=0)

    @property
    def size_for_human(self: "S3Path") -> str:
        """
        A human-readable string version of the size.

        .. versionadded:: 1.0.1
        """
        return utils.repr_data_size(self.size)

    @property
    def _static_version_id(self: "S3Path") -> T.Optional[str]:
        """
        This method use the ``self._meta`` to get the version id. Unlike
        other metadata property methods, this method does not call head_object().
        """
        if self._meta is None:
            return None
        else:
            return self._meta.get("VersionId", None)

    @FilterableProperty
    def version_id(self: "S3Path") -> T.Optional[str]:
        """
        Only available if you turned on versioning for the bucket.

        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        .. versionadded:: 1.0.1

        .. versionchanged:: 2.0.1

            return 'null' if it is not a version enabled bucket
        """
        return self._get_meta_value(key="VersionId", default="null")

    @FilterableProperty
    def expire_at(self: "S3Path") -> datetime:
        """
        Only available if you turned on TTL.

        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        .. versionadded:: 1.0.1
        """
        return self._get_meta_value(key="Expires")

    @property
    def metadata(self: "S3Path") -> dict:
        """
        Access the metadata of the object.

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
            "ChecksumAlgorithm": dct.get("ChecksumAlgorithm", []),
            "Owner": dct.get("Owner", {}),
        }
        return p

    @classmethod
    def _from_version_dict(cls: T.Type["S3Path"], bucket: str, dct: dict) -> "S3Path":
        p = cls(bucket, dct["Key"])
        p._meta = {
            "Key": dct["Key"],
            "VersionId": dct["VersionId"],
            "LastModified": dct["LastModified"],
            "ETag": dct["ETag"],
            "ContentLength": dct["Size"],
            "StorageClass": dct["StorageClass"],
            "IsLatest": dct["IsLatest"],
            "ChecksumAlgorithm": dct.get("ChecksumAlgorithm", []),
            "Owner": dct.get("Owner", {}),
        }
        return p

    @classmethod
    def _from_delete_marker(cls: T.Type["S3Path"], bucket: str, dct: dict) -> "S3Path":
        p = cls(bucket, dct["Key"])
        p._meta = {
            "Key": dct["Key"],
            "VersionId": dct["VersionId"],
            "LastModified": dct["LastModified"],
            "IsLatest": dct["IsLatest"],
            "Owner": dct.get("Owner", {}),
            IS_DELETE_MARKER: True,
        }
        return p

    def update_metadata(self: "S3Path", metadata: dict):  # pragma: no cover
        raise NotImplementedError(
            "You CANNOT only update metadata without changing the content of the "
            "object! You can only do full replace ment via the .write_text() or "
            ".write_bytes() API. This method will NEVER be implemented!"
        )
