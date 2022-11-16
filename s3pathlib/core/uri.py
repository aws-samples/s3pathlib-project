# -*- coding: utf-8 -*-

"""
S3 URI (Uniform Resource Identifier), ARN (Amazon Resource Name),
console URL related API.
"""

import typing as T

from .filterable_property import FilterableProperty
from .. import utils, validate

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path


class UriAPIMixin:
    """
    A mixin class that implements the S3 URI, ARN, console url etc ...
    """
    @FilterableProperty
    def bucket(self: 'S3Path') -> T.Optional[str]:
        """
        Return bucket name as string, if available.

        Example::

            >>> S3Path("bucket/folder/file.txt").bucket
            'bucket'

        .. versionadded:: 1.0.1
        """
        return self._bucket

    @FilterableProperty
    def key(self: 'S3Path') -> T.Optional[str]:
        """
        Return object or directory key as string, if available.

        Examples::

            # a s3 object
            >>> S3Path("bucket/folder/file.txt").key
            'folder/file.txt'

            # a s3 object
            >>> S3Path("bucket/folder/").key
            'folder/file.txt'

            # a relative path
            >>> S3Path("bucket/folder/file.txt").relative_to(S3Path("bucket")).key
            'folder/file.txt

            >>> S3Path("bucket/folder/").relative_to(S3Path("bucket")).key
            'folder/'

            # an empty S3Path
            >>> S3Path().key
            ''

        .. versionadded:: 1.0.1
        """
        if len(self._parts):
            return "{}{}".format(
                "/".join(self._parts),
                "/" if self._is_dir else ""
            )
        else:
            return ""

    @FilterableProperty
    def uri(self: 'S3Path') -> T.Optional[str]:
        """
        Return AWS S3 URI.

        - for regular s3 object, it returns ``"s3://{bucket}/{key}"``
        - if it is a directory, the s3 uri always ends with ``"/"``.
        - if it is bucket only (no key), it returns ``"s3://{bucket}/"``
        - if it is not an concrete S3Path, it returns ``None``
        - it has to have bucket, if not (usually because it is an relative path)
            it returns ``None``

        Examples::

            >>> S3Path("bucket", "folder", "file.txt").uri
            's3://bucket/folder/file.txt'

            >>> S3Path("bucket", "folder/").uri
            's3://bucket/folder/'

            >>> S3Path("bucket").uri
            's3://bucket/'

            # void path doesn't have uri
            >>> S3Path().uri
            None

            # relative path doesn't have uri
            >>> S3Path("bucket/folder/file.txt").relative_to(S3Path("bucket")).uri
            None

        .. versionadded:: 1.0.1
        """
        if self._bucket is None:
            return None
        if len(self._parts):
            return "s3://{}/{}".format(
                self.bucket,
                self.key,
            )
        else:
            return "s3://{}/".format(self._bucket)

    @property
    def console_url(self: 'S3Path') -> T.Optional[str]:
        """
        Return an AWS S3 Console url that can inspect the details.

        .. versionadded:: 1.0.1
        """
        uri: str = self.uri
        if uri is None:
            return None
        else:
            console_url = utils.make_s3_console_url(s3_uri=uri)
            return console_url

    @property
    def us_gov_cloud_console_url(self: 'S3Path') -> T.Optional[str]:
        """
        Return an AWS US Gov Cloud S3 Console url that can inspect the details.

        .. versionadded:: 1.0.5
        """
        uri: str = self.uri
        if uri is None:
            return None
        else:
            console_url = utils.make_s3_console_url(
                s3_uri=uri, is_us_gov_cloud=True
            )
            return console_url

    @property
    def s3_select_console_url(self: 'S3Path') -> T.Optional[str]:
        """
        Return an AWS US Gov Cloud S3 Console url that can inspect data with s3 select.

        .. versionadded:: 1.0.12
        """
        if self.is_file():
            return utils.make_s3_select_console_url(
                bucket=self.bucket,
                key=self.key,
                is_us_gov_cloud=False,
            )
        else:
            raise TypeError("you can only do s3 select with an object!")

    @property
    def s3_select_us_gov_cloud_console_url(self: 'S3Path') -> T.Optional[str]:
        """

        Return an AWS S3 Console url that can inspect data with s3 select.

        .. versionadded:: 1.0.12
        """
        if self.is_file():
            return utils.make_s3_select_console_url(
                bucket=self.bucket,
                key=self.key,
                is_us_gov_cloud=True,
            )
        else:
            raise TypeError("you can only do s3 select with an object!")

    @FilterableProperty
    def arn(self: 'S3Path') -> T.Optional[str]:
        """
        Return an AWS S3 Resource ARN. See `ARN definition here <https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html>`_

        .. versionadded:: 1.0.1
        """
        if self._bucket is None:
            return None
        if len(self._parts):
            return "arn:aws:s3:::{}/{}".format(
                self.bucket,
                self.key,
            )
        else:
            return "arn:aws:s3:::{}".format(self._bucket)

    @classmethod
    def from_s3_uri(cls: T.Type['S3Path'], uri: str) -> 'S3Path':
        """
        Construct an :class:`S3Path` from S3 URI.

        >>> p = S3Path.from_s3_uri("s3://bucket/folder/file.txt")

        >>> p
        S3Path('s3://bucket/folder/file.txt')

        >>> p.uri
        's3://bucket/folder/file.txt'
        """
        validate.validate_s3_uri(uri)
        bucket, key = utils.split_s3_uri(uri)
        return cls._from_parts([bucket, key])

    @classmethod
    def from_s3_arn(cls: T.Type['S3Path'], arn: str) -> 'S3Path':
        """

        :param arn:
        :return:
        """
        validate.validate_s3_arn(arn)
        return cls._from_parts([arn.replace("arn:aws:s3:::", "", 1), ])
