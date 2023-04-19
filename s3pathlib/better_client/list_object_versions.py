# -*- coding: utf-8 -*-

"""
Improve the list_object_versions_ and ListObjectVersions_ API.

.. _list_object_versions: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_object_versions.html
.. _ListObjectVersions: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/paginator/ListObjectVersions.html

.. versionadded:: 2.1.1
"""

import typing as T

from func_args import NOTHING, resolve_kwargs
from iterproxy import IterProxy


if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.type_defs import (
        ListObjectVersionsOutputTypeDef,
        ObjectVersionTypeDef,
        DeleteMarkerEntryTypeDef,
        CommonPrefixTypeDef,
    )

    _ = ListObjectVersionsOutputTypeDef


class ObjectVersionTypeDefIterproxy(IterProxy["ObjectVersionTypeDef"]):
    """
    An iterproxy that yields the "Versions" part of the ListObjectVersions_ response.

    .. versionadded:: 2.1.1
    """


class DeleteMarkerEntryTypeDefIterproxy(IterProxy["DeleteMarkerEntryTypeDef"]):
    """
    An iterproxy that yields the "DeleteMarkers" part of the ListObjectVersions_ response.

    .. versionadded:: 2.1.1
    """


class CommonPrefixTypeDefIterproxy(IterProxy["CommonPrefixTypeDef"]):
    """
    An iterproxy that yields the "CommonPrefixes" part of the ListObjectsV2_ response.

    .. versionadded:: 2.1.1
    """


class ListObjectVersionsOutputTypeDefIterproxy(
    IterProxy["ListObjectVersionsOutputTypeDef"]
):
    """
    An iterproxy that yields the original ListObjectsV2_ response.
    It has two utility methods to get the contents and common prefixes.

    .. versionadded:: 2.1.1
    """

    def _yield_versions(self) -> T.Iterator["ObjectVersionTypeDef"]:
        for response in self:
            for version in response.get("Versions", []):
                yield version

    def versions(self) -> ObjectVersionTypeDefIterproxy:
        """
        Iterate object versions.

        .. versionadded:: 2.1.1
        """
        return ObjectVersionTypeDefIterproxy(self._yield_versions())

    def _yield_delete_markers(self) -> T.Iterator["DeleteMarkerEntryTypeDef"]:
        for response in self:
            for delete_marker in response.get("DeleteMarkers", []):
                yield delete_marker

    def delete_markers(self) -> DeleteMarkerEntryTypeDefIterproxy:
        """
        Iterate delete markers.

        .. versionadded:: 2.1.1
        """
        return DeleteMarkerEntryTypeDefIterproxy(self._yield_delete_markers())

    def _yield_common_prefixes(self) -> T.Iterator["CommonPrefixTypeDef"]:
        for response in self:
            for common_prefix in response.get("CommonPrefixes", []):
                yield common_prefix

    def common_prefixes(self) -> CommonPrefixTypeDefIterproxy:
        """
        Iterate folders.

        .. versionadded:: 2.1.1
        """
        return CommonPrefixTypeDefIterproxy(self._yield_common_prefixes())

    def extract_versions_and_delete_markers_and_common_prefixes(
        self, response: dict
    ) -> T.Tuple[
        T.List["ObjectVersionTypeDef"],
        T.List["DeleteMarkerEntryTypeDef"],
        T.List["CommonPrefixTypeDef"],
    ]:
        return (
            response.get("Versions", []),
            response.get("DeleteMarkers", []),
            response.get("CommonPrefixes", []),
        )

    def versions_and_delete_markers_and_common_prefixes(
        self,
    ) -> T.Tuple[
        T.List["ObjectVersionTypeDef"],
        T.List["DeleteMarkerEntryTypeDef"],
        T.List["CommonPrefixTypeDef"],
    ]:
        """
        Return the list of object versions, delete markers and folders.

        .. versionadded:: 2.1.1
        """
        versions = list()
        delete_markers = list()
        common_prefixes = list()
        for response in self:
            versions.extend(response.get("Versions", []))
            delete_markers.extend(response.get("DeleteMarkers", []))
            common_prefixes.extend(response.get("CommonPrefixes", []))
        return versions, delete_markers, common_prefixes


def paginate_list_object_versions(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    batch_size: int = 1000,
    limit: int = NOTHING,
    delimiter: str = NOTHING,
    encoding_type: str = NOTHING,
    expected_bucket_owner: str = NOTHING,
) -> ListObjectVersionsOutputTypeDefIterproxy:
    """
    Wrapper of list_object_versions_ and ListObjectVersions_. However, it returns
    a user-friendly :class:`ListObjectsV2OutputTypeDefIterproxy` object.

    Example::

        >>> result = paginate_list_objects_v2(
        ...     s3_client=s3_client,
        ...     bucket="my-bucket",
        ...     prefix="my-folder",
        ... )
        >>> for content in result.contents():
        ...     print(content)
        {"Key": "1.json", "ETag": "...", "Size": 123, "LastModified": datetime(2015, 1, 1), "StorageClass": "...", "Owner", {...}}
        {"Key": "2.json", "ETag": "...", "Size": 123, "LastModified": datetime(2015, 1, 1), "StorageClass": "...", "Owner", {...}}
        {"Key": "3.json", "ETag": "...", "Size": 123, "LastModified": datetime(2015, 1, 1), "StorageClass": "...", "Owner", {...}}
        ...

    :param s3_client: ``boto3.session.Session().client("s3")`` object.
    :param bucket: See ListObjectsV2_.
    :param prefix: See ListObjectsV2_.
    :param batch_size: See ListObjectsV2_.
    :param limit: See ListObjectsV2_.
    :param delimiter: See ListObjectsV2_.
    :param encoding_type: See ListObjectsV2_.
    :param fetch_owner: See ListObjectsV2_.
    :param start_after: See ListObjectsV2_.
    :param request_payer: See ListObjectsV2_.
    :param expected_bucket_owner: See ListObjectsV2_.

    :return: a :class:`ListObjectVersionsOutputTypeDefIterproxy` object.

    .. versionadded:: 2.1.1
    """
    # validate arguments
    if batch_size < 1 or batch_size > 1000:
        raise ValueError("``batch_size`` has to be 1 ~ 1000.")
    if (limit is not NOTHING) and (batch_size > limit):
        batch_size = limit

    def _paginate_list_objects_v2():
        paginator = s3_client.get_paginator("list_object_versions")
        kwargs = resolve_kwargs(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter=delimiter,
            EncodingType=encoding_type,
            ExpectedBucketOwner=expected_bucket_owner,
            PaginationConfig=resolve_kwargs(
                MaxItems=limit,
                PageSize=batch_size,
            ),
        )
        for response in paginator.paginate(**kwargs):
            yield response

    return ListObjectVersionsOutputTypeDefIterproxy(_paginate_list_objects_v2())
