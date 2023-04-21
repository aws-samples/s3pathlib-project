# -*- coding: utf-8 -*-

"""
Improve the list_objects_v2_ and ListObjectsV2_ API.

.. _list_objects_v2: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
.. _ListObjectsV2: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/paginator/ListObjectsV2.html
"""

import typing as T

from func_args import NOTHING, resolve_kwargs
from iterproxy import IterProxy


if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.type_defs import (
        ListObjectsV2OutputTypeDef,
        ObjectTypeDef,
        CommonPrefixTypeDef,
    )

    _ = ListObjectsV2OutputTypeDef


class ObjectTypeDefIterproxy(IterProxy["ObjectTypeDef"]):
    """
    An iterproxy that yields the "Contents" part of the ListObjectsV2_ response.

    .. versionadded:: 2.0.1
    """


class CommonPrefixTypeDefIterproxy(IterProxy["CommonPrefixTypeDef"]):
    """
    An iterproxy that yields the "CommonPrefixes" part of the ListObjectsV2_ response.

    .. versionadded:: 2.0.1
    """


class ListObjectsV2OutputTypeDefIterproxy(IterProxy["ListObjectsV2OutputTypeDef"]):
    """
    An iterproxy that yields the original ListObjectsV2_ response.
    It has two utility methods to get the contents and common prefixes.

    .. versionadded:: 2.0.1
    """

    def _yield_content(self) -> T.Iterator["ObjectTypeDef"]:
        for response in self:
            for content in response.get("Contents", []):
                yield content

    def contents(self) -> ObjectTypeDefIterproxy:
        """
        Iterate object contents.

        .. versionadded:: 2.0.1
        """
        return ObjectTypeDefIterproxy(self._yield_content())

    def _yield_common_prefixes(self) -> T.Iterator["CommonPrefixTypeDef"]:
        for response in self:
            for common_prefix in response.get("CommonPrefixes", []):
                yield common_prefix

    def common_prefixs(self) -> CommonPrefixTypeDefIterproxy:
        """
        Iterate folders.

        .. versionadded:: 2.0.1
        """
        return CommonPrefixTypeDefIterproxy(self._yield_common_prefixes())

    def extract_contents_and_common_prefixes(
        self, response: dict
    ) -> T.Tuple[
        T.List["ObjectTypeDef"],
        T.List["CommonPrefixTypeDef"],
    ]:
        return (
            response.get("Contents", []),
            response.get("CommonPrefixes", []),
        )

    def contents_and_common_prefixs(
        self,
    ) -> T.Tuple[T.List["ObjectTypeDef"], T.List["CommonPrefixTypeDef"]]:
        """
        Return the full list of object contents and folders.

        .. versionadded:: 2.0.1
        """
        contents = list()
        common_prefixs = list()
        for response in self:
            (
                _contents,
                _common_prefixs,
            ) = self.extract_contents_and_common_prefixes(response)
            contents.extend(_contents)
            common_prefixs.extend(_common_prefixs)
        return contents, common_prefixs


def paginate_list_objects_v2(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    batch_size: int = 1000,
    limit: int = NOTHING,
    delimiter: str = NOTHING,
    encoding_type: str = NOTHING,
    fetch_owner: bool = NOTHING,
    start_after: str = NOTHING,
    request_payer: str = NOTHING,
    expected_bucket_owner: str = NOTHING,
) -> ListObjectsV2OutputTypeDefIterproxy:
    """
    Wrapper of list_objects_v2_ and ListObjectsV2_. However, it returns
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

    :return: a :class:`ListObjectsV2OutputTypeDefIterproxy` object.

    .. versionadded:: 2.0.1
    """
    # validate arguments
    if batch_size < 1 or batch_size > 1000:
        raise ValueError("``batch_size`` has to be 1 ~ 1000.")
    if (limit is not NOTHING) and (batch_size > limit):
        batch_size = limit

    def _paginate_list_objects_v2():
        paginator = s3_client.get_paginator("list_objects_v2")
        kwargs = resolve_kwargs(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter=delimiter,
            EncodingType=encoding_type,
            FetchOwner=fetch_owner,
            StartAfter=start_after,
            RequestPayer=request_payer,
            ExpectedBucketOwner=expected_bucket_owner,
            PaginationConfig=resolve_kwargs(
                MaxItems=limit,
                PageSize=batch_size,
            ),
        )
        for response in paginator.paginate(**kwargs):
            yield response

    return ListObjectsV2OutputTypeDefIterproxy(_paginate_list_objects_v2())


def is_content_an_object(content: "ObjectTypeDef") -> bool:
    """
    Return True if the content is an object (not a folder).

    Truth table

    - ends with "/", size is 0: False
    - ends with "/", size > 0: False
    - ends without "/", size is 0:
    - ends without "/", size > 0:
    """
    return (not content["Key"].endswith("/")) or (content["Size"] != 0)


def calculate_total_size(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    include_folder: bool = False,
) -> T.Tuple[int, int]:
    """
    Perform the "Calculate Total Size" action in AWS S3 console.

    :param s3_client: ``boto3.session.Session().client("s3")`` object
    :param bucket: S3 bucket name
    :param prefix: The s3 prefix (logic directory) you want to calculate
    :param include_folder: Default False, whether counting the hard folder
        (an empty "/" object).

    :return: Tuple of ``(count, total_size)``. First value is number of objects,
        Second value is total size in bytes.

    .. versionadded:: 2.0.1
    """
    count = 0
    total_size = 0
    contents_iterproxy = paginate_list_objects_v2(
        s3_client=s3_client,
        bucket=bucket,
        prefix=prefix,
    ).contents()
    if include_folder is False:
        contents_iterproxy = contents_iterproxy.filter(is_content_an_object)
    for content in contents_iterproxy:
        count += 1
        total_size += content["Size"]
    return count, total_size


def count_objects(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    include_folder: bool = False,
) -> int:
    """
    Count number of objects under prefix.

    :param s3_client: ``boto3.session.Session().client("s3")`` object
    :param bucket: S3 bucket name
    :param prefix: The s3 prefix (logic directory) you want to calculate
    :param include_folder: Default False, whether counting the hard folder
        (an empty "/" object).

    :return: Number of objects under prefix.

    .. versionadded:: 2.0.1
    """
    contents_iterproxy = paginate_list_objects_v2(
        s3_client=s3_client,
        bucket=bucket,
        prefix=prefix,
    ).contents()
    if include_folder is False:
        contents_iterproxy = contents_iterproxy.filter(is_content_an_object)
    count = 0
    for count, _ in enumerate(contents_iterproxy, start=1):
        pass
    return count
