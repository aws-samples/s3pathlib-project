# -*- coding: utf-8 -*-

"""
Tagging related API.
"""

import typing as T

from iterproxy import IterProxy
from func_args import NOTHING, resolve_kwargs

from .. import utils
from ..aws import context
from ..better_client.list_objects import (
    paginate_list_objects_v2,
    is_content_an_object,
    calculate_total_size,
    count_objects,
)
from .resolve_s3_client import resolve_s3_client

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


class S3PathIterProxy(IterProxy):
    """
    An iterator proxy utility class provide client side in-memory filter.
    It is highly inspired by sqlalchemy Result Proxy that depends on SQL server
    side filter.

    Allow client side in-memory filtering for iterator object that yield
    :class:`S3Path`.

    It is a special variation of :class:`s3pathlib.iterproxy.IterProxy`,
    See :class:`s3pathlib.iterproxy.IterProxy` for more details

    .. versionadded:: 1.0.3
    """

    def __next__(self) -> "S3Path":
        return super(S3PathIterProxy, self).__next__()

    def one(self) -> "S3Path":
        return super(S3PathIterProxy, self).one()

    def one_or_none(self) -> T.Union["S3Path", None]:
        return super(S3PathIterProxy, self).one_or_none()

    def many(self, k: int) -> T.List["S3Path"]:
        return super(S3PathIterProxy, self).many(k)

    def all(self) -> T.List["S3Path"]:
        return super(S3PathIterProxy, self).all()

    def skip(self, k: int) -> "S3PathIterProxy":
        return super(S3PathIterProxy, self).skip(k=k)

    def filter_by_ext(self, *exts: str) -> "S3PathIterProxy":
        """
        Filter S3 object by file extension. Case is insensitive.

        Example::

            >>> p = S3Path("bucket")
            >>> for path in p.iter_objects().filter_by_ext(".csv", ".json"):
            ...      print(path)
        """
        n = len(exts)
        if n == 0:
            raise ValueError
        elif n == 1:
            ext = exts[0].lower()

            def f(p: "S3Path") -> bool:
                return p.ext.lower() == ext

            return self.filter(f)
        else:
            valid_exts = set([ext.lower() for ext in exts])

            def f(p: "S3Path") -> bool:
                return p.ext.lower() in valid_exts

            return self.filter(f)


class IterObjectsAPIMixin:
    """
    A mixin class that implements the iter objects methods.
    """

    def iter_objects(
        self: "S3Path",
        batch_size: int = 1000,
        limit: int = NOTHING,
        encoding_type: str = NOTHING,
        fetch_owner: bool = NOTHING,
        start_after: str = NOTHING,
        request_payer: str = NOTHING,
        expected_bucket_owner: str = NOTHING,
        recursive: bool = True,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> S3PathIterProxy:
        """
        Recursively iterate objects under this prefix, yield :class:`S3Path`.

        :param batch_size: number of s3 object returned per paginator,
            valid value is from 1 ~ 1000. large number can reduce IO.
        :param limit: total number of s3 object to return
        :param recursive: if True, it won't include files in sub folders

        .. versionadded:: 1.0.1

        .. versionchanged:: 2.1.1

            Remove ``include_folder`` argument. Support all list_objects_v2
            arguments.

        TODO: add unix glob liked syntax for pattern matching
        """
        s3_client = resolve_s3_client(context, bsm)
        bucket = self.bucket

        def _iter_s3path() -> T.Iterable["S3Path"]:
            kwargs = dict(
                s3_client=s3_client,
                bucket=bucket,
                prefix=self.key,
                batch_size=batch_size,
                limit=limit,
                encoding_type=encoding_type,
                fetch_owner=fetch_owner,
                start_after=start_after,
                request_payer=request_payer,
                expected_bucket_owner=expected_bucket_owner,
            )
            if recursive is False:
                kwargs["delimiter"] = "/"
            for content in (
                paginate_list_objects_v2(**kwargs)
                .contents()
                .filter(is_content_an_object)
            ):
                yield self._from_content_dict(bucket, dct=content)

        return S3PathIterProxy(_iter_s3path())

    def iterdir(
        self: "S3Path",
        batch_size: int = 1000,
        limit: int = NOTHING,
        encoding_type: str = NOTHING,
        fetch_owner: bool = NOTHING,
        start_after: str = NOTHING,
        request_payer: str = NOTHING,
        expected_bucket_owner: str = NOTHING,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> S3PathIterProxy:
        """
        iterate objects and folder under this prefix non-recursively,
        yield :class:`S3Path`.

        :param batch_size: number of s3 object returned per paginator,
            valid value is from 1 ~ 1000. large number can reduce IO.
        :param limit: total number of s3 object (not folder)to return

        .. versionadded:: 1.0.6

        .. versionchanged:: 2.1.1

            Support all list_objects_v2 arguments.
        """
        s3_client = resolve_s3_client(context, bsm)
        bucket = self.bucket

        def _iter_s3path() -> T.Iterable["S3Path"]:
            proxy = paginate_list_objects_v2(
                s3_client=s3_client,
                bucket=bucket,
                prefix=self.key,
                batch_size=batch_size,
                limit=limit,
                delimiter="/",
                encoding_type=encoding_type,
                fetch_owner=fetch_owner,
                start_after=start_after,
                request_payer=request_payer,
                expected_bucket_owner=expected_bucket_owner,
            )
            for res in proxy:
                for dct in res.get("CommonPrefixes", list()):
                    yield self.root.joinpath(dct["Prefix"])

                for dct in res.get("Contents", list()):
                    yield self._from_content_dict(self.bucket, dct)

        return S3PathIterProxy(_iter_s3path())

    def calculate_total_size(
        self: "S3Path",
        for_human: bool = False,
        include_folder: bool = False,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> T.Tuple[int, T.Union[int, str]]:
        """
        Perform the "Calculate Total Size" action in AWS S3 console

        :param for_human: if true, returns human readable string for "size"
        :param include_folder: see :meth:`iter_objects`

        :return: a tuple, first value is number of objects,
            second value is total size in bytes

        .. versionadded:: 1.0.1
        """
        self.ensure_dir()
        s3_client = resolve_s3_client(context, bsm)
        count, size = calculate_total_size(
            s3_client=s3_client,
            bucket=self.bucket,
            prefix=self.key,
            include_folder=include_folder,
        )
        if for_human:
            size = utils.repr_data_size(size)
        return count, size

    def count_objects(
        self: "S3Path",
        include_folder: bool = False,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> int:
        """
        Count how many objects are under this s3 directory.

        :param include_folder: see :meth:`iter_objects`

        :return: an integer represents the number of objects

        .. versionadded:: 1.0.1
        """
        self.ensure_dir()
        s3_client = resolve_s3_client(context, bsm)
        return count_objects(
            s3_client=s3_client,
            bucket=self.bucket,
            prefix=self.key,
            include_folder=include_folder,
        )
