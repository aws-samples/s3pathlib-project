# -*- coding: utf-8 -*-

"""
List object versions related API.
"""

import typing as T

from func_args import NOTHING

from ..aws import context
from ..better_client.list_object_versions import paginate_list_object_versions
from .iter_objects import S3PathIterProxy
from .resolve_s3_client import resolve_s3_client

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


class IterObjectVersionsAPIMixin:
    """
    A mixin class that implements the iter object versions methods.
    """

    def list_object_versions(
        self: "S3Path",
        batch_size: int = 1000,
        limit: int = NOTHING,
        delimiter: str = NOTHING,
        encoding_type: str = NOTHING,
        expected_bucket_owner: str = NOTHING,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> S3PathIterProxy:
        """
        :param bsm: See bsm_.
        """
        s3_client = resolve_s3_client(context, bsm)
        bucket = self.bucket

        def _iter_s3path() -> T.Iterable["S3Path"]:
            proxy = paginate_list_object_versions(
                s3_client=s3_client,
                bucket=bucket,
                prefix=self.key,
                batch_size=batch_size,
                limit=limit,
                delimiter=delimiter,
                encoding_type=encoding_type,
                expected_bucket_owner=expected_bucket_owner,
            )
            s3path_list = list()
            for response in proxy:
                (
                    versions,
                    delete_markers,
                    _,
                ) = proxy.extract_versions_and_delete_markers_and_common_prefixes(
                    response
                )

                s3path_list.extend(
                    [self._from_version_dict(bucket, dct=dct) for dct in versions]
                )
                s3path_list.extend(
                    [
                        self._from_delete_marker(bucket, dct=dct)
                        for dct in delete_markers
                    ]
                )
                for s3path in sorted(
                    s3path_list, key=lambda x: x.last_modified_at, reverse=True
                ):
                    yield s3path

        return S3PathIterProxy(_iter_s3path())
