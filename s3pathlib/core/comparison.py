# -*- coding: utf-8 -*-

"""
Comparison operator implementation.
"""

import typing as T

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path


class ComparisonAPIMixin:
    """
    A mixin class that implements the comparison operator magic methods.
    """
    @property
    def _cparts(self: "S3Path"):
        """
        Cached comparison parts, for hashing and comparison
        """
        try:
            return self._cached_cparts
        except AttributeError:
            cparts = list()

            if self._bucket:
                cparts.append(self._bucket)
            else:
                cparts.append("")

            cparts.extend(self._parts)

            if self._is_dir:
                cparts.append("/")

            self._cached_cparts = cparts
            return self._cached_cparts

    def __eq__(self: "S3Path", other: "S3Path") -> bool:
        """
        Return ``self == other``.
        """
        return self._cparts == other._cparts

    def __lt__(self: "S3Path", other: "S3Path") -> bool:
        """
        Return ``self < other``.
        """
        return self._cparts < other._cparts

    def __gt__(self: "S3Path", other: "S3Path") -> bool:
        """
        Return ``self > other``.
        """
        return self._cparts > other._cparts

    def __le__(self: "S3Path", other: "S3Path") -> bool:
        """
        Return ``self <= other``.
        """
        return self._cparts <= other._cparts

    def __ge__(self: "S3Path", other: "S3Path") -> bool:
        """
        Return ``self >= other``.
        """
        return self._cparts >= other._cparts

    def __hash__(self: "S3Path") -> int:
        """
        Return ``hash(self)``
        """
        try:
            return self._hash
        except AttributeError:
            self._hash = hash(tuple(self._cparts))
            return self._hash
