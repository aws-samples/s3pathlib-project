# -*- coding: utf-8 -*-

"""
Relative Path implementation.
"""

import typing as T

from .. import utils

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path


class RelativePathAPIMixin:
    """
    A mixin class that implements the relative path concept.
    """

    @classmethod
    def make_relpath(
        cls: T.Type["S3Path"],
        *parts: str,
    ) -> "S3Path":
        """
        A construct method that create a relative S3 Path.

        Definition of relative path:

        - no bucket (self.bucket is None)
        - has some parts or no part. when no part, it is a special relative path.
            Any path add this relative path resulting to itself. We call this
            special relative path **Void relative path**. A
            **Void relative path** is logically equivalent to **Void s3 path**.
        - relative path can be a file (object) or a directory. The
            **Void relative path** is neither a file or a directory.

        :param parts:

        .. versionadded:: 1.0.1

        **中文文档**

        相对路径的概念是 p1 + p2 = p3, 其中 p1 和 p3 都是实际存在的路径, 而 p2 则是
        相对路径.

        相对路径的功能是如果 p3 - p1 = p2, 那么 p1 + p2 必须还能等于 p3. 有一个特殊情况是
        如果 p1 - p1 = p0, 两个相同的绝对路径之间的相对路径是 p0, 我们还是需要满足
        p1 + p0 = p1 以保证逻辑上的一致.
        """
        _parts = list()
        last_non_empty_part = None
        for part in parts:
            chunks = utils.split_parts(part)
            if len(chunks):
                last_non_empty_part = part
            for _part in chunks:
                _parts.append(_part)

        if len(_parts):
            _is_dir = last_non_empty_part.endswith("/")
        else:
            _is_dir = None

        return cls._from_parsed_parts(
            bucket=None,
            parts=_parts,
            is_dir=_is_dir,
        )

    def relative_to(self: "S3Path", other: "S3Path") -> "S3Path":
        """
        Return the relative path to another path. If the operation
        is not possible (because this is not a sub path of the other path),
        raise ``ValueError``.

        ``-`` is a syntax sugar for ``relative_to``. See more information at
        :meth:`~S3Path.__sub__`.

        The relative path usually works with :meth:`join_path` to form a new
        path. Or you can use the ``/`` syntax sugar as well. See more
        information at :meth:`~S3Path.__truediv__`.

        Examples::

            >>> S3Path("bucket", "a/b/c").relative_to(S3Path("bucket", "a")).parts
            ['b', 'c']

            >>> S3Path("bucket", "a").relative_to(S3Path("bucket", "a")).parts
            []

            >>> S3Path("bucket", "a").relative_to(S3Path("bucket", "a/b/c")).parts
            ValueError ...

        :param other: other :class:`S3Path` instance.

        :return: a relative path object, which is a special version of S3Path

        .. versionadded:: 1.0.1
        """
        if (
            (self._bucket != other._bucket)
            or (self._bucket is None)
            or (other._bucket is None)
        ):
            msg = (
                "both 'from path' {} and 'to path' {} has to be concrete path!"
            ).format(self, other)
            raise ValueError(msg)

        n = len(other._parts)
        if self._parts[:n] != other._parts:
            msg = "{} does not start with {}".format(
                self.uri,
                other.uri,
            )
            raise ValueError(msg)
        rel_parts = self._parts[n:]
        if len(rel_parts):
            is_dir = self._is_dir
        else:
            is_dir = None
        return self._from_parsed_parts(
            bucket=None,
            parts=rel_parts,
            is_dir=is_dir,
        )

    def is_relpath(self: "S3Path") -> bool:
        """
        Relative path is a special path supposed to join with other concrete
        path.

        **Definition**

        A long full path relating to its parent directory is a relative path.
        A :meth:`void <is_void>` path also a special relative path.

        .. versionadded:: 1.0.1
        """
        if self._bucket is None:
            if len(self._parts) == 0:
                if self._is_dir is None:
                    return True
                else:
                    return False
            else:
                return True
        else:
            return False

    def __sub__(self: "S3Path", other: "S3Path") -> "S3Path":
        """
        A syntax sugar. Basically ``s3path1 - s3path2`` is equal to
        ``s3path2.relative_to(s3path1)``

        .. versionadded:: 1.0.11
        """
        return self.relative_to(other)

    def ensure_relpath(self: "S3Path") -> None:
        """
        A validator method that ensure it represents a S3 relative path.

        .. versionadded:: 1.2.1
        """
        if self.is_relpath() is not True:
            raise TypeError(f"{self} IS a s3 relative path!")

    def ensure_not_relpath(self: "S3Path") -> None:
        """
        A validator method that ensure it represents a S3 relative path.

        Can be used if you want to raise error if it is not a relative path.

        .. versionadded:: 1.0.1
        """
        if self.is_relpath() is True:
            raise TypeError(f"{self} is not a valid s3 relative path!")
