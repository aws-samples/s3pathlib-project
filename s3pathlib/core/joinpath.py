# -*- coding: utf-8 -*-

"""
Join path operator implementation.
"""

import typing as T

from .relative import RelativePathAPIMixin
from ..marker import warn_deprecate

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path


class JoinPathAPIMixin:
    """
    A mixin class that implements the join path operator.
    """

    def join_path(self: "S3Path", *others: "S3Path") -> "S3Path":
        """
        Join with other relative path to form a new path

        Example::

            # create some s3path
            >>> p1 = S3Path("bucket", "folder", "subfolder", "file.txt")
            >>> p2 = p1.parent
            >>> relpath1 = p1.relative_to(p2)

            # preview value
            >>> p1
            S3Path('s3://bucket/folder/subfolder/file.txt')
            >>> p2
            S3Path('s3://bucket/folder/subfolder/')
            >>> relpath1
            S3Path('file.txt')

            # join one relative path
            >>> p2.join_path(relpath1)
            S3Path('s3://bucket/folder/subfolder/file.txt')

            # join multiple relative path
            >>> p3 = p2.parent
            >>> relpath2 = p2.relative_to(p3)
            >>> p3.join_path(relpath2, relpath1)
            S3Path('s3://bucket/folder/subfolder/file.txt')

        :param others: many relative path

        :return: a new :class:`S3Path`

        .. versionadded:: 1.0.1
        """
        warn_deprecate(
            func_name="S3Path.join_path",
            version="2.1.1",
            message="S3Path.joinpath",
        )
        args = [
            self,
        ]
        for relp in others:
            if relp.is_relpath() is False:
                msg = (
                    "you can only join with relative path! " "{} is not a relative path"
                ).format(relp)
                raise TypeError(msg)
            args.append(relp)
        return self._from_parts(args)

    def joinpath(self: "S3Path", *other: T.Union[str, "S3Path"]) -> "S3Path":
        """
        Join with other relative path or string parts.

        Example::

            # join with string parts
            >>> p = S3Path("bucket")
            >>> p.joinpath("folder", "file.txt")
            S3Path('s3://bucket/folder/file.txt')

            # join ith relative path or string parts
            >>> p = S3Path("bucket")
            >>> relpath = S3Path("my-bucket", "data", "folder/").relative_to(S3Path("my-bucket", "data"))
            >>> p.joinpath("data", relpath, "file.txt")
            S3Path('s3://bucket/data/folder/file.txt')

        :param others: many string or relative path

        .. versionadded:: 1.1.1
        """
        args = [
            self,
        ]
        for part in other:
            if isinstance(part, str):
                args.append(part)
            elif isinstance(part, RelativePathAPIMixin):
                if part.is_relpath() is False:
                    msg = (
                        "you can only join with string part or relative path! "
                        "{} is not a relative path"
                    ).format(part)
                    raise TypeError(msg)
                else:
                    args.append(part)
            else:
                msg = (
                    "you can only join with string part or relative path! "
                    "{} is not a relative path"
                ).format(part)
                raise TypeError(msg)
        return self._from_parts(args)

    def __truediv__(
        self: "S3Path", other: T.Union[str, "S3Path", T.List[T.Union[str, "S3Path"]]]
    ) -> "S3Path":
        """
        A syntax sugar. Basically ``S3Path(s3path, part1, part2, ...)``
        is equal to ``s3path / part1 / part2 / ...`` or
        ``s3path / [part1, part2]``

        Example::

            >>> S3Path("bucket") / "folder" / "file.txt"
            S3Path('s3://bucket/folder/file.txt')

            >>> S3Path("bucket") / ["folder", "file.txt"]
            S3Path('s3://bucket/folder/file.txt')

            # relative path also work
            >>> S3Path("new-bucket") / (S3Path("bucket/file.txt").relative_to(S3Path("bucket")))
            S3Path('s3://new-bucket/file.txt')

        .. versionadded:: 1.0.11
        """
        if self.is_void():
            raise TypeError("You cannot do ``VoidS3Path / other``!")
        if isinstance(other, list):
            res = self
            for part in other:
                res = res / part
            return res
        else:
            if (
                self.is_relpath()
                and isinstance(other, RelativePathAPIMixin)
                and other.is_relpath() is False
            ):
                raise TypeError("you cannot do ``RelativeS3Path / NonRelativeS3Path``!")
            return self.joinpath(other)
