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
