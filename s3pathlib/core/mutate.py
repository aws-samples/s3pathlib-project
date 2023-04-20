# -*- coding: utf-8 -*-

"""
S3Path object mutation implementation.
"""

import typing as T

from .. import exc

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path


class MutateAPIMixin:
    """
    A mixin class that implements the S3Path object mutation.
    """

    def copy(self: "S3Path") -> "S3Path":
        """
        Create a copy of S3Path object that logically equals to this one,
        but is actually different identity in memory. Also, the cache data
        are cleared.

        Example::

            >>> p1 = S3Path("bucket", "folder", "file.txt")
            >>> p2 = p1.copy()
            >>> p1 == p2
            True
            >>> p1 is p2
            False

        .. versionadded:: 1.0.1
        """
        return self._from_parsed_parts(
            bucket=self._bucket,
            parts=list(self._parts),
            is_dir=self._is_dir,
        )

    def change(
        self: "S3Path",
        new_bucket: str = None,
        new_abspath: str = None,
        new_dirpath: str = None,
        new_dirname: str = None,
        new_basename: str = None,
        new_fname: str = None,
        new_ext: str = None,
    ) -> "S3Path":
        """
        Create a new S3Path by replacing part of the attributes. If no argument
        is given, it behaves like :meth:`copy`.

        Example:

            >>> s3path = S3Path("bucket", "folder", "file.txt")
            >>> s3path.change(new_bucket="new_bucket")
            S3Path('s3://new_bucket/folder/file.txt')

            >>> s3path = S3Path("bucket", "folder", "file.txt")
            >>> s3path.change(new_basename="data.json")
            S3Path('s3://bucket/folder/data.json')

            >>> s3path = S3Path("bucket", "folder", "file.txt")
            >>> s3path.change(new_fname="log")
            S3Path('s3://bucket/folder/log.txt')

        :param new_bucket: The new bucket name
        :param new_abspath:
        :param new_dirpath:
        :param new_dirname:
        :param new_basename:
        :param new_fname:
        :param new_ext:

        .. versionadded:: 1.0.2
        """
        if new_bucket is None:
            new_bucket = self.bucket

        if new_abspath is not None:
            exc.ensure_all_none(
                new_dirpath=new_dirpath,
                new_dirname=new_dirname,
                new_basename=new_basename,
                new_fname=new_fname,
                new_ext=new_ext,
            )
            p = self._from_parts([self.bucket, new_abspath])
            return p

        if (new_dirpath is None) and (new_dirname is not None):
            dir_parts = self.parent.parent._parts + [new_dirname]
        elif (new_dirpath is not None) and (new_dirname is None):
            dir_parts = [
                new_dirpath,
            ]
        elif (new_dirpath is None) and (new_dirname is None):
            dir_parts = self.parent._parts
        else:
            raise ValueError("Cannot having both 'new_dirpath' and 'new_dirname'!")

        if new_basename is None:
            if new_fname is None:
                new_fname = self.fname
            if new_ext is None:
                new_ext = self.ext
            new_basename = new_fname + new_ext
        else:
            if (new_fname is not None) or (new_ext is not None):
                raise ValueError(
                    "Cannot having both "
                    "'new_basename' / 'new_fname', "
                    "or 'new_basename' / 'new_ext'!"
                )
        if new_bucket is None:
            p = self._from_parts(
                [
                    "dummy-bucket",
                ]
                + dir_parts
                + [
                    new_basename,
                ]
            )
            p._bucket = None
        else:
            p = self._from_parts(
                [
                    new_bucket,
                ]
                + dir_parts
                + [
                    new_basename,
                ]
            )
        return p

    def to_dir(self: "S3Path") -> "S3Path":
        """
        Convert the S3Path to a directory. If the S3Path is a file, then append
        a "/" at the end. If the S3Path is already a directory, then do nothing.

        Example:

            >>> S3Path.from_s3_uri("s3://bucket/folder").to_dir()
            S3Path('s3://bucket/folder/')
        """
        if self.is_dir():
            return self.copy()
        elif self.is_file():
            return self.joinpath("/")
        else:
            raise ValueError("only concrete file or folder S3Path can do .to_dir()")

    def to_file(self: "S3Path") -> "S3Path":
        """
        Convert the S3Path to a file. If the S3Path is a directory, then strip
        out the last "/". If the S3Path is already a file, then do nothing.

        Example:

            >>> S3Path.from_s3_uri("s3://bucket/file/").to_dir()
            S3Path('s3://bucket/file/')
        """
        if self.is_file():
            return self.copy()
        elif self.is_dir():
            p = self.copy()
            p._is_dir = False
            return p
        else:
            raise ValueError("only concrete file or folder S3Path can do .to_file()")
