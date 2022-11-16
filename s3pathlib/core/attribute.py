# -*- coding: utf-8 -*-

"""
S3Path property methods.
"""

import typing as T

from .filterable_property import FilterableProperty

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path


class AttributeAPIMixin:
    """
    A mixin class that implements the property methods.
    """

    @property
    def parent(self: "S3Path") -> T.Optional["S3Path"]:
        """
        Return the parent s3 directory.

        - if current object is on s3 bucket root directory, it returns bucket
            root directory
        - it is always a directory (``s3path.is_dir() is True``)
        - if it is already s3 bucket root directory, it returns ``None``

        Examples::

            >>> S3Path("my-bucket", "my-folder", "my-file.json").parent.uri
            s3://my-bucket/my-folder/

            >>> S3Path("my-bucket", "my-folder", "my-subfolder/").parent.uri
            s3://my-bucket/my-folder/

            >>> S3Path("my-bucket", "my-folder").parent.uri
            s3://my-bucket/

            >>> S3Path("my-bucket", "my-file.json").parent.uri
            s3://my-bucket/

        .. versionadded:: 1.0.1
        """
        if len(self._parts) == 0:
            return self
        else:
            return self._from_parsed_parts(
                bucket=self._bucket,
                parts=self._parts[:-1],
                is_dir=True,
            )

    @property
    def parents(self: "S3Path") -> T.List["S3Path"]:
        """
        An immutable sequence providing access to the logical ancestors of
        the path.

        .. versionadded:: 1.0.6
        """
        if self.is_void():
            raise ValueError(f"void S3path doesn't support .parents method!")
        if self.is_relpath():
            raise ValueError(f"relative S3path doesn't support .parents method!")
        l = list()
        parent = self
        while 1:
            new_parent = parent.parent
            if parent.uri == new_parent.uri:
                break
            else:
                l.append(new_parent)
                parent = new_parent
        return l

    def is_parent_of(self: "S3Path", other: "S3Path") -> bool:
        """
        Test if it is the parent directory or grand-grand-... parent directory
        of another one.

        Examples::

            # is parent
            >>> S3Path("bucket").is_parent_of(S3Path("bucket/folder/"))
            True

            # is grand parent
            >>> S3Path("bucket").is_parent_of(S3Path("bucket/folder/file.txt"))
            True

            # the root bucket's parent is itself
            >>> S3Path("bucket").is_parent_of(S3Path("bucket"))
            True

            # the 'self' has to be a directory
            >>> S3Path("bucket/a").is_parent_of(S3Path("bucket/a/b/c"))
            TypeError: S3Path('s3://bucket/a') is not a valid directory!

            # the 'self' and 'other' has to be concrete S3Path
            >>> S3Path().is_parent_of(S3Path())
            TypeError: both S3Path(), S3Path() has to be a concrete S3Path!

        .. versionadded:: 1.0.2
        """
        if self._bucket is None or other._bucket is None:
            raise TypeError(f"both {self}, {other} has to be a concrete S3Path!")
        if self._bucket != other._bucket:
            return False
        if self.is_dir() is False:
            raise TypeError(f"{self} is not a valid directory!")
        n_parts_other = len(other.parts)
        if n_parts_other == 0:
            return len(self._parts) == 0
        else:
            return (
                self._parts[: (n_parts_other - 1)] == other._parts[:-1]
                and len(self._parts) < n_parts_other
            )

    def is_prefix_of(self: "S3Path", other: "S3Path") -> bool:
        """
        Test if it is a prefix of another one.

        Example::

            >>> S3Path("bucket/folder/").is_prefix_of(S3Path("bucket/folder/file.txt"))
            True

            >>> S3Path("bucket/folder/").is_prefix_of(S3Path("bucket/folder/"))
            True

        .. versionadded:: 1.0.2
        """
        if self._bucket is None or other._bucket is None:
            raise TypeError(f"both {self}, {other} has to be a concrete S3Path!")
        if self._bucket != other._bucket:
            return False
        return self.uri <= other.uri

    @FilterableProperty
    def basename(self: "S3Path") -> T.Optional[str]:
        """
        The file name with extension, or the last folder name if it is a
        directory. If not available, it returns None. For example it doesn't
        make sence for s3 bucket.

        Logically: dirname + basename = abspath

        Example::

            # s3 object
            >>> S3Path("bucket", "folder", "file.txt").basename
            'file.txt'

            # s3 directory
            >>> S3Path("bucket", "folder/").basename
            'folder'

            # s3 bucket
            >>> S3Path("bucket").basename
            None

            # void path
            >>> S3Path().basename
            ''

            # relative path
            >>> S3Path.make_relpath("folder", "file.txt").basename
            None

        .. versionadded:: 1.0.1
        """
        if len(self._parts):
            return self._parts[-1]
        else:
            return ""

    @FilterableProperty
    def dirname(self: "S3Path") -> T.Optional[str]:
        """
        The basename of it's parent directory.

        Example::

            >>> S3Path("bucket", "folder", "file.txt").dirname
            'folder'

            # root dir name is ''
            >>> S3Path("bucket", "folder").dirname
            ''

        .. versionadded:: 1.0.1
        """
        return self.parent.basename

    @FilterableProperty
    def fname(self: "S3Path") -> str:
        """
        The final path component, minus its last suffix (file extension).
        Only if it is not a directory.

        Example::

            >>> S3Path("bucket", "folder", "file.txt").fname
            'file'

        .. versionadded:: 1.0.1
        """
        if self.is_dir():
            raise TypeError
        basename: str = self.basename
        if not basename:
            raise ValueError
        i = basename.rfind(".")
        if 0 < i < len(basename) - 1:
            return basename[:i]
        else:
            return basename

    @FilterableProperty
    def ext(self: "S3Path") -> str:
        """
        The final component's last suffix, if any. Usually it is the file
        extension. Only if it is not a directory.

        Example::

            >>> S3Path("bucket", "folder", "file.txt").fname
            '.txt'

        .. versionadded:: 1.0.1
        """
        if self.is_dir():
            raise TypeError
        basename: str = self.basename
        if not basename:
            raise ValueError
        i = basename.rfind(".")
        if 0 < i < len(basename) - 1:
            return basename[i:]
        else:
            return ""

    @FilterableProperty
    def abspath(self: "S3Path") -> str:
        """
        The Unix styled absolute path from the bucket. You can think of the
        bucket as a root drive.

        Example::

            # s3 object
            >>> S3Path("bucket", "folder", "file.txt").abspath
            '/folder/file.txt'

            # s3 directory
            >>> S3Path("bucket", "folder/").abspath
            '/folder/'

            # s3 bucket
            >>> S3Path("bucket").abspath
            '/'

            # void path
            >>> S3Path().abspath
            TypeError: relative path doesn't have absolute path!

            # relative path
            >>> S3Path.make_relpath("folder", "file.txt").abspath
            TypeError: relative path doesn't have absolute path!

        .. versionadded:: 1.0.1
        """
        if self._bucket is None:
            raise TypeError("relative path doesn't have absolute path!")
        if self.is_bucket():
            return "/"
        elif self.is_dir():
            return "/" + "/".join(self._parts) + "/"
        elif self.is_file():
            return "/" + "/".join(self._parts)
        else:  # pragma: no cover
            raise TypeError

    @FilterableProperty
    def dirpath(self: "S3Path"):
        """
        The Unix styled absolute path from the bucket of the **parent directory**.

        Example::

            # s3 object
            >>> S3Path("bucket", "folder", "file.txt").dirpath
            '/folder/'

            # s3 directory
            >>> S3Path("bucket", "folder/").dirpath
            '/'

            # s3 bucket
            >>> S3Path("bucket").dirpath
            '/'

            # void path
            >>> S3Path().dirpath
            TypeError: relative path doesn't have absolute path!

            # relative path
            >>> S3Path.make_relpath("folder", "file.txt").dirpath
            TypeError: relative path doesn't have absolute path!

        .. versionadded:: 1.0.2
        """
        return self.parent.abspath

    @property
    def root(self: "S3Path") -> "S3Path":
        """
        Return the S3 Bucket root folder S3Path object.

        Example::

            # s3 object
            >>> S3Path("bucket", "folder", "file.txt").root
            '/folder/'
        """
        if self.is_relpath() or self.is_void():
            raise TypeError("only concrete File or Directory has a bucket root!")
        else:
            return self._from_parsed_parts(
                bucket=self.bucket,
                parts=[],
                is_dir=True,
            )

    def __repr__(self: "S3Path"):
        """
        You cannot use the returned string of __repr__ to recover the original
        S3Path method.
        """
        classname = self.__class__.__name__

        if self.is_void():
            return "S3VoidPath()"

        elif self.is_relpath():
            key = self.key
            if len(key):
                return f"S3RelPath({key!r})"
            else:  # pragma: no cover
                return "S3RelPath()"
        else:
            uri = self.uri
            return "{}('{}')".format(classname, uri)

    def __str__(self: "S3Path"):
        return self.__repr__()
