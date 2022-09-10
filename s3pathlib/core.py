# -*- coding: utf-8 -*-

"""
This module implements the core OOP interface :class:`S3Path`.

Import::

    >>> from s3pathlib.core import S3Path
    # or
    >>> from s3pathlib import S3Path
"""

import functools
from datetime import datetime
from typing import Tuple, List, Iterable, Union, Optional, Any
from pathlib_mate import Path
from boto_session_manager import BotoSesManager, AwsServiceEnum

try:
    import botocore.exceptions
except ImportError:  # pragma: no cover
    pass
except:  # pragma: no cover
    raise

try:
    import smart_open

    smart_open_version = smart_open.__version__
    (
        smart_open_version_major, smart_open_version_minor, _
    ) = smart_open_version.split(".")
    smart_open_version_major = int(smart_open_version_major)
    smart_open_version_minor = int(smart_open_version_minor)
except ImportError:  # pragma: no cover
    pass
except:  # pragma: no cover
    raise

from . import utils, exc, validate
from .aws import context, Context
from .iterproxy import IterProxy
from .marker import warn_deprecate


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

    def __next__(self) -> 'S3Path':
        return super(S3PathIterProxy, self).__next__()

    def one(self) -> 'S3Path':
        return super(S3PathIterProxy, self).one()

    def one_or_none(self) -> Union['S3Path', None]:
        return super(S3PathIterProxy, self).one_or_none()

    def many(self, k: int) -> List['S3Path']:
        return super(S3PathIterProxy, self).many(k)

    def all(self) -> List['S3Path']:
        return super(S3PathIterProxy, self).all()

    def skip(self, k: int) -> 'S3PathIterProxy':
        return super(S3PathIterProxy, self).skip(k=k)

    def filter_by_ext(self, *exts: str) -> 'S3PathIterProxy':
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

            def f(p: S3Path) -> bool:
                return p.ext.lower() == ext

            return self.filter(f)
        else:
            valid_exts = set([ext.lower() for ext in exts])

            def f(p: S3Path) -> bool:
                return p.ext.lower() in valid_exts

            return self.filter(f)


class FilterableProperty:
    """
    A descriptor decorator that convert a method to a property method.
    ALSO, convert the class attribute to be a comparable object that returns
    filter function for IterProxy to use.

    .. versionadded:: 1.0.3
    """

    def __init__(self, func: callable):
        functools.wraps(func)(self)
        self._func = func

    def __get__(self, obj: Union['S3Path', None], obj_type):
        if obj is None:
            return self
        return self._func(obj)

    def __eq__(self, other):
        """
        .. versionadded:: 1.0.3
        """

        def filter_(obj):
            return self._func(obj) == other

        return filter_

    def __ne__(self, other):
        """
        .. versionadded:: 1.0.3
        """

        def filter_(obj):
            return self._func(obj) != other

        return filter_

    def __gt__(self, other):
        """
        .. versionadded:: 1.0.3
        """

        def filter_(obj):
            return self._func(obj) > other

        return filter_

    def __lt__(self, other):
        """
        .. versionadded:: 1.0.3
        """

        def filter_(obj):
            return self._func(obj) < other

        return filter_

    def __ge__(self, other):
        """
        .. versionadded:: 1.0.3
        """

        def filter_(obj):
            return self._func(obj) >= other

        return filter_

    def __le__(self, other):
        """
        .. versionadded:: 1.0.3
        """

        def filter_(obj):
            return self._func(obj) <= other

        return filter_

    def equal_to(self, other):  # pragma: no cover
        """
        Return a filter function that returns True
        only if ``S3Path.attribute_name == ``other``

        .. versionadded:: 1.0.3
        """
        return self.__eq__(other)

    def not_equal_to(self, other):  # pragma: no cover
        """
        Return a filter function that returns True
        only if ``S3Path.attribute_name != ``other``

        .. versionadded:: 1.0.4
        """
        return self.__ne__(other)

    def greater(self, other):  # pragma: no cover
        """
        Return a filter function that returns True
        only if ``S3Path.attribute_name > ``other``

        .. versionadded:: 1.0.4
        """
        return self.__gt__(other)

    def less(self, other):  # pragma: no cover
        """
        Return a filter function that returns True
        only if ``S3Path.attribute_name < ``other``

        .. versionadded:: 1.0.4
        """
        return self.__lt__(other)

    def greater_equal(self, other):  # pragma: no cover
        """
        Return a filter function that returns True
        only if ``S3Path.attribute_name >= ``other``

        .. versionadded:: 1.0.4
        """
        return self.__eq__(other)

    def less_equal(self, other):  # pragma: no cover
        """
        Return a filter function that returns True
        only if ``S3Path.attribute_name <= ``other``

        .. versionadded:: 1.0.4
        """
        return self.__eq__(other)

    def between(self, lower, upper):
        """
        Return a filter function that returns True
        only if ``lower <= S3Path.attribute_name <= upper``

        .. versionadded:: 1.0.3
        """

        def filter_(obj):
            return lower <= self._func(obj) <= upper

        return filter_

    def startswith(self, other: str):
        """
        Return a filter function that returns True
        only if ``S3Path.attribute_name.startswith(other)``.
        The attribute has to be a string attribute.

        .. versionadded:: 1.0.3
        """

        def filter_(obj):
            return self._func(obj).startswith(other)

        return filter_

    def endswith(self, other: str):
        """
        Return a filter function that returns True
        only if ``S3Path.attribute_name.endswith(other)``.
        The attribute has to be a string attribute.

        .. versionadded:: 1.0.3
        """

        def filter_(obj):
            return self._func(obj).endswith(other)

        return filter_

    def contains(self, other):
        """
        Return a filter function that returns True
        only if ``other in S3Path.attribute_name``

        .. versionadded:: 1.0.3
        """

        def filter_(obj):
            return other in self._func(obj)

        return filter_


def _resolve_s3_client(
    context: Context,
    bsm: Optional['BotoSesManager'] = None,
):
    if bsm is None:
        return context.s3_client
    else:
        return bsm.get_client(AwsServiceEnum.S3)


class S3Path:
    """
    Similar to ``pathlib.Path``. An objective oriented programming interface
    for AWS S3 object or logical directory.

    You can use this class in different way.

    1. pure s3 object / directory path manipulation without actually
        talking to AWS API.
    2. get metadata of an object, count objects, get statistics information
        of a directory
    3. enhanced s3 API that do: :meth:`upload_file <upload_file>`,
        :meth:`upload_dir <upload_dir>`,
        :meth:`copy <copy_to>` a file or directory,
        :meth:`move <move_to>` a file or directory,
        :meth:`delete <delete_if_exists>`  a file or directory,
        :meth:`iter_objects <iter_objects>` from a prefix.

    **Constructor**

    The :class:`S3Path` itself is a constructor. It takes ``str`` and other
    relative :class:`S3Path` as arguments.

    1. The first argument defines the S3 bucket
    2. The rest of arguments defines the path parts of the S3 key
    3. The final argument defines the type whether is a file (object) or
        a directory

    First let's create a S3 object path from string::

        # first argument becomes the bucket
        >>> s3path = S3Path("bucket", "folder", "file.txt")
        # print S3Path object gives you info in S3 URI format
        >>> s3path
        S3Path('s3://bucket/folder/file.txt')

        # last argument defines that it is a file
        >>> s3path.is_file()
        True
        >>> s3path.is_dir()
        True

        # "/" separator will be automatically handled
        >>> S3Path("bucket", "folder/file.txt")
        S3Path('s3://bucket/folder/file.txt')

        >>> S3Path("bucket/folder/file.txt")
        S3Path('s3://bucket/folder/file.txt')

    Then let's create a S3 directory path::

        >>> s3path= S3Path("bucket/folder/")
        >>> s3path
        S3Path('s3://bucket/folder/')

        # last argument defines that it is a directory
        >>> s3path.is_dir()
        True
        >>> s3path.is_file()
        False

    .. versionadded:: 1.0.1
    """
    __slots__ = (
        "_bucket",
        "_parts",
        "_is_dir",
        "_cached_cparts",  # cached comparison parts
        "_hash",  # cached hash value
        "_meta",  # s3 object metadata cache object
    )

    # --------------------------------------------------------------------------
    #               Objective Oriented Programming Implementation
    # --------------------------------------------------------------------------
    __S1_OOP_IMPLEMENTATION__ = None

    def __new__(
        cls,
        *args: Union[str, 'S3Path'],
    ) -> 'S3Path':
        return cls._from_parts(args)

    @classmethod
    def _from_parts(
        cls,
        args: List[Union[str, 'S3Path']],
        init: bool = True,
    ) -> 'S3Path':
        _bucket = None
        _parts = list()
        _is_dir = None

        if len(args) == 0:
            return cls._from_parsed_parts(
                bucket=_bucket,
                parts=_parts,
                is_dir=_is_dir,
            )

        # resolve self._bucket
        arg = args[0]
        if isinstance(arg, str):
            utils.validate_s3_bucket(arg)
            parts = utils.split_parts(arg)
            _bucket = parts[0]
            _parts.extend(parts[1:])
        elif isinstance(arg, S3Path):
            _bucket = arg._bucket
            _parts.extend(arg._parts)
        else:
            raise TypeError

        # resolve self._parts
        for arg in args[1:]:
            if isinstance(arg, str):
                utils.validate_s3_key(arg)
                _parts.extend(utils.split_parts(arg))
            elif isinstance(arg, S3Path):
                _parts.extend(arg._parts)
            else:
                raise TypeError

        # resolve self._is_dir
        # inspect the last argument
        if isinstance(arg, str):
            _is_dir = arg.endswith("/")
        elif isinstance(arg, S3Path):
            _is_dir = arg._is_dir
        else:  # pragma: no cover
            raise TypeError

        if (_bucket is not None) and len(_parts) == 0:  # bucket root
            _is_dir = True

        return cls._from_parsed_parts(
            bucket=_bucket,
            parts=_parts,
            is_dir=_is_dir,
            init=init,
        )

    @classmethod
    def _from_parsed_parts(
        cls,
        bucket: Optional[str],
        parts: List[str],
        is_dir: Optional[bool],
        init: bool = True,
    ) -> 'S3Path':
        self = object.__new__(cls)
        self._bucket = bucket
        self._parts = parts
        self._is_dir = is_dir
        self._meta = None
        if init:
            self._init()
        return self

    def _init(self) -> None:
        """
        Additional instance initialization
        """
        pass

    @classmethod
    def _from_content_dict(cls, bucket: str, dct: dict):
        """
        Construct S3Path object from the response["Content"] dictionary data.

        Example ``dct``::

            {
                'Key': 'string',
                'LastModified': datetime(2015, 1, 1),
                'ETag': 'string',
                'ChecksumAlgorithm': [
                    'CRC32'|'CRC32C'|'SHA1'|'SHA256',
                ],
                'Size': 123,
                'StorageClass': 'STANDARD'|'REDUCED_REDUNDANCY'|'GLACIER'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'DEEP_ARCHIVE'|'OUTPOSTS'|'GLACIER_IR',
                'Owner': {
                    'DisplayName': 'string',
                    'ID': 'string'
                }
            }

        Ref:

        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
        :return:
        """
        p = S3Path(bucket, dct["Key"])
        p._meta = {
            "Key": dct["Key"],
            "LastModified": dct["LastModified"],
            "ETag": dct["ETag"],
            "ContentLength": dct["Size"],
            "StorageClass": dct["StorageClass"],
            "Owner": dct.get("Owner", {}),
        }
        return p

    @classmethod
    def make_relpath(
        cls,
        *parts: str,
    ) -> 'S3Path':
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

    @classmethod
    def from_s3_uri(cls, uri: str) -> 'S3Path':
        """
        Construct an :class:`S3Path` from S3 URI.

        >>> p = S3Path.from_s3_uri("s3://bucket/folder/file.txt")

        >>> p
        S3Path('s3://bucket/folder/file.txt')

        >>> p.uri
        's3://bucket/folder/file.txt'
        """
        validate.validate_s3_uri(uri)
        bucket, key = utils.split_s3_uri(uri)
        return cls._from_parts([bucket, key])

    @classmethod
    def from_s3_arn(cls, arn: str) -> 'S3Path':
        """

        :param arn:
        :return:
        """
        validate.validate_s3_arn(arn)
        return cls._from_parts([arn.replace("arn:aws:s3:::", "", 1), ])

    # --------------------------------------------------------------------------
    #                Method that DOESN't need boto3 API call
    # --------------------------------------------------------------------------
    __S2_ATTRIBUTE_AND_METHOD__ = None

    def to_dict(self) -> dict:
        """
        Serialize to Python dict

        .. versionadded:: 1.0.1
        """
        return {
            "bucket": self._bucket,
            "parts": self._parts,
            "is_dir": self._is_dir,
        }

    @classmethod
    def from_dict(cls, dct) -> 'S3Path':
        """
        Deserialize from Python dict

        .. versionadded:: 1.0.2
        """
        return cls._from_parsed_parts(
            bucket=dct["bucket"],
            parts=dct["parts"],
            is_dir=dct["is_dir"],
        )

    @property
    def _cparts(self):
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

    def __eq__(self, other: 'S3Path') -> bool:
        """
        Return ``self == other``.
        """
        return self._cparts == other._cparts

    def __lt__(self, other: 'S3Path') -> bool:
        """
        Return ``self < other``.
        """
        return self._cparts < other._cparts

    def __gt__(self, other: 'S3Path') -> bool:
        """
        Return ``self > other``.
        """
        return self._cparts > other._cparts

    def __le__(self, other: 'S3Path') -> bool:
        """
        Return ``self <= other``.
        """
        return self._cparts <= other._cparts

    def __ge__(self, other: 'S3Path') -> bool:
        """
        Return ``self >= other``.
        """
        return self._cparts >= other._cparts

    def __hash__(self) -> int:
        """
        Return ``hash(self)``
        """
        try:
            return self._hash
        except AttributeError:
            self._hash = hash(tuple(self._cparts))
            return self._hash

    def __truediv__(
        self,
        other: Union[
            str,
            'S3Path',
            List[Union[str, 'S3Path']]
        ]
    ) -> 'S3Path':
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
                and isinstance(other, S3Path)
                and other.is_relpath() is False
            ):
                raise TypeError("you cannot do ``RelativeS3Path / NonRelativeS3Path``!")
            return S3Path(self, other)

    def __sub__(self, other: 'S3Path') -> 'S3Path':
        """
        A syntax sugar. Basically ``s3path1 - s3path2`` is equal to
        ``s3path2.relative_to(s3path1)``

        .. versionadded:: 1.0.11
        """
        return self.relative_to(other)

    def copy(self) -> 'S3Path':
        """
        Create an duplicate copy of S3Path object that logically equals to
        this one, but is actually different identity in memory. Also the
        cache data are cleared.

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
        self,
        new_bucket: str = None,
        new_abspath: str = None,
        new_dirpath: str = None,
        new_dirname: str = None,
        new_basename: str = None,
        new_fname: str = None,
        new_ext: str = None,
    ) -> 'S3Path':
        """
        Create a new S3Path by replacing part of the attributes. If no argument
        is given, it behave like :meth:`copy`.

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
            dir_parts = [new_dirpath, ]
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
                ["dummy-bucket", ] + dir_parts + [new_basename, ]
            )
            p._bucket = None
        else:
            p = self._from_parts(
                [new_bucket, ] + dir_parts + [new_basename, ]
            )
        return p

    def to_dir(self):
        if self.is_dir():
            return self.copy()
        elif self.is_file():
            return S3Path(self, "/")
        else:
            raise ValueError("only concrete file or folder S3Path can do .to_dir()")

    def to_file(self):
        if self.is_file():
            return self.copy()
        elif self.is_dir():
            p = self.copy()
            p._is_dir = False
            return p
        else:
            raise ValueError("only concrete file or folder S3Path can do .to_file()")

    def is_void(self) -> bool:
        """
        Test if it is a void S3 path.

        **Definition**

        no bucket, no key, no parts, no type, no nothing.
        A void path is also a special :meth:`relative path <is_relpath>`,
        because any path join with void path results to itself.
        """
        return (self._bucket is None) and (len(self._parts) == 0)

    def is_dir(self) -> bool:
        """
        Test if it is a S3 directory

        **Definition**

        A logical S3 directory that never physically exists. An S3
        :meth:`bucket <is_bucket>` is also a special dir, which is the root dir.

        .. versionadded:: 1.0.1
        """
        if self._is_dir is None:
            return False
        else:
            return self._is_dir

    def is_file(self) -> bool:
        """
        Test if it is a S3 object

        **Definition**

        A S3 object.

        .. versionadded:: 1.0.1
        """
        if self._is_dir is None:
            return False
        else:
            return not self._is_dir

    def is_bucket(self) -> bool:
        """
        Test if it is a S3 bucket.

        **Definition**

        A S3 bucket, the root folder S3 path is equivalent to a S3 bucket.
        A S3 bucket are always :meth:`is_dir() is True <is_dir>`

        .. versionadded:: 1.0.1
        """
        return (self._bucket is not None) and \
               (len(self._parts) == 0) and \
               (self._is_dir is True)

    def is_relpath(self) -> bool:
        """
        Relative path is a special path supposed to join with other concrete
        path.

        **Definition**

        A long full path relating to its parents directory is a relative path.
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

    def is_parent_of(self, other: 'S3Path') -> bool:
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
            return self._parts[:(n_parts_other - 1)] == other._parts[:-1] and \
                   len(self._parts) < n_parts_other

    def is_prefix_of(self, other: 'S3Path') -> bool:
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
    def bucket(self) -> Optional[str]:
        """
        Return bucket name as string, if available.

        Example::

            >>> S3Path("bucket/folder/file.txt").bucket
            'bucket'

        .. versionadded:: 1.0.1
        """
        return self._bucket

    @FilterableProperty
    def key(self) -> Optional[str]:
        """
        Return object or directory key as string, if available.

        Examples::

            # a s3 object
            >>> S3Path("bucket/folder/file.txt").key
            'folder/file.txt'

            # a s3 object
            >>> S3Path("bucket/folder/").key
            'folder/file.txt'

            # a relative path
            >>> S3Path("bucket/folder/file.txt").relative_to(S3Path("bucket")).key
            'folder/file.txt

            >>> S3Path("bucket/folder/").relative_to(S3Path("bucket")).key
            'folder/'

            # an empty S3Path
            >>> S3Path().key
            ''

        .. versionadded:: 1.0.1
        """
        if len(self._parts):
            return "{}{}".format(
                "/".join(self._parts),
                "/" if self._is_dir else ""
            )
        else:
            return ""

    @FilterableProperty
    def uri(self) -> Optional[str]:
        """
        Return AWS S3 URI.

        - for regular s3 object, it returns ``"s3://{bucket}/{key}"``
        - if it is a directory, the s3 uri always ends with ``"/"``.
        - if it is bucket only (no key), it returns ``"s3://{bucket}/"``
        - if it is not an concrete S3Path, it returns ``None``
        - it has to have bucket, if not (usually because it is an relative path)
            it returns ``None``

        Examples::

            >>> S3Path("bucket", "folder", "file.txt").uri
            's3://bucket/folder/file.txt'

            >>> S3Path("bucket", "folder/").uri
            's3://bucket/folder/'

            >>> S3Path("bucket").uri
            's3://bucket/'

            # void path doesn't have uri
            >>> S3Path().uri
            None

            # relative path doesn't have uri
            >>> S3Path("bucket/folder/file.txt").relative_to(S3Path("bucket")).uri
            None

        .. versionadded:: 1.0.1
        """
        if self._bucket is None:
            return None
        if len(self._parts):
            return "s3://{}/{}".format(
                self.bucket,
                self.key,
            )
        else:
            return "s3://{}/".format(self._bucket)

    @property
    def console_url(self) -> Optional[str]:
        """
        Return an AWS S3 Console url that can inspect the details.

        .. versionadded:: 1.0.1
        """
        uri: str = self.uri
        if uri is None:
            return None
        else:
            console_url = utils.make_s3_console_url(s3_uri=uri)
            return console_url

    @property
    def us_gov_cloud_console_url(self) -> Optional[str]:
        """
        Return an AWS US Gov Cloud S3 Console url that can inspect the details.

        .. versionadded:: 1.0.5
        """
        uri: str = self.uri
        if uri is None:
            return None
        else:
            console_url = utils.make_s3_console_url(
                s3_uri=uri, is_us_gov_cloud=True
            )
            return console_url

    @property
    def s3_select_console_url(self) -> Optional[str]:
        """
        Return an AWS US Gov Cloud S3 Console url that can inspect data with s3 select.

        .. versionadded:: 1.0.12
        """
        if self.is_file():
            return utils.make_s3_select_console_url(
                bucket=self.bucket,
                key=self.key,
                is_us_gov_cloud=False,
            )
        else:
            raise TypeError("you can only do s3 select with an object!")

    @property
    def s3_select_us_gov_cloud_console_url(self) -> Optional[str]:
        """

        Return an AWS S3 Console url that can inspect data with s3 select.

        .. versionadded:: 1.0.12
        """
        if self.is_file():
            return utils.make_s3_select_console_url(
                bucket=self.bucket,
                key=self.key,
                is_us_gov_cloud=True,
            )
        else:
            raise TypeError("you can only do s3 select with an object!")

    @FilterableProperty
    def arn(self) -> Optional[str]:
        """
        Return an AWS S3 Resource ARN. See `ARN definition here <https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html>`_

        .. versionadded:: 1.0.1
        """
        if self._bucket is None:
            return None
        if len(self._parts):
            return "arn:aws:s3:::{}/{}".format(
                self.bucket,
                self.key,
            )
        else:
            return "arn:aws:s3:::{}".format(self._bucket)

    @FilterableProperty
    def parts(self) -> List[str]:
        """
        Provides sequence-like access to the components in the filesystem path.
        It doesn't include the bucket, because bucket is considered as "drive".

        .. versionadded:: 1.0.1
        """
        return self._parts

    @property
    def parent(self) -> Optional['S3Path']:
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
    def parents(self) -> List['S3Path']:
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

    @FilterableProperty
    def basename(self) -> Optional[str]:
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
    def dirname(self) -> Optional[str]:
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
    def fname(self) -> str:
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
    def ext(self) -> str:
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
    def abspath(self) -> str:
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
    def dirpath(self):
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

    def __repr__(self):
        classname = self.__class__.__name__

        if self.is_void():
            return "S3VoidPath()"

        elif self.is_relpath():
            key = self.key
            if len(key):
                return f"S3RelPath({key!r})"
            else:
                return "S3RelPath()"
        else:
            uri = self.uri
            return "{}('{}')".format(classname, uri)

    def __str__(self):
        return self.__repr__()

    def relative_to(self, other: 'S3Path') -> 'S3Path':
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
        if (self._bucket != other._bucket) or \
            (self._bucket is None) or \
            (other._bucket is None):
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

    def join_path(self, *others: 'S3Path') -> 'S3Path':
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
        args = [self, ]
        for relp in others:
            if relp.is_relpath() is False:
                msg = (
                    "you can only join with relative path! "
                    "{} is not a relative path"
                ).format(relp)
                raise TypeError(msg)
            args.append(relp)
        return self._from_parts(args)

    def joinpath(
        self,
        *other: Union[str, 'S3Path']
    ) -> 'S3Path':
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
        args = [self, ]
        for part in other:
            if isinstance(part, str):
                args.append(part)
            elif isinstance(part, S3Path):
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

    def ensure_object(self) -> None:
        """
        A validator method that ensure it represents a S3 object.

        .. versionadded:: 1.0.1
        """
        if self.is_file() is not True:
            raise TypeError(f"S3 URI: {self} is not a valid s3 object!")

    def ensure_dir(self) -> None:
        """
        A validator method that ensure it represents a S3 object.

        .. versionadded:: 1.0.1
        """
        if self.is_dir() is not True:
            raise TypeError(f"{self} is not a valid s3 directory!")

    def ensure_not_relpath(self) -> None:
        """
        A validator method that ensure it represents a S3 relative path.

        Can be used if you want to raise error if it is not an relative path.

        .. versionadded:: 1.0.1
        """
        if self.is_relpath() is True:
            raise TypeError(f"{self} is not a valid s3 relative path!")

    # --------------------------------------------------------------------------
    #                   Method that need boto3 API call
    # --------------------------------------------------------------------------
    __S3_STATELESS_API__ = None

    def clear_cache(self) -> None:
        """
        Clear all cache that stores metadata information.

        .. versionadded:: 1.0.1
        """
        self._meta = None

    def _head_bucket(self, bsm: Optional['BotoSesManager'] = None) -> dict:
        s3_client = _resolve_s3_client(context, bsm)
        return s3_client.head_bucket(
            Bucket=self.bucket,
        )

    def head_object(self, bsm: Optional['BotoSesManager'] = None) -> dict:
        """
        Call head_object() api, store metadata value.
        """
        s3_client = _resolve_s3_client(context, bsm)
        dct = s3_client.head_object(
            Bucket=self.bucket,
            Key=self.key
        )
        if "ResponseMetadata" in dct:
            del dct["ResponseMetadata"]
        self._meta = dct
        return dct

    def _get_meta_value(
        self,
        key: str,
        default: Any = None,
        bsm: Optional['BotoSesManager'] = None,
    ) -> Any:
        if self._meta is None:
            self.head_object(bsm=bsm)
        return self._meta.get(key, default)

    @FilterableProperty
    def etag(self) -> str:
        """
        For small file, it is the md5 check sum. For large file, because it is
        created from multi part upload, it is the sum of md5 for each part and
        md5 of the sum.

        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        .. versionadded:: 1.0.1
        """
        return self._get_meta_value(key="ETag")[1:-1]

    @FilterableProperty
    def last_modified_at(self) -> datetime:
        """
        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        .. versionadded:: 1.0.1
        """
        return self._get_meta_value(key="LastModified")

    @FilterableProperty
    def size(self) -> int:
        """
        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        .. versionadded:: 1.0.1
        """
        return self._get_meta_value(key="ContentLength")

    @property
    def size_for_human(self) -> str:
        """
        A human readable string version of the size.

        .. versionadded:: 1.0.1
        """
        return utils.repr_data_size(self.size)

    @FilterableProperty
    def version_id(self) -> int:
        """
        Only available if you turned on versioning for the bucket.

        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        .. versionadded:: 1.0.1
        """
        return self._get_meta_value(key="VersionId")

    @FilterableProperty
    def expire_at(self) -> datetime:
        """
        Only available if you turned on TTL

        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        .. versionadded:: 1.0.1
        """
        return self._get_meta_value(key="Expires")

    @property
    def metadata(self) -> dict:
        """
        Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        .. versionadded:: 1.0.1
        """
        return self._get_meta_value(key="Metadata", default=dict())

    def exists(self, bsm: Optional['BotoSesManager'] = None) -> bool:
        """
        - For S3 bucket: check if the bucket exists. If you don't have the
            access, then it raise exception.
        - For S3 object: check if the object exists
        - For S3 directory: check if the directory exists, it returns ``True``
            even if the folder doesn't have any object.

        .. versionadded:: 1.0.1
        """
        if self.is_bucket():
            try:
                self._head_bucket(bsm=bsm)
                return True
            except botocore.exceptions.ClientError as e:
                if "Not Found" in str(e):
                    return False
                else:  # pragma: no cover
                    raise e
            except:  # pragma: no cover
                raise
        elif self.is_file():
            s3_client = _resolve_s3_client(context, bsm)
            dct = utils.head_object_if_exists(
                s3_client=s3_client,
                bucket=self.bucket,
                key=self.key,
            )
            if isinstance(dct, dict):
                self._meta = dct
                return True
            else:
                return False
        elif self.is_dir():
            l = list(self.iter_objects(
                batch_size=1,
                limit=1,
                include_folder=True,
                bsm=bsm,
            ))
            if len(l):
                return True
            else:
                return False
        else:  # pragma: no cover
            raise TypeError

    def ensure_not_exists(self, bsm: Optional['BotoSesManager'] = None) -> None:
        """
        A validator method ensure that it doesn't exists.

        .. versionadded:: 1.0.1
        """
        if self.exists(bsm=bsm):
            utils.raise_file_exists_error(self.uri)

    # --------------------------------------------------------------------------
    #            Method that change the state of S3 bucket / objects
    # --------------------------------------------------------------------------
    __S4_STATEFUL_API__ = None

    def upload_file(
        self,
        path: str,
        overwrite: bool = False,
        extra_args: dict = None,
        callback: callable = None,
        config=None,
        bsm: Optional['BotoSesManager'] = None,
    ) -> dict:
        """
        Upload a file from local file system to targeted S3 path

        Example::

            >>> s3path = S3Path("bucket", "artifacts", "deployment.zip")
            >>> s3path.upload_file(path="/tmp/build/deployment.zip", overwrite=True)

        :param path: absolute path of the file on the local file system
            you want to upload
        :param overwrite: if False, non of the file will be upload / overwritten
            if any of target s3 location already taken.

        .. versionadded:: 1.0.1
        """
        self.ensure_object()
        if overwrite is False:
            self.ensure_not_exists(bsm=bsm)
        p = Path(path)
        s3_client = _resolve_s3_client(context, bsm)
        return s3_client.upload_file(
            p.abspath,
            Bucket=self.bucket,
            Key=self.key,
            ExtraArgs=extra_args,
            Callback=callback,
            Config=config,
        )

    def upload_dir(
        self,
        local_dir: str,
        pattern: str = "**/*",
        overwrite: bool = False,
        bsm: Optional['BotoSesManager'] = None,
    ) -> int:
        """
        Upload a directory on local file system and all sub-folders, files to
        a S3 prefix (logical directory)

        Example::

            >>> s3path = S3Path("bucket", "datalake", "orders/")
            >>> s3path.upload_dir(path="/data/orders", overwrite=True)

        :param local_dir: absolute path of the directory on the
            local file system you want to upload
        :param pattern: linux styled glob pattern match syntax. see this
            official reference
            https://docs.python.org/3/library/pathlib.html#pathlib.Path.glob
            for more details
        :param overwrite: if False, non of the file will be upload / overwritten
            if any of target s3 location already taken.

        :return: number of files uploaded

        .. versionadded:: 1.0.1
        """
        self.ensure_dir()
        s3_client = _resolve_s3_client(context, bsm)
        return utils.upload_dir(
            s3_client=s3_client,
            bucket=self.bucket,
            prefix=self.key,
            local_dir=local_dir,
            pattern=pattern,
            overwrite=overwrite,
        )

    def _iter_objects(
        self,
        batch_size: int = 1000,
        limit: int = None,
        recursive: bool = True,
        include_folder: bool = False,
        bsm: Optional['BotoSesManager'] = None,
    ) -> Iterable['S3Path']:
        s3_client = _resolve_s3_client(context, bsm)
        for dct in utils.iter_objects(
            s3_client=s3_client,
            bucket=self.bucket,
            prefix=self.key,
            batch_size=batch_size,
            limit=limit,
            recursive=recursive,
            include_folder=include_folder,
        ):
            yield S3Path._from_content_dict(
                bucket=self.bucket,
                dct=dct,
            )

    def iter_objects(
        self,
        batch_size: int = 1000,
        limit: int = None,
        recursive: bool = True,
        include_folder: bool = False,
        bsm: Optional['BotoSesManager'] = None,
    ) -> S3PathIterProxy:
        """
        Recursively iterate objects under this prefix, yield :class:`S3Path`.

        :param batch_size: number of s3 object returned per paginator,
            valid value is from 1 ~ 1000. large number can reduce IO.
        :param limit: total number of s3 object to return
        :param recursive: if True, it won't include files in sub folders
        :param include_folder: AWS S3 consider object that key endswith "/"
            and size = 0 as a logical folder. But physically it is still object.
            By default ``list_objects_v2`` API returns logical folder object,
            you can use this flag to filter it out.

        .. versionadded:: 1.0.1

        TODO: add unix glob liked syntax for pattern matching
        """
        return S3PathIterProxy(
            iterable=self._iter_objects(
                batch_size=batch_size,
                limit=limit,
                recursive=recursive,
                include_folder=include_folder,
                bsm=bsm,
            )
        )

    def _iterdir(
        self,
        batch_size: int = 1000,
        limit: int = None,
        bsm: Optional['BotoSesManager'] = None,
    ) -> Iterable['S3Path']:
        s3_client = _resolve_s3_client(context, bsm)
        paginator = s3_client.get_paginator("list_objects_v2")
        pagination_config = dict(PageSize=batch_size)
        if limit:  # pragma: no cover
            pagination_config["MaxItems"] = limit

        for res in paginator.paginate(
            Bucket=self.bucket,
            Prefix=self.key,
            Delimiter="/",
            PaginationConfig=pagination_config,
        ):
            for dct in res.get("CommonPrefixes", list()):
                yield S3Path(self.bucket, dct["Prefix"])

            for dct in res.get("Contents", list()):
                yield S3Path._from_content_dict(self.bucket, dct)

    def iterdir(
        self,
        batch_size: int = 1000,
        limit: int = None,
        bsm: Optional['BotoSesManager'] = None,
    ) -> S3PathIterProxy:
        """
        iterate objects and folder under this prefix non-recursively,
        yield :class:`S3Path`.

        :param batch_size: number of s3 object returned per paginator,
            valid value is from 1 ~ 1000. large number can reduce IO.
        :param limit: total number of s3 object (not folder)to return

        .. versionadded:: 1.0.6
        """
        return S3PathIterProxy(
            iterable=self._iterdir(
                batch_size=batch_size,
                limit=limit,
                bsm=bsm,
            )
        )

    def calculate_total_size(
        self,
        for_human: bool = False,
        include_folder: bool = False,
        bsm: Optional['BotoSesManager'] = None,
    ) -> Tuple[int, Union[int, str]]:
        """
        Perform the "Calculate Total Size" action in AWS S3 console

        :param for_human: if true, returns human readable string for "size"
        :param include_folder: see :meth:`iter_objects`

        :return: a tuple, first value is number of objects,
            second value is total size in bytes

        .. versionadded:: 1.0.1
        """
        self.ensure_dir()
        s3_client = _resolve_s3_client(context, bsm)
        count, size = utils.calculate_total_size(
            s3_client=s3_client,
            bucket=self.bucket,
            prefix=self.key,
            include_folder=include_folder,
        )
        if for_human:
            size = utils.repr_data_size(size)
        return count, size

    def count_objects(
        self,
        include_folder: bool = False,
        bsm: Optional['BotoSesManager'] = None,
    ) -> int:
        """
        Count how many objects are under this s3 directory.

        :param include_folder: see :meth:`iter_objects`

        :return: an integer represents the number of objects

        .. versionadded:: 1.0.1
        """
        self.ensure_dir()
        s3_client = _resolve_s3_client(context, bsm)
        return utils.count_objects(
            s3_client=s3_client,
            bucket=self.bucket,
            prefix=self.key,
            include_folder=include_folder,
        )

    def delete_if_exists(
        self,
        mfa: str = None,
        version_id: str = None,
        request_payer: str = None,
        bypass_governance_retention: bool = None,
        expected_bucket_owner: str = None,
        include_folder: bool = True,
        bsm: Optional['BotoSesManager'] = None,
    ):
        """
        Delete an object or an entire directory. Will do nothing
        if it doesn't exist.

        Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object

        :param include_folder: see :meth:`iter_objects`

        :return: number of object is deleted

        .. versionadded:: 1.0.1
        """
        s3_client = _resolve_s3_client(context, bsm)
        if self.is_file():
            if self.exists(bsm=bsm):
                kwargs = dict(
                    Bucket=self.bucket,
                    Key=self.key,
                )
                additional_kwargs = utils.collect_not_null_kwargs(
                    MFA=mfa,
                    VersionId=version_id,
                    RequestPayer=request_payer,
                    BypassGovernanceRetention=bypass_governance_retention,
                    ExpectedBucketOwner=expected_bucket_owner,
                )
                kwargs.update(additional_kwargs)
                s3_client.delete_object(**kwargs)
                return 1
            else:
                return 0
        elif self.is_dir():
            return utils.delete_dir(
                s3_client=s3_client,
                bucket=self.bucket,
                prefix=self.key,
                mfa=mfa,
                request_payer=request_payer,
                bypass_governance_retention=bypass_governance_retention,
                expected_bucket_owner=expected_bucket_owner,
                include_folder=include_folder,
            )
        else:  # pragma: no cover
            raise ValueError

    def copy_file(
        self,
        dst: 'S3Path',
        overwrite: bool = False,
        bsm: Optional['BotoSesManager'] = None,
    ) -> dict:
        """
        Copy an S3 directory to a different S3 directory, including all
        sub directory and files.

        :param dst: copy to s3 object, it has to be an object
        :param overwrite: if False, non of the file will be upload / overwritten
            if any of target s3 location already taken.

        :return: number of object are copied, 0 or 1.

        .. versionadded:: 1.0.1
        """
        self.ensure_object()
        dst.ensure_object()
        self.ensure_not_relpath()
        dst.ensure_not_relpath()
        if overwrite is False:
            dst.ensure_not_exists(bsm=bsm)
        s3_client = _resolve_s3_client(context, bsm)
        return s3_client.copy_object(
            Bucket=dst.bucket,
            Key=dst.key,
            CopySource={
                "Bucket": self.bucket,
                "Key": self.key
            }
        )

    def copy_dir(
        self,
        dst: 'S3Path',
        overwrite: bool = False,
        bsm: Optional['BotoSesManager'] = None,
    ):
        """
        Copy an S3 directory to a different S3 directory, including all
        sub directory and files.

        :param dst: copy to s3 directory, it has to be a directory
        :param overwrite: if False, non of the file will be upload / overwritten
            if any of target s3 location already taken.

        :return: number of objects are copied

        .. versionadded:: 1.0.1
        """
        self.ensure_dir()
        dst.ensure_dir()
        self.ensure_not_relpath()
        dst.ensure_not_relpath()
        todo: List[Tuple[S3Path, S3Path]] = list()
        for p_src in self.iter_objects(bsm=bsm):
            p_relpath = p_src.relative_to(self)
            p_dst = S3Path(dst, p_relpath)
            todo.append((p_src, p_dst))

        if overwrite is False:
            for p_src, p_dst in todo:
                p_dst.ensure_not_exists(bsm=bsm)

        for p_src, p_dst in todo:
            p_src.copy_file(p_dst, overwrite=True, bsm=bsm)

        return len(todo)

    def copy_to(
        self,
        dst: 'S3Path',
        overwrite: bool = False,
        bsm: Optional['BotoSesManager'] = None,
    ) -> int:
        """
        Copy s3 object or s3 directory from one place to another place.

        :param dst: copy to s3 path
        :param overwrite: if False, non of the file will be upload / overwritten
            if any of target s3 location already taken.

        .. versionadded:: 1.0.1
        """
        if self.is_dir():
            return self.copy_dir(
                dst=dst,
                overwrite=overwrite,
                bsm=bsm,
            )
        elif self.is_file():
            self.copy_file(
                dst=dst,
                overwrite=overwrite,
                bsm=bsm,
            )
            return 1
        else:  # pragma: no cover
            raise TypeError

    def move_to(
        self,
        dst: 'S3Path',
        overwrite: bool = False,
        bsm: Optional['BotoSesManager'] = None,
    ) -> int:
        """
        Move s3 object or s3 directory from one place to another place. It is
        firstly :meth:`S3Path.copy_to` then :meth:`S3Path.delete_if_exists`

        :param dst: copy to s3 path
        :param overwrite: if False, non of the file will be upload / overwritten
            if any of target s3 location already taken.

        .. versionadded:: 1.0.1
        """
        count = self.copy_to(
            dst=dst,
            overwrite=overwrite,
            bsm=bsm,
        )
        self.delete_if_exists(bsm=bsm)
        return count

    def open(
        self,
        mode="r",
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
        closefd=True,
        opener=None,
        ignore_ext=False,
        compression=None,
        api_kwargs: dict = None,
        bsm: Optional['BotoSesManager'] = None,
    ):
        """
        Open S3Path as a file-liked object.

        :return: a file-like object.

        See https://github.com/RaRe-Technologies/smart_open for more info.
        """
        s3_client = _resolve_s3_client(context, bsm)
        kwargs = dict(
            uri=self.uri,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            closefd=closefd,
            opener=opener,
            transport_params={"client": s3_client}
        )
        if smart_open_version_major < 6:  # pragma: no cover
            kwargs["ignore_ext"] = ignore_ext
        if smart_open_version_major >= 5 and smart_open_version_major >= 1:  # pragma: no cover
            if compression is not None:
                kwargs["compression"] = compression
        return smart_open.open(**kwargs)

    def read_text(
        self,
        encoding="utf-8",
        errors=None,
        bsm: Optional['BotoSesManager'] = None,
    ) -> str:
        with self.open(
            mode="r",
            encoding=encoding,
            errors=errors,
            bsm=bsm,
        ) as f:
            return f.read()

    def read_bytes(self, bsm: Optional['BotoSesManager'] = None) -> bytes:
        with self.open(mode="rb", bsm=bsm) as f:
            return f.read()

    def write_text(
        self,
        data: str,
        encoding="utf-8",
        errors=None,
        newline=None,
        bsm: Optional['BotoSesManager'] = None,
    ):
        with self.open(
            mode="w",
            encoding=encoding,
            errors=errors,
            newline=newline,
            bsm=bsm
        ) as f:
            f.write(data)

    def write_bytes(self, data: bytes, bsm: Optional['BotoSesManager'] = None):
        with self.open(mode="wb", bsm=bsm) as f:
            f.write(data)

    def touch(
        self,
        exist_ok: bool = True,
        bsm: Optional['BotoSesManager'] = None,
    ):  # pragma: no cover
        if not self.is_file():
            raise ValueError

        if self.exists(bsm=bsm):
            if exist_ok:
                pass
            else:
                raise FileExistsError
        else:
            self.write_text("", bsm=bsm)

    def mkdir(
        self,
        exist_ok: bool = False,
        parents: bool = False,
        bsm: Optional['BotoSesManager'] = None,
    ):
        if not self.is_dir():
            raise ValueError

        s3_client = _resolve_s3_client(context, bsm)
        dct = utils.head_object_if_exists(
            s3_client=s3_client,
            bucket=self.bucket,
            key=self.key,
        )
        if dct:
            if exist_ok:
                pass
            else:
                raise FileExistsError
        else:
            s3_client.put_object(
                Bucket=self.bucket,
                Key=self.key,
                Body="",
            )

        if parents:
            for p in self.parents:
                if p.is_bucket() is False:
                    p.mkdir(exist_ok=True, parents=False, bsm=bsm)

    # --------------------------------------------------------------------------
    #                         S3 Object Tagging
    # --------------------------------------------------------------------------
    __S5_TAGGING__ = None

    def get_tagging(self, *args, **kwargs) -> dict:
        raise NotImplementedError

    def put_tagging(self, *args, **kwargs) -> dict:
        raise NotImplementedError

    def delete_tagging(self, *args, **kwargs) -> dict:
        raise NotImplementedError

    def get_acl(self, *args, **kwargs) -> dict:
        raise NotImplementedError

    def put_acl(self, *args, **kwargs) -> dict:
        raise NotImplementedError

    def get_legal_hold(self, *args, **kwargs) -> dict:
        raise NotImplementedError

    def put_legal_hold(self, *args, **kwargs) -> dict:
        raise NotImplementedError
