# -*- coding: utf-8 -*-

"""
The base constructor of S3Path object.
"""

import typing as T

try:
    import botocore.exceptions
except ImportError:  # pragma: no cover
    pass
except:  # pragma: no cover
    raise

from .filterable_property import FilterableProperty
from .. import utils

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path


class BaseS3Path:
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

    def __new__(
        cls: T.Type["S3Path"],
        *args: T.Union[str, "S3Path"],
    ) -> "S3Path":
        return cls._from_parts(args)

    @classmethod
    def _from_parts(
        cls: T.Type["S3Path"],
        args: T.List[T.Union[str, "S3Path"]],
        init: bool = True,
    ) -> "S3Path":
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
        elif isinstance(arg, BaseS3Path):
            _bucket = arg._bucket
            _parts.extend(arg._parts)
        else:
            raise TypeError

        # resolve self._parts
        for arg in args[1:]:
            if isinstance(arg, str):
                utils.validate_s3_key(arg)
                _parts.extend(utils.split_parts(arg))
            elif isinstance(arg, BaseS3Path):
                if arg._bucket is None:
                    _parts.extend(arg._parts)
                else:
                    raise TypeError(
                        "from the second arguments, it has to be raw string "
                        "(as a part) or a relative S3Path (without bucket)! "
                        f"this is invalid: {arg}."
                    )
            else:
                raise TypeError

        # resolve self._is_dir
        # inspect the last argument
        if isinstance(arg, str):
            _is_dir = arg.endswith("/")
        elif isinstance(arg, BaseS3Path):
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
        cls: T.Type["S3Path"],
        bucket: T.Optional[str],
        parts: T.List[str],
        is_dir: T.Optional[bool],
        init: bool = True,
    ) -> "S3Path":
        self = object.__new__(cls)
        self._bucket = bucket
        self._parts = parts
        self._is_dir = is_dir
        self._meta = None
        if init:
            self._init()
        return self

    def _init(self: "S3Path") -> None:
        """
        Additional instance initialization
        """
        pass

    @FilterableProperty
    def parts(self: "S3Path") -> T.List[str]:
        """
        Provides sequence-like access to the components in the filesystem path.
        It doesn't include the bucket, because bucket is considered as "drive".

        .. versionadded:: 1.0.1
        """
        return self._parts
