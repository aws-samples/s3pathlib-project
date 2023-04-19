# -*- coding: utf-8 -*-

"""
Exception creator and helpers, argument validators, and more.
"""

import typing as T

if T.TYPE_CHECKING:  # pragma: no cover
    from .core.s3path import S3Path


def ensure_one_and_only_one_not_none(**kwargs) -> None:
    """
    Ensure only exact one of the keyword argument is not None.
    """
    if len(kwargs) == 0:
        raise ValueError
    if sum([v is not None for _, v in kwargs.items()]) != 1:
        raise ValueError(
            f"one and only one of arguments from " f"{list(kwargs)} can be not None!"
        )


def ensure_all_none(**kwargs) -> None:
    """
    Ensure all the keyword arguments are None.
    """
    if len(kwargs) == 0:
        raise ValueError
    if sum([v is not None for _, v in kwargs.items()]) != 0:
        raise ValueError(f"arguments from {list(kwargs)} has to be all None!")


class _UriRelatedError:
    _tpl: str

    @classmethod
    def make(
        cls: T.Type[T.Union[Exception, "_UriRelatedError"]],
        uri: str,
    ):
        return cls(cls._tpl.format(uri=uri))


class S3NotExist(FileNotFoundError, _UriRelatedError):
    _tpl = "{uri!r} does not exist!"


class S3BucketNotExist(FileNotFoundError, _UriRelatedError):
    _tpl = "S3 bucket {uri!r} does not exist!"


class S3FolderNotExist(FileNotFoundError, _UriRelatedError):
    _tpl = "S3 folder {uri!r} does not exist!"


class S3FileNotExist(FileNotFoundError, _UriRelatedError):
    _tpl = "S3 object {uri!r} does not exist!"


class S3AlreadyExist(FileExistsError, _UriRelatedError):
    _tpl = "{uri!r} already exist!"


class S3BucketAlreadyExist(FileExistsError, _UriRelatedError):
    _tpl = "S3 bucket {uri!r} already exist!"


class S3FolderAlreadyExist(FileExistsError, _UriRelatedError):
    _tpl = "S3 folder {uri!r} already exist!"


class S3FileAlreadyExist(FileExistsError, _UriRelatedError):
    _tpl = "S3 object {uri!r} already exist!"


class S3PermissionDenied(PermissionError):
    pass


class _S3PathTypeError(TypeError):
    _expected_type: str

    @classmethod
    def make(
        cls: T.Type[T.Union[Exception, "_S3PathTypeError"]],
        s3path: "S3Path",
    ):
        return cls(f"{s3path!r} is not a {cls._expected_type}! ")


class S3PathIsNotBucketError(_S3PathTypeError):
    _expected_type = "bucket"


class S3PathIsNotFolderError(_S3PathTypeError):
    _expected_type = "dir"


class S3PathIsNotFileError(_S3PathTypeError):
    _expected_type = "file"


class S3PathIsNotRelpathError(_S3PathTypeError):
    _expected_type = "relpath"
