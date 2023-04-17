# -*- coding: utf-8 -*-

"""
Exception creator and helpers, argument validators, and more.
"""


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


class _UriRelatedError(Exception):
    _tpl: str

    @classmethod
    def make(cls, uri: str):
        return cls._tpl.format(uri=uri)


class S3BucketNotExist(_UriRelatedError):
    _tpl = "S3 bucket {uri} does not exist!"


class S3FolderNotExist(_UriRelatedError):
    _tpl = "S3 folder {uri} does not exist!"


class S3ObjectNotExist(_UriRelatedError):
    _tpl = "S3 object {uri} does not exist!"


class S3BucketAlreadyExist(_UriRelatedError):
    _tpl = "S3 bucket {uri} already exist!"


class S3FolderAlreadyExist(_UriRelatedError):
    _tpl = "S3 folder {uri} already exist!"


class S3ObjectAlreadyExist(_UriRelatedError):
    _tpl = "S3 object {uri} already exist!"


class S3PermissionDenied(Exception):
    pass
