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
            f"one and only one of arguments from "
            f"{list(kwargs)} can be not None!"
        )


def ensure_all_none(**kwargs) -> None:
    """
    Ensure all the keyword arguments are None.
    """
    if len(kwargs) == 0:
        raise ValueError
    if sum([v is not None for _, v in kwargs.items()]) != 0:
        raise ValueError(
            f"arguments from {list(kwargs)} has to be all None!"
        )


class S3BucketNotExist(Exception):
    pass


class S3ObjectNotExist(Exception):
    pass


class S3PermissionDenied(Exception):
    pass
