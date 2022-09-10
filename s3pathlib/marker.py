# -*- coding: utf-8 -*-

import warnings
import functools


def warn_deprecate(
    func_name: str,
    version: str,
    message: str,
):
    warnings.warn(f"{func_name!r} will be deprecated on {version}: {message}")


def deprecate_v1(version: str, message: str):
    def deco(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            warn_deprecate(func.__name__, version, message)
            return func(*args, **kwargs)

        return wrapper

    return deco


def deprecate_v2(version: str, message: str):
    from decorator import decorator
    
    @decorator
    def deco(func, *args, **kwargs):
        warn_deprecate(func.__name__, version, message)
        return func(*args, **kwargs)

    return deco
