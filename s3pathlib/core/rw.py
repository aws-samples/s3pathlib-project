# -*- coding: utf-8 -*-

"""
Read and write related API.
"""

import typing as T

if T.TYPE_CHECKING:
    from .s3path import S3Path

MixinReadWriteAPI: T.Type['S3Path']


class MixinReadWriteAPI:
    pass
