# -*- coding: utf-8 -*-

"""
Bucket related API.
"""

import typing as T


if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


class BucketAPIMixin:
    """
    A mixin class that implements the bucket related methods.
    """
