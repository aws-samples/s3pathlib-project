# -*- coding: utf-8 -*-

"""
Tagging related API.
"""

import typing as T

from iterproxy import IterProxy

from .. import utils
from ..aws import context
from .resolve_s3_client import resolve_s3_client
from .iter_objects import S3PathIterProxy

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager

class IterObjectVersionsAPIMixin:
    """
    A mixin class that implements the iter object versions methods.
    """
    pass