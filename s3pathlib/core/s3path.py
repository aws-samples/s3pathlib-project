# -*- coding: utf-8 -*-

from .base import S3Path as BaseS3Path
from .rw import MixinReadWriteAPI


class S3Path(
    BaseS3Path,
    MixinReadWriteAPI,
):
    pass
