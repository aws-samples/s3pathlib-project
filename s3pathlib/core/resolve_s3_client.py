# -*- coding: utf-8 -*-

import typing as T

from boto_session_manager import BotoSesManager

from ..aws import Context

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3 import S3Client


def resolve_s3_client(
    context: Context,
    bsm: T.Optional["BotoSesManager"] = None,
) -> "S3Client":
    """
    Figure out the final boto session to use for API call.
    If ``BotoSesManager`` is defined, then prioritize to use it.
    """
    if bsm is None:
        return context.s3_client
    else:
        return bsm.s3_client
