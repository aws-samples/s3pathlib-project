# -*- coding: utf-8 -*-

import typing as T
from boto_session_manager import BotoSesManager, AwsServiceEnum
from ..aws import Context


def resolve_s3_client(
    context: Context,
    bsm: T.Optional['BotoSesManager'] = None,
):
    """
    Figure out the final boto session to use for API call.
    If ``BotoSesManager`` is defined, then prioritize to use it.
    """
    if bsm is None:
        return context.s3_client
    else:
        return bsm.get_client(AwsServiceEnum.S3)
