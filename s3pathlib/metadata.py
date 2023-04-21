# -*- coding: utf-8 -*-

"""
This module provides AWS S3 object metadata manipulation helpers.
"""

import warnings

from .type import MetadataType


def warn_upper_case_in_metadata_key(metadata: MetadataType):
    """
    Warn if there are uppercase letters used in user-defined metadata.

    Ref:

    - https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#UserMetadata
    """
    for k, v in metadata.items():
        if k.lower() != k:
            msg = (
                f"Based on this document "
                f"https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#UserMetadata "
                f"Amazon will automatically convert user-defined metadata key to lowercase. "
                f"However, you have a key {k!r} in the metadata that uses uppercase letters."
            )
            warnings.warn(msg, UserWarning)
