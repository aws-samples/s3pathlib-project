# -*- coding: utf-8 -*-

"""
The ``S3Path`` public API class.
"""

# The import order is very important. The later one depends on the earlier one.

from .base import BaseS3Path
from .is_test import IsTestAPIMixin
from .uri import UriAPIMixin
from .relative import RelativePathAPIMixin
from .comparison import ComparisonAPIMixin
from .attribute import AttributeAPIMixin
from .joinpath import JoinPathAPIMixin
from .mutate import MutateAPIMixin
from .metadata import MetadataAPIMixin
from .bucket import BucketAPIMixin
from .tagging import TaggingAPIMixin
from .iter_objects import IterObjectsAPIMixin
from .exists import ExistsAPIMixin
from .rw import ReadAndWriteAPIMixin
from .delete import DeleteAPIMixin
from .upload import UploadAPIMixin
from .copy import CopyAPIMixin
from .sync import SyncAPIMixin
from .serde import SerdeAPIMixin
from .opener import OpenerAPIMixin


class S3Path(
    BaseS3Path,
    IsTestAPIMixin,
    UriAPIMixin,
    RelativePathAPIMixin,
    ComparisonAPIMixin,
    AttributeAPIMixin,
    JoinPathAPIMixin,
    MutateAPIMixin,
    MetadataAPIMixin,
    BucketAPIMixin,
    TaggingAPIMixin,
    IterObjectsAPIMixin,
    ExistsAPIMixin,
    ReadAndWriteAPIMixin,
    DeleteAPIMixin,
    UploadAPIMixin,
    CopyAPIMixin,
    SyncAPIMixin,
    SerdeAPIMixin,
    OpenerAPIMixin,
):
    """
    The ``S3Path`` public API class.
    """
