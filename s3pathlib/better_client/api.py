# -*- coding: utf-8 -*-

from .head_bucket import is_bucket_exists
from .head_object import (
    head_object,
    is_object_exists,
)
from .tagging import (
    update_bucket_tagging,
    update_object_tagging,
)
