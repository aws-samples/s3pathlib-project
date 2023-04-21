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
from .upload import (
    upload_dir,
)
from .list_objects import (
    ObjectTypeDefIterproxy,
    CommonPrefixTypeDefIterproxy,
    ListObjectsV2OutputTypeDefIterproxy,
    paginate_list_objects_v2,
    is_content_an_object,
    calculate_total_size,
    count_objects,
)
from .list_object_versions import (
    ObjectVersionTypeDefIterproxy,
    DeleteMarkerEntryTypeDefIterproxy,
    ListObjectVersionsOutputTypeDefIterproxy,
    paginate_list_object_versions,
)
from .delete_object import (
    delete_object,
    delete_dir,
    delete_object_versions,
)
