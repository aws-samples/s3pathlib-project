# -*- coding: utf-8 -*-

"""
Type hint variables.
"""

import typing as T

from pathlib import Path as Path1
from pathlib_mate import Path as Path2

TagType = T.Dict[str, str]  # {"Key": "Value"}
TagSetType = T.List[T.Dict[str, str]]  # [{"Key": "Value"}, ...]

MetadataType = T.Dict[str, str]  # {"Key": "Value"}
PathType = T.Union[str, Path1, Path2]  # str, pathlib.Path, pathlib_mate.Path
