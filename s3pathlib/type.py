# -*- coding: utf-8 -*-

"""
Type hint variables.

Note:

- This module is not for public API
"""

import typing as T

from pathlib import Path as Path1
from pathlib_mate import Path as Path2

TagType = T.Dict[str, str]
TagSetType = T.List[T.Dict[str, str]]

MetadataType = T.Dict[str, str]
PathType = T.Union[str, Path1, Path2]
