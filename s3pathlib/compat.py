# -*- coding: utf-8 -*-

"""
Provide compatibility with older versions of Python and dependent libraries.
"""

import sys

try:
    import smart_open

    smart_open_version = smart_open.__version__
    (
        smart_open_version_major,
        smart_open_version_minor,
        _,
    ) = smart_open_version.split(".")
    smart_open_version_major = int(smart_open_version_major)
    smart_open_version_minor = int(smart_open_version_minor)
except ImportError:  # pragma: no cover
    smart_open = None
    smart_open_version_major = None
    smart_open_version_minor = None
except:  # pragma: no cover
    raise

if sys.version_info.minor < 8:
    from cached_property import cached_property
else:
    from functools import cached_property


class Compat:  # pragma: no cover
    @property
    def smart_open_version_major(self) -> int:
        if smart_open_version_major is None:
            raise ImportError("You don't have smart_open installed")
        return smart_open_version_major

    @property
    def smart_open_version_minor(self) -> int:
        if smart_open_version_minor is None:
            raise ImportError("You don't have smart_open installed")
        return smart_open_version_minor


compat = Compat()
