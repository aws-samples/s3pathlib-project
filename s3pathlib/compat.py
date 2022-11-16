# -*- coding: utf-8 -*-

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
