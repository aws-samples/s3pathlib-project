# -*- coding: utf-8 -*-

"""
Objective Oriented Interface for AWS S3, similar to pathlib.
"""

from ._version import __version__

__short_description__ = "Objective Oriented Interface for AWS S3, similar to pathlib."
__license__ = "MIT"
__author__ = "Sanhe Hu"
__author_email__ = "husanhe@gmail.com"
__github_username__ = "MacHu-GWU"

try:
    from . import utils
    from .aws import context
    from .core import S3Path
except ImportError:  # pragma: no cover
    pass
except:  # pragma: no cover
    raise
