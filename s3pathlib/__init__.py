# -*- coding: utf-8 -*-
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
  
#   Licensed under the Apache License, Version 2.0 (the "License").
#   You may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
  
#       http://www.apache.org/licenses/LICENSE-2.0
  
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

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
except ImportError: # pragma: no cover
    pass
except: # pragma: no cover
    raise
