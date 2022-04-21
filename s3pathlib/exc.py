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
Exception creator and helpers, argument validators, and more.
"""

def ensure_one_and_only_one_not_none(**kwargs) -> None:
    """
    Ensure only exact one of the keyword argument is not None.
    """
    if len(kwargs) == 0:
        raise ValueError
    if sum([v is not None for _, v in kwargs.items()]) != 1:
        raise ValueError(
            f"one and only one of arguments from "
            f"{list(kwargs)} can be not None!"
        )

def ensure_all_none(**kwargs) -> None:
    """
    Ensure all of the keyword argument is None.
    """
    if len(kwargs) == 0:
        raise ValueError
    if sum([v is not None for _, v in kwargs.items()]) != 0:
        raise ValueError(
            f"arguments from {list(kwargs)} has to be all None!"
        )