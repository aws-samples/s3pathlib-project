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

from typing import Optional

try:
    import boto3
except ImportError:  # pragma: no cover
    pass
except:  # pragma: no cover
    raise


class Context:
    """
    A globally available context object managing P

    TODO: use singleton pattern to create context object
    """

    def __init__(self):
        self.boto_ses: Optional[boto3.session.Session] = None
        self._s3_client = None

        # try to create default session
        try:
            self.boto_ses = boto3.session.Session()
        except:  # pragma: no cover
            pass

    def attach_boto_session(self, boto_ses):
        """
        Attach a custom boto session.

        :type boto_ses: boto3.session.Session
        """
        self.boto_ses = boto_ses
        self._s3_client = None

    @property
    def s3_client(self):
        """
        Access the s3 client.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client
        """
        if self._s3_client is None:
            self._s3_client = self.boto_ses.client("s3")
        return self._s3_client


context = Context()
