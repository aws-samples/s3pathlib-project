# -*- coding: utf-8 -*-

"""
AWS environment for testing.
"""

import os
import sys
from boto_session_manager import BotoSesManager, AwsServiceEnum
from ..aws import context


if "CI" in os.environ:
    aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID_FOR_GITHUB_CI"]
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY_FOR_GITHUB_CI"]
    bsm = BotoSesManager(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name="us-east-1",
    )
    boto_ses = bsm.boto_ses
    runtime = "ci"
else:
    bsm = BotoSesManager(profile_name="aws_data_lab_sanhe_opensource_s3pathlib")
    boto_ses = bsm.boto_ses
    runtime = "local"

context.attach_boto_session(boto_ses)

s3_client = bsm.get_client(AwsServiceEnum.S3)

bucket = "aws-data-lab-sanhe-for-opensource"
prefix = "unittest/s3pathlib/{runtime}/{os}/py{major}{minor}".format(
    runtime=runtime,
    os=sys.platform,
    major=sys.version_info.major,
    minor=sys.version_info.minor,
)
