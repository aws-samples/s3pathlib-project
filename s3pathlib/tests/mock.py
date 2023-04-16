# -*- coding: utf-8 -*-

import typing as T
import os
import sys
import moto
from boto_session_manager import BotoSesManager
from s3pathlib import S3Path, context

if "CI" in os.environ:
    runtime = "ci"
else:
    runtime = "local"

prefix = "projects/s3pathlib/unittest/{runtime}/{os}/py{major}{minor}".format(
    runtime=runtime,
    os=sys.platform,
    major=sys.version_info.major,
    minor=sys.version_info.minor,
)


def get_test_bucket(bsm: BotoSesManager, suffix: str) -> str:
    return f"{bsm.aws_account_id}-{bsm.aws_region}-{suffix}"


class BaseTest:
    module: T.Optional[str] = None
    mock_s3 = None
    mock_sts = None
    bsm: T.Optional[BotoSesManager] = None

    @classmethod
    def custom_setup_class(cls):
        pass

    @classmethod
    def custom_teardown_class(cls):
        pass

    @classmethod
    def get_bucket(cls) -> str:
        return get_test_bucket(cls.bsm, "s3pathlib-test")

    @classmethod
    def get_bucket_with_versioning(cls) -> str:
        return get_test_bucket(cls.bsm, "s3pathlib-test-versioning-on")

    @classmethod
    def setup_class(cls):
        cls.mock_s3 = moto.mock_s3()
        cls.mock_sts = moto.mock_sts()
        cls.mock_s3.start()
        cls.mock_sts.start()

        cls.bsm = BotoSesManager(region_name="us-east-1")
        context.attach_boto_session(cls.bsm.boto_ses)

        cls.bsm.s3_client.create_bucket(Bucket=cls.get_bucket())
        cls.bsm.s3_client.create_bucket(Bucket=cls.get_bucket_with_versioning())

        cls.custom_setup_class()

    @classmethod
    def teardown_class(cls):
        cls.mock_s3.stop()
        cls.mock_sts.stop()

        cls.custom_teardown_class()

    @property
    def prefix(self) -> str:
        return prefix

    @property
    def s3dir_root(self) -> S3Path:
        return S3Path(
            self.get_bucket(),
            self.prefix,
            *self.module.split(".")
        ).to_dir()

    @property
    def s3dir_root_with_versioning(self) -> S3Path:
        return S3Path(
            self.get_bucket_with_versioning(),
            self.prefix,
            *self.module.split(".")
        ).to_dir()
