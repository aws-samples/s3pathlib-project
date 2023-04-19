# -*- coding: utf-8 -*-

import typing as T
import os
import sys

import botocore.exceptions
import moto
from boto_session_manager import BotoSesManager
from rich import print as rprint
from s3pathlib import S3Path, context
from s3pathlib.compat import cached_property

if T.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

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


def get_bucket_name(bsm: BotoSesManager, name: str) -> str:
    return f"{bsm.aws_account_id}-{bsm.aws_region}-{name}"


def is_bucket_exists(bsm: BotoSesManager, name: str) -> bool:
    try:
        bsm.s3_client.head_bucket(Bucket=name)
        return True
    except botocore.exceptions.ClientError as e:
        if "Not Found" in str(e):
            return False
        else:  # pragma: no cover
            raise e


def create_bucket(bsm: BotoSesManager, name: str):
    if is_bucket_exists(bsm, name) is False:
        kwargs = dict(Bucket=name)
        if bsm.aws_region != "us-east-1":
            kwargs["CreateBucketConfiguration"] = dict(
                LocationConstraint=bsm.aws_region
            )
        bsm.s3_client.create_bucket(**kwargs)


class BaseTest:
    """
    Class attributes:

    - use_mock: if True, use moto.mock_s3, otherwise use real S3
    - module: the Python module you are testing with, the module name will become
        a sub-folder in the test S3 bucket.
    """
    use_mock: bool
    module: str

    bsm: BotoSesManager
    force_use_mock: bool = False

    @classmethod
    def custom_setup_class(cls):
        """
        You can override this method to do custom setup.
        """
        pass

    @classmethod
    def custom_teardown_class(cls):
        """
        You can override this method to do custom teardown.
        """
        pass

    @classmethod
    def get_bucket(cls) -> str:
        """
        Get the bucket name for testing.
        """
        return get_bucket_name(cls.bsm, "s3pathlib-test")

    @classmethod
    def get_bucket_with_versioning(cls) -> str:
        """
        Get the bucket name for testing versioning feature.
        """
        return get_bucket_name(cls.bsm, "s3pathlib-test-versioning-on")

    @classmethod
    def setup_moto(cls):
        if cls.force_use_mock is True:
            cls.use_mock = True

        if cls.use_mock is True:
            cls.mock_s3 = moto.mock_s3()
            cls.mock_sts = moto.mock_sts()

            cls.mock_s3.start()
            cls.mock_sts.start()
        elif cls.use_mock is False:
            pass
        else:
            raise NotImplementedError("use_mock must be True or False!")

    @classmethod
    def teardown_moto(cls):
        if cls.force_use_mock is True:
            cls.use_mock = True

        if cls.use_mock is True:
            cls.mock_s3.stop()
            cls.mock_sts.stop()
        elif cls.use_mock is False:
            pass
        else:
            raise NotImplementedError("use_mock must be True or False!")

    @classmethod
    def setup_boto_session(cls):
        kwargs = dict(region_name="us-east-1")
        if cls.use_mock is True:
            pass
        elif cls.use_mock is False:
            if runtime == "ci":
                kwargs["aws_access_key_id"] = os.environ[
                    "AWS_ACCESS_KEY_ID_FOR_GITHUB_CI"
                ]
                kwargs["aws_secret_access_key"] = os.environ[
                    "AWS_SECRET_ACCESS_KEY_FOR_GITHUB_CI"
                ]
            else:
                kwargs["profile_name"] = "aws_data_lab_sanhe_opensource_s3pathlib"
        else:
            raise NotImplementedError("use_mock must be True or False!")

        cls.bsm = BotoSesManager(**kwargs)
        context.attach_boto_session(cls.bsm.boto_ses)

    @classmethod
    def setup_bucket(cls):
        """
        We need two buckets for testing. One is a regular bucket, another is a
        bucket with versioning enabled.

        For integration test, we should manually create these two buckets and
        turn on versioning for the second bucket.
        """
        if cls.use_mock:
            create_bucket(cls.bsm, cls.get_bucket())
            create_bucket(cls.bsm, cls.get_bucket_with_versioning())

            cls.bsm.s3_client.create_bucket(Bucket=cls.get_bucket())
            cls.bsm.s3_client.create_bucket(Bucket=cls.get_bucket_with_versioning())
            cls.bsm.s3_client.put_bucket_versioning(
                Bucket=cls.get_bucket_with_versioning(),
                VersioningConfiguration={"Status": "Enabled"},
            )

    @classmethod
    def setup_class(cls):
        cls.setup_moto()
        cls.setup_boto_session()
        cls.setup_bucket()
        cls.custom_setup_class()

    @classmethod
    def teardown_class(cls):
        cls.teardown_moto()
        cls.custom_teardown_class()

    @classmethod
    def get_module_folders(cls) -> str:
        """
        The module name will become a sub-folder in the test S3 bucket,
        providing module level test cases isolation.
        """
        return "/".join(cls.module.split("."))

    @cached_property
    def s3_client(self):
        return self.bsm.s3_client

    @cached_property
    def bucket(self) -> str:
        return self.get_bucket()

    @cached_property
    def bucket_with_versioning(self) -> str:
        return self.get_bucket_with_versioning()

    @classmethod
    def get_prefix(cls) -> str:
        return f"{prefix}/{cls.get_module_folders()}"

    @cached_property
    def prefix(self) -> str:
        return self.get_prefix()

    @classmethod
    def get_s3dir_root(cls) -> S3Path:
        return S3Path(
            cls.get_bucket(),
            cls.get_prefix(),
        ).to_dir()

    @classmethod
    def get_s3dir_root_with_versioning(cls) -> S3Path:
        return S3Path(
            cls.get_bucket_with_versioning(),
            cls.get_prefix(),
        ).to_dir()

    @cached_property
    def s3dir_root(self) -> S3Path:
        """
        The root S3 directory for testing in the regular S3 bucket.
        """
        return self.get_s3dir_root()

    @cached_property
    def s3dir_root_with_versioning(self) -> S3Path:
        """
        The root S3 directory for testing in the versioning S3 bucket.
        """
        return self.get_s3dir_root_with_versioning()

    def rprint_response(self, res: dict):
        """
        Pretty print the boto3 response.
        """
        if "ResponseMetadata" in res:
            res.pop("ResponseMetadata")
        rprint(res)

    def rprint(self, data):
        rprint(data)
