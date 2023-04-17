# -*- coding: utf-8 -*-

from s3pathlib.better_client.head_bucket import is_bucket_exists
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


class BetterHeadBucket(BaseTest):
    module = "better_client.head_bucket"

    @classmethod
    def custom_setup_class(cls):
        cls.bsm.s3_client.create_bucket(Bucket="this-bucket-exists")

    def test(self):
        assert is_bucket_exists(self.s3_client, "this-bucket-exists") is True
        assert is_bucket_exists(self.s3_client, "this-bucket-not-exists") is False


class TestUseMock(BetterHeadBucket):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.better_client.head_bucket", preview=False)
