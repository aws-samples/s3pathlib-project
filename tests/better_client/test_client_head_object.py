# -*- coding: utf-8 -*-

import pytest
from s3pathlib import exc
from s3pathlib.better_client.head_object import (
    head_object,
    is_object_exists,
)
from s3pathlib.utils import smart_join_s3_key
from s3pathlib.tests import run_cov_test

from dummy_data import DummyData


class BetterHeadObject(DummyData):
    module = "better_client.head_object"

    @classmethod
    def custom_setup_class(cls):
        cls.setup_dummy_data()

    def _test_before_and_after_put_object(self):
        # at begin, no object exists
        bucket = self.bucket
        key = smart_join_s3_key([self.prefix, "file.txt"], is_dir=False)
        assert is_object_exists(self.s3_client, bucket=bucket, key=key) is False

        with pytest.raises(exc.S3FileNotExist):
            head_object(self.s3_client, bucket=bucket, key=key)

        assert (
            head_object(self.s3_client, bucket=bucket, key=key, ignore_not_found=True)
            is None
        )

        # put the object
        self.s3_client.put_object(Bucket=bucket, Key=key, Body="hello")

        assert is_object_exists(self.s3_client, bucket=bucket, key=key) is True

        res = head_object(self.s3_client, bucket=bucket, key=key)
        assert res["ContentLength"] == len("hello")

    def _test_is_object_exists(self):
        s3_client = self.s3_client
        bucket = self.bucket

        assert (
            is_object_exists(s3_client=s3_client, bucket=bucket, key=self.key_hello)
            is True
        )
        assert (
            is_object_exists(
                s3_client=s3_client, bucket=bucket, key=self.key_soft_folder
            )
            is False
        )
        assert (
            is_object_exists(
                s3_client=s3_client, bucket=bucket, key=self.prefix_soft_folder
            )
            is False
        )
        assert (
            is_object_exists(
                s3_client=s3_client, bucket=bucket, key=self.key_soft_folder_file
            )
            is True
        )
        assert (
            is_object_exists(
                s3_client=s3_client, bucket=bucket, key=self.key_hard_folder
            )
            is False
        )
        assert (
            is_object_exists(
                s3_client=s3_client, bucket=bucket, key=self.prefix_hard_folder
            )
            is True
        )
        assert (
            is_object_exists(
                s3_client=s3_client, bucket=bucket, key=self.key_hard_folder_file
            )
            is True
        )
        assert (
            is_object_exists(
                s3_client=s3_client, bucket=bucket, key=self.key_empty_hard_folder
            )
            is False
        )
        assert (
            is_object_exists(
                s3_client=s3_client, bucket=bucket, key=self.prefix_empty_hard_folder
            )
            is True
        )

    def test(self):
        self._test_before_and_after_put_object()
        self._test_is_object_exists()


# NOTE: this module should ONLY be tested with MOCK
# DO NOT USE REAL S3 BUCKET
class TestUseMock(BetterHeadObject):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.better_client.head_object", preview=False)
