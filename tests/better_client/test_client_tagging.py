# -*- coding: utf-8 -*-

import pytest

from s3pathlib.better_client.tagging import (
    update_bucket_tagging,
    update_object_tagging,
)
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


class BetterUpload(BaseTest):
    module = "better_client.tagging"

    def _test_bucket_tagging(self):
        s3_client = self.s3_client
        bucket = self.bucket

        tags = update_bucket_tagging(
            s3_client=s3_client,
            bucket=bucket,
            tags={"k1": "v1", "k2": "v2"},
        )
        assert tags == {"k1": "v1", "k2": "v2"}
        res = s3_client.get_bucket_tagging(
            Bucket=bucket,
        )
        assert res["TagSet"] == [
            {"Key": "k1", "Value": "v1"},
            {"Key": "k2", "Value": "v2"},
        ]

        tags = update_bucket_tagging(
            s3_client=s3_client,
            bucket=bucket,
            tags={"k2": "v22", "k3": "v3"},
        )
        assert tags == {"k1": "v1", "k2": "v22", "k3": "v3"}
        res = s3_client.get_bucket_tagging(
            Bucket=bucket,
        )
        assert res["TagSet"] == [
            {"Key": "k1", "Value": "v1"},
            {"Key": "k2", "Value": "v22"},
            {"Key": "k3", "Value": "v3"},
        ]

    def _test_object_tagging(self):
        s3_client = self.s3_client
        bucket = self.bucket
        key = f"{self.get_prefix()}/test_object_tagging"

        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body="",
        )
        tags = update_object_tagging(
            s3_client=s3_client,
            bucket=bucket,
            key=key,
            tags={"k1": "v1", "k2": "v2"},
        )[1]
        assert tags == {"k1": "v1", "k2": "v2"}
        res = s3_client.get_object_tagging(Bucket=bucket, Key=key)
        assert res["TagSet"] == [
            {"Key": "k1", "Value": "v1"},
            {"Key": "k2", "Value": "v2"},
        ]

        tags = update_object_tagging(
            s3_client=s3_client,
            bucket=bucket,
            key=key,
            tags={"k2": "v22", "k3": "v3"},
        )[1]
        assert tags == {"k1": "v1", "k2": "v22", "k3": "v3"}
        res = s3_client.get_object_tagging(
            Bucket=bucket,
            Key=key,
        )
        assert res["TagSet"] == [
            {"Key": "k1", "Value": "v1"},
            {"Key": "k2", "Value": "v22"},
            {"Key": "k3", "Value": "v3"},
        ]

    def test(self):
        self._test_bucket_tagging()
        self._test_object_tagging()


# NOTE: this module should ONLY be tested with MOCK
# DO NOT USE REAL S3 BUCKET
class TestUseMock(BetterUpload):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.better_client.tagging", preview=False)
