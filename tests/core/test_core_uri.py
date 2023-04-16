# -*- coding: utf-8 -*-

import pytest
from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test


class TestUriAPIMixin:
    def test_uri_properties(self):
        # s3 object
        p = S3Path("bucket", "folder", "file.txt")
        assert p.bucket == "bucket"
        assert p.key == "folder/file.txt"
        assert p.uri == "s3://bucket/folder/file.txt"
        assert p.arn == "arn:aws:s3:::bucket/folder/file.txt"
        assert (
            p.console_url
            == "https://console.aws.amazon.com/s3/object/bucket?prefix=folder/file.txt"
        )
        assert (
            p.us_gov_cloud_console_url
            == "https://console.amazonaws-us-gov.com/s3/object/bucket?prefix=folder/file.txt"
        )
        assert (
            p.s3_select_console_url
            == "https://console.aws.amazon.com/s3/buckets/bucket/object/select?prefix=folder/file.txt"
        )
        assert (
            p.s3_select_us_gov_cloud_console_url
            == "https://console.amazonaws-us-gov.com/s3/buckets/bucket/object/select?prefix=folder/file.txt"
        )

        # s3 directory
        p = S3Path("bucket", "folder/")
        assert p.bucket == "bucket"
        assert p.key == "folder/"
        assert p.uri == "s3://bucket/folder/"
        assert p.arn == "arn:aws:s3:::bucket/folder/"
        assert (
            p.console_url
            == "https://console.aws.amazon.com/s3/buckets/bucket?prefix=folder/"
        )

        with pytest.raises(TypeError):
            assert p.s3_select_console_url
        with pytest.raises(TypeError):
            assert p.s3_select_us_gov_cloud_console_url

        # s3 bucket
        p = S3Path("bucket")
        assert p.bucket == "bucket"
        assert p.key == ""
        assert p.uri == "s3://bucket/"
        assert p.arn == "arn:aws:s3:::bucket"
        assert (
            p.console_url
            == "https://console.aws.amazon.com/s3/buckets/bucket?tab=objects"
        )

        with pytest.raises(TypeError):
            assert p.s3_select_console_url
        with pytest.raises(TypeError):
            assert p.s3_select_us_gov_cloud_console_url

        # void path
        p = S3Path()
        assert p.bucket is None
        assert p.key == ""
        assert p.uri is None
        assert p.arn is None
        assert p.console_url is None
        assert p.us_gov_cloud_console_url is None

        with pytest.raises(TypeError):
            assert p.s3_select_console_url
        with pytest.raises(TypeError):
            assert p.s3_select_us_gov_cloud_console_url

    def test_from_s3_uri(self):
        p = S3Path.from_s3_uri("s3://bucket/")
        assert p._bucket == "bucket"
        assert p._parts == []
        assert p._is_dir is True

        p = S3Path.from_s3_uri("s3://bucket/folder/")
        assert p._bucket == "bucket"
        assert p._parts == [
            "folder",
        ]
        assert p._is_dir is True

        p = S3Path.from_s3_uri("s3://bucket/folder/file.txt")
        assert p._bucket == "bucket"
        assert p._parts == ["folder", "file.txt"]
        assert p._is_dir is False

    def test_from_s3_arn(self):
        p = S3Path.from_s3_arn("arn:aws:s3:::bucket")
        assert p._bucket == "bucket"
        assert p._parts == []
        assert p._is_dir is True

        p = S3Path.from_s3_arn("arn:aws:s3:::bucket/")
        assert p._bucket == "bucket"
        assert p._parts == []
        assert p._is_dir is True

        p = S3Path.from_s3_arn("arn:aws:s3:::bucket/folder/")
        assert p._bucket == "bucket"
        assert p._parts == [
            "folder",
        ]
        assert p._is_dir is True

        p = S3Path.from_s3_arn("arn:aws:s3:::bucket/folder/file.txt")
        assert p._bucket == "bucket"
        assert p._parts == ["folder", "file.txt"]
        assert p._is_dir is False


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.uri", preview=False)
