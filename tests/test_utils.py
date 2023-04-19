# -*- coding: utf-8 -*-

import pytest
import os
from s3pathlib import utils
from s3pathlib.tests import run_cov_test

dir_here = os.path.dirname(os.path.abspath(__file__))
dir_project_root = os.path.dirname(dir_here)


def test_split_s3_uri():
    s3_uri = "s3://my-bucket/my-prefix/my-file.zip"
    bucket, key = utils.split_s3_uri(s3_uri)
    assert bucket == "my-bucket"
    assert key == "my-prefix/my-file.zip"


def test_join_s3_uri():
    bucket = "my-bucket"
    key = "my-prefix/my-file.zip"
    s3_uri = utils.join_s3_uri(bucket, key)
    assert s3_uri == "s3://my-bucket/my-prefix/my-file.zip"


def test_split_parts():
    assert utils.split_parts("a/b/c") == ["a", "b", "c"]
    assert utils.split_parts("//a//b//c//") == ["a", "b", "c"]
    assert utils.split_parts("") == []
    assert utils.split_parts("////") == []


def test_s3_key_smart_join():
    assert utils.smart_join_s3_key(
        parts=["/a/", "b/", "/c"],
        is_dir=True,
    ) == "a/b/c/"

    assert utils.smart_join_s3_key(
        parts=["/a/", "b/", "/c"],
        is_dir=False,
    ) == "a/b/c"

    assert utils.smart_join_s3_key(
        parts=["//a//b//c//"],
        is_dir=True,
    ) == "a/b/c/"

    assert utils.smart_join_s3_key(
        parts=["//a//b//c//"],
        is_dir=False,
    ) == "a/b/c"


def test_make_s3_console_url():
    # object
    url = utils.make_s3_console_url("my-bucket", "my-file.zip")
    assert "object" in url

    # folder
    url = utils.make_s3_console_url("my-bucket", "my-folder/")
    assert "bucket" in url

    # uri
    url = utils.make_s3_console_url(s3_uri="s3://my-bucket/my-folder/data.json")
    assert url == "https://console.aws.amazon.com/s3/object/my-bucket?prefix=my-folder/data.json"

    # s3 bucket root
    url = utils.make_s3_console_url(s3_uri="s3://my-bucket/")
    assert url == "https://console.aws.amazon.com/s3/buckets/my-bucket?tab=objects"

    # version id
    url = utils.make_s3_console_url(s3_uri="s3://my-bucket/my-folder/my-file.zip", version_id="v123")
    assert url == "https://console.aws.amazon.com/s3/object/my-bucket?prefix=my-folder/my-file.zip&versionId=v123"

    # us gov cloud
    url = utils.make_s3_console_url(
        s3_uri="s3://my-bucket/my-folder/data.json", is_us_gov_cloud=True
    )
    assert url == "https://console.amazonaws-us-gov.com/s3/object/my-bucket?prefix=my-folder/data.json"

    with pytest.raises(ValueError):
        utils.make_s3_console_url(bucket="")

    with pytest.raises(ValueError):
        utils.make_s3_console_url(prefix="", s3_uri="")


def test_ensure_s3_object():
    utils.ensure_s3_object("path/to/key")
    with pytest.raises(Exception):
        utils.ensure_s3_object("path/to/dir/")


def test_ensure_s3_dir():
    utils.ensure_s3_dir("path/to/dir/")
    with pytest.raises(Exception):
        utils.ensure_s3_dir("path/to/key")


def test_repr_data_size():
    assert utils.repr_data_size(3600000) == "3.43 MB"


def test_parse_data_size():
    assert utils.parse_data_size("3.43 MB") == 3596615
    assert utils.parse_data_size("2_512.4 MB") == 2634442342
    assert utils.parse_data_size("2,512.4 MB") == 2634442342


if __name__ == "__main__":
    run_cov_test(__file__, "s3pathlib.utils", preview=False)
