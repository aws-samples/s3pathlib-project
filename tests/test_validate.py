# -*- coding: utf-8 -*-

import pytest
from s3pathlib import validate as val


def test_validate_s3_bucket():
    test_cases = [
        # bad case
        ("", False),
        ("a", False),
        ("a" * 100, False),
        ("bucket@example.com", False),
        ("my_bucket", False),
        ("-my-bucket", False),
        ("my-bucket-", False),
        ("my-bucket-", False),
        ("192.168.0.1", False),
        ("xn--my-bucket", False),
        ("my-bucket-s3alias", False),
        # good case
        ("my-bucket", True),
    ]
    for bucket, flag in test_cases:
        if flag:
            val.validate_s3_bucket(bucket)
        else:
            with pytest.raises(Exception):
                val.validate_s3_bucket(bucket)


def test_validate_s3_key():
    test_cases = [
        # bad cases
        ("a" * 2000, False),
        ("%20", False),
        # good cases
        (
            "",
            True,
        ),
        (
            "abcd",
            True,
        ),
    ]
    for key, flag in test_cases:
        if flag:
            val.validate_s3_key(key)
        else:
            with pytest.raises(Exception):
                val.validate_s3_key(key)


def test_validate_s3_uri():
    test_cases = [
        # bad cases
        ("bucket/key", False),
        ("s3://bucket", False),
        ("s3://ab/%20", False),
        # good cases
        ("s3://bucket/key", True),
        ("s3://bucket/folder/file.txt", True),
        ("s3://bucket/", True),
        ("s3://bucket/folder/", True),
    ]
    for uri, flag in test_cases:
        if flag:
            val.validate_s3_uri(uri)
        else:
            with pytest.raises(Exception):
                val.validate_s3_uri(uri)


def test_validate_s3_arn():
    test_cases = [
        # bad cases
        ("bucket/key", False),
        ("s3://bucket", False),
        ("arn:aws:s3:::b", False),
        ("arn:aws:s3:::{}".format("b" * 100), False),
        ("arn:aws:s3:::bucket/%20", False),
        # good cases
        ("arn:aws:s3:::bucket/key", True),
        ("arn:aws:s3:::bucket/folder/file.txt", True),
        ("arn:aws:s3:::bucket/", True),
        ("arn:aws:s3:::bucket/folder", True),
        ("arn:aws:s3:::bucket", True),
    ]
    for arn, flag in test_cases:
        if flag:
            val.validate_s3_arn(arn)
        else:
            with pytest.raises(Exception):
                val.validate_s3_arn(arn)


if __name__ == "__main__":
    from s3pathlib.tests import run_cov_test

    run_cov_test(__file__, module="s3pathlib.validate", preview=False)
