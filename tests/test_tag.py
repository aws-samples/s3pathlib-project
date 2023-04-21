# -*- coding: utf-8 -*-

from s3pathlib.tag import (
    parse_tags,
    encode_tag_set,
    encode_url_query,
    encode_for_put_object,
    encode_for_put_bucket_tagging,
    encode_for_put_object_tagging,
)


def test_parse_tags():
    assert parse_tags([{"Key": "Name", "Value": "Alice"}]) == {"Name": "Alice"}


def test_encode_tag_set():
    assert encode_tag_set(dict(k1="v1", k2="v2")) == [
        {"Key": "k1", "Value": "v1"},
        {"Key": "k2", "Value": "v2"},
    ]


def test_encode_url_query():
    assert encode_url_query(dict(k="v", message="a=b")) == "k=v&message=a%3Db"


def test_encode_for_xyz():
    tags = {"k": "v"}
    assert encode_for_put_object(tags) == "k=v"
    assert encode_for_put_bucket_tagging(tags) == [{"Key": "k", "Value": "v"}]
    assert encode_for_put_object_tagging(tags) == [{"Key": "k", "Value": "v"}]


if __name__ == "__main__":
    from s3pathlib.tests import run_cov_test

    run_cov_test(__file__, module="s3pathlib.tag", preview=False)
