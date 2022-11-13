# -*- coding: utf-8 -*-

from s3pathlib.tag import parse_tags, encode_tag_set, encode_url_query


def test_parse_tags():
    assert parse_tags([{"Key": "Name", "Value": "Alice"}]) == {"Name": "Alice"}


def test_encode_tag_set():
    assert encode_tag_set(dict(k1="v1", k2="v2")) == [
        {"Key": "k1", "Value": "v1"},
        {"Key": "k2", "Value": "v2"},
    ]


def test_encode_url_query():
    assert encode_url_query(dict(k="v", message="a=b")) == "k=v&message=a%3Db"


if __name__ == "__main__":
    from s3pathlib.tests import run_cov_test

    run_cov_test(__file__, module="s3pathlib.tag", open_browser=False)
    # run_cov_test(__file__, module="s3pathlib.tag", open_browser=True)
