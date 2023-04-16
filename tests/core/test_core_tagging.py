# -*- coding: utf-8 -*-

from s3pathlib import client as better_client
from s3pathlib.core import S3Path
from s3pathlib.tests import bsm, s3_client, bucket, prefix, run_cov_test

s3dir_root = S3Path(bucket, prefix, "core", "tagging")
s3path_file = s3dir_root / "hello.txt"


class TestTaggingAPIMixin:
    def test_attributes(self):
        better_client.put_object(
            s3_client,
            bucket=s3path_file.bucket,
            key=s3path_file.key,
            body=b"Hello World!",
        )

        # tags is empty
        assert s3path_file.get_tags() == {}

        # use put_object API
        better_client.put_object(
            s3_client,
            bucket=s3path_file.bucket,
            key=s3path_file.key,
            body=b"Hello World!",
            tags={"k1": "v1"},
        )
        assert s3path_file.get_tags() == {"k1": "v1"}

        # put object tagging is a full replacement
        s3path_file.put_tags(tags={"k2": "v2", "k3": "v3"})
        assert s3path_file.get_tags() == {"k2": "v2", "k3": "v3"}

        # update object is a "true update" operation
        tags = s3path_file.update_tags(tags={"k1": "v1", "k2": "v22"})
        assert tags == {"k1": "v1", "k2": "v22", "k3": "v3"}

        existing_tags = s3path_file.get_tags()
        assert existing_tags == {"k1": "v1", "k2": "v22", "k3": "v3"}


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.tagging", preview=False)
