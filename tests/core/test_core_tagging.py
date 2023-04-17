# -*- coding: utf-8 -*-

from s3pathlib import client as better_client
from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


class TaggingAPIMixin(BaseTest):
    module = "core.tagging"

    def test_attributes(self):
        s3path_file = S3Path(self.s3dir_root, "hello.txt")

        better_client.put_object(
            self.s3_client,
            bucket=s3path_file.bucket,
            key=s3path_file.key,
            body=b"Hello World!",
        )

        # tags is empty
        assert s3path_file.get_tags() == {}

        # use put_object API
        better_client.put_object(
            self.s3_client,
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


class Test(TaggingAPIMixin):
    use_mock = False


class TestUseMock(TaggingAPIMixin):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.tagging", preview=False)
