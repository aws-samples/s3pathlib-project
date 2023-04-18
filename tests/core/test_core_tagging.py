# -*- coding: utf-8 -*-

from s3pathlib import client as better_client
from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


class TaggingAPIMixin(BaseTest):
    module = "core.tagging"

    def _test_get_put_update(self):
        s3path_file = S3Path(self.s3dir_root, "hello.txt")

        better_client.put_object(
            self.s3_client,
            bucket=s3path_file.bucket,
            key=s3path_file.key,
            body=b"Hello World!",
        )

        # tags is empty
        assert s3path_file.get_tags()[1] == {}

        # use put_object API
        better_client.put_object(
            self.s3_client,
            bucket=s3path_file.bucket,
            key=s3path_file.key,
            body=b"Hello World!",
            tags={"k1": "v1"},
        )
        assert s3path_file.get_tags()[1] == {"k1": "v1"}

        # put object tagging is a full replacement
        _, tags = s3path_file.put_tags(tags={"k2": "v2", "k3": "v3"})
        assert tags == {"k2": "v2", "k3": "v3"}
        assert s3path_file.get_tags()[1] == {"k2": "v2", "k3": "v3"}

        # update object is a "true update" operation
        _, tags = s3path_file.update_tags(tags={"k1": "v1", "k2": "v22"})
        assert tags == {"k1": "v1", "k2": "v22", "k3": "v3"}

        assert s3path_file.get_tags()[1] == {"k1": "v1", "k2": "v22", "k3": "v3"}

    def _test_get_put_update_with_versioning(self):
        s3path_file = S3Path(self.s3dir_root_with_versioning, "hello.txt")

        better_client.put_object(
            self.s3_client,
            bucket=s3path_file.bucket,
            key=s3path_file.key,
            body=b"Hello World!",
        )

        # tags is empty
        v1, tags = s3path_file.get_tags()
        assert tags == {}

        # use put_object API
        better_client.put_object(
            self.s3_client,
            bucket=s3path_file.bucket,
            key=s3path_file.key,
            body=b"Hello World!",
            tags={"k1": "v1"},
        )

        # version is the new one, tags is the new one
        v2, tags = s3path_file.get_tags()
        assert v1 != v2
        assert tags == {"k1": "v1"}

        # get specific version
        v, tags = s3path_file.get_tags(version_id=v1)
        assert v == v1
        assert tags == {}

        # put tags to the latest version
        # put object tagging is a full replacement
        _, tags = s3path_file.put_tags(tags={"k2": "v2", "k3": "v3"})
        assert tags == {"k2": "v2", "k3": "v3"}

        # get latest version
        v, tags = s3path_file.get_tags()
        assert v == v2
        assert tags == {"k2": "v2", "k3": "v3"}

        # get older version
        v, tags = s3path_file.get_tags(version_id=v1)
        assert v == v1
        assert tags == {}

        # put tags to the older version
        # put object tagging is a full replacement
        _, tags = s3path_file.put_tags(tags={"k1": "v1", "k2": "v2"}, version_id=v1)
        assert tags == {"k1": "v1", "k2": "v2"}

        # update object is a "true update" operation
        _, tags = s3path_file.update_tags(tags={"k1": "v1", "k2": "v22"})
        assert tags == {"k1": "v1", "k2": "v22", "k3": "v3"}
        v, tags = s3path_file.get_tags()
        assert v == v2
        assert tags == {"k1": "v1", "k2": "v22", "k3": "v3"}

        _, tags = s3path_file.update_tags(tags={"k2": "v2", "k3": "v3"}, version_id=v1)
        assert tags == {"k1": "v1", "k2": "v2", "k3": "v3"}
        v, tags = s3path_file.get_tags(version_id=v1)
        assert v == v1
        assert tags == {"k1": "v1", "k2": "v2", "k3": "v3"}

    def test(self):
        self._test_get_put_update()
        self._test_get_put_update_with_versioning()


class Test(TaggingAPIMixin):
    use_mock = False


class TestUseMock(TaggingAPIMixin):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.tagging", preview=False)
