# -*- coding: utf-8 -*-

import pytest
from s3pathlib.core import S3Path
from s3pathlib import exc
from s3pathlib.better_client.list_objects import (
    ObjectTypeDefIterproxy,
    CommonPrefixTypeDefIterproxy,
    ListObjectsV2OutputTypeDefIterproxy,
    paginate_list_objects_v2,
    filter_object_only,
    calculate_total_size,
    count_objects,
)
from s3pathlib.utils import smart_join_s3_key
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import prefix, BaseTest


class BetterListObjects(BaseTest):
    module = "better_client.list_objects"

    key_hello: str
    key_soft_folder_file: str
    prefix_hard_folder: str
    key_hard_folder_file: str
    prefix_empty_hard_folder: str

    @classmethod
    def custom_setup_class(cls):
        """
        Following test S3 objects are created. Key endswith "/" are special
        empty S3 object representing a folder.

        - ``/{prefix}/hello.txt``
        - ``/{prefix}/soft_folder/file.txt``
        - ``/{prefix}/hard_folder/``
        - ``/{prefix}/hard_folder/file.txt``
        - ``/{prefix}/empty_hard_folder/``
        """
        s3_client = cls.bsm.s3_client
        bucket = cls.get_bucket()

        cls.key_hello = smart_join_s3_key(
            parts=[prefix, "hello.txt"],
            is_dir=False,
        )
        cls.key_soft_folder_file = smart_join_s3_key(
            parts=[prefix, "soft_folder/file.txt"],
            is_dir=False,
        )
        cls.prefix_hard_folder = smart_join_s3_key(
            parts=[prefix, "hard_folder"],
            is_dir=True,
        )
        cls.key_hard_folder_file = smart_join_s3_key(
            parts=[prefix, "hard_folder/file.txt"],
            is_dir=False,
        )
        cls.prefix_empty_hard_folder = smart_join_s3_key(
            parts=[prefix, "empty_hard_folder"],
            is_dir=True,
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=cls.key_hello,
            Body="Hello World!",
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=cls.key_soft_folder_file,
            Body="Hello World!",
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=cls.prefix_hard_folder,
            Body="",
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=cls.key_hard_folder_file,
            Body="Hello World!",
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=cls.prefix_empty_hard_folder,
            Body="",
        )

    def _test(self):
        # at begin, no object exists
        s3path = S3Path(self.s3dir_root, "file.txt")

        assert (
            is_object_exists(
                self.s3_client,
                bucket=s3path.bucket,
                key=s3path.key,
            )
            is False
        )

        with pytest.raises(exc.S3ObjectNotExist):
            head_object(
                self.s3_client,
                bucket=s3path.bucket,
                key=s3path.key,
            )

        assert (
            head_object(
                self.s3_client,
                bucket=s3path.bucket,
                key=s3path.key,
                ignore_not_found=True,
            )
            is None
        )

        # put the object
        self.s3_client.put_object(Bucket=s3path.bucket, Key=s3path.key, Body="hello")

        assert (
            is_object_exists(
                self.s3_client,
                bucket=s3path.bucket,
                key=s3path.key,
            )
            is True
        )

        assert head_object(self.s3_client, bucket=s3path.bucket, key=s3path.key,)[
            "ContentLength"
        ] == len("hello")

    def test(self):
        self._test()


# class Test(BetterHeadObject):
#     use_mock = False


class TestUseMock(BetterHeadObject):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.better_client.head_object", preview=False)
