# -*- coding: utf-8 -*-

import typing as T
import pytest
from s3pathlib.better_client.list_object_versions import (
    paginate_list_object_versions,
)
from s3pathlib.utils import smart_join_s3_key
from s3pathlib.tests import run_cov_test

from dummy_data import DummyData


class BetterListObjectVersions(DummyData):
    module = "better_client.list_object_versions"

    def _test_paginate_list_object_versions_for_object(self):
        # prepare data
        s3_client = self.s3_client
        bucket = self.bucket_with_versioning
        prefix = smart_join_s3_key(
            [self.get_prefix(), "list_object_versions"], is_dir=True
        )
        key = smart_join_s3_key([prefix, "for_object/file.txt"], is_dir=False)

        # create 5 version and 2 delete markers
        for i in [1, 2]:
            s3_client.put_object(Bucket=bucket, Key=key, Body=f"v{i}")
        s3_client.delete_object(Bucket=bucket, Key=key)
        for i in [3, 4]:
            s3_client.put_object(Bucket=bucket, Key=key, Body=f"v{i}")
        s3_client.delete_object(Bucket=bucket, Key=key)
        for i in [5]:
            s3_client.put_object(Bucket=bucket, Key=key, Body=f"v{i}")

        # check the number of versions, delete markers and common prefixes
        proxy = paginate_list_object_versions(
            s3_client=s3_client,
            bucket=bucket,
            prefix=key,
        )
        (
            versions,
            delete_markers,
            common_prefixes,
        ) = proxy.versions_and_delete_markers_and_common_prefixes()
        assert len(versions) == 5
        assert len(delete_markers) == 2
        assert len(common_prefixes) == 0

    def _test_paginate_list_object_versions_for_folder(self):
        # prepare data
        s3_client = self.s3_client
        bucket = self.bucket_with_versioning
        prefix = smart_join_s3_key(
            [self.get_prefix(), "list_object_versions_for_folder"], is_dir=True
        )

        def put(suffix: str, content: str):
            s3_client.put_object(Bucket=bucket, Key=f"{prefix}{suffix}", Body=content)

        def delete(suffix: str):
            s3_client.delete_object(Bucket=bucket, Key=f"{prefix}{suffix}")

        def list_object_versions(delimiter: T.Optional[str] = None):
            kwargs = dict(
                s3_client=s3_client,
                bucket=bucket,
                prefix=prefix,
            )
            if delimiter:
                kwargs["delimiter"] = delimiter
            return paginate_list_object_versions(**kwargs)

        put("README.txt", "this is read me v1")
        delete("README.txt")
        put("README.txt", "this is read me v2")

        put("hard_folder", "this is a hard folder")

        put("hard_folder/hard_copy.txt", "hard copy v1")
        delete("hard_folder/hard_copy.txt")
        put("hard_folder/hard_copy.txt", "hard copy v2")

        put("soft_folder/soft_copy.txt", "soft copy v1")
        delete("soft_folder/soft_copy.txt")
        put("soft_folder/soft_copy.txt", "soft copy v2")
        put("soft_folder/soft_copy.txt", "soft copy v3")

        put("soft_folder/sub_folder/password.txt", "pwd v1")

        # check the number of versions, delete markers and common prefixes
        (
            versions,
            delete_markers,
            common_prefixes,
        ) = list_object_versions().versions_and_delete_markers_and_common_prefixes()
        assert len(versions) == 9
        assert len(delete_markers) == 3
        assert len(common_prefixes) == 0

        # check the number of versions, delete markers and common prefixes
        assert len(list_object_versions().versions().all()) == 9
        assert len(list_object_versions().delete_markers().all()) == 3
        assert len(list_object_versions().common_prefixes().all()) == 0

        # check the number of versions, delete markers and common prefixes
        (
            versions,
            delete_markers,
            common_prefixes,
        ) = list_object_versions("/").versions_and_delete_markers_and_common_prefixes()
        assert len(versions) == 3
        assert len(delete_markers) == 1
        assert len(common_prefixes) == 2

    def test(self):
        self._test_paginate_list_object_versions_for_object()
        self._test_paginate_list_object_versions_for_folder()


# class Test(BetterListObjectVersions):
#     use_mock = False


class TestUseMock(BetterListObjectVersions):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(
        __file__, module="s3pathlib.better_client.list_object_versions", preview=False
    )
