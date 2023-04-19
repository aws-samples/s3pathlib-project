# -*- coding: utf-8 -*-

import pytest
from s3pathlib.better_client.list_object_versions import (
    paginate_list_object_versions,
)
from s3pathlib.tests import run_cov_test

from dummy_data import DummyData


class BetterListObjectVersions(DummyData):
    module = "better_client.list_object_versions"

    def _test_paginate_list_object_versions_for_object(self):
        s3_client = self.s3_client
        bucket = self.bucket_with_versioning
        key = f"{self.get_prefix()}/for_object/file.txt"
        for i in [1, 2]:
            s3_client.put_object(Bucket=bucket, Key=key, Body=f"v{i}")
        s3_client.delete_object(Bucket=bucket, Key=key)
        for i in [3, 4]:
            s3_client.put_object(Bucket=bucket, Key=key, Body=f"v{i}")
        s3_client.delete_object(Bucket=bucket, Key=key)
        for i in [5]:
            s3_client.put_object(Bucket=bucket, Key=key, Body=f"v{i}")

        (versions, delete_markers, common_prefixes,) = paginate_list_object_versions(
            s3_client=s3_client,
            bucket=bucket,
            prefix=key,
        ).versions_and_delete_markers_and_common_prefixes()
        assert len(versions) == 5
        assert len(delete_markers) == 2
        assert len(common_prefixes) == 0

    def _test_paginate_list_object_versions_for_folder(self):
        s3_client = self.s3_client
        bucket = self.bucket_with_versioning
        prefix = f"{self.get_prefix()}/for_folder/"

        def put(suffix: str, content: str):
            s3_client.put_object(Bucket=bucket, Key=f"{prefix}{suffix}", Body=content)

        def delete(suffix: str):
            s3_client.delete_object(Bucket=bucket, Key=f"{prefix}{suffix}")

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

        (versions, delete_markers, common_prefixes,) = paginate_list_object_versions(
            s3_client=s3_client,
            bucket=bucket,
            prefix=prefix,
        ).versions_and_delete_markers_and_common_prefixes()

        assert len(versions) == 9
        assert len(delete_markers) == 3

        (versions, delete_markers, common_prefixes,) = paginate_list_object_versions(
            s3_client=s3_client, bucket=bucket, prefix=prefix, delimiter="/"
        ).versions_and_delete_markers_and_common_prefixes()

        assert len(versions) == 3
        assert len(delete_markers) == 1
        assert len(common_prefixes) == 2

        # self.rprint(versions)
        # self.rprint(delete_markers)
        # self.rprint(common_prefixes)

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
