# -*- coding: utf-8 -*-

from s3pathlib.better_client.head_object import is_object_exists
from s3pathlib.better_client.list_objects import (
    calculate_total_size,
    count_objects,
)
from s3pathlib.better_client.delete_object import (
    delete_object,
    delete_dir,
    delete_object_versions,
)
from s3pathlib.utils import smart_join_s3_key
from s3pathlib.tests import run_cov_test

from dummy_data import DummyData


class BetterDeleteObject(DummyData):
    module = "better_client.delete_object"

    @classmethod
    def custom_setup_class(cls):
        cls.setup_list_objects_folder()

    def _test_delete_object(self):
        s3_client = self.s3_client
        bucket = self.bucket
        prefix = smart_join_s3_key([self.prefix, "delete_object"], is_dir=False)
        key = smart_join_s3_key([prefix, "file.txt"], is_dir=False)

        res = delete_object(
            s3_client=s3_client,
            bucket=bucket,
            key=key,
        )
        assert len(res) > 0

        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body="hello",
        )
        res = delete_object(
            s3_client=s3_client,
            bucket=bucket,
            key=key,
        )
        assert len(res) > 0

    def _test_delete_object_versions(self):
        # prepare data
        s3_client = self.s3_client
        bucket = self.bucket_with_versioning
        prefix = smart_join_s3_key([self.prefix, "delete_object_versions"], is_dir=True)

        def put(suffix: str, content: str):
            s3_client.put_object(Bucket=bucket, Key=f"{prefix}{suffix}", Body=content)

        def delete(suffix: str):
            s3_client.delete_object(Bucket=bucket, Key=f"{prefix}{suffix}")

        put("README.txt", "this is read me v1")
        delete("README.txt")
        put("README.txt", "this is read me v2")

        put("hard_folder/", "")

        put("hard_folder/hard_copy.txt", "hard copy v1")
        delete("hard_folder/hard_copy.txt")
        put("hard_folder/hard_copy.txt", "hard copy v2")

        put("soft_folder/soft_copy.txt", "soft copy v1")
        delete("soft_folder/soft_copy.txt")
        put("soft_folder/soft_copy.txt", "soft copy v2")
        put("soft_folder/soft_copy.txt", "soft copy v3")

        put("soft_folder/sub_folder/password.txt", "pwd v1")

        count = delete_object_versions(
            s3_client=s3_client,
            bucket=bucket,
            prefix=prefix,
        )
        assert count == 12

    def _test_with_list_objects_folder(self):
        s3_client = self.s3_client
        bucket = self.bucket

        # before state
        count = count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_test_list_objects,
        )
        assert count == 11

        count, total_size = calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_test_list_objects,
        )
        assert total_size > 0

        # call api
        delete_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_test_list_objects,
        )

        # after state
        count = count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_test_list_objects,
        )
        assert count == 0

        count, total_size = calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_test_list_objects,
        )
        assert total_size == 0

    def _test_with_dummy_data(self):
        self.setup_dummy_data()

        s3_client = self.s3_client
        bucket = self.bucket

        def _is_object_exists(key: str):
            return is_object_exists(
                s3_client=s3_client,
                bucket=bucket,
                key=self.prefix_hard_folder,
            )

        # delete a hard folder, the hard folder object should be deleted too
        assert _is_object_exists(key=self.prefix_hard_folder) is True
        assert (
            delete_dir(
                s3_client=s3_client,
                bucket=bucket,
                prefix=self.prefix_hard_folder,
            )
            == 2
        )
        assert _is_object_exists(key=self.prefix_hard_folder) is False

        # delete an empty hard folder, the hard folder object should be deleted too
        assert _is_object_exists(key=self.prefix_empty_hard_folder) is False
        assert (
            delete_dir(
                s3_client=s3_client,
                bucket=bucket,
                prefix=self.prefix_empty_hard_folder,
            )
            == 1
        )
        assert _is_object_exists(key=self.prefix_empty_hard_folder) is False

        # delete a soft folder
        assert _is_object_exists(key=self.prefix_soft_folder) is False
        assert (
            delete_dir(
                s3_client=s3_client,
                bucket=bucket,
                prefix=self.prefix_soft_folder,
            )
            == 1
        )
        assert (
            count_objects(
                s3_client=s3_client,
                bucket=bucket,
                prefix=self.prefix_soft_folder,
            )
            == 0
        )

    def test(self):
        self._test_delete_object()
        self._test_delete_object_versions()
        self._test_with_list_objects_folder()
        self._test_with_dummy_data()


class Test(BetterDeleteObject):
    use_mock = False


class TestUseMock(BetterDeleteObject):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(
        __file__,
        module="s3pathlib.better_client.delete_object",
        preview=False,
    )
