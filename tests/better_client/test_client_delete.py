# -*- coding: utf-8 -*-

from s3pathlib.better_client.head_object import is_object_exists
from s3pathlib.better_client.list_objects import (
    calculate_total_size,
    count_objects,
)
from s3pathlib.better_client.delete_object import (
    delete_object,
    delete_dir,
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
        key = smart_join_s3_key([self.prefix, "file.txt"], is_dir=False)

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

        # delete a hard folder, the hard folder object should be deleted too
        assert (
            is_object_exists(
                s3_client=s3_client,
                bucket=bucket,
                key=self.prefix_hard_folder,
            )
            is True
        )
        assert delete_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_hard_folder,
        ) == 2
        assert (
            is_object_exists(
                s3_client=s3_client,
                bucket=bucket,
                key=self.prefix_hard_folder,
            )
            is False
        )

        # delete an empty hard folder, the hard folder object should be deleted too
        assert (
            is_object_exists(
                s3_client=s3_client,
                bucket=bucket,
                key=self.prefix_empty_hard_folder,
            )
            is True
        )
        assert delete_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_empty_hard_folder,
        ) == 1
        assert (
            is_object_exists(
                s3_client=s3_client,
                bucket=bucket,
                key=self.prefix_empty_hard_folder,
            )
            is False
        )

        # delete a soft folder
        assert (
            is_object_exists(
                s3_client=s3_client,
                bucket=bucket,
                key=self.prefix_soft_folder,
            )
            is False
        )
        assert delete_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_soft_folder,
        ) == 1
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
