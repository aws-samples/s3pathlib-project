# -*- coding: utf-8 -*-

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
        cls.setup_dummy_data()

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
        s3_client = self.s3_client
        bucket = self.bucket

        # first delete, only delete files
        delete_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_dummy_data,
            include_folder=False,
        )
        count, total_size = calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_dummy_data,
            include_folder=True,
        )
        assert count == 2
        assert total_size == 0

        # second delete, also delete hard folders
        delete_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_dummy_data,
        )
        count, total_size = calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_dummy_data,
            include_folder=True,
        )
        assert count == 0
        assert total_size == 0

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
