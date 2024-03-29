# -*- coding: utf-8 -*-

import pytest
from s3pathlib.better_client.list_objects import (
    paginate_list_objects_v2,
    calculate_total_size,
    count_objects,
)
from s3pathlib.tests import run_cov_test

from dummy_data import DummyData


class BetterListObjects(DummyData):
    module = "better_client.list_objects"
    prefix_test_list_objects: str

    @classmethod
    def custom_setup_class(cls):
        cls.setup_list_objects_folder()
        cls.setup_dummy_data()

    def _test_paginate_list_objects_v2_argument_error(self):
        # invalid batch_size
        for batch_size in [-1, 9999]:
            with pytest.raises(ValueError):
                paginate_list_objects_v2(
                    s3_client=None,
                    bucket=None,
                    prefix=None,
                    batch_size=batch_size,
                )

    def _test_paginate_list_objects_v2_contents(self):
        # batch_size < limit
        result = paginate_list_objects_v2(
            s3_client=self.s3_client,
            bucket=self.bucket,
            prefix=self.prefix_test_list_objects,
            batch_size=3,
            limit=5,
        )
        assert len(result.contents().all()) == 5

        # batch_size > limit
        result = paginate_list_objects_v2(
            s3_client=self.s3_client,
            bucket=self.bucket,
            prefix=self.prefix_test_list_objects,
            batch_size=10,
            limit=3,
        )
        assert len(result.contents().all()) == 3

        # limit >> total number objects,
        result = paginate_list_objects_v2(
            s3_client=self.s3_client,
            bucket=self.bucket,
            prefix=self.prefix_test_list_objects,
            batch_size=10,
        )
        assert len(result.contents().all()) == 11

    def _test_paginate_list_objects_v2_common_prefixs(self):
        contents, common_prefixes = paginate_list_objects_v2(
            s3_client=self.s3_client,
            bucket=self.bucket,
            prefix=self.prefix_test_list_objects,
            delimiter="/",
        ).contents_and_common_prefixs()
        assert len(contents) == 2 # 2 files
        assert len(common_prefixes) == 3 # 3 folders

        result = paginate_list_objects_v2(
            s3_client=self.s3_client,
            bucket=self.bucket,
            prefix=self.prefix_test_list_objects,
            delimiter="/",
        )
        assert len(result.common_prefixs().all()) == 3 # 3 folders

    def _test_paginate_list_objects_v2_hard_and_soft_folder(self):
        contents, common_prefixes = paginate_list_objects_v2(
            s3_client=self.s3_client,
            bucket=self.bucket,
            prefix=self.prefix_dummy_data,
        ).contents_and_common_prefixs()

        # file.txt
        # soft_folder/file.txt,
        # hard_folder/
        # hard_folder/file.txt
        # empty_hard_folder/
        assert len(contents) == 5
        assert len(common_prefixes) == 0

        contents, common_prefixes = paginate_list_objects_v2(
            s3_client=self.s3_client,
            bucket=self.bucket,
            prefix=self.prefix_dummy_data,
            delimiter="/",
        ).contents_and_common_prefixs()

        assert len(contents) == 1  # file.txt
        assert len(common_prefixes) == 3  # soft_folder, hard_folder, empty_hard_folder

        contents, common_prefixes = paginate_list_objects_v2(
            s3_client=self.s3_client,
            bucket=self.bucket,
            prefix=self.prefix_hard_folder,
            delimiter="/",
        ).contents_and_common_prefixs()
        assert len(contents) == 2  # hard_folder/, hard_folder/file.txt
        assert len(common_prefixes) == 0

    def _test_calculate_total_size(self):
        s3_client = self.s3_client
        bucket = self.bucket

        count, total_size = calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_soft_folder,
            include_folder=False,
        )
        assert count == 1

        count, total_size = calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_soft_folder,
        )
        assert count == 1

        count, total_size = calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_hard_folder,
            include_folder=False,
        )
        assert count == 1

        count, total_size = calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_hard_folder,
            include_folder=True,
        )
        assert count == 2

        count, total_size = calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_empty_hard_folder,
            include_folder=False,
        )
        assert count == 0

        count, total_size = calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_empty_hard_folder,
            include_folder=True,
        )
        assert count == 1

        count, total_size = calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.key_never_exists,
            include_folder=True,
        )
        assert count == 0

        count, total_size = calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_never_exists,
            include_folder=True,
        )
        assert count == 0

    def _test_count_objects(self):
        s3_client = self.s3_client
        bucket = self.bucket

        count = count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_soft_folder,
            include_folder=False,
        )
        assert count == 1

        count = count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_soft_folder,
            include_folder=True,
        )
        assert count == 1

        count = count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_hard_folder,
            include_folder=False,
        )
        assert count == 1

        count = count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_hard_folder,
            include_folder=True,
        )
        assert count == 2

        count = count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_empty_hard_folder,
            include_folder=False,
        )
        assert count == 0

        count = count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_empty_hard_folder,
            include_folder=True,
        )
        assert count == 1

        count = count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.key_never_exists,
            include_folder=True,
        )
        assert count == 0

        count = count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=self.prefix_never_exists,
            include_folder=True,
        )
        assert count == 0

    def test(self):
        self._test_paginate_list_objects_v2_argument_error()
        self._test_paginate_list_objects_v2_contents()
        self._test_paginate_list_objects_v2_common_prefixs()
        self._test_paginate_list_objects_v2_hard_and_soft_folder()
        self._test_calculate_total_size()
        self._test_count_objects()


class Test(BetterListObjects):
    use_mock = False


class TestUseMock(BetterListObjects):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.better_client.list_objects", preview=False)
